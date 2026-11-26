use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use rcaster_core::protocol::ProtocolDemux;
use tokio::sync::Mutex;

use crate::{
    cluster_runtime::SharedClusterRuntime,
    coordinator_grpc::{GrpcClientTls, proto::RelayOpenRequest},
    hub::{MountpointHub, SourceLease},
    relay_grpc::RelayGrpcClient,
};

pub struct RelayManager {
    hub: Arc<MountpointHub>,
    runtime: SharedClusterRuntime,
    tls: GrpcClientTls,
    relays: Arc<Mutex<HashMap<String, RelaySlot>>>,
    backoff: Mutex<HashMap<String, (Instant, u32)>>,
    max_relays: usize,
    max_starting: usize,
    idle_timeout: Duration,
    failures: Arc<AtomicU64>,
    idle_closes: Arc<AtomicU64>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RelayView {
    pub mountpoint: String,
    pub subscribers: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RelayMetrics {
    pub starting: usize,
    pub failures: u64,
    pub idle_closes: u64,
}

enum RelaySlot {
    Starting(Arc<Mutex<()>>),
    Active(Arc<SourceLease>),
}

impl RelayManager {
    pub fn new(hub: Arc<MountpointHub>, runtime: SharedClusterRuntime, tls: GrpcClientTls) -> Self {
        Self::with_limits(hub, runtime, tls, 100, 16, Duration::from_secs(30))
    }

    pub fn with_limits(
        hub: Arc<MountpointHub>,
        runtime: SharedClusterRuntime,
        tls: GrpcClientTls,
        max_relays: usize,
        max_starting: usize,
        idle_timeout: Duration,
    ) -> Self {
        Self {
            hub,
            runtime,
            tls,
            relays: Arc::new(Mutex::new(HashMap::new())),
            backoff: Mutex::new(HashMap::new()),
            max_relays,
            max_starting,
            idle_timeout,
            failures: Arc::new(AtomicU64::new(0)),
            idle_closes: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn ensure(&self, mountpoint: &str) -> Result<(), String> {
        if let Some((until, _)) = self.backoff.lock().await.get(mountpoint)
            && Instant::now() < *until
        {
            return Err("relay retry cooldown active".to_owned());
        }
        let result = self.ensure_inner(mountpoint).await;
        if result.is_ok() {
            self.backoff.lock().await.remove(mountpoint);
        } else {
            self.failures.fetch_add(1, Ordering::Relaxed);
            let mut backoff = self.backoff.lock().await;
            let failures = backoff
                .get(mountpoint)
                .map(|(_, failures)| failures.saturating_add(1))
                .unwrap_or(1);
            let seconds = (1_u64 << failures.saturating_sub(1).min(5)).min(60);
            backoff.insert(
                mountpoint.to_owned(),
                (Instant::now() + Duration::from_secs(seconds), failures),
            );
        }
        result
    }

    pub async fn active_count(&self) -> usize {
        self.relays
            .lock()
            .await
            .values()
            .filter(|slot| matches!(slot, RelaySlot::Active(source) if source.is_active()))
            .count()
    }

    pub async fn views(&self) -> Vec<RelayView> {
        let mut views = self
            .relays
            .lock()
            .await
            .values()
            .filter_map(|slot| match slot {
                RelaySlot::Active(source) if source.is_active() => Some(RelayView {
                    mountpoint: source.mountpoint().to_owned(),
                    subscribers: source.subscriber_count(),
                }),
                _ => None,
            })
            .collect::<Vec<_>>();
        views.sort_by(|left, right| left.mountpoint.cmp(&right.mountpoint));
        views
    }

    pub async fn metrics(&self) -> RelayMetrics {
        RelayMetrics {
            starting: self
                .relays
                .lock()
                .await
                .values()
                .filter(|slot| matches!(slot, RelaySlot::Starting(_)))
                .count(),
            failures: self.failures.load(Ordering::Relaxed),
            idle_closes: self.idle_closes.load(Ordering::Relaxed),
        }
    }

    async fn ensure_inner(&self, mountpoint: &str) -> Result<(), String> {
        if self.max_relays == 0 || self.max_starting == 0 || self.idle_timeout.is_zero() {
            return Err("invalid relay limits".to_owned());
        }
        let (start_gate, owns_start) = {
            let mut relays = self.relays.lock().await;
            relays.retain(|_, slot| match slot {
                RelaySlot::Active(source) => source.is_active(),
                RelaySlot::Starting(_) => true,
            });
            match relays.get(mountpoint) {
                Some(RelaySlot::Active(_)) => return Ok(()),
                Some(RelaySlot::Starting(gate)) => (Arc::clone(gate), false),
                None => {
                    if relays.len() >= self.max_relays {
                        return Err("relay limit reached".to_owned());
                    }
                    if relays
                        .values()
                        .filter(|slot| matches!(slot, RelaySlot::Starting(_)))
                        .count()
                        >= self.max_starting
                    {
                        return Err("relay starting limit reached".to_owned());
                    }
                    let gate = Arc::new(Mutex::new(()));
                    relays.insert(
                        mountpoint.to_owned(),
                        RelaySlot::Starting(Arc::clone(&gate)),
                    );
                    (gate, true)
                }
            }
        };
        let mut starting_guard = owns_start.then(|| StartingRelayGuard {
            relays: Arc::clone(&self.relays),
            mountpoint: mountpoint.to_owned(),
            gate: Arc::clone(&start_gate),
        });
        let _start_guard = start_gate.lock().await;
        if self
            .relays
            .lock()
            .await
            .get(mountpoint)
            .is_some_and(|slot| matches!(slot, RelaySlot::Active(source) if source.is_active()))
        {
            if let Some(guard) = starting_guard.as_mut() {
                guard.disarm();
            }
            return Ok(());
        }
        let route = match self.runtime.source_route(mountpoint).await {
            Some(route) => route,
            None => {
                self.relays.lock().await.remove(mountpoint);
                return Err("source is not active".to_owned());
            }
        };
        if self.runtime.caster_id().await.as_deref() == Some(route.caster_id.as_str()) {
            self.relays.lock().await.remove(mountpoint);
            return Err("source is owned by this caster".to_owned());
        }
        let source = Arc::new(
            self.hub
                .claim_source(mountpoint)
                .map_err(|error| error.to_string())?,
        );
        let mut client = RelayGrpcClient::connect_mtls(
            format!("https://{}", route.relay_endpoint),
            self.tls.clone(),
        )
        .await
        .map_err(|error| {
            source.close();
            error.message().to_owned()
        })?;
        let cluster_id = match self.runtime.cluster_id().await {
            Some(cluster_id) => cluster_id,
            None => {
                source.close();
                return Err("relay runtime unavailable".to_owned());
            }
        };
        let request = RelayOpenRequest {
            cluster_id,
            mountpoint: route.mountpoint.clone(),
            source_generation: route.source_generation,
            connection_id: route.connection_id.clone(),
            requesting_caster_id: self.runtime.caster_id().await.unwrap_or_default(),
        };
        let mut stream = tokio::time::timeout(Duration::from_secs(5), client.open(request))
            .await
            .map_err(|_| {
                source.close();
                "relay connect timed out".to_owned()
            })?
            .map_err(|error| {
                source.close();
                error.message().to_owned()
            })?;
        source.set_str_record(route.str_record).map_err(|error| {
            source.close();
            error.to_string()
        })?;
        self.relays.lock().await.insert(
            mountpoint.to_owned(),
            RelaySlot::Active(Arc::clone(&source)),
        );
        if let Some(guard) = starting_guard.as_mut() {
            guard.disarm();
        }
        let idle_source = Arc::clone(&source);
        let idle_timeout = self.idle_timeout;
        let idle_closes = Arc::clone(&self.idle_closes);
        tokio::spawn(async move {
            let interval = (idle_timeout / 4).min(Duration::from_secs(1));
            loop {
                tokio::time::sleep(interval).await;
                if !idle_source.is_active() {
                    return;
                }
                if idle_source.subscriber_count() == 0 {
                    tokio::time::sleep(idle_timeout).await;
                    if idle_source.is_active() && idle_source.subscriber_count() == 0 {
                        idle_closes.fetch_add(1, Ordering::Relaxed);
                        idle_source.close();
                        return;
                    }
                }
            }
        });
        let mut demux = ProtocolDemux::new();
        tokio::spawn(async move {
            loop {
                match stream.message().await {
                    Ok(Some(chunk)) => {
                        if source
                            .publish_analyzed(Arc::from(chunk.data), &mut demux)
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(None) => {
                        eprintln!("rcaster: relay stream ended for {}", source.mountpoint());
                        break;
                    }
                    Err(error) => {
                        eprintln!(
                            "rcaster: relay stream failed for {}: {error}",
                            source.mountpoint()
                        );
                        break;
                    }
                }
            }
            source.close();
        });
        Ok(())
    }
}

struct StartingRelayGuard {
    relays: Arc<Mutex<HashMap<String, RelaySlot>>>,
    mountpoint: String,
    gate: Arc<Mutex<()>>,
}

impl StartingRelayGuard {
    fn disarm(&mut self) {
        self.mountpoint.clear();
    }
}

impl Drop for StartingRelayGuard {
    fn drop(&mut self) {
        if self.mountpoint.is_empty() {
            return;
        }
        let relays = Arc::clone(&self.relays);
        let mountpoint = std::mem::take(&mut self.mountpoint);
        let gate = Arc::clone(&self.gate);
        tokio::spawn(async move {
            let mut relays = relays.lock().await;
            if relays.get(&mountpoint).is_some_and(
                |slot| matches!(slot, RelaySlot::Starting(existing) if Arc::ptr_eq(existing, &gate)),
            ) {
                relays.remove(&mountpoint);
            }
        });
    }
}
