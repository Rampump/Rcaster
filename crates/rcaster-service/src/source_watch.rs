use std::{
    collections::BTreeMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use etcd_client::{Client, EventType, GetOptions, WatchOptions};
use rcaster_core::str_record::StrRecord;
use tokio::{sync::oneshot, task::JoinHandle};

use crate::{
    ETCD_WATCH_IDLE_TIMEOUT,
    etcd_watch_metrics::{EtcdWatchMetrics, WatchLane},
    source_claim::{ClaimError, SourceRoute, decode_source_route},
};

pub struct SourceWatcher {
    sources: Arc<RwLock<BTreeMap<String, (i64, SourceRoute)>>>,
    healthy: Arc<AtomicBool>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl SourceWatcher {
    pub(crate) async fn start_with_metrics(
        mut client: Client,
        cluster_id: &str,
        metrics: Arc<EtcdWatchMetrics>,
    ) -> Result<Self, ClaimError> {
        let prefix = source_prefix(cluster_id)?;
        let (loaded, revision) = load_sources(&mut client, &prefix).await?;
        metrics.observe(WatchLane::Sources, revision, []);
        let sources = Arc::new(RwLock::new(loaded));
        let healthy = Arc::new(AtomicBool::new(false));
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let task_sources = Arc::clone(&sources);
        let task_healthy = Arc::clone(&healthy);
        let task_metrics = Arc::clone(&metrics);
        let task = tokio::spawn(async move {
            run_source_watch(
                client,
                prefix,
                revision + 1,
                task_sources,
                task_healthy,
                task_metrics,
                &mut stop_rx,
            )
            .await;
        });
        Ok(Self {
            sources,
            healthy,
            stop: Some(stop_tx),
            task: Some(task),
        })
    }

    pub fn sourcetable_records(&self) -> Vec<StrRecord> {
        self.sources
            .read()
            .map(|sources| {
                sources
                    .values()
                    .map(|(_, route)| route.str_record.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn route(&self, mountpoint: &str) -> Option<SourceRoute> {
        self.sources
            .read()
            .ok()?
            .get(mountpoint)
            .map(|(_, route)| route.clone())
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub(crate) fn health_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.healthy)
    }

    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

async fn run_source_watch(
    mut client: Client,
    prefix: String,
    mut start_revision: i64,
    sources: Arc<RwLock<BTreeMap<String, (i64, SourceRoute)>>>,
    healthy: Arc<AtomicBool>,
    metrics: Arc<EtcdWatchMetrics>,
    stop: &mut oneshot::Receiver<()>,
) {
    loop {
        let loaded = tokio::select! {
            _ = &mut *stop => return,
            result = load_sources(&mut client, &prefix) => result,
        };
        match loaded {
            Ok((snapshot, revision)) => {
                if let Ok(mut current) = sources.write() {
                    *current = snapshot;
                } else {
                    healthy.store(false, Ordering::Release);
                    return;
                }
                start_revision = start_revision.max(revision + 1);
            }
            Err(_) => {
                metrics.record_recovery(WatchLane::Sources);
                healthy.store(false, Ordering::Release);
                if stop_or_sleep(stop, std::time::Duration::from_millis(250)).await {
                    return;
                }
                continue;
            }
        }

        let stream = tokio::select! {
            _ = &mut *stop => return,
            result = client.watch(
                prefix.clone(),
                Some(
                    WatchOptions::new()
                        .with_prefix()
                        .with_start_revision(start_revision)
                        .with_progress_notify(),
                ),
            ) => result,
        };
        let Ok(mut stream) = stream else {
            metrics.record_recovery(WatchLane::Sources);
            healthy.store(false, Ordering::Release);
            if stop_or_sleep(stop, std::time::Duration::from_millis(250)).await {
                return;
            }
            continue;
        };
        healthy.store(true, Ordering::Release);

        'watch: loop {
            let response = tokio::select! {
                _ = &mut *stop => return,
                response = tokio::time::timeout(ETCD_WATCH_IDLE_TIMEOUT, stream.message()) => response,
            };
            let Some(Some(response)) = response.ok().and_then(Result::ok) else {
                metrics.record_recovery(WatchLane::Sources);
                break;
            };
            let response_revision = response
                .header()
                .map(|header| header.revision())
                .unwrap_or(start_revision);
            metrics.observe(
                WatchLane::Sources,
                response_revision,
                response
                    .events()
                    .iter()
                    .filter_map(|event| event.kv().map(|value| value.mod_revision())),
            );
            if response.canceled() {
                metrics.record_recovery(WatchLane::Sources);
                break;
            }
            for event in response.events() {
                let Some(kv) = event.kv() else {
                    continue;
                };
                start_revision = start_revision.max(kv.mod_revision() + 1);
                let Some(mountpoint) = mountpoint_from_key(&prefix, kv.key()) else {
                    healthy.store(false, Ordering::Release);
                    break 'watch;
                };
                let Ok(mut current) = sources.write() else {
                    healthy.store(false, Ordering::Release);
                    return;
                };
                if current
                    .get(mountpoint)
                    .is_some_and(|(revision, _)| *revision > kv.mod_revision())
                {
                    continue;
                }
                if event.event_type() == EventType::Delete {
                    current.remove(mountpoint);
                    continue;
                }
                match decode_source_for_key(mountpoint, kv.value()) {
                    Ok(Some(route)) => {
                        current.insert(mountpoint.to_owned(), (kv.mod_revision(), route));
                    }
                    Ok(None) => {
                        current.remove(mountpoint);
                    }
                    Err(_) => {
                        healthy.store(false, Ordering::Release);
                        break 'watch;
                    }
                }
            }
        }
        healthy.store(false, Ordering::Release);
        if stop_or_sleep(stop, std::time::Duration::from_millis(100)).await {
            return;
        }
    }
}

async fn load_sources(
    client: &mut Client,
    prefix: &str,
) -> Result<(BTreeMap<String, (i64, SourceRoute)>, i64), ClaimError> {
    let response = client
        .get(prefix.to_owned(), Some(GetOptions::new().with_prefix()))
        .await?;
    let revision = response
        .header()
        .ok_or(ClaimError::CorruptState)?
        .revision();
    let mut sources = BTreeMap::new();
    for kv in response.kvs() {
        let Some(mountpoint) = mountpoint_from_key(prefix, kv.key()) else {
            return Err(ClaimError::CorruptState);
        };
        if let Some(route) = decode_source_for_key(mountpoint, kv.value())? {
            sources.insert(mountpoint.to_owned(), (kv.mod_revision(), route));
        }
    }
    Ok((sources, revision))
}

fn source_prefix(cluster_id: &str) -> Result<String, ClaimError> {
    if cluster_id.is_empty()
        || !cluster_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ClaimError::InvalidArgument("cluster ID"));
    }
    Ok(format!("/rcaster/{cluster_id}/runtime/sources/"))
}

fn mountpoint_from_key<'a>(prefix: &str, key: &'a [u8]) -> Option<&'a str> {
    let key = std::str::from_utf8(key).ok()?.strip_prefix(prefix)?;
    (!key.is_empty() && !key.contains('/')).then_some(key)
}

fn decode_source_for_key(
    mountpoint: &str,
    value: &[u8],
) -> Result<Option<SourceRoute>, ClaimError> {
    let Some(route) = decode_source_route(value)? else {
        return Ok(None);
    };
    if route.mountpoint != mountpoint || route.str_record.mountpoint != mountpoint {
        return Err(ClaimError::CorruptState);
    }
    Ok(Some(route))
}

async fn stop_or_sleep(stop: &mut oneshot::Receiver<()>, duration: std::time::Duration) -> bool {
    tokio::select! {
        _ = &mut *stop => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::mountpoint_from_key;

    #[test]
    fn source_keys_must_name_one_mountpoint() {
        let prefix = "/rcaster/test/runtime/sources/";

        assert_eq!(
            mountpoint_from_key(prefix, b"/rcaster/test/runtime/sources/C63"),
            Some("C63")
        );
        assert_eq!(
            mountpoint_from_key(prefix, b"/rcaster/test/runtime/sources/C63/extra"),
            None
        );
        assert_eq!(
            mountpoint_from_key(prefix, b"/rcaster/test/runtime/sources/"),
            None
        );
    }
}
