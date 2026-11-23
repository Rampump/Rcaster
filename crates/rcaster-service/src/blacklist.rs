use std::{
    collections::HashSet,
    fmt,
    net::IpAddr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use etcd_client::{
    Client, Compare, CompareOp, DeleteOptions, GetOptions, PutOptions, Txn, TxnOp, WatchOptions,
};
use serde::{Deserialize, Serialize};
use tokio::{sync::oneshot, task::JoinHandle};

use crate::{ETCD_WATCH_IDLE_TIMEOUT, session_limit::IpNetwork};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct BlacklistRule {
    pub schema_version: u8,
    pub mod_revision: u64,
    pub id: String,
    pub cidr: String,
    pub quota: i32,
    pub enabled: bool,
    pub description: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Default)]
pub struct BlacklistSnapshot {
    rules: Vec<BlacklistRule>,
    all_rules: Vec<BlacklistRule>,
}

pub struct BlacklistWatcher {
    snapshot: Arc<RwLock<BlacklistSnapshot>>,
    healthy: Arc<AtomicBool>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlacklistError {
    InvalidConfig,
    NotFound,
    Conflict,
    Etcd(String),
    Json(String),
}

impl BlacklistSnapshot {
    pub fn quota_for_ip(&self, address: IpAddr) -> Option<i32> {
        self.rules
            .iter()
            .filter_map(|rule| {
                let network = IpNetwork::parse(&rule.cidr)?;
                network
                    .contains(address)
                    .then_some((network.prefix(), rule.quota))
            })
            .max_by_key(|(prefix, _)| *prefix)
            .map(|(_, quota)| quota)
    }

    pub fn is_ip_denied(&self, address: IpAddr) -> bool {
        self.quota_for_ip(address) == Some(0)
    }

    pub fn rules(&self) -> &[BlacklistRule] {
        &self.rules
    }

    pub fn all_rules(&self) -> &[BlacklistRule] {
        &self.all_rules
    }
}

impl BlacklistWatcher {
    pub async fn start(mut client: Client, cluster_id: &str) -> Result<Self, BlacklistError> {
        let loaded = load_snapshot(&mut client, cluster_id).await?;
        let snapshot = Arc::new(RwLock::new(loaded.snapshot));
        let healthy = Arc::new(AtomicBool::new(false));
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let task_snapshot = Arc::clone(&snapshot);
        let task_healthy = Arc::clone(&healthy);
        let cluster = cluster_id.to_owned();
        let task = tokio::spawn(async move {
            run_watch(client, cluster, task_snapshot, task_healthy, &mut stop_rx).await;
        });
        Ok(Self {
            snapshot,
            healthy,
            stop: Some(stop_tx),
            task: Some(task),
        })
    }

    pub fn snapshot(&self) -> BlacklistSnapshot {
        self.snapshot
            .read()
            .map(|snapshot| snapshot.clone())
            .unwrap_or_default()
    }

    pub fn quota_for_ip(&self, address: IpAddr) -> Option<i32> {
        self.snapshot
            .read()
            .ok()
            .and_then(|snapshot| snapshot.quota_for_ip(address))
    }

    pub fn is_ip_denied(&self, address: IpAddr) -> bool {
        self.quota_for_ip(address) == Some(0)
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
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

async fn run_watch(
    mut client: Client,
    cluster: String,
    snapshot: Arc<RwLock<BlacklistSnapshot>>,
    healthy: Arc<AtomicBool>,
    stop: &mut oneshot::Receiver<()>,
) {
    loop {
        let loaded = tokio::select! {
            _ = &mut *stop => return,
            result = load_snapshot(&mut client, &cluster) => result,
        };
        let start_revision = match loaded {
            Ok(loaded) => {
                if let Ok(mut current) = snapshot.write() {
                    *current = loaded.snapshot;
                }
                loaded.revision + 1
            }
            Err(_) => {
                healthy.store(false, Ordering::Release);
                if stop_or_sleep(stop, std::time::Duration::from_millis(250)).await {
                    return;
                }
                continue;
            }
        };
        let watcher = tokio::select! {
            _ = &mut *stop => return,
            result = client.watch(
                blacklist_prefix(&cluster),
                Some(
                    WatchOptions::new()
                        .with_prefix()
                        .with_start_revision(start_revision)
                        .with_progress_notify(),
                ),
            ) => result,
        };
        let Ok(mut stream) = watcher else {
            healthy.store(false, Ordering::Release);
            continue;
        };
        healthy.store(true, Ordering::Release);
        loop {
            let response = tokio::select! {
                _ = &mut *stop => return,
                response = tokio::time::timeout(ETCD_WATCH_IDLE_TIMEOUT, stream.message()) => response,
            };
            let Some(Some(response)) = response.ok().and_then(Result::ok) else {
                break;
            };
            if response.canceled() {
                healthy.store(false, Ordering::Release);
                break;
            }
            if response.events().is_empty() {
                continue;
            }
            match load_snapshot(&mut client, &cluster).await {
                Ok(loaded) => {
                    if let Ok(mut current) = snapshot.write() {
                        *current = loaded.snapshot;
                        healthy.store(true, Ordering::Release);
                    } else {
                        healthy.store(false, Ordering::Release);
                    }
                }
                Err(_) => {
                    healthy.store(false, Ordering::Release);
                }
            }
        }
        healthy.store(false, Ordering::Release);
        if stop_or_sleep(stop, std::time::Duration::from_millis(100)).await {
            return;
        }
    }
}

struct LoadedBlacklist {
    snapshot: BlacklistSnapshot,
    revision: i64,
}

async fn load_snapshot(
    client: &mut Client,
    cluster_id: &str,
) -> Result<LoadedBlacklist, BlacklistError> {
    let response = client
        .get(
            blacklist_prefix(cluster_id),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .map_err(|error| BlacklistError::Etcd(error.to_string()))?;
    let revision = response
        .header()
        .map(|header| header.revision())
        .unwrap_or_default();
    let mut networks = HashSet::new();
    let mut rules = Vec::new();
    let mut all_rules = Vec::new();
    for kv in response.kvs() {
        let mut rule: BlacklistRule = serde_json::from_slice(kv.value())
            .map_err(|error| BlacklistError::Json(error.to_string()))?;
        if rule.schema_version != 1 {
            return Err(BlacklistError::InvalidConfig);
        }
        rule.mod_revision = kv.mod_revision() as u64;
        all_rules.push(rule.clone());
        if !rule.enabled {
            continue;
        }
        let network = IpNetwork::parse(&rule.cidr).ok_or(BlacklistError::InvalidConfig)?;
        if !networks.insert(network.normalized_key()) {
            return Err(BlacklistError::InvalidConfig);
        }
        rules.push(rule);
    }
    Ok(LoadedBlacklist {
        snapshot: BlacklistSnapshot { rules, all_rules },
        revision,
    })
}

fn blacklist_prefix(cluster_id: &str) -> String {
    format!("/rcaster/{cluster_id}/config/blacklist/")
}

async fn stop_or_sleep(stop: &mut oneshot::Receiver<()>, duration: std::time::Duration) -> bool {
    tokio::select! {
        _ = &mut *stop => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

impl fmt::Display for BlacklistError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig => formatter.write_str("invalid blacklist configuration"),
            Self::NotFound => formatter.write_str("blacklist rule not found"),
            Self::Conflict => formatter.write_str("blacklist rule changed concurrently"),
            Self::Etcd(error) => write!(formatter, "blacklist etcd error: {error}"),
            Self::Json(error) => write!(formatter, "blacklist JSON error: {error}"),
        }
    }
}

pub async fn upsert_blacklist_rule(
    client: &mut Client,
    cluster_id: &str,
    rule: &BlacklistRule,
    expected_revision: Option<u64>,
) -> Result<u64, BlacklistError> {
    validate_rule(rule)?;
    let network = IpNetwork::parse(&rule.cidr).ok_or(BlacklistError::InvalidConfig)?;
    let prefix = blacklist_prefix(cluster_id);
    for kv in client
        .get(prefix, Some(GetOptions::new().with_prefix()))
        .await
        .map_err(|error| BlacklistError::Etcd(error.to_string()))?
        .kvs()
    {
        let existing: BlacklistRule = serde_json::from_slice(kv.value())
            .map_err(|error| BlacklistError::Json(error.to_string()))?;
        if existing.id != rule.id
            && existing.enabled
            && rule.enabled
            && IpNetwork::parse(&existing.cidr)
                .is_some_and(|existing| existing.normalized_key() == network.normalized_key())
        {
            return Err(BlacklistError::InvalidConfig);
        }
    }
    let key = format!("{}{}", blacklist_prefix(cluster_id), rule.id);
    let current = client
        .get(key.clone(), None)
        .await
        .map_err(|error| BlacklistError::Etcd(error.to_string()))?
        .kvs()
        .first()
        .cloned();
    let compare = match expected_revision {
        Some(revision) => {
            if current.as_ref().map(|value| value.mod_revision() as u64) != Some(revision) {
                return Err(BlacklistError::Conflict);
            }
            Compare::mod_revision(key.clone(), CompareOp::Equal, revision as i64)
        }
        None if current.is_none() => Compare::create_revision(key.clone(), CompareOp::Equal, 0),
        None => return Err(BlacklistError::Conflict),
    };
    let value =
        serde_json::to_vec(rule).map_err(|error| BlacklistError::Json(error.to_string()))?;
    let response = client
        .txn(
            Txn::new()
                .when([compare])
                .and_then([TxnOp::put(key, value, Some(PutOptions::new()))]),
        )
        .await
        .map_err(|error| BlacklistError::Etcd(error.to_string()))?;
    if !response.succeeded() {
        return Err(BlacklistError::Conflict);
    }
    response
        .header()
        .map(|header| header.revision() as u64)
        .ok_or(BlacklistError::Conflict)
}

pub async fn delete_blacklist_rule(
    client: &mut Client,
    cluster_id: &str,
    id: &str,
    expected_revision: u64,
) -> Result<(), BlacklistError> {
    let key = format!("{}{}", blacklist_prefix(cluster_id), id);
    let response = client
        .txn(
            Txn::new()
                .when([Compare::mod_revision(
                    key.clone(),
                    CompareOp::Equal,
                    expected_revision as i64,
                )])
                .and_then([TxnOp::delete(key, Some(DeleteOptions::new()))]),
        )
        .await
        .map_err(|error| BlacklistError::Etcd(error.to_string()))?;
    if response.succeeded() {
        Ok(())
    } else {
        Err(BlacklistError::Conflict)
    }
}

fn validate_rule(rule: &BlacklistRule) -> Result<(), BlacklistError> {
    if rule.schema_version != 1
        || rule.id.is_empty()
        || rule.id.len() > 128
        || !rule
            .id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        || rule.description.len() > 512
        || rule.quota < -1
        || IpNetwork::parse(&rule.cidr).is_none()
    {
        return Err(BlacklistError::InvalidConfig);
    }
    Ok(())
}

impl std::error::Error for BlacklistError {}
