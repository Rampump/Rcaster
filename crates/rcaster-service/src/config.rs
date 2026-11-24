use std::{
    fmt,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use etcd_client::{Client, Compare, CompareOp, EventType, GetOptions, Txn, TxnOp, WatchOptions};
use serde::{Deserialize, Serialize};
use tokio::{sync::oneshot, task::JoinHandle};

use crate::{
    ETCD_WATCH_IDLE_TIMEOUT,
    etcd_watch_metrics::{EtcdWatchMetrics, WatchLane},
};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CasterConfig {
    pub schema_version: u8,
    pub generation: u64,
    #[serde(default)]
    pub source_health: SourceHealthConfig,
    #[serde(default)]
    pub limits: CasterLimits,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SourceHealthConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_health_short_window_s")]
    pub short_window_s: u64,
    #[serde(default = "default_health_long_window_s")]
    pub long_window_s: u64,
    pub observation_timeout_s: u64,
    #[serde(default = "default_health_valid_frame_ratio_min")]
    pub valid_frame_ratio_min: f64,
    pub crc_error_ratio_max: f64,
    #[serde(default = "default_health_alert_clear_s")]
    pub alert_clear_s: u64,
    #[serde(default = "default_true")]
    pub station_id_change_alert: bool,
    #[serde(default = "default_base_position_jump_m")]
    pub base_position_jump_m: f64,
    #[serde(default = "default_health_action")]
    pub action: String,
}

impl Default for SourceHealthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            short_window_s: 10,
            long_window_s: 60,
            observation_timeout_s: 10,
            valid_frame_ratio_min: 0.8,
            crc_error_ratio_max: 0.1,
            alert_clear_s: 30,
            station_id_change_alert: true,
            base_position_jump_m: 100.0,
            action: default_health_action(),
        }
    }
}

impl SourceHealthConfig {
    pub fn is_valid(&self) -> bool {
        self.short_window_s > 0
            && self.long_window_s >= self.short_window_s
            && self.observation_timeout_s > 0
            && (0.0..=1.0).contains(&self.valid_frame_ratio_min)
            && (0.0..=1.0).contains(&self.crc_error_ratio_max)
            && self.alert_clear_s > 0
            && self.base_position_jump_m.is_finite()
            && self.base_position_jump_m > 0.0
            && self.action == "alert"
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CasterLimits {
    pub max_source_bitrate_bps: u64,
    pub max_sources: usize,
    #[serde(default = "default_max_validating_sources")]
    pub max_validating_sources: usize,
    pub max_subscribers: usize,
    pub max_subscribers_per_source: usize,
}

impl Default for CasterLimits {
    fn default() -> Self {
        Self {
            max_source_bitrate_bps: 2_000_000,
            max_sources: 500,
            max_validating_sources: 64,
            max_subscribers: 2500,
            max_subscribers_per_source: 2000,
        }
    }
}

impl CasterConfig {
    pub fn new(generation: u64) -> Self {
        Self {
            schema_version: 1,
            generation,
            source_health: SourceHealthConfig::default(),
            limits: CasterLimits::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConfigSnapshot {
    pub config: CasterConfig,
    pub revision: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ConfigRevision {
    pub config: CasterConfig,
    pub revision: i64,
}

pub struct ConfigWatcher {
    snapshot: Arc<RwLock<ConfigSnapshot>>,
    healthy: Arc<AtomicBool>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

#[derive(Debug)]
pub enum ConfigError {
    InvalidIdentifier(&'static str),
    InvalidConfig,
    UnsupportedSchema(u8),
    GenerationNotMonotonic,
    ConfigConflict,
    Etcd(etcd_client::Error),
    Json(serde_json::Error),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentifier(name) => write!(formatter, "invalid {name}"),
            Self::InvalidConfig => formatter.write_str("caster config is missing or invalid"),
            Self::UnsupportedSchema(version) => {
                write!(formatter, "unsupported caster config schema {version}")
            }
            Self::GenerationNotMonotonic => {
                formatter.write_str("caster config generation must increase")
            }
            Self::ConfigConflict => formatter.write_str("caster config changed concurrently"),
            Self::Etcd(error) => write!(formatter, "etcd error: {error}"),
            Self::Json(error) => write!(formatter, "json error: {error}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<etcd_client::Error> for ConfigError {
    fn from(error: etcd_client::Error) -> Self {
        Self::Etcd(error)
    }
}

impl From<serde_json::Error> for ConfigError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

pub async fn publish_config(
    client: &mut Client,
    cluster_id: &str,
    config: CasterConfig,
) -> Result<ConfigSnapshot, ConfigError> {
    let key = config_key(cluster_id)?;
    let current = client.get(key.clone(), None).await?.kvs().first().cloned();
    let compare = match &current {
        Some(value) => {
            let old = decode_config(value.value())?;
            if config.generation <= old.generation {
                return Err(ConfigError::GenerationNotMonotonic);
            }
            Compare::mod_revision(key.clone(), CompareOp::Equal, value.mod_revision())
        }
        None => Compare::create_revision(key.clone(), CompareOp::Equal, 0),
    };
    let value = serde_json::to_vec(&config)?;
    let archive_key = config_revision_key(cluster_id, config.generation)?;
    let transaction = Txn::new().when([compare]).and_then([
        TxnOp::put(key, value.clone(), None),
        TxnOp::put(archive_key, value, None),
    ]);
    let response = client.txn(transaction).await?;
    if !response.succeeded() {
        return Err(ConfigError::ConfigConflict);
    }
    let revision = response
        .header()
        .ok_or(ConfigError::InvalidConfig)?
        .revision();

    Ok(ConfigSnapshot { config, revision })
}

pub async fn load_config_revisions(
    client: &mut Client,
    cluster_id: &str,
    limit: usize,
) -> Result<Vec<ConfigRevision>, ConfigError> {
    let prefix = format!(
        "/rcaster/{}/config/revisions/",
        validate_cluster_id(cluster_id)?
    );
    let response = client
        .get(prefix, Some(GetOptions::new().with_prefix()))
        .await?;
    let mut revisions = response
        .kvs()
        .iter()
        .map(|value| {
            Ok(ConfigRevision {
                config: decode_config(value.value())?,
                revision: value.mod_revision(),
            })
        })
        .collect::<Result<Vec<_>, ConfigError>>()?;
    revisions.sort_by_key(|revision| std::cmp::Reverse(revision.config.generation));
    revisions.truncate(limit.clamp(1, 500));
    Ok(revisions)
}

pub async fn prune_config_revisions(
    client: &mut Client,
    cluster_id: &str,
    retain: usize,
) -> Result<usize, ConfigError> {
    let prefix = format!(
        "/rcaster/{}/config/revisions/",
        validate_cluster_id(cluster_id)?
    );
    let response = client
        .get(prefix, Some(GetOptions::new().with_prefix()))
        .await?;
    let mut all = response
        .kvs()
        .iter()
        .map(|value| {
            Ok(ConfigRevision {
                config: decode_config(value.value())?,
                revision: value.mod_revision(),
            })
        })
        .collect::<Result<Vec<_>, ConfigError>>()?;
    all.sort_by_key(|revision| std::cmp::Reverse(revision.config.generation));
    if all.len() <= retain {
        return Ok(0);
    }
    let remove = all.split_off(retain);
    let removed = remove.len();
    let mut operations = Vec::with_capacity(remove.len());
    for revision in remove {
        operations.push(TxnOp::delete(
            config_revision_key(cluster_id, revision.config.generation)?,
            None,
        ));
    }
    client
        .txn(Txn::new().and_then(operations))
        .await
        .map_err(ConfigError::Etcd)?;
    Ok(removed)
}

pub async fn load_config_snapshot(
    client: &mut Client,
    cluster_id: &str,
) -> Result<ConfigSnapshot, ConfigError> {
    let key = config_key(cluster_id)?;
    let value = client
        .get(key, None)
        .await?
        .kvs()
        .first()
        .cloned()
        .ok_or(ConfigError::InvalidConfig)?;
    let config = decode_config(value.value())?;
    Ok(ConfigSnapshot {
        config,
        revision: value.mod_revision(),
    })
}

impl ConfigWatcher {
    pub async fn start(client: Client, cluster_id: &str) -> Result<Self, ConfigError> {
        Self::start_with_metrics(client, cluster_id, Arc::new(EtcdWatchMetrics::default())).await
    }

    pub(crate) async fn start_with_metrics(
        mut client: Client,
        cluster_id: &str,
        metrics: Arc<EtcdWatchMetrics>,
    ) -> Result<Self, ConfigError> {
        let snapshot = load_config_snapshot(&mut client, cluster_id).await?;
        let snapshot = Arc::new(RwLock::new(snapshot));
        let healthy = Arc::new(AtomicBool::new(false));
        let key = config_key(cluster_id)?;
        let cluster_id = cluster_id.to_owned();
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let task_snapshot = Arc::clone(&snapshot);
        let task_healthy = Arc::clone(&healthy);
        let task_metrics = Arc::clone(&metrics);
        let task = tokio::spawn(async move {
            run_config_watch(
                client,
                cluster_id,
                key,
                task_snapshot,
                task_healthy,
                task_metrics,
                &mut stop_rx,
            )
            .await;
        });
        Ok(Self {
            snapshot,
            healthy,
            stop: Some(stop_tx),
            task: Some(task),
        })
    }

    pub fn snapshot(&self) -> ConfigSnapshot {
        self.snapshot
            .read()
            .map(|snapshot| snapshot.clone())
            .unwrap_or_else(|_| ConfigSnapshot {
                config: CasterConfig::new(0),
                revision: 0,
            })
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub fn replace_snapshot(&self, snapshot: ConfigSnapshot) {
        if let Ok(mut current) = self.snapshot.write()
            && snapshot.revision >= current.revision
        {
            *current = snapshot;
        }
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

async fn run_config_watch(
    mut client: Client,
    cluster_id: String,
    key: String,
    snapshot: Arc<RwLock<ConfigSnapshot>>,
    healthy: Arc<AtomicBool>,
    metrics: Arc<EtcdWatchMetrics>,
    stop: &mut oneshot::Receiver<()>,
) {
    loop {
        let loaded = tokio::select! {
            _ = &mut *stop => return,
            result = load_config_snapshot(&mut client, &cluster_id) => result,
        };
        let loaded = match loaded {
            Ok(loaded) => loaded,
            Err(_) => {
                metrics.record_recovery(WatchLane::Config);
                healthy.store(false, Ordering::Release);
                if stop_or_sleep(stop, std::time::Duration::from_millis(250)).await {
                    return;
                }
                continue;
            }
        };
        if let Ok(mut current) = snapshot.write()
            && loaded.revision >= current.revision
        {
            *current = loaded;
        }
        let start_revision = snapshot
            .read()
            .map(|current| current.revision + 1)
            .unwrap_or(1);
        let stream = tokio::select! {
            _ = &mut *stop => return,
            result = client.watch(
                key.clone(),
                Some(
                    WatchOptions::new()
                        .with_start_revision(start_revision)
                        .with_progress_notify(),
                ),
            ) => result,
        };
        let Ok(mut stream) = stream else {
            metrics.record_recovery(WatchLane::Config);
            healthy.store(false, Ordering::Release);
            if stop_or_sleep(stop, std::time::Duration::from_millis(250)).await {
                return;
            }
            continue;
        };
        healthy.store(true, Ordering::Release);

        loop {
            let response = tokio::select! {
                _ = &mut *stop => return,
                response = tokio::time::timeout(ETCD_WATCH_IDLE_TIMEOUT, stream.message()) => response,
            };
            let Some(Some(response)) = response.ok().and_then(Result::ok) else {
                metrics.record_recovery(WatchLane::Config);
                break;
            };
            if let Some(header) = response.header() {
                metrics.observe(
                    WatchLane::Config,
                    header.revision(),
                    response
                        .events()
                        .iter()
                        .filter_map(|event| event.kv().map(|value| value.mod_revision())),
                );
            }
            if response.canceled() {
                metrics.record_recovery(WatchLane::Config);
                healthy.store(false, Ordering::Release);
                break;
            }
            for event in response.events() {
                let Some(kv) = event.kv() else {
                    continue;
                };
                if event.event_type() == EventType::Delete {
                    healthy.store(false, Ordering::Release);
                    continue;
                }
                let Ok(config) = decode_config(kv.value()) else {
                    healthy.store(false, Ordering::Release);
                    continue;
                };
                let next = ConfigSnapshot {
                    config,
                    revision: kv.mod_revision(),
                };
                let Ok(mut current) = snapshot.write() else {
                    healthy.store(false, Ordering::Release);
                    return;
                };
                if next.revision >= current.revision {
                    *current = next;
                    healthy.store(true, Ordering::Release);
                }
            }
        }
        healthy.store(false, Ordering::Release);
        if stop_or_sleep(stop, std::time::Duration::from_millis(100)).await {
            return;
        }
    }
}

async fn stop_or_sleep(stop: &mut oneshot::Receiver<()>, duration: std::time::Duration) -> bool {
    tokio::select! {
        _ = &mut *stop => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

pub(crate) fn config_key(cluster_id: &str) -> Result<String, ConfigError> {
    Ok(format!(
        "/rcaster/{}/config/caster",
        validate_cluster_id(cluster_id)?
    ))
}

fn config_revision_key(cluster_id: &str, generation: u64) -> Result<String, ConfigError> {
    Ok(format!(
        "/rcaster/{}/config/revisions/{generation}",
        validate_cluster_id(cluster_id)?
    ))
}

fn validate_cluster_id(cluster_id: &str) -> Result<&str, ConfigError> {
    if cluster_id.is_empty()
        || !cluster_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ConfigError::InvalidIdentifier("cluster ID"));
    }
    Ok(cluster_id)
}

fn decode_config(value: &[u8]) -> Result<CasterConfig, ConfigError> {
    let config: CasterConfig = serde_json::from_slice(value)?;
    if config.schema_version != 1 {
        return Err(ConfigError::UnsupportedSchema(config.schema_version));
    }
    if !config.source_health.is_valid()
        || config.limits.max_source_bitrate_bps == 0
        || config.limits.max_sources == 0
        || config.limits.max_validating_sources == 0
        || config.limits.max_subscribers == 0
        || config.limits.max_subscribers_per_source == 0
    {
        return Err(ConfigError::InvalidConfig);
    }
    Ok(config)
}

fn default_max_validating_sources() -> usize {
    64
}

fn default_true() -> bool {
    true
}

fn default_base_position_jump_m() -> f64 {
    100.0
}

fn default_health_short_window_s() -> u64 {
    10
}

fn default_health_long_window_s() -> u64 {
    60
}

fn default_health_valid_frame_ratio_min() -> f64 {
    0.8
}

fn default_health_alert_clear_s() -> u64 {
    30
}

fn default_health_action() -> String {
    "alert".to_owned()
}

#[cfg(test)]
mod tests {
    use super::{config_key, decode_config};

    #[test]
    fn rejects_cluster_ids_that_escape_the_config_key() {
        assert!(config_key("cluster-east").is_ok());
        assert!(config_key("cluster/extra").is_err());
        assert!(config_key("").is_err());
    }

    #[test]
    fn rejects_invalid_config_json() {
        assert!(decode_config(b"{not-json}").is_err());
    }

    #[test]
    fn validates_source_health_thresholds() {
        let mut config = super::CasterConfig::new(1);
        assert!(decode_config(&serde_json::to_vec(&config).unwrap()).is_ok());
        config.source_health.short_window_s = 61;
        config.source_health.long_window_s = 60;
        assert!(decode_config(&serde_json::to_vec(&config).unwrap()).is_err());
        config.source_health.short_window_s = 10;
        config.source_health.observation_timeout_s = 0;
        assert!(decode_config(&serde_json::to_vec(&config).unwrap()).is_err());
        config.source_health.observation_timeout_s = 10;
        config.source_health.valid_frame_ratio_min = -0.1;
        assert!(decode_config(&serde_json::to_vec(&config).unwrap()).is_err());
        config.source_health.valid_frame_ratio_min = 0.8;
        config.source_health.crc_error_ratio_max = 1.1;
        assert!(decode_config(&serde_json::to_vec(&config).unwrap()).is_err());
        config.source_health.crc_error_ratio_max = 0.1;
        config.source_health.alert_clear_s = 0;
        assert!(decode_config(&serde_json::to_vec(&config).unwrap()).is_err());
        config.source_health.alert_clear_s = 30;
        config.source_health.action = "disconnect".to_owned();
        assert!(decode_config(&serde_json::to_vec(&config).unwrap()).is_err());
        config.source_health.action = "alert".to_owned();
        config.limits.max_source_bitrate_bps = 0;
        assert!(decode_config(&serde_json::to_vec(&config).unwrap()).is_err());
    }
}
