use std::{
    fmt, fs, io,
    path::{Path, PathBuf},
    time::Duration,
};

use rcaster_service::{
    hub::HealthThresholds,
    proxy_protocol::ProxyProtocolConfig,
    session_limit::SessionLimitConfig,
    tls_reload::{
        OcspResponderConfig,
        validate_private_key_permissions as validate_tls_private_key_permissions,
    },
};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct StandaloneConfig {
    pub bind: String,
    #[serde(default)]
    pub health_bind: Option<String>,
    #[serde(default)]
    pub health_metrics_token: Option<String>,
    #[serde(default)]
    pub admin_bind: Option<String>,
    #[serde(default)]
    pub admin_client_crl: Option<PathBuf>,
    #[serde(default)]
    pub relay_client_crl: Option<PathBuf>,
    #[serde(default)]
    pub public_bind: Option<String>,
    #[serde(default)]
    pub public_map_tiles: Option<PublicMapTilesConfig>,
    pub account_db: PathBuf,
    #[serde(default)]
    pub audit_log: Option<PathBuf>,
    #[serde(default)]
    pub maintenance_state: Option<PathBuf>,
    #[serde(default = "default_audit_max_bytes")]
    pub audit_max_bytes: u64,
    #[serde(default = "default_audit_retention_days")]
    pub audit_retention_days: u64,
    #[serde(default = "default_config_revision_retention")]
    pub config_revision_retention: usize,
    #[serde(default)]
    pub history_db: Option<PathBuf>,
    #[serde(default)]
    pub standalone_backup_dir: Option<PathBuf>,
    #[serde(default = "default_standalone_backup_interval_s")]
    pub standalone_backup_interval_s: u64,
    #[serde(default = "default_standalone_backup_retention")]
    pub standalone_backup_retention: usize,
    #[serde(default)]
    pub tls_bind: Option<String>,
    #[serde(default)]
    pub tls_certificate: Option<PathBuf>,
    #[serde(default)]
    pub tls_private_key: Option<PathBuf>,
    #[serde(default)]
    pub tls_ocsp_response: Option<PathBuf>,
    #[serde(default)]
    pub tls_ocsp_responder: Option<OcspResponderConfigFile>,
    #[serde(default = "default_tls_handshake_timeout_ms")]
    pub tls_handshake_timeout_ms: u64,
    #[serde(default = "default_head_timeout_ms")]
    pub head_timeout_ms: u64,
    #[serde(default = "default_content_length_max_bytes")]
    pub content_length_max_bytes: u64,
    #[serde(default = "default_subscriber_buffer_events")]
    pub subscriber_buffer_events: usize,
    #[serde(default)]
    pub client_auth_required: bool,
    #[serde(default = "default_true")]
    pub client_auth_require_tls: bool,
    #[serde(default = "default_true")]
    pub source_auth_require_tls: bool,
    #[serde(default = "default_source_stale_after_ms")]
    pub source_stale_after_ms: u64,
    #[serde(default = "default_source_max_crc_ratio")]
    pub source_max_crc_ratio: f64,
    #[serde(default = "default_source_max_bitrate_bps")]
    pub source_max_bitrate_bps: u64,
    #[serde(default)]
    pub source_health: SourceHealthPolicyFile,
    #[serde(default = "default_max_sources")]
    pub max_sources: usize,
    #[serde(default = "default_max_validating_sources")]
    pub max_validating_sources: usize,
    #[serde(default = "default_validation_buffer_max_bytes")]
    pub validation_buffer_max_bytes: u64,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    #[serde(default = "default_auth_max_concurrency")]
    pub auth_max_concurrency: usize,
    #[serde(default = "default_accept_rate_per_second")]
    pub accept_rate_per_second: u32,
    #[serde(default = "default_max_subscribers")]
    pub max_subscribers: usize,
    #[serde(default = "default_max_subscribers_per_source")]
    pub max_subscribers_per_source: usize,
    #[serde(default)]
    pub session_limits: SessionLimitConfig,
    #[serde(default)]
    pub cluster: Option<ClusterConfig>,
    #[serde(default)]
    pub source_admission: Option<SourceAdmissionConfigFile>,
    #[serde(default)]
    pub near: Option<NearConfigFile>,
    #[serde(default)]
    pub upgrade: Option<UpgradeConfigFile>,
    #[serde(default)]
    pub proxy_protocol: Option<ProxyProtocolConfigFile>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct UpgradeConfigFile {
    pub trusted_public_keys: Vec<String>,
    pub artifact_hosts: Vec<String>,
    #[serde(default = "default_true")]
    pub require_approval: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct PublicMapTilesConfig {
    pub url_template: String,
    pub attribution: String,
    #[serde(default)]
    pub min_zoom: u8,
    #[serde(default = "default_map_max_zoom")]
    pub max_zoom: u8,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct ProxyProtocolConfigFile {
    pub trusted_l4_cidrs: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct SourceHealthPolicyFile {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub short_window_s: Option<u64>,
    #[serde(default)]
    pub long_window_s: Option<u64>,
    #[serde(default)]
    pub observation_timeout_s: Option<u64>,
    #[serde(default)]
    pub valid_frame_ratio_min: Option<f64>,
    #[serde(default)]
    pub crc_error_ratio_max: Option<f64>,
    #[serde(default)]
    pub alert_clear_s: Option<u64>,
    #[serde(default)]
    pub station_id_change_alert: Option<bool>,
    #[serde(default)]
    pub base_position_jump_m: Option<f64>,
    #[serde(default)]
    pub action: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct NearConfigFile {
    pub enabled: bool,
    pub mountpoint: String,
    #[serde(default)]
    pub advertise: bool,
    #[serde(default = "default_near_max_distance_m")]
    pub max_distance_m: f64,
    #[serde(default = "default_near_hysteresis_m")]
    pub hysteresis_m: f64,
    #[serde(default = "default_near_base_position_uncertainty_m")]
    pub base_position_uncertainty_m: f64,
    #[serde(default = "default_near_hdop_m_per_unit")]
    pub hdop_m_per_unit: f64,
    #[serde(default = "default_near_max_hdop")]
    pub max_hdop: f32,
    #[serde(default = "default_near_switch_observe_ms")]
    pub switch_observe_ms: u64,
    #[serde(default = "default_near_switch_position_threshold_m")]
    pub switch_position_threshold_m: f64,
    #[serde(default = "default_near_min_recompute_interval_ms")]
    pub min_recompute_interval_ms: u64,
    #[serde(default = "default_near_max_recompute_interval_ms")]
    pub max_recompute_interval_ms: u64,
    #[serde(default = "default_near_min_recompute_distance_m")]
    pub min_recompute_distance_m: f64,
    #[serde(default = "default_near_max_spatial_qps")]
    pub max_spatial_queries_per_second: u32,
    #[serde(default = "default_near_max_query_concurrency")]
    pub max_query_concurrency: usize,
    #[serde(default = "default_near_gga_max_input_per_second")]
    pub gga_max_input_per_second: u32,
    #[serde(default = "default_near_gga_max_jump_speed_mps")]
    pub gga_max_jump_speed_mps: f64,
    #[serde(default)]
    pub position_ttl_s: u64,
    #[serde(default = "default_near_position_ttl_action")]
    pub position_ttl_action: String,
    #[serde(default = "default_near_no_source_timeout_ms")]
    pub no_source_timeout_ms: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct SourceAdmissionConfigFile {
    pub mode: String,
    pub allowed_formats: Vec<String>,
    pub require_correction_data: bool,
    #[serde(default)]
    pub msm: SourceMsmConfigFile,
    pub min_valid_frames: u64,
    #[serde(default = "default_min_correction_frames")]
    pub min_correction_frames: u64,
    #[serde(default)]
    pub max_crc_error_ratio: Option<f64>,
    #[serde(default)]
    pub require_base_position: bool,
    #[serde(default)]
    pub max_unknown_ratio: Option<f64>,
    #[serde(default)]
    pub min_observed_bytes: u64,
    #[serde(default)]
    pub str_refresh: SourceStrRefreshConfigFile,
    pub max_bytes: u64,
    pub max_frames: u64,
    pub timeout_ms: u64,
    #[serde(default = "default_source_read_timeout_ms")]
    pub read_timeout_ms: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct SourceStrRefreshConfigFile {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Default for SourceStrRefreshConfigFile {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
pub struct SourceMsmConfigFile {
    #[serde(default)]
    pub restrict_versions: bool,
    #[serde(default)]
    pub allowed_versions: Vec<u8>,
    #[serde(default)]
    pub require_any: bool,
    #[serde(default = "default_msm_required_within_s")]
    pub required_within_s: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct ClusterConfig {
    pub etcd_endpoints: Vec<String>,
    pub cluster_id: String,
    pub caster_id: String,
    pub runtime_epoch: String,
    pub relay_endpoint: String,
    #[serde(default)]
    pub relay_bind: Option<String>,
    #[serde(default = "default_relay_max_relays")]
    pub relay_max_relays: usize,
    #[serde(default = "default_relay_max_starting")]
    pub relay_max_starting: usize,
    #[serde(default = "default_relay_max_inbound")]
    pub relay_max_inbound: usize,
    #[serde(default = "default_relay_idle_timeout_ms")]
    pub relay_idle_timeout_ms: u64,
    #[serde(default = "default_cluster_lease_ttl_s")]
    pub lease_ttl_s: i64,
    #[serde(default)]
    pub coordinator_endpoint: Option<String>,
    #[serde(default)]
    pub coordinator_tls: Option<CoordinatorTlsConfig>,
    #[serde(default)]
    pub etcd_tls: Option<EtcdTlsConfig>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct CoordinatorTlsConfig {
    pub ca_certificate: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    #[serde(default)]
    pub server_ocsp_response: Option<PathBuf>,
    #[serde(default)]
    pub server_ocsp_responder: Option<OcspResponderConfigFile>,
    pub server_name: String,
    pub service_token: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct OcspResponderConfigFile {
    pub url: String,
    #[serde(default = "default_ocsp_refresh_interval_s")]
    pub refresh_interval_s: u64,
}

impl OcspResponderConfigFile {
    fn is_valid(&self) -> bool {
        self.url.len() <= 2048
            && (1..=86_400).contains(&self.refresh_interval_s)
            && (self.url.starts_with("http://") || self.url.starts_with("https://"))
            && !self.url.bytes().any(|byte| byte.is_ascii_whitespace())
    }

    pub fn to_runtime_config(&self) -> OcspResponderConfig {
        OcspResponderConfig {
            url: self.url.clone(),
            refresh_interval: Duration::from_secs(self.refresh_interval_s),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct EtcdTlsConfig {
    pub ca_certificate: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub domain_name: String,
}

#[derive(Debug)]
pub enum ConfigError {
    Io(io::Error),
    Json(serde_json::Error),
    InvalidBind,
    IncompleteTls,
    InvalidLimits,
    InvalidCluster,
    InvalidSourceAdmission,
    InvalidUpgrade,
    InvalidPublicMap,
    InvalidProxyProtocol,
    InvalidOcspResponder,
    InsecurePrivateKey,
    InvalidHealth,
}

impl StandaloneConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let mut config: Self = serde_json::from_slice(&fs::read(path)?)?;
        if config.account_db.is_relative() {
            config.account_db = resolve_relative(path, &config.account_db);
        }
        if let Some(audit_log) = &config.audit_log
            && audit_log.is_relative()
        {
            config.audit_log = Some(resolve_relative(path, audit_log));
        }
        if let Some(maintenance_state) = &config.maintenance_state
            && maintenance_state.is_relative()
        {
            config.maintenance_state = Some(resolve_relative(path, maintenance_state));
        }
        if let Some(history_db) = &config.history_db
            && history_db.is_relative()
        {
            config.history_db = Some(resolve_relative(path, history_db));
        }
        if let Some(backup_dir) = &config.standalone_backup_dir
            && backup_dir.is_relative()
        {
            config.standalone_backup_dir = Some(resolve_relative(path, backup_dir));
        }
        if let Some(certificate) = &config.tls_certificate
            && certificate.is_relative()
        {
            config.tls_certificate = Some(resolve_relative(path, certificate));
        }
        if let Some(private_key) = &config.tls_private_key
            && private_key.is_relative()
        {
            config.tls_private_key = Some(resolve_relative(path, private_key));
        }
        if let Some(ocsp_response) = &config.tls_ocsp_response
            && ocsp_response.is_relative()
        {
            config.tls_ocsp_response = Some(resolve_relative(path, ocsp_response));
        }
        if let Some(client_crl) = &config.admin_client_crl
            && client_crl.is_relative()
        {
            config.admin_client_crl = Some(resolve_relative(path, client_crl));
        }
        if let Some(client_crl) = &config.relay_client_crl
            && client_crl.is_relative()
        {
            config.relay_client_crl = Some(resolve_relative(path, client_crl));
        }
        if let Some(cluster) = &mut config.cluster
            && let Some(tls) = &mut cluster.coordinator_tls
        {
            if tls.ca_certificate.is_relative() {
                tls.ca_certificate = resolve_relative(path, &tls.ca_certificate);
            }
            if tls.certificate.is_relative() {
                tls.certificate = resolve_relative(path, &tls.certificate);
            }
            if tls.private_key.is_relative() {
                tls.private_key = resolve_relative(path, &tls.private_key);
            }
            if let Some(ocsp_response) = &tls.server_ocsp_response
                && ocsp_response.is_relative()
            {
                tls.server_ocsp_response = Some(resolve_relative(path, ocsp_response));
            }
        }
        if let Some(cluster) = &mut config.cluster
            && let Some(tls) = &mut cluster.etcd_tls
        {
            if tls.ca_certificate.is_relative() {
                tls.ca_certificate = resolve_relative(path, &tls.ca_certificate);
            }
            if tls.certificate.is_relative() {
                tls.certificate = resolve_relative(path, &tls.certificate);
            }
            if tls.private_key.is_relative() {
                tls.private_key = resolve_relative(path, &tls.private_key);
            }
        }
        if let Some(private_key) = &config.tls_private_key {
            validate_private_key_permissions(private_key)?;
        }
        if let Some(cluster) = &config.cluster {
            if let Some(tls) = &cluster.coordinator_tls {
                validate_private_key_permissions(&tls.private_key)?;
            }
            if let Some(tls) = &cluster.etcd_tls {
                validate_private_key_permissions(&tls.private_key)?;
            }
        }
        config.validate()?;
        Ok(config)
    }

    pub fn health_thresholds(&self) -> Result<HealthThresholds, ConfigError> {
        let source_health = &self.source_health;
        let legacy_stale_s = self.source_stale_after_ms.div_ceil(1000).max(1);
        let short_window_s = source_health.short_window_s.unwrap_or(10);
        let long_window_s = source_health.long_window_s.unwrap_or(60);
        let observation_timeout_s = source_health
            .observation_timeout_s
            .unwrap_or(legacy_stale_s);
        let valid_frame_ratio_min = source_health.valid_frame_ratio_min.unwrap_or(0.8);
        let crc_error_ratio_max = source_health
            .crc_error_ratio_max
            .unwrap_or(self.source_max_crc_ratio);
        let alert_clear_s = source_health.alert_clear_s.unwrap_or(30);
        let base_position_jump_m = source_health.base_position_jump_m.unwrap_or(100.0);
        let action = source_health.action.as_deref().unwrap_or("alert");
        if short_window_s == 0
            || long_window_s == 0
            || short_window_s > long_window_s
            || observation_timeout_s == 0
            || !(0.0..=1.0).contains(&valid_frame_ratio_min)
            || !(0.0..=1.0).contains(&crc_error_ratio_max)
            || alert_clear_s == 0
            || !base_position_jump_m.is_finite()
            || base_position_jump_m <= 0.0
            || action != "alert"
        {
            return Err(ConfigError::InvalidLimits);
        }
        Ok(HealthThresholds {
            enabled: source_health.enabled.unwrap_or(true),
            short_window: Duration::from_secs(short_window_s),
            long_window: Duration::from_secs(long_window_s),
            observation_timeout: Duration::from_secs(observation_timeout_s),
            valid_frame_ratio_min,
            max_crc_ratio: crc_error_ratio_max,
            alert_clear: Duration::from_secs(alert_clear_s),
            max_source_bitrate_bps: self.source_max_bitrate_bps,
            station_id_change_alert: source_health.station_id_change_alert.unwrap_or(true),
            base_position_jump_m,
        })
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.bind.trim().is_empty() {
            return Err(ConfigError::InvalidBind);
        }
        if self
            .health_bind
            .as_ref()
            .is_some_and(|bind| bind.trim().is_empty())
        {
            return Err(ConfigError::InvalidBind);
        }
        if self
            .health_metrics_token
            .as_ref()
            .is_some_and(|token| token.is_empty() || token.len() > 256)
            || (self
                .health_bind
                .as_deref()
                .is_some_and(health_bind_requires_auth)
                && self.health_metrics_token.is_none())
        {
            return Err(ConfigError::InvalidHealth);
        }
        if self
            .admin_bind
            .as_ref()
            .is_some_and(|bind| bind.trim().is_empty())
        {
            return Err(ConfigError::InvalidBind);
        }
        if self
            .public_bind
            .as_ref()
            .is_some_and(|bind| bind.trim().is_empty())
        {
            return Err(ConfigError::InvalidBind);
        }
        if self.admin_bind.is_some()
            && match self.cluster.as_ref() {
                Some(cluster) => cluster.coordinator_tls.is_none(),
                None => self.tls_certificate.is_none() || self.tls_private_key.is_none(),
            }
        {
            return Err(ConfigError::IncompleteTls);
        }
        if self.tls_certificate.is_some() != self.tls_private_key.is_some() {
            return Err(ConfigError::IncompleteTls);
        }
        if self.tls_ocsp_response.is_some() && self.tls_certificate.is_none() {
            return Err(ConfigError::IncompleteTls);
        }
        if self
            .tls_ocsp_responder
            .as_ref()
            .is_some_and(|responder| !responder.is_valid() || self.tls_certificate.is_none())
        {
            return Err(ConfigError::InvalidOcspResponder);
        }
        if self
            .cluster
            .as_ref()
            .and_then(|cluster| cluster.coordinator_tls.as_ref())
            .and_then(|tls| tls.server_ocsp_responder.as_ref())
            .is_some_and(|responder| !responder.is_valid())
        {
            return Err(ConfigError::InvalidOcspResponder);
        }
        if self.tls_bind.is_some()
            && (self.tls_certificate.is_none() || self.tls_private_key.is_none())
        {
            return Err(ConfigError::IncompleteTls);
        }
        if self.public_bind.is_some() && self.tls_certificate.is_none() {
            return Err(ConfigError::IncompleteTls);
        }
        if self
            .tls_bind
            .as_ref()
            .is_some_and(|bind| bind.trim().is_empty())
        {
            return Err(ConfigError::InvalidBind);
        }
        if (self.source_auth_require_tls
            || self.client_auth_required && self.client_auth_require_tls)
            && self.tls_bind.is_none()
        {
            return Err(ConfigError::IncompleteTls);
        }
        if self.head_timeout_ms == 0
            || self.content_length_max_bytes == 0
            || self.subscriber_buffer_events == 0
            || self.tls_handshake_timeout_ms == 0
            || self.source_stale_after_ms == 0
            || !(0.0..=1.0).contains(&self.source_max_crc_ratio)
            || self.source_max_bitrate_bps == 0
            || self.health_thresholds().is_err()
            || self.max_sources == 0
            || self.max_validating_sources == 0
            || self.validation_buffer_max_bytes == 0
            || self.max_connections == 0
            || self.auth_max_concurrency == 0
            || self.accept_rate_per_second == 0
            || self.max_subscribers == 0
            || self.max_subscribers_per_source == 0
            || self.audit_max_bytes == 0
            || !(1..=3650).contains(&self.audit_retention_days)
            || !(1..=500).contains(&self.config_revision_retention)
            || self.standalone_backup_interval_s == 0
            || !(1..=10_000).contains(&self.standalone_backup_retention)
        {
            return Err(ConfigError::InvalidLimits);
        }
        if let Some(cluster) = &self.cluster
            && (cluster.etcd_endpoints.is_empty()
                || cluster
                    .etcd_endpoints
                    .iter()
                    .any(|endpoint| endpoint.trim().is_empty())
                || cluster.cluster_id.is_empty()
                || cluster.caster_id.is_empty()
                || cluster.runtime_epoch.is_empty()
                || cluster.relay_endpoint.is_empty()
                || cluster
                    .coordinator_endpoint
                    .as_ref()
                    .is_some_and(|endpoint| endpoint.trim().is_empty())
                || cluster
                    .relay_bind
                    .as_ref()
                    .is_some_and(|bind| bind.trim().is_empty())
                || (cluster.relay_bind.is_some() && cluster.coordinator_tls.is_none())
                || cluster.relay_max_relays == 0
                || cluster.relay_max_starting == 0
                || cluster.relay_max_inbound == 0
                || cluster.relay_idle_timeout_ms == 0
                || cluster
                    .coordinator_tls
                    .as_ref()
                    .is_some_and(|tls| tls.server_name.is_empty() || tls.service_token.is_empty())
                || cluster
                    .etcd_tls
                    .as_ref()
                    .is_some_and(|tls| tls.domain_name.is_empty())
                || cluster.lease_ttl_s <= 0)
        {
            return Err(ConfigError::InvalidCluster);
        }
        if let Some(admission) = &self.source_admission
            && (!matches!(admission.mode.as_str(), "observe" | "enforce")
                || admission.allowed_formats.is_empty()
                || admission.allowed_formats.iter().any(|format| {
                    !matches!(
                        format.as_str(),
                        "rtcm3" | "rtcm2" | "binex" | "cmr" | "nmea"
                    )
                })
                || admission.min_valid_frames == 0
                || admission.min_correction_frames == 0
                || admission
                    .max_crc_error_ratio
                    .is_some_and(|ratio| !(0.0..=1.0).contains(&ratio))
                || admission
                    .max_unknown_ratio
                    .is_some_and(|ratio| !(0.0..=1.0).contains(&ratio))
                || admission.msm.require_any && admission.msm.required_within_s == 0
                || admission
                    .msm
                    .allowed_versions
                    .iter()
                    .any(|version| !(1..=7).contains(version))
                || (admission.msm.restrict_versions && admission.msm.allowed_versions.is_empty())
                || admission.max_bytes == 0
                || admission.max_frames == 0
                || admission.timeout_ms == 0
                || admission.read_timeout_ms == 0)
        {
            return Err(ConfigError::InvalidSourceAdmission);
        }
        if let Some(admission) = &self.source_admission
            && admission
                .max_bytes
                .saturating_mul(self.max_validating_sources as u64)
                > self.validation_buffer_max_bytes
        {
            return Err(ConfigError::InvalidSourceAdmission);
        }
        if let Some(near) = &self.near
            && (near.mountpoint.is_empty()
                || !near
                    .mountpoint
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
                || near.no_source_timeout_ms == 0
                || !near.max_distance_m.is_finite()
                || near.max_distance_m <= 0.0
                || !near.hysteresis_m.is_finite()
                || near.hysteresis_m < 0.0
                || !near.base_position_uncertainty_m.is_finite()
                || near.base_position_uncertainty_m < 0.0
                || !near.hdop_m_per_unit.is_finite()
                || near.hdop_m_per_unit < 0.0
                || !near.max_hdop.is_finite()
                || near.max_hdop <= 0.0
                || near.switch_observe_ms == 0
                || !near.switch_position_threshold_m.is_finite()
                || near.switch_position_threshold_m < 0.0
                || near.min_recompute_interval_ms == 0
                || near.max_recompute_interval_ms < near.min_recompute_interval_ms
                || !near.min_recompute_distance_m.is_finite()
                || near.min_recompute_distance_m < 0.0
                || near.max_spatial_queries_per_second == 0
                || near.max_query_concurrency == 0
                || near.gga_max_input_per_second == 0
                || !near.gga_max_jump_speed_mps.is_finite()
                || near.gga_max_jump_speed_mps <= 0.0
                || near.position_ttl_action != "exclude_candidate")
        {
            return Err(ConfigError::InvalidSourceAdmission);
        }
        if let Some(upgrade) = &self.upgrade
            && (self.cluster.is_none()
                || upgrade.trusted_public_keys.is_empty()
                || upgrade.trusted_public_keys.iter().any(|key| {
                    key.len() != 64 || !key.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
                || upgrade.artifact_hosts.is_empty()
                || upgrade
                    .artifact_hosts
                    .iter()
                    .any(|host| host.trim().is_empty()))
        {
            return Err(ConfigError::InvalidUpgrade);
        }
        if let Some(tiles) = &self.public_map_tiles
            && !valid_public_map_tiles(tiles)
        {
            return Err(ConfigError::InvalidPublicMap);
        }
        if let Some(proxy) = &self.proxy_protocol
            && ProxyProtocolConfig::new(&proxy.trusted_l4_cidrs).is_err()
        {
            return Err(ConfigError::InvalidProxyProtocol);
        }
        Ok(())
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "config read failed: {error}"),
            Self::Json(error) => write!(formatter, "config JSON error: {error}"),
            Self::InvalidBind => formatter.write_str("bind address must not be empty"),
            Self::IncompleteTls => {
                formatter.write_str("TLS bind, certificate, and private key are all required")
            }
            Self::InvalidLimits => formatter.write_str("configured limits must be positive"),
            Self::InvalidCluster => formatter.write_str("invalid cluster configuration"),
            Self::InvalidSourceAdmission => {
                formatter.write_str("invalid source admission configuration")
            }
            Self::InvalidUpgrade => formatter.write_str("invalid upgrade configuration"),
            Self::InvalidPublicMap => formatter.write_str("invalid public map tile configuration"),
            Self::InvalidProxyProtocol => {
                formatter.write_str("invalid PROXY protocol configuration")
            }
            Self::InvalidOcspResponder => formatter.write_str(
                "OCSP responder URL must be http(s) and refresh_interval_s must be 1..=86400",
            ),
            Self::InsecurePrivateKey => {
                formatter.write_str("private key file permissions allow group or other access")
            }
            Self::InvalidHealth => {
                formatter.write_str("public health bind requires a non-empty metrics token")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<io::Error> for ConfigError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for ConfigError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct UpdaterConfig {
    pub etcd_endpoints: Vec<String>,
    #[serde(default)]
    pub etcd_tls: Option<EtcdTlsConfig>,
    pub cluster_id: String,
    pub caster_id: String,
    pub work_dir: PathBuf,
    pub service_program: PathBuf,
    #[serde(default)]
    pub service_args: Vec<String>,
    pub health_url: String,
    pub trusted_public_keys: Vec<String>,
    pub artifact_hosts: Vec<String>,
    #[serde(default = "default_updater_health_timeout_s")]
    pub health_timeout_s: u64,
    #[serde(default = "default_updater_drain_timeout_s")]
    pub drain_timeout_s: u64,
}

impl UpdaterConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let mut config: Self = serde_json::from_slice(&fs::read(path)?)?;
        if config.work_dir.is_relative() {
            config.work_dir = resolve_relative(path, &config.work_dir);
        }
        if config.service_program.is_relative() {
            config.service_program = resolve_relative(path, &config.service_program);
        }
        if let Some(tls) = &mut config.etcd_tls {
            if tls.ca_certificate.is_relative() {
                tls.ca_certificate = resolve_relative(path, &tls.ca_certificate);
            }
            if tls.certificate.is_relative() {
                tls.certificate = resolve_relative(path, &tls.certificate);
            }
            if tls.private_key.is_relative() {
                tls.private_key = resolve_relative(path, &tls.private_key);
            }
        }
        if let Some(tls) = &config.etcd_tls {
            validate_private_key_permissions(&tls.private_key)?;
        }
        if config.etcd_endpoints.is_empty()
            || config
                .etcd_endpoints
                .iter()
                .any(|endpoint| endpoint.trim().is_empty())
            || config.cluster_id.is_empty()
            || config.caster_id.is_empty()
            || config.work_dir.as_os_str().is_empty()
            || config.service_program.as_os_str().is_empty()
            || !config.health_url.starts_with("http://")
            || config.trusted_public_keys.is_empty()
            || config
                .trusted_public_keys
                .iter()
                .any(|key| key.len() != 64 || !key.bytes().all(|byte| byte.is_ascii_hexdigit()))
            || config.artifact_hosts.is_empty()
            || config
                .artifact_hosts
                .iter()
                .any(|host| host.trim().is_empty())
            || config.health_timeout_s == 0
            || config.health_timeout_s > 600
            || config.drain_timeout_s == 0
            || config.drain_timeout_s > 300
        {
            return Err(ConfigError::InvalidUpgrade);
        }
        Ok(config)
    }
}

fn default_updater_health_timeout_s() -> u64 {
    60
}

fn default_true() -> bool {
    true
}

fn default_map_max_zoom() -> u8 {
    19
}

fn default_audit_max_bytes() -> u64 {
    16 * 1024 * 1024
}

fn default_audit_retention_days() -> u64 {
    90
}

fn default_config_revision_retention() -> usize {
    200
}

fn default_standalone_backup_interval_s() -> u64 {
    3600
}

fn default_standalone_backup_retention() -> usize {
    168
}

fn valid_public_map_tiles(tiles: &PublicMapTilesConfig) -> bool {
    tiles.url_template.len() <= 2048
        && tiles.url_template.starts_with("https://")
        && !tiles
            .url_template
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || matches!(byte, b'"' | b'\'' | b'\\'))
        && ["{z}", "{x}", "{y}"]
            .iter()
            .all(|placeholder| tiles.url_template.contains(placeholder))
        && !tiles.attribution.trim().is_empty()
        && tiles.attribution.len() <= 512
        && tiles.min_zoom <= tiles.max_zoom
        && tiles.max_zoom <= 22
}

fn default_updater_drain_timeout_s() -> u64 {
    30
}

fn default_head_timeout_ms() -> u64 {
    10_000
}

fn default_source_stale_after_ms() -> u64 {
    10_000
}

fn default_source_max_crc_ratio() -> f64 {
    0.1
}

fn default_source_max_bitrate_bps() -> u64 {
    2_000_000
}

fn default_max_sources() -> usize {
    500
}

fn default_max_validating_sources() -> usize {
    64
}

fn default_validation_buffer_max_bytes() -> u64 {
    64 * 1024 * 1024
}

fn default_max_connections() -> usize {
    3000
}

fn default_auth_max_concurrency() -> usize {
    4
}

fn default_accept_rate_per_second() -> u32 {
    1000
}

fn default_max_subscribers() -> usize {
    2500
}

fn default_max_subscribers_per_source() -> usize {
    2000
}

fn default_content_length_max_bytes() -> u64 {
    1024 * 1024
}

fn default_subscriber_buffer_events() -> usize {
    64
}

fn default_tls_handshake_timeout_ms() -> u64 {
    5000
}

fn default_ocsp_refresh_interval_s() -> u64 {
    3600
}

fn default_cluster_lease_ttl_s() -> i64 {
    30
}

fn default_source_read_timeout_ms() -> u64 {
    30_000
}

fn default_min_correction_frames() -> u64 {
    1
}

fn default_msm_required_within_s() -> u64 {
    30
}

fn default_relay_max_relays() -> usize {
    100
}

fn default_relay_max_starting() -> usize {
    16
}

fn default_relay_max_inbound() -> usize {
    500
}

fn default_relay_idle_timeout_ms() -> u64 {
    30_000
}

fn default_near_no_source_timeout_ms() -> u64 {
    10_000
}

fn default_near_max_distance_m() -> f64 {
    100_000.0
}

fn default_near_hysteresis_m() -> f64 {
    5.0
}

fn default_near_base_position_uncertainty_m() -> f64 {
    1.0
}

fn default_near_hdop_m_per_unit() -> f64 {
    1.5
}

fn default_near_max_hdop() -> f32 {
    2.5
}

fn default_near_switch_observe_ms() -> u64 {
    3_000
}

fn default_near_switch_position_threshold_m() -> f64 {
    4.0
}

fn default_near_min_recompute_interval_ms() -> u64 {
    1_000
}

fn default_near_max_recompute_interval_ms() -> u64 {
    5_000
}

fn default_near_min_recompute_distance_m() -> f64 {
    4.0
}

fn default_near_max_spatial_qps() -> u32 {
    10_000
}

fn default_near_max_query_concurrency() -> usize {
    8
}

fn default_near_gga_max_input_per_second() -> u32 {
    10
}

fn default_near_gga_max_jump_speed_mps() -> f64 {
    1_000.0
}

fn default_near_position_ttl_action() -> String {
    "exclude_candidate".to_owned()
}

fn resolve_relative(config_path: &Path, path: &Path) -> PathBuf {
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(path)
}

pub fn validate_private_key_permissions(path: &Path) -> Result<(), ConfigError> {
    match validate_tls_private_key_permissions(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            Err(ConfigError::InsecurePrivateKey)
        }
        Err(error) => Err(ConfigError::Io(error)),
    }
}

fn health_bind_requires_auth(bind: &str) -> bool {
    let host = bind.rsplit_once(':').map_or(bind, |(host, _)| host);
    matches!(host, "0.0.0.0" | "[::]" | "::")
}

#[cfg(test)]
mod tests {
    use super::{PublicMapTilesConfig, valid_public_map_tiles};

    #[test]
    fn validates_public_tile_templates() {
        let valid = PublicMapTilesConfig {
            url_template: "https://tiles.example.test/{z}/{x}/{y}.png".to_owned(),
            attribution: "Example".to_owned(),
            min_zoom: 1,
            max_zoom: 18,
        };
        assert!(valid_public_map_tiles(&valid));
        let mut invalid = valid.clone();
        invalid.url_template = "http://tiles.example.test/{z}/{x}/{y}.png".to_owned();
        assert!(!valid_public_map_tiles(&invalid));
        invalid = valid.clone();
        invalid.url_template = "https://tiles.example.test/{z}/{x}.png".to_owned();
        assert!(!valid_public_map_tiles(&invalid));
        invalid = valid;
        invalid.attribution.clear();
        assert!(!valid_public_map_tiles(&invalid));
    }

    #[test]
    fn wildcard_health_bind_requires_authentication() {
        assert!(super::health_bind_requires_auth("0.0.0.0:9100"));
        assert!(super::health_bind_requires_auth("[::]:9100"));
        assert!(!super::health_bind_requires_auth("127.0.0.1:9100"));
        assert!(!super::health_bind_requires_auth("localhost:9100"));
    }
}
