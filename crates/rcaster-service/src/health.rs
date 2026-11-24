use std::{
    future::Future,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};

use rcaster_core::protocol::AdmissionViolation;

use crate::hub::HubMetrics;

#[derive(Default)]
pub struct HealthState {
    ready: AtomicBool,
    maintenance: AtomicBool,
    active_sources: AtomicUsize,
    relays: AtomicUsize,
    config_generation: AtomicU64,
    source_accepted: AtomicU64,
    source_rejected: AtomicU64,
    source_rejected_no_valid_frames: AtomicU64,
    source_rejected_format: AtomicU64,
    source_rejected_expected_format: AtomicU64,
    source_rejected_missing_correction: AtomicU64,
    source_rejected_no_observation: AtomicU64,
    source_rejected_mixed_formats: AtomicU64,
    source_rejected_crc_ratio: AtomicU64,
    source_rejected_base_position: AtomicU64,
    source_rejected_msm_missing: AtomicU64,
    source_rejected_msm_version: AtomicU64,
    source_rejected_unknown_ratio: AtomicU64,
    auth_failure_fallback: AtomicU64,
    source_packets: AtomicU64,
    source_bytes: AtomicU64,
    source_valid_frames: AtomicU64,
    source_crc_errors: AtomicU64,
    sources_rtcm3: AtomicUsize,
    sources_rtcm2: AtomicUsize,
    sources_binex: AtomicUsize,
    sources_cmr: AtomicUsize,
    sources_nmea: AtomicUsize,
    sources_unknown: AtomicUsize,
    source_health_warning_sources: AtomicUsize,
    no_data_warning_sources: AtomicUsize,
    no_correction_warning_sources: AtomicUsize,
    high_crc_warning_sources: AtomicUsize,
    low_valid_frame_ratio_sources: AtomicUsize,
    station_id_warning_sources: AtomicUsize,
    base_position_jump_sources: AtomicUsize,
    max_source_packet_age_seconds: AtomicU64,
    station_id_changes: AtomicU64,
    base_position_jumps: AtomicU64,
    backlog_kicks: AtomicU64,
    near_queries: AtomicU64,
    near_queries_limited: AtomicU64,
    near_gga_rate_limited: AtomicU64,
    near_gga_jump_rejected: AtomicU64,
    near_gga_invalid: AtomicU64,
    near_switches: AtomicU64,
    process_rss_bytes: AtomicU64,
    process_open_fds: AtomicU64,
    process_usage_available: AtomicBool,
    subscribers: AtomicUsize,
    active_connections: AtomicUsize,
    tls_certificate_expiry: AtomicI64,
    sessions: AtomicUsize,
    lease_healthy: AtomicBool,
    config_healthy: AtomicBool,
    blacklist_healthy: AtomicBool,
    source_watch_healthy: AtomicBool,
    account_watch_healthy: AtomicBool,
}

impl HealthState {
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire) && !self.maintenance.load(Ordering::Acquire)
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    pub fn set_maintenance(&self, maintenance: bool) {
        self.maintenance.store(maintenance, Ordering::Release);
    }

    pub fn is_maintenance(&self) -> bool {
        self.maintenance.load(Ordering::Acquire)
    }

    pub fn set_counts(&self, active_sources: usize, relays: usize) {
        self.active_sources.store(active_sources, Ordering::Release);
        self.relays.store(relays, Ordering::Release);
    }

    pub fn set_hub_metrics(&self, metrics: HubMetrics) {
        self.active_sources
            .store(metrics.active_sources, Ordering::Release);
        self.subscribers
            .store(metrics.subscribers, Ordering::Release);
        self.source_packets
            .store(metrics.packets, Ordering::Release);
        self.source_bytes.store(metrics.bytes, Ordering::Release);
        self.source_valid_frames
            .store(metrics.valid_frames, Ordering::Release);
        self.source_crc_errors
            .store(metrics.crc_errors, Ordering::Release);
        self.sources_rtcm3
            .store(metrics.rtcm3_sources, Ordering::Release);
        self.sources_rtcm2
            .store(metrics.rtcm2_sources, Ordering::Release);
        self.sources_binex
            .store(metrics.binex_sources, Ordering::Release);
        self.sources_cmr
            .store(metrics.cmr_sources, Ordering::Release);
        self.sources_nmea
            .store(metrics.nmea_sources, Ordering::Release);
        self.sources_unknown
            .store(metrics.unknown_sources, Ordering::Release);
        self.source_health_warning_sources
            .store(metrics.source_health_warning_sources, Ordering::Release);
        self.no_data_warning_sources
            .store(metrics.no_data_warning_sources, Ordering::Release);
        self.no_correction_warning_sources
            .store(metrics.no_correction_warning_sources, Ordering::Release);
        self.high_crc_warning_sources
            .store(metrics.high_crc_warning_sources, Ordering::Release);
        self.low_valid_frame_ratio_sources
            .store(metrics.low_valid_frame_ratio_sources, Ordering::Release);
        self.station_id_warning_sources
            .store(metrics.station_id_warning_sources, Ordering::Release);
        self.base_position_jump_sources
            .store(metrics.base_position_jump_sources, Ordering::Release);
        self.max_source_packet_age_seconds
            .store(metrics.max_source_packet_age_seconds, Ordering::Release);
        self.station_id_changes
            .store(metrics.station_id_changes, Ordering::Release);
        self.base_position_jumps
            .store(metrics.base_position_jumps, Ordering::Release);
        self.backlog_kicks
            .store(metrics.backlog_kicks, Ordering::Release);
    }

    pub fn set_sessions(&self, sessions: usize) {
        self.sessions.store(sessions, Ordering::Release);
    }

    pub fn set_connections(&self, connections: usize) {
        self.active_connections
            .store(connections, Ordering::Release);
    }

    pub fn set_tls_certificate_expiry(&self, expires_at: Option<i64>) {
        self.tls_certificate_expiry
            .store(expires_at.unwrap_or_default(), Ordering::Release);
    }

    pub fn set_config_generation(&self, generation: u64) {
        self.config_generation.store(generation, Ordering::Release);
    }

    pub fn set_dependency_health(&self, health: [bool; 5]) {
        self.lease_healthy.store(health[0], Ordering::Release);
        self.config_healthy.store(health[1], Ordering::Release);
        self.blacklist_healthy.store(health[2], Ordering::Release);
        self.source_watch_healthy
            .store(health[3], Ordering::Release);
        self.account_watch_healthy
            .store(health[4], Ordering::Release);
    }

    pub fn source_accepted(&self) {
        self.source_accepted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn near_query(&self) {
        self.near_queries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn near_query_limited(&self) {
        self.near_queries_limited.fetch_add(1, Ordering::Relaxed);
    }

    pub fn near_gga_rate_limited(&self) {
        self.near_gga_rate_limited.fetch_add(1, Ordering::Relaxed);
    }

    pub fn near_gga_jump_rejected(&self) {
        self.near_gga_jump_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn near_gga_invalid(&self) {
        self.near_gga_invalid.fetch_add(1, Ordering::Relaxed);
    }

    pub fn near_switched(&self) {
        self.near_switches.fetch_add(1, Ordering::Relaxed);
    }

    pub fn source_rejected(&self) {
        self.source_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn source_rejected_with_reasons(&self, violations: &[AdmissionViolation]) {
        self.source_rejected();
        if violations.contains(&AdmissionViolation::NoValidFrames) {
            self.source_rejected_no_valid_frames
                .fetch_add(1, Ordering::Relaxed);
        }
        if violations
            .iter()
            .any(|violation| matches!(violation, AdmissionViolation::FormatNotAllowed(_)))
        {
            self.source_rejected_format.fetch_add(1, Ordering::Relaxed);
        }
        if violations
            .iter()
            .any(|violation| matches!(violation, AdmissionViolation::ExpectedFormatMismatch(_)))
        {
            self.source_rejected_expected_format
                .fetch_add(1, Ordering::Relaxed);
        }
        if violations.contains(&AdmissionViolation::MissingCorrectionData) {
            self.source_rejected_missing_correction
                .fetch_add(1, Ordering::Relaxed);
        }
        if violations.contains(&AdmissionViolation::NoObservationOrSsr) {
            self.source_rejected_no_observation
                .fetch_add(1, Ordering::Relaxed);
        }
        if violations.contains(&AdmissionViolation::MixedCorrectionFormats) {
            self.source_rejected_mixed_formats
                .fetch_add(1, Ordering::Relaxed);
        }
        if violations.contains(&AdmissionViolation::CrcThresholdExceeded) {
            self.source_rejected_crc_ratio
                .fetch_add(1, Ordering::Relaxed);
        }
        if violations.contains(&AdmissionViolation::BasePositionMissing) {
            self.source_rejected_base_position
                .fetch_add(1, Ordering::Relaxed);
        }
        if violations.contains(&AdmissionViolation::MsmRequiredMissing) {
            self.source_rejected_msm_missing
                .fetch_add(1, Ordering::Relaxed);
        }
        if violations
            .iter()
            .any(|violation| matches!(violation, AdmissionViolation::MsmVersionNotAllowed(_)))
        {
            self.source_rejected_msm_version
                .fetch_add(1, Ordering::Relaxed);
        }
        if violations.contains(&AdmissionViolation::UnknownDataRatioExceeded) {
            self.source_rejected_unknown_ratio
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn set_auth_failure_fallback(&self, total: u64) {
        self.auth_failure_fallback.store(total, Ordering::Release);
    }

    pub fn set_process_usage(&self, usage: Option<(u64, u64)>) {
        let available = usage.is_some();
        let (rss, fds) = usage.unwrap_or_default();
        self.process_rss_bytes.store(rss, Ordering::Release);
        self.process_open_fds.store(fds, Ordering::Release);
        self.process_usage_available
            .store(available, Ordering::Release);
    }

    fn metrics(&self) -> String {
        let ready = usize::from(self.is_ready());
        let expiry = self.tls_certificate_expiry.load(Ordering::Acquire);
        let expiry_seconds = (expiry != 0).then(|| {
            expiry.saturating_sub(
                SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64,
            )
        });
        let mut metrics = format!(
            "# HELP rcaster_up Process liveness.\n# TYPE rcaster_up gauge\nrcaster_up 1\n# HELP rcaster_ready Node readiness.\n# TYPE rcaster_ready gauge\nrcaster_ready {ready}\n# HELP rcaster_cluster_dependency_healthy Cluster control-plane dependency health.\n# TYPE rcaster_cluster_dependency_healthy gauge\nrcaster_cluster_dependency_healthy{{kind=\"lease\"}} {}\nrcaster_cluster_dependency_healthy{{kind=\"config\"}} {}\nrcaster_cluster_dependency_healthy{{kind=\"blacklist\"}} {}\nrcaster_cluster_dependency_healthy{{kind=\"sources\"}} {}\nrcaster_cluster_dependency_healthy{{kind=\"accounts\"}} {}\n# HELP rcaster_active_sources Local ACTIVE sources.\n# TYPE rcaster_active_sources gauge\nrcaster_active_sources {}\n# HELP rcaster_subscribers Local subscribers.\n# TYPE rcaster_subscribers gauge\nrcaster_subscribers {}\n# HELP rcaster_connections Active listener connections.\n# TYPE rcaster_connections gauge\nrcaster_connections {}\n# HELP rcaster_tls_certificate_expires_seconds Seconds until listener TLS certificate expiry.\n# TYPE rcaster_tls_certificate_expires_seconds gauge\nrcaster_tls_certificate_expires_seconds {}\n# HELP rcaster_sessions Cluster sessions visible to this Caster.\n# TYPE rcaster_sessions gauge\nrcaster_sessions {}\n# HELP rcaster_relays Active shared upstream relays.\n# TYPE rcaster_relays gauge\nrcaster_relays {}\n# HELP rcaster_config_generation Active config generation.\n# TYPE rcaster_config_generation gauge\nrcaster_config_generation {}\n# HELP rcaster_source_packets_total Source packets forwarded.\n# TYPE rcaster_source_packets_total counter\nrcaster_source_packets_total {}\n# HELP rcaster_source_bytes_total Source bytes forwarded.\n# TYPE rcaster_source_bytes_total counter\nrcaster_source_bytes_total {}\n# HELP rcaster_source_valid_frames_total Valid source frames observed.\n# TYPE rcaster_source_valid_frames_total counter\nrcaster_source_valid_frames_total {}\n# HELP rcaster_source_crc_errors_total Source CRC errors observed.\n# TYPE rcaster_source_crc_errors_total counter\nrcaster_source_crc_errors_total {}\n# HELP rcaster_source_admissions_total Source admission decisions.\n# TYPE rcaster_source_admissions_total counter\nrcaster_source_admissions_total{{result=\"accepted\"}} {}\nrcaster_source_admissions_total{{result=\"rejected\"}} {}\n# HELP rcaster_source_rejections_total Source admission rejection reasons.\n# TYPE rcaster_source_rejections_total counter\nrcaster_source_rejections_total{{reason=\"no_valid_frames\"}} {}\nrcaster_source_rejections_total{{reason=\"format_not_allowed\"}} {}\nrcaster_source_rejections_total{{reason=\"missing_correction_data\"}} {}\nrcaster_source_rejections_total{{reason=\"unknown_data_ratio\"}} {}\n",
            usize::from(self.lease_healthy.load(Ordering::Acquire)),
            usize::from(self.config_healthy.load(Ordering::Acquire)),
            usize::from(self.blacklist_healthy.load(Ordering::Acquire)),
            usize::from(self.source_watch_healthy.load(Ordering::Acquire)),
            usize::from(self.account_watch_healthy.load(Ordering::Acquire)),
            self.active_sources.load(Ordering::Acquire),
            self.subscribers.load(Ordering::Acquire),
            self.active_connections.load(Ordering::Acquire),
            expiry_seconds.map_or_else(|| "NaN".to_owned(), |value| value.to_string()),
            self.sessions.load(Ordering::Acquire),
            self.relays.load(Ordering::Acquire),
            self.config_generation.load(Ordering::Acquire),
            self.source_packets.load(Ordering::Acquire),
            self.source_bytes.load(Ordering::Acquire),
            self.source_valid_frames.load(Ordering::Acquire),
            self.source_crc_errors.load(Ordering::Acquire),
            self.source_accepted.load(Ordering::Relaxed),
            self.source_rejected.load(Ordering::Relaxed),
            self.source_rejected_no_valid_frames.load(Ordering::Relaxed),
            self.source_rejected_format.load(Ordering::Relaxed),
            self.source_rejected_missing_correction
                .load(Ordering::Relaxed),
            self.source_rejected_unknown_ratio.load(Ordering::Relaxed),
        );
        metrics.push_str(&format!(
            "rcaster_source_rejections_total{{reason=\"msm_version_not_allowed\"}} {}\n",
            self.source_rejected_msm_version.load(Ordering::Relaxed)
        ));
        metrics.push_str(&format!(
            "rcaster_source_rejections_total{{reason=\"expected_format_mismatch\"}} {}\n",
            self.source_rejected_expected_format.load(Ordering::Relaxed)
        ));
        metrics.push_str(&format!(
            "rcaster_source_rejections_total{{reason=\"no_observation_or_ssr\"}} {}\nrcaster_source_rejections_total{{reason=\"mixed_correction_formats\"}} {}\nrcaster_source_rejections_total{{reason=\"crc_threshold_exceeded\"}} {}\nrcaster_source_rejections_total{{reason=\"base_position_missing\"}} {}\nrcaster_source_rejections_total{{reason=\"msm_required_missing\"}} {}\n",
            self.source_rejected_no_observation.load(Ordering::Relaxed),
            self.source_rejected_mixed_formats.load(Ordering::Relaxed),
            self.source_rejected_crc_ratio.load(Ordering::Relaxed),
            self.source_rejected_base_position.load(Ordering::Relaxed),
            self.source_rejected_msm_missing.load(Ordering::Relaxed),
        ));
        metrics.push_str(&format!(
            "# HELP rcaster_sources_by_format Active local sources by primary format.\n# TYPE rcaster_sources_by_format gauge\nrcaster_sources_by_format{{format=\"rtcm3\"}} {}\nrcaster_sources_by_format{{format=\"rtcm2\"}} {}\nrcaster_sources_by_format{{format=\"binex\"}} {}\nrcaster_sources_by_format{{format=\"cmr\"}} {}\nrcaster_sources_by_format{{format=\"nmea\"}} {}\nrcaster_sources_by_format{{format=\"unknown\"}} {}\n",
            self.sources_rtcm3.load(Ordering::Acquire),
            self.sources_rtcm2.load(Ordering::Acquire),
            self.sources_binex.load(Ordering::Acquire),
            self.sources_cmr.load(Ordering::Acquire),
            self.sources_nmea.load(Ordering::Acquire),
            self.sources_unknown.load(Ordering::Acquire),
        ));
        metrics.push_str(&format!(
            "# HELP rcaster_source_health_alerting_sources Active local sources with at least one latched health warning.\n# TYPE rcaster_source_health_alerting_sources gauge\nrcaster_source_health_alerting_sources {}\n# HELP rcaster_source_health_warnings Active local sources by latched health warning.\n# TYPE rcaster_source_health_warnings gauge\nrcaster_source_health_warnings{{reason=\"no_data\"}} {}\nrcaster_source_health_warnings{{reason=\"no_correction\"}} {}\nrcaster_source_health_warnings{{reason=\"high_crc\"}} {}\nrcaster_source_health_warnings{{reason=\"low_valid_frame_ratio\"}} {}\nrcaster_source_health_warnings{{reason=\"station_id_changed\"}} {}\nrcaster_source_health_warnings{{reason=\"base_position_jump\"}} {}\n# HELP rcaster_source_max_packet_age_seconds Oldest live-source packet age.\n# TYPE rcaster_source_max_packet_age_seconds gauge\nrcaster_source_max_packet_age_seconds {}\n# HELP rcaster_source_station_id_changes_total Source station ID changes.\n# TYPE rcaster_source_station_id_changes_total counter\nrcaster_source_station_id_changes_total {}\n# HELP rcaster_source_position_jumps_total Source base-position jumps.\n# TYPE rcaster_source_position_jumps_total counter\nrcaster_source_position_jumps_total {}\n# HELP rcaster_backlog_kicks_total Subscribers disconnected for exceeding their broadcast backlog.\n# TYPE rcaster_backlog_kicks_total counter\nrcaster_backlog_kicks_total {}\n",
            self.source_health_warning_sources.load(Ordering::Acquire),
            self.no_data_warning_sources.load(Ordering::Acquire),
            self.no_correction_warning_sources.load(Ordering::Acquire),
            self.high_crc_warning_sources.load(Ordering::Acquire),
            self.low_valid_frame_ratio_sources.load(Ordering::Acquire),
            self.station_id_warning_sources.load(Ordering::Acquire),
            self.base_position_jump_sources.load(Ordering::Acquire),
            self.max_source_packet_age_seconds.load(Ordering::Acquire),
            self.station_id_changes.load(Ordering::Relaxed),
            self.base_position_jumps.load(Ordering::Relaxed),
            self.backlog_kicks.load(Ordering::Relaxed),
        ));
        metrics.push_str(&format!(
            "# HELP rcaster_auth_failure_fallback_total Shared authentication limiter failures that fell back to node-local protection.\n# TYPE rcaster_auth_failure_fallback_total counter\nrcaster_auth_failure_fallback_total {}\n",
            self.auth_failure_fallback.load(Ordering::Relaxed)
        ));
        metrics.push_str(&format!(
            "# HELP rcaster_near_events_total NEAR selection and input limiter events.\n# TYPE rcaster_near_events_total counter\nrcaster_near_events_total{{event=\"query\"}} {}\nrcaster_near_events_total{{event=\"query_limited\"}} {}\nrcaster_near_events_total{{event=\"gga_rate_limited\"}} {}\nrcaster_near_events_total{{event=\"gga_jump_rejected\"}} {}\nrcaster_near_events_total{{event=\"gga_invalid\"}} {}\nrcaster_near_events_total{{event=\"switch\"}} {}\n",
            self.near_queries.load(Ordering::Relaxed),
            self.near_queries_limited.load(Ordering::Relaxed),
            self.near_gga_rate_limited.load(Ordering::Relaxed),
            self.near_gga_jump_rejected.load(Ordering::Relaxed),
            self.near_gga_invalid.load(Ordering::Relaxed),
            self.near_switches.load(Ordering::Relaxed),
        ));
        let process_available = self.process_usage_available.load(Ordering::Acquire);
        metrics.push_str(&format!(
            "# HELP rcaster_process_resident_memory_bytes Resident memory size.\n# TYPE rcaster_process_resident_memory_bytes gauge\nrcaster_process_resident_memory_bytes {}\n# HELP rcaster_process_open_fds Open file descriptors.\n# TYPE rcaster_process_open_fds gauge\nrcaster_process_open_fds {}\n",
            if process_available {
                self.process_rss_bytes.load(Ordering::Relaxed).to_string()
            } else {
                "NaN".to_owned()
            },
            if process_available {
                self.process_open_fds.load(Ordering::Relaxed).to_string()
            } else {
                "NaN".to_owned()
            }
        ));
        let (clock_offset, clock_synchronized) = clock_health().map_or_else(
            || ("NaN".to_owned(), "NaN".to_owned()),
            |clock| {
                (
                    format!("{:.9}", clock.offset_seconds),
                    usize::from(clock.synchronized).to_string(),
                )
            },
        );
        metrics.push_str(&format!(
            "# HELP rcaster_node_clock_offset_seconds Node clock offset from chrony or the kernel discipline.\n# TYPE rcaster_node_clock_offset_seconds gauge\nrcaster_node_clock_offset_seconds {clock_offset}\n# HELP rcaster_node_clock_synchronized Node clock discipline is synchronized.\n# TYPE rcaster_node_clock_synchronized gauge\nrcaster_node_clock_synchronized {clock_synchronized}\n"
        ));
        metrics
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ClockHealth {
    pub offset_seconds: f64,
    pub synchronized: bool,
}

pub fn clock_health() -> Option<ClockHealth> {
    #[cfg(target_os = "linux")]
    {
        if let Some(clock) = chronyc_clock_health() {
            return Some(clock);
        }
        adjtimex_clock_health()
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(target_os = "linux")]
fn chronyc_clock_health() -> Option<ClockHealth> {
    let command = b"timeout 2 chronyc -c tracking 2>/dev/null\0";
    // SAFETY: command is a fixed NUL-terminated string and the returned FILE is
    // closed with pclose on every path after a successful popen.
    let file = unsafe { libc::popen(command.as_ptr().cast(), c"r".as_ptr()) };
    if file.is_null() {
        return None;
    }
    let descriptor = unsafe { libc::fileno(file) };
    let mut output = Vec::new();
    let mut chunk = [0_u8; 256];
    loop {
        // SAFETY: descriptor belongs to file and chunk is valid for writes.
        let read = unsafe {
            libc::read(
                descriptor,
                chunk.as_mut_ptr().cast(),
                chunk.len() as libc::size_t,
            )
        };
        if read <= 0 {
            break;
        }
        output.extend_from_slice(&chunk[..read as usize]);
        if output.len() > 4096 {
            break;
        }
    }
    let status = unsafe { libc::pclose(file) };
    if status != 0 {
        return None;
    }
    parse_chronyc_tracking(&String::from_utf8_lossy(&output))
}

fn parse_chronyc_tracking(output: &str) -> Option<ClockHealth> {
    let fields = output.trim().split(',').collect::<Vec<_>>();
    if fields.len() < 14 {
        return None;
    }
    Some(ClockHealth {
        offset_seconds: fields[4].parse().ok()?,
        synchronized: fields[13].eq_ignore_ascii_case("Normal"),
    })
}

#[cfg(target_os = "linux")]
fn adjtimex_clock_health() -> Option<ClockHealth> {
    // SAFETY: adjtimex only reads the current clock state when modes is zero.
    let mut timex = unsafe { std::mem::zeroed::<libc::timex>() };
    let state = unsafe { libc::adjtimex(&mut timex) };
    if state < 0 {
        return None;
    }
    Some(ClockHealth {
        offset_seconds: timex.offset as f64 / 1_000_000.0,
        synchronized: state != libc::TIME_ERROR,
    })
}

pub fn process_usage() -> Option<(u64, u64)> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let rss_kib = status.lines().find_map(|line| {
            line.strip_prefix("VmRSS:")
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })?;
        let fds = std::fs::read_dir("/proc/self/fd")
            .ok()?
            .filter_map(Result::ok)
            .count() as u64;
        Some((rss_kib.saturating_mul(1024), fds))
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

pub async fn serve_health<F>(
    listener: TcpListener,
    state: Arc<HealthState>,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()>,
{
    serve_health_with_token(listener, state, None, shutdown).await
}

pub async fn serve_health_with_token<F>(
    listener: TcpListener,
    state: Arc<HealthState>,
    metrics_token: Option<String>,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()>,
{
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (mut stream, _) = accepted?;
                let state = Arc::clone(&state);
                let metrics_token = metrics_token.clone();
                tokio::spawn(async move {
                    let mut request = vec![0_u8; 8192];
                    let read = timeout(Duration::from_secs(5), stream.read(&mut request)).await;
                    let Ok(Ok(read)) = read else {
                        return;
                    };
                    let request = std::str::from_utf8(&request[..read]).unwrap_or_default();
                    let path = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1));
                    let (status, body) = match path {
                        Some("/health/live") => ("200 OK", "ok\n".to_owned()),
                        Some("/health/ready")
                            if state.is_ready() =>
                        {
                            ("200 OK", "ready\n".to_owned())
                        }
                        Some("/health/ready") => {
                            ("503 Service Unavailable", "not ready\n".to_owned())
                        }
                        Some("/metrics")
                            if metrics_token.as_ref().is_some_and(|expected| {
                                let supplied = request
                                    .lines()
                                    .find_map(|line| {
                                        let (name, value) = line.split_once(':')?;
                                        name.eq_ignore_ascii_case("authorization")
                                            .then(|| value.trim())
                                    })
                                    .and_then(|value| value.strip_prefix("Bearer "));
                                supplied.is_some_and(|supplied| {
                                    supplied.len() == expected.len()
                                        && bool::from(
                                            supplied.as_bytes().ct_eq(expected.as_bytes()),
                                        )
                                })
                            }) =>
                        {
                            ("200 OK", state.metrics())
                        }
                        Some("/metrics") if metrics_token.is_some() => {
                            ("401 Unauthorized", "unauthorized\n".to_owned())
                        }
                        Some("/metrics") => ("200 OK", state.metrics()),
                        _ => ("404 Not Found", "not found\n".to_owned()),
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::parse_chronyc_tracking;

    #[test]
    fn parses_chronyc_tracking_clock_offset() {
        let tracking = "A9FE0051,169.254.0.81,3,1795453587.943154236,0.000194947,-0.000168719,0.000200348,-15.533,-0.004,0.043,0.002016898,0.026644034,1038.5,Normal\n";
        assert_eq!(
            parse_chronyc_tracking(tracking),
            Some(super::ClockHealth {
                offset_seconds: 0.000194947,
                synchronized: true,
            })
        );
    }
}
