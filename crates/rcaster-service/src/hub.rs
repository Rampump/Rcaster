use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{
        Arc, Mutex, MutexGuard, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rcaster_core::ntrip::validate_mountpoint;
use rcaster_core::{
    protocol::{DataFormat, DataProfile, GeoPosition, ProtocolDemux},
    str_record::StrRecord,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::{self, Receiver};

pub struct MountpointHub {
    inner: Arc<HubInner>,
    buffer_events: usize,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HealthThresholds {
    pub stale_after: Duration,
    pub max_crc_ratio: f64,
    pub max_source_bitrate_bps: u64,
    pub station_id_change_alert: bool,
    pub base_position_jump_m: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HubLimits {
    pub max_sources: usize,
    pub max_validating_sources: usize,
    pub max_subscribers: usize,
    pub max_subscribers_per_source: usize,
}

impl Default for HubLimits {
    fn default() -> Self {
        Self {
            max_sources: 500,
            max_validating_sources: 64,
            max_subscribers: 2500,
            max_subscribers_per_source: 2000,
        }
    }
}

impl Default for HealthThresholds {
    fn default() -> Self {
        Self {
            stale_after: Duration::from_secs(10),
            max_crc_ratio: 0.1,
            max_source_bitrate_bps: 2_000_000,
            station_id_change_alert: true,
            base_position_jump_m: 100.0,
        }
    }
}

struct HubInner {
    sources: Mutex<HashMap<String, SourceState>>,
    health: RwLock<HealthThresholds>,
    limits: RwLock<HubLimits>,
    history_seed: Mutex<HashMap<String, Vec<StreamHistoryPoint>>>,
    source_revision: AtomicUsize,
}

struct SourceState {
    id: u64,
    sender: broadcast::Sender<Arc<[u8]>>,
    str_record: Option<StrRecord>,
    profile: Option<DataProfile>,
    packets: u64,
    bytes: u64,
    last_packet: Option<Instant>,
    last_correction: Option<Instant>,
    samples: VecDeque<(Instant, u64)>,
    station_frame: Option<Arc<[u8]>>,
    history: VecDeque<StreamHistoryPoint>,
    validating: bool,
    station_id_changed: bool,
    base_position_jump: bool,
}

pub struct SourceLease {
    inner: Arc<HubInner>,
    mountpoint: String,
    id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HubError {
    InvalidCapacity,
    InvalidMountpoint,
    SourceAlreadyActive,
    SourceNotFound,
    SourceClosed,
    SourceBitrateExceeded,
    SourceLimitReached,
    ValidatingSourceLimitReached,
    SubscriberLimitReached,
}

impl MountpointHub {
    pub fn new(buffer_events: usize) -> Result<Self, HubError> {
        Self::new_with_health(buffer_events, HealthThresholds::default())
    }

    pub fn new_with_health(
        buffer_events: usize,
        health: HealthThresholds,
    ) -> Result<Self, HubError> {
        if buffer_events == 0 {
            return Err(HubError::InvalidCapacity);
        }
        Ok(Self {
            inner: Arc::new(HubInner {
                sources: Mutex::new(HashMap::new()),
                health: RwLock::new(health),
                limits: RwLock::new(HubLimits::default()),
                history_seed: Mutex::new(HashMap::new()),
                source_revision: AtomicUsize::new(0),
            }),
            buffer_events,
        })
    }

    pub fn set_health_thresholds(&self, health: HealthThresholds) {
        *self
            .inner
            .health
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = health;
    }

    pub fn set_limits(&self, limits: HubLimits) {
        *self
            .inner
            .limits
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = limits;
    }

    pub fn restore_history(&self, mountpoint: &str, points: Vec<StreamHistoryPoint>) {
        self.inner
            .history_seed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(mountpoint.to_owned(), points);
    }

    pub fn claim_source(&self, mountpoint: &str) -> Result<SourceLease, HubError> {
        self.claim_source_inner(mountpoint, false)
    }

    pub fn claim_validating_source(&self, mountpoint: &str) -> Result<SourceLease, HubError> {
        self.claim_source_inner(mountpoint, true)
    }

    fn claim_source_inner(
        &self,
        mountpoint: &str,
        validating: bool,
    ) -> Result<SourceLease, HubError> {
        let mountpoint =
            validate_mountpoint(mountpoint).map_err(|_| HubError::InvalidMountpoint)?;
        let (sender, _receiver) = broadcast::channel(self.buffer_events);
        let mut sources = self.inner.sources();
        if sources.contains_key(&mountpoint) {
            return Err(HubError::SourceAlreadyActive);
        }
        let limits = self
            .inner
            .limits
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if sources.len() >= limits.max_sources {
            return Err(HubError::SourceLimitReached);
        }
        if validating
            && sources.values().filter(|source| source.validating).count()
                >= limits.max_validating_sources
        {
            return Err(HubError::ValidatingSourceLimitReached);
        }
        drop(limits);
        let history = self
            .inner
            .history_seed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&mountpoint)
            .unwrap_or_default()
            .into_iter()
            .collect();
        let id = sources
            .values()
            .map(|source| source.id)
            .max()
            .unwrap_or_default()
            .saturating_add(1);
        sources.insert(
            mountpoint.clone(),
            SourceState {
                id,
                sender: sender.clone(),
                str_record: None,
                profile: None,
                packets: 0,
                bytes: 0,
                last_packet: None,
                last_correction: None,
                samples: VecDeque::new(),
                station_frame: None,
                history,
                validating,
                station_id_changed: false,
                base_position_jump: false,
            },
        );
        self.inner.source_revision.fetch_add(1, Ordering::AcqRel);
        Ok(SourceLease {
            inner: Arc::clone(&self.inner),
            mountpoint,
            id,
        })
    }

    pub fn subscribe(&self, mountpoint: &str) -> Result<Receiver<Arc<[u8]>>, HubError> {
        let sources = self.inner.sources();
        let source = sources.get(mountpoint).ok_or(HubError::SourceNotFound)?;
        if source.str_record.is_none() {
            return Err(HubError::SourceNotFound);
        }
        let limits = *self
            .inner
            .limits
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if source.sender.receiver_count() >= limits.max_subscribers_per_source
            || sources
                .values()
                .filter(|source| source.str_record.is_some())
                .map(|source| source.sender.receiver_count())
                .sum::<usize>()
                >= limits.max_subscribers
        {
            return Err(HubError::SubscriberLimitReached);
        }
        Ok(source.sender.subscribe())
    }

    pub fn active_sources(&self) -> Vec<String> {
        let mut sources = self
            .inner
            .sources()
            .iter()
            .filter(|(_, source)| source.str_record.is_some())
            .map(|(mountpoint, _)| mountpoint.clone())
            .collect::<Vec<_>>();
        sources.sort();
        sources
    }

    pub fn source_revision(&self) -> usize {
        self.inner.source_revision.load(Ordering::Acquire)
    }

    pub fn sourcetable_records(&self) -> Vec<StrRecord> {
        let mut records = self
            .inner
            .sources()
            .values()
            .filter_map(|source| source.str_record.clone())
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.mountpoint.cmp(&right.mountpoint));
        records
    }

    pub fn subscriber_count(&self, mountpoint: &str) -> Option<usize> {
        self.inner
            .sources()
            .get(mountpoint)
            .filter(|source| source.str_record.is_some())
            .map(|source| source.sender.receiver_count())
    }

    pub fn source_profile(&self, mountpoint: &str) -> Option<DataProfile> {
        self.inner
            .sources()
            .get(mountpoint)
            .filter(|source| source.str_record.is_some())
            .and_then(|source| source.profile.clone())
    }

    pub fn source_health(&self, mountpoint: &str) -> Option<SourceHealth> {
        let mut sources = self.inner.sources();
        let source = sources
            .get_mut(mountpoint)
            .filter(|source| source.str_record.is_some())?;
        Some(compute_source_health(source, self.thresholds()))
    }

    pub fn record_health_snapshots(&self) {
        let thresholds = self.thresholds();
        let now = Instant::now();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        for source in self
            .inner
            .sources()
            .values_mut()
            .filter(|source| source.str_record.is_some())
        {
            let health = compute_source_health_at(source, thresholds, now);
            source.history.push_back(StreamHistoryPoint {
                timestamp,
                packets: health.packets,
                bytes: health.bytes,
                bitrate_bps: health.bitrate_bps,
                warning_codes: health.warning_codes,
            });
            while source.history.len() > 300 {
                source.history.pop_front();
            }
        }
    }

    pub fn source_history(&self, mountpoint: &str, limit: usize) -> Vec<StreamHistoryPoint> {
        self.inner
            .sources()
            .get(mountpoint)
            .filter(|source| source.str_record.is_some())
            .map(|source| {
                let skip = source.history.len().saturating_sub(limit.min(300));
                source.history.iter().skip(skip).copied().collect()
            })
            .unwrap_or_default()
    }

    pub fn metrics(&self) -> HubMetrics {
        let sources = self.inner.sources();
        let mut rtcm3_sources = 0;
        let mut rtcm2_sources = 0;
        let mut binex_sources = 0;
        let mut cmr_sources = 0;
        let mut nmea_sources = 0;
        let mut unknown_sources = 0;
        for source in sources
            .values()
            .filter(|source| source.str_record.is_some())
        {
            match source
                .profile
                .as_ref()
                .and_then(|profile| profile.primary_format)
            {
                Some(DataFormat::Rtcm3) => rtcm3_sources += 1,
                Some(DataFormat::Rtcm2) => rtcm2_sources += 1,
                Some(DataFormat::Binex) => binex_sources += 1,
                Some(DataFormat::Cmr) => cmr_sources += 1,
                Some(DataFormat::Nmea) => nmea_sources += 1,
                None => unknown_sources += 1,
            }
        }
        HubMetrics {
            active_sources: sources
                .values()
                .filter(|source| source.str_record.is_some())
                .count(),
            subscribers: sources
                .values()
                .filter(|source| source.str_record.is_some())
                .map(|source| source.sender.receiver_count())
                .sum(),
            packets: sources
                .values()
                .filter(|source| source.str_record.is_some())
                .map(|source| source.packets)
                .sum(),
            bytes: sources
                .values()
                .filter(|source| source.str_record.is_some())
                .map(|source| source.bytes)
                .sum(),
            valid_frames: sources
                .values()
                .filter_map(|source| source.profile.as_ref())
                .map(|profile| profile.valid_frames)
                .sum(),
            crc_errors: sources
                .values()
                .filter_map(|source| source.profile.as_ref())
                .map(|profile| profile.crc_errors)
                .sum(),
            rtcm3_sources,
            rtcm2_sources,
            binex_sources,
            cmr_sources,
            nmea_sources,
            unknown_sources,
        }
    }

    fn thresholds(&self) -> HealthThresholds {
        *self
            .inner
            .health
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn station_frames(&self, mountpoint: &str) -> Vec<Arc<[u8]>> {
        self.inner
            .sources()
            .get(mountpoint)
            .filter(|source| source.str_record.is_some())
            .and_then(|source| source.station_frame.clone())
            .into_iter()
            .collect::<Vec<_>>()
    }
}

impl SourceLease {
    pub fn mountpoint(&self) -> &str {
        &self.mountpoint
    }

    pub fn publish(&self, packet: Arc<[u8]>) -> Result<(), HubError> {
        self.publish_inner(packet, false, [])
    }

    pub fn publish_correction(&self, packet: Arc<[u8]>) -> Result<(), HubError> {
        self.publish_inner(packet, true, [])
    }

    pub fn publish_frame(
        &self,
        packet: Arc<[u8]>,
        format: DataFormat,
        message_number: Option<u16>,
    ) -> Result<(), HubError> {
        let station = message_number
            .filter(|number| matches!(number, 1005 | 1006))
            .map(|number| (number, Arc::clone(&packet)));
        self.publish_inner(packet, format != DataFormat::Nmea, station)
    }

    pub fn publish_analyzed(
        &self,
        packet: Arc<[u8]>,
        demux: &mut ProtocolDemux,
    ) -> Result<(), HubError> {
        let frames = demux.push(&packet);
        let correction = frames.iter().any(|frame| frame.format != DataFormat::Nmea);
        let stations = frames
            .iter()
            .filter_map(|frame| {
                frame
                    .message_number
                    .filter(|number| matches!(number, 1005 | 1006))
                    .map(|number| (number, Arc::from(frame.bytes.clone())))
            })
            .collect::<Vec<_>>();
        self.publish_inner(packet, correction, stations)?;
        self.set_profile(demux.profile().clone())
    }

    fn publish_inner(
        &self,
        packet: Arc<[u8]>,
        correction: bool,
        station: impl IntoIterator<Item = (u16, Arc<[u8]>)>,
    ) -> Result<(), HubError> {
        let sender = {
            let mut sources = self.inner.sources();
            let source = sources
                .get_mut(&self.mountpoint)
                .filter(|source| source.id == self.id)
                .ok_or(HubError::SourceClosed)?;
            if source.str_record.is_none() {
                return Err(HubError::SourceNotFound);
            }
            let now = Instant::now();
            let recent_bytes = source
                .samples
                .iter()
                .filter(|(at, _)| now.duration_since(*at) <= Duration::from_secs(1))
                .map(|(_, bytes)| *bytes)
                .sum::<u64>()
                .saturating_add(packet.len() as u64);
            let thresholds = *self
                .inner
                .health
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if recent_bytes.saturating_mul(8) > thresholds.max_source_bitrate_bps {
                return Err(HubError::SourceBitrateExceeded);
            }
            source.packets = source.packets.saturating_add(1);
            source.bytes = source.bytes.saturating_add(packet.len() as u64);
            source.last_packet = Some(now);
            if correction {
                source.last_correction = Some(now);
            }
            for (_, frame) in station {
                source.station_frame = Some(frame);
            }
            source.samples.push_back((now, packet.len() as u64));
            source.sender.clone()
        };
        let _ = sender.send(packet);
        Ok(())
    }

    pub fn set_str_record(&self, record: StrRecord) -> Result<(), HubError> {
        let mut sources = self.inner.sources();
        let source = sources
            .get_mut(&self.mountpoint)
            .filter(|source| source.id == self.id)
            .ok_or(HubError::SourceClosed)?;
        source.str_record = Some(record);
        source.validating = false;
        self.inner.source_revision.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub fn is_active(&self) -> bool {
        self.inner
            .sources()
            .get(&self.mountpoint)
            .is_some_and(|source| source.id == self.id && source.str_record.is_some())
    }

    pub fn subscriber_count(&self) -> usize {
        self.inner
            .sources()
            .get(&self.mountpoint)
            .filter(|source| source.id == self.id)
            .map(|source| source.sender.receiver_count())
            .unwrap_or_default()
    }

    pub fn close(&self) {
        let mut sources = self.inner.sources();
        if let Some(source) = sources.get(&self.mountpoint)
            && source.id == self.id
        {
            let history = source.history.iter().copied().collect();
            sources.remove(&self.mountpoint);
            self.inner.source_revision.fetch_add(1, Ordering::AcqRel);
            self.inner
                .history_seed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(self.mountpoint.clone(), history);
        }
    }

    pub fn set_profile(&self, profile: DataProfile) -> Result<(), HubError> {
        let mut sources = self.inner.sources();
        let source = sources
            .get_mut(&self.mountpoint)
            .filter(|source| source.id == self.id)
            .ok_or(HubError::SourceClosed)?;
        let thresholds = *self
            .inner
            .health
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(previous) = &source.profile {
            if thresholds.station_id_change_alert
                && previous.station_id.is_some()
                && profile.station_id.is_some()
                && previous.station_id != profile.station_id
            {
                source.station_id_changed = true;
            }
            if let (Some(previous), Some(current)) = (previous.position, profile.position)
                && distance_m(previous, current) > thresholds.base_position_jump_m
            {
                source.base_position_jump = true;
            }
        }
        source.profile = Some(profile);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceHealth {
    pub packets: u64,
    pub bytes: u64,
    pub last_packet_age_seconds: Option<u64>,
    pub bitrate_bps: Option<u64>,
    pub warning_codes: u8,
    pub last_correction_age_seconds: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct StreamHistoryPoint {
    pub timestamp: u64,
    pub packets: u64,
    pub bytes: u64,
    pub bitrate_bps: Option<u64>,
    pub warning_codes: u8,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HubMetrics {
    pub active_sources: usize,
    pub subscribers: usize,
    pub packets: u64,
    pub bytes: u64,
    pub valid_frames: u64,
    pub crc_errors: u64,
    pub rtcm3_sources: usize,
    pub rtcm2_sources: usize,
    pub binex_sources: usize,
    pub cmr_sources: usize,
    pub nmea_sources: usize,
    pub unknown_sources: usize,
}

pub const NO_DATA_WARNING: u8 = 1;
pub const HIGH_CRC_WARNING: u8 = 2;
pub const NO_CORRECTION_WARNING: u8 = 4;
pub const HIGH_BITRATE_WARNING: u8 = 8;
pub const STATION_ID_CHANGED_WARNING: u8 = 16;
pub const BASE_POSITION_JUMP_WARNING: u8 = 32;

fn compute_source_health(source: &mut SourceState, thresholds: HealthThresholds) -> SourceHealth {
    compute_source_health_at(source, thresholds, Instant::now())
}

fn compute_source_health_at(
    source: &mut SourceState,
    thresholds: HealthThresholds,
    now: Instant,
) -> SourceHealth {
    source
        .samples
        .retain(|(at, _)| now.duration_since(*at) <= Duration::from_secs(5));
    let window_bytes = source.samples.iter().map(|(_, bytes)| *bytes).sum::<u64>();
    let bitrate_bps = source.samples.front().and_then(|(first, _)| {
        let elapsed = now.duration_since(*first).as_secs_f64();
        (elapsed >= 1.0).then_some((window_bytes as f64 * 8.0 / elapsed) as u64)
    });
    let age_seconds = source
        .last_packet
        .map(|last| now.duration_since(last).as_secs());
    let integrity_attempts = source
        .profile
        .as_ref()
        .map(|profile| profile.valid_frames.saturating_add(profile.crc_errors))
        .unwrap_or_default();
    let crc_ratio = source
        .profile
        .as_ref()
        .filter(|_| integrity_attempts > 0)
        .map(|profile| profile.crc_errors as f64 / integrity_attempts as f64);
    let mut warning_codes = 0_u8;
    if age_seconds.is_none_or(|age| age > thresholds.stale_after.as_secs()) {
        warning_codes |= NO_DATA_WARNING;
    }
    if crc_ratio.is_some_and(|ratio| ratio > thresholds.max_crc_ratio) {
        warning_codes |= HIGH_CRC_WARNING;
    }
    if bitrate_bps.is_some_and(|bitrate| bitrate > thresholds.max_source_bitrate_bps) {
        warning_codes |= HIGH_BITRATE_WARNING;
    }
    if source
        .last_correction
        .is_none_or(|last| now.duration_since(last) > thresholds.stale_after)
    {
        warning_codes |= NO_CORRECTION_WARNING;
    }
    if source.station_id_changed {
        warning_codes |= STATION_ID_CHANGED_WARNING;
    }
    if source.base_position_jump {
        warning_codes |= BASE_POSITION_JUMP_WARNING;
    }
    SourceHealth {
        packets: source.packets,
        bytes: source.bytes,
        last_packet_age_seconds: age_seconds,
        bitrate_bps,
        warning_codes,
        last_correction_age_seconds: source
            .last_correction
            .map(|last| now.duration_since(last).as_secs()),
    }
}

fn distance_m(left: GeoPosition, right: GeoPosition) -> f64 {
    const EARTH_RADIUS_M: f64 = 6_371_000.0;
    let lat1 = f64::from(left.latitude_e7) / 1e7 * std::f64::consts::PI / 180.0;
    let lat2 = f64::from(right.latitude_e7) / 1e7 * std::f64::consts::PI / 180.0;
    let dlat = lat2 - lat1;
    let dlon = (f64::from(right.longitude_e7) - f64::from(left.longitude_e7)) / 1e7
        * std::f64::consts::PI
        / 180.0;
    let value = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * value.sqrt().asin()
}

impl Drop for SourceLease {
    fn drop(&mut self) {
        self.close();
    }
}

impl HubInner {
    fn sources(&self) -> MutexGuard<'_, HashMap<String, SourceState>> {
        self.sources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl fmt::Display for HubError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCapacity => formatter.write_str("subscriber buffer must be positive"),
            Self::InvalidMountpoint => formatter.write_str("invalid mountpoint"),
            Self::SourceAlreadyActive => {
                formatter.write_str("mountpoint already has an active source")
            }
            Self::SourceNotFound => formatter.write_str("mountpoint source is not active"),
            Self::SourceClosed => formatter.write_str("source lease is no longer active"),
            Self::SourceBitrateExceeded => formatter.write_str("source bitrate limit exceeded"),
            Self::SourceLimitReached => formatter.write_str("source limit reached"),
            Self::ValidatingSourceLimitReached => {
                formatter.write_str("validating source limit reached")
            }
            Self::SubscriberLimitReached => formatter.write_str("subscriber limit reached"),
        }
    }
}

impl std::error::Error for HubError {}
