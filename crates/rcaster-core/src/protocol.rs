use std::collections::{BTreeMap, BTreeSet};

use crate::account::SourceExpectation;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum DataFormat {
    Rtcm3,
    Rtcm2,
    Binex,
    Cmr,
    Nmea,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionMode {
    Observe,
    Enforce,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdmissionPolicy {
    pub mode: AdmissionMode,
    pub allowed_formats: Vec<DataFormat>,
    pub require_correction_data: bool,
    pub expected_formats: Option<Vec<DataFormat>>,
    pub allowed_msm_versions: Option<Vec<u8>>,
    pub min_valid_frames: u64,
    pub min_correction_frames: u64,
    pub max_crc_error_ratio: Option<f64>,
    pub require_base_position: bool,
    pub require_msm: bool,
    pub max_unknown_ratio: Option<f64>,
    pub min_observed_bytes: u64,
}

impl Default for AdmissionPolicy {
    fn default() -> Self {
        Self {
            mode: AdmissionMode::Enforce,
            allowed_formats: vec![
                DataFormat::Rtcm3,
                DataFormat::Rtcm2,
                DataFormat::Binex,
                DataFormat::Cmr,
                DataFormat::Nmea,
            ],
            require_correction_data: false,
            expected_formats: None,
            allowed_msm_versions: None,
            min_valid_frames: 1,
            min_correction_frames: 1,
            max_crc_error_ratio: Some(1.0),
            require_base_position: false,
            require_msm: false,
            max_unknown_ratio: None,
            min_observed_bytes: 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmissionViolation {
    NoValidFrames,
    FormatNotAllowed(DataFormat),
    ExpectedFormatMismatch(DataFormat),
    MissingCorrectionData,
    NoObservationOrSsr,
    MixedCorrectionFormats,
    CrcThresholdExceeded,
    BasePositionMissing,
    MsmRequiredMissing,
    MsmVersionNotAllowed(u8),
    UnknownDataRatioExceeded,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmissionProgress {
    Pending,
    Accepted { warnings: Vec<AdmissionViolation> },
    Rejected { violations: Vec<AdmissionViolation> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub format: DataFormat,
    pub bytes: Vec<u8>,
    pub message_number: Option<u16>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GeoPosition {
    pub latitude_e7: i32,
    pub longitude_e7: i32,
    pub height_cm: i64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NmeaGga {
    pub position: GeoPosition,
    pub fix_quality: u8,
    pub hdop: f32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeviceInfo {
    pub station_id: u16,
    pub antenna_descriptor: String,
    pub antenna_serial: String,
    pub receiver_type: String,
    pub receiver_firmware: String,
    pub receiver_serial: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NmeaSatellite {
    pub system: String,
    pub prn: u16,
    pub elevation_deg: u8,
    pub azimuth_deg: u16,
    pub snr_db: Option<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GnssSystem {
    Gps,
    Glonass,
    Galileo,
    Sbas,
    Qzss,
    NavIc,
    Bds,
}

impl GnssSystem {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gps => "GPS",
            Self::Glonass => "GLONASS",
            Self::Galileo => "Galileo",
            Self::Sbas => "SBAS",
            Self::Qzss => "QZSS",
            Self::NavIc => "NavIC",
            Self::Bds => "BeiDou",
        }
    }

    fn from_message_number(message_number: u16) -> Option<Self> {
        match message_number {
            1071..=1077 => Some(Self::Gps),
            1081..=1087 => Some(Self::Glonass),
            1091..=1097 => Some(Self::Galileo),
            1101..=1107 => Some(Self::Sbas),
            1111..=1117 => Some(Self::Qzss),
            1121..=1127 => Some(Self::Bds),
            1131..=1137 => Some(Self::NavIc),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GnssObservation {
    pub system: GnssSystem,
    pub message_number: u16,
    pub station_id: u16,
    pub epoch_time_ms: u32,
    pub satellite_id: u8,
    pub signal_id: u8,
    pub pseudorange_mm: Option<i64>,
    pub carrier_phase_mm: Option<i64>,
    pub doppler_mm_s: Option<i64>,
    pub cn0_q4: Option<u16>,
    pub lock_time_raw: Option<u16>,
    pub half_cycle_ambiguity: Option<bool>,
    pub pseudorange_ambiguity_raw: Option<u16>,
    pub glonass_frequency_channel: Option<i8>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DataProfile {
    pub primary_format: Option<DataFormat>,
    pub total_bytes: u64,
    pub unknown_bytes: u64,
    pub valid_frames: u64,
    pub crc_errors: u64,
    pub parser_framing_errors: u64,
    pub parser_resynchronizations: u64,
    pub parser_dropped_bytes: u64,
    pub parser_max_pending_bytes: u64,
    pub correction_frames: u64,
    pub correction_formats: BTreeSet<DataFormat>,
    pub position_message_seen: bool,
    pub msm_message_seen: bool,
    pub nmea_lines: u64,
    pub message_numbers: BTreeSet<u16>,
    pub msm_versions: BTreeSet<u8>,
    pub satellite_count: u16,
    pub signal_count: u16,
    pub satellite_ids: BTreeSet<u8>,
    pub signal_ids: BTreeSet<u8>,
    pub satellite_positions: Vec<NmeaSatellite>,
    pub satellite_cn0_q4: BTreeMap<u8, u16>,
    pub msm_observations: Vec<GnssObservation>,
    pub has_correction_data: bool,
    pub position: Option<GeoPosition>,
    pub device: Option<DeviceInfo>,
    pub station_id: Option<u16>,
}

pub struct ProtocolDemux {
    buffer: Vec<u8>,
    profile: DataProfile,
    rtcm2_tail: Option<u8>,
}

const MAX_PENDING_BYTES: usize = 8192;

impl ProtocolDemux {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            profile: DataProfile::default(),
            rtcm2_tail: None,
        }
    }

    pub fn profile(&self) -> &DataProfile {
        &self.profile
    }

    pub fn buffered_bytes(&self) -> usize {
        self.buffer.len()
    }

    pub fn push(&mut self, input: &[u8]) -> Vec<Frame> {
        self.profile.total_bytes = self.profile.total_bytes.saturating_add(input.len() as u64);
        self.buffer.extend_from_slice(input);
        let mut frames = Vec::new();
        while let Some(first) = self.buffer.first().copied() {
            match first {
                0xD3 => match parse_rtcm3(&self.buffer) {
                    Rtcm3Parse::Incomplete => break,
                    Rtcm3Parse::Invalid => {
                        self.profile.parser_framing_errors =
                            self.profile.parser_framing_errors.saturating_add(1);
                        self.profile.parser_resynchronizations =
                            self.profile.parser_resynchronizations.saturating_add(1);
                        self.profile.unknown_bytes = self.profile.unknown_bytes.saturating_add(1);
                        self.remove_first();
                    }
                    Rtcm3Parse::CrcError => {
                        self.profile.parser_framing_errors =
                            self.profile.parser_framing_errors.saturating_add(1);
                        self.profile.parser_resynchronizations =
                            self.profile.parser_resynchronizations.saturating_add(1);
                        self.profile.crc_errors = self.profile.crc_errors.saturating_add(1);
                        self.profile.unknown_bytes = self.profile.unknown_bytes.saturating_add(1);
                        self.remove_first();
                    }
                    Rtcm3Parse::Frame {
                        length,
                        message_number,
                    } => {
                        let bytes = self.drain_prefix(length);
                        self.record_rtcm3(message_number, &bytes[3..bytes.len() - 3]);
                        frames.push(Frame {
                            format: DataFormat::Rtcm3,
                            bytes,
                            message_number: Some(message_number),
                        });
                    }
                },
                b'$' => {
                    let boundary = self
                        .buffer
                        .iter()
                        .position(|byte| matches!(*byte, b'\n' | 0xD3 | 0x02 | 0xE2));
                    let Some(boundary) = boundary else {
                        if self.buffer.len() > 512 {
                            self.profile.parser_dropped_bytes = self
                                .profile
                                .parser_dropped_bytes
                                .saturating_add(self.buffer.len() as u64);
                            self.profile.unknown_bytes = self
                                .profile
                                .unknown_bytes
                                .saturating_add(self.buffer.len() as u64);
                            self.clear_buffer();
                        }
                        break;
                    };
                    if self.buffer[boundary] != b'\n' {
                        self.profile.parser_resynchronizations =
                            self.profile.parser_resynchronizations.saturating_add(1);
                        self.profile.unknown_bytes =
                            self.profile.unknown_bytes.saturating_add(boundary as u64);
                        self.drain_prefix(boundary);
                        continue;
                    }
                    let bytes = self.drain_prefix(boundary + 1);
                    if valid_nmea(&bytes) {
                        self.profile.valid_frames = self.profile.valid_frames.saturating_add(1);
                        self.profile.nmea_lines = self.profile.nmea_lines.saturating_add(1);
                        if self.profile.primary_format.is_none() {
                            self.profile.primary_format = Some(DataFormat::Nmea);
                        }
                        if let Some(gga) = parse_nmea_gga(&bytes) {
                            self.profile.position = Some(gga.position);
                        }
                        if let Some(satellites) = parse_nmea_gsv(&bytes) {
                            for satellite in satellites {
                                if let Some(existing) = self
                                    .profile
                                    .satellite_positions
                                    .iter_mut()
                                    .find(|existing| {
                                        existing.system == satellite.system
                                            && existing.prn == satellite.prn
                                    })
                                {
                                    *existing = satellite;
                                } else {
                                    self.profile.satellite_positions.push(satellite);
                                }
                            }
                        }
                        frames.push(Frame {
                            format: DataFormat::Nmea,
                            bytes,
                            message_number: None,
                        });
                    } else {
                        self.profile.parser_framing_errors =
                            self.profile.parser_framing_errors.saturating_add(1);
                        self.profile.unknown_bytes = self
                            .profile
                            .unknown_bytes
                            .saturating_add(bytes.len() as u64);
                    }
                }
                0x02 => match parse_cmr(&self.buffer) {
                    CmrParse::Incomplete => break,
                    CmrParse::Invalid => {
                        self.profile.parser_framing_errors =
                            self.profile.parser_framing_errors.saturating_add(1);
                        self.profile.parser_resynchronizations =
                            self.profile.parser_resynchronizations.saturating_add(1);
                        self.profile.unknown_bytes = self.profile.unknown_bytes.saturating_add(1);
                        self.remove_first();
                    }
                    CmrParse::ChecksumError => {
                        self.profile.parser_framing_errors =
                            self.profile.parser_framing_errors.saturating_add(1);
                        self.profile.parser_resynchronizations =
                            self.profile.parser_resynchronizations.saturating_add(1);
                        self.profile.crc_errors = self.profile.crc_errors.saturating_add(1);
                        self.profile.unknown_bytes = self.profile.unknown_bytes.saturating_add(1);
                        self.remove_first();
                    }
                    CmrParse::Frame {
                        length,
                        message_number,
                    } => {
                        let bytes = self.drain_prefix(length);
                        self.record_format_only(DataFormat::Cmr, Some(message_number));
                        frames.push(Frame {
                            format: DataFormat::Cmr,
                            bytes,
                            message_number: Some(message_number),
                        });
                    }
                },
                0xE2 => match parse_binex(&self.buffer) {
                    BinexParse::Incomplete => break,
                    BinexParse::Invalid => {
                        self.profile.parser_framing_errors =
                            self.profile.parser_framing_errors.saturating_add(1);
                        self.profile.parser_resynchronizations =
                            self.profile.parser_resynchronizations.saturating_add(1);
                        self.profile.unknown_bytes = self.profile.unknown_bytes.saturating_add(1);
                        self.remove_first();
                    }
                    BinexParse::ChecksumError => {
                        self.profile.parser_framing_errors =
                            self.profile.parser_framing_errors.saturating_add(1);
                        self.profile.parser_resynchronizations =
                            self.profile.parser_resynchronizations.saturating_add(1);
                        self.profile.crc_errors = self.profile.crc_errors.saturating_add(1);
                        self.profile.unknown_bytes = self.profile.unknown_bytes.saturating_add(1);
                        self.remove_first();
                    }
                    BinexParse::Frame { length, record_id } => {
                        let bytes = self.drain_prefix(length);
                        self.record_format_only(DataFormat::Binex, Some(record_id));
                        frames.push(Frame {
                            format: DataFormat::Binex,
                            bytes,
                            message_number: Some(record_id),
                        });
                    }
                },
                _ => match parse_rtcm2(&self.buffer, self.rtcm2_tail) {
                    Rtcm2Parse::NoCandidate => {
                        let skip = self
                            .buffer
                            .iter()
                            .position(|byte| matches!(*byte, 0xD3 | 0x02 | 0xE2 | b'$'))
                            .unwrap_or(self.buffer.len());
                        if skip > 0 {
                            self.profile.parser_resynchronizations =
                                self.profile.parser_resynchronizations.saturating_add(1);
                        }
                        self.profile.unknown_bytes =
                            self.profile.unknown_bytes.saturating_add(skip as u64);
                        self.drain_prefix(skip);
                    }
                    Rtcm2Parse::Incomplete { skip } => {
                        if let Some(boundary) = self
                            .buffer
                            .iter()
                            .position(|byte| matches!(*byte, 0xD3 | 0x02 | 0xE2 | b'$'))
                        {
                            if boundary > 0 {
                                self.profile.parser_resynchronizations =
                                    self.profile.parser_resynchronizations.saturating_add(1);
                            }
                            self.profile.unknown_bytes =
                                self.profile.unknown_bytes.saturating_add(boundary as u64);
                            self.drain_prefix(boundary);
                        } else {
                            if skip > 0 {
                                self.profile.parser_resynchronizations =
                                    self.profile.parser_resynchronizations.saturating_add(1);
                            }
                            self.profile.unknown_bytes =
                                self.profile.unknown_bytes.saturating_add(skip as u64);
                            self.drain_prefix(skip);
                            break;
                        }
                    }
                    Rtcm2Parse::ParityError { skip } => {
                        self.profile.parser_framing_errors =
                            self.profile.parser_framing_errors.saturating_add(1);
                        self.profile.parser_resynchronizations =
                            self.profile.parser_resynchronizations.saturating_add(1);
                        self.profile.crc_errors = self.profile.crc_errors.saturating_add(1);
                        self.profile.unknown_bytes =
                            self.profile.unknown_bytes.saturating_add(skip as u64 + 1);
                        self.drain_prefix(skip + 1);
                    }
                    Rtcm2Parse::Frame {
                        skip,
                        length,
                        message_number,
                    } => {
                        if skip > 0 {
                            self.profile.parser_resynchronizations =
                                self.profile.parser_resynchronizations.saturating_add(1);
                        }
                        self.profile.unknown_bytes =
                            self.profile.unknown_bytes.saturating_add(skip as u64);
                        self.drain_prefix(skip);
                        let bytes = self.drain_prefix(length);
                        self.record_format_only(DataFormat::Rtcm2, Some(message_number));
                        frames.push(Frame {
                            format: DataFormat::Rtcm2,
                            bytes,
                            message_number: Some(message_number),
                        });
                    }
                },
            }
        }
        self.trim_pending_tail();
        self.profile.parser_max_pending_bytes = self
            .profile
            .parser_max_pending_bytes
            .max(self.buffer.len() as u64);
        frames
    }

    fn trim_pending_tail(&mut self) {
        let excess = self.buffer.len().saturating_sub(MAX_PENDING_BYTES);
        if excess == 0 {
            return;
        }
        self.profile.parser_dropped_bytes = self
            .profile
            .parser_dropped_bytes
            .saturating_add(excess as u64);
        self.profile.unknown_bytes = self.profile.unknown_bytes.saturating_add(excess as u64);
        self.drain_prefix(excess);
    }

    fn drain_prefix(&mut self, length: usize) -> Vec<u8> {
        let bytes = self.buffer.drain(..length).collect::<Vec<_>>();
        self.observe_rtcm2_tail(&bytes);
        bytes
    }

    fn remove_first(&mut self) {
        let byte = self.buffer.remove(0);
        if is_rtcm2_byte(byte) {
            self.rtcm2_tail = Some(rtcm2_byte_tail(byte));
        }
    }

    fn clear_buffer(&mut self) {
        let bytes = std::mem::take(&mut self.buffer);
        self.observe_rtcm2_tail(&bytes);
    }

    fn observe_rtcm2_tail(&mut self, bytes: &[u8]) {
        if let Some(byte) = bytes.iter().rev().find(|byte| is_rtcm2_byte(**byte)) {
            self.rtcm2_tail = Some(rtcm2_byte_tail(*byte));
        }
    }

    fn record_format_only(&mut self, format: DataFormat, message_number: Option<u16>) {
        self.profile.valid_frames = self.profile.valid_frames.saturating_add(1);
        self.profile.correction_frames = self.profile.correction_frames.saturating_add(1);
        self.profile.correction_formats.insert(format);
        if let Some(message_number) = message_number {
            self.profile.message_numbers.insert(message_number);
        }
        self.profile.has_correction_data = true;
        self.profile.primary_format = Some(format);
    }

    fn record_rtcm3(&mut self, message_number: u16, payload: &[u8]) {
        self.profile.valid_frames = self.profile.valid_frames.saturating_add(1);
        self.profile.message_numbers.insert(message_number);
        let correction = is_correction_message(message_number);
        if correction {
            self.profile.correction_frames = self.profile.correction_frames.saturating_add(1);
            self.profile.correction_formats.insert(DataFormat::Rtcm3);
            self.profile.has_correction_data = true;
            self.profile.primary_format = Some(DataFormat::Rtcm3);
        } else if self.profile.primary_format.is_none() {
            self.profile.primary_format = Some(DataFormat::Rtcm3);
        }
        if (1071..=1137).contains(&message_number) {
            let version = (message_number % 10) as u8;
            if (1..=7).contains(&version) {
                self.profile.msm_message_seen = true;
                self.profile.msm_versions.insert(version);
                if let Some(satellites) = unsigned_bits(payload, 73, 64) {
                    self.profile.satellite_count = satellites.count_ones() as u16;
                    self.profile.satellite_ids = mask_ids(satellites, 64);
                }
                if let Some(signals) = unsigned_bits(payload, 137, 32) {
                    self.profile.signal_count = signals.count_ones() as u16;
                    self.profile.signal_ids = mask_ids(signals, 32);
                }
                if version >= 4 {
                    let observations = parse_msm_observations(payload, message_number);
                    self.record_observations(&observations);
                }
            }
        } else {
            let observations = parse_legacy_observations(payload, message_number);
            self.record_observations(&observations);
        }
        if matches!(message_number, 1005 | 1006)
            && let Some(position) = parse_rtcm3_position(payload)
        {
            self.profile.position_message_seen = true;
            self.profile.position = Some(position);
        }
        if matches!(message_number, 1005 | 1006 | 1033)
            && let Some(station_id) = unsigned_bits(payload, 12, 12)
        {
            self.profile.station_id = Some(station_id as u16);
        }
        if message_number == 1033
            && let Some(device) = parse_rtcm3_device(payload)
        {
            self.profile.device = Some(device);
        }
    }

    fn record_observations(&mut self, observations: &[GnssObservation]) {
        let Some(system) = observations.first().map(|observation| observation.system) else {
            return;
        };
        let epoch = observations[0].epoch_time_ms;
        let same_epoch =
            self.profile.msm_observations.iter().any(|observation| {
                observation.system == system && observation.epoch_time_ms == epoch
            });
        if !same_epoch {
            self.profile
                .msm_observations
                .retain(|observation| observation.system != system);
        }
        for observation in observations {
            if let Some(existing) = self.profile.msm_observations.iter_mut().find(|existing| {
                existing.system == observation.system
                    && existing.satellite_id == observation.satellite_id
                    && existing.signal_id == observation.signal_id
                    && existing.epoch_time_ms == observation.epoch_time_ms
            }) {
                *existing = *observation;
            } else {
                self.profile.msm_observations.push(*observation);
            }
        }
        self.profile.msm_observations.truncate(512);
        self.profile.satellite_ids = self
            .profile
            .msm_observations
            .iter()
            .map(|observation| observation.satellite_id)
            .collect();
        self.profile.satellite_count = self.profile.satellite_ids.len() as u16;
        self.profile.signal_ids = self
            .profile
            .msm_observations
            .iter()
            .map(|observation| observation.signal_id)
            .collect();
        self.profile.signal_count = self.profile.signal_ids.len() as u16;
        self.profile.satellite_cn0_q4.clear();
        for observation in &self.profile.msm_observations {
            if let Some(cn0) = observation.cn0_q4 {
                self.profile
                    .satellite_cn0_q4
                    .entry(observation.satellite_id)
                    .and_modify(|current| *current = (*current).max(cn0))
                    .or_insert(cn0);
            }
        }
    }
}

fn parse_legacy_observations(payload: &[u8], message_number: u16) -> Vec<GnssObservation> {
    let (system, first_signal, has_l2) = match message_number {
        1001..=1004 => (GnssSystem::Gps, 1, message_number >= 1003),
        1009..=1012 => (GnssSystem::Glonass, 1, message_number >= 1011),
        _ => return Vec::new(),
    };
    let has_cnr = matches!(message_number, 1002 | 1004 | 1010 | 1012);
    let Some(station_id) = unsigned_bits(payload, 12, 12) else {
        return Vec::new();
    };
    let epoch_bits = if system == GnssSystem::Glonass {
        27
    } else {
        30
    };
    let Some(epoch_time_ms) = unsigned_bits(payload, 24, epoch_bits) else {
        return Vec::new();
    };
    let count_offset = 24 + epoch_bits + 1;
    let Some(satellite_count) = unsigned_bits(payload, count_offset, 5) else {
        return Vec::new();
    };
    let mut offset = count_offset + 9;
    let mut observations = Vec::with_capacity(satellite_count as usize * 2);
    for _ in 0..satellite_count {
        let Some(satellite_id) = take_unsigned(payload, &mut offset, 6) else {
            return Vec::new();
        };
        let Some(_code) = take_unsigned(payload, &mut offset, 1) else {
            return Vec::new();
        };
        let glonass_frequency_channel = if system == GnssSystem::Glonass {
            let Some(raw) = take_unsigned(payload, &mut offset, 5) else {
                return Vec::new();
            };
            Some(raw as i8 - 7)
        } else {
            None
        };
        let pseudorange_bits = if system == GnssSystem::Glonass {
            25
        } else {
            24
        };
        let Some(pseudorange_raw) = take_unsigned(payload, &mut offset, pseudorange_bits) else {
            return Vec::new();
        };
        let pseudorange_mm = (pseudorange_raw != (1_u64 << pseudorange_bits) - 1)
            .then_some(pseudorange_raw as i64 * 20);
        let Some(phase_diff_raw) = take_signed(payload, &mut offset, 20) else {
            return Vec::new();
        };
        let Some(lock_time) = take_unsigned(payload, &mut offset, 7) else {
            return Vec::new();
        };
        let phase_mm = pseudorange_mm
            .zip((phase_diff_raw != -524_288).then_some(round_half_mm(phase_diff_raw)))
            .map(|(pseudorange, phase_diff)| pseudorange.saturating_add(phase_diff));
        let (ambiguity, cn0_q4) = if has_cnr {
            let ambiguity_bits = if system == GnssSystem::Glonass { 7 } else { 8 };
            let Some(ambiguity) = take_unsigned(payload, &mut offset, ambiguity_bits) else {
                return Vec::new();
            };
            let Some(cnr) = take_unsigned(payload, &mut offset, 8) else {
                return Vec::new();
            };
            (Some(ambiguity as u16), Some(cnr as u16))
        } else {
            (None, None)
        };
        observations.push(GnssObservation {
            system,
            message_number,
            station_id: station_id as u16,
            epoch_time_ms: epoch_time_ms as u32,
            satellite_id: satellite_id as u8,
            signal_id: first_signal,
            pseudorange_mm,
            carrier_phase_mm: phase_mm,
            doppler_mm_s: None,
            cn0_q4,
            lock_time_raw: Some(lock_time as u16),
            half_cycle_ambiguity: None,
            pseudorange_ambiguity_raw: ambiguity,
            glonass_frequency_channel,
        });

        if has_l2 {
            let Some(_l2_code) = take_unsigned(payload, &mut offset, 2) else {
                return Vec::new();
            };
            let Some(l2_diff_raw) = take_signed(payload, &mut offset, 14) else {
                return Vec::new();
            };
            if l2_diff_raw != -8192 {
                let l2_phase_diff_raw = take_signed(payload, &mut offset, 20);
                let Some(l2_phase_diff_raw) = l2_phase_diff_raw else {
                    return Vec::new();
                };
                let Some(l2_lock_time) = take_unsigned(payload, &mut offset, 7) else {
                    return Vec::new();
                };
                let l2_cnr = if message_number == 1004 || message_number == 1012 {
                    let Some(cnr) = take_unsigned(payload, &mut offset, 8) else {
                        return Vec::new();
                    };
                    Some(cnr as u16)
                } else {
                    None
                };
                let l2_pseudorange_mm = pseudorange_mm
                    .map(|pseudorange| pseudorange.saturating_add(l2_diff_raw.saturating_mul(20)));
                observations.push(GnssObservation {
                    system,
                    message_number,
                    station_id: station_id as u16,
                    epoch_time_ms: epoch_time_ms as u32,
                    satellite_id: satellite_id as u8,
                    signal_id: 2,
                    pseudorange_mm: l2_pseudorange_mm,
                    carrier_phase_mm: l2_pseudorange_mm
                        .zip(
                            (l2_phase_diff_raw != -524_288)
                                .then_some(round_half_mm(l2_phase_diff_raw)),
                        )
                        .map(|(pseudorange, phase_diff)| pseudorange.saturating_add(phase_diff)),
                    doppler_mm_s: None,
                    cn0_q4: l2_cnr,
                    lock_time_raw: Some(l2_lock_time as u16),
                    half_cycle_ambiguity: None,
                    pseudorange_ambiguity_raw: None,
                    glonass_frequency_channel,
                });
            } else {
                let _ = take_signed(payload, &mut offset, 20);
                let _ = take_unsigned(payload, &mut offset, 7);
                if message_number == 1004 || message_number == 1012 {
                    let _ = take_unsigned(payload, &mut offset, 8);
                }
            }
        }
    }
    observations
}

fn parse_msm_observations(payload: &[u8], message_number: u16) -> Vec<GnssObservation> {
    let Some(system) = GnssSystem::from_message_number(message_number) else {
        return Vec::new();
    };
    let version = (message_number % 10) as u8;
    if !(4..=7).contains(&version) {
        return Vec::new();
    }
    let Some(satellites) = unsigned_bits(payload, 73, 64) else {
        return Vec::new();
    };
    let Some(signals) = unsigned_bits(payload, 137, 32) else {
        return Vec::new();
    };
    let Some(station_id) = unsigned_bits(payload, 12, 12) else {
        return Vec::new();
    };
    let Some(epoch_time_ms) = unsigned_bits(payload, 24, 30) else {
        return Vec::new();
    };
    let satellite_ids = mask_ids(satellites, 64);
    let signal_ids = mask_ids(signals, 32);
    let cell_count = satellite_ids.len().saturating_mul(signal_ids.len());
    if cell_count > 64 {
        return Vec::new();
    }
    let mut cells = Vec::with_capacity(cell_count);
    for satellite_index in 0..satellite_ids.len() {
        for signal_index in 0..signal_ids.len() {
            let bit = 169 + satellite_index * signal_ids.len() + signal_index;
            if unsigned_bits(payload, bit, 1) == Some(1) {
                cells.push((satellite_index, signal_index));
            }
        }
    }
    let satellite_bits = satellite_ids.len()
        * if matches!(version, 5 | 7) {
            if system == GnssSystem::Glonass {
                36
            } else {
                32
            }
        } else {
            18
        };
    let cell_bits = cells.len()
        * match version {
            4 => 48,
            5 => 63,
            6 => 65,
            7 => 80,
            _ => return Vec::new(),
        };
    if 169 + cell_count + satellite_bits + cell_bits > payload.len() * 8 {
        return Vec::new();
    }
    let mut offset = 169 + cell_count;
    let satellite_count = satellite_ids.len();
    let mut rough_range_ms = vec![255_u8; satellite_count];
    let mut rough_range_mod = vec![0_u16; satellite_count];
    let mut rough_phase_rate = vec![None; satellite_count];
    let mut glonass_frequency_channel = vec![None; satellite_count];

    read_msm_fields(payload, &mut offset, satellite_count, 8, |index, value| {
        rough_range_ms[index] = value as u8;
    });
    if system == GnssSystem::Glonass && matches!(version, 5 | 7) {
        read_msm_fields(payload, &mut offset, satellite_count, 4, |index, value| {
            glonass_frequency_channel[index] = (value != 15).then_some(value as i8 - 7);
        });
    }
    read_msm_fields(payload, &mut offset, satellite_count, 10, |index, value| {
        rough_range_mod[index] = value as u16;
    });
    if matches!(version, 5 | 7) {
        read_msm_fields_signed(payload, &mut offset, satellite_count, 14, |index, value| {
            rough_phase_rate[index] = (value != -8192).then_some(value);
        });
    }

    let cell_count = cells.len();
    let mut fine_pseudorange = vec![None; cell_count];
    let mut fine_phase = vec![None; cell_count];
    let mut lock_time = vec![None; cell_count];
    let mut half_cycle = vec![None; cell_count];
    let mut cn0_q4 = vec![None; cell_count];
    let mut fine_phase_rate = vec![None; cell_count];
    match version {
        4 | 5 => {
            read_msm_fields_signed(payload, &mut offset, cell_count, 15, |index, value| {
                fine_pseudorange[index] = (value != -16_384).then_some(value as f64 / 16_777_216.0);
            });
            read_msm_fields_signed(payload, &mut offset, cell_count, 22, |index, value| {
                fine_phase[index] = (value != -2_097_152).then_some(value as f64 / 536_870_912.0);
            });
            read_msm_fields(payload, &mut offset, cell_count, 4, |index, value| {
                lock_time[index] = Some(value as u16);
            });
            read_msm_fields(payload, &mut offset, cell_count, 1, |index, value| {
                half_cycle[index] = Some(value != 0);
            });
            read_msm_fields(payload, &mut offset, cell_count, 6, |index, value| {
                cn0_q4[index] = Some(value as u16 * 4);
            });
            if version == 5 {
                read_msm_fields_signed(payload, &mut offset, cell_count, 15, |index, value| {
                    fine_phase_rate[index] = (value != -16_384).then_some(value as f64 * 0.0001);
                });
            }
        }
        6 | 7 => {
            read_msm_fields_signed(payload, &mut offset, cell_count, 20, |index, value| {
                fine_pseudorange[index] =
                    (value != -524_288).then_some(value as f64 / 536_870_912.0);
            });
            read_msm_fields_signed(payload, &mut offset, cell_count, 24, |index, value| {
                fine_phase[index] = (value != -8_388_608).then_some(value as f64 / 2_147_483_648.0);
            });
            read_msm_fields(payload, &mut offset, cell_count, 10, |index, value| {
                lock_time[index] = Some(value as u16);
            });
            read_msm_fields(payload, &mut offset, cell_count, 1, |index, value| {
                half_cycle[index] = Some(value != 0);
            });
            read_msm_fields(payload, &mut offset, cell_count, 10, |index, value| {
                cn0_q4[index] = Some(value as u16);
            });
            if version == 7 {
                read_msm_fields_signed(payload, &mut offset, cell_count, 15, |index, value| {
                    fine_phase_rate[index] = (value != -16_384).then_some(value as f64 * 0.0001);
                });
            }
        }
        _ => return Vec::new(),
    }

    cells
        .into_iter()
        .enumerate()
        .filter_map(|(cell_index, (satellite_index, signal_index))| {
            let satellite_id = *satellite_ids.iter().nth(satellite_index)?;
            let signal_id = *signal_ids.iter().nth(signal_index)?;
            let coarse_ms = (rough_range_ms[satellite_index] != 255).then_some(
                f64::from(rough_range_ms[satellite_index])
                    + f64::from(rough_range_mod[satellite_index]) / 1024.0,
            );
            let pseudorange_ms = coarse_ms
                .zip(fine_pseudorange[cell_index])
                .map(|(a, b)| a + b);
            let carrier_ms = coarse_ms.zip(fine_phase[cell_index]).map(|(a, b)| a + b);
            let doppler_m_s = match (
                rough_phase_rate[satellite_index],
                fine_phase_rate[cell_index],
            ) {
                (Some(rough), Some(fine)) => Some(rough as f64 + fine),
                (Some(rough), None) => Some(rough as f64),
                _ => None,
            };
            Some(GnssObservation {
                system,
                message_number,
                station_id: station_id as u16,
                epoch_time_ms: epoch_time_ms as u32,
                satellite_id,
                signal_id,
                pseudorange_mm: pseudorange_ms
                    .map(|value| (value * 299_792_458_000.0).round() as i64),
                carrier_phase_mm: carrier_ms
                    .map(|value| (value * 299_792_458_000.0).round() as i64),
                doppler_mm_s: doppler_m_s.map(|value| (value * 1000.0).round() as i64),
                cn0_q4: cn0_q4[cell_index],
                lock_time_raw: lock_time[cell_index],
                half_cycle_ambiguity: half_cycle[cell_index],
                pseudorange_ambiguity_raw: None,
                glonass_frequency_channel: glonass_frequency_channel[satellite_index],
            })
        })
        .collect()
}

fn read_msm_fields(
    payload: &[u8],
    offset: &mut usize,
    count: usize,
    length: usize,
    mut read: impl FnMut(usize, u64),
) {
    for index in 0..count {
        let Some(value) = unsigned_bits(payload, *offset, length) else {
            return;
        };
        read(index, value);
        *offset += length;
    }
}

fn take_unsigned(data: &[u8], offset: &mut usize, length: usize) -> Option<u64> {
    let value = unsigned_bits(data, *offset, length)?;
    *offset += length;
    Some(value)
}

fn take_signed(data: &[u8], offset: &mut usize, length: usize) -> Option<i64> {
    let value = signed_bits(data, *offset, length)?;
    *offset += length;
    Some(value)
}

fn round_half_mm(raw: i64) -> i64 {
    (raw as f64 * 0.5).round() as i64
}

fn read_msm_fields_signed(
    payload: &[u8],
    offset: &mut usize,
    count: usize,
    length: usize,
    mut read: impl FnMut(usize, i64),
) {
    for index in 0..count {
        let Some(value) = signed_bits(payload, *offset, length) else {
            return;
        };
        read(index, value);
        *offset += length;
    }
}

fn mask_ids(mask: u64, width: u8) -> BTreeSet<u8> {
    (0..width)
        .filter(|bit| mask & (1_u64 << bit) != 0)
        .map(|bit| width - bit)
        .collect()
}

impl DataProfile {
    pub fn format_message_numbers(&self) -> String {
        self.message_numbers
            .iter()
            .map(u16::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl AdmissionPolicy {
    pub fn with_source_expectation(&self, expectation: &SourceExpectation) -> Self {
        let mut policy = self.clone();
        policy.expected_formats = Some(
            expectation
                .formats
                .iter()
                .filter_map(|format| match format.as_str() {
                    "rtcm3" => Some(DataFormat::Rtcm3),
                    "rtcm2" => Some(DataFormat::Rtcm2),
                    "binex" => Some(DataFormat::Binex),
                    "cmr" => Some(DataFormat::Cmr),
                    "nmea" => Some(DataFormat::Nmea),
                    _ => None,
                })
                .collect(),
        );
        if !expectation.msm_versions.is_empty() {
            policy.allowed_msm_versions = Some(match &policy.allowed_msm_versions {
                Some(global) => global
                    .iter()
                    .filter(|version| expectation.msm_versions.contains(version))
                    .copied()
                    .collect(),
                None => expectation.msm_versions.clone(),
            });
        }
        policy
    }

    pub fn evaluate(&self, profile: &DataProfile) -> AdmissionProgress {
        if profile.valid_frames < self.min_valid_frames {
            return AdmissionProgress::Pending;
        }
        let mut violations = Vec::new();
        if profile.valid_frames == 0 {
            violations.push(AdmissionViolation::NoValidFrames);
        }
        if let Some(format) = profile.primary_format
            && !self.allowed_formats.contains(&format)
        {
            violations.push(AdmissionViolation::FormatNotAllowed(format));
        }
        if let Some(expected_formats) = &self.expected_formats
            && let Some(format) = profile.primary_format
            && !expected_formats.contains(&format)
        {
            violations.push(AdmissionViolation::ExpectedFormatMismatch(format));
        }
        if self.require_correction_data && profile.correction_frames == 0 {
            violations.push(if profile.primary_format == Some(DataFormat::Rtcm3) {
                AdmissionViolation::NoObservationOrSsr
            } else {
                AdmissionViolation::MissingCorrectionData
            });
        }
        if profile.correction_frames > 0 && profile.correction_frames < self.min_correction_frames {
            violations.push(AdmissionViolation::MissingCorrectionData);
        }
        if profile.correction_formats.len() > 1 {
            violations.push(AdmissionViolation::MixedCorrectionFormats);
        }
        let integrity_attempts = profile.valid_frames.saturating_add(profile.crc_errors);
        if profile.valid_frames >= self.min_valid_frames
            && integrity_attempts > 0
            && profile.crc_errors as f64 / integrity_attempts as f64
                > self.max_crc_error_ratio.unwrap_or(1.0)
        {
            violations.push(AdmissionViolation::CrcThresholdExceeded);
        }
        if self.require_base_position
            && profile.primary_format == Some(DataFormat::Rtcm3)
            && !profile.position_message_seen
        {
            violations.push(AdmissionViolation::BasePositionMissing);
        }
        if self.require_msm
            && profile.primary_format == Some(DataFormat::Rtcm3)
            && !profile.msm_message_seen
        {
            violations.push(AdmissionViolation::MsmRequiredMissing);
        }
        if let Some(allowed_versions) = &self.allowed_msm_versions {
            violations.extend(
                profile
                    .msm_versions
                    .iter()
                    .filter(|version| !allowed_versions.contains(version))
                    .copied()
                    .map(AdmissionViolation::MsmVersionNotAllowed),
            );
        }
        if let Some(max_ratio) = self.max_unknown_ratio
            && profile.total_bytes >= self.min_observed_bytes
            && profile.unknown_bytes as f64 / profile.total_bytes.max(1) as f64 > max_ratio
        {
            violations.push(AdmissionViolation::UnknownDataRatioExceeded);
        }
        if violations.is_empty() {
            return AdmissionProgress::Accepted {
                warnings: Vec::new(),
            };
        }
        match self.mode {
            AdmissionMode::Observe => AdmissionProgress::Accepted {
                warnings: violations,
            },
            AdmissionMode::Enforce => AdmissionProgress::Rejected { violations },
        }
    }
}

impl Default for ProtocolDemux {
    fn default() -> Self {
        Self::new()
    }
}

enum Rtcm3Parse {
    Incomplete,
    Invalid,
    CrcError,
    Frame { length: usize, message_number: u16 },
}

enum Rtcm2Parse {
    NoCandidate,
    Incomplete {
        skip: usize,
    },
    ParityError {
        skip: usize,
    },
    Frame {
        skip: usize,
        length: usize,
        message_number: u16,
    },
}

enum BinexParse {
    Incomplete,
    Invalid,
    ChecksumError,
    Frame { length: usize, record_id: u16 },
}

enum CmrParse {
    Incomplete,
    Invalid,
    ChecksumError,
    Frame { length: usize, message_number: u16 },
}

fn parse_rtcm3(buffer: &[u8]) -> Rtcm3Parse {
    if buffer.len() < 3 {
        return Rtcm3Parse::Incomplete;
    }
    if buffer[0] != 0xD3 || buffer[1] & 0xFC != 0 {
        return Rtcm3Parse::Invalid;
    }
    let payload_length = (((buffer[1] & 0x03) as usize) << 8) | buffer[2] as usize;
    let length = payload_length + 6;
    if buffer.len() < length {
        return Rtcm3Parse::Incomplete;
    }
    let expected = u32::from_be_bytes([
        0,
        buffer[length - 3],
        buffer[length - 2],
        buffer[length - 1],
    ]);
    if crc24q(&buffer[..length - 3]) != expected {
        return Rtcm3Parse::CrcError;
    }
    let payload = &buffer[3..length - 3];
    if payload.len() < 2 {
        return Rtcm3Parse::Invalid;
    }
    let message_number = (u16::from(payload[0]) << 4) | u16::from(payload[1] >> 4);
    Rtcm3Parse::Frame {
        length,
        message_number,
    }
}

fn parse_rtcm2(buffer: &[u8], mut previous_tail: Option<u8>) -> Rtcm2Parse {
    let mut incomplete = None;
    let mut parity_error = None;
    for skip in 0..buffer.len() {
        let candidate_tail = previous_tail;
        if is_rtcm2_byte(buffer[skip]) {
            previous_tail = Some(rtcm2_byte_tail(buffer[skip]));
        }
        let candidate = &buffer[skip..];
        if candidate.len() < 5 {
            if candidate.iter().all(|byte| is_rtcm2_byte(*byte)) {
                incomplete.get_or_insert(skip);
            }
            continue;
        }
        if !candidate[..5].iter().all(|byte| is_rtcm2_byte(*byte)) {
            continue;
        }
        let known_tail = candidate_tail;
        let last_tail = if known_tail.is_some() { 0 } else { 3 };
        for tail_index in 0..=last_tail {
            let previous_tail = known_tail.unwrap_or(tail_index);
            let Some((first, first_tail)) = decode_rtcm2_word(&candidate[..5], previous_tail)
            else {
                continue;
            };
            let message_number = u16::from(first[1] >> 2);
            if first[0] != 0x66 || !(1..=64).contains(&message_number) {
                continue;
            }
            if candidate.len() < 10 {
                incomplete.get_or_insert(skip);
                continue;
            }
            if !candidate[5..10].iter().all(|byte| is_rtcm2_byte(*byte)) {
                parity_error.get_or_insert(skip);
                continue;
            }
            let Some((second, second_tail)) = decode_rtcm2_word(&candidate[5..10], first_tail)
            else {
                parity_error.get_or_insert(skip);
                continue;
            };
            let length = usize::from(second[2] >> 3) * 3 + 6;
            if length > 99 {
                continue;
            }
            let expected = length / 3 * 5;
            if candidate.len() < expected {
                if candidate[..candidate.len()]
                    .iter()
                    .all(|byte| is_rtcm2_byte(*byte))
                {
                    incomplete.get_or_insert(skip);
                } else {
                    parity_error.get_or_insert(skip);
                }
                continue;
            }
            if !candidate[..expected]
                .iter()
                .all(|byte| is_rtcm2_byte(*byte))
            {
                parity_error.get_or_insert(skip);
                continue;
            }
            let mut tail = second_tail;
            let mut valid = true;
            for word in 2..(length / 3) {
                let start = word * 5;
                let Some((_, next_tail)) = decode_rtcm2_word(&candidate[start..start + 5], tail)
                else {
                    valid = false;
                    break;
                };
                tail = next_tail;
            }
            if !valid {
                parity_error.get_or_insert(skip);
                continue;
            }
            if let Some(incomplete) = incomplete
                && incomplete < skip
            {
                return Rtcm2Parse::Incomplete { skip: incomplete };
            }
            return Rtcm2Parse::Frame {
                skip,
                length: expected,
                message_number,
            };
        }
    }
    match (parity_error, incomplete) {
        (Some(parity), Some(incomplete)) if incomplete <= parity => {
            Rtcm2Parse::Incomplete { skip: incomplete }
        }
        (Some(skip), _) => Rtcm2Parse::ParityError { skip },
        (None, Some(skip)) => Rtcm2Parse::Incomplete { skip },
        (None, None) => Rtcm2Parse::NoCandidate,
    }
}

fn decode_rtcm2_word(input: &[u8], previous_tail: u8) -> Option<([u8; 3], u8)> {
    const HAMMING: [u32; 6] = [
        0xBB1F3480, 0x5D8F9A40, 0xAEC7CD00, 0x5763E680, 0x6BB1F340, 0x8B7A89C0,
    ];
    let mut chunk = 0_u32;
    for byte in &input[..5] {
        for bit in 0..6 {
            chunk = (chunk << 1) | u32::from((byte >> bit) & 1);
        }
    }
    let mut word = (u32::from(previous_tail & 0x03) << 30) | chunk;
    if word & 0x4000_0000 != 0 {
        word ^= 0x3FFF_FFC0;
    }
    let mut parity = 0_u32;
    for mask in HAMMING {
        parity <<= 1;
        let mut bits = (word & mask) >> 6;
        while bits != 0 {
            parity ^= bits & 1;
            bits >>= 1;
        }
    }
    if parity != word & 0x3F {
        return None;
    }
    Some((
        [(word >> 22) as u8, (word >> 14) as u8, (word >> 6) as u8],
        (chunk & 0x03) as u8,
    ))
}

fn is_rtcm2_byte(byte: u8) -> bool {
    (byte & 0xC0) == 0x40
}

fn rtcm2_byte_tail(byte: u8) -> u8 {
    (((byte >> 4) & 1) << 1) | ((byte >> 5) & 1)
}

fn parse_binex(buffer: &[u8]) -> BinexParse {
    // shortcut: only big-endian regular-checksum records (E2); add other sync forms with device fixtures.
    if buffer.len() < 3 {
        return BinexParse::Incomplete;
    }
    if buffer[0] != 0xE2 {
        return BinexParse::Invalid;
    }
    let record_id = buffer[1];
    if !matches!(record_id, 0x00..=0x03 | 0x7D..=0x7F) {
        return BinexParse::Invalid;
    }
    let Some((payload_length, length_bytes)) = read_binex_length(&buffer[2..]) else {
        return BinexParse::Incomplete;
    };
    if payload_length > 4096 {
        return BinexParse::Invalid;
    }
    let checksum_length = if payload_length + length_bytes as u64 + 1 < 128 {
        1
    } else {
        2
    };
    let length = payload_length as usize + length_bytes + 2 + checksum_length;
    if buffer.len() < length {
        return BinexParse::Incomplete;
    }
    let checksum_start = length - checksum_length;
    let valid = if checksum_length == 1 {
        buffer[checksum_start] == checksum8(&buffer[1..checksum_start])
    } else {
        let expected = u16::from_be_bytes([buffer[checksum_start], buffer[checksum_start + 1]]);
        crc16(&buffer[1..checksum_start]) == expected
    };
    if !valid {
        return BinexParse::ChecksumError;
    }
    BinexParse::Frame {
        length,
        record_id: u16::from(record_id),
    }
}

fn read_binex_length(input: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0_u64;
    for (index, byte) in input.iter().take(3).enumerate() {
        value = (value << 7) | u64::from(byte & 0x7F);
        if byte & 0x80 == 0 {
            return Some((value, index + 1));
        }
    }
    input
        .get(3)
        .map(|byte| ((value << 8) | u64::from(*byte), 4))
}

fn checksum8(data: &[u8]) -> u8 {
    data.iter().fold(0, |checksum, byte| checksum ^ byte)
}

fn crc16(data: &[u8]) -> u16 {
    let mut crc = 0_u16;
    for byte in data {
        crc ^= u16::from(*byte) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

fn parse_cmr(buffer: &[u8]) -> CmrParse {
    // shortcut: NTRIP CMR is carried in Trimble TrimComm packets; standalone CMR streams need fixtures.
    if buffer.len() < 4 {
        return CmrParse::Incomplete;
    }
    if buffer[0] != 0x02 || !matches!(buffer[2], 0x93 | 0x94 | 0x98) {
        return CmrParse::Invalid;
    }
    let length = usize::from(buffer[3]) + 6;
    if buffer.len() < length {
        return CmrParse::Incomplete;
    }
    if buffer[length - 1] != 0x03 {
        return CmrParse::Invalid;
    }
    if buffer[length - 2]
        != buffer[1..length - 2]
            .iter()
            .fold(0, |sum, byte| sum.wrapping_add(*byte))
    {
        return CmrParse::ChecksumError;
    }
    let packet_type = u16::from(buffer[2]);
    let inner_type = if packet_type == 0x93 && buffer[3] >= 2 {
        buffer[5] >> 5
    } else {
        buffer[2]
    };
    CmrParse::Frame {
        length,
        message_number: u16::from(inner_type),
    }
}

fn crc24q(data: &[u8]) -> u32 {
    let mut crc = 0_u32;
    for byte in data {
        crc ^= u32::from(*byte) << 16;
        for _ in 0..8 {
            crc <<= 1;
            if crc & 0x100_0000 != 0 {
                crc ^= 0x1864CFB;
            }
        }
    }
    crc & 0xFF_FFFF
}

fn valid_nmea(line: &[u8]) -> bool {
    let line = line
        .strip_suffix(b"\n")
        .unwrap_or(line)
        .strip_suffix(b"\r")
        .unwrap_or(line);
    let Some(checksum_start) = line.iter().rposition(|byte| *byte == b'*') else {
        return false;
    };
    if !line.starts_with(b"$")
        || checksum_start + 3 != line.len()
        || !line[checksum_start + 1..].iter().all(u8::is_ascii_hexdigit)
    {
        return false;
    }
    let expected = line[checksum_start + 1..].iter().fold(0_u8, |value, byte| {
        (value << 4) | nmea_hex_value(*byte).unwrap_or(0)
    });
    let actual = line[1..checksum_start]
        .iter()
        .fold(0_u8, |checksum, byte| checksum ^ byte);
    actual == expected
}

fn nmea_hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn is_correction_message(message_type: u16) -> bool {
    matches!(message_type, 1001..=1004 | 1009..=1012)
        || ((1071..=1137).contains(&message_type) && (1..=7).contains(&(message_type % 10)))
        || (1057..=1068).contains(&message_type)
        || (1240..=1270).contains(&message_type)
}

fn parse_rtcm3_position(payload: &[u8]) -> Option<GeoPosition> {
    let x = signed_bits(payload, 31, 38)? as f64 * 0.0001;
    let y = signed_bits(payload, 69, 38)? as f64 * 0.0001;
    let z = signed_bits(payload, 107, 38)? as f64 * 0.0001;
    Some(ecef_to_position(x, y, z))
}

fn signed_bits(data: &[u8], start: usize, length: usize) -> Option<i64> {
    if start + length > data.len() * 8 {
        return None;
    }
    let mut value = 0_u64;
    for bit in start..start + length {
        let byte = data[bit / 8];
        value = (value << 1) | u64::from((byte >> (7 - bit % 8)) & 1);
    }
    if length < 64 && value & (1 << (length - 1)) != 0 {
        value |= u64::MAX << length;
    }
    Some(value as i64)
}

fn unsigned_bits(data: &[u8], start: usize, length: usize) -> Option<u64> {
    if start + length > data.len() * 8 {
        return None;
    }
    let mut value = 0_u64;
    for bit in start..start + length {
        let byte = data[bit / 8];
        value = (value << 1) | u64::from((byte >> (7 - bit % 8)) & 1);
    }
    Some(value)
}

fn parse_rtcm3_device(payload: &[u8]) -> Option<DeviceInfo> {
    let station_id = unsigned_bits(payload, 12, 12)? as u16;
    Some(DeviceInfo {
        station_id,
        antenna_descriptor: read_ascii(payload, 24, 16)?,
        antenna_serial: read_ascii(payload, 160, 32)?,
        receiver_type: read_ascii(payload, 416, 16)?,
        receiver_firmware: read_ascii(payload, 544, 8)?,
        receiver_serial: read_ascii(payload, 608, 32)?,
    })
}

fn read_ascii(data: &[u8], start: usize, length: usize) -> Option<String> {
    let mut value = String::with_capacity(length);
    for index in 0..length {
        let byte = unsigned_bits(data, start + index * 8, 8)? as u8;
        if byte != 0 {
            value.push(char::from(byte));
        }
    }
    Some(value.trim().to_owned())
}

fn ecef_to_position(x: f64, y: f64, z: f64) -> GeoPosition {
    const A: f64 = 6_378_137.0;
    const E2: f64 = 0.006_694_379_990_14;
    let longitude = y.atan2(x);
    let p = x.hypot(y);
    let mut latitude = z.atan2(p * (1.0 - E2));
    let mut height = 0.0;
    for _ in 0..8 {
        let sin = latitude.sin();
        let radius = A / (1.0 - E2 * sin * sin).sqrt();
        height = p / latitude.cos() - radius;
        latitude = z.atan2(p * (1.0 - E2 * radius / (radius + height)));
    }
    GeoPosition {
        latitude_e7: (latitude.to_degrees() * 1e7).round() as i32,
        longitude_e7: (longitude.to_degrees() * 1e7).round() as i32,
        height_cm: (height * 100.0).round() as i64,
    }
}

pub fn parse_nmea_gga(line: &[u8]) -> Option<NmeaGga> {
    if !valid_nmea(line) {
        return None;
    }
    let line = std::str::from_utf8(line).ok()?;
    let mut fields = line.trim_end().split(',');
    let sentence = fields.next()?;
    if !(sentence.starts_with("$GPGGA") || sentence.starts_with("$GNGGA")) {
        return None;
    }
    fields.next()?; // UTC time
    let position = GeoPosition {
        latitude_e7: parse_nmea_coordinate(fields.next()?, fields.next()?)?,
        longitude_e7: parse_nmea_coordinate(fields.next()?, fields.next()?)?,
        height_cm: 0,
    };
    let fix_quality = fields.next()?.parse::<u8>().ok()?;
    let _satellites = fields.next()?;
    let hdop = fields.next()?.parse::<f32>().ok()?;
    let height = fields.next()?.parse::<f64>().ok()?;
    Some(NmeaGga {
        position: GeoPosition {
            height_cm: (height * 100.0).round() as i64,
            ..position
        },
        fix_quality,
        hdop,
    })
}

fn parse_nmea_gsv(line: &[u8]) -> Option<Vec<NmeaSatellite>> {
    if !valid_nmea(line) {
        return None;
    }
    let line = std::str::from_utf8(line).ok()?;
    let mut fields = line.trim_end().split(',');
    let sentence = fields.next()?;
    if !sentence.ends_with("GSV") || sentence.len() < 6 {
        return None;
    }
    let system = &sentence[1..3];
    fields.next()?;
    fields.next()?;
    fields.next()?;
    let values = fields.collect::<Vec<_>>();
    let mut satellites = Vec::new();
    for chunk in values.chunks(4) {
        if chunk.len() < 4 {
            break;
        }
        let Some(prn) = chunk[0].parse::<u16>().ok() else {
            break;
        };
        let elevation_deg = chunk[1].parse::<u8>().unwrap_or_default();
        let azimuth_deg = chunk[2].parse::<u16>().unwrap_or_default();
        let snr = chunk[3].split('*').next().unwrap_or_default();
        let snr_db = snr.parse::<u8>().ok();
        satellites.push(NmeaSatellite {
            system: system.to_owned(),
            prn,
            elevation_deg,
            azimuth_deg,
            snr_db,
        });
    }
    (!satellites.is_empty()).then_some(satellites)
}

fn parse_nmea_coordinate(value: &str, hemisphere: &str) -> Option<i32> {
    if value.is_empty() {
        return None;
    }
    let number = value.parse::<f64>().ok()?;
    let degrees = (number / 100.0).trunc();
    let minutes = number - degrees * 100.0;
    let mut result = degrees + minutes / 60.0;
    if hemisphere.starts_with('S') || hemisphere.starts_with('W') {
        result = -result;
    }
    Some((result * 1e7).round() as i32)
}
