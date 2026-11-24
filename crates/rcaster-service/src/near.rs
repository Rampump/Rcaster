use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use rcaster_core::{
    protocol::{DataFormat, GeoPosition},
    str_record::StrRecord,
};

#[derive(Clone, Debug)]
pub struct NearConfig {
    pub max_distance_m: f64,
    pub hysteresis_m: f64,
    pub base_position_uncertainty_m: f64,
    pub hdop_m_per_unit: f64,
    pub max_hdop: f32,
    pub switch_position_threshold_m: f64,
    pub observe_time: Duration,
    pub min_recompute_interval: Duration,
    pub max_recompute_interval: Duration,
    pub min_recompute_distance_m: f64,
    pub max_spatial_queries_per_second: u32,
    pub max_query_concurrency: usize,
    pub gga_max_input_per_second: u32,
    pub gga_max_jump_speed_mps: f64,
    pub position_ttl_s: u64,
}

impl Default for NearConfig {
    fn default() -> Self {
        Self {
            max_distance_m: 100_000.0,
            hysteresis_m: 5.0,
            base_position_uncertainty_m: 1.0,
            hdop_m_per_unit: 1.5,
            max_hdop: 2.5,
            switch_position_threshold_m: 4.0,
            observe_time: Duration::from_secs(3),
            min_recompute_interval: Duration::from_secs(1),
            max_recompute_interval: Duration::from_secs(5),
            min_recompute_distance_m: 4.0,
            max_spatial_queries_per_second: 10_000,
            max_query_concurrency: 8,
            gga_max_input_per_second: 10,
            gga_max_jump_speed_mps: 1_000.0,
            position_ttl_s: 0,
        }
    }
}

pub struct NearQueryLimiter {
    inner: Arc<QueryLimiterInner>,
}

struct QueryLimiterInner {
    max_queries_per_second: u32,
    max_concurrency: usize,
    state: Mutex<QueryLimiterState>,
}

struct QueryLimiterState {
    tokens: f64,
    last_refill: Instant,
    active: usize,
}

pub struct NearQueryPermit {
    inner: Arc<QueryLimiterInner>,
}

impl NearQueryLimiter {
    pub fn new(max_queries_per_second: u32, max_concurrency: usize) -> Option<Self> {
        if max_queries_per_second == 0 || max_concurrency == 0 {
            return None;
        }
        Some(Self {
            inner: Arc::new(QueryLimiterInner {
                max_queries_per_second,
                max_concurrency,
                state: Mutex::new(QueryLimiterState {
                    tokens: f64::from(max_queries_per_second),
                    last_refill: Instant::now(),
                    active: 0,
                }),
            }),
        })
    }

    pub fn try_acquire(&self) -> Option<NearQueryPermit> {
        self.try_acquire_at(Instant::now())
    }

    #[doc(hidden)]
    pub fn try_acquire_at(&self, now: Instant) -> Option<NearQueryPermit> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.active >= self.inner.max_concurrency {
            return None;
        }
        let elapsed = now
            .saturating_duration_since(state.last_refill)
            .as_secs_f64();
        let capacity = f64::from(self.inner.max_queries_per_second);
        state.tokens = (state.tokens + elapsed * capacity).min(capacity);
        state.last_refill = now;
        if state.tokens < 1.0 {
            return None;
        }
        state.tokens -= 1.0;
        state.active += 1;
        Some(NearQueryPermit {
            inner: Arc::clone(&self.inner),
        })
    }
}

impl Drop for NearQueryPermit {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active = state.active.saturating_sub(1);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GgaAcceptance {
    Accepted,
    RateLimited,
    JumpRejected,
    Invalid,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NearGga {
    pub position: GeoPosition,
    pub fix_quality: u8,
    pub hdop: f32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NearSource {
    pub mountpoint: String,
    pub position: GeoPosition,
    pub format: DataFormat,
    pub usable_correction: bool,
}

impl NearSource {
    pub fn from_str_record(record: &StrRecord) -> Option<Self> {
        if !record.format.eq_ignore_ascii_case("RTCM3") && !record.format.starts_with("RTCM 3") {
            return None;
        }
        let latitude = record.latitude.parse::<f64>().ok()?;
        let longitude = record.longitude.parse::<f64>().ok()?;
        if !(-90.0..=90.0).contains(&latitude) || !(-180.0..=180.0).contains(&longitude) {
            return None;
        }
        Some(Self {
            mountpoint: record.mountpoint.clone(),
            position: GeoPosition {
                latitude_e7: (latitude * 1e7).round() as i32,
                longitude_e7: (longitude * 1e7).round() as i32,
                height_cm: 0,
            },
            format: DataFormat::Rtcm3,
            usable_correction: true,
        })
    }
}

pub struct NearIndex {
    sources: Vec<NearSource>,
    cells: BTreeMap<(i32, i32), Vec<usize>>,
}

impl NearIndex {
    const CELL_DEGREES: f64 = 1.0;

    pub fn new(sources: &[NearSource]) -> Self {
        let mut cells = BTreeMap::<(i32, i32), Vec<usize>>::new();
        for (index, source) in sources.iter().enumerate() {
            cells
                .entry(Self::cell(source.position))
                .or_default()
                .push(index);
        }
        Self {
            sources: sources.to_vec(),
            cells,
        }
    }

    pub fn get(&self, mountpoint: &str) -> Option<&NearSource> {
        self.sources
            .iter()
            .find(|source| source.mountpoint == mountpoint)
    }

    pub fn nearest(&self, position: GeoPosition, max_distance_m: f64) -> Option<&NearSource> {
        self.nearest_excluding(position, max_distance_m, None)
    }

    pub fn nearest_excluding(
        &self,
        position: GeoPosition,
        max_distance_m: f64,
        excluded: Option<&str>,
    ) -> Option<&NearSource> {
        let latitude_span = (max_distance_m / 111_320.0).ceil() as i32 + 1;
        let longitude_scale = (position.latitude_e7 as f64 / 1e7)
            .to_radians()
            .cos()
            .abs()
            .max(0.01);
        let longitude_span = (max_distance_m / (111_320.0 * longitude_scale)).ceil() as i32 + 1;
        let (center_lat, center_lon) = Self::cell(position);
        let mut nearest = None::<(&NearSource, f64)>;
        for latitude in (center_lat - latitude_span)..=(center_lat + latitude_span) {
            for longitude in (center_lon - longitude_span)..=(center_lon + longitude_span) {
                let Some(indices) = self.cells.get(&(latitude, longitude)) else {
                    continue;
                };
                for index in indices {
                    let source = &self.sources[*index];
                    if source.format != DataFormat::Rtcm3
                        || !source.usable_correction
                        || excluded == Some(source.mountpoint.as_str())
                    {
                        continue;
                    }
                    let distance = distance_m(position, source.position);
                    if distance <= max_distance_m
                        && nearest.is_none_or(|(_, current)| distance < current)
                    {
                        nearest = Some((source, distance));
                    }
                }
            }
        }
        nearest.map(|(source, _)| source)
    }

    fn cell(position: GeoPosition) -> (i32, i32) {
        let latitude = (position.latitude_e7 as f64 / 1e7 / Self::CELL_DEGREES).floor() as i32;
        let longitude = (position.longitude_e7 as f64 / 1e7 / Self::CELL_DEGREES).floor() as i32;
        (latitude, longitude)
    }
}

pub struct NearSelector {
    config: NearConfig,
    current: Option<String>,
    observation: Option<Observation>,
    gga_window: Option<GgaWindow>,
    last_gga: Option<(Instant, GeoPosition)>,
}

struct Observation {
    target: String,
    started: Instant,
    position: GeoPosition,
    previous_position: GeoPosition,
    progress_toward_target_m: f64,
}

struct GgaWindow {
    started: Instant,
    count: u32,
}

impl NearSelector {
    pub fn new(config: NearConfig) -> Self {
        Self {
            config,
            current: None,
            observation: None,
            gga_window: None,
            last_gga: None,
        }
    }

    pub fn current(&self) -> Option<&str> {
        self.current.as_deref()
    }

    pub fn should_query(
        &self,
        last: Option<(Instant, GeoPosition)>,
        gga: &NearGga,
        now: Instant,
    ) -> bool {
        let Some((last_time, last_position)) = last else {
            return true;
        };
        let elapsed = now.saturating_duration_since(last_time);
        elapsed >= self.config.min_recompute_interval
            && (elapsed >= self.config.max_recompute_interval
                || distance_m(last_position, gga.position) >= self.config.min_recompute_distance_m)
    }

    pub fn accept_gga(&mut self, gga: &NearGga, now: Instant) -> GgaAcceptance {
        let window = self.gga_window.get_or_insert(GgaWindow {
            started: now,
            count: 0,
        });
        if now.saturating_duration_since(window.started) >= Duration::from_secs(1) {
            window.started = now;
            window.count = 0;
        }
        if window.count >= self.config.gga_max_input_per_second {
            return GgaAcceptance::RateLimited;
        }
        window.count += 1;
        if !matches!(gga.fix_quality, 1 | 2 | 4 | 5) || gga.hdop > self.config.max_hdop {
            return GgaAcceptance::Invalid;
        }
        if let Some((previous_at, previous_position)) = self.last_gga {
            let elapsed = now.saturating_duration_since(previous_at).as_secs_f64();
            let speed = if elapsed > 0.0 {
                distance_m(previous_position, gga.position) / elapsed
            } else if previous_position == gga.position {
                0.0
            } else {
                f64::INFINITY
            };
            if speed > self.config.gga_max_jump_speed_mps {
                return GgaAcceptance::JumpRejected;
            }
        }
        self.last_gga = Some((now, gga.position));
        GgaAcceptance::Accepted
    }

    pub fn update(
        &mut self,
        gga: &NearGga,
        candidates: &[NearSource],
        now: Instant,
    ) -> Option<String> {
        self.update_indexed(gga, &NearIndex::new(candidates), now)
    }

    pub fn update_indexed(
        &mut self,
        gga: &NearGga,
        candidates: &NearIndex,
        now: Instant,
    ) -> Option<String> {
        let valid_gga =
            matches!(gga.fix_quality, 1 | 2 | 4 | 5) && gga.hdop <= self.config.max_hdop;
        let Some(current_name) = self.current.clone() else {
            if !valid_gga {
                return None;
            }
            return candidates
                .nearest(gga.position, self.config.max_distance_m)
                .map(|source| {
                    self.current = Some(source.mountpoint.clone());
                    source.mountpoint.clone()
                });
        };
        let Some(current) = candidates.get(&current_name).filter(|source| {
            source.mountpoint == current_name
                && source.format == DataFormat::Rtcm3
                && source.usable_correction
        }) else {
            self.observation = None;
            return candidates
                .nearest_excluding(
                    gga.position,
                    self.config.max_distance_m,
                    Some(&current_name),
                )
                .map(|source| {
                    self.current = Some(source.mountpoint.clone());
                    source.mountpoint.clone()
                });
        };
        if !valid_gga {
            return None;
        }
        let current_distance = distance_m(gga.position, current.position);
        let nearest = candidates.nearest_excluding(
            gga.position,
            self.config.max_distance_m,
            Some(&current_name),
        )?;
        let nearest_distance = distance_m(gga.position, nearest.position);
        let uncertainty = f64::from(gga.hdop) * self.config.hdop_m_per_unit;
        let guard = self
            .config
            .hysteresis_m
            .max(self.config.base_position_uncertainty_m + uncertainty);
        if nearest_distance + guard >= current_distance {
            self.observation = None;
            return None;
        }
        match &mut self.observation {
            Some(observation) if observation.target == nearest.mountpoint => {
                let previous_distance = distance_m(observation.previous_position, nearest.position);
                let current_distance = distance_m(gga.position, nearest.position);
                observation.progress_toward_target_m +=
                    (previous_distance - current_distance).max(0.0);
                observation.previous_position = gga.position;
                if now.duration_since(observation.started) < self.config.observe_time {
                    return None;
                }
                let stationary = distance_m(observation.position, gga.position)
                    <= self.config.switch_position_threshold_m;
                if !stationary
                    && observation.progress_toward_target_m
                        < self.config.switch_position_threshold_m
                {
                    self.observation = None;
                    return None;
                }
                self.current = Some(nearest.mountpoint.clone());
                self.observation = None;
                Some(nearest.mountpoint.clone())
            }
            _ => {
                self.observation = Some(Observation {
                    target: nearest.mountpoint.clone(),
                    started: now,
                    position: gga.position,
                    previous_position: gga.position,
                    progress_toward_target_m: 0.0,
                });
                None
            }
        }
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
