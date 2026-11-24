use std::{
    collections::BTreeMap,
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
        }
    }
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
        if !record.format.starts_with("RTCM 3") {
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
}

struct Observation {
    target: String,
    started: Instant,
    position: GeoPosition,
}

impl NearSelector {
    pub fn new(config: NearConfig) -> Self {
        Self {
            config,
            current: None,
            observation: None,
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
                if now.duration_since(observation.started) < self.config.observe_time {
                    return None;
                }
                if distance_m(observation.position, gga.position)
                    > self.config.switch_position_threshold_m
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
