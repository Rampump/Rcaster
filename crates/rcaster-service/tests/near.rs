use std::time::{Duration, Instant};

use rcaster_core::{
    protocol::{DataFormat, GeoPosition},
    str_record::StrRecord,
};
use rcaster_service::near::{NearConfig, NearGga, NearIndex, NearSelector, NearSource};

fn position(latitude: f64, longitude: f64) -> GeoPosition {
    GeoPosition {
        latitude_e7: (latitude * 1e7).round() as i32,
        longitude_e7: (longitude * 1e7).round() as i32,
        height_cm: 0,
    }
}

fn source(name: &str, latitude: f64, longitude: f64) -> NearSource {
    NearSource {
        mountpoint: name.to_owned(),
        position: position(latitude, longitude),
        format: DataFormat::Rtcm3,
        usable_correction: true,
    }
}

fn gga(latitude: f64, longitude: f64, hdop: f32) -> NearGga {
    NearGga {
        position: position(latitude, longitude),
        fix_quality: 1,
        hdop,
    }
}

#[test]
fn selects_nearest_usable_rtcm3_source() {
    let mut selector = NearSelector::new(NearConfig::default());
    let now = Instant::now();
    let candidates = [
        source("FAR", 0.0, 0.01),
        source("NEAR", 0.0, 0.001),
        NearSource {
            format: DataFormat::Cmr,
            ..source("CMR", 0.0, 0.0001)
        },
        NearSource {
            usable_correction: false,
            ..source("BAD", 0.0, 0.0001)
        },
    ];

    assert_eq!(
        selector
            .update(&gga(0.0, 0.0, 0.8), &candidates, now)
            .as_deref(),
        Some("NEAR")
    );
}

#[test]
fn builds_candidate_from_rtcm3_str_position() {
    let profile = rcaster_core::protocol::DataProfile {
        primary_format: Some(DataFormat::Rtcm3),
        has_correction_data: true,
        position: Some(position(31.2, 121.4)),
        ..rcaster_core::protocol::DataProfile::default()
    };
    let record = StrRecord::from_profile("C63", &profile);
    let candidate = NearSource::from_str_record(&record).expect("NEAR candidate");

    assert_eq!(candidate.mountpoint, "C63");
    assert_eq!(candidate.format, DataFormat::Rtcm3);
    assert_eq!(candidate.position.latitude_e7, 312_000_000);
    assert_eq!(candidate.position.longitude_e7, 1_214_000_000);
}

#[test]
fn spatial_index_matches_full_scan() {
    let candidates = vec![
        source("A", 30.0, 120.0),
        source("B", 31.0, 121.0),
        source("C", 30.8, 120.8),
    ];
    let index = NearIndex::new(&candidates);

    assert_eq!(
        index
            .nearest(position(30.85, 120.85), 100_000.0)
            .map(|source| source.mountpoint.as_str()),
        Some("C")
    );
}

#[test]
fn hysteresis_prevents_nearby_source_flapping() {
    let mut selector = NearSelector::new(NearConfig::default());
    let now = Instant::now();
    let a = source("A", 0.0, 0.001);
    let b = source("B", 0.0, 0.00099);

    assert_eq!(
        selector
            .update(&gga(0.0, 0.001, 0.8), &[a.clone(), b.clone()], now)
            .as_deref(),
        Some("A")
    );
    assert_eq!(
        selector.update(&gga(0.0, 0.0, 0.8), &[a, b], now + Duration::from_secs(1)),
        None
    );
}

#[test]
fn better_source_switches_after_stable_observation_window() {
    let config = NearConfig {
        observe_time: Duration::from_secs(3),
        ..NearConfig::default()
    };
    let mut selector = NearSelector::new(config);
    let now = Instant::now();
    let a = source("A", 0.0, 0.002);
    let b = source("B", 0.0, 0.0004);

    assert_eq!(
        selector
            .update(&gga(0.0, 0.002, 0.8), &[a.clone(), b.clone()], now,)
            .as_deref(),
        Some("A")
    );
    assert_eq!(
        selector.update(
            &gga(0.0, 0.0, 0.8),
            &[a.clone(), b.clone()],
            now + Duration::from_secs(1)
        ),
        None
    );
    assert_eq!(
        selector
            .update(&gga(0.0, 0.0, 0.8), &[a, b], now + Duration::from_secs(4),)
            .as_deref(),
        Some("B")
    );
}

#[test]
fn high_hdop_and_invalid_fix_do_not_switch() {
    let mut selector = NearSelector::new(NearConfig::default());
    let now = Instant::now();
    let a = source("A", 0.0, 0.002);
    let b = source("B", 0.0, 0.0004);
    assert_eq!(
        selector
            .update(&gga(0.0, 0.002, 0.8), &[a.clone(), b.clone()], now,)
            .as_deref(),
        Some("A")
    );

    assert_eq!(
        selector.update(
            &gga(0.0, 0.0, 9.0),
            &[a.clone(), b.clone()],
            now + Duration::from_secs(1)
        ),
        None
    );
    assert_eq!(
        selector.update(
            &NearGga {
                fix_quality: 0,
                ..gga(0.0, 0.0, 0.8)
            },
            &[a, b],
            now + Duration::from_secs(4)
        ),
        None
    );
}

#[test]
fn query_rate_limits_until_movement_or_max_interval() {
    let selector = NearSelector::new(NearConfig::default());
    let now = Instant::now();
    let last = (now, position(0.0, 0.0));

    assert!(!selector.should_query(
        Some(last),
        &gga(0.0, 0.00001, 0.8),
        now + Duration::from_millis(500)
    ));
    assert!(selector.should_query(
        Some(last),
        &gga(0.0, 0.0001, 0.8),
        now + Duration::from_millis(1200)
    ));
    assert!(selector.should_query(
        Some(last),
        &gga(0.0, 0.0, 0.8),
        now + Duration::from_secs(5)
    ));
}
