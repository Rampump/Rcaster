use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use rcaster_core::{
    protocol::{DataFormat, GeoPosition},
    str_record::StrRecord,
};
use rcaster_service::near::{
    GgaAcceptance, NearConfig, NearGga, NearIndex, NearQueryLimiter, NearSelector, NearSource,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

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
fn spatial_index_wraps_across_the_date_line() {
    let candidates = vec![source("WEST", 0.0, 179.99), source("EAST", 0.0, -179.99)];
    let index = NearIndex::new(&candidates);

    assert_eq!(
        index
            .nearest(position(0.0, 180.0), 5_000.0)
            .map(|source| source.mountpoint.as_str()),
        Some("EAST")
    );
}

#[test]
fn origin_is_not_a_valid_source_position() {
    let mut record =
        StrRecord::from_profile("ORIGIN", &rcaster_core::protocol::DataProfile::default());
    record.latitude = "0.0000000".to_owned();
    record.longitude = "0.0000000".to_owned();

    assert!(NearSource::from_str_record(&record).is_none());
}

#[test]
fn selection_is_not_committed_before_subscription_succeeds() {
    let mut selector = NearSelector::new(NearConfig::default());
    let now = Instant::now();
    let candidates = [source("A", 0.0, 0.001)];
    let index = NearIndex::new(&candidates);

    assert_eq!(
        selector
            .propose_update_indexed(&gga(0.0, 0.0, 0.8), &index, now)
            .as_deref(),
        Some("A")
    );
    assert_eq!(selector.current(), None);
    selector.commit("A");
    assert_eq!(selector.current(), Some("A"));
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

#[test]
fn completed_observation_triggers_a_stationary_query_before_max_interval() {
    let mut selector = NearSelector::new(NearConfig {
        observe_time: Duration::from_secs(3),
        ..NearConfig::default()
    });
    let now = Instant::now();
    let a = source("A", 0.0, 0.002);
    let b = source("B", 0.0, 0.0);
    assert_eq!(
        selector
            .update(&gga(0.0, 0.002, 0.9), &[a.clone(), b.clone()], now)
            .as_deref(),
        Some("A")
    );
    let observation_started = now + Duration::from_secs(1);
    assert_eq!(
        selector.update(&gga(0.0, 0.0, 0.9), &[a, b], observation_started),
        None
    );
    assert!(selector.should_query(
        Some((observation_started, position(0.0, 0.0))),
        &gga(0.0, 0.0, 0.9),
        observation_started + Duration::from_secs(3),
    ));
}

#[test]
fn global_query_limiter_enforces_rate_and_concurrency() {
    let limiter = NearQueryLimiter::new(1, 1).expect("query limiter");
    let now = Instant::now();
    let permit = limiter.try_acquire_at(now).expect("first query");

    assert!(limiter.try_acquire_at(now).is_none());
    drop(permit);
    assert!(
        limiter
            .try_acquire_at(now + Duration::from_millis(500))
            .is_none()
    );
    assert!(
        limiter
            .try_acquire_at(now + Duration::from_secs(1))
            .is_some()
    );
}

#[test]
fn gga_input_rate_and_jump_speed_are_bounded() {
    let mut selector = NearSelector::new(NearConfig {
        gga_max_input_per_second: 2,
        gga_max_jump_speed_mps: 100.0,
        ..NearConfig::default()
    });
    let now = Instant::now();

    assert_eq!(
        selector.accept_gga(&gga(0.0, 0.0, 0.8), now),
        GgaAcceptance::Accepted
    );
    assert_eq!(
        selector.accept_gga(&gga(0.0, 0.0, 0.8), now),
        GgaAcceptance::Accepted
    );
    assert_eq!(
        selector.accept_gga(&gga(0.0, 0.00002, 0.8), now),
        GgaAcceptance::RateLimited
    );
    assert_eq!(
        selector.accept_gga(&gga(0.0, 1.0, 0.8), now + Duration::from_secs(1)),
        GgaAcceptance::JumpRejected
    );
    assert_eq!(
        selector.accept_gga(&gga(0.0, 0.001, 0.8), now + Duration::from_secs(2)),
        GgaAcceptance::Accepted
    );
}

#[tokio::test]
#[ignore = "live external CRTK.NET sourcetable"]
async fn live_crtk_sourcetable_models_near_candidates() {
    let mut stream = TcpStream::connect("crtk.net:2101")
        .await
        .expect("connect CRTK.NET");
    stream
        .write_all(
            b"GET / HTTP/1.0\r\nHost: crtk.net:2101\r\nUser-Agent: RCaster-NEAR-validation\r\n\r\n",
        )
        .await
        .expect("request CRTK.NET sourcetable");
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(30), stream.read_to_end(&mut response))
        .await
        .expect("CRTK.NET response timeout")
        .expect("read CRTK.NET response");
    let body = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|offset| &response[offset + 4..])
        .expect("CRTK.NET HTTP body");
    let mut candidates = body
        .split(|byte| *byte == b'\n')
        .filter_map(|line| std::str::from_utf8(line).ok())
        .filter_map(parse_crtk_str_source)
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.mountpoint.cmp(&right.mountpoint));
    assert!(
        candidates.len() > 100,
        "expected a useful CRTK.NET candidate set, got {}",
        candidates.len()
    );

    let index = NearIndex::new(&candidates);
    let mut checks = 0;
    for candidate in candidates.iter().step_by((candidates.len() / 25).max(1)) {
        let expected = brute_nearest(&candidates, candidate.position, 100_000.0);
        let actual = index
            .nearest(candidate.position, 100_000.0)
            .map(|source| source.mountpoint.as_str());
        assert_eq!(actual, expected, "query at {}", candidate.mountpoint);
        checks += 1;
    }
    eprintln!(
        "CRTK.NET NEAR model: {} RTCM3 candidates, {checks} sampled queries matched brute force",
        candidates.len()
    );
}

#[test]
#[ignore = "offline CRTK.NET snapshot and synthetic motion paths"]
fn offline_snapshot_matches_full_scan_and_stationary_arrival_switches() {
    let candidates = load_offline_snapshot();
    assert!(candidates.len() > 100, "offline snapshot is too small");
    let index = NearIndex::new(&candidates);
    let mut random = Lcg::new(0x5EED_C0DE_2026_1125);
    for sample in 0..200 {
        let anchor = &candidates[random.next_usize(candidates.len())];
        let latitude =
            f64::from(anchor.position.latitude_e7) / 1e7 + (random.next_f64() - 0.5) * 0.6;
        let longitude =
            f64::from(anchor.position.longitude_e7) / 1e7 + (random.next_f64() - 0.5) * 0.6;
        let query = position(latitude, longitude);
        let expected = brute_nearest_distance(&candidates, query, 100_000.0);
        let actual = index.nearest(query, 100_000.0).map(|source| {
            (
                source.mountpoint.as_str(),
                haversine_m(query, source.position),
            )
        });
        match (expected, actual) {
            (None, None) => {}
            (Some((expected_name, expected_distance)), Some((actual_name, actual_distance))) => {
                assert!(
                    (expected_distance - actual_distance).abs() < 0.01,
                    "sample {sample}: indexed distance differs from full scan"
                );
                if expected_name != actual_name {
                    assert!(
                        expected_distance + 0.01 >= actual_distance
                            && actual_distance + 0.01 >= expected_distance,
                        "sample {sample}: expected {expected_name}, got {actual_name}"
                    );
                }
            }
            (expected, actual) => panic!("sample {sample}: expected {expected:?}, got {actual:?}"),
        }
    }

    for path in 0..10 {
        let distance_m = 1_000.0 + path as f64 * 350.0;
        let bearing = path as f64 * std::f64::consts::TAU / 10.0;
        let a = position(30.0, 120.0);
        let b = offset_position(a, distance_m, bearing);
        let candidates = vec![
            NearSource {
                mountpoint: "A".to_owned(),
                position: a,
                format: DataFormat::Rtcm3,
                usable_correction: true,
            },
            NearSource {
                mountpoint: "B".to_owned(),
                position: b,
                format: DataFormat::Rtcm3,
                usable_correction: true,
            },
        ];
        let index = NearIndex::new(&candidates);
        let mut selector = NearSelector::new(NearConfig::default());
        let start = Instant::now();
        assert_eq!(
            selector
                .update_indexed(&gga(30.0, 120.0, 0.9), &index, start)
                .as_deref(),
            Some("A")
        );
        let mut switches = Vec::new();
        for step in 1..=24 {
            let at = start + Duration::from_millis(250 * step);
            let fraction = step as f64 / 24.0;
            let query = interpolate_position(a, b, fraction);
            if let Some(source) = selector.update_indexed(
                &gga(
                    f64::from(query.latitude_e7) / 1e7,
                    f64::from(query.longitude_e7) / 1e7,
                    0.9,
                ),
                &index,
                at,
            ) {
                switches.push((source, at.duration_since(start)));
            }
        }
        for stable in 1..=16 {
            let at = start + Duration::from_millis(250 * (24 + stable));
            if let Some(source) = selector.update_indexed(
                &gga(
                    f64::from(b.latitude_e7) / 1e7,
                    f64::from(b.longitude_e7) / 1e7,
                    0.9,
                ),
                &index,
                at,
            ) {
                switches.push((source, at.duration_since(start)));
            }
        }
        assert_eq!(
            switches
                .iter()
                .map(|(source, _)| source.as_str())
                .collect::<Vec<_>>(),
            vec!["B"],
            "path {path}"
        );
        assert!(
            switches[0].1 >= Duration::from_secs(3),
            "path {path}: switched after {:?}",
            switches[0].1
        );
    }
}

#[test]
fn moving_gga_crossing_switches_after_progress_toward_target() {
    let a = position(30.0, 120.0);
    let b = position(30.018, 120.0);
    let candidates = vec![
        NearSource {
            mountpoint: "A".to_owned(),
            position: a,
            format: DataFormat::Rtcm3,
            usable_correction: true,
        },
        NearSource {
            mountpoint: "B".to_owned(),
            position: b,
            format: DataFormat::Rtcm3,
            usable_correction: true,
        },
    ];
    let index = NearIndex::new(&candidates);
    let mut selector = NearSelector::new(NearConfig::default());
    let start = Instant::now();
    assert_eq!(
        selector
            .update_indexed(&gga(30.0, 120.0, 0.9), &index, start)
            .as_deref(),
        Some("A")
    );

    let mut switched_at = None;
    for second in 1..=300 {
        let latitude = 30.0 + 8.0 * second as f64 / 111_320.0;
        if latitude > 30.018 + 0.002 {
            break;
        }
        if let Some(source) = selector.update_indexed(
            &gga(latitude, 120.0, 0.9),
            &index,
            start + Duration::from_secs(second),
        ) {
            assert_eq!(source, "B");
            assert!(switched_at.replace(second).is_none());
        }
    }
    let switched_at = switched_at.expect("moving client switched source");
    assert!(switched_at >= 3, "switch occurred before observation");
    assert!(switched_at <= 140, "switch took {switched_at} seconds");
}

fn load_offline_snapshot() -> Vec<NearSource> {
    let path = std::env::var_os("RCASTER_NEAR_E2E_SNAPSHOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../tools/near-e2e/data/crtk-str-20261125.json")
        });
    let value: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
    )
    .expect("parse CRTK snapshot");
    let mut candidates = value["records"]
        .as_array()
        .expect("snapshot records")
        .iter()
        .filter_map(|record| {
            NearSource::from_str_record(&StrRecord {
                mountpoint: record["mountpoint"].as_str()?.to_owned(),
                identifier: record["identifier"].as_str().unwrap_or_default().to_owned(),
                format: record["format"].as_str()?.to_owned(),
                format_details: String::new(),
                carrier: String::new(),
                navigation_system: String::new(),
                network: record["network"].as_str().unwrap_or_default().to_owned(),
                country: record["country"].as_str().unwrap_or_default().to_owned(),
                latitude: record["latitude"].to_string(),
                longitude: record["longitude"].to_string(),
                nmea: String::new(),
                solution: String::new(),
                generator: String::new(),
                compression: String::new(),
                authentication: String::new(),
                fee: String::new(),
                bitrate: String::new(),
                misc: String::new(),
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.mountpoint.cmp(&right.mountpoint));
    candidates
}

fn brute_nearest_distance(
    candidates: &[NearSource],
    query: GeoPosition,
    max_distance_m: f64,
) -> Option<(&str, f64)> {
    candidates
        .iter()
        .filter(|source| source.usable_correction && source.format == DataFormat::Rtcm3)
        .filter_map(|source| {
            let distance = haversine_m(query, source.position);
            (distance <= max_distance_m).then_some((source.mountpoint.as_str(), distance))
        })
        .min_by(|left, right| left.1.total_cmp(&right.1))
}

fn offset_position(origin: GeoPosition, distance_m: f64, bearing_radians: f64) -> GeoPosition {
    let latitude = f64::from(origin.latitude_e7) / 1e7;
    let longitude = f64::from(origin.longitude_e7) / 1e7;
    let north = distance_m * bearing_radians.cos() / 111_320.0;
    let east = distance_m * bearing_radians.sin() / (111_320.0 * latitude.to_radians().cos());
    position(latitude + north, longitude + east)
}

fn interpolate_position(start: GeoPosition, end: GeoPosition, fraction: f64) -> GeoPosition {
    let interpolate = |left: i32, right: i32| {
        (f64::from(left) + (f64::from(right) - f64::from(left)) * fraction).round() as i32
    };
    GeoPosition {
        latitude_e7: interpolate(start.latitude_e7, end.latitude_e7),
        longitude_e7: interpolate(start.longitude_e7, end.longitude_e7),
        height_cm: 0,
    }
}

struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn next_usize(&mut self, upper: usize) -> usize {
        (self.next_u64() % upper as u64) as usize
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / ((1_u64 << 53) as f64)
    }
}

fn parse_crtk_str_source(line: &str) -> Option<NearSource> {
    let fields = line.trim_end_matches('\r').split(';').collect::<Vec<_>>();
    if fields.len() < 11 || fields[0] != "STR" || fields[3] != "RTCM3" || fields[1].is_empty() {
        return None;
    }
    let field = |index: usize| fields.get(index).copied().unwrap_or_default().to_owned();
    NearSource::from_str_record(&StrRecord {
        mountpoint: field(1),
        identifier: field(2),
        format: field(3),
        format_details: field(4),
        carrier: field(5),
        navigation_system: field(6),
        network: field(7),
        country: field(8),
        latitude: field(9),
        longitude: field(10),
        nmea: field(11),
        solution: field(12),
        generator: field(13),
        compression: field(14),
        authentication: field(15),
        fee: field(16),
        bitrate: field(17),
        misc: field(18),
    })
}

fn brute_nearest(
    candidates: &[NearSource],
    query: GeoPosition,
    max_distance_m: f64,
) -> Option<&str> {
    candidates
        .iter()
        .filter(|source| source.usable_correction && source.format == DataFormat::Rtcm3)
        .filter_map(|source| {
            let distance = haversine_m(query, source.position);
            (distance <= max_distance_m).then_some((source, distance))
        })
        .min_by(|left, right| left.1.total_cmp(&right.1))
        .map(|(source, _)| source.mountpoint.as_str())
}

fn haversine_m(left: GeoPosition, right: GeoPosition) -> f64 {
    let lat1 = f64::from(left.latitude_e7) / 1e7 * std::f64::consts::PI / 180.0;
    let lat2 = f64::from(right.latitude_e7) / 1e7 * std::f64::consts::PI / 180.0;
    let dlat = lat2 - lat1;
    let dlon = (f64::from(right.longitude_e7) - f64::from(left.longitude_e7)) / 1e7
        * std::f64::consts::PI
        / 180.0;
    let value = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * 6_371_000.0 * value.sqrt().asin()
}
