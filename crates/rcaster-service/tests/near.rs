use std::time::{Duration, Instant};

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
