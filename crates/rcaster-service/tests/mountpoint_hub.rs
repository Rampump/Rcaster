use std::sync::Arc;

use rcaster_core::{
    protocol::{DataProfile, GeoPosition, ProtocolDemux},
    str_record::StrRecord,
};
use rcaster_service::hub::{
    BASE_POSITION_JUMP_WARNING, HIGH_CRC_WARNING, HealthThresholds, HubError, HubLimits,
    LOW_VALID_FRAME_RATIO_WARNING, MountpointHub, NO_CORRECTION_WARNING,
    STATION_ID_CHANGED_WARNING,
};

#[tokio::test]
async fn one_source_fans_out_shared_packets_to_subscribers() {
    let hub = MountpointHub::new(16).expect("hub");
    let source = hub.claim_source("C63").expect("source claim");
    activate(&source);
    let mut first = hub.subscribe("C63").expect("first subscriber");
    let mut second = hub.subscribe("C63").expect("second subscriber");
    let packet: Arc<[u8]> = Arc::from(&b"\xD3\x00\x00\x00"[..]);

    source.publish(packet.clone()).expect("publish");
    let health = hub.source_health("C63").expect("source health");
    assert_eq!(health.packets, 1);
    assert_eq!(health.bytes, 4);
    assert_eq!(health.last_packet_age_seconds, Some(0));
    assert_eq!(health.bitrate_bps, None);
    assert_eq!(health.warning_codes, 0);
    assert_eq!(health.last_correction_age_seconds, None);

    let first_packet = first.recv().await.expect("first packet");
    let second_packet = second.recv().await.expect("second packet");
    assert!(Arc::ptr_eq(&packet, &first_packet));
    assert!(Arc::ptr_eq(&packet, &second_packet));
}

#[tokio::test]
async fn source_health_waits_for_observation_timeout() {
    let hub = MountpointHub::new_with_health(
        16,
        HealthThresholds {
            observation_timeout: std::time::Duration::from_millis(30),
            ..HealthThresholds::default()
        },
    )
    .expect("hub");
    let source = hub.claim_source("C63").expect("source");
    activate(&source);
    assert_eq!(
        hub.source_health("C63").expect("health").warning_codes & NO_CORRECTION_WARNING,
        0
    );

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(
        hub.source_health("C63").expect("health").warning_codes & NO_CORRECTION_WARNING,
        NO_CORRECTION_WARNING
    );
}

#[tokio::test]
async fn rejects_a_second_source_until_the_first_disconnects() {
    let hub = MountpointHub::new(16).expect("hub");
    let first = hub.claim_source("C63").expect("first source");

    assert!(matches!(
        hub.claim_source("C63"),
        Err(HubError::SourceAlreadyActive)
    ));

    drop(first);
    hub.claim_source("C63").expect("replacement source");
}

#[tokio::test]
async fn source_disconnect_closes_subscriber_channels() {
    let hub = MountpointHub::new(16).expect("hub");
    let source = hub.claim_source("C63").expect("source");
    activate(&source);
    let mut subscriber = hub.subscribe("C63").expect("subscriber");

    drop(source);

    assert!(matches!(
        subscriber.recv().await,
        Err(tokio::sync::broadcast::error::RecvError::Closed)
    ));
}

#[tokio::test]
async fn local_source_replacement_changes_the_generation() {
    let hub = MountpointHub::new(16).expect("hub");
    let first = hub.claim_source("C63").expect("first source");
    activate(&first);
    let first_generation = hub.source_generation("C63").expect("first generation");
    assert_eq!(first.generation(), first_generation);
    drop(first);

    let second = hub.claim_source("C63").expect("second source");
    activate(&second);
    let second_generation = hub.source_generation("C63").expect("second generation");
    assert_ne!(first_generation, second_generation);
}

#[tokio::test]
async fn slow_subscriber_observes_bounded_lag() {
    let hub = MountpointHub::new(2).expect("hub");
    let source = hub.claim_source("C63").expect("source");
    activate(&source);
    let mut subscriber = hub.subscribe("C63").expect("subscriber");

    for value in 0..3_u8 {
        source.publish(Arc::from([value])).expect("publish");
    }

    assert!(matches!(
        subscriber.recv().await,
        Err(tokio::sync::broadcast::error::RecvError::Lagged(1))
    ));
}

#[tokio::test]
async fn source_health_reports_bitrate_window() {
    let hub = MountpointHub::new(16).expect("hub");
    let source = hub.claim_source("C63").expect("source");
    activate(&source);
    source
        .publish(Arc::from(vec![0_u8; 100]))
        .expect("first packet");
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    source
        .publish_correction(Arc::from(vec![0_u8; 100]))
        .expect("second packet");

    assert!(
        hub.source_health("C63")
            .expect("source health")
            .bitrate_bps
            .is_some_and(|bitrate| bitrate > 0)
    );
    assert_eq!(hub.source_health("C63").expect("health").warning_codes, 0);
    assert_eq!(
        hub.source_health("C63")
            .expect("health")
            .last_correction_age_seconds,
        Some(0)
    );
}

#[test]
fn health_history_is_bounded_and_returns_latest_points() {
    let hub = MountpointHub::new(16).expect("hub");
    let source = hub.claim_source("C63").expect("source");
    activate(&source);
    source.publish(Arc::from([1_u8, 2, 3])).expect("packet");
    hub.record_health_snapshots();
    hub.record_health_snapshots();

    let history = hub.source_history("C63", 1);
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].packets, 1);
    assert_eq!(history[0].bytes, 3);
}

#[test]
fn hub_metrics_aggregate_sources_subscribers_and_integrity() {
    let hub = MountpointHub::new(16).expect("hub");
    let source = hub.claim_source("C63").expect("source");
    activate(&source);
    let _subscriber = hub.subscribe("C63").expect("subscriber");
    source
        .set_profile(DataProfile {
            valid_frames: 2,
            crc_errors: 1,
            ..DataProfile::default()
        })
        .expect("profile");
    source.publish(Arc::from([1_u8, 2, 3])).expect("packet");

    let metrics = hub.metrics();
    assert_eq!(metrics.active_sources, 1);
    assert_eq!(metrics.subscribers, 1);
    assert_eq!(metrics.packets, 1);
    assert_eq!(metrics.bytes, 3);
    assert_eq!(metrics.valid_frames, 2);
    assert_eq!(metrics.crc_errors, 1);
    assert_eq!(metrics.parser_framing_errors, 0);
    assert_eq!(metrics.parser_resynchronizations, 0);
    assert_eq!(metrics.parser_dropped_bytes, 0);
    assert_eq!(metrics.parser_max_pending_bytes, 0);
    assert_eq!(metrics.relay_slow_consumers, 0);
}

#[test]
fn hub_metrics_include_parser_and_relay_slow_consumer_counters() {
    let hub = MountpointHub::new(16).expect("hub");
    let source = hub.claim_source("C63").expect("source");
    activate(&source);
    source
        .set_profile(DataProfile {
            parser_framing_errors: 2,
            parser_resynchronizations: 3,
            parser_dropped_bytes: 4,
            parser_max_pending_bytes: 5,
            ..DataProfile::default()
        })
        .expect("profile");
    hub.record_relay_slow_consumer();

    let metrics = hub.metrics();
    assert_eq!(metrics.parser_framing_errors, 2);
    assert_eq!(metrics.parser_resynchronizations, 3);
    assert_eq!(metrics.parser_dropped_bytes, 4);
    assert_eq!(metrics.parser_max_pending_bytes, 5);
    assert_eq!(metrics.relay_slow_consumers, 1);
}

#[test]
fn hub_metrics_include_source_health_and_disconnect_counters() {
    let hub = MountpointHub::new(16).expect("hub");
    let source = hub.claim_source("C63").expect("source");
    activate(&source);
    source
        .set_profile(DataProfile {
            station_id: Some(1001),
            position: Some(position(30.0, 120.0)),
            ..DataProfile::default()
        })
        .expect("first profile");
    source
        .set_profile(DataProfile {
            station_id: Some(1002),
            position: Some(position(30.01, 120.01)),
            ..DataProfile::default()
        })
        .expect("changed profile");
    hub.record_health_snapshots();
    hub.record_backlog_kick();

    let metrics = hub.metrics();
    assert_eq!(metrics.station_id_changes, 1);
    assert_eq!(metrics.base_position_jumps, 1);
    assert_eq!(metrics.backlog_kicks, 1);
    assert_eq!(metrics.source_health_warning_sources, 1);
    assert_eq!(metrics.station_id_warning_sources, 1);
    assert_eq!(metrics.base_position_jump_sources, 1);
}

#[tokio::test]
async fn source_health_warns_on_high_crc_ratio() {
    let hub = MountpointHub::new(16).expect("hub");
    let source = hub.claim_source("C63").expect("source");
    activate(&source);
    source
        .set_profile(DataProfile {
            crc_errors: 2,
            ..DataProfile::default()
        })
        .expect("profile");
    source
        .publish_correction(Arc::from([1_u8]))
        .expect("packet");

    assert_eq!(
        hub.source_health("C63").expect("health").warning_codes & HIGH_CRC_WARNING,
        HIGH_CRC_WARNING
    );
}

#[tokio::test]
async fn source_health_uses_configured_crc_threshold() {
    let hub = MountpointHub::new_with_health(
        16,
        HealthThresholds {
            max_crc_ratio: 0.4,
            valid_frame_ratio_min: 0.0,
            alert_clear: std::time::Duration::from_millis(10),
            ..HealthThresholds::default()
        },
    )
    .expect("hub");
    let source = hub.claim_source("C63").expect("source");
    activate(&source);
    source
        .set_profile(DataProfile {
            valid_frames: 1,
            crc_errors: 1,
            ..DataProfile::default()
        })
        .expect("profile");
    source
        .publish_correction(Arc::from([1_u8]))
        .expect("packet");

    assert_eq!(
        hub.source_health("C63").expect("health").warning_codes & HIGH_CRC_WARNING,
        HIGH_CRC_WARNING
    );

    hub.set_health_thresholds(HealthThresholds {
        max_crc_ratio: 0.75,
        valid_frame_ratio_min: 0.0,
        alert_clear: std::time::Duration::from_millis(10),
        ..HealthThresholds::default()
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(
        hub.source_health("C63").expect("health").warning_codes & HIGH_CRC_WARNING,
        0
    );
}

#[test]
fn source_health_warns_when_station_id_changes() {
    let hub = MountpointHub::new(16).expect("hub");
    let source = hub.claim_source("C63").expect("source");
    activate(&source);
    source
        .set_profile(DataProfile {
            station_id: Some(1001),
            position: Some(position(30.0, 120.0)),
            ..DataProfile::default()
        })
        .expect("first station profile");
    source
        .set_profile(DataProfile {
            station_id: Some(1002),
            position: Some(position(30.0, 120.0)),
            ..DataProfile::default()
        })
        .expect("second station profile");

    assert_eq!(
        hub.source_health("C63").expect("health").warning_codes & STATION_ID_CHANGED_WARNING,
        STATION_ID_CHANGED_WARNING
    );
}

#[test]
fn source_health_warns_when_base_position_jumps() {
    let hub = MountpointHub::new(16).expect("hub");
    let source = hub.claim_source("C63").expect("source");
    activate(&source);
    source
        .set_profile(DataProfile {
            station_id: Some(1001),
            position: Some(position(30.0, 120.0)),
            ..DataProfile::default()
        })
        .expect("first position");
    source
        .set_profile(DataProfile {
            station_id: Some(1001),
            position: Some(position(30.01, 120.01)),
            ..DataProfile::default()
        })
        .expect("jumped position");

    assert_eq!(
        hub.source_health("C63").expect("health").warning_codes & BASE_POSITION_JUMP_WARNING,
        BASE_POSITION_JUMP_WARNING
    );
}

#[test]
fn source_health_warns_on_low_valid_frame_ratio() {
    let hub = MountpointHub::new(16).expect("hub");
    let source = hub.claim_source("C63").expect("source");
    activate(&source);
    source
        .set_profile(DataProfile {
            valid_frames: 1,
            crc_errors: 3,
            ..DataProfile::default()
        })
        .expect("profile");
    source
        .publish_correction(Arc::from([1_u8]))
        .expect("packet");

    let health = hub.source_health("C63").expect("health");
    assert_eq!(
        health.warning_codes & LOW_VALID_FRAME_RATIO_WARNING,
        LOW_VALID_FRAME_RATIO_WARNING
    );
    assert_eq!(health.valid_frame_ratio, Some(0.25));
}

#[tokio::test]
async fn source_health_clears_after_configured_stable_period() {
    let hub = MountpointHub::new_with_health(
        16,
        HealthThresholds {
            alert_clear: std::time::Duration::from_millis(50),
            ..HealthThresholds::default()
        },
    )
    .expect("hub");
    let source = hub.claim_source("C63").expect("source");
    activate(&source);
    source
        .set_profile(DataProfile {
            station_id: Some(1001),
            ..DataProfile::default()
        })
        .expect("first profile");
    source
        .set_profile(DataProfile {
            station_id: Some(1002),
            ..DataProfile::default()
        })
        .expect("changed profile");
    source
        .publish_correction(Arc::from([1_u8]))
        .expect("correction");
    assert_eq!(
        hub.source_health("C63").expect("health").warning_codes & STATION_ID_CHANGED_WARNING,
        STATION_ID_CHANGED_WARNING
    );

    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    source
        .publish_correction(Arc::from([2_u8]))
        .expect("normal correction");
    assert_eq!(
        hub.source_health("C63").expect("health").warning_codes & STATION_ID_CHANGED_WARNING,
        0
    );
}

#[test]
fn disabled_source_health_keeps_metrics_without_warnings() {
    let hub = MountpointHub::new_with_health(
        16,
        HealthThresholds {
            enabled: false,
            ..HealthThresholds::default()
        },
    )
    .expect("hub");
    let source = hub.claim_source("C63").expect("source");
    activate(&source);
    source
        .set_profile(DataProfile {
            valid_frames: 1,
            crc_errors: 5,
            ..DataProfile::default()
        })
        .expect("profile");
    source
        .publish_correction(Arc::from([1_u8]))
        .expect("packet");

    let health = hub.source_health("C63").expect("health");
    assert_eq!(health.warning_codes, 0);
    assert_eq!(health.packets, 1);
    assert_eq!(health.valid_frame_ratio, Some(1.0 / 6.0));
}

#[test]
fn source_bitrate_limit_rejects_excess_data() {
    let hub = MountpointHub::new_with_health(
        16,
        HealthThresholds {
            max_source_bitrate_bps: 8,
            ..HealthThresholds::default()
        },
    )
    .expect("hub");
    let source = hub.claim_source("C63").expect("source");
    activate(&source);
    assert!(matches!(
        source.publish(Arc::from([1_u8, 2])),
        Err(HubError::SourceBitrateExceeded)
    ));
}

#[test]
fn hub_enforces_source_and_subscriber_limits() {
    let hub = MountpointHub::new(16).expect("hub");
    hub.set_limits(HubLimits {
        max_sources: 1,
        max_validating_sources: 1,
        max_subscribers: 1,
        max_subscribers_per_source: 1,
    });
    let first = hub.claim_source("C63").expect("first source");
    activate(&first);
    assert!(matches!(
        hub.claim_source("C64"),
        Err(HubError::SourceLimitReached)
    ));
    let _subscriber = hub.subscribe("C63").expect("first subscriber");
    assert!(matches!(
        hub.subscribe("C63"),
        Err(HubError::SubscriberLimitReached)
    ));
}

#[test]
fn hub_limits_sources_waiting_for_admission() {
    let hub = MountpointHub::new(16).expect("hub");
    hub.set_limits(HubLimits {
        max_sources: 2,
        max_validating_sources: 1,
        max_subscribers: 1,
        max_subscribers_per_source: 1,
    });
    let first = hub
        .claim_validating_source("C63")
        .expect("first validating source");
    assert!(matches!(
        hub.claim_validating_source("C64"),
        Err(HubError::ValidatingSourceLimitReached)
    ));
    activate(&first);
    hub.claim_validating_source("C64")
        .expect("source after admission releases validating capacity");
}

#[test]
fn validating_source_is_neither_discoverable_nor_subscribable() {
    let hub = MountpointHub::new(16).expect("hub");
    let source = hub.claim_source("C63").expect("source claim");

    assert!(hub.sourcetable_records().is_empty());
    assert!(matches!(
        hub.subscribe("C63"),
        Err(HubError::SourceNotFound)
    ));
    assert!(matches!(
        source.publish(Arc::from([1_u8])),
        Err(HubError::SourceNotFound)
    ));

    activate(&source);
    assert_eq!(hub.sourcetable_records()[0].mountpoint, "C63");
    hub.subscribe("C63").expect("active subscriber");
}

#[test]
fn caches_latest_station_position_frames_for_near_switches() {
    let hub = MountpointHub::new(16).expect("hub");
    let source = hub.claim_source("C63").expect("source claim");
    activate(&source);
    let mut demux = ProtocolDemux::new();
    let frame = synthetic_rtcm3(1006);

    source
        .publish_analyzed(Arc::from(frame.clone()), &mut demux)
        .expect("analyzed frame");

    let cached = hub.station_frames("C63");
    assert_eq!(cached.len(), 1);
    assert_eq!(&*cached[0], frame.as_slice());
    assert!(
        hub.source_profile("C63")
            .expect("live profile")
            .message_numbers
            .contains(&1006)
    );
}

#[test]
fn rejects_invalid_capacity_and_mountpoint() {
    assert!(matches!(
        MountpointHub::new(0),
        Err(HubError::InvalidCapacity)
    ));

    let hub = MountpointHub::new(16).expect("hub");
    assert!(matches!(
        hub.claim_source("../C63"),
        Err(HubError::InvalidMountpoint)
    ));
    assert!(matches!(
        hub.subscribe("missing"),
        Err(HubError::SourceNotFound)
    ));
}

fn activate(source: &rcaster_service::hub::SourceLease) {
    source
        .set_str_record(StrRecord::from_profile(
            source.mountpoint(),
            &DataProfile::default(),
        ))
        .expect("activate source");
}

fn position(latitude: f64, longitude: f64) -> GeoPosition {
    GeoPosition {
        latitude_e7: (latitude * 1e7).round() as i32,
        longitude_e7: (longitude * 1e7).round() as i32,
        height_cm: 0,
    }
}

fn synthetic_rtcm3(message_number: u16) -> Vec<u8> {
    let payload = [
        (message_number >> 4) as u8,
        ((message_number & 0x0F) as u8) << 4,
    ];
    let mut frame = vec![0xD3, 0, payload.len() as u8];
    frame.extend_from_slice(&payload);
    let crc = crc24q(&frame);
    frame.extend_from_slice(&crc.to_be_bytes()[1..]);
    frame
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
