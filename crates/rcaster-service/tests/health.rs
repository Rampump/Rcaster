use std::sync::Arc;

use rcaster_core::protocol::{AdmissionViolation, DataFormat};
use rcaster_service::health::{HealthState, serve_health, serve_health_with_token};
#[cfg(target_os = "linux")]
use rcaster_service::health::{clock_health, process_usage};
use rcaster_service::hub::HubMetrics;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

#[cfg(target_os = "linux")]
#[test]
fn linux_process_usage_reports_rss_and_fds() {
    let (rss, fds) = process_usage().expect("Linux process usage");
    assert!(rss > 0);
    assert!(fds > 0);
}

#[cfg(target_os = "linux")]
#[test]
fn linux_clock_health_reports_kernel_offset() {
    let health = clock_health().expect("Linux clock health");
    assert!(health.offset_seconds.is_finite());
    assert!(health.offset_seconds.abs() < 1.0);
}

#[tokio::test]
async fn live_is_independent_and_ready_reflects_state() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("health listener");
    let address = listener.local_addr().expect("address");
    let state = Arc::new(HealthState::default());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_health(listener, Arc::clone(&state), async {
        let _ = shutdown_rx.await;
    }));

    assert!(
        request(address, "/health/live")
            .await
            .starts_with("HTTP/1.1 200 OK")
    );
    assert!(
        request(address, "/health/ready")
            .await
            .starts_with("HTTP/1.1 503")
    );
    state.set_ready(true);
    state.set_counts(3, 2);
    state.set_connections(7);
    state.set_tls_certificate_expiry(Some(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + 100,
    ));
    state.set_hub_metrics(HubMetrics {
        active_sources: 3,
        subscribers: 5,
        packets: 11,
        bytes: 1024,
        valid_frames: 9,
        crc_errors: 2,
        rtcm3_sources: 2,
        rtcm2_sources: 1,
        binex_sources: 0,
        cmr_sources: 0,
        nmea_sources: 0,
        unknown_sources: 0,
    });
    state.set_sessions(4);
    state.set_config_generation(7);
    state.set_auth_failure_fallback(3);
    state.set_process_usage(Some((123_456, 42)));
    state.set_dependency_health([true, true, false, true, false]);
    state.source_accepted();
    state.source_rejected();
    state.source_rejected_with_reasons(&[
        AdmissionViolation::NoValidFrames,
        AdmissionViolation::FormatNotAllowed(DataFormat::Nmea),
        AdmissionViolation::ExpectedFormatMismatch(DataFormat::Nmea),
        AdmissionViolation::MissingCorrectionData,
        AdmissionViolation::NoObservationOrSsr,
        AdmissionViolation::MixedCorrectionFormats,
        AdmissionViolation::CrcThresholdExceeded,
        AdmissionViolation::BasePositionMissing,
        AdmissionViolation::MsmRequiredMissing,
        AdmissionViolation::MsmVersionNotAllowed(7),
        AdmissionViolation::UnknownDataRatioExceeded,
    ]);
    assert!(
        request(address, "/health/ready")
            .await
            .starts_with("HTTP/1.1 200 OK")
    );
    let metrics = request(address, "/metrics").await;
    assert!(metrics.contains("rcaster_ready 1"));
    assert!(metrics.contains("rcaster_active_sources 3"));
    assert!(metrics.contains("rcaster_relays 2"));
    assert!(metrics.contains("rcaster_subscribers 5"));
    assert!(metrics.contains("rcaster_connections 7"));
    assert!(!metrics.contains("rcaster_tls_certificate_expires_seconds NaN"));
    assert!(metrics.contains("rcaster_sessions 4"));
    assert!(metrics.contains("rcaster_source_packets_total 11"));
    assert!(metrics.contains("rcaster_source_bytes_total 1024"));
    assert!(metrics.contains("rcaster_source_valid_frames_total 9"));
    assert!(metrics.contains("rcaster_source_crc_errors_total 2"));
    assert!(metrics.contains("rcaster_sources_by_format{format=\"rtcm3\"} 2"));
    assert!(metrics.contains("rcaster_sources_by_format{format=\"rtcm2\"} 1"));
    assert!(metrics.contains("rcaster_config_generation 7"));
    assert!(metrics.contains("rcaster_auth_failure_fallback_total 3"));
    assert!(metrics.contains("rcaster_process_resident_memory_bytes 123456"));
    assert!(metrics.contains("rcaster_process_open_fds 42"));
    assert!(metrics.contains("rcaster_node_clock_offset_seconds"));
    assert!(metrics.contains("rcaster_node_clock_synchronized"));
    assert!(metrics.contains("kind=\"lease\"} 1"));
    assert!(metrics.contains("kind=\"blacklist\"} 0"));
    assert!(metrics.contains("kind=\"accounts\"} 0"));
    assert!(metrics.contains("result=\"accepted\"} 1"));
    assert!(metrics.contains("result=\"rejected\"} 2"));
    assert!(metrics.contains("reason=\"no_valid_frames\"} 1"));
    assert!(metrics.contains("reason=\"format_not_allowed\"} 1"));
    assert!(metrics.contains("reason=\"no_observation_or_ssr\"} 1"));
    assert!(metrics.contains("reason=\"mixed_correction_formats\"} 1"));
    assert!(metrics.contains("reason=\"crc_threshold_exceeded\"} 1"));
    assert!(metrics.contains("reason=\"base_position_missing\"} 1"));
    assert!(metrics.contains("reason=\"msm_required_missing\"} 1"));
    assert!(metrics.contains("reason=\"expected_format_mismatch\"} 1"));
    assert!(metrics.contains("reason=\"missing_correction_data\"} 1"));
    assert!(metrics.contains("reason=\"msm_version_not_allowed\"} 1"));
    assert!(metrics.contains("reason=\"unknown_data_ratio\"} 1"));
    state.set_maintenance(true);
    assert!(
        request(address, "/health/ready")
            .await
            .starts_with("HTTP/1.1 503")
    );
    assert!(
        request(address, "/metrics")
            .await
            .contains("rcaster_ready 0")
    );
    state.set_maintenance(false);
    assert!(
        request(address, "/health/ready")
            .await
            .starts_with("HTTP/1.1 200 OK")
    );

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("health task").expect("health server");
}

#[tokio::test]
async fn metrics_token_requires_bearer_auth_but_health_probes_stay_open() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("health listener");
    let address = listener.local_addr().expect("address");
    let state = Arc::new(HealthState::default());
    state.set_ready(true);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_health_with_token(
        listener,
        state,
        Some("metrics-secret".to_owned()),
        async {
            let _ = shutdown_rx.await;
        },
    ));
    assert!(
        request(address, "/health/ready")
            .await
            .starts_with("HTTP/1.1 200 OK")
    );
    assert!(
        request(address, "/metrics")
            .await
            .starts_with("HTTP/1.1 401")
    );
    assert!(
        request_with_authorization(address, "/metrics", "Bearer wrong")
            .await
            .starts_with("HTTP/1.1 401")
    );
    assert!(
        request_with_authorization(address, "/metrics", "Bearer metrics-secret")
            .await
            .starts_with("HTTP/1.1 200 OK")
    );
    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("health task").expect("health server");
}

async fn request(address: std::net::SocketAddr, path: &str) -> String {
    request_with_authorization(address, path, "").await
}

async fn request_with_authorization(
    address: std::net::SocketAddr,
    path: &str,
    authorization: &str,
) -> String {
    let mut stream = TcpStream::connect(address).await.expect("health connect");
    stream
        .write_all(
            format!(
                "GET {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: {authorization}\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .expect("health request");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("health response");
    String::from_utf8(response).expect("UTF-8 health response")
}
