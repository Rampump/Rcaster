use std::{collections::BTreeMap, io, net::SocketAddr, sync::Arc, time::Duration};

use rcaster_core::{
    account::{AccountSnapshot, Mountpoint, User, UserRole},
    protocol::{AdmissionMode, AdmissionPolicy, DataFormat, DataProfile, ProtocolDemux},
    str_record::StrRecord,
};
use rcaster_service::{
    account_directory::AccountDirectory,
    health::HealthState,
    hub::MountpointHub,
    ntrip_server::{
        AuthError, AuthorizedUser, ClientAuthorizer, ClientCredentials, NtripServerConfig,
        SourceAuthorizer, SourceCredentials, serve_ntrip,
    },
    proxy_protocol::ProxyProtocolConfig,
    session_limit::{ConnectionLimiter, LocalSessionLimiter, SessionLimitConfig},
    source_admission::SourceAdmissionConfig,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

struct TestAuthorizer;

impl SourceAuthorizer for TestAuthorizer {
    async fn authorize_source(
        &self,
        mountpoint: &str,
        credentials: SourceCredentials<'_>,
    ) -> Result<AuthorizedUser, AuthError> {
        if matches!(mountpoint, "C63" | "C64")
            && matches!(
                credentials,
                SourceCredentials::Ntrip1 { password: "secret" }
                    | SourceCredentials::Ntrip2 {
                        username: "owner",
                        password: "secret"
                    }
            )
        {
            Ok(AuthorizedUser {
                user_id: "owner".to_owned(),
                username: "owner".to_owned(),
                auth_version: 1,
                user_mod_revision: 0,
                mountpoint_mod_revision: 0,
                source_expectation: None,
            })
        } else {
            Err(AuthError::Unauthorized)
        }
    }
}

impl ClientAuthorizer for TestAuthorizer {
    async fn authorize_client(
        &self,
        _mountpoint: &str,
        credentials: Option<ClientCredentials<'_>>,
    ) -> Result<AuthorizedUser, AuthError> {
        if matches!(
            credentials,
            Some(ClientCredentials::Basic {
                username: "owner",
                password: "secret"
            })
        ) {
            Ok(AuthorizedUser {
                user_id: "owner".to_owned(),
                username: "owner".to_owned(),
                auth_version: 1,
                user_mod_revision: 0,
                mountpoint_mod_revision: 0,
                source_expectation: None,
            })
        } else {
            Err(AuthError::Unauthorized)
        }
    }
}

#[tokio::test]
async fn proxy_protocol_source_ip_drives_ip_quota() {
    let hub = Arc::new(MountpointHub::new(8).expect("hub"));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let mut limits = SessionLimitConfig::default();
    limits.per_ip.insert("192.0.2.55/32".to_owned(), 0);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_ntrip(
        listener,
        hub,
        TestAuthorizer,
        NtripServerConfig {
            head_timeout: Duration::from_secs(1),
            content_length_max_bytes: 1024,
            client_auth_required: false,
            session_limiter: Arc::new(LocalSessionLimiter::new(limits).expect("limiter")),
            connection_limiter: Arc::new(ConnectionLimiter::new(100).expect("connections")),
            cluster_runtime: None,
            relay_manager: None,
            source_admission: None,
            near: None,
            accounts: None,
            metrics: None,
            connections: None,
            proxy_protocol: Some(
                ProxyProtocolConfig::new(&["127.0.0.1/32".to_owned()]).expect("proxy config"),
            ),
            auth_login_limiter: None,
        },
        async {
            let _ = shutdown_rx.await;
        },
    ));
    let mut client = TcpStream::connect(address).await.expect("client connect");
    client
        .write_all(
            b"PROXY TCP4 192.0.2.55 127.0.0.1 4000 2101\r\nGET /C63 HTTP/1.1\r\nNtrip-Version: Ntrip/2.0\r\n\r\n",
        )
        .await
        .expect("proxy request");
    let response = read_http_head(&mut client).await;
    assert!(response.starts_with(b"HTTP/1.1 403"));
    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn max_connections_rejects_pre_auth_connection_flood() {
    let hub = Arc::new(MountpointHub::new(8).expect("hub"));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let connections = Arc::new(ConnectionLimiter::new(1).expect("connections"));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_ntrip(
        listener,
        hub,
        TestAuthorizer,
        NtripServerConfig {
            head_timeout: Duration::from_secs(1),
            content_length_max_bytes: 1024,
            client_auth_required: false,
            session_limiter: Arc::new(
                LocalSessionLimiter::new(SessionLimitConfig::default()).expect("limiter"),
            ),
            connection_limiter: Arc::clone(&connections),
            cluster_runtime: None,
            relay_manager: None,
            source_admission: None,
            near: None,
            accounts: None,
            metrics: None,
            connections: None,
            proxy_protocol: None,
            auth_login_limiter: None,
        },
        async {
            let _ = shutdown_rx.await;
        },
    ));
    let first = TcpStream::connect(address).await.expect("first connect");
    tokio::time::timeout(Duration::from_secs(1), async {
        while connections.active() != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("first connection slot");
    let mut second = TcpStream::connect(address).await.expect("second connect");
    second
        .write_all(b"GET /C63\r\n\r\n")
        .await
        .expect("second request");
    let mut buffer = [0_u8; 64];
    match tokio::time::timeout(Duration::from_secs(1), second.read(&mut buffer))
        .await
        .expect("rejection timeout")
    {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            ) => {}
        result => panic!("second connection was not rejected: {result:?}"),
    }
    drop(first);
    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn maintenance_rejects_new_connections_but_keeps_existing_streams() {
    let hub = Arc::new(MountpointHub::new(8).expect("hub"));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let health = Arc::new(HealthState::default());
    health.set_ready(true);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_ntrip(
        listener,
        hub,
        TestAuthorizer,
        NtripServerConfig {
            head_timeout: Duration::from_secs(1),
            content_length_max_bytes: 1024,
            client_auth_required: false,
            session_limiter: Arc::new(
                LocalSessionLimiter::new(SessionLimitConfig::default()).expect("limiter"),
            ),
            connection_limiter: Arc::new(ConnectionLimiter::new(100).expect("connections")),
            cluster_runtime: None,
            relay_manager: None,
            source_admission: None,
            near: None,
            accounts: None,
            metrics: Some(Arc::clone(&health)),
            connections: None,
            proxy_protocol: None,
            auth_login_limiter: None,
        },
        async {
            let _ = shutdown_rx.await;
        },
    ));
    let mut existing = TcpStream::connect(address).await.expect("existing connect");
    tokio::time::sleep(Duration::from_millis(20)).await;
    health.set_maintenance(true);
    let mut rejected = TcpStream::connect(address).await.expect("second connect");
    rejected
        .write_all(b"GET /C63\r\n\r\n")
        .await
        .expect("rejected request");
    let mut buffer = [0_u8; 64];
    match tokio::time::timeout(Duration::from_secs(1), rejected.read(&mut buffer)).await {
        Ok(Ok(0)) => {}
        Ok(Err(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            ) => {}
        result => panic!("maintenance connection was not rejected: {result:?}"),
    }
    existing
        .write_all(b"SOURCE secret /C63\r\n\r\n")
        .await
        .expect("existing request");
    assert_eq!(
        read_ntrip1_success(&mut existing).await,
        b"ICY 200 OK\r\n\r\n"
    );
    drop(existing);
    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn shutdown_cancels_open_connections_and_releases_hub_sources() {
    let (address, hub, shutdown_tx, server) = start_test_server_with_hub().await;
    let mut source = TcpStream::connect(address).await.expect("source connect");
    source
        .write_all(b"SOURCE secret /C63\r\n\r\n")
        .await
        .expect("source request");
    read_ntrip1_success(&mut source).await;
    let mut subscriber = TcpStream::connect(address)
        .await
        .expect("subscriber connect");
    subscriber
        .write_all(b"GET /C63\r\n\r\n")
        .await
        .expect("subscriber request");
    read_ntrip1_success(&mut subscriber).await;
    assert_eq!(hub.active_sources(), vec!["C63".to_owned()]);
    assert_eq!(hub.metrics().subscribers, 1);

    shutdown_tx.send(()).expect("shutdown");
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timed out")
        .expect("server join")
        .expect("NTRIP server");
    assert!(hub.active_sources().is_empty());
    assert_eq!(hub.metrics().subscribers, 0);
}

#[tokio::test]
async fn source_upload_is_fanned_out_to_tcp_subscriber() {
    let (address, hub, shutdown_tx, server) = start_test_server_with_hub().await;

    let mut source = TcpStream::connect(address).await.expect("source connect");
    source
        .write_all(b"SOURCE secret /C63\r\n\r\n")
        .await
        .expect("source request");
    assert_eq!(
        read_ntrip1_success(&mut source).await,
        b"ICY 200 OK\r\n\r\n"
    );

    let mut subscriber = TcpStream::connect(address)
        .await
        .expect("subscriber connect");
    subscriber
        .write_all(b"GET /C63\r\n\r\n")
        .await
        .expect("subscriber request");
    assert_eq!(
        read_ntrip1_success(&mut subscriber).await,
        b"ICY 200 OK\r\n\r\n"
    );
    subscriber
        .write_all(b"$GPGGA,ignored\r\n")
        .await
        .expect("ignored client input");

    let packet = synthetic_rtcm3(1074);
    source.write_all(&packet).await.expect("source packet");
    let mut received = vec![0_u8; packet.len()];
    subscriber
        .read_exact(&mut received)
        .await
        .expect("subscriber packet");
    assert_eq!(received, packet);
    let record = hub
        .sourcetable_records()
        .into_iter()
        .find(|record| record.mountpoint == "C63")
        .expect("source record");
    assert!(
        record.format.starts_with("RTCM 3"),
        "standalone source record must refresh after parsing: {}",
        record.format
    );
    for _ in 0..100 {
        if hub
            .source_profile("C63")
            .is_some_and(|profile| profile.valid_frames == 1)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let profile = hub.source_profile("C63").expect("live source profile");
    assert_eq!(profile.primary_format, Some(DataFormat::Rtcm3));
    assert!(
        hub.source_health("C63")
            .expect("source health")
            .last_correction_age_seconds
            .is_some()
    );

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn near_selects_source_from_gga_header_and_forwards_rtcm3() {
    let (address, shutdown_tx, server) = start_test_near_server().await;
    let frame = synthetic_rtcm3(1074);
    let mut source = TcpStream::connect(address).await.expect("source connect");
    source
        .write_all(b"SOURCE secret /C63\r\n\r\n")
        .await
        .expect("source request");
    read_ntrip1_success(&mut source).await;
    source.write_all(&frame).await.expect("admission frame");
    let table = sourcetable_response(address).await;
    assert!(table.contains("STR;AUTO_NEAR;NEAR;RTCM 3;"));
    assert!(table.contains(";1;0;RCaster;none;N;N;0;virtual=near"));

    let gga = nmea_gga(30.0, 0.0);
    let mut client = TcpStream::connect(address).await.expect("near connect");
    client
        .write_all(
            format!(
                "GET /AUTO_NEAR HTTP/1.1\r\nNtrip-Version: Ntrip/2.0\r\nNtrip-GGA: {gga}\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .expect("near request");
    assert!(
        read_http_head(&mut client)
            .await
            .starts_with(b"HTTP/1.1 200 OK\r\n")
    );

    source.write_all(&frame).await.expect("publish NEAR frame");
    let mut received = vec![0_u8; frame.len()];
    client.read_exact(&mut received).await.expect("NEAR frame");
    assert_eq!(received, frame);

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn near_switch_sends_cached_station_position_from_new_source() {
    let hub = Arc::new(MountpointHub::new(64).expect("hub"));
    let source_a = activate_at(&hub, "A", 30.002);
    let source_b = activate_at(&hub, "B", 30.0);
    let header = synthetic_rtcm3(1006);
    let mut demux = ProtocolDemux::new();
    source_b
        .publish_analyzed(Arc::from(header.clone()), &mut demux)
        .expect("cache station frame");
    let near = rcaster_service::ntrip_server::NearServerConfig {
        mountpoint: "AUTO_NEAR".to_owned(),
        selector: rcaster_service::near::NearConfig {
            observe_time: Duration::ZERO,
            min_recompute_interval: Duration::ZERO,
            min_recompute_distance_m: 0.0,
            gga_max_jump_speed_mps: 1_000_000_000.0,
            ..rcaster_service::near::NearConfig::default()
        },
        query_limiter: Arc::new(
            rcaster_service::near::NearQueryLimiter::new(10_000, 8).expect("NEAR query limiter"),
        ),
        no_source_timeout: Duration::from_secs(1),
        advertise: true,
    };
    let (address, shutdown_tx, server) =
        start_test_server_for_existing_hub(Arc::clone(&hub), Some(near)).await;

    let mut client = TcpStream::connect(address).await.expect("near connect");
    let initial_gga = nmea_gga(30.002, 0.0);
    client
        .write_all(
            format!(
                "GET /AUTO_NEAR HTTP/1.1\r\nNtrip-Version: Ntrip/2.0\r\nNtrip-GGA: {initial_gga}\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .expect("near request");
    assert!(
        read_http_head(&mut client)
            .await
            .starts_with(b"HTTP/1.1 200 OK\r\n")
    );

    client
        .write_all(format!("{}\r\n{}\r\n", nmea_gga(30.0, 0.0), nmea_gga(30.0, 0.0)).as_bytes())
        .await
        .expect("near switch GGA");
    let mut received = vec![0_u8; header.len()];
    client
        .read_exact(&mut received)
        .await
        .expect("cached station frame");
    assert_eq!(received, header);

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
    drop((source_a, source_b));
}

#[tokio::test]
async fn near_fails_over_when_active_source_disconnects() {
    let hub = Arc::new(MountpointHub::new(64).expect("hub"));
    let source_a = activate_at(&hub, "A", 30.002);
    let source_b = activate_at(&hub, "B", 30.0);
    let near = rcaster_service::ntrip_server::NearServerConfig {
        mountpoint: "AUTO_NEAR".to_owned(),
        selector: rcaster_service::near::NearConfig {
            observe_time: Duration::ZERO,
            min_recompute_interval: Duration::ZERO,
            min_recompute_distance_m: 0.0,
            gga_max_jump_speed_mps: 1_000_000_000.0,
            ..rcaster_service::near::NearConfig::default()
        },
        query_limiter: Arc::new(
            rcaster_service::near::NearQueryLimiter::new(10_000, 8).expect("NEAR query limiter"),
        ),
        no_source_timeout: Duration::from_secs(5),
        advertise: true,
    };
    let (address, shutdown_tx, server) =
        start_test_server_for_existing_hub(Arc::clone(&hub), Some(near)).await;

    let mut client = TcpStream::connect(address).await.expect("near connect");
    client
        .write_all(
            format!(
                "GET /AUTO_NEAR HTTP/1.1\r\nNtrip-Version: Ntrip/2.0\r\nNtrip-GGA: {}\r\n\r\n",
                nmea_gga(30.002, 0.0)
            )
            .as_bytes(),
        )
        .await
        .expect("near request");
    assert!(
        read_http_head(&mut client)
            .await
            .starts_with(b"HTTP/1.1 200 OK\r\n")
    );

    let frame_a = synthetic_rtcm3(1074);
    source_a
        .publish_frame(Arc::from(frame_a.clone()), DataFormat::Rtcm3, Some(1074))
        .expect("publish source A");
    let mut received = vec![0_u8; frame_a.len()];
    client
        .read_exact(&mut received)
        .await
        .expect("source A frame");
    assert_eq!(received, frame_a);

    drop(source_a);
    tokio::time::timeout(Duration::from_secs(1), async {
        while hub.active_sources() != vec!["B".to_owned()] {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("source A removed");
    tokio::time::sleep(Duration::from_millis(20)).await;

    let frame_b = synthetic_rtcm3(1075);
    source_b
        .publish_frame(Arc::from(frame_b.clone()), DataFormat::Rtcm3, Some(1075))
        .expect("publish source B");
    let mut received = vec![0_u8; frame_b.len()];
    tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut received))
        .await
        .expect("same NEAR client remained connected")
        .expect("source B frame");
    assert_eq!(received, frame_b);

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn near_without_alternative_closes_at_no_source_deadline_while_receiving_gga() {
    let hub = Arc::new(MountpointHub::new(64).expect("hub"));
    let source = activate_at(&hub, "A", 30.0);
    let near = rcaster_service::ntrip_server::NearServerConfig {
        mountpoint: "AUTO_NEAR".to_owned(),
        selector: rcaster_service::near::NearConfig {
            min_recompute_interval: Duration::ZERO,
            min_recompute_distance_m: 0.0,
            gga_max_input_per_second: 100,
            ..rcaster_service::near::NearConfig::default()
        },
        query_limiter: Arc::new(
            rcaster_service::near::NearQueryLimiter::new(10_000, 8).expect("NEAR query limiter"),
        ),
        no_source_timeout: Duration::from_secs(1),
        advertise: true,
    };
    let (address, shutdown_tx, server) =
        start_test_server_for_existing_hub(Arc::clone(&hub), Some(near)).await;
    let client = TcpStream::connect(address).await.expect("near connect");
    let (mut reader, mut writer) = client.into_split();
    writer
        .write_all(
            format!(
                "GET /AUTO_NEAR HTTP/1.1\r\nNtrip-Version: Ntrip/2.0\r\nNtrip-GGA: {}\r\n\r\n",
                nmea_gga(30.0, 0.0)
            )
            .as_bytes(),
        )
        .await
        .expect("near request");
    assert!(
        read_http_head_reader(&mut reader)
            .await
            .starts_with(b"HTTP/1.1 200 OK\r\n")
    );

    drop(source);
    let writer_task = tokio::spawn(async move {
        for _ in 0..20 {
            if writer
                .write_all(format!("{}\r\n", nmea_gga(30.0, 0.0)).as_bytes())
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    let mut buffer = [0_u8; 1];
    let result = tokio::time::timeout(Duration::from_secs(3), reader.read(&mut buffer))
        .await
        .expect("no-source deadline");
    assert!(matches!(result, Ok(0) | Err(_)), "connection stayed open");
    writer_task.abort();
    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn ntrip2_chunked_source_reaches_ntrip2_subscriber() {
    let (address, shutdown_tx, server) = start_test_server(false).await;
    let mut source = TcpStream::connect(address).await.expect("source connect");
    source
        .write_all(
            b"POST /C63 HTTP/1.1\r\nAuthorization: Basic b3duZXI6c2VjcmV0\r\nTransfer-Encoding: chunked\r\n\r\n",
        )
        .await
        .expect("source request");
    let source_response = read_http_head(&mut source).await;
    assert!(source_response.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(
        source_response
            .windows(26)
            .any(|line| line == b"Ntrip-Version: Ntrip/2.0\r\n")
    );

    let mut subscriber = TcpStream::connect(address)
        .await
        .expect("subscriber connect");
    subscriber
        .write_all(b"GET /C63 HTTP/1.1\r\nNtrip-Version: Ntrip/2.0\r\n\r\n")
        .await
        .expect("subscriber request");
    let subscriber_response = read_http_head(&mut subscriber).await;
    assert!(subscriber_response.starts_with(b"HTTP/1.1 200 OK\r\n"));

    source
        .write_all(b"4\r\nDATA\r\n0\r\n\r\n")
        .await
        .expect("chunked packet");
    let mut received = [0_u8; 4];
    subscriber
        .read_exact(&mut received)
        .await
        .expect("subscriber packet");
    assert_eq!(&received, b"DATA");

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn ntrip1_missing_mountpoint_returns_current_sourcetable() {
    let (address, shutdown_tx, server) = start_test_server(false).await;
    let mut source = TcpStream::connect(address).await.expect("source connect");
    source
        .write_all(b"SOURCE secret /C63\r\n\r\n")
        .await
        .expect("source request");
    read_ntrip1_success(&mut source).await;

    let mut client = TcpStream::connect(address).await.expect("client connect");
    client
        .write_all(b"GET /missing\r\n\r\n")
        .await
        .expect("missing mountpoint request");
    let mut response = Vec::new();
    client
        .read_to_end(&mut response)
        .await
        .expect("sourcetable response");

    assert!(response.starts_with(b"SOURCETABLE 200 OK\r\n"));
    assert!(
        response
            .windows(b"STR;C63;".len())
            .any(|window| window == b"STR;C63;")
    );

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn sourcetable_hides_mountpoints_not_advertised() {
    let hub = Arc::new(MountpointHub::new(8).expect("hub"));
    let _unknown = activate_at(&hub, "C61", 0.0);
    let _disabled = activate_at(&hub, "C62", 0.0);
    let _hidden = activate_at(&hub, "C63", 0.0);
    let _public = activate_at(&hub, "C64", 0.0);
    let account = User {
        schema_version: 1,
        mod_revision: 0,
        user_id: "owner".to_owned(),
        username: "owner".to_owned(),
        password_hash: "test-hash".to_owned(),
        auth_version: 1,
        enabled: true,
        ntrip_enabled: true,
        web_enabled: false,
        role: UserRole::User,
        created_at: "0".to_owned(),
        updated_at: "0".to_owned(),
    };
    let mountpoint = |name: &str, enabled: bool, advertise: bool| Mountpoint {
        schema_version: 1,
        mod_revision: 0,
        mountpoint: name.to_owned(),
        owner_user_id: "owner".to_owned(),
        enabled,
        ntrip1_upload_password_hash: None,
        advertise,
        region: None,
        latitude: None,
        longitude: None,
        coverage_radius_m: None,
        str_fields: None,
        source_expectation: None,
        created_at: "0".to_owned(),
        updated_at: "0".to_owned(),
    };
    let accounts = AccountDirectory::static_snapshot(
        AccountSnapshot::new(
            [account],
            [
                mountpoint("C62", false, true),
                mountpoint("C63", true, false),
                mountpoint("C64", true, true),
            ],
        )
        .expect("accounts"),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind NTRIP listener");
    let address = listener.local_addr().expect("listener address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_ntrip(
        listener,
        Arc::clone(&hub),
        TestAuthorizer,
        NtripServerConfig {
            head_timeout: Duration::from_secs(1),
            content_length_max_bytes: 1024,
            client_auth_required: false,
            session_limiter: Arc::new(
                LocalSessionLimiter::new(SessionLimitConfig::default()).expect("limiter"),
            ),
            connection_limiter: Arc::new(ConnectionLimiter::new(100).expect("connections")),
            cluster_runtime: None,
            relay_manager: None,
            source_admission: None,
            accounts: Some(accounts),
            near: None,
            metrics: None,
            connections: None,
            proxy_protocol: None,
            auth_login_limiter: None,
        },
        async {
            let _ = shutdown_rx.await;
        },
    ));

    let mut client = TcpStream::connect(address).await.expect("client connect");
    client
        .write_all(b"GET / HTTP/1.1\r\nNtrip-Version: Ntrip/2.0\r\n\r\n")
        .await
        .expect("sourcetable request");
    let response = read_http_response(&mut client).await;
    assert!(windows_contains(&response, b"STR;C64;"));
    assert!(!windows_contains(&response, b"STR;C61;"));
    assert!(!windows_contains(&response, b"STR;C62;"));
    assert!(!windows_contains(&response, b"STR;C63;"));

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn ntrip2_sourcetable_keep_alive_accepts_another_request() {
    let (address, shutdown_tx, server) = start_test_server(false).await;
    let mut client = TcpStream::connect(address).await.expect("client connect");

    client
        .write_all(b"GET / HTTP/1.1\r\nNtrip-Version: Ntrip/2.0\r\nConnection: keep-alive\r\n\r\n")
        .await
        .expect("first request");
    let first = read_http_response(&mut client).await;
    assert!(first.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(windows_contains(&first, b"Connection: keep-alive\r\n"));
    assert!(windows_contains(&first, b"ENDSOURCETABLE"));

    client
        .write_all(
            b"GET /SOURCETABLE HTTP/1.1\r\nNtrip-Version: Ntrip/2.0\r\nConnection: keep-alive\r\n\r\n",
        )
        .await
        .expect("second request");
    let second = read_http_response(&mut client).await;
    assert!(second.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(windows_contains(&second, b"ENDSOURCETABLE"));

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn ntrip1_client_authentication_is_enforced() {
    let (address, shutdown_tx, server) = start_test_server(true).await;
    let mut source = TcpStream::connect(address).await.expect("source connect");
    source
        .write_all(b"SOURCE secret /C63\r\n\r\n")
        .await
        .expect("source request");
    read_ntrip1_success(&mut source).await;

    let mut anonymous = TcpStream::connect(address).await.expect("client connect");
    anonymous
        .write_all(b"GET /C63\r\n\r\n")
        .await
        .expect("anonymous request");
    let unauthorized = read_http_head(&mut anonymous).await;
    assert!(unauthorized.starts_with(b"HTTP/1.1 401 Unauthorized\r\n"));

    let mut client = TcpStream::connect(address).await.expect("client connect");
    client
        .write_all(b"GET /C63\r\nAuthorization: Basic b3duZXI6c2VjcmV0\r\n\r\n")
        .await
        .expect("authenticated request");
    assert_eq!(
        read_ntrip1_success(&mut client).await,
        b"ICY 200 OK\r\n\r\n"
    );

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn source_and_client_share_the_final_user_slot() {
    let (address, shutdown_tx, server) = start_test_server_with_limits(
        true,
        SessionLimitConfig {
            default_max_sessions: 1,
            per_username: BTreeMap::new(),
            ..SessionLimitConfig::default()
        },
    )
    .await;
    let mut source = TcpStream::connect(address).await.expect("source connect");
    source
        .write_all(b"SOURCE secret /C63\r\n\r\n")
        .await
        .expect("source request");
    read_ntrip1_success(&mut source).await;

    let mut client = TcpStream::connect(address).await.expect("client connect");
    client
        .write_all(b"GET /C63\r\nAuthorization: Basic b3duZXI6c2VjcmV0\r\n\r\n")
        .await
        .expect("client request");
    let limited = read_http_head(&mut client).await;
    assert!(limited.starts_with(b"HTTP/1.1 429 Too Many Requests\r\n"));

    drop(source);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut replacement = TcpStream::connect(address)
        .await
        .expect("replacement connect");
    replacement
        .write_all(b"SOURCE secret /C63\r\n\r\n")
        .await
        .expect("replacement request");
    assert_eq!(
        read_ntrip1_success(&mut replacement).await,
        b"ICY 200 OK\r\n\r\n"
    );

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn ntrip1_source_quota_rejection_returns_an_error_response() {
    let (address, shutdown_tx, server) = start_test_server_with_limits(
        false,
        SessionLimitConfig {
            default_max_sessions: 1,
            per_username: BTreeMap::new(),
            ..SessionLimitConfig::default()
        },
    )
    .await;
    let mut first = TcpStream::connect(address).await.expect("first source");
    first
        .write_all(b"SOURCE secret /C63\r\n\r\n")
        .await
        .expect("first source request");
    read_ntrip1_success(&mut first).await;

    let mut second = TcpStream::connect(address).await.expect("second source");
    second
        .write_all(b"SOURCE secret /C64\r\n\r\n")
        .await
        .expect("second source request");
    let mut response = Vec::new();
    second
        .read_to_end(&mut response)
        .await
        .expect("quota rejection response");
    assert_eq!(response, b"ERROR - Mount Point Taken or Invalid\r\n");

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn zero_quota_ip_is_rejected_before_client_auth() {
    let (address, shutdown_tx, server) = start_test_server_with_limits(
        true,
        SessionLimitConfig {
            default_max_sessions: -1,
            per_ip: BTreeMap::from([("127.0.0.1/32".to_owned(), 0)]),
            ..SessionLimitConfig::default()
        },
    )
    .await;
    let mut source = TcpStream::connect(address).await.expect("source connect");
    source
        .write_all(b"SOURCE secret /C63\r\n\r\n")
        .await
        .expect("source request");
    read_ntrip1_success(&mut source).await;

    let mut client = TcpStream::connect(address).await.expect("client connect");
    client
        .write_all(b"GET /C63\r\nAuthorization: Basic b3duZXI6c2VjcmV0\r\n\r\n")
        .await
        .expect("client request");
    let denied = read_http_head(&mut client).await;
    assert!(denied.starts_with(b"HTTP/1.1 403 Forbidden\r\n"));

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn ip_quota_counts_clients_but_not_the_source() {
    let (address, shutdown_tx, server) = start_test_server_with_limits(
        false,
        SessionLimitConfig {
            default_max_sessions: -1,
            default_max_ip_sessions: 1,
            ..SessionLimitConfig::default()
        },
    )
    .await;
    let mut source = TcpStream::connect(address).await.expect("source connect");
    source
        .write_all(b"SOURCE secret /C63\r\n\r\n")
        .await
        .expect("source request");
    read_ntrip1_success(&mut source).await;

    let mut first = TcpStream::connect(address).await.expect("first client");
    first
        .write_all(b"GET /C63\r\n\r\n")
        .await
        .expect("first request");
    read_ntrip1_success(&mut first).await;

    let mut second = TcpStream::connect(address).await.expect("second client");
    second
        .write_all(b"GET /C63\r\n\r\n")
        .await
        .expect("second request");
    let limited = read_http_head(&mut second).await;
    assert!(limited.starts_with(b"HTTP/1.1 429 Too Many Requests\r\n"));

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn source_admission_window_gates_fanout_until_accept() {
    let frame = synthetic_rtcm3(1074);
    let (address, shutdown_tx, server) = start_test_server_with_admission(
        AdmissionPolicy {
            mode: AdmissionMode::Enforce,
            allowed_formats: vec![DataFormat::Rtcm3],
            require_correction_data: true,
            expected_formats: None,
            allowed_msm_versions: None,
            min_valid_frames: 1,
            max_unknown_ratio: None,
            min_observed_bytes: 0,
            ..AdmissionPolicy::default()
        },
        frame.len() as u64 + 16,
        Duration::from_secs(1),
    )
    .await;
    let mut source = TcpStream::connect(address).await.expect("source connect");
    source
        .write_all(b"SOURCE secret /C63\r\n\r\n")
        .await
        .expect("source request");
    read_ntrip1_success(&mut source).await;
    assert!(!sourcetable_response(address).await.contains("STR;C63;"));

    source.write_all(&frame).await.expect("admission frame");
    let mut subscriber = subscribe_ntrip2_when_active(address).await;
    source.write_all(&frame).await.expect("active frame");
    let mut received = vec![0_u8; frame.len()];
    subscriber
        .read_exact(&mut received)
        .await
        .expect("admitted frame");
    assert_eq!(received, frame);

    let table_response = sourcetable_response(address).await;
    assert!(table_response.contains("STR;C63;"));
    assert!(table_response.contains("RTCM 3.3;1074;"));

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn admission_preserves_partial_frame_after_accept() {
    let frame = synthetic_rtcm3(1074);
    let (address, shutdown_tx, server) = start_test_server_with_admission(
        AdmissionPolicy {
            mode: AdmissionMode::Enforce,
            allowed_formats: vec![DataFormat::Rtcm3],
            require_correction_data: true,
            expected_formats: None,
            allowed_msm_versions: None,
            min_valid_frames: 1,
            max_unknown_ratio: None,
            min_observed_bytes: 0,
            ..AdmissionPolicy::default()
        },
        (frame.len() * 2) as u64,
        Duration::from_secs(1),
    )
    .await;
    let mut source = TcpStream::connect(address).await.expect("source connect");
    source
        .write_all(b"SOURCE secret /C63\r\n\r\n")
        .await
        .expect("source request");
    read_ntrip1_success(&mut source).await;
    source.write_all(&frame).await.expect("admission frame");
    source.write_all(&frame[..7]).await.expect("partial frame");
    let mut subscriber = subscribe_ntrip2_when_active(address).await;
    source.write_all(&frame[7..]).await.expect("frame tail");
    let mut received = vec![0_u8; frame.len()];
    tokio::time::timeout(Duration::from_secs(1), subscriber.read_exact(&mut received))
        .await
        .expect("partial frame timed out")
        .expect("continued frame");
    assert_eq!(received, frame);

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn fixed_length_source_uses_admission_window() {
    let frame = synthetic_rtcm3(1074);
    let (address, shutdown_tx, server) = start_test_server_with_admission(
        AdmissionPolicy {
            mode: AdmissionMode::Enforce,
            allowed_formats: vec![DataFormat::Rtcm3],
            require_correction_data: true,
            expected_formats: None,
            allowed_msm_versions: None,
            min_valid_frames: 1,
            max_unknown_ratio: None,
            min_observed_bytes: 0,
            ..AdmissionPolicy::default()
        },
        (frame.len() * 2) as u64,
        Duration::from_secs(1),
    )
    .await;
    let mut source = TcpStream::connect(address).await.expect("source connect");
    source
        .write_all(
            format!(
                "POST /C63 HTTP/1.1\r\nAuthorization: Basic b3duZXI6c2VjcmV0\r\nContent-Length: {}\r\n\r\n",
                frame.len() * 2
            )
            .as_bytes(),
        )
        .await
        .expect("source request");
    read_http_head(&mut source).await;
    source.write_all(&frame).await.expect("fixed frame");
    let mut subscriber = subscribe_ntrip2_when_active(address).await;
    source.write_all(&frame).await.expect("second fixed frame");
    let mut received = vec![0_u8; frame.len()];
    subscriber
        .read_exact(&mut received)
        .await
        .expect("fixed admitted frame");
    assert_eq!(received, frame);

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn chunked_source_uses_admission_window() {
    let frame = synthetic_rtcm3(1074);
    let (address, shutdown_tx, server) = start_test_server_with_admission(
        AdmissionPolicy {
            mode: AdmissionMode::Enforce,
            allowed_formats: vec![DataFormat::Rtcm3],
            require_correction_data: true,
            expected_formats: None,
            allowed_msm_versions: None,
            min_valid_frames: 1,
            max_unknown_ratio: None,
            min_observed_bytes: 0,
            ..AdmissionPolicy::default()
        },
        (frame.len() * 2) as u64,
        Duration::from_secs(1),
    )
    .await;
    let mut source = TcpStream::connect(address).await.expect("source connect");
    source
        .write_all(
            b"POST /C63 HTTP/1.1\r\nAuthorization: Basic b3duZXI6c2VjcmV0\r\nTransfer-Encoding: chunked\r\n\r\n",
        )
        .await
        .expect("source request");
    read_http_head(&mut source).await;
    source
        .write_all(format!("{:X}\r\n", frame.len()).as_bytes())
        .await
        .expect("chunk size");
    source.write_all(&frame).await.expect("chunk data");
    source.write_all(b"\r\n").await.expect("first chunk end");
    let mut subscriber = subscribe_ntrip2_when_active(address).await;
    source
        .write_all(format!("{:X}\r\n", frame.len()).as_bytes())
        .await
        .expect("second chunk size");
    source.write_all(&frame).await.expect("second chunk data");
    source.write_all(b"\r\n0\r\n\r\n").await.expect("chunk end");
    let mut received = vec![0_u8; frame.len()];
    subscriber
        .read_exact(&mut received)
        .await
        .expect("chunked admitted frame");
    assert_eq!(received, frame);

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

#[tokio::test]
async fn admitted_stream_is_not_closed_by_window_timeout() {
    let frame = synthetic_rtcm3(1074);
    let (address, shutdown_tx, server) = start_test_server_with_admission(
        AdmissionPolicy {
            mode: AdmissionMode::Enforce,
            allowed_formats: vec![DataFormat::Rtcm3],
            require_correction_data: true,
            expected_formats: None,
            allowed_msm_versions: None,
            min_valid_frames: 1,
            max_unknown_ratio: None,
            min_observed_bytes: 0,
            ..AdmissionPolicy::default()
        },
        (frame.len() * 2) as u64,
        Duration::from_millis(100),
    )
    .await;
    let mut source = TcpStream::connect(address).await.expect("source connect");
    source
        .write_all(b"SOURCE secret /C63\r\n\r\n")
        .await
        .expect("source request");
    read_ntrip1_success(&mut source).await;
    source.write_all(&frame).await.expect("first frame");
    let mut subscriber = subscribe_ntrip2_when_active(address).await;
    source
        .write_all(&frame)
        .await
        .expect("first subscribed frame");
    let mut received = vec![0_u8; frame.len()];
    subscriber
        .read_exact(&mut received)
        .await
        .expect("first admitted frame");
    assert_eq!(received, frame);

    tokio::time::sleep(Duration::from_millis(250)).await;
    source.write_all(&frame).await.expect("second frame");
    subscriber
        .read_exact(&mut received)
        .await
        .expect("second admitted frame");
    assert_eq!(received, frame);

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
}

async fn start_test_server(
    client_auth_required: bool,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<io::Result<()>>,
) {
    start_test_server_with_limits(client_auth_required, SessionLimitConfig::default()).await
}

async fn start_test_server_with_hub() -> (
    SocketAddr,
    Arc<MountpointHub>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<io::Result<()>>,
) {
    start_test_server_with_admission_inner_with_hub(
        false,
        SessionLimitConfig::default(),
        None,
        None,
        None,
    )
    .await
}

async fn start_test_server_for_existing_hub(
    hub: Arc<MountpointHub>,
    near: Option<rcaster_service::ntrip_server::NearServerConfig>,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<io::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind NTRIP listener");
    let address = listener.local_addr().expect("listener address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_ntrip(
        listener,
        hub,
        TestAuthorizer,
        NtripServerConfig {
            head_timeout: Duration::from_secs(1),
            content_length_max_bytes: 1024,
            client_auth_required: false,
            session_limiter: Arc::new(
                LocalSessionLimiter::new(SessionLimitConfig::default()).expect("session limiter"),
            ),
            connection_limiter: Arc::new(ConnectionLimiter::new(100).expect("connections")),
            cluster_runtime: None,
            relay_manager: None,
            source_admission: None,
            near,
            accounts: None,
            metrics: None,
            connections: None,
            proxy_protocol: None,
            auth_login_limiter: None,
        },
        async {
            let _ = shutdown_rx.await;
        },
    ));
    (address, shutdown_tx, server)
}

async fn start_test_server_with_limits(
    client_auth_required: bool,
    session_limits: SessionLimitConfig,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<io::Result<()>>,
) {
    let (address, _, shutdown_tx, server) = start_test_server_with_admission_inner_with_hub(
        client_auth_required,
        session_limits,
        None,
        None,
        None,
    )
    .await;
    (address, shutdown_tx, server)
}

async fn start_test_server_with_admission(
    policy: AdmissionPolicy,
    max_bytes: u64,
    timeout: Duration,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<io::Result<()>>,
) {
    start_test_server_with_admission_inner(
        false,
        SessionLimitConfig::default(),
        Some(SourceAdmissionConfig {
            policy,
            max_bytes,
            max_frames: 8,
            timeout,
            read_timeout: Duration::from_secs(1),
            str_refresh: true,
        }),
        None,
        None,
    )
    .await
}

async fn start_test_near_server() -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<io::Result<()>>,
) {
    start_test_server_with_admission_inner(
        false,
        SessionLimitConfig::default(),
        Some(SourceAdmissionConfig {
            policy: AdmissionPolicy {
                mode: AdmissionMode::Enforce,
                allowed_formats: vec![DataFormat::Rtcm3],
                require_correction_data: false,
                expected_formats: None,
                allowed_msm_versions: None,
                min_valid_frames: 1,
                max_unknown_ratio: None,
                min_observed_bytes: 0,
                ..AdmissionPolicy::default()
            },
            max_bytes: 1024,
            max_frames: 8,
            timeout: Duration::from_secs(1),
            read_timeout: Duration::from_secs(1),
            str_refresh: true,
        }),
        Some(rcaster_service::ntrip_server::NearServerConfig {
            mountpoint: "AUTO_NEAR".to_owned(),
            selector: rcaster_service::near::NearConfig::default(),
            query_limiter: Arc::new(
                rcaster_service::near::NearQueryLimiter::new(10_000, 8)
                    .expect("NEAR query limiter"),
            ),
            no_source_timeout: Duration::from_secs(1),
            advertise: true,
        }),
        Some(near_test_accounts()),
    )
    .await
}

async fn start_test_server_with_admission_inner(
    client_auth_required: bool,
    session_limits: SessionLimitConfig,
    source_admission: Option<SourceAdmissionConfig>,
    near: Option<rcaster_service::ntrip_server::NearServerConfig>,
    accounts: Option<AccountDirectory>,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<io::Result<()>>,
) {
    let (address, _, shutdown_tx, server) = start_test_server_with_admission_inner_with_hub(
        client_auth_required,
        session_limits,
        source_admission,
        near,
        accounts,
    )
    .await;
    (address, shutdown_tx, server)
}

async fn start_test_server_with_admission_inner_with_hub(
    client_auth_required: bool,
    session_limits: SessionLimitConfig,
    source_admission: Option<SourceAdmissionConfig>,
    near: Option<rcaster_service::ntrip_server::NearServerConfig>,
    accounts: Option<AccountDirectory>,
) -> (
    SocketAddr,
    Arc<MountpointHub>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<io::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind NTRIP listener");
    let address = listener.local_addr().expect("listener address");
    let hub = Arc::new(MountpointHub::new(64).expect("hub"));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_ntrip(
        listener,
        hub.clone(),
        TestAuthorizer,
        NtripServerConfig {
            head_timeout: Duration::from_secs(1),
            content_length_max_bytes: 1024,
            client_auth_required,
            session_limiter: Arc::new(
                LocalSessionLimiter::new(session_limits).expect("session limiter"),
            ),
            connection_limiter: Arc::new(ConnectionLimiter::new(100).expect("connections")),
            cluster_runtime: None,
            relay_manager: None,
            source_admission,
            near,
            accounts,
            metrics: None,
            connections: None,
            proxy_protocol: None,
            auth_login_limiter: None,
        },
        async {
            let _ = shutdown_rx.await;
        },
    ));
    (address, hub, shutdown_tx, server)
}

async fn read_ntrip1_success(stream: &mut TcpStream) -> Vec<u8> {
    let mut response = vec![0_u8; b"ICY 200 OK\r\n\r\n".len()];
    stream
        .read_exact(&mut response)
        .await
        .expect("NTRIP1 response");
    response
}

async fn subscribe_ntrip2_when_active(address: SocketAddr) -> TcpStream {
    for _ in 0..100 {
        let mut subscriber = TcpStream::connect(address)
            .await
            .expect("subscriber connect");
        subscriber
            .write_all(b"GET /C63 HTTP/1.1\r\nNtrip-Version: Ntrip/2.0\r\n\r\n")
            .await
            .expect("subscriber request");
        let response = read_http_head(&mut subscriber).await;
        if response.starts_with(b"HTTP/1.1 200 OK\r\n") {
            return subscriber;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("source did not become active");
}

async fn sourcetable_response(address: SocketAddr) -> String {
    let mut table = TcpStream::connect(address).await.expect("table client");
    table
        .write_all(b"GET /\r\n\r\n")
        .await
        .expect("table request");
    let mut response = Vec::new();
    table
        .read_to_end(&mut response)
        .await
        .expect("table response");
    String::from_utf8_lossy(&response).into_owned()
}

async fn read_http_head(stream: &mut TcpStream) -> Vec<u8> {
    let mut response = Vec::new();
    let mut chunk = [0_u8; 512];
    while !response.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut chunk).await.expect("HTTP response");
        assert!(read > 0, "connection ended before HTTP headers");
        response.extend_from_slice(&chunk[..read]);
        assert!(response.len() <= 4096, "oversized HTTP headers");
    }
    response
}

async fn read_http_response(stream: &mut TcpStream) -> Vec<u8> {
    let mut response = read_http_head(stream).await;
    let body_start = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("response header terminator")
        + 4;
    let content_length = std::str::from_utf8(&response[..body_start])
        .expect("ASCII response header")
        .lines()
        .find_map(|line| {
            line.strip_prefix("Content-Length: ")
                .and_then(|value| value.parse::<usize>().ok())
        })
        .expect("Content-Length");
    let buffered_body = response.len() - body_start;
    if buffered_body < content_length {
        let mut body = vec![0_u8; content_length - buffered_body];
        stream.read_exact(&mut body).await.expect("response body");
        response.extend_from_slice(&body);
    }
    response
}

async fn read_http_head_reader<R>(stream: &mut R) -> Vec<u8>
where
    R: AsyncRead + Unpin,
{
    let mut head = Vec::new();
    let mut chunk = [0_u8; 512];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut chunk).await.expect("socket response");
        assert!(read > 0, "connection ended before response headers");
        head.extend_from_slice(&chunk[..read]);
        assert!(head.len() <= 8192, "response headers are too large");
    }
    head
}

fn windows_contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
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

fn near_test_accounts() -> AccountDirectory {
    AccountDirectory::static_snapshot(
        AccountSnapshot::new(
            [User {
                schema_version: 1,
                mod_revision: 1,
                user_id: "owner".to_owned(),
                username: "owner".to_owned(),
                password_hash: "hash".to_owned(),
                auth_version: 1,
                enabled: true,
                ntrip_enabled: true,
                web_enabled: false,
                role: UserRole::User,
                created_at: "0".to_owned(),
                updated_at: "0".to_owned(),
            }],
            [Mountpoint {
                schema_version: 1,
                mod_revision: 1,
                mountpoint: "C63".to_owned(),
                owner_user_id: "owner".to_owned(),
                enabled: true,
                ntrip1_upload_password_hash: None,
                advertise: true,
                region: None,
                latitude: Some(30.0),
                longitude: Some(0.0),
                coverage_radius_m: None,
                str_fields: None,
                source_expectation: None,
                created_at: "0".to_owned(),
                updated_at: "0".to_owned(),
            }],
        )
        .expect("NEAR accounts"),
    )
}

fn activate_at(
    hub: &MountpointHub,
    mountpoint: &str,
    latitude: f64,
) -> rcaster_service::hub::SourceLease {
    let source = hub.claim_source(mountpoint).expect("source claim");
    let profile = DataProfile {
        primary_format: Some(DataFormat::Rtcm3),
        has_correction_data: true,
        position: Some(rcaster_core::protocol::GeoPosition {
            latitude_e7: (latitude * 1e7).round() as i32,
            longitude_e7: 0,
            height_cm: 0,
        }),
        ..DataProfile::default()
    };
    source
        .set_str_record(StrRecord::from_profile(mountpoint, &profile))
        .expect("activate source");
    source.set_profile(profile).expect("source profile");
    source
}

fn nmea_gga(latitude: f64, longitude: f64) -> String {
    let body = format!(
        "GPGGA,000000,{},{},{},{},1,08,0.9,0.0,M,0.0,M,,",
        nmea_coordinate(latitude, 2),
        if latitude < 0.0 { "S" } else { "N" },
        nmea_coordinate(longitude, 3),
        if longitude < 0.0 { "W" } else { "E" },
    );
    let checksum = body.bytes().fold(0_u8, |checksum, byte| checksum ^ byte);
    format!("${body}*{checksum:02X}")
}

fn nmea_coordinate(value: f64, degree_width: usize) -> String {
    let absolute = value.abs();
    let degrees = absolute.trunc();
    let minutes = (absolute - degrees) * 60.0;
    format!(
        "{degrees:0degree_width$.0}{minutes:07.4}",
        degrees = degrees as u64,
        degree_width = degree_width
    )
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
