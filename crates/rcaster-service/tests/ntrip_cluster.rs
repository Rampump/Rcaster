use std::{
    fs,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, ConnectOptions, PutOptions, Txn, TxnOp};
use rcaster_core::{
    account::{AccountSnapshot, Mountpoint, MountpointStrConfig, User, UserRole},
    protocol::{AdmissionMode, AdmissionPolicy, DataFormat},
};
use rcaster_service::{
    account_directory::AccountDirectory,
    admission::AdmissionError,
    auth::{
        AuthError, AuthorizedUser, ClientAuthorizer, ClientCredentials, SourceAuthorizer,
        SourceCredentials,
    },
    cluster_runtime::{ClusterRuntimeOptions, SharedClusterRuntime},
    config::{CasterConfig, publish_config},
    connections::{CommandWatcher, ConnectionController},
    coordinator_admission::{AdmissionCoordinator, CoordinatorOptions},
    hub::MountpointHub,
    ntrip_server::{NtripServerConfig, serve_ntrip},
    session_limit::{ConnectionLimiter, LocalSessionLimiter, SessionLimitConfig},
    source_admission::SourceAdmissionConfig,
    source_claim::ClaimError,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

const RTCM3_1005: &str = "D300133ED0000236FDB80DDE08005B2BC108A7B98D3DD8AB37";

struct TestAuthorizer;

impl SourceAuthorizer for TestAuthorizer {
    async fn authorize_source(
        &self,
        _mountpoint: &str,
        _credentials: SourceCredentials<'_>,
    ) -> Result<AuthorizedUser, AuthError> {
        Ok(AuthorizedUser {
            user_id: "owner-id".to_owned(),
            username: "owner".to_owned(),
            auth_version: 1,
            user_mod_revision: 0,
            mountpoint_mod_revision: 0,
            source_expectation: None,
        })
    }
}

impl ClientAuthorizer for TestAuthorizer {
    async fn authorize_client(
        &self,
        _mountpoint: &str,
        _credentials: Option<ClientCredentials<'_>>,
    ) -> Result<AuthorizedUser, AuthError> {
        Ok(AuthorizedUser {
            user_id: "client-id".to_owned(),
            username: "client".to_owned(),
            auth_version: 1,
            user_mod_revision: 0,
            mountpoint_mod_revision: 0,
            source_expectation: None,
        })
    }
}

#[tokio::test]
async fn uploaded_rtcm3_generates_active_etcd_str_record() {
    let cluster = unique("ntrip-etcd-str");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("coordinator"),
    );
    let shared = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        ClusterRuntimeOptions {
            cluster_id: cluster.clone(),
            caster_id: "caster-etcd-str".to_owned(),
            runtime_epoch: unique("epoch"),
            relay_endpoint: "10.0.0.23:12102".to_owned(),
            lease_ttl_s: 30,
            keep_alive: true,
        },
    )
    .await
    .expect("runtime");
    let configured_mountpoint = Mountpoint {
        schema_version: 1,
        mod_revision: 1,
        mountpoint: "C66".to_owned(),
        owner_user_id: "owner-id".to_owned(),
        enabled: true,
        ntrip1_upload_password_hash: None,
        advertise: true,
        region: Some("east".to_owned()),
        latitude: None,
        longitude: None,
        coverage_radius_m: Some(30_000.0),
        str_fields: Some(MountpointStrConfig {
            identifier: Some("Configured Farm".to_owned()),
            format: None,
            formats: None,
            carrier: Some("2".to_owned()),
            navigation_system: Some("GPS".to_owned()),
            network: Some("RTK-NET".to_owned()),
            country: Some("CHN".to_owned()),
            solution: Some("0".to_owned()),
        }),
        source_expectation: None,
        created_at: "2026-11-24T00:00:00Z".to_owned(),
        updated_at: "2026-11-24T00:00:00Z".to_owned(),
    };
    let accounts = AccountDirectory::static_snapshot(
        AccountSnapshot::new(
            [User {
                schema_version: 1,
                mod_revision: 1,
                user_id: "owner-id".to_owned(),
                username: "owner".to_owned(),
                password_hash: "hash".to_owned(),
                auth_version: 1,
                enabled: true,
                ntrip_enabled: true,
                web_enabled: false,
                role: UserRole::User,
                created_at: "2026-11-24T00:00:00Z".to_owned(),
                updated_at: "2026-11-24T00:00:00Z".to_owned(),
            }],
            [configured_mountpoint],
        )
        .expect("account snapshot"),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_ntrip(
        listener,
        Arc::new(MountpointHub::new(64).expect("hub")),
        TestAuthorizer,
        NtripServerConfig {
            head_timeout: Duration::from_secs(1),
            content_length_max_bytes: 1024,
            client_auth_required: false,
            session_limiter: Arc::new(
                LocalSessionLimiter::new(SessionLimitConfig::default()).expect("limiter"),
            ),
            connection_limiter: Arc::new(ConnectionLimiter::new(100).expect("connections")),
            cluster_runtime: Some(shared.clone()),
            relay_manager: None,
            source_admission: Some(SourceAdmissionConfig {
                policy: AdmissionPolicy {
                    mode: AdmissionMode::Observe,
                    allowed_formats: vec![DataFormat::Rtcm3],
                    require_correction_data: true,
                    min_valid_frames: 1,
                    ..AdmissionPolicy::default()
                },
                max_bytes: 4096,
                max_frames: 8,
                timeout: Duration::from_millis(150),
                read_timeout: Duration::from_secs(2),
                str_refresh: true,
            }),
            near: None,
            accounts: Some(accounts),
            metrics: None,
            connections: None,
            proxy_protocol: None,
            auth_login_limiter: None,
        },
        async {
            let _ = shutdown_rx.await;
        },
    ));

    let mut source = TcpStream::connect(address).await.expect("source");
    source
        .write_all(b"SOURCE secret /C66\r\nUser-Agent: rtcm-test\r\n\r\n")
        .await
        .expect("source request");
    assert_eq!(read_http_head(&mut source).await, b"ICY 200 OK\r\n\r\n");
    let capture = std::env::var_os("RCASTER_SERIAL_CAPTURE");
    if let Some(capture) = &capture {
        let capture = fs::read(capture).expect("read serial capture");
        source.write_all(&capture).await.expect("serial capture");
    } else {
        source
            .write_all(&synthetic_rtcm3(1074))
            .await
            .expect("MSM frame");
    }

    let mut value = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let record = admin
                .get(format!("/rcaster/{cluster}/runtime/sources/C66"), None)
                .await
                .expect("read runtime source");
            if let Some(record) = record.kvs().first() {
                let value: serde_json::Value =
                    serde_json::from_slice(record.value()).expect("source record JSON");
                if value["state"] == "ACTIVE" {
                    break value;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("etcd STR record");

    if capture.is_none() {
        assert_eq!(value["str"]["latitude"], "0");
        assert_eq!(value["str"]["longitude"], "0");
        assert!(
            value["str"]["format_details"]
                .as_str()
                .is_some_and(|details| details.contains("1074") && !details.contains("1005"))
        );
        source
            .write_all(&decode_hex(RTCM3_1005))
            .await
            .expect("late position frame");
        let refreshed = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let record = admin
                    .get(format!("/rcaster/{cluster}/runtime/sources/C66"), None)
                    .await
                    .expect("read refreshed runtime source");
                if let Some(record) = record.kvs().first() {
                    let value: serde_json::Value =
                        serde_json::from_slice(record.value()).expect("refreshed JSON");
                    if value["str"]["latitude"] != "0"
                        && value["str"]["format_details"]
                            .as_str()
                            .is_some_and(|details| details.contains("1005"))
                    {
                        break value;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("refreshed etcd STR");
        assert_ne!(refreshed["str"]["latitude"], "0");
        assert_ne!(refreshed["str"]["longitude"], "0");
        value = refreshed;
    }

    assert_eq!(value["mountpoint"], "C66");
    assert_eq!(value["caster_id"], "caster-etcd-str");
    assert!(value["source_generation"].as_u64().unwrap_or_default() >= 1);
    assert_eq!(value["str"]["mountpoint"], "C66");
    assert_eq!(value["str"]["format"], "RTCM 3.3");
    assert_eq!(value["str"]["network"], "RTK-NET");
    assert_eq!(value["str"]["country"], "CHN");
    assert!(
        value["str"]["format_details"]
            .as_str()
            .is_some_and(|details| details.contains("1005") && details.contains("1074"))
    );
    assert_ne!(value["str"]["latitude"], "0");
    assert_ne!(value["str"]["longitude"], "0");

    let mut unknown_source = TcpStream::connect(address).await.expect("unknown source");
    unknown_source
        .write_all(b"SOURCE secret /C67\r\nUser-Agent: unknown-test\r\n\r\n")
        .await
        .expect("unknown source request");
    assert_eq!(
        read_http_head(&mut unknown_source).await,
        b"ICY 200 OK\r\n\r\n"
    );
    unknown_source
        .write_all(b"not-a-gnss-protocol")
        .await
        .expect("unknown data");
    let unknown_value = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let record = admin
                .get(format!("/rcaster/{cluster}/runtime/sources/C67"), None)
                .await
                .expect("read unknown source");
            if let Some(record) = record.kvs().first() {
                let value: serde_json::Value =
                    serde_json::from_slice(record.value()).expect("unknown source JSON");
                if value["state"] == "ACTIVE" {
                    break value;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("unknown source remains active");
    assert_eq!(unknown_value["str"]["format"], "UNKNOWN");

    drop(unknown_source);
    drop(source);
    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("server");
    shared.shutdown().await.expect("runtime shutdown");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn listener_releases_cluster_source_when_connection_closes() {
    let cluster = unique("ntrip-cluster");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    admin
        .txn(
            Txn::new().and_then([TxnOp::put(
                format!("/rcaster/{cluster}/config/blacklist/deny-local"),
                serde_json::to_vec(&serde_json::json!({
                    "schema_version": 1,
                    "mod_revision": 1,
                    "id": "deny-local",
                    "cidr": "127.0.0.1/32",
                    "quota": 0,
                    "enabled": true,
                    "description": "test",
                    "updated_at": "2026-11-22T00:00:00Z"
                }))
                .expect("encode rule"),
                Some(PutOptions::new()),
            )]),
        )
        .await
        .expect("write blacklist");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("coordinator"),
    );
    let shared = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        ClusterRuntimeOptions {
            cluster_id: cluster,
            caster_id: "caster-listener".to_owned(),
            runtime_epoch: unique("epoch"),
            relay_endpoint: "10.0.0.20:12102".to_owned(),
            lease_ttl_s: 30,
            keep_alive: true,
        },
    )
    .await
    .expect("shared runtime");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_ntrip(
        listener,
        Arc::new(MountpointHub::new(64).expect("hub")),
        TestAuthorizer,
        NtripServerConfig {
            head_timeout: Duration::from_secs(1),
            content_length_max_bytes: 1024,
            client_auth_required: false,
            session_limiter: Arc::new(
                LocalSessionLimiter::new(SessionLimitConfig {
                    default_max_ip_sessions: -1,
                    ..SessionLimitConfig::default()
                })
                .expect("limiter"),
            ),
            connection_limiter: Arc::new(ConnectionLimiter::new(100).expect("connections")),
            cluster_runtime: Some(shared.clone()),
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

    let mut source = TcpStream::connect(address).await.expect("source");
    source
        .write_all(b"SOURCE secret /C63\r\nUser-Agent: test-source\r\n\r\n")
        .await
        .expect("source request");
    let mut response = vec![0_u8; b"ICY 200 OK\r\n\r\n".len()];
    source
        .read_exact(&mut response)
        .await
        .expect("source response");
    assert_eq!(response, b"ICY 200 OK\r\n\r\n");
    let sessions = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let sessions = shared.sessions().await.expect("sessions");
            if !sessions.is_empty() {
                break sessions;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("source session");
    assert_eq!(sessions[0].ntrip_version.as_deref(), Some("1"));
    assert_eq!(sessions[0].software.as_deref(), Some("test-source"));

    let mut client = TcpStream::connect(address).await.expect("client");
    client
        .write_all(b"GET /C63\r\n\r\n")
        .await
        .expect("client request");
    let denied = read_http_head(&mut client).await;
    assert!(denied.starts_with(b"HTTP/1.1 403 Forbidden\r\n"));
    drop(source);

    let replacement = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match shared
                .admit_source("owner-id", "C63", "replacement", 3)
                .await
            {
                Ok(permit) => break permit,
                Err(AdmissionError::Source(ClaimError::AlreadyClaimed)) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("replacement failed: {error}"),
            }
        }
    })
    .await
    .expect("listener released source");
    shared
        .release_source(&replacement)
        .await
        .expect("release replacement");

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
    shared.shutdown().await.expect("runtime shutdown");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn source_and_clients_close_after_the_cluster_lease_is_removed() {
    let cluster = unique("ntrip-fence");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("coordinator"),
    );
    let shared = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        ClusterRuntimeOptions {
            cluster_id: cluster,
            caster_id: "caster-fence".to_owned(),
            runtime_epoch: unique("epoch"),
            relay_endpoint: "10.0.0.21:12102".to_owned(),
            lease_ttl_s: 30,
            keep_alive: true,
        },
    )
    .await
    .expect("shared runtime");
    tokio::time::timeout(Duration::from_secs(2), async {
        while !shared.lease_is_healthy().await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("lease healthy");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_ntrip(
        listener,
        Arc::new(MountpointHub::new(64).expect("hub")),
        TestAuthorizer,
        NtripServerConfig {
            head_timeout: Duration::from_secs(1),
            content_length_max_bytes: 1024,
            client_auth_required: false,
            session_limiter: Arc::new(
                LocalSessionLimiter::new(SessionLimitConfig {
                    default_max_ip_sessions: -1,
                    ..SessionLimitConfig::default()
                })
                .expect("limiter"),
            ),
            connection_limiter: Arc::new(ConnectionLimiter::new(100).expect("connections")),
            cluster_runtime: Some(shared.clone()),
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

    let mut source = TcpStream::connect(address).await.expect("source");
    source
        .write_all(b"SOURCE secret /C66\r\nUser-Agent: fence-test\r\n\r\n")
        .await
        .expect("source request");
    let mut response = vec![0_u8; b"ICY 200 OK\r\n\r\n".len()];
    source
        .read_exact(&mut response)
        .await
        .expect("source response");
    let mut client = TcpStream::connect(address).await.expect("client");
    client
        .write_all(b"GET /C66\r\n\r\n")
        .await
        .expect("client request");
    client
        .read_exact(&mut response)
        .await
        .expect("client response");
    shared.shutdown().await.expect("remove cluster lease");
    let mut byte = [0_u8; 1];
    let closed = tokio::time::timeout(Duration::from_secs(2), source.read(&mut byte))
        .await
        .expect("source fence timeout");
    assert!(matches!(closed, Ok(0) | Err(_)));
    let closed = tokio::time::timeout(Duration::from_secs(2), client.read(&mut byte))
        .await
        .expect("client fence timeout");
    assert!(matches!(closed, Ok(0) | Err(_)));

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn owner_command_closes_source_connection_and_releases_claim() {
    let cluster = unique("ntrip-command");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("coordinator"),
    );
    let shared = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        ClusterRuntimeOptions {
            cluster_id: cluster.clone(),
            caster_id: "caster-command".to_owned(),
            runtime_epoch: unique("epoch"),
            relay_endpoint: "10.0.0.21:12102".to_owned(),
            lease_ttl_s: 30,
            keep_alive: true,
        },
    )
    .await
    .expect("shared runtime");
    let connections = Arc::new(ConnectionController::default());
    let watcher = CommandWatcher::start(
        connect().await,
        &cluster,
        "caster-command",
        Arc::clone(&connections),
    )
    .await
    .expect("command watcher");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_ntrip(
        listener,
        Arc::new(MountpointHub::new(64).expect("hub")),
        TestAuthorizer,
        NtripServerConfig {
            head_timeout: Duration::from_secs(1),
            content_length_max_bytes: 1024,
            client_auth_required: false,
            session_limiter: Arc::new(
                LocalSessionLimiter::new(SessionLimitConfig::default()).expect("limiter"),
            ),
            connection_limiter: Arc::new(ConnectionLimiter::new(100).expect("connections")),
            cluster_runtime: Some(shared.clone()),
            relay_manager: None,
            source_admission: None,
            near: None,
            accounts: None,
            metrics: None,
            connections: Some(connections),
            proxy_protocol: None,
            auth_login_limiter: None,
        },
        async {
            let _ = shutdown_rx.await;
        },
    ));
    let mut source = TcpStream::connect(address).await.expect("source");
    source
        .write_all(b"SOURCE secret /C64\r\n\r\n")
        .await
        .expect("source request");
    let mut response = vec![0_u8; b"ICY 200 OK\r\n\r\n".len()];
    source
        .read_exact(&mut response)
        .await
        .expect("source response");
    let route = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(route) = shared.source_route("C64").await {
                break route;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("source route");
    shared
        .request_source_disconnect(&route)
        .await
        .expect("disconnect command");
    let mut byte = [0_u8; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), source.read(&mut byte))
            .await
            .expect("source close")
            .expect("source read"),
        0
    );
    let replacement = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match shared
                .admit_source("owner-id", "C64", "replacement-2", 2)
                .await
            {
                Ok(permit) => break permit,
                Err(AdmissionError::Source(ClaimError::AlreadyClaimed)) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("replacement failed: {error}"),
            }
        }
    })
    .await
    .expect("claim release");
    shared
        .release_source(&replacement)
        .await
        .expect("release replacement");
    watcher.shutdown().await;
    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("NTRIP server");
    shared.shutdown().await.expect("runtime shutdown");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn rejected_source_sets_cluster_cooldown_for_reconnect() {
    let cluster = unique("ntrip-cooldown");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("coordinator"),
    );
    let shared = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        ClusterRuntimeOptions {
            cluster_id: cluster,
            caster_id: "caster-cooldown-data".to_owned(),
            runtime_epoch: unique("epoch"),
            relay_endpoint: "10.0.0.22:12102".to_owned(),
            lease_ttl_s: 30,
            keep_alive: true,
        },
    )
    .await
    .expect("runtime");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_ntrip(
        listener,
        Arc::new(MountpointHub::new(64).expect("hub")),
        TestAuthorizer,
        NtripServerConfig {
            head_timeout: Duration::from_secs(1),
            content_length_max_bytes: 1024,
            client_auth_required: false,
            session_limiter: Arc::new(
                LocalSessionLimiter::new(SessionLimitConfig::default()).expect("limiter"),
            ),
            connection_limiter: Arc::new(ConnectionLimiter::new(100).expect("connections")),
            cluster_runtime: Some(shared.clone()),
            relay_manager: None,
            source_admission: Some(SourceAdmissionConfig {
                policy: AdmissionPolicy {
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
                max_bytes: 4096,
                max_frames: 8,
                timeout: Duration::from_secs(1),
                read_timeout: Duration::from_secs(1),
                str_refresh: true,
            }),
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
    let mut source = TcpStream::connect(address).await.expect("source");
    source
        .write_all(b"POST /C65 HTTP/1.1\r\nAuthorization: Basic b3duZXI6c2VjcmV0\r\n\r\n")
        .await
        .expect("source request");
    source.write_all(nmea_gga().as_bytes()).await.expect("nmea");
    assert!(
        read_http_head(&mut source)
            .await
            .starts_with(b"HTTP/1.1 200 OK\r\n")
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if shared
                .source_cooldown_remaining("C65", "127.0.0.1")
                .await
                .expect("cooldown")
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cooldown write");
    let mut retry = TcpStream::connect(address).await.expect("retry");
    retry
        .write_all(b"POST /C65 HTTP/1.1\r\nAuthorization: Basic b3duZXI6c2VjcmV0\r\n\r\n")
        .await
        .expect("retry request");
    assert!(
        read_http_head(&mut retry)
            .await
            .starts_with(b"HTTP/1.1 429 Too Many Requests\r\n")
    );
    drop(retry);
    drop(source);
    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server join").expect("server");
    shared.shutdown().await.expect("runtime shutdown");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn second_caster_serves_cached_etcd_str_without_local_source() {
    let cluster = unique("cross-node-str-cache");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("coordinator"),
    );
    let source_caster = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        ClusterRuntimeOptions {
            cluster_id: cluster.clone(),
            caster_id: "caster-source".to_owned(),
            runtime_epoch: unique("epoch-source"),
            relay_endpoint: "10.0.0.31:12102".to_owned(),
            lease_ttl_s: 30,
            keep_alive: true,
        },
    )
    .await
    .expect("source caster");
    let edge_caster = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        ClusterRuntimeOptions {
            cluster_id: cluster.clone(),
            caster_id: "caster-edge".to_owned(),
            runtime_epoch: unique("epoch-edge"),
            relay_endpoint: "10.0.0.32:12102".to_owned(),
            lease_ttl_s: 30,
            keep_alive: true,
        },
    )
    .await
    .expect("edge caster");
    let accounts = AccountDirectory::static_snapshot(
        AccountSnapshot::new(
            [User {
                schema_version: 1,
                mod_revision: 1,
                user_id: "owner-id".to_owned(),
                username: "owner".to_owned(),
                password_hash: "hash".to_owned(),
                auth_version: 1,
                enabled: true,
                ntrip_enabled: true,
                web_enabled: false,
                role: UserRole::User,
                created_at: "2026-11-24T00:00:00Z".to_owned(),
                updated_at: "2026-11-24T00:00:00Z".to_owned(),
            }],
            [Mountpoint {
                schema_version: 1,
                mod_revision: 1,
                mountpoint: "C66".to_owned(),
                owner_user_id: "owner-id".to_owned(),
                enabled: true,
                ntrip1_upload_password_hash: None,
                advertise: true,
                region: Some("east".to_owned()),
                latitude: None,
                longitude: None,
                coverage_radius_m: None,
                str_fields: None,
                source_expectation: None,
                created_at: "2026-11-24T00:00:00Z".to_owned(),
                updated_at: "2026-11-24T00:00:00Z".to_owned(),
            }],
        )
        .expect("accounts"),
    );
    let (source_address, source_shutdown, source_server) =
        start_cluster_listener(source_caster.clone(), accounts.clone()).await;
    let (edge_address, edge_shutdown, edge_server) =
        start_cluster_listener(edge_caster.clone(), accounts).await;

    let mut source = TcpStream::connect(source_address)
        .await
        .expect("source connect");
    source
        .write_all(b"SOURCE secret /C66\r\nUser-Agent: cache-test\r\n\r\n")
        .await
        .expect("source request");
    assert_eq!(read_http_head(&mut source).await, b"ICY 200 OK\r\n\r\n");
    source
        .write_all(&decode_hex(RTCM3_1005))
        .await
        .expect("position frame");
    source
        .write_all(&synthetic_rtcm3(1074))
        .await
        .expect("MSM frame");

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if edge_caster
                .source_route("C66")
                .await
                .is_some_and(|route| route.caster_id == "caster-source")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("edge source cache");
    assert!(
        edge_caster
            .sourcetable_records()
            .await
            .iter()
            .any(|record| record.mountpoint == "C66")
    );

    let mut edge_client = TcpStream::connect(edge_address)
        .await
        .expect("edge client connect");
    edge_client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nNtrip-Version: Ntrip/2.0\r\n\r\n")
        .await
        .expect("edge sourcetable request");
    let mut response = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(2),
        edge_client.read_to_end(&mut response),
    )
    .await
    .expect("edge response timeout")
    .expect("edge response");
    let response = String::from_utf8(response).expect("UTF-8 source table");
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("STR;C66;"));
    assert!(response.contains("1005"));
    assert!(response.contains("1074"));

    drop(source);
    source_shutdown.send(()).expect("source shutdown");
    edge_shutdown.send(()).expect("edge shutdown");
    source_server
        .await
        .expect("source server join")
        .expect("source server");
    edge_server
        .await
        .expect("edge server join")
        .expect("edge server");
    source_caster.shutdown().await.expect("source runtime");
    edge_caster.shutdown().await.expect("edge runtime");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

async fn start_cluster_listener(
    runtime: SharedClusterRuntime,
    accounts: AccountDirectory,
) -> (
    std::net::SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_ntrip(
        listener,
        Arc::new(MountpointHub::new(64).expect("hub")),
        TestAuthorizer,
        NtripServerConfig {
            head_timeout: Duration::from_secs(1),
            content_length_max_bytes: 1024,
            client_auth_required: false,
            session_limiter: Arc::new(
                LocalSessionLimiter::new(SessionLimitConfig::default()).expect("limiter"),
            ),
            connection_limiter: Arc::new(ConnectionLimiter::new(100).expect("connections")),
            cluster_runtime: Some(runtime),
            relay_manager: None,
            source_admission: Some(SourceAdmissionConfig {
                policy: AdmissionPolicy {
                    mode: AdmissionMode::Observe,
                    allowed_formats: vec![DataFormat::Rtcm3],
                    require_correction_data: true,
                    min_valid_frames: 1,
                    ..AdmissionPolicy::default()
                },
                max_bytes: 4096,
                max_frames: 8,
                timeout: Duration::from_millis(150),
                read_timeout: Duration::from_secs(2),
                str_refresh: true,
            }),
            near: None,
            accounts: Some(accounts),
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

fn nmea_gga() -> String {
    let body = "GPGGA,000000,0000.0000,N,00000.0000,E,1,08,0.9,0.0,M,0.0,M,,";
    let checksum = body.bytes().fold(0_u8, |checksum, byte| checksum ^ byte);
    format!("${body}*{checksum:02X}\r\n")
}

fn decode_hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
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

async fn read_http_head(stream: &mut TcpStream) -> Vec<u8> {
    let mut response = Vec::new();
    let mut chunk = [0_u8; 512];
    while !response.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut chunk).await.expect("HTTP response");
        assert!(read > 0, "connection closed before headers");
        response.extend_from_slice(&chunk[..read]);
    }
    response
}

async fn connect() -> Client {
    let endpoint = std::env::var("RCASTER_ETCD_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned());
    Client::connect([endpoint], Some(ConnectOptions::new()))
        .await
        .expect("etcd must be running")
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    )
}
