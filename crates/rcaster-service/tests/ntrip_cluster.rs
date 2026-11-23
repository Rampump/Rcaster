use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, ConnectOptions, PutOptions, Txn, TxnOp};
use rcaster_core::protocol::{AdmissionMode, AdmissionPolicy, DataFormat};
use rcaster_service::{
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
async fn source_stops_publishing_after_its_cluster_lease_is_removed() {
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
    shared.shutdown().await.expect("remove cluster lease");
    let mut byte = [0_u8; 1];
    let closed = tokio::time::timeout(Duration::from_secs(2), source.read(&mut byte))
        .await
        .expect("source fence timeout");
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
                },
                max_bytes: 4096,
                max_frames: 8,
                timeout: Duration::from_secs(1),
                read_timeout: Duration::from_secs(1),
            }),
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

fn nmea_gga() -> String {
    let body = "GPGGA,000000,0000.0000,N,00000.0000,E,1,08,0.9,0.0,M,0.0,M,,";
    let checksum = body.bytes().fold(0_u8, |checksum, byte| checksum ^ byte);
    format!("${body}*{checksum:02X}\r\n")
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
