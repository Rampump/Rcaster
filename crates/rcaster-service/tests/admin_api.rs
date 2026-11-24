use std::{fs, sync::Arc, time::Duration};

use ed25519_dalek::{Signer, SigningKey};
use etcd_client::{Client, ConnectOptions, GetOptions};
use futures_util::StreamExt;
use rcaster_core::{account::AccountSnapshot, protocol::DataProfile, str_record::StrRecord};
use rcaster_service::{
    admin::{AdminState, serve_admin_mtls},
    admission::SessionRecord,
    cluster_runtime::{ClusterRuntimeOptions, SharedClusterRuntime},
    config::{CasterConfig, publish_config},
    connections::ConnectionController,
    coordinator_admission::{AdmissionCoordinator, CoordinatorOptions},
    coordinator_grpc::GrpcServerTls,
    health::HealthState,
    hub::MountpointHub,
    upgrade::{SignedUpgradeManifest, UpgradeManager, UpgradeManifest, UpgradePolicy},
};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{PrivatePkcs8KeyDer, ServerName},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

#[tokio::test]
async fn admin_routes_require_mtls_and_service_token() {
    let pki = TestPki::generate();
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("admin listener");
    let address = listener.local_addr().expect("admin address");
    let hub = Arc::new(MountpointHub::new(8).expect("hub"));
    let source = hub.claim_source("C63").expect("source");
    source
        .set_str_record(StrRecord::from_profile("C63", &DataProfile::default()))
        .expect("activate source");
    source
        .set_profile(DataProfile::default())
        .expect("source profile");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let state = AdminState {
        cluster_id: "cluster".to_owned(),
        caster_id: "caster".to_owned(),
        token: "secret".to_owned(),
        ready: Arc::new(HealthState::default()),
        hub,
        runtime: None,
        relays: None,
        near_mountpoint: Some("AUTO_NEAR".to_owned()),
        accounts: rcaster_service::account_directory::AccountDirectory::static_snapshot(
            AccountSnapshot::default(),
        ),
        relay_tls: None,
        upgrades: None,
        proxy_protocol: None,
        maintenance_state: None,
        listener_tls: None,
        config_revision_retention: 200,
        login_limiter: Default::default(),
        audit: Default::default(),
        websockets: Default::default(),
        connections: Arc::new(ConnectionController::default()),
        auth_limiter: Default::default(),
    };
    let server = tokio::spawn(serve_admin_mtls(
        listener,
        pki.server_tls(),
        state,
        async move {
            let _ = shutdown_rx.await;
        },
    ));
    let page = request_raw(address, &pki, "GET", "/admin/", "", "", "").await;
    assert!(page.starts_with("HTTP/1.1 200 OK"));
    assert!(page.contains("RCaster Admin"));
    assert!(page.contains("Content-Security-Policy"));
    assert!(page.contains("<h3>Users</h3>"));
    assert!(page.contains("<h3>Sessions</h3>"));
    assert!(page.contains("addBlacklist()"));
    assert!(page.contains("deleteBlacklist"));
    assert!(page.contains("If-Match"));
    assert!(page.contains("showSourceHealth"));
    assert!(page.contains("saveUser"));
    assert!(page.contains("saveMount"));
    assert!(page.contains("createApiToken"));
    assert!(page.contains("id=\"api-tokens-section\""));
    assert!(page.contains("session.kind==='service'"));
    assert!(page.contains("disconnectSource"));
    assert!(page.contains("disconnectSession"));
    assert!(page.contains("monitorSource"));
    assert!(page.contains("clearCooldown"));
    assert!(page.contains("Source cooldowns"));
    assert!(page.contains("admin/v1/auth/login"));
    assert!(page.contains("setInterval"));

    let unauthorized = request(address, &pki, "/admin/v1/cluster/status", "Bearer wrong").await;
    assert!(unauthorized.starts_with("HTTP/1.1 401"));
    assert!(unauthorized.contains("\"details\":null"));
    assert!(unauthorized.contains("\"request_id\":\""));
    assert!(unauthorized.contains("X-Request-ID:"));
    let response = request(address, &pki, "/admin/v1/cluster/status", "Bearer secret").await;
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("\"cluster_id\":\"cluster\""));
    let cluster_status = response_body(&response);
    assert_eq!(cluster_status["local_active_sources"], 1);
    assert_eq!(cluster_status["cluster_active_sources"], 1);
    assert!(cluster_status.get("active_sources").is_none());
    let session = request(address, &pki, "/admin/v1/auth/session", "Bearer secret").await;
    assert_eq!(response_body(&session)["kind"], "service");
    assert!(
        request(address, &pki, "/admin/v1/auth/csrf", "Bearer secret")
            .await
            .starts_with("HTTP/1.1 403 Forbidden")
    );
    assert!(
        request(address, &pki, "/admin/v1/api-tokens", "Bearer secret")
            .await
            .starts_with("HTTP/1.1 403 Forbidden")
    );
    assert!(
        request_fragmented_get(address, &pki, "/admin/v1/cluster/status", "Bearer secret")
            .await
            .starts_with("HTTP/1.1 200 OK")
    );
    let near = request(address, &pki, "/admin/v1/near", "Bearer secret").await;
    assert!(near.contains("\"mountpoint\":\"AUTO_NEAR\""));
    let detail = request(address, &pki, "/admin/v1/sources/C63", "Bearer secret").await;
    assert!(detail.contains("\"mountpoint\":\"C63\""));
    let filtered = request(
        address,
        &pki,
        "/admin/v1/sources?primary_format=unknown&caster_id=caster&limit=1",
        "Bearer secret",
    )
    .await;
    assert!(filtered.contains("\"mountpoint\":\"C63\""));
    assert!(filtered.contains("\"caster_id\":\"caster\""));
    assert!(filtered.contains("\"next_cursor\":null"));
    let filtered_body = response_body(&filtered);
    assert_eq!(
        filtered_body
            .get("sources")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len),
        Some(1)
    );
    assert!(filtered_body.get("next_cursor").is_some());
    assert!(
        request(
            address,
            &pki,
            "/admin/v1/sources?primary_format=nmea",
            "Bearer secret",
        )
        .await
        .contains("\"sources\":[]")
    );
    let summary = request(
        address,
        &pki,
        "/admin/v1/streams/C63/summary",
        "Bearer secret",
    )
    .await;
    assert!(summary.contains("\"subscribers\":0"));
    assert!(summary.contains("\"valid_frames\":0"));
    assert!(summary.contains("\"packets\":0"));
    assert!(summary.contains("\"bitrate_bps\":null"));
    assert!(summary.contains("\"stream_health_warning\":true"));
    let health = request(
        address,
        &pki,
        "/admin/v1/sources/C63/health",
        "Bearer secret",
    )
    .await;
    assert!(health.contains("\"stream_health_warning\":true"));
    assert!(health.contains("\"last_correction_age_seconds\":null"));
    assert!(
        request(address, &pki, "/admin/v1/users", "Bearer secret")
            .await
            .contains("\"users\":[]")
    );
    assert!(
        request(address, &pki, "/admin/v1/mountpoints", "Bearer secret")
            .await
            .contains("\"mountpoints\":[]")
    );
    assert!(
        request(address, &pki, "/admin/v1/blacklist", "Bearer secret")
            .await
            .contains("\"rules\":[]")
    );
    assert!(
        request(address, &pki, "/admin/v1/sessions", "Bearer secret")
            .await
            .contains("\"sessions\":[]")
    );
    assert!(
        request(address, &pki, "/admin/v1/config/status", "Bearer secret")
            .await
            .contains("\"available\":false")
    );
    assert!(
        request(address, &pki, "/admin/v1/versions", "Bearer secret")
            .await
            .contains("\"nodes\":[]")
    );

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server task").expect("admin server");
}

async fn request(
    address: std::net::SocketAddr,
    pki: &TestPki,
    path: &str,
    authorization: &str,
) -> String {
    request_raw(address, pki, "GET", path, authorization, "", "").await
}

async fn request_raw(
    address: std::net::SocketAddr,
    pki: &TestPki,
    method: &str,
    path: &str,
    authorization: &str,
    extra_headers: &str,
    body: &str,
) -> String {
    let tcp = TcpStream::connect(address).await.expect("admin connect");
    let mut roots = RootCertStore::empty();
    roots.add(pki.ca_certificate.der().clone()).expect("CA");
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(
            vec![pki.client_certificate.der().clone()],
            PrivatePkcs8KeyDer::from(pki.client_key.serialize_der()).into(),
        )
        .expect("client TLS");
    let mut stream = TlsConnector::from(Arc::new(config))
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .expect("admin TLS");
    stream
        .write_all(
            format!(
                "{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: {authorization}\r\n{extra_headers}Content-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .expect("admin request");
    let mut response = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        match stream.read(&mut buffer).await {
            Ok(0) => break,
            Ok(read) => response.extend_from_slice(&buffer[..read]),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => panic!("admin response: {error}"),
        }
    }
    String::from_utf8(response).expect("UTF-8 admin response")
}

async fn request_fragmented_get(
    address: std::net::SocketAddr,
    pki: &TestPki,
    path: &str,
    authorization: &str,
) -> String {
    let tcp = TcpStream::connect(address).await.expect("admin connect");
    let mut roots = RootCertStore::empty();
    roots.add(pki.ca_certificate.der().clone()).expect("CA");
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(
            vec![pki.client_certificate.der().clone()],
            PrivatePkcs8KeyDer::from(pki.client_key.serialize_der()).into(),
        )
        .expect("client TLS");
    let mut stream = TlsConnector::from(Arc::new(config))
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .expect("admin TLS");
    for fragment in [
        format!("GET {path} HTTP/1.1\r\n"),
        "Host: localhost\r\n".to_owned(),
        format!("Authorization: {authorization}\r\n"),
        "\r\n".to_owned(),
    ] {
        stream
            .write_all(fragment.as_bytes())
            .await
            .expect("fragmented admin request");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let mut response = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        match stream.read(&mut buffer).await {
            Ok(0) => break,
            Ok(read) => response.extend_from_slice(&buffer[..read]),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => panic!("fragmented admin response: {error}"),
        }
    }
    String::from_utf8(response).expect("UTF-8 admin response")
}

#[tokio::test]
async fn admin_writes_blacklist_with_revision_cas() {
    let cluster = unique("admin-blacklist");
    let command_cluster = cluster.clone();
    let mut etcd = connect().await;
    publish_config(&mut etcd, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("coordinator"),
    );
    let runtime = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        ClusterRuntimeOptions {
            cluster_id: cluster.clone(),
            caster_id: "caster-admin".to_owned(),
            runtime_epoch: unique("epoch"),
            relay_endpoint: "127.0.0.1:12102".to_owned(),
            lease_ttl_s: 30,
            keep_alive: true,
        },
    )
    .await
    .expect("runtime");
    let pki = TestPki::generate();
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("admin listener");
    let address = listener.local_addr().expect("admin address");
    let hub = Arc::new(MountpointHub::new(8).expect("hub"));
    let remote_relay_source = hub.claim_source("REMOTE").expect("remote relay source");
    let remote_record = StrRecord::from_profile("REMOTE", &DataProfile::default());
    remote_relay_source
        .set_str_record(remote_record.clone())
        .expect("remote relay STR");
    etcd.put(
        format!("/rcaster/{cluster}/runtime/sources/REMOTE"),
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "state": "ACTIVE",
            "source_generation": 7,
            "connection_id": "remote-connection",
            "mountpoint": "REMOTE",
            "caster_id": "caster-remote",
            "runtime_epoch": "remote-epoch",
            "relay_endpoint": "127.0.0.1:12103",
            "effective_user_id": "owner",
            "created_at": "0",
            "str": remote_record,
        }))
        .expect("encode remote source"),
        None,
    )
    .await
    .expect("publish remote source");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if runtime
                .source_route("REMOTE")
                .await
                .is_some_and(|route| route.caster_id == "caster-remote")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("remote source route");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let accounts = runtime
        .account_directory()
        .await
        .expect("account directory");
    let upgrade_key = SigningKey::from_bytes(&[11_u8; 32]);
    let upgrades = Some(UpgradeManager::new(
        connect().await,
        cluster.clone(),
        UpgradePolicy::new(
            &[upgrade_key
                .verifying_key()
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()],
            &["updates.example.test".to_owned()],
        )
        .expect("upgrade policy"),
    ));
    let audit_path = std::env::temp_dir().join(format!("{cluster}.audit.jsonl"));
    let audit = rcaster_service::audit::AuditLog::open(&audit_path).expect("audit log");
    let maintenance_path = std::env::temp_dir().join(format!("{cluster}.maintenance"));
    let _ = fs::remove_file(&maintenance_path);
    let server = tokio::spawn(serve_admin_mtls(
        listener,
        pki.server_tls(),
        AdminState {
            cluster_id: cluster,
            caster_id: "caster-admin".to_owned(),
            token: "secret".to_owned(),
            ready: Arc::new(HealthState::default()),
            hub,
            runtime: Some(runtime.clone()),
            relays: None,
            near_mountpoint: None,
            accounts: accounts.clone(),
            relay_tls: None,
            upgrades,
            proxy_protocol: None,
            maintenance_state: Some(maintenance_path.clone()),
            listener_tls: None,
            config_revision_retention: 200,
            login_limiter: Default::default(),
            audit,
            websockets: Default::default(),
            connections: Arc::new(ConnectionController::default()),
            auth_limiter: Default::default(),
        },
        async move {
            let _ = shutdown_rx.await;
        },
    ));

    let created = request_raw(
        address,
        &pki,
        "POST",
        "/admin/v1/blacklist",
        "Bearer secret",
        "",
        r#"{"id":"blocked","cidr":"192.0.2.0/24","quota":0,"enabled":true,"description":"test"}"#,
    )
    .await;
    assert!(created.starts_with("HTTP/1.1 201 Created"));
    let remote_source = response_body(
        &request(
            address,
            &pki,
            "/admin/v1/sources?caster_id=caster-remote",
            "Bearer secret",
        )
        .await,
    );
    assert_eq!(
        remote_source
            .get("sources")
            .and_then(serde_json::Value::as_array)
            .and_then(|sources| sources.first())
            .and_then(|source| source.get("caster_id"))
            .and_then(serde_json::Value::as_str),
        Some("caster-remote")
    );
    let revision = created
        .split("\r\n\r\n")
        .nth(1)
        .and_then(|body| serde_json::from_str::<serde_json::Value>(body).ok())
        .and_then(|body| body.get("revision").and_then(serde_json::Value::as_u64))
        .expect("created revision");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if request(address, &pki, "/admin/v1/blacklist", "Bearer secret")
                .await
                .contains("\"id\":\"blocked\"")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("blacklist create propagation");
    assert!(
        request(
            address,
            &pki,
            "/admin/v1/blacklist/blocked",
            "Bearer secret"
        )
        .await
        .contains("\"cidr\":\"192.0.2.0/24\"")
    );
    let updated = request_raw(
        address,
        &pki,
        "PATCH",
        "/admin/v1/blacklist",
        "Bearer secret",
        &format!("If-Match: \"{revision}\"\r\n"),
        r#"{"id":"blocked","cidr":"192.0.2.0/24","quota":2,"enabled":true,"description":"updated"}"#,
    )
    .await;
    assert!(updated.starts_with("HTTP/1.1 200 OK"));
    let revision = updated
        .split("\r\n\r\n")
        .nth(1)
        .and_then(|body| serde_json::from_str::<serde_json::Value>(body).ok())
        .and_then(|body| body.get("revision").and_then(serde_json::Value::as_u64))
        .expect("updated revision");
    assert!(
        request(
            address,
            &pki,
            "/admin/v1/blacklist/blocked",
            "Bearer secret"
        )
        .await
        .contains("\"quota\":2")
    );
    let deleted = request_raw(
        address,
        &pki,
        "DELETE",
        "/admin/v1/blacklist/blocked",
        "Bearer secret",
        &format!("If-Match: \"{revision}\"\r\n"),
        "",
    )
    .await;
    assert!(deleted.starts_with("HTTP/1.1 204 No Content"));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if !request(address, &pki, "/admin/v1/blacklist", "Bearer secret")
                .await
                .contains("\"id\":\"blocked\"")
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("blacklist delete propagation");

    let created_user = request_raw(
        address,
        &pki,
        "POST",
        "/admin/v1/users",
        "Bearer secret",
        "",
        r#"{"user_id":"editor-id","username":"editor","password":"secret","role":"user","enabled":true,"ntrip_enabled":true,"web_enabled":true}"#,
    )
    .await;
    assert!(created_user.starts_with("HTTP/1.1 201 Created"));
    let mut user_revision = body_revision(&created_user);
    assert!(
        request(address, &pki, "/admin/v1/users/editor-id", "Bearer secret")
            .await
            .contains("\"username\":\"editor\"")
    );
    let created_mount = request_raw(
        address,
        &pki,
        "POST",
        "/admin/v1/mountpoints",
        "Bearer secret",
        "",
        r#"{"mountpoint":"C64","owner_user_id":"editor-id","ntrip1_upload_password":"upload","source_expectation":{"formats":["rtcm3"],"msm_versions":[4]}}"#,
    )
    .await;
    assert!(created_mount.starts_with("HTTP/1.1 201 Created"));
    let mut mount_revision = body_revision(&created_mount);
    assert!(
        request_raw(
            address,
            &pki,
            "DELETE",
            "/admin/v1/users/editor-id",
            "Bearer secret",
            &format!("If-Match: \"{user_revision}\"\r\n"),
            "",
        )
        .await
        .starts_with("HTTP/1.1 409 Conflict")
    );

    let updated_user = request_raw(
        address,
        &pki,
        "PATCH",
        "/admin/v1/users/editor-id",
        "Bearer secret",
        &format!("If-Match: \"{user_revision}\"\r\n"),
        r#"{"enabled":false}"#,
    )
    .await;
    assert!(updated_user.starts_with("HTTP/1.1 200 OK"));
    user_revision = body_revision(&updated_user);
    assert!(
        request_raw(
            address,
            &pki,
            "PATCH",
            "/admin/v1/users/editor-id",
            "Bearer secret",
            &format!("If-Match: \"{}\"\r\n", user_revision - 1),
            r#"{"enabled":true}"#,
        )
        .await
        .starts_with("HTTP/1.1 409 Conflict")
    );
    let reenabled_user = request_raw(
        address,
        &pki,
        "PATCH",
        "/admin/v1/users/editor-id",
        "Bearer secret",
        &format!("If-Match: \"{user_revision}\"\r\n"),
        r#"{"enabled":true}"#,
    )
    .await;
    assert!(reenabled_user.starts_with("HTTP/1.1 200 OK"));
    user_revision = body_revision(&reenabled_user);

    tokio::time::timeout(Duration::from_secs(2), async {
        while !runtime.is_healthy().await {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("cluster runtime healthy");
    let c64_permit = runtime
        .admit_source("editor-id", "C64", "c64-test-connection", 2)
        .await
        .expect("C64 source admission");
    let c64_record = StrRecord::from_profile("C64", &DataProfile::default());
    runtime
        .activate_source(&c64_permit, &c64_record)
        .await
        .expect("C64 source activation");
    tokio::time::timeout(Duration::from_secs(2), async {
        while runtime.source_route("C64").await.is_none() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("C64 source route");

    let updated_mount = request_raw(
        address,
        &pki,
        "PATCH",
        "/admin/v1/mountpoints/C64",
        "Bearer secret",
        &format!("If-Match: \"{mount_revision}\"\r\n"),
        r#"{"region":"east","advertise":false,"enabled":false,"source_expectation":{"formats":["rtcm3"],"msm_versions":[4,7]}}"#,
    )
    .await;
    assert!(updated_mount.starts_with("HTTP/1.1 200 OK"));
    assert!(updated_mount.contains("\"affected_connections\":1"));
    mount_revision = body_revision(&updated_mount);
    let cluster_id = runtime
        .cluster_id()
        .await
        .expect("cluster ID for source command");
    let commands = etcd
        .get(
            format!("/rcaster/{cluster_id}/runtime/source-commands/C64/"),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .expect("C64 source commands");
    assert_eq!(commands.kvs().len(), 1);
    let mountpoint = request(address, &pki, "/admin/v1/mountpoints/C64", "Bearer secret").await;
    assert!(mountpoint.contains("\"region\":\"east\""));
    assert!(mountpoint.contains("\"advertise\":false"));
    assert!(mountpoint.contains("\"enabled\":false"));
    assert!(
        mountpoint
            .contains("\"source_expectation\":{\"formats\":[\"rtcm3\"],\"msm_versions\":[4,7]}")
    );
    assert!(!mountpoint.contains("upload"));
    assert!(accounts.snapshot().user_by_id("editor-id").is_some());
    assert!(
        request_raw(
            address,
            &pki,
            "DELETE",
            "/admin/v1/mountpoints/C64",
            "Bearer secret",
            &format!("If-Match: \"{mount_revision}\"\r\n"),
            "",
        )
        .await
        .starts_with("HTTP/1.1 409 Conflict")
    );

    let cleanup_runtime = runtime.clone();
    let cleanup = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        cleanup_runtime
            .release_source(&c64_permit)
            .await
            .expect("release C64 source");
    });
    let deleted_mount = request_raw(
        address,
        &pki,
        "DELETE",
        "/admin/v1/mountpoints/C64?force=true",
        "Bearer secret",
        &format!("If-Match: \"{mount_revision}\"\r\n"),
        "",
    )
    .await;
    assert!(deleted_mount.starts_with("HTTP/1.1 204 No Content"));
    cleanup.await.expect("C64 cleanup task");
    let deleted_user = request_raw(
        address,
        &pki,
        "DELETE",
        "/admin/v1/users/editor-id",
        "Bearer secret",
        &format!("If-Match: \"{user_revision}\"\r\n"),
        "",
    )
    .await;
    assert!(deleted_user.starts_with("HTTP/1.1 204 No Content"));

    let admin_user = request_raw(
        address,
        &pki,
        "POST",
        "/admin/v1/users",
        "Bearer secret",
        "",
        r#"{"user_id":"admin-http","username":"admin-http","password":"admin-password","role":"admin","enabled":true,"ntrip_enabled":false,"web_enabled":true}"#,
    )
    .await;
    assert!(admin_user.starts_with("HTTP/1.1 201 Created"));
    let login = request_raw(
        address,
        &pki,
        "POST",
        "/admin/v1/auth/login",
        "",
        "",
        r#"{"username":"admin-http","password":"admin-password"}"#,
    )
    .await;
    assert!(login.starts_with("HTTP/1.1 200 OK"));
    assert!(login.contains("HttpOnly"));
    assert!(login.contains("SameSite=Strict"));
    assert!(login.contains("Path=/admin"));
    let cookie = response_header(&login, "set-cookie")
        .and_then(|value| value.split(';').next())
        .expect("session cookie")
        .to_owned();
    let csrf = response_body(&login)
        .get("csrf_token")
        .and_then(serde_json::Value::as_str)
        .expect("csrf token")
        .to_owned();
    assert!(
        request_raw(
            address,
            &pki,
            "GET",
            "/admin/v1/auth/session",
            "",
            &format!("Cookie: {cookie}\r\n"),
            "",
        )
        .await
        .contains("\"username\":\"admin-http\"")
    );
    assert!(
        request_raw(
            address,
            &pki,
            "POST",
            "/admin/v1/users",
            "",
            &format!("Cookie: {cookie}\r\n"),
            r#"{"user_id":"csrf-user","username":"csrf-user","password":"secret"}"#,
        )
        .await
        .starts_with("HTTP/1.1 403 Forbidden")
    );
    assert!(
        request_raw(
            address,
            &pki,
            "POST",
            "/admin/v1/users",
            "",
            &format!("Cookie: {cookie}\r\nX-CSRF-Token: {csrf}\r\n"),
            r#"{"user_id":"csrf-user","username":"csrf-user","password":"secret"}"#,
        )
        .await
        .starts_with("HTTP/1.1 201 Created")
    );
    let created_token = request_raw(
        address,
        &pki,
        "POST",
        "/admin/v1/api-tokens",
        "",
        &format!("Cookie: {cookie}\r\nX-CSRF-Token: {csrf}\r\n"),
        r#"{"name":"automation"}"#,
    )
    .await;
    assert!(created_token.starts_with("HTTP/1.1 201 Created"));
    let token_body = response_body(&created_token);
    let token = token_body
        .get("token")
        .and_then(serde_json::Value::as_str)
        .expect("created token")
        .to_owned();
    let token_id = token_body
        .get("token_id")
        .and_then(serde_json::Value::as_str)
        .expect("token ID")
        .to_owned();
    assert!(
        request_raw(
            address,
            &pki,
            "GET",
            "/admin/v1/cluster/status",
            &format!("Bearer {token}"),
            "",
            "",
        )
        .await
        .starts_with("HTTP/1.1 200 OK")
    );
    assert!(
        request_raw(
            address,
            &pki,
            "DELETE",
            &format!("/admin/v1/api-tokens/{token_id}"),
            "",
            &format!("Cookie: {cookie}\r\nX-CSRF-Token: {csrf}\r\n"),
            "",
        )
        .await
        .starts_with("HTTP/1.1 204 No Content")
    );
    assert!(
        request_raw(
            address,
            &pki,
            "GET",
            "/admin/v1/cluster/status",
            &format!("Bearer {token}"),
            "",
            "",
        )
        .await
        .starts_with("HTTP/1.1 401 Unauthorized")
    );
    let read_only = request_raw(
        address,
        &pki,
        "POST",
        "/admin/v1/api-tokens",
        "",
        &format!("Cookie: {cookie}\r\nX-CSRF-Token: {csrf}\r\n"),
        r#"{"name":"read-only","scopes":["admin:read"]}"#,
    )
    .await;
    assert!(read_only.starts_with("HTTP/1.1 201 Created"));
    let read_only_token = response_body(&read_only)["token"]
        .as_str()
        .expect("read-only token")
        .to_owned();
    assert!(
        request_raw(
            address,
            &pki,
            "GET",
            "/admin/v1/config/status",
            &format!("Bearer {read_only_token}"),
            "",
            "",
        )
        .await
        .starts_with("HTTP/1.1 200 OK")
    );
    assert!(
        request_raw(
            address,
            &pki,
            "POST",
            "/admin/v1/operations/preview",
            &format!("Bearer {read_only_token}"),
            "",
            r#"{"action":"config.rollback","generation":2}"#,
        )
        .await
        .starts_with("HTTP/1.1 403 Forbidden")
    );
    let read_only_id = read_only_token
        .strip_prefix("rcaster_")
        .and_then(|token| token.split_once('.'))
        .map(|(token_id, _)| token_id)
        .expect("read-only token ID");
    let listed_tokens = request(
        address,
        &pki,
        "/admin/v1/api-tokens",
        &format!("Bearer {read_only_token}"),
    )
    .await;
    let listed_tokens = response_body(&listed_tokens);
    let read_only_view = listed_tokens["tokens"]
        .as_array()
        .unwrap_or_else(|| panic!("token list: {listed_tokens}"))
        .iter()
        .find(|token| token["token_id"] == read_only_id)
        .expect("read-only token view");
    assert!(read_only_view["last_used_at"].as_u64().is_some());
    let write_only = request_raw(
        address,
        &pki,
        "POST",
        "/admin/v1/api-tokens",
        "",
        &format!("Cookie: {cookie}\r\nX-CSRF-Token: {csrf}\r\n"),
        r#"{"name":"write-only","scopes":["admin:write"]}"#,
    )
    .await;
    let write_only_token = response_body(&write_only)["token"]
        .as_str()
        .expect("write-only token")
        .to_owned();
    assert!(
        request_raw(
            address,
            &pki,
            "POST",
            "/admin/v1/api-tokens",
            &format!("Bearer {write_only_token}"),
            "",
            r#"{"name":"escalated","scopes":["upgrade:write"]}"#,
        )
        .await
        .starts_with("HTTP/1.1 403 Forbidden")
    );
    assert!(
        request_raw(
            address,
            &pki,
            "POST",
            "/admin/v1/upgrades/preview",
            &format!("Bearer {write_only_token}"),
            "",
            "{}",
        )
        .await
        .starts_with("HTTP/1.1 403 Forbidden")
    );
    let upgrade_only = request_raw(
        address,
        &pki,
        "POST",
        "/admin/v1/api-tokens",
        "",
        &format!("Cookie: {cookie}\r\nX-CSRF-Token: {csrf}\r\n"),
        r#"{"name":"upgrade-only","scopes":["upgrade:write"]}"#,
    )
    .await;
    let upgrade_only_token = response_body(&upgrade_only)["token"]
        .as_str()
        .expect("upgrade-only token")
        .to_owned();
    assert!(
        !request_raw(
            address,
            &pki,
            "POST",
            "/admin/v1/upgrades/preview",
            &format!("Bearer {upgrade_only_token}"),
            "",
            "{}",
        )
        .await
        .starts_with("HTTP/1.1 403 Forbidden")
    );
    let expiring = request_raw(
        address,
        &pki,
        "POST",
        "/admin/v1/api-tokens",
        "",
        &format!("Cookie: {cookie}\r\nX-CSRF-Token: {csrf}\r\n"),
        r#"{"name":"expiring","scopes":["admin:read"],"expires_in_s":1}"#,
    )
    .await;
    let expiring_token = response_body(&expiring)["token"]
        .as_str()
        .expect("expiring token")
        .to_owned();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        request_raw(
            address,
            &pki,
            "GET",
            "/admin/v1/config/status",
            &format!("Bearer {expiring_token}"),
            "",
            "",
        )
        .await
        .starts_with("HTTP/1.1 401 Unauthorized")
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let audit = request(address, &pki, "/admin/v1/audit?limit=100", "Bearer secret").await;
            if audit.contains("\"action\":\"user.create\"")
                && audit.contains("\"action\":\"api_token.create\"")
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("audit write");
    let config = request_raw(
        address,
        &pki,
        "PUT",
        "/admin/v1/config",
        "Bearer secret",
        "",
        r#"{"schema_version":1,"generation":2,"source_health":{"observation_timeout_s":12,"crc_error_ratio_max":0.2}}"#,
    )
    .await;
    assert!(config.starts_with("HTTP/1.1 200 OK"));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if runtime
                .config_snapshot()
                .await
                .is_some_and(|snapshot| snapshot.config.generation == 2)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("config watch");
    let snapshot = runtime.config_snapshot().await.expect("config snapshot");
    assert_eq!(snapshot.config.source_health.observation_timeout_s, 12);
    assert_eq!(snapshot.config.source_health.crc_error_ratio_max, 0.2);
    let revisions = request(
        address,
        &pki,
        "/admin/v1/config/revisions?limit=10",
        "Bearer secret",
    )
    .await;
    assert!(revisions.contains("\"generation\":2"));
    assert!(revisions.contains("\"revision\":"));
    assert!(
        request_raw(
            address,
            &pki,
            "POST",
            "/admin/v1/config/reload",
            "Bearer secret",
            "",
            "{}",
        )
        .await
        .contains("\"generation\":2")
    );
    let preview = request_raw(
        address,
        &pki,
        "POST",
        "/admin/v1/operations/preview",
        "Bearer secret",
        "",
        r#"{"action":"config.rollback","generation":2}"#,
    )
    .await;
    assert!(preview.starts_with("HTTP/1.1 200 OK"));
    assert!(preview.contains("\"result_generation\":3"));
    let operation = request_raw(
        address,
        &pki,
        "POST",
        "/admin/v1/operations",
        "Bearer secret",
        "",
        r#"{"action":"config.rollback","generation":2}"#,
    )
    .await;
    assert!(operation.starts_with("HTTP/1.1 202 Accepted"));
    let operation_id = response_body(&operation)["operation_id"]
        .as_str()
        .expect("operation ID")
        .to_owned();
    assert!(
        request(
            address,
            &pki,
            &format!("/admin/v1/operations/{operation_id}"),
            "Bearer secret",
        )
        .await
        .contains("\"status\":\"SUCCEEDED\"")
    );
    assert!(
        request_raw(
            address,
            &pki,
            "POST",
            &format!("/admin/v1/operations/{operation_id}/cancel"),
            "Bearer secret",
            "",
            "{}",
        )
        .await
        .starts_with("HTTP/1.1 409")
    );
    assert!(
        request(address, &pki, "/admin/v1/near/clients", "Bearer secret")
            .await
            .contains("\"clients\":[]")
    );
    assert!(
        request_raw(
            address,
            &pki,
            "POST",
            "/admin/v1/maintenance",
            "Bearer secret",
            "",
            r#"{"maintenance":true}"#,
        )
        .await
        .contains("\"maintenance\":true")
    );
    assert_eq!(
        fs::read_to_string(&maintenance_path).expect("maintenance state"),
        "true\n"
    );
    assert!(
        request(address, &pki, "/admin/v1/maintenance", "Bearer secret")
            .await
            .contains("\"maintenance\":true")
    );
    assert!(
        request_raw(
            address,
            &pki,
            "POST",
            "/admin/v1/maintenance",
            "Bearer secret",
            "",
            r#"{"maintenance":false}"#,
        )
        .await
        .contains("\"maintenance\":false")
    );
    assert_eq!(
        fs::read_to_string(&maintenance_path).expect("maintenance state"),
        "false\n"
    );
    let signed = signed_upgrade_manifest(&upgrade_key, "9.9.9");
    let mut upgrade = json!({
        "manifest": signed.manifest,
        "signature": signed.signature,
        "nodes": [{"caster_id": "caster-admin", "role": "core"}],
        "max_unavailable_core": 1,
        "max_unavailable_edge": 1,
        "post_upgrade_stability_s": 0,
        "auto_rollback": true
    });
    let upgrade_preview = request_raw(
        address,
        &pki,
        "POST",
        "/admin/v1/upgrades/preview",
        "Bearer secret",
        "",
        &upgrade.to_string(),
    )
    .await;
    assert!(upgrade_preview.starts_with("HTTP/1.1 200 OK"));
    upgrade["preview_digest"] = response_body(&upgrade_preview)["preview_digest"].clone();
    let upgrade_start = request_raw(
        address,
        &pki,
        "POST",
        "/admin/v1/upgrades",
        "Bearer secret",
        "",
        &upgrade.to_string(),
    )
    .await;
    assert!(
        upgrade_start.starts_with("HTTP/1.1 202 Accepted"),
        "upgrade start response: {upgrade_start}"
    );
    let upgrade_id = response_body(&upgrade_start)["operation_id"]
        .as_str()
        .expect("upgrade ID")
        .to_owned();
    assert!(
        request(
            address,
            &pki,
            &format!("/admin/v1/upgrades/{upgrade_id}"),
            "Bearer secret",
        )
        .await
        .contains("\"status\":\"RUNNING\"")
    );
    assert!(
        request_raw(
            address,
            &pki,
            "POST",
            &format!("/admin/v1/upgrades/{upgrade_id}/cancel"),
            "Bearer secret",
            "",
            "{}",
        )
        .await
        .starts_with("HTTP/1.1 202 Accepted")
    );
    let session = SessionRecord::source(
        "command-session",
        "owner",
        "C63",
        "caster-admin",
        "epoch-command",
        "2026-11-22T00:00:00Z",
    );
    etcd.put(
        format!("/rcaster/{command_cluster}/online/users/owner/sessions/command-session"),
        serde_json::to_vec(&session).expect("session JSON"),
        None,
    )
    .await
    .expect("session record");
    assert!(
        request_raw(
            address,
            &pki,
            "DELETE",
            "/admin/v1/sessions/command-session",
            "Bearer secret",
            "",
            "",
        )
        .await
        .starts_with("HTTP/1.1 202 Accepted")
    );
    let commands = etcd
        .get(
            format!("/rcaster/{command_cluster}/online/commands/caster-admin/"),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .expect("command list");
    assert_eq!(commands.kvs().len(), 1);
    assert!(
        String::from_utf8_lossy(commands.kvs()[0].value())
            .contains("\"session_id\":\"command-session\"")
    );
    assert!(
        request(
            address,
            &pki,
            "/admin/v1/users/owner/sessions",
            "Bearer secret",
        )
        .await
        .contains("\"session_id\":\"command-session\"")
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !runtime.is_healthy().await {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("runtime healthy");
    let source_permit = runtime
        .admit_source("owner", "C99", "source-command-connection", 2)
        .await
        .expect("source permit");
    let source_record = StrRecord::from_profile("C99", &DataProfile::default());
    runtime
        .activate_source(&source_permit, &source_record)
        .await
        .expect("activate source");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if runtime.source_route("C99").await.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("source route");
    assert!(
        request_raw(
            address,
            &pki,
            "DELETE",
            "/admin/v1/sources/C99",
            "Bearer secret",
            "",
            "",
        )
        .await
        .starts_with("HTTP/1.1 202 Accepted")
    );
    let commands = etcd
        .get(
            format!("/rcaster/{command_cluster}/runtime/source-commands/C99/"),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .expect("source command list");
    assert_eq!(commands.kvs().len(), 1);
    assert!(
        String::from_utf8_lossy(commands.kvs()[0].value())
            .contains("\"connection_id\":\"source-command-connection\"")
    );
    assert!(
        websocket_connect(
            address,
            &pki,
            "/admin/v1/ws?mountpoint=C99",
            "Bearer secret",
            Some("https://attacker.example"),
        )
        .await
        .is_err()
    );
    let mut websocket = websocket_connect(
        address,
        &pki,
        "/admin/v1/ws?mountpoint=C99",
        "Bearer secret",
        None,
    )
    .await
    .expect("websocket handshake");
    let ready = tokio::time::timeout(Duration::from_secs(2), websocket.next())
        .await
        .expect("monitor ready timeout")
        .expect("monitor ready message")
        .expect("monitor ready");
    assert!(
        ready
            .to_text()
            .expect("ready text")
            .contains("monitor_ready")
    );
    let snapshot = tokio::time::timeout(Duration::from_secs(2), websocket.next())
        .await
        .expect("monitor snapshot timeout")
        .expect("monitor snapshot message")
        .expect("monitor snapshot");
    assert!(
        snapshot
            .to_text()
            .expect("snapshot text")
            .contains("monitor_snapshot")
    );
    assert!(
        request_raw(
            address,
            &pki,
            "POST",
            "/admin/v1/streams/C99/monitor",
            "Bearer secret",
            "",
            "{}",
        )
        .await
        .starts_with("HTTP/1.1 201 Created")
    );
    assert!(
        request_raw(
            address,
            &pki,
            "DELETE",
            "/admin/v1/streams/C99/monitor",
            "Bearer secret",
            "",
            "",
        )
        .await
        .starts_with("HTTP/1.1 202 Accepted")
    );
    let closed = tokio::time::timeout(Duration::from_secs(2), websocket.next())
        .await
        .expect("monitor close timeout")
        .expect("monitor close message")
        .expect("monitor close");
    assert!(
        closed
            .to_text()
            .expect("close text")
            .contains("ADMIN_CANCELED")
    );
    drop(websocket);
    runtime
        .release_source(&source_permit)
        .await
        .expect("release source");
    runtime
        .record_source_cooldown("C63", "198.51.100.20", "TEST_REJECTION")
        .await
        .expect("record cooldown");
    let cooldowns = request(address, &pki, "/admin/v1/source-cooldowns", "Bearer secret").await;
    assert!(cooldowns.contains("\"reason\":\"TEST_REJECTION\""));
    let cooldown_fingerprint = response_body(&cooldowns)["cooldowns"][0]["fingerprint"]
        .as_str()
        .expect("cooldown fingerprint")
        .to_owned();
    assert!(
        request_raw(
            address,
            &pki,
            "DELETE",
            &format!("/admin/v1/source-cooldowns/{cooldown_fingerprint}"),
            "Bearer secret",
            "",
            "",
        )
        .await
        .starts_with("HTTP/1.1 204 No Content")
    );
    assert!(
        request_raw(
            address,
            &pki,
            "POST",
            "/admin/v1/auth/logout",
            "",
            &format!("Cookie: {cookie}\r\nX-CSRF-Token: {csrf}\r\n"),
            "",
        )
        .await
        .starts_with("HTTP/1.1 204 No Content")
    );
    assert!(
        request_raw(
            address,
            &pki,
            "GET",
            "/admin/v1/auth/session",
            "",
            &format!("Cookie: {cookie}\r\n"),
            "",
        )
        .await
        .starts_with("HTTP/1.1 401 Unauthorized")
    );

    runtime.shutdown().await.expect("runtime shutdown");
    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server task").expect("admin server");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
    let _ = fs::remove_file(audit_path);
    let _ = fs::remove_file(maintenance_path);
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
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    )
}

fn body_revision(response: &str) -> u64 {
    response
        .split("\r\n\r\n")
        .nth(1)
        .and_then(|body| serde_json::from_str::<serde_json::Value>(body).ok())
        .and_then(|body| body.get("revision").and_then(serde_json::Value::as_u64))
        .expect("response revision")
}

fn response_header<'a>(response: &'a str, name: &str) -> Option<&'a str> {
    response
        .split_once("\r\n\r\n")
        .map(|(head, _)| head)
        .unwrap_or(response)
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(header, _)| header.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.trim())
        })
}

fn response_body(response: &str) -> serde_json::Value {
    response
        .split_once("\r\n\r\n")
        .and_then(|(_, body)| serde_json::from_str(body).ok())
        .expect("JSON response body")
}

fn signed_upgrade_manifest(key: &SigningKey, version: &str) -> SignedUpgradeManifest {
    let artifact = b"upgrade";
    let manifest = UpgradeManifest {
        schema_version: 1,
        version: version.to_owned(),
        os: std::env::consts::OS.to_owned(),
        arch: std::env::consts::ARCH.to_owned(),
        config_schema: 1,
        etcd_schema: 1,
        api_schema: 1,
        artifact_url: "https://updates.example.test/rcaster".to_owned(),
        artifact_size: artifact.len() as u64,
        artifact_sha256: format!("{:x}", Sha256::digest(artifact)),
    };
    let signature = key.sign(&serde_json::to_vec(&manifest).expect("manifest JSON"));
    SignedUpgradeManifest {
        manifest,
        signature: signature
            .to_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
    }
}

async fn websocket_connect(
    address: std::net::SocketAddr,
    pki: &TestPki,
    path: &str,
    authorization: &str,
    origin: Option<&str>,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>,
    tokio_tungstenite::tungstenite::Error,
> {
    let tcp = TcpStream::connect(address)
        .await
        .expect("websocket connect");
    let mut roots = RootCertStore::empty();
    roots.add(pki.ca_certificate.der().clone()).expect("CA");
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(
            vec![pki.client_certificate.der().clone()],
            PrivatePkcs8KeyDer::from(pki.client_key.serialize_der()).into(),
        )
        .expect("client TLS");
    let stream = TlsConnector::from(Arc::new(config))
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .expect("websocket TLS");
    let mut request = format!("ws://localhost{path}")
        .into_client_request()
        .expect("websocket request");
    request.headers_mut().insert(
        "Authorization",
        authorization.parse().expect("authorization header"),
    );
    if let Some(origin) = origin {
        request
            .headers_mut()
            .insert("Origin", origin.parse().expect("origin header"));
    }
    tokio_tungstenite::client_async(request, stream)
        .await
        .map(|(websocket, _)| websocket)
}

struct TestPki {
    ca_certificate: rcgen::Certificate,
    server_certificate: rcgen::Certificate,
    server_key: KeyPair,
    client_certificate: rcgen::Certificate,
    client_key: KeyPair,
}

impl TestPki {
    fn generate() -> Self {
        let mut ca_params = CertificateParams::new(Vec::new()).expect("CA params");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "admin-test-ca");
        ca_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let ca_key = KeyPair::generate().expect("CA key");
        let ca_certificate = ca_params.self_signed(&ca_key).expect("CA certificate");
        let server_key = KeyPair::generate().expect("server key");
        let mut server_params =
            CertificateParams::new(vec!["localhost".to_owned()]).expect("server params");
        server_params
            .distinguished_name
            .push(DnType::CommonName, "localhost");
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_certificate = server_params
            .signed_by(&server_key, &ca_certificate, &ca_key)
            .expect("server certificate");
        let client_key = KeyPair::generate().expect("client key");
        let mut client_params =
            CertificateParams::new(vec!["admin-client".to_owned()]).expect("client params");
        client_params
            .distinguished_name
            .push(DnType::CommonName, "admin-client");
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_certificate = client_params
            .signed_by(&client_key, &ca_certificate, &ca_key)
            .expect("client certificate");
        Self {
            ca_certificate,
            server_certificate,
            server_key,
            client_certificate,
            client_key,
        }
    }

    fn server_tls(&self) -> GrpcServerTls {
        GrpcServerTls {
            certificate_pem: self.server_certificate.pem(),
            private_key_pem: self.server_key.serialize_pem(),
            client_ca_pem: self.ca_certificate.pem(),
            client_crl_pem: None,
            server_ocsp_der: None,
        }
    }
}
