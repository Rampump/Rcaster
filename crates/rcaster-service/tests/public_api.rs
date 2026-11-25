use std::{io::Read, sync::Arc, time::Duration};

use brotli::Decompressor;
use etcd_client::{Client, ConnectOptions};
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use rcaster_core::{
    account::{AccountSnapshot, Mountpoint, User, UserRole},
    protocol::{DataFormat, DataProfile, GeoPosition},
    str_record::StrRecord,
};
use rcaster_service::{
    account_auth::hash_password,
    account_directory::{AccountDirectory, AccountWatcher},
    audit::AuditLog,
    cluster_runtime::{ClusterRuntimeOptions, SharedClusterRuntime},
    config::{CasterConfig, publish_config},
    connections::ConnectionController,
    coordinator_admission::{AdmissionCoordinator, CoordinatorOptions},
    hub::MountpointHub,
    public::{PublicMapCoordinateSystem, PublicMapTiles, PublicState, serve_public},
    public_auth,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

#[tokio::test]
async fn standalone_public_sessions_and_tickets_use_local_state() {
    let now = "2026-11-25T00:00:00Z";
    let user = User {
        schema_version: 1,
        mod_revision: 1,
        user_id: "owner-id".to_owned(),
        username: "owner".to_owned(),
        password_hash: hash_password("secret").expect("password hash"),
        auth_version: 1,
        enabled: true,
        ntrip_enabled: true,
        web_enabled: true,
        role: UserRole::User,
        created_at: now.to_owned(),
        updated_at: now.to_owned(),
    };
    let mountpoint = Mountpoint {
        schema_version: 1,
        mod_revision: 1,
        mountpoint: "C63".to_owned(),
        owner_user_id: "owner-id".to_owned(),
        enabled: true,
        ntrip1_upload_password_hash: None,
        advertise: true,
        region: None,
        latitude: None,
        longitude: None,
        coverage_radius_m: None,
        str_fields: None,
        source_expectation: None,
        created_at: now.to_owned(),
        updated_at: now.to_owned(),
    };
    let directory = AccountDirectory::static_snapshot(
        AccountSnapshot::new([user], [mountpoint]).expect("account snapshot"),
    );
    let issued = public_auth::login(&directory, &Default::default(), "owner", "secret")
        .await
        .expect("standalone public login");
    let identity = public_auth::authenticate(&directory, &issued.cookie_value)
        .await
        .expect("standalone public session");
    assert_eq!(identity.username, "owner");

    let ticket = public_auth::issue_ticket(&directory, &identity, "C63", "127.0.0.1", Some(1))
        .await
        .expect("standalone monitor ticket");
    let consumed = public_auth::consume_ticket(&directory, &ticket, "C63", "127.0.0.1")
        .await
        .expect("consume standalone ticket");
    assert_eq!(consumed.0.user_id, "owner-id");
    assert_eq!(consumed.1, Some(1));
    assert!(
        public_auth::consume_ticket(&directory, &ticket, "C63", "127.0.0.1")
            .await
            .is_err(),
        "monitor ticket must be single-use"
    );
}

#[tokio::test]
async fn public_directory_exposes_only_advertised_anonymous_fields() {
    let hub = Arc::new(MountpointHub::new(16).expect("hub"));
    let _public_source = activate(&hub, "C63");
    let _private_source = activate(&hub, "PRIVATE");
    let _disabled_source = activate(&hub, "DISABLED");
    let _unknown_source = activate(&hub, "UNKNOWN");
    let _fallback_source = activate_without_position(&hub, "FALLBACK");
    let mut disabled = mountpoint("DISABLED", "east", true);
    disabled.enabled = false;
    let mut fallback = mountpoint("FALLBACK", "east", true);
    fallback.latitude = Some(30.0);
    fallback.longitude = Some(120.0);
    fallback.coverage_radius_m = Some(25_000.0);
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
                web_enabled: true,
                role: UserRole::User,
                created_at: "2026-11-22T00:00:00Z".to_owned(),
                updated_at: "2026-11-22T00:00:00Z".to_owned(),
            }],
            [
                mountpoint("C63", "east", true),
                fallback,
                mountpoint("PRIVATE", "east", false),
                disabled,
            ],
        )
        .expect("accounts"),
    );
    hub.record_health_snapshots();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_public(
        listener,
        PublicState {
            hub,
            runtime: None,
            accounts,
            relays: None,
            audit: Default::default(),
            websockets: Default::default(),
            login_limiter: Default::default(),
            monitor_starts: Default::default(),
            map_tiles: Some(PublicMapTiles {
                url_template: "https://tiles.example.test/{z}/{x}/{y}.png".to_owned(),
                attribution: "Test tiles".to_owned(),
                min_zoom: 1,
                max_zoom: 18,
                coordinate_system: PublicMapCoordinateSystem::Gcj02,
            }),
            proxy_protocol: None,
            connections: Arc::new(ConnectionController::default()),
            auth_limiter: Default::default(),
        },
        async {
            let _ = shutdown_rx.await;
        },
    ));

    let page = request(address, "/").await;
    assert!(page.contains("RCaster public stations"));
    assert!(page.contains("/api/v1/public/str"));
    assert!(page.contains("Content-Security-Policy"));
    assert!(page.contains("<svg id=\"map\""));
    assert!(page.contains("function renderMap"));
    assert!(page.contains("function drawChart"));
    assert!(page.contains("function renderSatellites"));
    assert!(page.contains("satellite_ids"));
    assert!(page.contains("<svg id=\"skyplot\""));
    assert!(page.contains("elevation_deg"));
    assert!(page.contains("satellite_cn0_db_hz"));
    assert!(page.contains("initializeMapControls"));
    assert!(page.contains("function drawMap"));
    assert!(page.contains("function mapBbox"));
    assert!(page.contains("function wgs84ToGcj02"));
    assert!(page.contains("coordinate_system"));
    assert!(page.contains("dataset.mountpoint"));
    assert!(page.contains("dataset.displayLongitude"));
    assert!(page.contains("async function refreshMap"));
    assert!(page.contains("/api/v1/public/map/stations?bbox="));
    assert!(page.contains("/api/v1/mountpoints?"));
    assert!(page.contains("next_cursor"));
    assert!(page.contains("function drawMapClusters"));
    assert!(page.contains("tile-layer"));
    assert!(page.contains("https://tiles.example.test"));
    assert!(page.contains("/api/v1/public/map/stations"));
    assert!(page.contains("data-freshness"));
    assert!(page.contains("Data stale."));
    assert!(page.contains("refreshPublicSession"));
    assert!(page.contains("if(socket!==next)return"));
    assert!(page.contains("status.textContent='Live: '+mountpoint"));

    let str = request(address, "/api/v1/public/str").await;
    assert!(str.contains("\"mountpoint\":\"C63\""));
    assert!(!str.contains("PRIVATE"));
    assert!(!str.contains("DISABLED"));
    assert!(!str.contains("UNKNOWN"));
    assert!(!str.contains("owner-id"));
    assert!(!str.contains("password"));
    assert!(str.contains("Cache-Control: public, max-age=5"));
    let etag = header_value(&str, "etag").expect("ETag").to_owned();
    let not_modified = request_raw(
        address,
        "GET",
        "/api/v1/public/str",
        &format!("If-None-Match: {etag}\r\n"),
        "",
    )
    .await;
    assert!(not_modified.starts_with("HTTP/1.1 304 Not Modified"));
    assert!(not_modified.contains("Content-Length: 0"));
    assert!(not_modified.ends_with("\r\n\r\n"));
    let compressed = request_raw_bytes(
        address,
        "GET",
        "/api/v1/public/str",
        "Accept-Encoding: gzip\r\n",
        "",
    )
    .await;
    let body_start = compressed
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("gzip response head")
        + 4;
    let compressed_head = String::from_utf8_lossy(&compressed[..body_start]);
    assert!(compressed_head.contains("Content-Encoding: gzip"));
    assert!(compressed_head.contains("Vary: Accept-Encoding"));
    let gzip_etag = header_value(&compressed_head, "etag")
        .expect("gzip ETag")
        .to_owned();
    assert_ne!(gzip_etag, etag);
    let mut decoded = String::new();
    GzDecoder::new(&compressed[body_start..])
        .read_to_string(&mut decoded)
        .expect("gzip body");
    assert!(decoded.contains("\"mountpoint\":\"C63\""));
    let gzip_not_modified = request_raw_bytes(
        address,
        "GET",
        "/api/v1/public/str",
        &format!("Accept-Encoding: gzip\r\nIf-None-Match: {gzip_etag}\r\n"),
        "",
    )
    .await;
    assert!(String::from_utf8_lossy(&gzip_not_modified).starts_with("HTTP/1.1 304 Not Modified"));
    let brotli = request_raw_bytes(
        address,
        "GET",
        "/api/v1/public/str",
        "Accept-Encoding: br\r\n",
        "",
    )
    .await;
    let body_start = brotli
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("brotli response head")
        + 4;
    let brotli_head = String::from_utf8_lossy(&brotli[..body_start]);
    assert!(brotli_head.contains("Content-Encoding: br"));
    let brotli_etag = header_value(&brotli_head, "etag")
        .expect("brotli ETag")
        .to_owned();
    assert_ne!(brotli_etag, etag);
    assert_ne!(brotli_etag, gzip_etag);
    let mut decoded = String::new();
    Decompressor::new(&brotli[body_start..], 4096)
        .read_to_string(&mut decoded)
        .expect("brotli body");
    assert!(decoded.contains("\"mountpoint\":\"C63\""));
    let brotli_not_modified = request_raw_bytes(
        address,
        "GET",
        "/api/v1/public/str",
        &format!("Accept-Encoding: br\r\nIf-None-Match: {brotli_etag}\r\n"),
        "",
    )
    .await;
    assert!(String::from_utf8_lossy(&brotli_not_modified).starts_with("HTTP/1.1 304 Not Modified"));
    assert!(
        request_fragmented(address, "/api/v1/public/str")
            .await
            .contains("\"mountpoint\":\"C63\"")
    );

    let mountpoints = request(address, "/api/v1/mountpoints?region=east&limit=10").await;
    assert!(mountpoints.contains("\"region\":\"east\""));
    assert!(!mountpoints.contains("PRIVATE"));

    let map = request(address, "/api/v1/public/map/stations").await;
    assert!(map.contains("\"type\":\"FeatureCollection\""));
    assert!(map.contains("[121.4,31.2]"));
    assert!(map.contains("[120.0,30.0]"));
    assert!(map.contains("\"coverage_m\":25000.0"));
    assert!(!map.contains("PRIVATE"));
    let viewport = request(
        address,
        "/api/v1/public/map/stations?bbox=121.3,31.1,121.5,31.3",
    )
    .await;
    assert!(viewport.contains("\"mountpoint\":\"C63\""));
    let encoded_viewport = request(
        address,
        "/api/v1/public/map/stations?bbox=121.3%2C31.1%2C121.5%2C31.3",
    )
    .await;
    assert!(encoded_viewport.contains("\"mountpoint\":\"C63\""));
    let outside = request(address, "/api/v1/public/map/stations?bbox=0,0,1,1").await;
    assert!(outside.contains("\"features\":[]"));
    assert!(
        request(address, "/api/v1/public/map/stations?bbox=broken")
            .await
            .starts_with("HTTP/1.1 400 Bad Request")
    );
    let map_config = request(address, "/api/v1/public/map/config").await;
    assert!(map_config.contains("\"attribution\":\"Test tiles\""));
    assert!(map_config.contains("\"max_zoom\":18"));
    assert!(map_config.contains("\"coordinate_system\":\"gcj02\""));
    let history = request(address, "/api/v1/public/streams/C63/history?limit=1").await;
    assert!(history.contains("\"mountpoint\":\"C63\""));
    assert!(history.contains("\"packets\":0"));

    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server").expect("public server");
}

#[tokio::test]
async fn public_login_ticket_and_monitor_websocket_are_single_use() {
    let cluster = format!(
        "public-monitor-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    );
    let watcher = AccountWatcher::start(
        connect().await,
        &cluster,
        AccountSnapshot::new(
            [User {
                password_hash: hash_password("secret").expect("hash"),
                username: "viewer".to_owned(),
                ..user()
            }],
            [mountpoint("C63", "east", true)],
        )
        .expect("snapshot"),
    )
    .await
    .expect("watcher");
    let accounts = watcher.directory();
    let hub = Arc::new(MountpointHub::new(16).expect("hub"));
    let _source = activate(&hub, "C63");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let connections = Arc::new(ConnectionController::default());
    let audit_path = std::env::temp_dir().join(format!(
        "rcaster-public-audit-{}-{}.jsonl",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let audit = AuditLog::open(&audit_path).expect("audit log");
    let server = tokio::spawn(serve_public(
        listener,
        PublicState {
            hub,
            runtime: None,
            accounts,
            relays: None,
            audit: audit.clone(),
            websockets: Default::default(),
            login_limiter: Default::default(),
            monitor_starts: Default::default(),
            map_tiles: None,
            proxy_protocol: None,
            connections: Arc::clone(&connections),
            auth_limiter: Default::default(),
        },
        async {
            let _ = shutdown_rx.await;
        },
    ));
    let failed_login = request_raw(
        address,
        "POST",
        "/api/v1/auth/login",
        "",
        r#"{"username":"viewer","password":"wrong"}"#,
    )
    .await;
    assert!(failed_login.starts_with("HTTP/1.1 401 Unauthorized"));
    let login = request_raw(
        address,
        "POST",
        "/api/v1/auth/login",
        "",
        r#"{"username":"viewer","password":"secret"}"#,
    )
    .await;
    assert!(login.starts_with("HTTP/1.1 200 OK"));
    assert!(login.contains("Path=/api/v1"));
    let cookie = header_value(&login, "set-cookie")
        .and_then(|value| value.split(';').next())
        .expect("cookie")
        .to_owned();
    let csrf = response_json(&login)["csrf_token"]
        .as_str()
        .expect("csrf")
        .to_owned();
    let ticket_response = request_raw(
        address,
        "POST",
        "/api/v1/monitors/C63/ticket",
        &format!("Cookie: {cookie}\r\nX-CSRF-Token: {csrf}\r\n"),
        "",
    )
    .await;
    assert!(ticket_response.starts_with("HTTP/1.1 201 Created"));
    assert!(response_json(&ticket_response).get("ticket").is_none());
    let ticket_cookie = header_value(&ticket_response, "set-cookie")
        .and_then(|value| value.split(';').next())
        .expect("ticket cookie")
        .to_owned();
    let path = "/api/v1/monitors/C63/ws";
    let query_ticket = ticket_cookie
        .split_once('=')
        .map(|(_, value)| value)
        .expect("ticket value");
    assert!(
        websocket_connect(address, &format!("{path}?ticket={query_ticket}"), None)
            .await
            .is_err()
    );
    assert!(
        websocket_connect_with_origin(
            address,
            path,
            Some("https://attacker.example"),
            Some(&ticket_cookie),
        )
        .await
        .is_err()
    );
    let mut websocket = websocket_connect(address, path, Some(&ticket_cookie))
        .await
        .expect("websocket");
    let ready = tokio::time::timeout(Duration::from_secs(2), websocket.next())
        .await
        .expect("ready timeout")
        .expect("ready")
        .expect("ready frame");
    assert!(ready.to_text().expect("ready").contains("monitor_ready"));
    let snapshot = tokio::time::timeout(Duration::from_secs(2), websocket.next())
        .await
        .expect("snapshot timeout")
        .expect("snapshot")
        .expect("snapshot frame");
    assert!(
        snapshot
            .to_text()
            .expect("snapshot")
            .contains("monitor_snapshot")
    );
    connections.cancel_web_user("owner-id");
    let closed = tokio::time::timeout(Duration::from_secs(2), websocket.next())
        .await
        .expect("auth close timeout")
        .expect("auth close")
        .expect("auth close frame");
    assert!(
        closed
            .to_text()
            .expect("auth close text")
            .contains("AUTH_CHANGED")
    );
    drop(websocket);
    assert!(
        websocket_connect(address, path, Some(&ticket_cookie))
            .await
            .is_err()
    );
    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server").expect("public server");
    watcher.shutdown().await;
    let events = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let events = audit.recent(10).await.expect("read audit");
            if events.len() >= 2 {
                break events;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("audit timeout");
    assert!(
        events
            .iter()
            .all(|event| event.action == "auth.public_login")
    );
    assert!(events.iter().any(|event| event.result == "failure"));
    assert!(events.iter().any(|event| event.result == "success"));
    let _ = std::fs::remove_file(audit_path);
}

#[tokio::test]
async fn public_monitor_closes_when_source_generation_changes() {
    let cluster = format!(
        "public-generation-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    );
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
            caster_id: "caster-public".to_owned(),
            runtime_epoch: format!("epoch-{}", std::process::id()),
            relay_endpoint: "127.0.0.1:12102".to_owned(),
            lease_ttl_s: 30,
            keep_alive: true,
        },
    )
    .await
    .expect("runtime");
    let watcher = AccountWatcher::start(
        connect().await,
        &cluster,
        AccountSnapshot::new(
            [User {
                password_hash: hash_password("secret").expect("hash"),
                username: "viewer".to_owned(),
                ..user()
            }],
            [mountpoint("C63", "east", true)],
        )
        .expect("snapshot"),
    )
    .await
    .expect("account watcher");
    let accounts = watcher.directory();
    let hub = Arc::new(MountpointHub::new(16).expect("hub"));
    let source = activate(&hub, "C63");
    let str_record = hub
        .sourcetable_records()
        .into_iter()
        .next()
        .expect("source STR");
    let source_key = format!("/rcaster/{cluster}/runtime/sources/C63");
    let source_value = |generation: u64| {
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "state": "ACTIVE",
            "source_generation": generation,
            "connection_id": "public-source",
            "mountpoint": "C63",
            "caster_id": "caster-public",
            "runtime_epoch": "public-epoch",
            "relay_endpoint": "127.0.0.1:12102",
            "effective_user_id": "owner-id",
            "created_at": "0",
            "str": &str_record,
        }))
        .expect("source JSON")
    };
    etcd.put(source_key.clone(), source_value(1001), None)
        .await
        .expect("publish source generation");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if runtime
                .source_route("C63")
                .await
                .is_some_and(|route| route.source_generation == 1001)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("initial source generation");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_public(
        listener,
        PublicState {
            hub,
            runtime: Some(runtime.clone()),
            accounts,
            relays: None,
            audit: Default::default(),
            websockets: Default::default(),
            login_limiter: Default::default(),
            monitor_starts: Default::default(),
            map_tiles: None,
            proxy_protocol: None,
            connections: Arc::new(ConnectionController::default()),
            auth_limiter: Default::default(),
        },
        async {
            let _ = shutdown_rx.await;
        },
    ));

    let login = request_raw(
        address,
        "POST",
        "/api/v1/auth/login",
        "",
        r#"{"username":"viewer","password":"secret"}"#,
    )
    .await;
    assert!(login.starts_with("HTTP/1.1 200 OK"));
    let cookie = header_value(&login, "set-cookie")
        .and_then(|value| value.split(';').next())
        .expect("cookie")
        .to_owned();
    let csrf = response_json(&login)["csrf_token"]
        .as_str()
        .expect("csrf")
        .to_owned();
    let ticket_response = request_raw(
        address,
        "POST",
        "/api/v1/monitors/C63/ticket",
        &format!("Cookie: {cookie}\r\nX-CSRF-Token: {csrf}\r\n"),
        "",
    )
    .await;
    assert!(ticket_response.starts_with("HTTP/1.1 201 Created"));
    let ticket_cookie = header_value(&ticket_response, "set-cookie")
        .and_then(|value| value.split(';').next())
        .expect("ticket cookie")
        .to_owned();
    let path = "/api/v1/monitors/C63/ws";
    let mut websocket = websocket_connect(address, path, Some(&ticket_cookie))
        .await
        .expect("websocket");
    let ready = tokio::time::timeout(Duration::from_secs(2), websocket.next())
        .await
        .expect("ready timeout")
        .expect("ready")
        .expect("ready frame");
    assert!(ready.to_text().expect("ready").contains("monitor_ready"));
    let snapshot = tokio::time::timeout(Duration::from_secs(2), websocket.next())
        .await
        .expect("snapshot timeout")
        .expect("snapshot")
        .expect("snapshot frame");
    assert!(
        snapshot
            .to_text()
            .expect("snapshot")
            .contains("monitor_snapshot")
    );

    etcd.put(source_key, source_value(1002), None)
        .await
        .expect("replace source generation");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if runtime
                .source_route("C63")
                .await
                .is_some_and(|route| route.source_generation == 1002)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("replacement source generation");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = websocket
                .next()
                .await
                .expect("monitor close")
                .expect("monitor message");
            if message
                .to_text()
                .is_ok_and(|text| text.contains("SOURCE_CHANGED"))
            {
                break;
            }
        }
    })
    .await
    .expect("generation close timeout");
    drop(websocket);
    shutdown_tx.send(()).expect("shutdown");
    server.await.expect("server").expect("public server");
    watcher.shutdown().await;
    drop(source);
    runtime.shutdown().await.expect("runtime shutdown");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

fn activate(hub: &MountpointHub, name: &str) -> rcaster_service::hub::SourceLease {
    let source = hub.claim_source(name).expect("source");
    let profile = DataProfile {
        primary_format: Some(DataFormat::Rtcm3),
        has_correction_data: true,
        position: Some(GeoPosition {
            latitude_e7: 312_000_000,
            longitude_e7: 1_214_000_000,
            height_cm: 0,
        }),
        ..DataProfile::default()
    };
    source
        .set_str_record(StrRecord::from_profile(name, &profile))
        .expect("str");
    source.set_profile(profile).expect("profile");
    source
}

fn activate_without_position(hub: &MountpointHub, name: &str) -> rcaster_service::hub::SourceLease {
    let source = hub.claim_source(name).expect("source");
    let profile = DataProfile {
        primary_format: Some(DataFormat::Rtcm3),
        has_correction_data: true,
        ..DataProfile::default()
    };
    source
        .set_str_record(StrRecord::from_profile(name, &profile))
        .expect("str");
    source.set_profile(profile).expect("profile");
    source
}

fn mountpoint(name: &str, region: &str, advertise: bool) -> Mountpoint {
    Mountpoint {
        schema_version: 1,
        mod_revision: 1,
        mountpoint: name.to_owned(),
        owner_user_id: "owner-id".to_owned(),
        enabled: true,
        ntrip1_upload_password_hash: None,
        advertise,
        region: Some(region.to_owned()),
        latitude: None,
        longitude: None,
        coverage_radius_m: None,
        str_fields: None,
        source_expectation: None,
        created_at: "2026-11-22T00:00:00Z".to_owned(),
        updated_at: "2026-11-22T00:00:00Z".to_owned(),
    }
}

async fn request(address: std::net::SocketAddr, path: &str) -> String {
    request_raw(address, "GET", path, "", "").await
}

async fn request_fragmented(address: std::net::SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(address).await.expect("connect");
    for fragment in [
        format!("GET {path} HTTP/1.1\r\n"),
        "Host: localhost\r\n".to_owned(),
        "Content-Length: 0\r\n".to_owned(),
        "\r\n".to_owned(),
    ] {
        stream
            .write_all(fragment.as_bytes())
            .await
            .expect("fragmented request");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
        .await
        .expect("response timeout")
        .expect("response");
    String::from_utf8(response).expect("UTF-8 response")
}

async fn request_raw(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    extra_headers: &str,
    body: &str,
) -> String {
    let mut stream = TcpStream::connect(address).await.expect("connect");
    stream
        .write_all(
            format!(
                "{method} {path} HTTP/1.1\r\nHost: localhost\r\n{extra_headers}Content-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .expect("request");
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
        .await
        .expect("response timeout")
        .expect("response");
    String::from_utf8(response).expect("UTF-8 response")
}

async fn request_raw_bytes(
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    extra_headers: &str,
    body: &str,
) -> Vec<u8> {
    let mut stream = TcpStream::connect(address).await.expect("connect");
    stream
        .write_all(
            format!(
                "{method} {path} HTTP/1.1\r\nHost: localhost\r\n{extra_headers}Content-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .expect("request");
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
        .await
        .expect("response timeout")
        .expect("response");
    response
}

fn user() -> User {
    User {
        schema_version: 1,
        mod_revision: 1,
        user_id: "owner-id".to_owned(),
        username: "owner".to_owned(),
        password_hash: "hash".to_owned(),
        auth_version: 1,
        enabled: true,
        ntrip_enabled: true,
        web_enabled: true,
        role: UserRole::User,
        created_at: "2026-11-22T00:00:00Z".to_owned(),
        updated_at: "2026-11-22T00:00:00Z".to_owned(),
    }
}

fn header_value<'a>(response: &'a str, name: &str) -> Option<&'a str> {
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

fn response_json(response: &str) -> serde_json::Value {
    response
        .split_once("\r\n\r\n")
        .and_then(|(_, body)| serde_json::from_str(body).ok())
        .expect("JSON response")
}

async fn websocket_connect(
    address: std::net::SocketAddr,
    path: &str,
    cookie: Option<&str>,
) -> Result<tokio_tungstenite::WebSocketStream<TcpStream>, tokio_tungstenite::tungstenite::Error> {
    websocket_connect_with_origin(address, path, None, cookie).await
}

async fn websocket_connect_with_origin(
    address: std::net::SocketAddr,
    path: &str,
    origin: Option<&str>,
    cookie: Option<&str>,
) -> Result<tokio_tungstenite::WebSocketStream<TcpStream>, tokio_tungstenite::tungstenite::Error> {
    let mut request = format!("ws://{address}{path}")
        .into_client_request()
        .expect("websocket request");
    if let Some(origin) = origin {
        request
            .headers_mut()
            .insert("Origin", origin.parse().expect("origin header"));
    }
    if let Some(cookie) = cookie {
        request
            .headers_mut()
            .insert("Cookie", cookie.parse().expect("cookie header"));
    }
    tokio_tungstenite::client_async(
        request,
        TcpStream::connect(address)
            .await
            .expect("websocket connect"),
    )
    .await
    .map(|(websocket, _)| websocket)
}

async fn connect() -> Client {
    let endpoint = std::env::var("RCASTER_ETCD_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned());
    Client::connect([endpoint], Some(ConnectOptions::new()))
        .await
        .expect("etcd must be running")
}
