use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client as EtcdClient, ConnectOptions};
use rcaster_core::account::{AccountSnapshot, Mountpoint, User, UserRole};
use rcaster_service::{
    account_auth::hash_password,
    account_store::AccountStore,
    config::{CasterConfig, publish_config},
};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use serde_json::json;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpSocket, TcpStream},
    process::Command,
    time::{sleep, timeout},
};
use tokio_rustls::TlsConnector;

#[tokio::test]
async fn import_accounts_command_replaces_the_database() {
    let directory = TempDirectory::new();
    let database = directory.path().join("accounts.redb");
    let input = directory.path().join("accounts.json");
    let (user, mountpoint) = account_records();
    fs::write(
        &input,
        serde_json::to_vec(&json!({
            "users": [user],
            "mountpoints": [mountpoint]
        }))
        .expect("encode accounts"),
    )
    .expect("write accounts");

    let output = Command::new(env!("CARGO_BIN_EXE_rcaster"))
        .arg("import-accounts")
        .arg("--database")
        .arg(&database)
        .arg("--input")
        .arg(&input)
        .output()
        .await
        .expect("run import");
    assert!(
        output.status.success(),
        "import failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let snapshot = AccountStore::open(&database)
        .expect("open imported store")
        .load()
        .expect("load imported accounts");
    assert!(snapshot.user_by_username("owner").is_some());
    assert!(snapshot.mountpoint("C63").is_some());
}

#[tokio::test]
async fn backup_standalone_exports_accounts_and_config_without_overwriting() {
    let directory = TempDirectory::new();
    let database = directory.path().join("accounts.redb");
    let config = directory.path().join("caster.json");
    let output = directory.path().join("backup");
    write_accounts(&database);
    let config_bytes = serde_json::to_vec_pretty(&json!({
        "bind": "127.0.0.1:0",
        "account_db": database,
        "client_auth_require_tls": false,
        "source_auth_require_tls": false
    }))
    .expect("encode config");
    fs::write(&config, &config_bytes).expect("write config");

    let first = Command::new(env!("CARGO_BIN_EXE_rcaster"))
        .arg("backup-standalone")
        .arg("--database")
        .arg(&database)
        .arg("--config")
        .arg(&config)
        .arg("--output-dir")
        .arg(&output)
        .output()
        .await
        .expect("run standalone backup");
    assert!(
        first.status.success(),
        "standalone backup failed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let accounts: serde_json::Value = serde_json::from_slice(
        &fs::read(output.join("accounts.json")).expect("read exported accounts"),
    )
    .expect("parse exported accounts");
    assert_eq!(accounts["users"].as_array().expect("users").len(), 1);
    assert_eq!(
        accounts["mountpoints"]
            .as_array()
            .expect("mountpoints")
            .len(),
        1
    );
    assert_eq!(
        fs::read(output.join("caster.json")).expect("read config backup"),
        config_bytes
    );
    assert!(
        fs::read_dir(directory.path())
            .expect("list backup parent")
            .flatten()
            .all(|entry| !entry.file_name().to_string_lossy().ends_with(".tmp")),
        "temporary backup directory was not removed"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for name in ["accounts.json", "caster.json"] {
            let mode = fs::metadata(output.join(name))
                .expect("backup metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o077, 0, "{name} is readable by group or others");
        }
    }

    let second = Command::new(env!("CARGO_BIN_EXE_rcaster"))
        .arg("backup-standalone")
        .arg("--database")
        .arg(&database)
        .arg("--config")
        .arg(&config)
        .arg("--output-dir")
        .arg(&output)
        .output()
        .await
        .expect("rerun standalone backup");
    assert!(!second.status.success());
    assert!(String::from_utf8_lossy(&second.stderr).contains("refusing to overwrite"));
}

#[tokio::test]
async fn standalone_backup_restores_a_corrupted_database_snapshot() {
    let directory = TempDirectory::new();
    let database = directory.path().join("accounts.redb");
    let config = directory.path().join("caster.json");
    let backup_dir = directory.path().join("backup");
    let corrupt_db = directory.path().join("corrupt.redb");
    let restored_db = directory.path().join("restored.redb");
    write_accounts(&database);
    let original = AccountStore::open_existing(&database)
        .expect("open original")
        .load()
        .expect("load original");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&json!({
            "bind": "127.0.0.1:0",
            "account_db": database,
            "client_auth_require_tls": false,
            "source_auth_require_tls": false
        }))
        .expect("encode config"),
    )
    .expect("write config");
    let backup = Command::new(env!("CARGO_BIN_EXE_rcaster"))
        .arg("backup-standalone")
        .arg("--database")
        .arg(&database)
        .arg("--config")
        .arg(&config)
        .arg("--output-dir")
        .arg(&backup_dir)
        .output()
        .await
        .expect("run backup");
    assert!(
        backup.status.success(),
        "backup failed: {}",
        String::from_utf8_lossy(&backup.stderr)
    );

    fs::rename(&database, &corrupt_db).expect("rename original");
    fs::write(&corrupt_db, b"not a redb database").expect("corrupt database");
    assert!(AccountStore::open_existing(&corrupt_db).is_err());

    let restore = Command::new(env!("CARGO_BIN_EXE_rcaster"))
        .arg("import-accounts")
        .arg("--database")
        .arg(&restored_db)
        .arg("--input")
        .arg(backup_dir.join("accounts.json"))
        .output()
        .await
        .expect("restore backup");
    assert!(
        restore.status.success(),
        "restore failed: {}",
        String::from_utf8_lossy(&restore.stderr)
    );
    let restored = AccountStore::open_existing(&restored_db)
        .expect("open restored")
        .load()
        .expect("load restored");
    let original_user = original.user_by_username("owner").expect("original user");
    let restored_user = restored.user_by_username("owner").expect("restored user");
    assert_eq!(restored_user.user_id, original_user.user_id);
    assert_eq!(restored_user.password_hash, original_user.password_hash);
    let restored_mount = restored.mountpoint("C63").expect("restored mountpoint");
    assert_eq!(restored_mount.owner_user_id, original_user.user_id);
    assert_eq!(
        restored_mount.ntrip1_upload_password_hash,
        original
            .mountpoint("C63")
            .expect("original mountpoint")
            .ntrip1_upload_password_hash
    );
}

#[tokio::test]
async fn running_standalone_caster_creates_periodic_backups() {
    let directory = TempDirectory::new();
    let database = directory.path().join("accounts.redb");
    let config = directory.path().join("caster.json");
    let backup_dir = directory.path().join("backups");
    write_accounts(&database);
    fs::write(
        &config,
        serde_json::to_vec_pretty(&json!({
            "bind": "127.0.0.1:0",
            "account_db": database,
            "client_auth_require_tls": false,
            "source_auth_require_tls": false,
            "standalone_backup_dir": backup_dir,
            "standalone_backup_interval_s": 1
        }))
        .expect("encode config"),
    )
    .expect("write config");

    let mut command = Command::new(env!("CARGO_BIN_EXE_rcaster"));
    command
        .arg("serve")
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().expect("start standalone caster");
    let stdout = child.stdout.take().expect("child stdout");
    let mut lines = BufReader::new(stdout).lines();
    timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("startup timeout")
        .expect("read stdout")
        .expect("listener line");

    let backup = timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(entries) = fs::read_dir(&backup_dir) {
                for entry in entries.flatten() {
                    if !entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with("standalone-")
                    {
                        continue;
                    }
                    let path = entry.path();
                    if path.join("accounts.json").is_file() && path.join("caster.json").is_file() {
                        return path;
                    }
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("periodic backup timeout");
    let accounts: serde_json::Value =
        serde_json::from_slice(&fs::read(backup.join("accounts.json")).expect("read backup"))
            .expect("parse backup");
    assert_eq!(accounts["users"].as_array().expect("users").len(), 1);

    child.kill().await.expect("stop standalone caster");
    child.wait().await.expect("wait standalone caster");
}

#[tokio::test]
#[ignore = "requires HAPROXY_BIN"]
async fn haproxy_proxy_protocol_v2_preserves_the_client_ip() {
    let haproxy = std::env::var("HAPROXY_BIN").expect("HAPROXY_BIN");
    let directory = TempDirectory::new();
    let database = directory.path().join("accounts.redb");
    let config = directory.path().join("caster.json");
    let haproxy_config = directory.path().join("haproxy.cfg");
    let backend_port = available_port();
    let frontend_port = available_port();
    write_accounts(&database);
    fs::write(
        &config,
        serde_json::to_vec_pretty(&json!({
            "bind": format!("127.0.0.1:{backend_port}"),
            "account_db": database,
            "client_auth_require_tls": false,
            "source_auth_require_tls": false,
            "proxy_protocol": {
                "trusted_l4_cidrs": ["127.0.0.1/32"]
            },
            "session_limits": {
                "per_ip": {
                    "127.0.0.2/32": 0
                }
            }
        }))
        .expect("encode config"),
    )
    .expect("write config");
    fs::write(
        &haproxy_config,
        format!(
            "global\n\
             \x20 maxconn 64\n\
             defaults\n\
             \x20 mode tcp\n\
             \x20 timeout connect 2s\n\
             \x20 timeout client 10s\n\
             \x20 timeout server 10s\n\
             frontend rcaster\n\
             \x20 bind 127.0.0.1:{frontend_port}\n\
             \x20 default_backend rcaster\n\
             backend rcaster\n\
             \x20 server caster 127.0.0.1:{backend_port} send-proxy-v2\n"
        ),
    )
    .expect("write HAProxy config");

    let mut caster = Command::new(env!("CARGO_BIN_EXE_rcaster"))
        .arg("serve")
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start caster");
    let stdout = caster.stdout.take().expect("caster stdout");
    let mut lines = BufReader::new(stdout).lines();
    let listener = timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("caster startup timeout")
        .expect("read caster stdout")
        .expect("caster listener");
    assert_eq!(listener, format!("listening on 127.0.0.1:{backend_port}"));

    let mut proxy = Command::new(haproxy)
        .arg("-db")
        .arg("-q")
        .arg("-f")
        .arg(&haproxy_config)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start HAProxy");
    let frontend = format!("127.0.0.1:{frontend_port}");
    let mut connected = false;
    for _ in 0..50 {
        if let Some(status) = proxy.try_wait().expect("HAProxy status") {
            let stderr = proxy.stderr.take().expect("HAProxy stderr");
            let mut error = String::new();
            BufReader::new(stderr)
                .read_line(&mut error)
                .await
                .expect("read HAProxy error");
            panic!("HAProxy exited with {status}: {error}");
        }
        if TcpStream::connect(&frontend).await.is_ok() {
            connected = true;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(connected, "HAProxy frontend did not become ready");

    let socket = TcpSocket::new_v4().expect("client socket");
    socket
        .bind("127.0.0.2:0".parse().expect("client bind address"))
        .expect("bind client source address");
    let mut client = socket
        .connect(frontend.parse().expect("frontend address"))
        .await
        .expect("connect through HAProxy");
    client
        .write_all(b"GET /C63 HTTP/1.1\r\nNtrip-Version: Ntrip/2.0\r\n\r\n")
        .await
        .expect("proxied request");
    let response = read_socket_head(&mut client).await;
    assert!(
        response.starts_with(b"HTTP/1.1 403"),
        "PROXY source IP was not applied: {}",
        String::from_utf8_lossy(&response)
    );

    proxy.kill().await.expect("stop HAProxy");
    proxy.wait().await.expect("wait HAProxy");
    caster.kill().await.expect("stop caster");
    caster.wait().await.expect("wait caster");
}

#[tokio::test]
async fn hash_password_command_reads_stdin_and_outputs_argon2id() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rcaster"))
        .arg("hash-password")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start hash-password");
    let mut stdin = child.stdin.take().expect("child stdin");
    stdin.write_all(b"secret\n").await.expect("write password");
    drop(stdin);

    let output = child.wait_with_output().await.expect("hash process");
    assert!(
        output.status.success(),
        "hash failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).starts_with("$argon2id$"),
        "unexpected hash output"
    );
}

#[tokio::test]
async fn serve_loads_accounts_and_accepts_ntrip1_source() {
    let directory = TempDirectory::new();
    let database = directory.path().join("accounts.redb");
    write_accounts(&database);
    let config = directory.path().join("caster.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&json!({
            "bind": "127.0.0.1:0",
            "account_db": database,
            "client_auth_require_tls": false,
            "source_auth_require_tls": false,
            "head_timeout_ms": 1000
        }))
        .expect("encode config"),
    )
    .expect("write config");

    let mut command = Command::new(env!("CARGO_BIN_EXE_rcaster"));
    command
        .arg("serve")
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().expect("start rcaster");
    let stdout = child.stdout.take().expect("child stdout");
    let mut lines = BufReader::new(stdout).lines();
    let startup = timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("startup timeout")
        .expect("read stdout");
    let Some(line) = startup else {
        let mut stderr = child.stderr.take().expect("child stderr");
        let mut message = String::new();
        stderr
            .read_to_string(&mut message)
            .await
            .expect("read stderr");
        panic!("rcaster exited before listening: {message}");
    };
    let address = line
        .strip_prefix("listening on ")
        .expect("listener announcement");

    let mut source = TcpStream::connect(address).await.expect("connect source");
    source
        .write_all(b"SOURCE upload /C63\r\n\r\n")
        .await
        .expect("source request");
    let mut response = vec![0_u8; b"ICY 200 OK\r\n\r\n".len()];
    source
        .read_exact(&mut response)
        .await
        .expect("source success");
    assert_eq!(response, b"ICY 200 OK\r\n\r\n");

    child.kill().await.expect("stop rcaster");
    child.wait().await.expect("wait rcaster");
}

#[tokio::test]
async fn serve_loads_certificates_and_accepts_tls_source() {
    let directory = TempDirectory::new();
    let database = directory.path().join("accounts.redb");
    write_accounts(&database);
    let pki = TestPki::generate(directory.path());
    let config = directory.path().join("caster.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&json!({
            "bind": "127.0.0.1:0",
            "account_db": database,
            "client_auth_required": true,
            "tls_bind": "127.0.0.1:0",
            "tls_certificate": "server.crt",
            "tls_private_key": "server.key",
            "head_timeout_ms": 1000
        }))
        .expect("encode config"),
    )
    .expect("write config");

    let mut command = Command::new(env!("CARGO_BIN_EXE_rcaster"));
    command
        .arg("serve")
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().expect("start rcaster");
    let stdout = child.stdout.take().expect("child stdout");
    let mut lines = BufReader::new(stdout).lines();
    let clear_line = timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("startup timeout")
        .expect("read stdout")
        .expect("cleartext listener line");
    let clear_address = clear_line
        .strip_prefix("listening on ")
        .expect("cleartext listener announcement");
    let tls_line = timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("TLS startup timeout")
        .expect("read stdout")
        .expect("TLS listener line");
    let address = tls_line
        .strip_prefix("listening on TLS ")
        .expect("TLS listener announcement");

    let mut roots = RootCertStore::empty();
    let (trusted, ignored) = roots.add_parsable_certificates(pki.ca_der);
    assert_eq!((trusted, ignored), (1, 0));
    let tls_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(tls_config));
    let tcp = TcpStream::connect(address)
        .await
        .expect("connect TLS listener");
    let mut stream = connector
        .connect(ServerName::try_from("localhost").expect("server name"), tcp)
        .await
        .expect("TLS handshake");
    stream
        .write_all(b"SOURCE upload /C63\r\n\r\n")
        .await
        .expect("source request");
    let mut response = vec![0_u8; b"ICY 200 OK\r\n\r\n".len()];
    stream
        .read_exact(&mut response)
        .await
        .expect("source success");
    assert_eq!(response, b"ICY 200 OK\r\n\r\n");

    let mut clear = TcpStream::connect(clear_address)
        .await
        .expect("connect cleartext listener");
    clear
        .write_all(b"SOURCE upload /C63\r\n\r\n")
        .await
        .expect("cleartext source request");
    let mut clear_reader = BufReader::new(clear);
    let mut clear_response = String::new();
    clear_reader
        .read_line(&mut clear_response)
        .await
        .expect("cleartext source response");
    assert_eq!(clear_response, "ERROR - Bad Password\r\n");

    let mut clear_client = TcpStream::connect(clear_address)
        .await
        .expect("connect cleartext client");
    clear_client
        .write_all(
            b"GET /C63 HTTP/1.1\r\nNtrip-Version: Ntrip/2.0\r\nAuthorization: Basic b3duZXI6c2VjcmV0\r\n\r\n",
        )
        .await
        .expect("cleartext client request");
    let mut clear_client_reader = BufReader::new(clear_client);
    let mut clear_client_response = String::new();
    clear_client_reader
        .read_line(&mut clear_client_response)
        .await
        .expect("cleartext client response");
    assert!(clear_client_response.starts_with("HTTP/1.1 401"));

    let tls_client = TcpStream::connect(address)
        .await
        .expect("connect TLS client");
    let mut tls_client = connector
        .connect(
            ServerName::try_from("localhost").expect("server name"),
            tls_client,
        )
        .await
        .expect("TLS client handshake");
    tls_client
        .write_all(
            b"GET /C63 HTTP/1.1\r\nNtrip-Version: Ntrip/2.0\r\nAuthorization: Basic b3duZXI6c2VjcmV0\r\n\r\n",
        )
        .await
        .expect("TLS client request");
    let mut tls_client_reader = BufReader::new(tls_client);
    let mut tls_client_response = String::new();
    tls_client_reader
        .read_line(&mut tls_client_response)
        .await
        .expect("TLS client response");
    assert!(tls_client_response.starts_with("HTTP/1.1 200"));

    child.kill().await.expect("stop rcaster");
    child.wait().await.expect("wait rcaster");
}

#[tokio::test]
async fn serve_starts_public_tls_without_ntrip_tls_listener() {
    let directory = TempDirectory::new();
    let database = directory.path().join("accounts.redb");
    write_accounts(&database);
    let pki = TestPki::generate(directory.path());
    let clear_port = available_port();
    let public_port = available_port();
    let config = directory.path().join("caster.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&json!({
            "bind": format!("127.0.0.1:{clear_port}"),
            "public_bind": format!("127.0.0.1:{public_port}"),
            "account_db": database,
            "tls_certificate": "server.crt",
            "tls_private_key": "server.key",
            "client_auth_require_tls": false,
            "source_auth_require_tls": false,
            "head_timeout_ms": 1000
        }))
        .expect("encode config"),
    )
    .expect("write config");

    let mut command = Command::new(env!("CARGO_BIN_EXE_rcaster"));
    command
        .arg("serve")
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().expect("start rcaster");
    let stdout = child.stdout.take().expect("child stdout");
    let mut lines = BufReader::new(stdout).lines();
    let public_line = timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("public listener startup timeout")
        .expect("read stdout")
        .expect("public listener line");
    let public_address = public_line
        .strip_prefix("public API listening on ")
        .expect("public listener announcement");
    let clear_line = timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("clear listener startup timeout")
        .expect("read stdout")
        .expect("clear listener line");
    assert_eq!(clear_line, format!("listening on 127.0.0.1:{clear_port}"));

    let mut roots = RootCertStore::empty();
    let (trusted, ignored) = roots.add_parsable_certificates(pki.ca_der);
    assert_eq!((trusted, ignored), (1, 0));
    let tls_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(tls_config));
    let tcp = TcpStream::connect(public_address)
        .await
        .expect("connect public listener");
    let mut stream = connector
        .connect(ServerName::try_from("localhost").expect("server name"), tcp)
        .await
        .expect("public TLS handshake");
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("public request");
    let response = read_tls_socket_head(&mut stream).await;
    assert!(
        response.starts_with(b"HTTP/1.1 200"),
        "public response was not successful: {}",
        String::from_utf8_lossy(&response)
    );

    child.kill().await.expect("stop rcaster");
    child.wait().await.expect("wait rcaster");
}

#[tokio::test]
async fn serve_starts_cluster_admission_from_config() {
    let directory = TempDirectory::new();
    let database = directory.path().join("accounts.redb");
    write_accounts(&database);
    let cluster_id = unique("app-cluster");
    let mut etcd = connect_etcd().await;
    publish_config(&mut etcd, &cluster_id, CasterConfig::new(1))
        .await
        .expect("publish config");
    let config = directory.path().join("caster.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&json!({
            "bind": "127.0.0.1:0",
            "account_db": database,
            "client_auth_require_tls": false,
            "source_auth_require_tls": false,
            "head_timeout_ms": 1000,
            "cluster": {
                "etcd_endpoints": [std::env::var("RCASTER_ETCD_ENDPOINT")
                    .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned())],
                "cluster_id": cluster_id,
                "caster_id": "caster-app",
                "runtime_epoch": unique("epoch"),
                "relay_endpoint": "10.0.0.30:12102"
            }
        }))
        .expect("encode cluster config"),
    )
    .expect("write cluster config");

    let mut command = Command::new(env!("CARGO_BIN_EXE_rcaster"));
    command
        .arg("serve")
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().expect("start cluster rcaster");
    let stdout = child.stdout.take().expect("child stdout");
    let mut lines = BufReader::new(stdout).lines();
    let line = timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("startup timeout")
        .expect("read stdout")
        .expect("listener line");
    let address = line
        .strip_prefix("listening on ")
        .expect("listener announcement");

    let mut source = TcpStream::connect(address).await.expect("connect source");
    source
        .write_all(b"SOURCE upload /C63\r\n\r\n")
        .await
        .expect("source request");
    let mut response = vec![0_u8; b"ICY 200 OK\r\n\r\n".len()];
    source
        .read_exact(&mut response)
        .await
        .expect("cluster source success");
    assert_eq!(response, b"ICY 200 OK\r\n\r\n");

    child.kill().await.expect("stop rcaster");
    child.wait().await.expect("wait rcaster");
}

fn write_accounts(path: &Path) {
    let (user, mountpoint) = account_records();
    let snapshot = AccountSnapshot::new([user], [mountpoint]).expect("account snapshot");
    AccountStore::open(path)
        .expect("open account store")
        .replace(&snapshot)
        .expect("write accounts");
}

fn account_records() -> (User, Mountpoint) {
    let password_hash = hash_password("upload").expect("password hash");
    (
        User {
            schema_version: 1,
            mod_revision: 1,
            user_id: "owner-id".to_owned(),
            username: "owner".to_owned(),
            password_hash: hash_password("secret").expect("owner password hash"),
            auth_version: 1,
            enabled: true,
            ntrip_enabled: true,
            web_enabled: true,
            role: UserRole::User,
            created_at: "2026-11-22T00:00:00Z".to_owned(),
            updated_at: "2026-11-22T00:00:00Z".to_owned(),
        },
        Mountpoint {
            schema_version: 1,
            mod_revision: 1,
            mountpoint: "C63".to_owned(),
            owner_user_id: "owner-id".to_owned(),
            enabled: true,
            ntrip1_upload_password_hash: Some(password_hash),
            advertise: true,
            region: Some("test".to_owned()),
            source_expectation: None,
            created_at: "2026-11-22T00:00:00Z".to_owned(),
            updated_at: "2026-11-22T00:00:00Z".to_owned(),
        },
    )
}

async fn read_socket_head(stream: &mut TcpStream) -> Vec<u8> {
    timeout(Duration::from_secs(5), async {
        let mut head = Vec::new();
        let mut chunk = [0_u8; 512];
        while !head.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut chunk).await.expect("socket response");
            assert!(read > 0, "connection ended before response headers");
            head.extend_from_slice(&chunk[..read]);
            assert!(head.len() <= 8192, "response headers are too large");
        }
        head
    })
    .await
    .expect("response head timeout")
}

async fn read_tls_socket_head(stream: &mut tokio_rustls::client::TlsStream<TcpStream>) -> Vec<u8> {
    timeout(Duration::from_secs(5), async {
        let mut head = Vec::new();
        let mut chunk = [0_u8; 512];
        while !head.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut chunk).await.expect("socket response");
            assert!(read > 0, "connection ended before response headers");
            head.extend_from_slice(&chunk[..read]);
            assert!(head.len() <= 8192, "response headers are too large");
        }
        head
    })
    .await
    .expect("response head timeout")
}

fn available_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind temporary port")
        .local_addr()
        .expect("temporary address")
        .port()
}

async fn connect_etcd() -> EtcdClient {
    let endpoint = std::env::var("RCASTER_ETCD_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned());
    EtcdClient::connect([endpoint], Some(ConnectOptions::new()))
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

struct TempDirectory(PathBuf);

static NEXT_TEMP_DIRECTORY: AtomicU64 = AtomicU64::new(1);

impl TempDirectory {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "rcaster-serve-{}-{unique}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("create temp directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct TestPki {
    ca_der: Vec<rustls::pki_types::CertificateDer<'static>>,
}

impl TestPki {
    fn generate(directory: &Path) -> Self {
        let mut ca_params = CertificateParams::new(Vec::new()).expect("CA params");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "rcaster-test-ca");
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
        server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_certificate = server_params
            .signed_by(&server_key, &ca_certificate, &ca_key)
            .expect("server certificate");
        fs::write(directory.join("server.crt"), server_certificate.pem())
            .expect("write server certificate");
        let server_key_path = directory.join("server.key");
        fs::write(&server_key_path, server_key.serialize_pem()).expect("write server key");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&server_key_path, fs::Permissions::from_mode(0o600))
                .expect("protect server key");
        }

        Self {
            ca_der: vec![ca_certificate.der().clone()],
        }
    }
}
