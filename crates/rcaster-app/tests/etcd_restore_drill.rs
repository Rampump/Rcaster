use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, GetOptions};
use rcaster_service::account_store::AccountStore;
use rcaster_service::config::{CasterConfig, publish_config};
use serde_json::json;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    process::{Child, Command},
    time::sleep,
};

#[tokio::test]
#[ignore = "requires RCASTER_ETCD_BIN and its sibling etcdutl binary"]
async fn restores_a_snapshot_cleans_stale_state_and_starts_a_ready_caster() {
    let etcd = PathBuf::from(std::env::var("RCASTER_ETCD_BIN").expect("RCASTER_ETCD_BIN"));
    let etcdutl = etcd.with_file_name(if cfg!(windows) {
        "etcdutl.exe"
    } else {
        "etcdutl"
    });
    let directory = TempDirectory::new();
    let ports = available_ports(6);
    let origin_port = ports[0];
    let origin_peer = ports[1];
    let restored_port = ports[2];
    let restored_peer = ports[3];
    let caster_port = ports[4];
    let health_port = ports[5];
    let cluster = format!(
        "restore-drill-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    );
    let origin = directory.path().join("origin");
    let restored = directory.path().join("restored");
    let snapshot = directory.path().join("cluster.db");
    let database = directory.path().join("accounts.redb");
    let caster_config = directory.path().join("caster.json");
    AccountStore::open(&database).expect("create account database");

    let mut origin_process = start_etcd(
        &etcd,
        &origin,
        "rcaster-origin",
        origin_port,
        origin_peer,
        "new",
    )
    .await;
    let origin_endpoint = format!("http://127.0.0.1:{origin_port}");
    let mut origin_client = connect_with_retry(&origin_endpoint).await;
    publish_config(&mut origin_client, &cluster, CasterConfig::new(7))
        .await
        .expect("publish config");
    origin_client
        .put(
            format!("/rcaster/{cluster}/runtime/source-claims/C63"),
            b"old-owner",
            None,
        )
        .await
        .expect("write stale source claim");
    origin_client
        .put(
            format!("/rcaster/{cluster}/online/users/owner/sessions/old"),
            b"old-session",
            None,
        )
        .await
        .expect("write stale session");

    let backup = Command::new(env!("CARGO_BIN_EXE_rcaster"))
        .arg("etcd-backup")
        .arg("--etcd-endpoint")
        .arg(&origin_endpoint)
        .arg("--output")
        .arg(&snapshot)
        .output()
        .await
        .expect("run etcd-backup");
    assert!(
        backup.status.success(),
        "backup failed: {}",
        String::from_utf8_lossy(&backup.stderr)
    );
    let _ = origin_process.kill().await;
    let _ = origin_process.wait().await;
    let rto_started = Instant::now();

    let restore = Command::new(&etcdutl)
        .arg("snapshot")
        .arg("restore")
        .arg(&snapshot)
        .arg("--data-dir")
        .arg(&restored)
        .arg("--name=rcaster-restored")
        .arg(format!(
            "--initial-cluster=rcaster-restored=http://127.0.0.1:{restored_peer}"
        ))
        .arg(format!(
            "--initial-advertise-peer-urls=http://127.0.0.1:{restored_peer}"
        ))
        .arg("--initial-cluster-token=rcaster-restore-drill")
        .output()
        .await
        .expect("run etcdutl restore");
    assert!(
        restore.status.success(),
        "restore failed: {}",
        String::from_utf8_lossy(&restore.stderr)
    );

    let mut restored_process = start_etcd(
        &etcd,
        &restored,
        "rcaster-restored",
        restored_port,
        restored_peer,
        "existing",
    )
    .await;
    let restored_endpoint = format!("http://127.0.0.1:{restored_port}");
    let mut restored_client = connect_with_retry(&restored_endpoint).await;
    assert_eq!(
        prefix(&mut restored_client, &cluster, "config/")
            .await
            .len(),
        2,
        "snapshot must restore the config and its archive"
    );
    assert_eq!(
        prefix(&mut restored_client, &cluster, "runtime/")
            .await
            .len(),
        1
    );
    assert_eq!(
        prefix(&mut restored_client, &cluster, "online/")
            .await
            .len(),
        1
    );

    let prepare = Command::new(env!("CARGO_BIN_EXE_rcaster"))
        .arg("etcd-recovery-prepare")
        .arg("--etcd-endpoint")
        .arg(&restored_endpoint)
        .arg("--cluster-id")
        .arg(&cluster)
        .arg("--confirm")
        .arg(&cluster)
        .output()
        .await
        .expect("run recovery prepare");
    assert!(
        prepare.status.success(),
        "recovery prepare failed: {}",
        String::from_utf8_lossy(&prepare.stderr)
    );
    assert_eq!(
        prefix(&mut restored_client, &cluster, "config/")
            .await
            .len(),
        2
    );
    assert!(
        prefix(&mut restored_client, &cluster, "runtime/")
            .await
            .is_empty()
    );
    assert!(
        prefix(&mut restored_client, &cluster, "online/")
            .await
            .is_empty()
    );
    assert!(
        prefix(&mut restored_client, &cluster, "locks/")
            .await
            .is_empty()
    );

    fs::write(
        &caster_config,
        serde_json::to_vec_pretty(&json!({
            "bind": format!("127.0.0.1:{caster_port}"),
            "health_bind": format!("127.0.0.1:{health_port}"),
            "account_db": database,
            "client_auth_require_tls": false,
            "source_auth_require_tls": false,
            "head_timeout_ms": 1000,
            "cluster": {
                "etcd_endpoints": [restored_endpoint],
                "cluster_id": cluster,
                "caster_id": "restore-drill-caster",
                "runtime_epoch": format!("restore-{}", unique("epoch")),
                "relay_endpoint": format!("127.0.0.1:{caster_port}")
            }
        }))
        .expect("encode caster config"),
    )
    .expect("write caster config");
    let mut caster = Command::new(env!("CARGO_BIN_EXE_rcaster"))
        .arg("serve")
        .arg("--config")
        .arg(&caster_config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start restored caster");
    let stdout = caster.stdout.take().expect("caster stdout");
    let mut lines = BufReader::new(stdout).lines();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match lines.next_line().await.expect("caster stdout") {
                Some(line) if line.starts_with("health listening on ") => break,
                Some(_) => {}
                None => {
                    let mut stderr = caster.stderr.take().expect("caster stderr");
                    let mut message = String::new();
                    stderr
                        .read_to_string(&mut message)
                        .await
                        .expect("caster stderr");
                    panic!("caster exited before health listener: {message}");
                }
            }
        }
    })
    .await
    .expect("caster health listener timeout");
    wait_for_caster_ready(health_port).await;
    let rto = rto_started.elapsed();
    println!("snapshot restore to Caster ready: {} ms", rto.as_millis());
    assert!(
        rto < Duration::from_secs(3600),
        "recovery RTO exceeded the 60-minute target: {rto:?}"
    );

    let _ = caster.kill().await;
    let _ = caster.wait().await;
    let _ = restored_process.kill().await;
    let _ = restored_process.wait().await;
}

async fn start_etcd(
    binary: &Path,
    data_dir: &Path,
    name: &str,
    client_port: u16,
    peer_port: u16,
    cluster_state: &str,
) -> Child {
    let client_url = format!("http://127.0.0.1:{client_port}");
    let peer_url = format!("http://127.0.0.1:{peer_port}");
    Command::new(binary)
        .arg(format!("--name={name}"))
        .arg(format!("--data-dir={}", data_dir.display()))
        .arg(format!("--listen-client-urls={client_url}"))
        .arg(format!("--advertise-client-urls={client_url}"))
        .arg(format!("--listen-peer-urls={peer_url}"))
        .arg(format!("--initial-advertise-peer-urls={peer_url}"))
        .arg(format!("--initial-cluster={name}={peer_url}"))
        .arg(format!("--initial-cluster-state={cluster_state}"))
        .arg("--initial-cluster-token=rcaster-restore-drill")
        .arg("--log-level=error")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start etcd")
}

async fn connect_with_retry(endpoint: &str) -> Client {
    for _ in 0..100 {
        if let Ok(mut client) = Client::connect([endpoint], None).await
            && client.status().await.is_ok()
        {
            return client;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("etcd did not become ready at {endpoint}");
}

async fn prefix(client: &mut Client, cluster: &str, suffix: &str) -> Vec<etcd_client::KeyValue> {
    client
        .get(
            format!("/rcaster/{cluster}/{suffix}"),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .expect("read etcd prefix")
        .kvs()
        .to_vec()
}

async fn wait_for_caster_ready(health_port: u16) {
    for _ in 0..100 {
        if let Ok(response) = health_request(health_port, "/health/ready").await
            && response.starts_with("HTTP/1.1 200")
            && response.ends_with("ready\n")
            && let Ok(metrics) = health_request(health_port, "/metrics").await
            && metrics.contains("rcaster_ready 1")
            && metrics
                .matches("rcaster_cluster_dependency_healthy{kind=")
                .count()
                == 5
            && !metrics.contains("rcaster_cluster_dependency_healthy{kind=\"lease\"} 0")
            && !metrics.contains("rcaster_cluster_dependency_healthy{kind=\"config\"} 0")
            && !metrics.contains("rcaster_cluster_dependency_healthy{kind=\"blacklist\"} 0")
            && !metrics.contains("rcaster_cluster_dependency_healthy{kind=\"sources\"} 0")
            && !metrics.contains("rcaster_cluster_dependency_healthy{kind=\"accounts\"} 0")
            && metrics.contains("rcaster_config_generation 7")
        {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("restored Caster did not become ready");
}

async fn health_request(port: u16, path: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    Ok(response)
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

fn available_ports(count: usize) -> Vec<u16> {
    let listeners = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("bind temporary port"))
        .collect::<Vec<_>>();
    let ports = listeners
        .iter()
        .map(|listener| listener.local_addr().expect("temporary address").port())
        .collect();
    drop(listeners);
    ports
}

struct TempDirectory(PathBuf);

impl TempDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rcaster-etcd-restore-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        fs::create_dir_all(&path).expect("create temporary directory");
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
