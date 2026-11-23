use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use etcd_client::Client;
use rcaster_service::config::{CasterConfig, publish_config};
use tokio::process::Command;

#[tokio::test]
async fn etcd_backup_writes_a_snapshot_without_overwriting_an_existing_file() {
    let endpoint =
        std::env::var("RCASTER_ETCD_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:2379".into());
    let directory = TempDirectory::new();
    let output = directory.path().join("cluster.db");
    let probe = format!(
        "/rcaster-backup-test/{}/{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    );
    let mut client = Client::connect([endpoint.as_str()], None)
        .await
        .expect("connect etcd");
    client
        .put(probe.as_str(), b"snapshot-probe", None)
        .await
        .expect("write snapshot probe");

    let first = Command::new(env!("CARGO_BIN_EXE_rcaster"))
        .arg("etcd-backup")
        .arg("--etcd-endpoint")
        .arg(&endpoint)
        .arg("--output")
        .arg(&output)
        .output()
        .await
        .expect("run etcd-backup");
    assert!(
        first.status.success(),
        "etcd-backup failed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(
        fs::metadata(&output).expect("snapshot metadata").len() > 4096,
        "snapshot is unexpectedly empty"
    );

    let second = Command::new(env!("CARGO_BIN_EXE_rcaster"))
        .arg("etcd-backup")
        .arg("--etcd-endpoint")
        .arg(&endpoint)
        .arg("--output")
        .arg(&output)
        .output()
        .await
        .expect("rerun etcd-backup");
    assert!(!second.status.success());
    assert!(
        String::from_utf8_lossy(&second.stderr).contains("refusing to overwrite"),
        "unexpected overwrite error: {}",
        String::from_utf8_lossy(&second.stderr)
    );

    for name in [
        "rcaster-0000000001.db",
        "rcaster-0000000002.db",
        "rcaster-0000000003.db",
    ] {
        fs::write(directory.path().join(name), b"old snapshot").expect("write old snapshot");
    }
    let scheduled = Command::new(env!("CARGO_BIN_EXE_rcaster"))
        .arg("etcd-backup")
        .arg("--etcd-endpoint")
        .arg(&endpoint)
        .arg("--backup-dir")
        .arg(directory.path())
        .arg("--retention")
        .arg("2")
        .output()
        .await
        .expect("run scheduled etcd-backup");
    assert!(
        scheduled.status.success(),
        "scheduled etcd-backup failed: {}",
        String::from_utf8_lossy(&scheduled.stderr)
    );
    assert!(!directory.path().join("rcaster-0000000001.db").exists());
    assert!(!directory.path().join("rcaster-0000000002.db").exists());
    assert!(directory.path().join("rcaster-0000000003.db").exists());

    client
        .delete(probe.as_str(), None)
        .await
        .expect("delete snapshot probe");
}

#[tokio::test]
async fn etcd_recovery_prepare_requires_confirmation_and_clears_runtime_state() {
    let endpoint =
        std::env::var("RCASTER_ETCD_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:2379".into());
    let cluster = format!(
        "recovery-cli-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    );
    let mut client = Client::connect([endpoint.as_str()], None)
        .await
        .expect("connect etcd");
    publish_config(&mut client, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    client
        .put(
            format!("/rcaster/{cluster}/runtime/sources/C63"),
            b"stale",
            None,
        )
        .await
        .expect("write stale source");

    let rejected = Command::new(env!("CARGO_BIN_EXE_rcaster"))
        .arg("etcd-recovery-prepare")
        .arg("--etcd-endpoint")
        .arg(&endpoint)
        .arg("--cluster-id")
        .arg(&cluster)
        .arg("--confirm")
        .arg("wrong")
        .output()
        .await
        .expect("run rejected recovery");
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("must exactly match"));

    let prepared = Command::new(env!("CARGO_BIN_EXE_rcaster"))
        .arg("etcd-recovery-prepare")
        .arg("--etcd-endpoint")
        .arg(&endpoint)
        .arg("--cluster-id")
        .arg(&cluster)
        .arg("--confirm")
        .arg(&cluster)
        .output()
        .await
        .expect("run recovery");
    assert!(
        prepared.status.success(),
        "recovery failed: {}",
        String::from_utf8_lossy(&prepared.stderr)
    );
    let report = String::from_utf8_lossy(&prepared.stdout);
    assert!(report.contains("\"config_generation\": 1"));
    assert!(report.contains("\"deleted_runtime_keys\": 1"));

    client
        .delete(
            format!("/rcaster/{cluster}/"),
            Some(etcd_client::DeleteOptions::new().with_prefix()),
        )
        .await
        .expect("clean test cluster");
}

struct TempDirectory(PathBuf);

impl TempDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rcaster-etcd-backup-{}-{}",
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
