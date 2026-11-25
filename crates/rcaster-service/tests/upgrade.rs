use std::{sync::Arc, time::Duration};

use ed25519_dalek::{Signer, SigningKey};
use etcd_client::{Client, ConnectOptions, GetOptions};
use rcaster_service::{
    config::{CasterConfig, publish_config},
    connections::ConnectionController,
    health::HealthState,
    upgrade::{
        SignedUpgradeManifest, UpgradeCommandWatcher, UpgradeManager, UpgradeManifest,
        UpgradePolicy,
    },
};
use serde_json::{Value, json};
use tokio::sync::watch;

#[tokio::test]
async fn signed_rollout_is_previewed_locked_and_advanced() {
    let cluster = unique("upgrade");
    let mut etcd = connect().await;
    publish_config(&mut etcd, &cluster, CasterConfig::new(1))
        .await
        .expect("config");
    put_version(&mut etcd, &cluster, "edge-1", "0.1.0", true).await;
    put_version(&mut etcd, &cluster, "core-1", "0.1.0", true).await;

    let signing_key = SigningKey::from_bytes(&[9_u8; 32]);
    let signed = signed_manifest(&signing_key, "0.2.0", b"new-binary");
    let policy = UpgradePolicy::new(
        &[hex(signing_key.verifying_key().as_bytes())],
        &["updates.example.test".to_owned()],
    )
    .expect("policy");
    let manager = UpgradeManager::new(connect().await, cluster.clone(), policy);
    let mut request = rollout_request(signed, "edge-1");
    request["nodes"] = json!([
        {"caster_id": "edge-1", "role": "edge"},
        {"caster_id": "core-1", "role": "core"}
    ]);
    let preview = manager.preview(&request).await.expect("preview");
    let preview_digest = preview["preview_digest"]
        .as_str()
        .expect("preview digest")
        .to_owned();
    request["preview_digest"] = json!(preview_digest.clone());

    let started = manager.start(&request).await.expect("start");
    assert_eq!(started["status"], "RUNNING");
    let operation_id = started["operation_id"]
        .as_str()
        .expect("operation ID")
        .to_owned();
    let commands = etcd
        .get(
            format!("/rcaster/{cluster}/runtime/upgrade-commands/{operation_id}/"),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .expect("commands");
    assert_eq!(commands.kvs().len(), 1);
    assert!(String::from_utf8_lossy(commands.kvs()[0].value()).contains("\"edge-1\""));

    let lock = etcd
        .get(format!("/rcaster/{cluster}/locks/upgrade/global"), None)
        .await
        .expect("lock");
    assert_eq!(lock.kvs().len(), 1);

    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrade-states/{operation_id}/edge-1"),
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "operation_id": operation_id,
            "caster_id": "edge-1",
            "status": "APPLIED",
            "updated_at": 1,
            "error": null
        }))
        .expect("state JSON"),
        None,
    )
    .await
    .expect("node state");
    put_version(&mut etcd, &cluster, "edge-1", "0.2.0", true).await;

    let progressed = wait_for_operation(&manager, &operation_id, |operation| {
        operation["current_node"] == 1
    })
    .await;
    assert_eq!(progressed["nodes"][0]["status"], "SUCCEEDED");
    let commands = etcd
        .get(
            format!("/rcaster/{cluster}/runtime/upgrade-commands/{operation_id}/"),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .expect("commands");
    assert_eq!(commands.kvs().len(), 2);
    etcd.delete(
        format!("/rcaster/{cluster}/runtime/upgrade-commands/{operation_id}/core-1"),
        None,
    )
    .await
    .expect("delete next-node command");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let command = etcd
                .get(
                    format!("/rcaster/{cluster}/runtime/upgrade-commands/{operation_id}/core-1"),
                    None,
                )
                .await
                .expect("next-node command");
            if !command.kvs().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("next-node command recovery timeout");

    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrade-states/{operation_id}/core-1"),
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "operation_id": operation_id,
            "caster_id": "core-1",
            "status": "APPLIED",
            "updated_at": 1,
            "error": null
        }))
        .expect("state JSON"),
        None,
    )
    .await
    .expect("core state");
    put_version(&mut etcd, &cluster, "core-1", "0.2.0", true).await;
    let succeeded = wait_for_operation(&manager, &operation_id, |operation| {
        operation["status"] == "SUCCEEDED"
    })
    .await;
    assert_eq!(succeeded["nodes"][1]["status"], "SUCCEEDED");
    let commands = etcd
        .get(
            format!("/rcaster/{cluster}/runtime/upgrade-commands/{operation_id}/"),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .expect("commands");
    assert!(commands.kvs().is_empty());
    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrade-commands/{operation_id}/leftover"),
        br#"{"operation_id":"leftover"}"#.to_vec(),
        None,
    )
    .await
    .expect("leftover command");
    let recovery_policy = UpgradePolicy::new(
        &[hex(signing_key.verifying_key().as_bytes())],
        &["updates.example.test".to_owned()],
    )
    .expect("recovery policy");
    let _recovery_manager = UpgradeManager::new(connect().await, cluster.clone(), recovery_policy);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let commands = etcd
                .get(
                    format!("/rcaster/{cluster}/runtime/upgrade-commands/{operation_id}/"),
                    Some(GetOptions::new().with_prefix()),
                )
                .await
                .expect("recovered commands");
            if commands.kvs().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("terminal command recovery timeout");
    let lock = etcd
        .get(format!("/rcaster/{cluster}/locks/upgrade/global"), None)
        .await
        .expect("lock");
    assert!(lock.kvs().is_empty());
}

#[tokio::test]
async fn preview_digest_changes_with_target_nodes_and_rejects_stale_start() {
    let cluster = unique("upgrade-preview");
    let mut etcd = connect().await;
    publish_config(&mut etcd, &cluster, CasterConfig::new(1))
        .await
        .expect("config");
    put_version(&mut etcd, &cluster, "edge-1", "0.1.0", true).await;
    put_version(&mut etcd, &cluster, "edge-2", "0.1.0", true).await;
    let signing_key = SigningKey::from_bytes(&[8_u8; 32]);
    let policy = UpgradePolicy::new(
        &[hex(signing_key.verifying_key().as_bytes())],
        &["updates.example.test".to_owned()],
    )
    .expect("policy");
    let manager = UpgradeManager::new(connect().await, cluster, policy);
    let request = rollout_request(signed_manifest(&signing_key, "0.2.0", b"new"), "edge-1");
    let preview = manager.preview(&request).await.expect("preview");
    let digest = preview["preview_digest"].as_str().unwrap().to_owned();
    let mut changed = request;
    changed["preview_digest"] = json!(digest);
    changed["nodes"] = json!([
        {"caster_id": "edge-1", "role": "edge"},
        {"caster_id": "edge-2", "role": "edge"}
    ]);
    assert!(manager.start(&changed).await.is_err());
}

#[tokio::test]
async fn upgrade_requires_a_different_administrator_to_approve() {
    let cluster = unique("upgrade-approval");
    let mut etcd = connect().await;
    publish_config(&mut etcd, &cluster, CasterConfig::new(1))
        .await
        .expect("config");
    put_version(&mut etcd, &cluster, "edge-1", "0.1.0", true).await;
    let signing_key = SigningKey::from_bytes(&[7_u8; 32]);
    let policy = UpgradePolicy::new(
        &[hex(signing_key.verifying_key().as_bytes())],
        &["updates.example.test".to_owned()],
    )
    .expect("policy");
    let manager = UpgradeManager::new_with_approval(connect().await, cluster.clone(), policy, true);
    let approver_manager = UpgradeManager::new_with_approval(
        connect().await,
        cluster.clone(),
        UpgradePolicy::new(
            &[hex(signing_key.verifying_key().as_bytes())],
            &["updates.example.test".to_owned()],
        )
        .expect("approver policy"),
        true,
    );
    let mut request = rollout_request(signed_manifest(&signing_key, "0.2.0", b"new"), "edge-1");
    let preview = manager.preview(&request).await.expect("preview");
    request["preview_digest"] = preview["preview_digest"].clone();

    let pending = manager
        .start_as(&request, "creator")
        .await
        .expect("pending operation");
    assert_eq!(pending["status"], "PENDING_APPROVAL");
    let operation_id = pending["operation_id"].as_str().expect("operation ID");
    assert!(
        approver_manager
            .approve(operation_id, "creator")
            .await
            .is_err()
    );
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let lock = etcd
        .get(format!("/rcaster/{cluster}/locks/upgrade/global"), None)
        .await
        .expect("lock");
    assert!(lock.kvs().is_empty());
    put_version(&mut etcd, &cluster, "edge-1", "0.0.9", true).await;
    assert!(
        approver_manager
            .approve(operation_id, "approver")
            .await
            .is_err()
    );
    put_version(&mut etcd, &cluster, "edge-1", "0.1.0", true).await;

    let approved = approver_manager
        .approve(operation_id, "approver")
        .await
        .expect("approval");
    assert_eq!(approved["status"], "RUNNING");
    assert_eq!(approved["created_by"], "creator");
    assert_eq!(approved["approved_by"], "approver");
    let commands = etcd
        .get(
            format!("/rcaster/{cluster}/runtime/upgrade-commands/{operation_id}/"),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .expect("commands");
    assert_eq!(commands.kvs().len(), 1);
    approver_manager.cancel(operation_id).await.expect("cancel");
}

#[tokio::test]
async fn local_command_marks_node_unready_and_requests_shutdown() {
    let cluster = unique("upgrade-local");
    let mut etcd = connect().await;
    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrades/op-1"),
        serde_json::to_vec(&json!({
            "status": "RUNNING",
            "current_node": 0,
            "nodes": [{"caster_id": "edge-1", "status": "COMMANDED"}]
        }))
        .expect("operation JSON"),
        None,
    )
    .await
    .expect("operation");
    let health = Arc::new(HealthState::default());
    health.set_ready(true);
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let watcher = UpgradeCommandWatcher::start(
        connect().await,
        cluster.clone(),
        "edge-1".to_owned(),
        Arc::clone(&health),
        None,
        shutdown_tx,
    );
    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrade-commands/op-1/edge-1"),
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "operation_id": "op-1",
            "caster_id": "edge-1",
            "action": "apply",
            "target_version": "0.2.0",
            "signed_manifest": {"manifest": {"version": "0.2.0"}}
        }))
        .expect("command JSON"),
        None,
    )
    .await
    .expect("command");
    tokio::time::timeout(Duration::from_secs(3), shutdown_rx.changed())
        .await
        .expect("drain timeout")
        .expect("drain signal");
    assert!(*shutdown_rx.borrow());
    assert!(!health.is_ready());
    let state = etcd
        .get(
            format!("/rcaster/{cluster}/runtime/upgrade-states/op-1/edge-1"),
            None,
        )
        .await
        .expect("state");
    assert!(String::from_utf8_lossy(state.kvs()[0].value()).contains("\"status\":\"DRAINING\""));
    watcher.shutdown().await;
}

#[tokio::test]
async fn stale_or_already_handled_local_commands_do_not_trigger_shutdown() {
    let cluster = unique("upgrade-local-stale");
    let mut etcd = connect().await;
    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrades/op-2"),
        serde_json::to_vec(&json!({
            "status": "SUCCEEDED",
            "current_node": 0,
            "nodes": [{"caster_id": "edge-1", "status": "SUCCEEDED"}]
        }))
        .expect("operation JSON"),
        None,
    )
    .await
    .expect("operation");
    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrade-commands/op-2/edge-1"),
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "operation_id": "op-2",
            "caster_id": "edge-1",
            "action": "apply",
            "target_version": "0.2.0",
            "signed_manifest": {"manifest": {"version": "0.2.0"}}
        }))
        .expect("command JSON"),
        None,
    )
    .await
    .expect("command");
    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrades/op-3"),
        serde_json::to_vec(&json!({
            "status": "RUNNING",
            "current_node": 0,
            "nodes": [{"caster_id": "edge-1", "status": "COMMANDED"}]
        }))
        .expect("operation JSON"),
        None,
    )
    .await
    .expect("running operation");
    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrade-commands/op-3/edge-1"),
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "operation_id": "op-3",
            "caster_id": "edge-1",
            "action": "apply",
            "target_version": "0.2.0",
            "signed_manifest": {"manifest": {"version": "0.2.0"}}
        }))
        .expect("command JSON"),
        None,
    )
    .await
    .expect("active command");
    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrade-states/op-3/edge-1"),
        br#"{"status":"APPLIED"}"#.to_vec(),
        None,
    )
    .await
    .expect("handled state");
    let health = Arc::new(HealthState::default());
    health.set_ready(true);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let watcher = UpgradeCommandWatcher::start(
        connect().await,
        cluster.clone(),
        "edge-1".to_owned(),
        Arc::clone(&health),
        None,
        shutdown_tx,
    );
    tokio::time::sleep(Duration::from_millis(1400)).await;
    assert!(!*shutdown_rx.borrow());
    assert!(health.is_ready());
    watcher.shutdown().await;
}

#[tokio::test]
async fn draining_state_does_not_overwrite_terminal_state() {
    let cluster = unique("upgrade-local-race");
    let mut etcd = connect().await;
    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrades/op-4"),
        serde_json::to_vec(&json!({
            "status": "RUNNING",
            "current_node": 0,
            "nodes": [{"caster_id": "edge-1", "status": "COMMANDED"}]
        }))
        .expect("operation JSON"),
        None,
    )
    .await
    .expect("operation");
    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrade-commands/op-4/edge-1"),
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "operation_id": "op-4",
            "caster_id": "edge-1",
            "action": "apply",
            "target_version": "0.2.0",
            "signed_manifest": {"manifest": {"version": "0.2.0"}}
        }))
        .expect("command JSON"),
        None,
    )
    .await
    .expect("command");
    let health = Arc::new(HealthState::default());
    health.set_ready(true);
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let watcher = UpgradeCommandWatcher::start(
        connect().await,
        cluster.clone(),
        "edge-1".to_owned(),
        Arc::clone(&health),
        Some(Arc::new(ConnectionController::default())),
        shutdown_tx,
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while health.is_ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("drain start timeout");
    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrade-states/op-4/edge-1"),
        br#"{"status":"APPLIED"}"#.to_vec(),
        None,
    )
    .await
    .expect("terminal state");
    tokio::time::timeout(Duration::from_secs(3), shutdown_rx.changed())
        .await
        .expect("drain timeout")
        .expect("drain signal");
    let state = etcd
        .get(
            format!("/rcaster/{cluster}/runtime/upgrade-states/op-4/edge-1"),
            None,
        )
        .await
        .expect("state");
    assert!(
        String::from_utf8_lossy(state.kvs()[0].value()).contains("\"status\":\"APPLIED\""),
        "DRAINING must not overwrite a concurrent terminal state"
    );
    watcher.shutdown().await;
}

#[tokio::test]
async fn mismatched_local_command_target_does_not_trigger_shutdown() {
    let cluster = unique("upgrade-local-version-mismatch");
    let mut etcd = connect().await;
    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrades/op-5"),
        serde_json::to_vec(&json!({
            "status": "RUNNING",
            "current_node": 0,
            "nodes": [{"caster_id": "edge-1", "status": "COMMANDED"}]
        }))
        .expect("operation JSON"),
        None,
    )
    .await
    .expect("operation");
    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrade-commands/op-5/edge-1"),
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "operation_id": "op-5",
            "caster_id": "edge-1",
            "action": "apply",
            "target_version": "0.2.0",
            "signed_manifest": {"manifest": {"version": "0.1.0"}}
        }))
        .expect("command JSON"),
        None,
    )
    .await
    .expect("command");
    let health = Arc::new(HealthState::default());
    health.set_ready(true);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let watcher = UpgradeCommandWatcher::start(
        connect().await,
        cluster,
        "edge-1".to_owned(),
        Arc::clone(&health),
        None,
        shutdown_tx,
    );
    tokio::time::sleep(Duration::from_millis(1400)).await;
    assert!(!*shutdown_rx.borrow());
    assert!(health.is_ready());
    watcher.shutdown().await;
}

#[tokio::test]
async fn failed_node_requests_rollback_and_does_not_advance() {
    let cluster = unique("upgrade-failure");
    let mut etcd = connect().await;
    publish_config(&mut etcd, &cluster, CasterConfig::new(1))
        .await
        .expect("config");
    put_version(&mut etcd, &cluster, "edge-1", "0.1.0", true).await;
    put_version(&mut etcd, &cluster, "edge-2", "0.1.0", true).await;
    let signing_key = SigningKey::from_bytes(&[6_u8; 32]);
    let policy = UpgradePolicy::new(
        &[hex(signing_key.verifying_key().as_bytes())],
        &["updates.example.test".to_owned()],
    )
    .expect("policy");
    let manager = UpgradeManager::new(connect().await, cluster.clone(), policy);
    let mut request = rollout_request(signed_manifest(&signing_key, "0.2.0", b"new"), "edge-1");
    request["nodes"] = json!([
        {"caster_id": "edge-1", "role": "edge"},
        {"caster_id": "edge-2", "role": "edge"}
    ]);
    let preview = manager.preview(&request).await.expect("preview");
    request["preview_digest"] = preview["preview_digest"].clone();
    let operation = manager.start(&request).await.expect("start");
    let operation_id = operation["operation_id"].as_str().expect("operation ID");
    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrade-states/{operation_id}/edge-1"),
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "operation_id": operation_id,
            "caster_id": "edge-1",
            "status": "FAILED",
            "updated_at": 1,
            "error": "readiness failed"
        }))
        .expect("state JSON"),
        None,
    )
    .await
    .expect("failed state");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = etcd
                .get(
                    format!("/rcaster/{cluster}/runtime/upgrade-commands/{operation_id}/"),
                    Some(GetOptions::new().with_prefix()),
                )
                .await
                .expect("commands");
            if response.kvs().iter().any(|value| {
                String::from_utf8_lossy(value.value()).contains("\"action\":\"rollback\"")
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("rollback command timeout");
    etcd.put(
        format!("/rcaster/{cluster}/runtime/upgrade-states/{operation_id}/edge-1"),
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "operation_id": operation_id,
            "caster_id": "edge-1",
            "status": "ROLLED_BACK",
            "updated_at": 2,
            "error": "readiness failed"
        }))
        .expect("state JSON"),
        None,
    )
    .await
    .expect("rolled back state");
    let failed = wait_for_operation(&manager, operation_id, |operation| {
        operation["status"] == "FAILED"
    })
    .await;
    assert_eq!(failed["nodes"][0]["status"], "ROLLED_BACK");
    assert_eq!(failed["nodes"][1]["status"], "PENDING");
    let commands = etcd
        .get(
            format!("/rcaster/{cluster}/runtime/upgrade-commands/{operation_id}/"),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .expect("commands");
    assert!(commands.kvs().is_empty());
    let lock = etcd
        .get(format!("/rcaster/{cluster}/locks/upgrade/global"), None)
        .await
        .expect("lock");
    assert!(lock.kvs().is_empty());
}

async fn wait_for_operation(
    manager: &UpgradeManager,
    operation_id: &str,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let operation = manager.get(operation_id).await.expect("operation");
            if predicate(&operation) {
                return operation;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("operation advance timeout")
}

fn rollout_request(signed: SignedUpgradeManifest, caster_id: &str) -> Value {
    json!({
        "manifest": signed.manifest,
        "signature": signed.signature,
        "nodes": [{"caster_id": caster_id, "role": "edge"}],
        "max_unavailable_core": 1,
        "max_unavailable_edge": 1,
        "post_upgrade_stability_s": 0,
        "auto_rollback": true
    })
}

fn signed_manifest(
    signing_key: &SigningKey,
    version: &str,
    artifact: &[u8],
) -> SignedUpgradeManifest {
    use sha2::{Digest, Sha256};
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
    SignedUpgradeManifest {
        signature: hex(&signing_key
            .sign(&serde_json::to_vec(&manifest).expect("manifest JSON"))
            .to_bytes()),
        manifest,
    }
}

async fn put_version(
    etcd: &mut Client,
    cluster: &str,
    caster_id: &str,
    version: &str,
    healthy: bool,
) {
    etcd.put(
        format!("/rcaster/{cluster}/runtime/version-status/{caster_id}"),
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "caster_id": caster_id,
            "software_version": version,
            "config_schema": 1,
            "etcd_schema": 1,
            "api_schema": 1,
            "healthy": healthy
        }))
        .expect("version JSON"),
        None,
    )
    .await
    .expect("version");
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
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
