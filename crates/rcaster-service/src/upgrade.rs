use std::{
    fmt,
    sync::{Arc, Weak},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use ed25519_dalek::{Signature, VerifyingKey};
use etcd_client::{
    AlarmAction, AlarmType, Client, Compare, CompareOp, DeleteOptions, EventType, GetOptions,
    PutOptions, Txn, TxnOp, WatchOptions,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    sync::{Mutex, oneshot, watch},
    task::JoinHandle,
};

use crate::{connections::ConnectionController, health::HealthState};

const MANIFEST_SCHEMA: u8 = 1;
const OPERATION_SCHEMA: u8 = 1;
const LOCK_TTL_S: i64 = 3600;
const COMMAND_TIMEOUT_S: u64 = 900;
const DEFAULT_ETCD_QUOTA_BYTES: i64 = 2 * 1024 * 1024 * 1024;
const ETCD_MAX_USED_RATIO: f64 = 0.90;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct UpgradeManifest {
    pub schema_version: u8,
    pub version: String,
    pub os: String,
    pub arch: String,
    pub config_schema: u16,
    pub etcd_schema: u16,
    pub api_schema: u16,
    pub artifact_url: String,
    pub artifact_size: u64,
    pub artifact_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SignedUpgradeManifest {
    pub manifest: UpgradeManifest,
    pub signature: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum UpgradeNodeRole {
    Edge,
    Core,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct UpgradeNode {
    pub caster_id: String,
    pub role: UpgradeNodeRole,
}

#[derive(Clone, Debug, Deserialize)]
pub struct UpgradeRequest {
    #[serde(flatten)]
    pub signed: SignedUpgradeManifest,
    pub nodes: Vec<UpgradeNode>,
    #[serde(default = "default_max_unavailable")]
    pub max_unavailable_core: usize,
    #[serde(default = "default_max_unavailable")]
    pub max_unavailable_edge: usize,
    #[serde(default = "default_stability_seconds")]
    pub post_upgrade_stability_s: u64,
    #[serde(default)]
    pub auto_rollback: bool,
    #[serde(default)]
    pub preview_digest: Option<String>,
}

#[derive(Clone, Debug)]
pub struct UpgradePolicy {
    trusted_keys: Vec<VerifyingKey>,
    artifact_hosts: Vec<String>,
}

impl UpgradePolicy {
    pub fn new(
        trusted_public_keys: &[String],
        artifact_hosts: &[String],
    ) -> Result<Self, UpgradeError> {
        if trusted_public_keys.is_empty() {
            return Err(UpgradeError::Policy(
                "at least one trusted public key is required".to_owned(),
            ));
        }
        if artifact_hosts.is_empty() {
            return Err(UpgradeError::Policy(
                "at least one artifact host is required".to_owned(),
            ));
        }
        let trusted_keys = trusted_public_keys
            .iter()
            .map(|key| {
                let bytes = decode_hex::<32>(key).map_err(|_| {
                    UpgradeError::Policy("trusted public key is invalid".to_owned())
                })?;
                VerifyingKey::from_bytes(&bytes)
                    .map_err(|_| UpgradeError::Policy("trusted public key is invalid".to_owned()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let artifact_hosts = artifact_hosts
            .iter()
            .map(|host| validate_artifact_host(host))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            trusted_keys,
            artifact_hosts,
        })
    }
}

#[derive(Debug)]
pub enum UpgradeError {
    Policy(String),
    InvalidManifest(String),
    InvalidSignature,
    EtcdPreflight(String),
    ApprovalRequired,
    NotFound,
    Conflict,
    Unavailable(etcd_client::Error),
    Json(serde_json::Error),
}

impl UpgradeError {
    pub fn status(&self) -> u16 {
        match self {
            Self::Policy(_) | Self::InvalidManifest(_) | Self::InvalidSignature | Self::Json(_) => {
                400
            }
            Self::EtcdPreflight(_) => 503,
            Self::ApprovalRequired | Self::Conflict => 409,
            Self::NotFound => 404,
            Self::Unavailable(_) => 503,
        }
    }
}

impl fmt::Display for UpgradeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Policy(message) | Self::InvalidManifest(message) => formatter.write_str(message),
            Self::InvalidSignature => formatter.write_str("upgrade manifest signature is invalid"),
            Self::EtcdPreflight(message) => formatter.write_str(message),
            Self::ApprovalRequired => {
                formatter.write_str("a different administrator must approve this upgrade")
            }
            Self::NotFound => formatter.write_str("upgrade operation not found"),
            Self::Conflict => formatter.write_str("another upgrade operation is active"),
            Self::Unavailable(error) => write!(formatter, "upgrade state unavailable: {error}"),
            Self::Json(error) => write!(formatter, "upgrade JSON error: {error}"),
        }
    }
}

impl std::error::Error for UpgradeError {}

impl From<etcd_client::Error> for UpgradeError {
    fn from(error: etcd_client::Error) -> Self {
        Self::Unavailable(error)
    }
}

impl From<serde_json::Error> for UpgradeError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl UpgradePolicy {
    pub fn verify(&self, signed: &SignedUpgradeManifest) -> Result<UpgradeManifest, UpgradeError> {
        let manifest = &signed.manifest;
        if manifest.schema_version != MANIFEST_SCHEMA {
            return Err(UpgradeError::InvalidManifest(format!(
                "unsupported manifest schema {}",
                manifest.schema_version
            )));
        }
        if manifest.version.is_empty() || manifest.version.len() > 128 {
            return Err(UpgradeError::InvalidManifest(
                "target version is invalid".to_owned(),
            ));
        }
        if manifest.os != std::env::consts::OS || manifest.arch != std::env::consts::ARCH {
            return Err(UpgradeError::InvalidManifest(format!(
                "artifact targets {}/{}, expected {}/{}",
                manifest.os,
                manifest.arch,
                std::env::consts::OS,
                std::env::consts::ARCH
            )));
        }
        if manifest.config_schema != 1 || manifest.etcd_schema != 1 || manifest.api_schema != 1 {
            return Err(UpgradeError::InvalidManifest(
                "manifest schemas are not compatible with this Caster".to_owned(),
            ));
        }
        if manifest.artifact_size == 0 || manifest.artifact_size > 2 * 1024 * 1024 * 1024 {
            return Err(UpgradeError::InvalidManifest(
                "artifact size is outside the allowed range".to_owned(),
            ));
        }
        if manifest.artifact_sha256.len() != 64
            || !manifest
                .artifact_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(UpgradeError::InvalidManifest(
                "artifact SHA-256 is invalid".to_owned(),
            ));
        }
        let host = artifact_host(&manifest.artifact_url)?;
        if !self
            .artifact_hosts
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(&host))
        {
            return Err(UpgradeError::InvalidManifest(
                "artifact host is not trusted".to_owned(),
            ));
        }
        let signature =
            decode_hex::<64>(&signed.signature).map_err(|_| UpgradeError::InvalidSignature)?;
        let signature = Signature::from_bytes(&signature);
        let signed_bytes = serde_json::to_vec(manifest)?;
        if !self
            .trusted_keys
            .iter()
            .any(|key| key.verify_strict(&signed_bytes, &signature).is_ok())
        {
            return Err(UpgradeError::InvalidSignature);
        }
        Ok(manifest.clone())
    }
}

pub fn verify_artifact(
    path: &std::path::Path,
    manifest: &UpgradeManifest,
) -> Result<(), UpgradeError> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        UpgradeError::InvalidManifest(format!("artifact cannot be read: {error}"))
    })?;
    if metadata.len() != manifest.artifact_size {
        return Err(UpgradeError::InvalidManifest(format!(
            "artifact size mismatch: expected {}, got {}",
            manifest.artifact_size,
            metadata.len()
        )));
    }
    let bytes = std::fs::read(path).map_err(|error| {
        UpgradeError::InvalidManifest(format!("artifact cannot be read: {error}"))
    })?;
    let actual = format!("{:x}", Sha256::digest(&bytes));
    if !actual.eq_ignore_ascii_case(&manifest.artifact_sha256) {
        return Err(UpgradeError::InvalidManifest(
            "artifact SHA-256 does not match the signed manifest".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Clone)]
pub struct UpgradeManager {
    inner: Arc<Mutex<UpgradeInner>>,
}

pub struct UpgradeCommandWatcher {
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

struct UpgradeInner {
    client: Client,
    cluster_id: String,
    policy: UpgradePolicy,
    require_approval: bool,
    lock: Option<HeldLock>,
}

struct HeldLock {
    operation_id: String,
    lease_id: i64,
    stop: Option<oneshot::Sender<()>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct UpgradeOperation {
    schema_version: u8,
    operation_id: String,
    status: OperationStatus,
    signed_manifest: SignedUpgradeManifest,
    nodes: Vec<OperationNode>,
    current_node: usize,
    max_unavailable_core: usize,
    max_unavailable_edge: usize,
    post_upgrade_stability_s: u64,
    auto_rollback: bool,
    preview_digest: String,
    #[serde(default)]
    created_by: String,
    #[serde(default)]
    approved_by: Option<String>,
    rollback_of: Option<String>,
    error: Option<String>,
    created_at: u64,
    updated_at: u64,
    command_at: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum OperationStatus {
    PendingApproval,
    Running,
    RollingBack,
    Succeeded,
    Failed,
    Canceled,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct OperationNode {
    caster_id: String,
    role: UpgradeNodeRole,
    status: OperationNodeStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum OperationNodeStatus {
    Pending,
    Commanded,
    Applied,
    Succeeded,
    Failed,
    RolledBack,
}

struct Preflight {
    manifest: UpgradeManifest,
    request: UpgradeRequest,
    digest: String,
    nodes: Vec<OperationNode>,
}

impl UpgradeManager {
    pub fn new(client: Client, cluster_id: String, policy: UpgradePolicy) -> Self {
        Self::new_with_approval(client, cluster_id, policy, false)
    }

    pub fn new_with_approval(
        client: Client,
        cluster_id: String,
        policy: UpgradePolicy,
        require_approval: bool,
    ) -> Self {
        let inner = Arc::new(Mutex::new(UpgradeInner {
            client,
            cluster_id,
            policy,
            require_approval,
            lock: None,
        }));
        let weak = Arc::downgrade(&inner);
        tokio::spawn(run_advance_loop(weak));
        Self { inner }
    }

    pub async fn preview(&self, body: &Value) -> Result<Value, UpgradeError> {
        let mut inner = self.inner.lock().await;
        let preflight = inner.preflight(body).await?;
        Ok(json!({
            "ready": true,
            "target_version": preflight.manifest.version,
            "artifact_sha256": preflight.manifest.artifact_sha256,
            "nodes": preflight.nodes,
            "preview_digest": preflight.digest,
            "post_upgrade_stability_s": preflight.request.post_upgrade_stability_s,
        }))
    }

    pub async fn start(&self, body: &Value) -> Result<Value, UpgradeError> {
        self.start_as(body, "local").await
    }

    pub async fn start_as(&self, body: &Value, actor: &str) -> Result<Value, UpgradeError> {
        let mut inner = self.inner.lock().await;
        inner.start(body, None, actor).await
    }

    pub async fn approve(&self, operation_id: &str, actor: &str) -> Result<Value, UpgradeError> {
        validate_identifier(operation_id)?;
        let mut inner = self.inner.lock().await;
        inner.approve(operation_id, actor).await
    }

    pub async fn get(&self, operation_id: &str) -> Result<Value, UpgradeError> {
        validate_identifier(operation_id)?;
        let mut inner = self.inner.lock().await;
        let operation = inner.load_operation(operation_id).await?;
        Ok(serde_json::to_value(operation)?)
    }

    pub async fn cancel(&self, operation_id: &str) -> Result<Value, UpgradeError> {
        validate_identifier(operation_id)?;
        let mut inner = self.inner.lock().await;
        let mut operation = inner.load_operation(operation_id).await?;
        if matches!(
            operation.status,
            OperationStatus::Succeeded | OperationStatus::Failed | OperationStatus::Canceled
        ) {
            return Err(UpgradeError::Conflict);
        }
        operation.status = OperationStatus::Canceled;
        operation.updated_at = now_timestamp();
        inner.put_operation(&operation).await?;
        inner.delete_commands(operation_id).await?;
        inner.release_lock(operation_id).await;
        inner.clear_lock_key(operation_id).await?;
        Ok(serde_json::to_value(operation)?)
    }

    pub async fn rollback(&self, operation_id: &str, body: &Value) -> Result<Value, UpgradeError> {
        self.rollback_as(operation_id, body, "local").await
    }

    pub async fn rollback_as(
        &self,
        operation_id: &str,
        body: &Value,
        actor: &str,
    ) -> Result<Value, UpgradeError> {
        validate_identifier(operation_id)?;
        let mut inner = self.inner.lock().await;
        let previous = inner.load_operation(operation_id).await?;
        if matches!(
            previous.status,
            OperationStatus::Running | OperationStatus::RollingBack
        ) {
            return Err(UpgradeError::Conflict);
        }
        let mut request = body.clone();
        if request.get("nodes").is_none() {
            let nodes = previous
                .nodes
                .iter()
                .filter(|node| {
                    matches!(
                        node.status,
                        OperationNodeStatus::Succeeded | OperationNodeStatus::Failed
                    )
                })
                .map(|node| UpgradeNode {
                    caster_id: node.caster_id.clone(),
                    role: node.role,
                })
                .collect::<Vec<_>>();
            request["nodes"] = serde_json::to_value(nodes)?;
        }
        inner
            .start(&request, Some(operation_id.to_owned()), actor)
            .await
    }
}

impl UpgradeCommandWatcher {
    pub fn start(
        client: Client,
        cluster_id: String,
        caster_id: String,
        health: Arc<HealthState>,
        connections: Option<Arc<ConnectionController>>,
        shutdown: watch::Sender<bool>,
    ) -> Self {
        let (stop_tx, stop_rx) = oneshot::channel();
        let task = tokio::spawn(run_local_command_watch(
            client,
            cluster_id,
            caster_id,
            health,
            connections,
            shutdown,
            stop_rx,
        ));
        Self {
            stop: Some(stop_tx),
            task: Some(task),
        }
    }

    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl UpgradeInner {
    async fn preflight(&mut self, body: &Value) -> Result<Preflight, UpgradeError> {
        let request: UpgradeRequest = serde_json::from_value(body.clone())?;
        if request.nodes.is_empty() {
            return Err(UpgradeError::InvalidManifest(
                "at least one node must be selected".to_owned(),
            ));
        }
        if request.max_unavailable_core == 0
            || request.max_unavailable_edge == 0
            || request.post_upgrade_stability_s > 3600
        {
            return Err(UpgradeError::InvalidManifest(
                "rollout limits are invalid".to_owned(),
            ));
        }
        self.check_etcd_preflight().await?;
        let manifest = self.policy.verify(&request.signed)?;
        let mut seen = std::collections::BTreeSet::new();
        let mut saw_core = false;
        for node in &request.nodes {
            validate_identifier(&node.caster_id)?;
            if !seen.insert(node.caster_id.as_str()) {
                return Err(UpgradeError::InvalidManifest(
                    "node appears more than once".to_owned(),
                ));
            }
            if node.role == UpgradeNodeRole::Core {
                saw_core = true;
            } else if saw_core {
                return Err(UpgradeError::InvalidManifest(
                    "edge nodes must be ordered before core nodes".to_owned(),
                ));
            }
        }
        let versions = self.node_versions().await?;
        let config = self.config_snapshot().await?;
        let mut observed = Vec::new();
        for node in &request.nodes {
            let Some(version) = versions
                .iter()
                .find(|version| version["caster_id"] == node.caster_id)
            else {
                return Err(UpgradeError::InvalidManifest(format!(
                    "node {} is not active",
                    node.caster_id
                )));
            };
            if version["healthy"] != true {
                return Err(UpgradeError::InvalidManifest(format!(
                    "node {} is not healthy",
                    node.caster_id
                )));
            }
            if version["config_schema"] != manifest.config_schema
                || version["etcd_schema"] != manifest.etcd_schema
                || version["api_schema"] != manifest.api_schema
            {
                return Err(UpgradeError::InvalidManifest(format!(
                    "node {} schemas are incompatible",
                    node.caster_id
                )));
            }
            observed.push(version.clone());
        }
        observed
            .sort_by_key(|version| version["caster_id"].as_str().unwrap_or_default().to_owned());
        let digest_bytes = serde_json::to_vec(&json!({
            "manifest": request.signed,
            "nodes": request.nodes,
            "observed": observed,
            "config_generation": config["generation"],
            "config_revision": config["revision"],
            "post_upgrade_stability_s": request.post_upgrade_stability_s,
            "auto_rollback": request.auto_rollback,
        }))?;
        let digest = format!("{:x}", Sha256::digest(digest_bytes));
        let nodes = request
            .nodes
            .iter()
            .map(|node| OperationNode {
                caster_id: node.caster_id.clone(),
                role: node.role,
                status: OperationNodeStatus::Pending,
            })
            .collect();
        Ok(Preflight {
            manifest,
            request,
            digest,
            nodes,
        })
    }

    async fn start(
        &mut self,
        body: &Value,
        rollback_of: Option<String>,
        actor: &str,
    ) -> Result<Value, UpgradeError> {
        let preflight = self.preflight(body).await?;
        if preflight.request.preview_digest.as_deref() != Some(&preflight.digest) {
            return Err(UpgradeError::InvalidManifest(
                "preview is missing or stale; run preview again".to_owned(),
            ));
        }
        if self.require_approval {
            return self.start_pending(preflight, rollback_of, actor).await;
        }
        if self.lock.is_some() {
            return Err(UpgradeError::Conflict);
        }
        let operation_id = random_hex(16);
        let lease_id = self.client.lease_grant(LOCK_TTL_S, None).await?.id();
        let now = now_timestamp();
        let mut operation = UpgradeOperation {
            schema_version: OPERATION_SCHEMA,
            operation_id: operation_id.clone(),
            status: OperationStatus::Running,
            signed_manifest: preflight.request.signed,
            nodes: preflight.nodes,
            current_node: 0,
            max_unavailable_core: preflight.request.max_unavailable_core,
            max_unavailable_edge: preflight.request.max_unavailable_edge,
            post_upgrade_stability_s: preflight.request.post_upgrade_stability_s,
            auto_rollback: preflight.request.auto_rollback,
            preview_digest: preflight.digest,
            created_by: actor.to_owned(),
            approved_by: Some(actor.to_owned()),
            rollback_of,
            error: None,
            created_at: now,
            updated_at: now,
            command_at: Some(now),
        };
        operation.nodes[0].status = OperationNodeStatus::Commanded;
        let lock_key = self.lock_key();
        let lock_value = serde_json::to_vec(&json!({
            "schema_version": 1,
            "operation_id": operation_id,
            "owner": "rcaster-admin",
            "created_at": now,
        }))?;
        let operation_value = serde_json::to_vec(&operation)?;
        let command = self.command(&operation, "apply")?;
        let command_key = self.command_key(&operation_id, &operation.nodes[0].caster_id);
        let result = self
            .client
            .txn(
                Txn::new()
                    .when([
                        Compare::create_revision(lock_key.clone(), CompareOp::Equal, 0),
                        Compare::create_revision(
                            self.operation_key(&operation_id),
                            CompareOp::Equal,
                            0,
                        ),
                    ])
                    .and_then([
                        TxnOp::put(
                            lock_key,
                            lock_value,
                            Some(PutOptions::new().with_lease(lease_id)),
                        ),
                        TxnOp::put(self.operation_key(&operation_id), operation_value, None),
                        TxnOp::put(command_key, serde_json::to_vec(&command)?, None),
                    ]),
            )
            .await?;
        if !result.succeeded() {
            let _ = self.client.lease_revoke(lease_id).await;
            return Err(UpgradeError::Conflict);
        }
        let mut client = self.client.clone();
        let (stop_tx, mut stop_rx) = oneshot::channel();
        tokio::spawn(async move {
            let Ok((mut keeper, _stream)) = client.lease_keep_alive(lease_id).await else {
                return;
            };
            loop {
                tokio::select! {
                    _ = &mut stop_rx => return,
                    _ = tokio::time::sleep(Duration::from_secs((LOCK_TTL_S / 3) as u64)) => {
                        if keeper.keep_alive().await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
        self.lock = Some(HeldLock {
            operation_id: operation_id.clone(),
            lease_id,
            stop: Some(stop_tx),
        });
        Ok(serde_json::to_value(operation)?)
    }

    async fn start_pending(
        &mut self,
        preflight: Preflight,
        rollback_of: Option<String>,
        actor: &str,
    ) -> Result<Value, UpgradeError> {
        if self.lock.is_some() {
            return Err(UpgradeError::Conflict);
        }
        let operation_id = random_hex(16);
        let now = now_timestamp();
        let operation = UpgradeOperation {
            schema_version: OPERATION_SCHEMA,
            operation_id: operation_id.clone(),
            status: OperationStatus::PendingApproval,
            signed_manifest: preflight.request.signed,
            nodes: preflight.nodes,
            current_node: 0,
            max_unavailable_core: preflight.request.max_unavailable_core,
            max_unavailable_edge: preflight.request.max_unavailable_edge,
            post_upgrade_stability_s: preflight.request.post_upgrade_stability_s,
            auto_rollback: preflight.request.auto_rollback,
            preview_digest: preflight.digest,
            created_by: actor.to_owned(),
            approved_by: None,
            rollback_of,
            error: None,
            created_at: now,
            updated_at: now,
            command_at: None,
        };
        let key = self.operation_key(&operation_id);
        let result = self
            .client
            .txn(
                Txn::new()
                    .when([Compare::create_revision(key.clone(), CompareOp::Equal, 0)])
                    .and_then([TxnOp::put(key, serde_json::to_vec(&operation)?, None)]),
            )
            .await?;
        if !result.succeeded() {
            return Err(UpgradeError::Conflict);
        }
        Ok(serde_json::to_value(operation)?)
    }

    async fn approve(&mut self, operation_id: &str, actor: &str) -> Result<Value, UpgradeError> {
        if self.lock.is_some() {
            return Err(UpgradeError::Conflict);
        }
        let mut operation = self.load_operation(operation_id).await?;
        if operation.status != OperationStatus::PendingApproval {
            return Err(UpgradeError::Conflict);
        }
        if operation.created_by == actor {
            return Err(UpgradeError::ApprovalRequired);
        }
        let body = json!({
            "manifest": &operation.signed_manifest.manifest,
            "signature": &operation.signed_manifest.signature,
            "nodes": operation.nodes.iter().map(|node| json!({
                "caster_id": &node.caster_id,
                "role": node.role,
            })).collect::<Vec<_>>(),
            "max_unavailable_core": operation.max_unavailable_core,
            "max_unavailable_edge": operation.max_unavailable_edge,
            "post_upgrade_stability_s": operation.post_upgrade_stability_s,
            "auto_rollback": operation.auto_rollback,
            "preview_digest": operation.preview_digest,
        });
        let preflight = self.preflight(&body).await?;
        if preflight.digest != operation.preview_digest {
            return Err(UpgradeError::InvalidManifest(
                "preview is stale; run preview again".to_owned(),
            ));
        }
        let old_operation = serde_json::to_vec(&operation)?;
        let lease_id = self.client.lease_grant(LOCK_TTL_S, None).await?.id();
        let now = now_timestamp();
        operation.status = OperationStatus::Running;
        operation.approved_by = Some(actor.to_owned());
        operation.updated_at = now;
        operation.command_at = Some(now);
        operation.nodes[0].status = OperationNodeStatus::Commanded;
        let lock_key = self.lock_key();
        let lock_value = serde_json::to_vec(&json!({
            "schema_version": 1,
            "operation_id": operation_id,
            "owner": "rcaster-admin",
            "created_at": now,
        }))?;
        let operation_key = self.operation_key(operation_id);
        let command = self.command(&operation, "apply")?;
        let command_key = self.command_key(operation_id, &operation.nodes[0].caster_id);
        let result = self
            .client
            .txn(
                Txn::new()
                    .when([
                        Compare::create_revision(lock_key.clone(), CompareOp::Equal, 0),
                        Compare::value(operation_key.clone(), CompareOp::Equal, old_operation),
                    ])
                    .and_then([
                        TxnOp::put(
                            lock_key,
                            lock_value,
                            Some(PutOptions::new().with_lease(lease_id)),
                        ),
                        TxnOp::put(operation_key, serde_json::to_vec(&operation)?, None),
                        TxnOp::put(command_key, serde_json::to_vec(&command)?, None),
                    ]),
            )
            .await?;
        if !result.succeeded() {
            let _ = self.client.lease_revoke(lease_id).await;
            return Err(UpgradeError::Conflict);
        }
        let mut client = self.client.clone();
        let (stop_tx, mut stop_rx) = oneshot::channel();
        tokio::spawn(async move {
            let Ok((mut keeper, _stream)) = client.lease_keep_alive(lease_id).await else {
                return;
            };
            loop {
                tokio::select! {
                    _ = &mut stop_rx => return,
                    _ = tokio::time::sleep(Duration::from_secs((LOCK_TTL_S / 3) as u64)) => {
                        if keeper.keep_alive().await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
        self.lock = Some(HeldLock {
            operation_id: operation_id.to_owned(),
            lease_id,
            stop: Some(stop_tx),
        });
        Ok(serde_json::to_value(operation)?)
    }

    async fn advance(&mut self) -> Result<(), UpgradeError> {
        if self.lock.is_none() {
            self.try_adopt_operation().await?;
        }
        let Some(lock) = self.lock.as_ref() else {
            return Ok(());
        };
        let operation_id = lock.operation_id.clone();
        let mut operation = self.load_operation(&operation_id).await?;
        if operation.status == OperationStatus::PendingApproval {
            self.release_lock(&operation_id).await;
            return Ok(());
        }
        if matches!(
            operation.status,
            OperationStatus::Succeeded | OperationStatus::Failed | OperationStatus::Canceled
        ) {
            self.release_lock(&operation_id).await;
            return Ok(());
        }
        let node = operation
            .nodes
            .get(operation.current_node)
            .cloned()
            .ok_or_else(|| {
                UpgradeError::InvalidManifest("operation has no current node".to_owned())
            })?;
        let state = self.node_state(&operation_id, &node.caster_id).await?;
        if state.is_none()
            && matches!(
                operation.status,
                OperationStatus::Running | OperationStatus::RollingBack
            )
        {
            self.ensure_command(&operation).await?;
        }
        let now = now_timestamp();
        if let Some(ref state) = state {
            let status = state["status"].as_str().unwrap_or_default();
            operation.error = state["error"].as_str().map(str::to_owned);
            match status {
                "APPLIED" => {
                    let target_version = &operation.signed_manifest.manifest.version;
                    let versions = self.node_versions().await?;
                    let target_ready = versions.iter().any(|version| {
                        version["caster_id"] == node.caster_id
                            && version["software_version"] == target_version.as_str()
                            && version["healthy"] == true
                    });
                    let stable = now.saturating_sub(state["updated_at"].as_u64().unwrap_or(now))
                        >= operation.post_upgrade_stability_s;
                    if target_ready && stable {
                        operation.nodes[operation.current_node].status =
                            OperationNodeStatus::Succeeded;
                        operation.current_node += 1;
                        operation.error = None;
                        let mut next_command = None;
                        if operation.current_node >= operation.nodes.len() {
                            operation.status = OperationStatus::Succeeded;
                            operation.command_at = None;
                        } else {
                            let next = operation.current_node;
                            operation.nodes[next].status = OperationNodeStatus::Commanded;
                            operation.command_at = Some(now);
                            let command = self.command(&operation, "apply")?;
                            next_command = Some((operation.nodes[next].caster_id.clone(), command));
                        }
                        operation.updated_at = now;
                        self.put_operation(&operation).await?;
                        if let Some((caster_id, command)) = next_command {
                            self.put_command(&operation_id, &caster_id, &command)
                                .await?;
                        }
                        if operation.status == OperationStatus::Succeeded {
                            self.release_lock(&operation_id).await;
                        }
                        return Ok(());
                    }
                }
                "FAILED" if operation.status == OperationStatus::Running => {
                    operation.nodes[operation.current_node].status = OperationNodeStatus::Failed;
                    let mut rollback_command = None;
                    if operation.auto_rollback {
                        operation.status = OperationStatus::RollingBack;
                        operation.command_at = Some(now);
                        let command = self.command(&operation, "rollback")?;
                        rollback_command = Some(command);
                    } else {
                        operation.status = OperationStatus::Failed;
                        operation.command_at = None;
                    }
                    operation.updated_at = now;
                    self.put_operation(&operation).await?;
                    if let Some(command) = rollback_command {
                        self.put_command(&operation_id, &node.caster_id, &command)
                            .await?;
                    }
                    if operation.status == OperationStatus::Failed {
                        self.release_lock(&operation_id).await;
                    }
                    return Ok(());
                }
                "FAILED" => {}
                "ROLLED_BACK" => {
                    operation.nodes[operation.current_node].status =
                        OperationNodeStatus::RolledBack;
                    operation.status = OperationStatus::Failed;
                    operation.updated_at = now;
                    operation.command_at = None;
                    self.put_operation(&operation).await?;
                    self.release_lock(&operation_id).await;
                    return Ok(());
                }
                _ => {}
            }
        }
        let timeout_deadline = state
            .as_ref()
            .and_then(|state| state["updated_at"].as_u64())
            .map(|updated_at| {
                updated_at.saturating_add(COMMAND_TIMEOUT_S).saturating_add(
                    if state.as_ref().and_then(|state| state["status"].as_str()) == Some("APPLIED")
                    {
                        operation.post_upgrade_stability_s
                    } else {
                        0
                    },
                )
            })
            .or_else(|| {
                operation
                    .command_at
                    .map(|command_at| command_at.saturating_add(COMMAND_TIMEOUT_S))
            });
        if matches!(
            operation.status,
            OperationStatus::Running | OperationStatus::RollingBack
        ) && timeout_deadline.is_some_and(|deadline| now > deadline)
        {
            operation.status = OperationStatus::Failed;
            operation.nodes[operation.current_node].status = OperationNodeStatus::Failed;
            operation.error = Some("node did not complete the upgrade command".to_owned());
            operation.updated_at = now;
            operation.command_at = None;
            self.put_operation(&operation).await?;
            self.release_lock(&operation_id).await;
        }
        Ok(())
    }

    async fn try_adopt_operation(&mut self) -> Result<(), UpgradeError> {
        let prefix = format!("/rcaster/{}/runtime/upgrades/", self.cluster_id);
        let response = self
            .client
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await?;
        for value in response.kvs() {
            let Ok(operation) = serde_json::from_slice::<UpgradeOperation>(value.value()) else {
                continue;
            };
            if matches!(
                operation.status,
                OperationStatus::PendingApproval
                    | OperationStatus::Succeeded
                    | OperationStatus::Failed
                    | OperationStatus::Canceled
            ) {
                continue;
            }
            let lease_id = self.client.lease_grant(LOCK_TTL_S, None).await?.id();
            let lock_key = self.lock_key();
            let lock_value = serde_json::to_vec(&json!({
                "schema_version": 1,
                "operation_id": &operation.operation_id,
                "owner": "rcaster-admin-recovery",
                "created_at": now_timestamp(),
            }))?;
            let result = self
                .client
                .txn(
                    Txn::new()
                        .when([
                            Compare::create_revision(lock_key.clone(), CompareOp::Equal, 0),
                            Compare::mod_revision(
                                self.operation_key(&operation.operation_id),
                                CompareOp::Equal,
                                value.mod_revision(),
                            ),
                        ])
                        .and_then([TxnOp::put(
                            lock_key,
                            lock_value,
                            Some(PutOptions::new().with_lease(lease_id)),
                        )]),
                )
                .await?;
            if !result.succeeded() {
                let _ = self.client.lease_revoke(lease_id).await;
                continue;
            }
            let mut client = self.client.clone();
            let (stop_tx, mut stop_rx) = oneshot::channel();
            tokio::spawn(async move {
                let Ok((mut keeper, _stream)) = client.lease_keep_alive(lease_id).await else {
                    return;
                };
                loop {
                    tokio::select! {
                        _ = &mut stop_rx => return,
                        _ = tokio::time::sleep(Duration::from_secs((LOCK_TTL_S / 3) as u64)) => {
                            if keeper.keep_alive().await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });
            self.lock = Some(HeldLock {
                operation_id: operation.operation_id,
                lease_id,
                stop: Some(stop_tx),
            });
            return Ok(());
        }
        Ok(())
    }

    async fn config_snapshot(&mut self) -> Result<Value, UpgradeError> {
        let key = format!("/rcaster/{}/config/caster", self.cluster_id);
        let response = self.client.get(key, None).await?;
        let Some(value) = response.kvs().first() else {
            return Err(UpgradeError::InvalidManifest(
                "Caster Config is unavailable".to_owned(),
            ));
        };
        let config: Value = serde_json::from_slice(value.value())?;
        Ok(json!({
            "generation": config["generation"],
            "revision": value.mod_revision(),
        }))
    }

    async fn node_versions(&mut self) -> Result<Vec<Value>, UpgradeError> {
        let prefix = format!("/rcaster/{}/runtime/version-status/", self.cluster_id);
        let response = self
            .client
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await?;
        response
            .kvs()
            .iter()
            .map(|value| serde_json::from_slice(value.value()).map_err(UpgradeError::Json))
            .collect()
    }

    async fn check_etcd_preflight(&mut self) -> Result<Value, UpgradeError> {
        let status = self.client.status().await?;
        if status.leader() == 0 || !status.errors().is_empty() {
            return Err(UpgradeError::EtcdPreflight(
                "etcd member is not healthy or has no leader".to_owned(),
            ));
        }
        let alarms = self
            .client
            .alarm(AlarmAction::Get, AlarmType::None, None)
            .await?;
        if !alarms.alarms().is_empty() {
            return Err(UpgradeError::EtcdPreflight(
                "etcd has active alarms".to_owned(),
            ));
        }
        let members = self.client.member_list().await?;
        if members.members().is_empty() {
            return Err(UpgradeError::EtcdPreflight(
                "etcd member list is empty".to_owned(),
            ));
        }
        let quota = if status.db_size_quota() > 0 {
            status.db_size_quota()
        } else {
            DEFAULT_ETCD_QUOTA_BYTES
        };
        if !etcd_capacity_allows(status.db_size(), quota) {
            return Err(UpgradeError::EtcdPreflight(format!(
                "etcd backend usage is too high: {} of {} bytes",
                status.db_size(),
                quota
            )));
        }
        Ok(json!({
            "member_count": members.members().len(),
            "leader": status.leader(),
            "db_size_bytes": status.db_size(),
            "db_quota_bytes": quota,
            "raft_index": status.raft_index(),
            "raft_applied_index": status.raft_applied_index(),
        }))
    }

    fn command(&self, operation: &UpgradeOperation, action: &str) -> Result<Value, UpgradeError> {
        Ok(json!({
            "schema_version": 1,
            "operation_id": operation.operation_id,
            "caster_id": operation.nodes[operation.current_node].caster_id,
            "action": action,
            "signed_manifest": operation.signed_manifest,
            "target_version": operation.signed_manifest.manifest.version,
            "post_upgrade_stability_s": operation.post_upgrade_stability_s,
            "auto_rollback": operation.auto_rollback,
            "created_at": now_timestamp(),
        }))
    }

    async fn put_command(
        &mut self,
        operation_id: &str,
        caster_id: &str,
        command: &Value,
    ) -> Result<(), UpgradeError> {
        self.client
            .put(
                self.command_key(operation_id, caster_id),
                serde_json::to_vec(command)?,
                None,
            )
            .await?;
        Ok(())
    }

    async fn ensure_command(&mut self, operation: &UpgradeOperation) -> Result<(), UpgradeError> {
        let caster_id = &operation.nodes[operation.current_node].caster_id;
        let key = self.command_key(&operation.operation_id, caster_id);
        if !self.client.get(key.clone(), None).await?.kvs().is_empty() {
            return Ok(());
        }
        let action = if operation.status == OperationStatus::RollingBack {
            "rollback"
        } else {
            "apply"
        };
        let command = self.command(operation, action)?;
        self.client
            .put(key, serde_json::to_vec(&command)?, None)
            .await?;
        Ok(())
    }

    async fn node_state(
        &mut self,
        operation_id: &str,
        caster_id: &str,
    ) -> Result<Option<Value>, UpgradeError> {
        let key = self.state_key(operation_id, caster_id);
        let response = self.client.get(key, None).await?;
        response
            .kvs()
            .first()
            .map(|value| serde_json::from_slice(value.value()).map_err(UpgradeError::Json))
            .transpose()
    }

    async fn load_operation(
        &mut self,
        operation_id: &str,
    ) -> Result<UpgradeOperation, UpgradeError> {
        let response = self
            .client
            .get(self.operation_key(operation_id), None)
            .await?;
        let Some(value) = response.kvs().first() else {
            return Err(UpgradeError::NotFound);
        };
        Ok(serde_json::from_slice(value.value())?)
    }

    async fn put_operation(&mut self, operation: &UpgradeOperation) -> Result<(), UpgradeError> {
        let key = self.operation_key(&operation.operation_id);
        let current = self.client.get(key.clone(), None).await?;
        let compare = match current.kvs().first() {
            Some(value) => {
                Compare::mod_revision(key.clone(), CompareOp::Equal, value.mod_revision())
            }
            None => Compare::create_revision(key.clone(), CompareOp::Equal, 0),
        };
        let response = self
            .client
            .txn(Txn::new().when([compare]).and_then([TxnOp::put(
                key,
                serde_json::to_vec(operation)?,
                None,
            )]))
            .await?;
        if !response.succeeded() {
            return Err(UpgradeError::Conflict);
        }
        Ok(())
    }

    async fn delete_commands(&mut self, operation_id: &str) -> Result<(), UpgradeError> {
        let prefix = format!(
            "/rcaster/{}/runtime/upgrade-commands/{operation_id}/",
            self.cluster_id
        );
        self.client
            .delete(prefix, Some(DeleteOptions::new().with_prefix()))
            .await?;
        Ok(())
    }

    async fn release_lock(&mut self, operation_id: &str) {
        let Some(mut lock) = self.lock.take() else {
            return;
        };
        if lock.operation_id != operation_id {
            self.lock = Some(lock);
            return;
        }
        if let Some(stop) = lock.stop.take() {
            let _ = stop.send(());
        }
        let _ = self.client.lease_revoke(lock.lease_id).await;
    }

    async fn clear_lock_key(&mut self, operation_id: &str) -> Result<(), UpgradeError> {
        let key = self.lock_key();
        let response = self.client.get(key.clone(), None).await?;
        let Some(value) = response.kvs().first() else {
            return Ok(());
        };
        let Ok(lock) = serde_json::from_slice::<Value>(value.value()) else {
            return Ok(());
        };
        if lock["operation_id"] != operation_id {
            return Ok(());
        }
        let response = self
            .client
            .txn(
                Txn::new()
                    .when([Compare::mod_revision(
                        key.clone(),
                        CompareOp::Equal,
                        value.mod_revision(),
                    )])
                    .and_then([TxnOp::delete(key, None)]),
            )
            .await?;
        if !response.succeeded() {
            return Err(UpgradeError::Conflict);
        }
        Ok(())
    }

    fn operation_key(&self, operation_id: &str) -> String {
        format!(
            "/rcaster/{}/runtime/upgrades/{operation_id}",
            self.cluster_id
        )
    }

    fn lock_key(&self) -> String {
        format!("/rcaster/{}/locks/upgrade/global", self.cluster_id)
    }

    fn command_key(&self, operation_id: &str, caster_id: &str) -> String {
        format!(
            "/rcaster/{}/runtime/upgrade-commands/{operation_id}/{caster_id}",
            self.cluster_id
        )
    }

    fn state_key(&self, operation_id: &str, caster_id: &str) -> String {
        format!(
            "/rcaster/{}/runtime/upgrade-states/{operation_id}/{caster_id}",
            self.cluster_id
        )
    }
}

async fn run_local_command_watch(
    mut client: Client,
    cluster_id: String,
    caster_id: String,
    health: Arc<HealthState>,
    connections: Option<Arc<ConnectionController>>,
    shutdown: watch::Sender<bool>,
    mut stop: oneshot::Receiver<()>,
) {
    let prefix = format!("/rcaster/{cluster_id}/runtime/upgrade-commands/");
    let mut triggered = std::collections::BTreeSet::new();
    let context = LocalCommandContext {
        cluster_id: &cluster_id,
        caster_id: &caster_id,
        health: &health,
        connections: connections.as_deref(),
        shutdown: &shutdown,
    };
    loop {
        let loaded = tokio::select! {
            _ = &mut stop => return,
            result = client.get(prefix.clone(), Some(GetOptions::new().with_prefix())) => result,
        };
        let Ok(loaded) = loaded else {
            if stop_or_sleep(&mut stop, Duration::from_millis(250)).await {
                return;
            }
            continue;
        };
        let start_revision = loaded
            .header()
            .map(|header| header.revision().saturating_add(1))
            .unwrap_or(1);
        for value in loaded.kvs() {
            if handle_local_command(&mut client, &context, value.value(), &mut triggered).await {
                return;
            }
        }
        let watched = tokio::select! {
            _ = &mut stop => return,
            result = client.watch(
                prefix.clone(),
                Some(
                    WatchOptions::new()
                        .with_prefix()
                        .with_start_revision(start_revision)
                        .with_progress_notify(),
                ),
            ) => result,
        };
        let Ok(mut stream) = watched else {
            if stop_or_sleep(&mut stop, Duration::from_millis(250)).await {
                return;
            }
            continue;
        };
        loop {
            let response = tokio::select! {
                _ = &mut stop => return,
                result = stream.message() => result,
            };
            let Ok(Some(response)) = response else {
                break;
            };
            if response.canceled() {
                break;
            }
            for event in response.events() {
                if event.event_type() == EventType::Delete {
                    continue;
                }
                let Some(value) = event.kv() else {
                    continue;
                };
                if handle_local_command(&mut client, &context, value.value(), &mut triggered).await
                {
                    return;
                }
            }
        }
        if stop_or_sleep(&mut stop, Duration::from_millis(100)).await {
            return;
        }
    }
}

struct LocalCommandContext<'a> {
    cluster_id: &'a str,
    caster_id: &'a str,
    health: &'a HealthState,
    connections: Option<&'a ConnectionController>,
    shutdown: &'a watch::Sender<bool>,
}

async fn handle_local_command(
    client: &mut Client,
    context: &LocalCommandContext<'_>,
    value: &[u8],
    triggered: &mut std::collections::BTreeSet<String>,
) -> bool {
    let Ok(command) = serde_json::from_slice::<Value>(value) else {
        return false;
    };
    let valid = command["schema_version"] == 1
        && command["caster_id"] == context.caster_id
        && matches!(command["action"].as_str(), Some("apply" | "rollback"));
    let Some(operation_id) = command["operation_id"].as_str() else {
        return false;
    };
    if !valid || triggered.contains(operation_id) {
        return false;
    }
    let action = command["action"].as_str().unwrap_or_default();
    if !local_command_is_active(client, context, operation_id, action).await {
        return false;
    }
    triggered.insert(operation_id.to_owned());
    context.health.set_ready(false);
    if let Some(connections) = context.connections {
        connections.cancel_all();
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let state_key = format!(
        "/rcaster/{}/runtime/upgrade-states/{operation_id}/{}",
        context.cluster_id, context.caster_id
    );
    let state = json!({
        "schema_version": 1,
        "operation_id": operation_id,
        "caster_id": context.caster_id,
        "status": "DRAINING",
        "target_version": command["target_version"],
        "updated_at": now_timestamp(),
        "error": null,
    });
    if let Ok(value) = serde_json::to_vec(&state) {
        let _ = client
            .txn(
                Txn::new()
                    .when([Compare::create_revision(
                        state_key.clone(),
                        CompareOp::Equal,
                        0,
                    )])
                    .and_then([TxnOp::put(state_key, value, None)]),
            )
            .await;
    }
    let _ = context.shutdown.send(true);
    true
}

async fn local_command_is_active(
    client: &mut Client,
    context: &LocalCommandContext<'_>,
    operation_id: &str,
    action: &str,
) -> bool {
    let operation_key = format!(
        "/rcaster/{}/runtime/upgrades/{operation_id}",
        context.cluster_id
    );
    let Ok(response) = client.get(operation_key, None).await else {
        return false;
    };
    let Some(value) = response.kvs().first() else {
        return false;
    };
    let Ok(operation) = serde_json::from_slice::<Value>(value.value()) else {
        return false;
    };
    let expected_status = if action == "apply" {
        "RUNNING"
    } else {
        "ROLLING_BACK"
    };
    let current_node = operation["current_node"].as_u64().unwrap_or(u64::MAX) as usize;
    if operation["status"] != expected_status
        || !operation["nodes"]
            .get(current_node)
            .is_some_and(|node| node["caster_id"] == context.caster_id)
    {
        return false;
    }
    let state_key = format!(
        "/rcaster/{}/runtime/upgrade-states/{operation_id}/{}",
        context.cluster_id, context.caster_id
    );
    client
        .get(state_key, None)
        .await
        .map(|response| response.kvs().is_empty())
        .unwrap_or(false)
}

async fn stop_or_sleep(stop: &mut oneshot::Receiver<()>, duration: Duration) -> bool {
    tokio::select! {
        _ = &mut *stop => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

async fn run_advance_loop(inner: Weak<Mutex<UpgradeInner>>) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        let _ = inner.lock().await.advance().await;
    }
}

fn artifact_host(url: &str) -> Result<String, UpgradeError> {
    let Some(rest) = url.strip_prefix("https://") else {
        return Err(UpgradeError::InvalidManifest(
            "artifact URL must use HTTPS".to_owned(),
        ));
    };
    if url.len() > 2048 || rest.contains('#') || rest.contains('@') {
        return Err(UpgradeError::InvalidManifest(
            "artifact URL is invalid".to_owned(),
        ));
    }
    let end = rest.find(['/', '?']).unwrap_or(rest.len());
    let host = &rest[..end];
    validate_artifact_host(host)
}

fn validate_artifact_host(host: &str) -> Result<String, UpgradeError> {
    let host = host.trim().to_ascii_lowercase();
    if host.is_empty()
        || host.len() > 253
        || !host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b':'))
    {
        return Err(UpgradeError::Policy("artifact host is invalid".to_owned()));
    }
    Ok(host)
}

fn validate_identifier(value: &str) -> Result<(), UpgradeError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(UpgradeError::InvalidManifest(
            "identifier is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn decode_hex<const N: usize>(value: &str) -> Result<[u8; N], ()> {
    if value.len() != N * 2 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(());
    }
    let mut decoded = [0_u8; N];
    for (index, output) in decoded.iter_mut().enumerate() {
        *output = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).map_err(|_| ())?;
    }
    Ok(decoded)
}

fn random_hex(bytes: usize) -> String {
    use rand::RngCore;
    let mut value = vec![0_u8; bytes];
    rand::rng().fill_bytes(&mut value);
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn now_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn default_max_unavailable() -> usize {
    1
}

fn default_stability_seconds() -> u64 {
    120
}

fn etcd_capacity_allows(db_size: i64, quota: i64) -> bool {
    quota > 0 && db_size >= 0 && db_size as f64 / (quota as f64) < ETCD_MAX_USED_RATIO
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::{Signer, SigningKey};
    use serde_json::json;
    use sha2::Digest;

    use super::{
        SignedUpgradeManifest, UpgradeError, UpgradeManifest, UpgradePolicy, verify_artifact,
    };

    fn signed_manifest() -> (SignedUpgradeManifest, String) {
        let signing_key = SigningKey::from_bytes(&[7_u8; 32]);
        let manifest = UpgradeManifest {
            schema_version: 1,
            version: "0.2.0".to_owned(),
            os: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
            config_schema: 1,
            etcd_schema: 1,
            api_schema: 1,
            artifact_url: "https://updates.example.test/rcaster".to_owned(),
            artifact_size: 4,
            artifact_sha256: format!("{:x}", sha2::Sha256::digest(b"test")),
        };
        let signature = signing_key.sign(&serde_json::to_vec(&manifest).unwrap());
        (
            SignedUpgradeManifest {
                manifest,
                signature: signature
                    .to_bytes()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect(),
            },
            signing_key
                .verifying_key()
                .to_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        )
    }

    #[test]
    fn accepts_signed_manifest_from_trusted_key() {
        let (signed, public_key) = signed_manifest();
        let policy =
            UpgradePolicy::new(&[public_key], &["updates.example.test".to_owned()]).unwrap();
        assert!(policy.verify(&signed).is_ok());
    }

    #[test]
    fn rejects_manifest_tampering_and_untrusted_hosts() {
        let (mut signed, public_key) = signed_manifest();
        let policy = UpgradePolicy::new(
            std::slice::from_ref(&public_key),
            &["updates.example.test".to_owned()],
        )
        .unwrap();
        signed.manifest.version = "9.9.9".to_owned();
        assert!(matches!(
            policy.verify(&signed),
            Err(UpgradeError::InvalidSignature)
        ));
        let (mut signed, _) = signed_manifest();
        signed.manifest.artifact_url = "https://evil.example/rcaster".to_owned();
        assert!(policy.verify(&signed).is_err());
        let other =
            UpgradePolicy::new(&["00".repeat(32)], &["updates.example.test".to_owned()]).unwrap();
        assert!(matches!(
            other.verify(&signed_manifest().0),
            Err(UpgradeError::InvalidSignature)
        ));
    }

    #[test]
    fn verifies_artifact_size_and_sha256() {
        let (signed, _) = signed_manifest();
        let path = std::env::temp_dir().join(format!("rcaster-upgrade-{}.bin", std::process::id()));
        std::fs::write(&path, b"test").unwrap();
        assert!(verify_artifact(&path, &signed.manifest).is_ok());
        std::fs::write(&path, b"fail").unwrap();
        assert!(verify_artifact(&path, &signed.manifest).is_err());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn upgrade_preflight_rejects_full_etcd_backend() {
        assert!(super::etcd_capacity_allows(1024, 2048));
        assert!(!super::etcd_capacity_allows(1844, 2048));
        assert!(!super::etcd_capacity_allows(0, 0));
    }

    #[test]
    fn request_shape_keeps_manifest_fields_top_level() {
        let (signed, _) = signed_manifest();
        let value = json!({
            "manifest": signed.manifest,
            "signature": signed.signature,
            "nodes": [{"caster_id": "edge-1", "role": "edge"}]
        });
        let request: super::UpgradeRequest = serde_json::from_value(value).unwrap();
        assert_eq!(request.nodes[0].caster_id, "edge-1");
    }
}
