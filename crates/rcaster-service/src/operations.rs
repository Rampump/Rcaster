use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    account_directory::{AccountDirectory, AccountDirectoryError},
    cluster_runtime::SharedClusterRuntime,
};

const MAX_TARGETS: usize = 500;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct TargetRevision {
    target: String,
    username: String,
    revision: u64,
    already_disabled: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct MountpointRevision {
    mountpoint: String,
    revision: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SourceRevision {
    mountpoint: String,
    source_generation: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SessionRevision {
    session_id: String,
    mountpoint: String,
    caster_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct TargetResult {
    target: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revision: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BatchOperation {
    schema_version: u8,
    operation_id: String,
    action: String,
    status: String,
    created_by: String,
    request_id: String,
    preview_digest: String,
    targets: Vec<TargetRevision>,
    affected_mountpoints: Vec<MountpointRevision>,
    affected_sources: Vec<SourceRevision>,
    affected_sessions: Vec<SessionRevision>,
    results: Vec<TargetResult>,
    created_at: u64,
    updated_at: u64,
}

#[derive(Serialize)]
struct DigestInput<'a> {
    action: &'a str,
    targets: &'a [TargetRevision],
    affected_mountpoints: &'a [MountpointRevision],
    affected_sources: &'a [SourceRevision],
    affected_sessions: &'a [SessionRevision],
}

pub struct OperationManager {
    runtime: SharedClusterRuntime,
    accounts: AccountDirectory,
    cluster_id: String,
}

impl OperationManager {
    pub fn new(
        runtime: SharedClusterRuntime,
        accounts: AccountDirectory,
        cluster_id: String,
    ) -> Self {
        Self {
            runtime,
            accounts,
            cluster_id,
        }
    }

    pub async fn preview(&self, body: &Value) -> Result<Value, (u16, String)> {
        let action = body
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| (400, "action is required".to_owned()))?;
        if action != "users.disable" {
            return Err((
                400,
                "only config.rollback and users.disable are supported".to_owned(),
            ));
        }
        let preview = self.preview_disable_users(body).await?;
        Ok(json!({
            "action": action,
            "state": "PREVIEW",
            "targets": preview.targets.len(),
            "target_revisions": preview.targets,
            "affected_mountpoints": preview.affected_mountpoints.len(),
            "affected_mountpoint_revisions": preview.affected_mountpoints,
            "affected_sources": preview.affected_sources.len(),
            "affected_source_revisions": preview.affected_sources,
            "affected_sessions": preview.affected_sessions.len(),
            "affected_session_revisions": preview.affected_sessions,
            "preview_digest": digest_preview(action, &preview),
        }))
    }

    pub async fn start(
        &self,
        body: &Value,
        actor: String,
        request_id: String,
    ) -> Result<Value, (u16, String)> {
        let action = body
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| (400, "action is required".to_owned()))?;
        if action != "users.disable" {
            return Err((400, "unsupported batch operation".to_owned()));
        }
        let expected_digest = body
            .get("preview_digest")
            .and_then(Value::as_str)
            .ok_or_else(|| (400, "preview_digest is required".to_owned()))?;
        let preview = self.preview_disable_users(body).await?;
        let current_digest = digest_preview(action, &preview);
        if current_digest != expected_digest {
            return Err((
                409,
                "previewed targets changed; preview the operation again".to_owned(),
            ));
        }

        let now = unix_time();
        let operation_id = format!("op_{}", random_hex());
        let operation = BatchOperation {
            schema_version: 1,
            operation_id: operation_id.clone(),
            action: action.to_owned(),
            status: "QUEUED".to_owned(),
            created_by: actor,
            request_id,
            preview_digest: current_digest,
            targets: preview.targets,
            affected_mountpoints: preview.affected_mountpoints,
            affected_sources: preview.affected_sources,
            affected_sessions: preview.affected_sessions,
            results: Vec::new(),
            created_at: now,
            updated_at: now,
        };
        self.put(&operation).await?;
        let manager = Self {
            runtime: self.runtime.clone(),
            accounts: self.accounts.clone(),
            cluster_id: self.cluster_id.clone(),
        };
        let worker_operation = operation.clone();
        tokio::spawn(async move {
            manager.run(worker_operation).await;
        });
        Ok(serde_json::to_value(operation).unwrap_or(Value::Null))
    }

    async fn preview_disable_users(&self, body: &Value) -> Result<Preview, (u16, String)> {
        let targets = parse_targets(body)?;
        if targets.is_empty() {
            return Err((400, "targets must not be empty".to_owned()));
        }
        if targets.len() > MAX_TARGETS {
            return Err((
                400,
                format!("targets must contain at most {MAX_TARGETS} users"),
            ));
        }
        let snapshot = self.accounts.snapshot();
        let mut target_revisions = Vec::with_capacity(targets.len());
        let mut owned_mountpoints = BTreeSet::new();
        for target in targets {
            let user = snapshot
                .user_by_id(&target)
                .ok_or_else(|| (404, format!("user {target} not found")))?;
            for mountpoint in snapshot.mountpoints() {
                if mountpoint.owner_user_id == target {
                    owned_mountpoints.insert(mountpoint.mountpoint.clone());
                }
            }
            target_revisions.push(TargetRevision {
                target,
                username: user.username.clone(),
                revision: user.mod_revision,
                already_disabled: !user.enabled,
            });
        }
        let mut mountpoint_revisions = owned_mountpoints
            .iter()
            .filter_map(|mountpoint| {
                snapshot
                    .mountpoint(mountpoint)
                    .map(|mountpoint| MountpointRevision {
                        mountpoint: mountpoint.mountpoint.clone(),
                        revision: mountpoint.mod_revision,
                    })
            })
            .collect::<Vec<_>>();
        mountpoint_revisions.sort_by(|left, right| left.mountpoint.cmp(&right.mountpoint));

        let mut source_revisions = Vec::new();
        for record in self.runtime.sourcetable_records().await {
            if owned_mountpoints.contains(&record.mountpoint)
                && let Some(route) = self.runtime.source_route(&record.mountpoint).await
            {
                source_revisions.push(SourceRevision {
                    mountpoint: record.mountpoint,
                    source_generation: route.source_generation,
                });
            }
        }
        source_revisions.sort_by(|left, right| left.mountpoint.cmp(&right.mountpoint));

        let target_set = target_revisions
            .iter()
            .map(|target| target.target.as_str())
            .collect::<BTreeSet<_>>();
        let mut session_revisions = self
            .runtime
            .sessions()
            .await
            .map_err(|error| (503, error.to_string()))?
            .into_iter()
            .filter(|session| target_set.contains(session.user_id.as_str()))
            .map(|session| SessionRevision {
                session_id: session.session_id,
                mountpoint: session.mountpoint,
                caster_id: session.caster_id,
            })
            .collect::<Vec<_>>();
        session_revisions.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        Ok(Preview {
            targets: target_revisions,
            affected_mountpoints: mountpoint_revisions,
            affected_sources: source_revisions,
            affected_sessions: session_revisions,
        })
    }

    async fn run(&self, mut operation: BatchOperation) {
        operation.status = "RUNNING".to_owned();
        operation.updated_at = unix_time();
        if self.put(&operation).await.is_err() {
            return;
        }
        for target in operation.targets.clone() {
            let current = match self.load(&operation.operation_id).await {
                Ok(current) if current.status == "CANCELED" => return,
                Ok(current) => current,
                Err(_) => return,
            };
            operation.status = current.status;
            let result = self.disable_user(&target).await;
            operation.results.push(result);
            operation.updated_at = unix_time();
            if self.put(&operation).await.is_err() {
                return;
            }
        }
        let current = match self.load(&operation.operation_id).await {
            Ok(current) => current,
            Err(_) => return,
        };
        if current.status == "CANCELED" {
            return;
        }
        let succeeded = operation
            .results
            .iter()
            .filter(|result| result.status == "succeeded" || result.status == "unchanged")
            .count();
        let failed = operation.results.len().saturating_sub(succeeded);
        operation.status = if failed == 0 {
            "SUCCEEDED"
        } else if succeeded == 0 {
            "FAILED"
        } else {
            "PARTIAL"
        }
        .to_owned();
        operation.updated_at = unix_time();
        let _ = self.put(&operation).await;
    }

    async fn disable_user(&self, target: &TargetRevision) -> TargetResult {
        let user = match self.accounts.linear_user_by_id(&target.target).await {
            Ok(Some(user)) => user,
            Ok(None) => return failed(target, "NOT_FOUND"),
            Err(error) => return failed(target, &error.to_string()),
        };
        if !user.enabled {
            return TargetResult {
                target: target.target.clone(),
                status: "unchanged".to_owned(),
                reason: Some("already_disabled".to_owned()),
                revision: Some(user.mod_revision),
            };
        }
        if user.mod_revision != target.revision {
            return failed(target, "STALE_REVISION");
        }
        let mut updated = user;
        updated.enabled = false;
        updated.mod_revision = updated.mod_revision.saturating_add(1);
        updated.updated_at = unix_time().to_string();
        match self
            .accounts
            .upsert_user(updated, Some(target.revision))
            .await
        {
            Ok(revision) => TargetResult {
                target: target.target.clone(),
                status: "succeeded".to_owned(),
                reason: None,
                revision: Some(revision),
            },
            Err(AccountDirectoryError::Conflict) => failed(target, "STALE_REVISION"),
            Err(error) => failed(target, &error.to_string()),
        }
    }

    fn operation_key(&self, operation_id: &str) -> String {
        format!(
            "/rcaster/{}/runtime/operations/{operation_id}",
            self.cluster_id
        )
    }

    async fn put(&self, operation: &BatchOperation) -> Result<(), (u16, String)> {
        self.accounts
            .etcd_put(
                &self.operation_key(&operation.operation_id),
                serde_json::to_vec(operation).map_err(|error| (500, error.to_string()))?,
                None,
            )
            .await
            .map(|_| ())
            .map_err(|error| (503, error.to_string()))
    }

    async fn load(&self, operation_id: &str) -> Result<BatchOperation, (u16, String)> {
        let (value, _) = self
            .accounts
            .etcd_get(&self.operation_key(operation_id))
            .await
            .map_err(|error| (503, error.to_string()))?
            .ok_or_else(|| (404, "operation not found".to_owned()))?;
        serde_json::from_slice(&value).map_err(|error| (503, error.to_string()))
    }
}

struct Preview {
    targets: Vec<TargetRevision>,
    affected_mountpoints: Vec<MountpointRevision>,
    affected_sources: Vec<SourceRevision>,
    affected_sessions: Vec<SessionRevision>,
}

fn parse_targets(body: &Value) -> Result<Vec<String>, (u16, String)> {
    let targets = body
        .get("targets")
        .and_then(Value::as_array)
        .ok_or_else(|| (400, "targets must be an array".to_owned()))?;
    let mut unique = BTreeSet::new();
    for target in targets {
        let target = target
            .as_str()
            .filter(|target| !target.is_empty())
            .ok_or_else(|| (400, "every target must be a non-empty string".to_owned()))?;
        unique.insert(target.to_owned());
    }
    Ok(unique.into_iter().collect())
}

fn digest_preview(action: &str, preview: &Preview) -> String {
    let input = DigestInput {
        action,
        targets: &preview.targets,
        affected_mountpoints: &preview.affected_mountpoints,
        affected_sources: &preview.affected_sources,
        affected_sessions: &preview.affected_sessions,
    };
    let bytes = serde_json::to_vec(&input).unwrap_or_default();
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn failed(target: &TargetRevision, reason: &str) -> TargetResult {
    TargetResult {
        target: target.target.clone(),
        status: "failed".to_owned(),
        reason: Some(reason.to_owned()),
        revision: None,
    }
}

fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn random_hex() -> String {
    rand::random::<[u8; 16]>()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
