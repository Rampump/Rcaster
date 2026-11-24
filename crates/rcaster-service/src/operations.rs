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
const OPERATION_LEASE_TTL_S: i64 = 30;

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
    runtime_epoch: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct OwnerTarget {
    mountpoint: String,
    revision: u64,
    owner_user_id: String,
    owner_revision: u64,
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
    #[serde(default)]
    owner_caster_id: String,
    created_by: String,
    request_id: String,
    preview_digest: String,
    targets: Vec<TargetRevision>,
    affected_mountpoints: Vec<MountpointRevision>,
    affected_sources: Vec<SourceRevision>,
    affected_sessions: Vec<SessionRevision>,
    owner_targets: Vec<OwnerTarget>,
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
    owner_targets: &'a [OwnerTarget],
}

pub struct OperationManager {
    runtime: SharedClusterRuntime,
    accounts: AccountDirectory,
    cluster_id: String,
    caster_id: String,
}

impl OperationManager {
    pub fn new(
        runtime: SharedClusterRuntime,
        accounts: AccountDirectory,
        cluster_id: String,
        caster_id: String,
    ) -> Self {
        Self {
            runtime,
            accounts,
            cluster_id,
            caster_id,
        }
    }

    pub async fn preview(&self, body: &Value) -> Result<Value, (u16, String)> {
        let action = body
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| (400, "action is required".to_owned()))?;
        if !matches!(
            action,
            "users.disable"
                | "users.delete"
                | "mountpoints.disable"
                | "mountpoints.change_owner"
                | "sessions.drop"
        ) {
            return Err((400, "unsupported batch operation".to_owned()));
        }
        let preview = match action {
            "users.disable" | "users.delete" => self.preview_disable_users(body).await?,
            "mountpoints.disable" => self.preview_disable_mountpoints(body).await?,
            "mountpoints.change_owner" => self.preview_change_owners(body).await?,
            "sessions.drop" => self.preview_drop_sessions(body).await?,
            _ => unreachable!(),
        };
        Ok(json!({
            "action": action,
            "state": "PREVIEW",
            "targets": preview.targets.len() + preview.owner_targets.len(),
            "target_revisions": preview.targets,
            "owner_targets": preview.owner_targets,
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
        if !matches!(
            action,
            "users.disable"
                | "users.delete"
                | "mountpoints.disable"
                | "mountpoints.change_owner"
                | "sessions.drop"
        ) {
            return Err((400, "unsupported batch operation".to_owned()));
        }
        let expected_digest = body
            .get("preview_digest")
            .and_then(Value::as_str)
            .ok_or_else(|| (400, "preview_digest is required".to_owned()))?;
        let preview = match action {
            "users.disable" | "users.delete" => self.preview_disable_users(body).await?,
            "mountpoints.disable" => self.preview_disable_mountpoints(body).await?,
            "mountpoints.change_owner" => self.preview_change_owners(body).await?,
            "sessions.drop" => self.preview_drop_sessions(body).await?,
            _ => unreachable!(),
        };
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
            owner_caster_id: self.caster_id.clone(),
            created_by: actor,
            request_id,
            preview_digest: current_digest,
            targets: preview.targets,
            affected_mountpoints: preview.affected_mountpoints,
            affected_sources: preview.affected_sources,
            affected_sessions: preview.affected_sessions,
            owner_targets: preview.owner_targets,
            results: Vec::new(),
            created_at: now,
            updated_at: now,
        };
        let lease_id = self.acquire_operation_lease(&operation).await?;
        self.put(&operation).await?;
        self.put_lease_key(&operation.operation_id, &self.caster_id, lease_id)
            .await?;
        self.spawn_run(operation.clone(), lease_id);
        Ok(serde_json::to_value(operation).unwrap_or(Value::Null))
    }

    pub async fn retry(&self, operation_id: &str) -> Result<Value, (u16, String)> {
        let mut operation = self.load(operation_id).await?;
        if !matches!(operation.status.as_str(), "PARTIAL" | "FAILED") {
            return Err((
                409,
                "only partially failed or failed operations can be retried".to_owned(),
            ));
        }
        operation.status = "QUEUED".to_owned();
        operation.owner_caster_id = self.caster_id.clone();
        operation.results.clear();
        operation.updated_at = unix_time();
        let lease_id = self.acquire_operation_lease(&operation).await?;
        self.put(&operation).await?;
        self.put_lease_key(&operation.operation_id, &self.caster_id, lease_id)
            .await?;
        self.spawn_run(operation.clone(), lease_id);
        Ok(serde_json::to_value(operation).unwrap_or(Value::Null))
    }

    pub async fn recover_stale_operations(&self) -> Result<usize, (u16, String)> {
        let prefix = format!("/rcaster/{}/runtime/operations/", self.cluster_id);
        let records = self
            .accounts
            .etcd_list(&prefix)
            .await
            .map_err(|error| (503, error.to_string()))?;
        let now = unix_time();
        let mut recovered = 0;
        for (_, value, revision) in records {
            let Ok(mut operation) = serde_json::from_slice::<BatchOperation>(&value) else {
                continue;
            };
            if !matches!(operation.status.as_str(), "QUEUED" | "RUNNING")
                || now.saturating_sub(operation.updated_at) < 5
                || self
                    .accounts
                    .etcd_get(&self.lease_key(&operation.operation_id))
                    .await
                    .map_err(|error| (503, error.to_string()))?
                    .is_some()
            {
                continue;
            }
            let lease_id = self.acquire_operation_lease(&operation).await?;
            operation.owner_caster_id = self.caster_id.clone();
            operation.status = "QUEUED".to_owned();
            operation.updated_at = now;
            let replaced = self
                .accounts
                .etcd_put_if_revision(
                    &self.operation_key(&operation.operation_id),
                    serde_json::to_vec(&operation).map_err(|error| (500, error.to_string()))?,
                    revision,
                )
                .await
                .map_err(|error| (503, error.to_string()))?;
            if !replaced {
                let _ = self.accounts.etcd_revoke_lease(lease_id).await;
                continue;
            }
            if let Err(error) = self
                .put_lease_key(&operation.operation_id, &self.caster_id, lease_id)
                .await
            {
                let _ = self.accounts.etcd_revoke_lease(lease_id).await;
                return Err(error);
            }
            self.spawn_run(operation, lease_id);
            recovered += 1;
        }
        Ok(recovered)
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
                runtime_epoch: session.runtime_epoch,
            })
            .collect::<Vec<_>>();
        session_revisions.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        Ok(Preview {
            targets: target_revisions,
            affected_mountpoints: mountpoint_revisions,
            affected_sources: source_revisions,
            affected_sessions: session_revisions,
            owner_targets: Vec::new(),
        })
    }

    async fn preview_drop_sessions(&self, body: &Value) -> Result<Preview, (u16, String)> {
        let targets = parse_targets(body)?;
        if targets.is_empty() {
            return Err((400, "targets must not be empty".to_owned()));
        }
        if targets.len() > MAX_TARGETS {
            return Err((
                400,
                format!("targets must contain at most {MAX_TARGETS} sessions"),
            ));
        }
        let target_set = targets.iter().map(String::as_str).collect::<BTreeSet<_>>();
        let sessions = self
            .runtime
            .sessions()
            .await
            .map_err(|error| (503, error.to_string()))?;
        let mut revisions = sessions
            .into_iter()
            .filter(|session| target_set.contains(session.session_id.as_str()))
            .map(|session| SessionRevision {
                session_id: session.session_id,
                mountpoint: session.mountpoint,
                caster_id: session.caster_id,
                runtime_epoch: session.runtime_epoch,
            })
            .collect::<Vec<_>>();
        revisions.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        if revisions.len() != target_set.len() {
            return Err((404, "one or more sessions were not found".to_owned()));
        }
        Ok(Preview {
            targets: Vec::new(),
            affected_mountpoints: Vec::new(),
            affected_sources: Vec::new(),
            affected_sessions: revisions,
            owner_targets: Vec::new(),
        })
    }

    async fn preview_disable_mountpoints(&self, body: &Value) -> Result<Preview, (u16, String)> {
        let targets = parse_targets(body)?;
        if targets.is_empty() {
            return Err((400, "targets must not be empty".to_owned()));
        }
        if targets.len() > MAX_TARGETS {
            return Err((
                400,
                format!("targets must contain at most {MAX_TARGETS} mountpoints"),
            ));
        }
        let snapshot = self.accounts.snapshot();
        let mut mountpoint_revisions = Vec::with_capacity(targets.len());
        for target in targets {
            let mountpoint = snapshot
                .mountpoint(&target)
                .ok_or_else(|| (404, format!("mountpoint {target} not found")))?;
            mountpoint_revisions.push(MountpointRevision {
                mountpoint: target,
                revision: mountpoint.mod_revision,
            });
        }
        mountpoint_revisions.sort_by(|left, right| left.mountpoint.cmp(&right.mountpoint));
        let target_set = mountpoint_revisions
            .iter()
            .map(|mountpoint| mountpoint.mountpoint.as_str())
            .collect::<BTreeSet<_>>();
        let mut source_revisions = Vec::new();
        for record in self.runtime.sourcetable_records().await {
            if target_set.contains(record.mountpoint.as_str())
                && let Some(route) = self.runtime.source_route(&record.mountpoint).await
            {
                source_revisions.push(SourceRevision {
                    mountpoint: record.mountpoint,
                    source_generation: route.source_generation,
                });
            }
        }
        source_revisions.sort_by(|left, right| left.mountpoint.cmp(&right.mountpoint));
        let mut session_revisions = self
            .runtime
            .sessions()
            .await
            .map_err(|error| (503, error.to_string()))?
            .into_iter()
            .filter(|session| target_set.contains(session.mountpoint.as_str()))
            .map(|session| SessionRevision {
                session_id: session.session_id,
                mountpoint: session.mountpoint,
                caster_id: session.caster_id,
                runtime_epoch: session.runtime_epoch,
            })
            .collect::<Vec<_>>();
        session_revisions.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        Ok(Preview {
            targets: Vec::new(),
            affected_mountpoints: mountpoint_revisions,
            affected_sources: source_revisions,
            affected_sessions: session_revisions,
            owner_targets: Vec::new(),
        })
    }

    async fn preview_change_owners(&self, body: &Value) -> Result<Preview, (u16, String)> {
        let targets = parse_owner_targets(body)?;
        if targets.is_empty() {
            return Err((400, "targets must not be empty".to_owned()));
        }
        if targets.len() > MAX_TARGETS {
            return Err((
                400,
                format!("targets must contain at most {MAX_TARGETS} mountpoints"),
            ));
        }
        let snapshot = self.accounts.snapshot();
        let mut owner_targets = Vec::with_capacity(targets.len());
        let mut mountpoint_revisions = Vec::with_capacity(targets.len());
        let mut target_set = BTreeSet::new();
        for (mountpoint_name, owner_user_id) in targets {
            let mountpoint = snapshot
                .mountpoint(&mountpoint_name)
                .ok_or_else(|| (404, format!("mountpoint {mountpoint_name} not found")))?;
            let owner = snapshot
                .user_by_id(&owner_user_id)
                .filter(|owner| owner.enabled)
                .ok_or_else(|| (400, format!("owner {owner_user_id} is not enabled")))?;
            if self.runtime.source_route(&mountpoint_name).await.is_some() {
                return Err((
                    409,
                    format!("mountpoint {mountpoint_name} has an active source"),
                ));
            }
            target_set.insert(mountpoint_name.clone());
            mountpoint_revisions.push(MountpointRevision {
                mountpoint: mountpoint_name.clone(),
                revision: mountpoint.mod_revision,
            });
            owner_targets.push(OwnerTarget {
                mountpoint: mountpoint_name,
                revision: mountpoint.mod_revision,
                owner_user_id,
                owner_revision: owner.mod_revision,
            });
        }
        mountpoint_revisions.sort_by(|left, right| left.mountpoint.cmp(&right.mountpoint));
        owner_targets.sort_by(|left, right| left.mountpoint.cmp(&right.mountpoint));
        let mut session_revisions = self
            .runtime
            .sessions()
            .await
            .map_err(|error| (503, error.to_string()))?
            .into_iter()
            .filter(|session| target_set.contains(&session.mountpoint))
            .map(|session| SessionRevision {
                session_id: session.session_id,
                mountpoint: session.mountpoint,
                caster_id: session.caster_id,
                runtime_epoch: session.runtime_epoch,
            })
            .collect::<Vec<_>>();
        session_revisions.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        Ok(Preview {
            targets: Vec::new(),
            affected_mountpoints: mountpoint_revisions,
            affected_sources: Vec::new(),
            affected_sessions: session_revisions,
            owner_targets,
        })
    }

    async fn run(&self, mut operation: BatchOperation) {
        let current = match self.load(&operation.operation_id).await {
            Ok(current) if current.owner_caster_id == self.caster_id => current,
            _ => return,
        };
        operation = current;
        operation.status = "RUNNING".to_owned();
        operation.updated_at = unix_time();
        if self.put(&operation).await.is_err() {
            return;
        }
        match operation.action.as_str() {
            "users.disable" | "users.delete" => {
                for target in operation.targets.clone() {
                    let current = match self.load(&operation.operation_id).await {
                        Ok(current)
                            if current.status == "CANCELED"
                                || current.owner_caster_id != self.caster_id =>
                        {
                            return;
                        }
                        Ok(current) => current,
                        Err(_) => return,
                    };
                    operation.status = current.status;
                    let result = if operation.action == "users.delete" {
                        self.delete_user(&target).await
                    } else {
                        self.disable_user(&target).await
                    };
                    operation.results.push(result);
                    operation.updated_at = unix_time();
                    if self.put(&operation).await.is_err() {
                        return;
                    }
                }
            }
            "mountpoints.disable" => {
                for target in operation.affected_mountpoints.clone() {
                    let current = match self.load(&operation.operation_id).await {
                        Ok(current)
                            if current.status == "CANCELED"
                                || current.owner_caster_id != self.caster_id =>
                        {
                            return;
                        }
                        Ok(current) => current,
                        Err(_) => return,
                    };
                    operation.status = current.status;
                    let result = self.disable_mountpoint(&target).await;
                    operation.results.push(result);
                    operation.updated_at = unix_time();
                    if self.put(&operation).await.is_err() {
                        return;
                    }
                }
            }
            "mountpoints.change_owner" => {
                for target in operation.owner_targets.clone() {
                    let current = match self.load(&operation.operation_id).await {
                        Ok(current)
                            if current.status == "CANCELED"
                                || current.owner_caster_id != self.caster_id =>
                        {
                            return;
                        }
                        Ok(current) => current,
                        Err(_) => return,
                    };
                    operation.status = current.status;
                    let result = self.change_mountpoint_owner(&target).await;
                    operation.results.push(result);
                    operation.updated_at = unix_time();
                    if self.put(&operation).await.is_err() {
                        return;
                    }
                }
            }
            "sessions.drop" => {
                for target in operation.affected_sessions.clone() {
                    let current = match self.load(&operation.operation_id).await {
                        Ok(current)
                            if current.status == "CANCELED"
                                || current.owner_caster_id != self.caster_id =>
                        {
                            return;
                        }
                        Ok(current) => current,
                        Err(_) => return,
                    };
                    operation.status = current.status;
                    let result = self.drop_session(&target).await;
                    operation.results.push(result);
                    operation.updated_at = unix_time();
                    if self.put(&operation).await.is_err() {
                        return;
                    }
                }
            }
            _ => return,
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

    fn spawn_run(&self, operation: BatchOperation, lease_id: i64) {
        let manager = Self {
            runtime: self.runtime.clone(),
            accounts: self.accounts.clone(),
            cluster_id: self.cluster_id.clone(),
            caster_id: self.caster_id.clone(),
        };
        let keepalive_manager = Self {
            runtime: self.runtime.clone(),
            accounts: self.accounts.clone(),
            cluster_id: self.cluster_id.clone(),
            caster_id: self.caster_id.clone(),
        };
        let operation_id = operation.operation_id.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(
                    (OPERATION_LEASE_TTL_S / 3) as u64,
                ))
                .await;
                let Ok(operation) = keepalive_manager.load(&operation_id).await else {
                    return;
                };
                if operation.owner_caster_id != keepalive_manager.caster_id
                    || matches!(
                        operation.status.as_str(),
                        "SUCCEEDED" | "PARTIAL" | "FAILED" | "CANCELED"
                    )
                {
                    return;
                }
                if keepalive_manager
                    .accounts
                    .etcd_keep_alive_once(lease_id)
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        tokio::spawn(async move {
            manager.run(operation).await;
        });
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

    async fn delete_user(&self, target: &TargetRevision) -> TargetResult {
        let snapshot = self.accounts.snapshot();
        let Some(user) = snapshot.user_by_id(&target.target) else {
            return TargetResult {
                target: target.target.clone(),
                status: "unchanged".to_owned(),
                reason: Some("already_deleted".to_owned()),
                revision: None,
            };
        };
        if user.mod_revision != target.revision {
            return failed(target, "STALE_REVISION");
        }
        match self
            .accounts
            .delete_user(&target.target, target.revision)
            .await
        {
            Ok(()) => TargetResult {
                target: target.target.clone(),
                status: "succeeded".to_owned(),
                reason: None,
                revision: None,
            },
            Err(AccountDirectoryError::InUse) => failed(target, "ACCOUNT_IN_USE"),
            Err(AccountDirectoryError::Conflict) => failed(target, "STALE_REVISION"),
            Err(error) => failed(target, &error.to_string()),
        }
    }

    async fn disable_mountpoint(&self, target: &MountpointRevision) -> TargetResult {
        let Some(mountpoint) = self
            .accounts
            .snapshot()
            .mountpoint(&target.mountpoint)
            .cloned()
        else {
            return failed_mountpoint(target, "NOT_FOUND");
        };
        if !mountpoint.enabled {
            return TargetResult {
                target: target.mountpoint.clone(),
                status: "unchanged".to_owned(),
                reason: Some("already_disabled".to_owned()),
                revision: Some(mountpoint.mod_revision),
            };
        }
        if mountpoint.mod_revision != target.revision {
            return failed_mountpoint(target, "STALE_REVISION");
        }
        let active_route = self.runtime.source_route(&target.mountpoint).await;
        let mut updated = mountpoint;
        updated.enabled = false;
        updated.updated_at = unix_time().to_string();
        let revision = match self
            .runtime
            .upsert_mountpoint(updated, Some(target.revision))
            .await
        {
            Ok(revision) => revision,
            Err(AccountDirectoryError::Conflict) => {
                return failed_mountpoint(target, "STALE_REVISION");
            }
            Err(error) => return failed_mountpoint(target, &error.to_string()),
        };
        if let Some(route) = active_route
            && let Err(error) = self.runtime.request_source_disconnect(&route).await
        {
            return TargetResult {
                target: target.mountpoint.clone(),
                status: "failed".to_owned(),
                reason: Some(format!("SOURCE_DISCONNECT_FAILED: {error}")),
                revision: Some(revision),
            };
        }
        TargetResult {
            target: target.mountpoint.clone(),
            status: "succeeded".to_owned(),
            reason: None,
            revision: Some(revision),
        }
    }

    async fn change_mountpoint_owner(&self, target: &OwnerTarget) -> TargetResult {
        let snapshot = self.accounts.snapshot();
        let Some(mountpoint) = snapshot.mountpoint(&target.mountpoint).cloned() else {
            return failed_owner(target, "NOT_FOUND");
        };
        let Some(owner) = snapshot.user_by_id(&target.owner_user_id) else {
            return failed_owner(target, "OWNER_NOT_FOUND");
        };
        if !owner.enabled {
            return failed_owner(target, "OWNER_DISABLED");
        }
        if mountpoint.owner_user_id == target.owner_user_id
            && owner.mod_revision == target.owner_revision
        {
            return TargetResult {
                target: target.mountpoint.clone(),
                status: "unchanged".to_owned(),
                reason: Some("already_owned".to_owned()),
                revision: Some(mountpoint.mod_revision),
            };
        }
        if mountpoint.mod_revision != target.revision || owner.mod_revision != target.owner_revision
        {
            return failed_owner(target, "STALE_REVISION");
        }
        if self
            .runtime
            .source_route(&target.mountpoint)
            .await
            .is_some()
        {
            return failed_owner(target, "ACTIVE_SOURCE");
        }
        let mut updated = mountpoint;
        updated.owner_user_id = target.owner_user_id.clone();
        updated.updated_at = unix_time().to_string();
        match self
            .runtime
            .upsert_mountpoint(updated, Some(target.revision))
            .await
        {
            Ok(revision) => TargetResult {
                target: target.mountpoint.clone(),
                status: "succeeded".to_owned(),
                reason: None,
                revision: Some(revision),
            },
            Err(AccountDirectoryError::Conflict) => failed_owner(target, "STALE_REVISION"),
            Err(error) => failed_owner(target, &error.to_string()),
        }
    }

    async fn drop_session(&self, target: &SessionRevision) -> TargetResult {
        let sessions = match self.runtime.sessions().await {
            Ok(sessions) => sessions,
            Err(error) => return failed_session(target, &error.to_string()),
        };
        let Some(session) = sessions
            .into_iter()
            .find(|session| session.session_id == target.session_id)
        else {
            return TargetResult {
                target: target.session_id.clone(),
                status: "unchanged".to_owned(),
                reason: Some("already_disconnected".to_owned()),
                revision: None,
            };
        };
        if session.caster_id != target.caster_id || session.runtime_epoch != target.runtime_epoch {
            return failed_session(target, "STALE_SESSION");
        }
        match self.runtime.request_session_disconnect(&session).await {
            Ok(()) => TargetResult {
                target: target.session_id.clone(),
                status: "succeeded".to_owned(),
                reason: None,
                revision: None,
            },
            Err(error) => failed_session(target, &error.to_string()),
        }
    }

    fn operation_key(&self, operation_id: &str) -> String {
        format!(
            "/rcaster/{}/runtime/operations/{operation_id}",
            self.cluster_id
        )
    }

    fn lease_key(&self, operation_id: &str) -> String {
        format!(
            "/rcaster/{}/runtime/operation-leases/{operation_id}",
            self.cluster_id
        )
    }

    async fn acquire_operation_lease(
        &self,
        operation: &BatchOperation,
    ) -> Result<i64, (u16, String)> {
        if operation.owner_caster_id.is_empty() {
            return Err((500, "operation owner is missing".to_owned()));
        }
        self.accounts
            .etcd_lease(OPERATION_LEASE_TTL_S)
            .await
            .map_err(|error| (503, error.to_string()))
    }

    async fn put_lease_key(
        &self,
        operation_id: &str,
        owner_caster_id: &str,
        lease_id: i64,
    ) -> Result<(), (u16, String)> {
        let value = serde_json::to_vec(&json!({
            "schema_version": 1,
            "operation_id": operation_id,
            "owner_caster_id": owner_caster_id,
            "created_at": unix_time(),
        }))
        .map_err(|error| (500, error.to_string()))?;
        self.accounts
            .etcd_put(&self.lease_key(operation_id), value, Some(lease_id))
            .await
            .map(|_| ())
            .map_err(|error| (503, error.to_string()))
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
    owner_targets: Vec<OwnerTarget>,
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

fn parse_owner_targets(body: &Value) -> Result<Vec<(String, String)>, (u16, String)> {
    let targets = body
        .get("targets")
        .and_then(Value::as_array)
        .ok_or_else(|| (400, "targets must be an array".to_owned()))?;
    let mut unique = BTreeSet::new();
    for target in targets {
        let mountpoint = target
            .get("mountpoint")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| (400, "every target needs a mountpoint".to_owned()))?;
        let owner_user_id = target
            .get("owner_user_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| (400, "every target needs an owner_user_id".to_owned()))?;
        unique.insert((mountpoint.to_owned(), owner_user_id.to_owned()));
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
        owner_targets: &preview.owner_targets,
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

fn failed_session(target: &SessionRevision, reason: &str) -> TargetResult {
    TargetResult {
        target: target.session_id.clone(),
        status: "failed".to_owned(),
        reason: Some(reason.to_owned()),
        revision: None,
    }
}

fn failed_mountpoint(target: &MountpointRevision, reason: &str) -> TargetResult {
    TargetResult {
        target: target.mountpoint.clone(),
        status: "failed".to_owned(),
        reason: Some(reason.to_owned()),
        revision: None,
    }
}

fn failed_owner(target: &OwnerTarget, reason: &str) -> TargetResult {
    TargetResult {
        target: target.mountpoint.clone(),
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
