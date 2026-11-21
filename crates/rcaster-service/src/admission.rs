use std::fmt;

use etcd_client::{Client, Compare, CompareOp, DeleteOptions, GetOptions, PutOptions, Txn, TxnOp};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::config::{ConfigError, ConfigSnapshot, load_config_snapshot};
use crate::source_claim::{
    ClaimError, ClaimedSource, SourceClaimRequest, claim_source_with_guards,
};

#[derive(Debug)]
pub enum AdmissionError {
    UserLockBusy,
    IpLockBusy,
    UserQuotaExceeded,
    IpQuotaExceeded,
    ConfigStale,
    AdmissionConflict,
    StaleLock,
    InvalidIdentifier(&'static str),
    CorruptState,
    SessionIdentityMismatch,
    MissingClientIp,
    Source(ClaimError),
    Config(ConfigError),
    Etcd(etcd_client::Error),
    Json(serde_json::Error),
}

impl fmt::Display for AdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UserLockBusy => formatter.write_str("user admission lock is busy"),
            Self::IpLockBusy => formatter.write_str("IP admission lock is busy"),
            Self::UserQuotaExceeded => formatter.write_str("user session quota exceeded"),
            Self::IpQuotaExceeded => formatter.write_str("IP session quota exceeded"),
            Self::ConfigStale => formatter.write_str("caster config snapshot is stale"),
            Self::AdmissionConflict => formatter.write_str("admission record changed concurrently"),
            Self::StaleLock => formatter.write_str("admission lock is stale"),
            Self::InvalidIdentifier(name) => write!(formatter, "invalid {name}"),
            Self::CorruptState => formatter.write_str("corrupt admission state"),
            Self::SessionIdentityMismatch => formatter.write_str("session identity mismatch"),
            Self::MissingClientIp => formatter.write_str("client session has no IP"),
            Self::Source(error) => write!(formatter, "source admission failed: {error}"),
            Self::Config(error) => write!(formatter, "config error: {error}"),
            Self::Etcd(error) => write!(formatter, "etcd error: {error}"),
            Self::Json(error) => write!(formatter, "json error: {error}"),
        }
    }
}

impl std::error::Error for AdmissionError {}

impl From<etcd_client::Error> for AdmissionError {
    fn from(error: etcd_client::Error) -> Self {
        Self::Etcd(error)
    }
}

impl From<ConfigError> for AdmissionError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<serde_json::Error> for AdmissionError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[derive(Debug)]
struct ResourceLock {
    key: String,
    token: String,
    lease_id: i64,
    revision: i64,
}

impl ResourceLock {
    fn compares(&self) -> [Compare; 3] {
        [
            Compare::mod_revision(self.key.clone(), CompareOp::Equal, self.revision),
            Compare::value(self.key.clone(), CompareOp::Equal, self.token.clone()),
            Compare::lease(self.key.clone(), CompareOp::Equal, self.lease_id),
        ]
    }
}

#[derive(Debug)]
pub struct UserLock {
    lock: ResourceLock,
    user_id: String,
}

#[derive(Debug)]
struct IpLock {
    lock: ResourceLock,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionType {
    Client,
    Source,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct SessionRecord {
    pub schema_version: u8,
    pub session_id: String,
    pub session_type: SessionType,
    pub user_id: String,
    pub mountpoint: String,
    pub caster_id: String,
    pub runtime_epoch: String,
    pub client_ip: Option<String>,
    pub started_at: String,
}

impl SessionRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn client(
        session_id: impl Into<String>,
        user_id: impl Into<String>,
        mountpoint: impl Into<String>,
        caster_id: impl Into<String>,
        runtime_epoch: impl Into<String>,
        client_ip: impl Into<String>,
        started_at: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: 1,
            session_id: session_id.into(),
            session_type: SessionType::Client,
            user_id: user_id.into(),
            mountpoint: mountpoint.into(),
            caster_id: caster_id.into(),
            runtime_epoch: runtime_epoch.into(),
            client_ip: Some(client_ip.into()),
            started_at: started_at.into(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn source(
        session_id: impl Into<String>,
        user_id: impl Into<String>,
        mountpoint: impl Into<String>,
        caster_id: impl Into<String>,
        runtime_epoch: impl Into<String>,
        started_at: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: 1,
            session_id: session_id.into(),
            session_type: SessionType::Source,
            user_id: user_id.into(),
            mountpoint: mountpoint.into(),
            caster_id: caster_id.into(),
            runtime_epoch: runtime_epoch.into(),
            client_ip: None,
            started_at: started_at.into(),
        }
    }
}

#[derive(Debug)]
pub struct ClientAdmissionRequest {
    pub(crate) session: SessionRecord,
    pub(crate) max_sessions: usize,
    pub(crate) ip_max_sessions: Option<usize>,
    pub(crate) config: ConfigSnapshot,
}

impl ClientAdmissionRequest {
    pub fn new(
        session: SessionRecord,
        max_sessions: usize,
        ip_max_sessions: Option<usize>,
        config: ConfigSnapshot,
    ) -> Self {
        Self {
            session,
            max_sessions,
            ip_max_sessions,
            config,
        }
    }
}

#[derive(Debug)]
pub struct SourceAdmissionRequest {
    pub(crate) session: SessionRecord,
    pub(crate) max_sessions: usize,
    pub(crate) source_claim: SourceClaimRequest,
    pub(crate) config: ConfigSnapshot,
}

impl SourceAdmissionRequest {
    pub fn new(
        session: SessionRecord,
        max_sessions: usize,
        source_claim: SourceClaimRequest,
        config: ConfigSnapshot,
    ) -> Self {
        Self {
            session,
            max_sessions,
            source_claim,
            config,
        }
    }
}

#[derive(Debug)]
pub struct ClientPermit {
    pub session: SessionRecord,
}

#[derive(Debug)]
pub struct SourcePermit {
    pub session: SessionRecord,
    pub claimed_source: ClaimedSource,
}

pub async fn try_acquire_user_lock(
    client: &mut Client,
    lease_id: i64,
    cluster_id: &str,
    user_id: &str,
) -> Result<UserLock, AdmissionError> {
    validate_key_segment(cluster_id, "cluster ID")?;
    validate_key_segment(user_id, "user ID")?;
    let key = format!("/rcaster/{cluster_id}/locks/users/{user_id}");
    let lock = try_acquire_lock(client, lease_id, key).await?;
    Ok(UserLock {
        lock,
        user_id: user_id.to_owned(),
    })
}

async fn try_acquire_ip_lock(
    client: &mut Client,
    lease_id: i64,
    cluster_id: &str,
    client_ip: &str,
) -> Result<IpLock, AdmissionError> {
    validate_key_segment(cluster_id, "cluster ID")?;
    validate_key_segment(client_ip, "client IP")?;
    let key = format!("/rcaster/{cluster_id}/locks/ips/{client_ip}");
    let lock = try_acquire_lock(client, lease_id, key).await?;
    Ok(IpLock { lock })
}

async fn try_acquire_lock(
    client: &mut Client,
    lease_id: i64,
    key: String,
) -> Result<ResourceLock, AdmissionError> {
    let token = owner_token();
    let transaction = Txn::new()
        .when([Compare::create_revision(key.clone(), CompareOp::Equal, 0)])
        .and_then([TxnOp::put(
            key.clone(),
            token.clone(),
            Some(PutOptions::new().with_lease(lease_id)),
        )]);
    let response = client.txn(transaction).await?;

    if !response.succeeded() {
        return Err(if key.contains("/locks/ips/") {
            AdmissionError::IpLockBusy
        } else {
            AdmissionError::UserLockBusy
        });
    }

    Ok(ResourceLock {
        key,
        token,
        lease_id,
        revision: response
            .header()
            .ok_or(AdmissionError::CorruptState)?
            .revision(),
    })
}

pub async fn release_user_lock(client: &mut Client, lock: &UserLock) -> Result<(), AdmissionError> {
    release_lock(client, &lock.lock).await
}

async fn release_lock(client: &mut Client, lock: &ResourceLock) -> Result<(), AdmissionError> {
    let transaction = Txn::new()
        .when(lock.compares())
        .and_then([TxnOp::delete(lock.key.clone(), Some(DeleteOptions::new()))]);

    if client.txn(transaction).await?.succeeded() {
        Ok(())
    } else {
        Err(AdmissionError::StaleLock)
    }
}

pub async fn read_user_session_count(
    client: &mut Client,
    cluster_id: &str,
    user_id: &str,
) -> Result<usize, AdmissionError> {
    validate_key_segment(cluster_id, "cluster ID")?;
    validate_key_segment(user_id, "user ID")?;
    let prefix = format!("/rcaster/{cluster_id}/online/users/{user_id}/sessions/");
    Ok(client
        .get(
            prefix,
            Some(GetOptions::new().with_prefix().with_count_only()),
        )
        .await?
        .count() as usize)
}

pub async fn read_ip_session_count(
    client: &mut Client,
    cluster_id: &str,
    client_ip: &str,
) -> Result<usize, AdmissionError> {
    validate_key_segment(cluster_id, "cluster ID")?;
    validate_key_segment(client_ip, "client IP")?;
    let prefix = format!("/rcaster/{cluster_id}/online/ips/{client_ip}/client-sessions/");
    Ok(client
        .get(
            prefix,
            Some(GetOptions::new().with_prefix().with_count_only()),
        )
        .await?
        .count() as usize)
}

pub async fn admit_client(
    client: &mut Client,
    lease_id: i64,
    cluster_id: &str,
    user_lock: UserLock,
    request: ClientAdmissionRequest,
) -> Result<ClientPermit, AdmissionError> {
    validate_key_segment(cluster_id, "cluster ID")?;
    validate_session(&request.session)?;
    if request.session.user_id != user_lock.user_id {
        let _ = release_user_lock(client, &user_lock).await;
        return Err(AdmissionError::SessionIdentityMismatch);
    }
    if request.ip_max_sessions.is_some() && request.session.client_ip.is_none() {
        let _ = release_user_lock(client, &user_lock).await;
        return Err(AdmissionError::MissingClientIp);
    }

    let current_sessions = read_user_session_count(client, cluster_id, &user_lock.user_id).await?;
    if current_sessions >= request.max_sessions {
        let _ = release_user_lock(client, &user_lock).await;
        return Err(AdmissionError::UserQuotaExceeded);
    }

    let session_key = user_session_key(cluster_id, &request.session);
    let session_value = serde_json::to_vec(&request.session)?;
    let mut compares = user_lock.lock.compares().to_vec();
    compares.push(config_compare(cluster_id, &request.config));
    let mut operations = Vec::new();

    if let Some(ip_max_sessions) = request.ip_max_sessions {
        let client_ip = request
            .session
            .client_ip
            .as_deref()
            .expect("validated above");
        let ip_lock =
            match try_acquire_ip_lock(client, user_lock.lock.lease_id, cluster_id, client_ip).await
            {
                Ok(lock) => lock,
                Err(error) => {
                    let _ = release_user_lock(client, &user_lock).await;
                    return Err(error);
                }
            };
        let current_ips = read_ip_session_count(client, cluster_id, client_ip).await?;
        if current_ips >= ip_max_sessions {
            let _ = release_lock(client, &ip_lock.lock).await;
            let _ = release_user_lock(client, &user_lock).await;
            return Err(AdmissionError::IpQuotaExceeded);
        }

        compares.extend(ip_lock.lock.compares());
        operations.push(TxnOp::put(
            ip_session_key(cluster_id, client_ip, &request.session.session_id),
            session_value.clone(),
            Some(PutOptions::new().with_lease(lease_id)),
        ));

        let write = write_client_sessions(
            client,
            cluster_id,
            lease_id,
            session_key,
            session_value,
            compares,
            operations,
            &request.config,
        )
        .await;
        let _ = release_lock(client, &ip_lock.lock).await;
        let _ = release_user_lock(client, &user_lock).await;
        write?;
    } else {
        let write = write_client_sessions(
            client,
            cluster_id,
            lease_id,
            session_key,
            session_value,
            compares,
            operations,
            &request.config,
        )
        .await;
        let _ = release_user_lock(client, &user_lock).await;
        write?;
    }

    Ok(ClientPermit {
        session: request.session,
    })
}

#[allow(clippy::too_many_arguments)]
async fn write_client_sessions(
    client: &mut Client,
    cluster_id: &str,
    lease_id: i64,
    session_key: String,
    session_value: Vec<u8>,
    compares: Vec<Compare>,
    mut operations: Vec<TxnOp>,
    config: &ConfigSnapshot,
) -> Result<(), AdmissionError> {
    operations.insert(
        0,
        TxnOp::put(
            session_key,
            session_value,
            Some(PutOptions::new().with_lease(lease_id)),
        ),
    );
    let transaction = Txn::new().when(compares).and_then(operations);

    if client.txn(transaction).await?.succeeded() {
        Ok(())
    } else if load_config_snapshot(client, cluster_id).await?.revision != config.revision {
        Err(AdmissionError::ConfigStale)
    } else {
        Err(AdmissionError::StaleLock)
    }
}

pub async fn admit_source(
    client: &mut Client,
    lease_id: i64,
    cluster_id: &str,
    user_lock: UserLock,
    request: SourceAdmissionRequest,
) -> Result<SourcePermit, AdmissionError> {
    validate_key_segment(cluster_id, "cluster ID")?;
    validate_session(&request.session)?;
    if request.session.user_id != user_lock.user_id
        || request.session.mountpoint != request.source_claim.mountpoint()
        || request.session.user_id != request.source_claim.effective_user_id()
    {
        let _ = release_user_lock(client, &user_lock).await;
        return Err(AdmissionError::SessionIdentityMismatch);
    }

    let current_sessions = read_user_session_count(client, cluster_id, &user_lock.user_id).await?;
    if current_sessions >= request.max_sessions {
        let _ = release_user_lock(client, &user_lock).await;
        return Err(AdmissionError::UserQuotaExceeded);
    }

    let session_key = user_session_key(cluster_id, &request.session);
    let session_value = serde_json::to_vec(&request.session)?;
    let session_operation = TxnOp::put(
        session_key,
        session_value,
        Some(PutOptions::new().with_lease(lease_id)),
    );
    let mut compares = user_lock.lock.compares().to_vec();
    compares.push(config_compare(cluster_id, &request.config));
    let claim = claim_source_with_guards(
        client,
        lease_id,
        cluster_id,
        request.source_claim,
        compares,
        vec![session_operation],
    )
    .await;
    let _ = release_user_lock(client, &user_lock).await;

    match claim {
        Ok(claimed_source) => Ok(SourcePermit {
            session: request.session,
            claimed_source,
        }),
        Err(ClaimError::GuardFailed) => {
            if load_config_snapshot(client, cluster_id).await?.revision != request.config.revision {
                Err(AdmissionError::ConfigStale)
            } else {
                Err(AdmissionError::StaleLock)
            }
        }
        Err(error) => Err(AdmissionError::Source(error)),
    }
}

pub(crate) fn validate_session(session: &SessionRecord) -> Result<(), AdmissionError> {
    validate_key_segment(&session.session_id, "session ID")?;
    validate_key_segment(&session.user_id, "user ID")?;
    validate_key_segment(&session.mountpoint, "mountpoint")?;
    validate_key_segment(&session.caster_id, "caster ID")?;
    validate_key_segment(&session.runtime_epoch, "runtime epoch")?;
    if let Some(client_ip) = &session.client_ip {
        validate_key_segment(client_ip, "client IP")?;
    }
    Ok(())
}

pub(crate) fn user_session_key(cluster_id: &str, session: &SessionRecord) -> String {
    format!(
        "/rcaster/{cluster_id}/online/users/{}/sessions/{}",
        session.user_id, session.session_id
    )
}

pub(crate) fn ip_session_key(cluster_id: &str, client_ip: &str, session_id: &str) -> String {
    format!("/rcaster/{cluster_id}/online/ips/{client_ip}/client-sessions/{session_id}")
}

pub(crate) fn config_compare(cluster_id: &str, config: &ConfigSnapshot) -> Compare {
    Compare::mod_revision(
        format!("/rcaster/{cluster_id}/config/caster"),
        CompareOp::Equal,
        config.revision,
    )
}

pub(crate) fn validate_key_segment(value: &str, name: &'static str) -> Result<(), AdmissionError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(AdmissionError::InvalidIdentifier(name));
    }
    Ok(())
}

fn owner_token() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::{owner_token, validate_key_segment};

    #[test]
    fn rejects_identifiers_that_escape_an_etcd_key_segment() {
        assert!(validate_key_segment("u_01j", "user ID").is_ok());
        assert!(validate_key_segment("u/01j", "user ID").is_err());
        assert!(validate_key_segment("", "user ID").is_err());
    }

    #[test]
    fn lock_owner_tokens_are_256_bits_of_hex() {
        let first = owner_token();
        let second = owner_token();

        assert_eq!(first.len(), 64);
        assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_ne!(first, second);
    }
}
