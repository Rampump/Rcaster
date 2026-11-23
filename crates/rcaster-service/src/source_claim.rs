use std::fmt;

use etcd_client::{Client, Compare, CompareOp, DeleteOptions, PutOptions, Txn, TxnOp};
use rcaster_core::{protocol::DataProfile, str_record::StrRecord};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct SourceClaimRequest {
    pub(crate) mountpoint: String,
    pub(crate) caster_id: String,
    pub(crate) connection_id: String,
    pub(crate) runtime_epoch: String,
    pub(crate) relay_endpoint: String,
    pub(crate) effective_user_id: String,
    pub(crate) created_at: String,
}

impl SourceClaimRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mountpoint: impl Into<String>,
        caster_id: impl Into<String>,
        connection_id: impl Into<String>,
        runtime_epoch: impl Into<String>,
        relay_endpoint: impl Into<String>,
        effective_user_id: impl Into<String>,
        created_at: impl Into<String>,
    ) -> Self {
        Self {
            mountpoint: mountpoint.into(),
            caster_id: caster_id.into(),
            connection_id: connection_id.into(),
            runtime_epoch: runtime_epoch.into(),
            relay_endpoint: relay_endpoint.into(),
            effective_user_id: effective_user_id.into(),
            created_at: created_at.into(),
        }
    }

    pub fn mountpoint(&self) -> &str {
        &self.mountpoint
    }

    pub fn effective_user_id(&self) -> &str {
        &self.effective_user_id
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct SourceClaim {
    pub schema_version: u8,
    pub source_generation: u64,
    pub connection_id: String,
    pub mountpoint: String,
    pub caster_id: String,
    pub runtime_epoch: String,
    pub relay_endpoint: String,
    pub effective_user_id: String,
    pub created_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceRoute {
    pub source_generation: u64,
    pub connection_id: String,
    pub mountpoint: String,
    pub caster_id: String,
    pub runtime_epoch: String,
    pub relay_endpoint: String,
    pub str_record: StrRecord,
}

#[derive(Clone, Debug)]
pub struct ClaimedSource {
    pub claim: SourceClaim,
    pub claim_revision: i64,
    pub source_revision: i64,
    lease_id: i64,
    claim_key: String,
    source_key: String,
    claim_value: Vec<u8>,
    provisional_source_value: Vec<u8>,
}

pub(crate) struct PreparedSourceClaim {
    claim: SourceClaim,
    lease_id: i64,
    claim_key: String,
    source_key: String,
    claim_value: Vec<u8>,
    provisional_source_value: Vec<u8>,
    pub(crate) compares: Vec<Compare>,
    pub(crate) operations: Vec<TxnOp>,
}

impl PreparedSourceClaim {
    pub(crate) fn finalize(self, revision: i64) -> ClaimedSource {
        ClaimedSource {
            claim: self.claim,
            claim_revision: revision,
            source_revision: revision,
            lease_id: self.lease_id,
            claim_key: self.claim_key,
            source_key: self.source_key,
            claim_value: self.claim_value,
            provisional_source_value: self.provisional_source_value,
        }
    }
}

#[derive(Debug)]
pub enum ClaimError {
    AlreadyClaimed,
    GenerationConflict,
    GuardFailed,
    StaleClaim,
    InvalidArgument(&'static str),
    CorruptState,
    Etcd(etcd_client::Error),
    Json(serde_json::Error),
}

impl fmt::Display for ClaimError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyClaimed => formatter.write_str("mountpoint already has a source claim"),
            Self::GenerationConflict => formatter.write_str("source generation changed"),
            Self::GuardFailed => formatter.write_str("source admission guard failed"),
            Self::StaleClaim => formatter.write_str("source claim is stale"),
            Self::InvalidArgument(message) => formatter.write_str(message),
            Self::CorruptState => formatter.write_str("corrupt source claim state"),
            Self::Etcd(error) => write!(formatter, "etcd error: {error}"),
            Self::Json(error) => write!(formatter, "json error: {error}"),
        }
    }
}

impl std::error::Error for ClaimError {}

impl From<etcd_client::Error> for ClaimError {
    fn from(error: etcd_client::Error) -> Self {
        Self::Etcd(error)
    }
}

impl From<serde_json::Error> for ClaimError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct SourceRecord {
    schema_version: u8,
    state: SourceState,
    source_generation: u64,
    connection_id: String,
    mountpoint: String,
    caster_id: String,
    runtime_epoch: String,
    relay_endpoint: String,
    effective_user_id: String,
    created_at: String,
    #[serde(rename = "str")]
    str_record: StrRecord,
}

impl SourceRecord {
    fn from_claim(claim: &SourceClaim, state: SourceState, str_record: &StrRecord) -> Self {
        let mut str_record = str_record.clone();
        str_record.mountpoint = claim.mountpoint.clone();
        Self {
            schema_version: claim.schema_version,
            state,
            source_generation: claim.source_generation,
            connection_id: claim.connection_id.clone(),
            mountpoint: claim.mountpoint.clone(),
            caster_id: claim.caster_id.clone(),
            runtime_epoch: claim.runtime_epoch.clone(),
            relay_endpoint: claim.relay_endpoint.clone(),
            effective_user_id: claim.effective_user_id.clone(),
            created_at: claim.created_at.clone(),
            str_record,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
enum SourceState {
    #[serde(rename = "VALIDATING")]
    Validating,
    #[serde(rename = "ACTIVE")]
    Active,
}

pub(crate) fn decode_source_route(value: &[u8]) -> Result<Option<SourceRoute>, ClaimError> {
    let record: SourceRecord = serde_json::from_slice(value)?;
    if record.schema_version != 1 {
        return Err(ClaimError::CorruptState);
    }
    if matches!(record.state, SourceState::Validating) {
        return Ok(None);
    }
    Ok(Some(SourceRoute {
        source_generation: record.source_generation,
        connection_id: record.connection_id,
        mountpoint: record.mountpoint,
        caster_id: record.caster_id,
        runtime_epoch: record.runtime_epoch,
        relay_endpoint: record.relay_endpoint,
        str_record: record.str_record,
    }))
}

pub async fn claim_source(
    client: &mut Client,
    lease_id: i64,
    cluster_id: &str,
    request: SourceClaimRequest,
) -> Result<ClaimedSource, ClaimError> {
    claim_source_with_guards(
        client,
        lease_id,
        cluster_id,
        request,
        Vec::new(),
        Vec::new(),
    )
    .await
}

pub(crate) async fn claim_source_with_guards(
    client: &mut Client,
    lease_id: i64,
    cluster_id: &str,
    request: SourceClaimRequest,
    additional_compares: Vec<Compare>,
    additional_operations: Vec<TxnOp>,
) -> Result<ClaimedSource, ClaimError> {
    for _ in 0..8 {
        let mut prepared =
            prepare_source_claim(client, lease_id, cluster_id, request.clone()).await?;
        prepared
            .compares
            .extend(additional_compares.iter().cloned());
        prepared
            .operations
            .extend(additional_operations.iter().cloned());
        let transaction = Txn::new()
            .when(std::mem::take(&mut prepared.compares))
            .and_then(std::mem::take(&mut prepared.operations));
        let response = client.txn(transaction).await?;
        if response.succeeded() {
            let revision = response
                .header()
                .ok_or(ClaimError::CorruptState)?
                .revision();
            return Ok(prepared.finalize(revision));
        }
        if !additional_compares.is_empty() {
            return Err(ClaimError::GuardFailed);
        }
    }
    Err(ClaimError::GenerationConflict)
}

pub(crate) async fn prepare_source_claim(
    client: &mut Client,
    lease_id: i64,
    cluster_id: &str,
    request: SourceClaimRequest,
) -> Result<PreparedSourceClaim, ClaimError> {
    validate_identifier(cluster_id, "cluster ID")?;
    validate_mountpoint(&request.mountpoint)?;

    let claim_key = format!(
        "/rcaster/{cluster_id}/runtime/source-claims/{}",
        request.mountpoint
    );
    let generation_key = format!(
        "/rcaster/{cluster_id}/runtime/source-generations/{}",
        request.mountpoint
    );
    let source_key = format!(
        "/rcaster/{cluster_id}/runtime/sources/{}",
        request.mountpoint
    );

    if !client.get(claim_key.clone(), None).await?.kvs().is_empty() {
        return Err(ClaimError::AlreadyClaimed);
    }

    let generation_value = client
        .get(generation_key.clone(), None)
        .await?
        .kvs()
        .first()
        .cloned();
    let old_generation = match &generation_value {
        Some(value) => std::str::from_utf8(value.value())
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or(ClaimError::CorruptState)?,
        None => 0,
    };
    let next_generation = old_generation
        .checked_add(1)
        .ok_or(ClaimError::CorruptState)?;
    let claim = SourceClaim {
        schema_version: 1,
        source_generation: next_generation,
        connection_id: request.connection_id.clone(),
        mountpoint: request.mountpoint.clone(),
        caster_id: request.caster_id.clone(),
        runtime_epoch: request.runtime_epoch.clone(),
        relay_endpoint: request.relay_endpoint.clone(),
        effective_user_id: request.effective_user_id.clone(),
        created_at: request.created_at.clone(),
    };
    let claim_value = serde_json::to_vec(&claim)?;
    let provisional_str = StrRecord::from_profile(&claim.mountpoint, &DataProfile::default());
    let provisional_source_value = serde_json::to_vec(&SourceRecord::from_claim(
        &claim,
        SourceState::Validating,
        &provisional_str,
    ))?;
    let generation_compare = match generation_value {
        Some(value) => Compare::value(generation_key.clone(), CompareOp::Equal, value.value()),
        None => Compare::create_revision(generation_key.clone(), CompareOp::Equal, 0),
    };

    Ok(PreparedSourceClaim {
        claim,
        lease_id,
        claim_key: claim_key.clone(),
        source_key: source_key.clone(),
        claim_value: claim_value.clone(),
        provisional_source_value: provisional_source_value.clone(),
        compares: vec![
            Compare::create_revision(claim_key.clone(), CompareOp::Equal, 0),
            generation_compare,
        ],
        operations: vec![
            TxnOp::put(generation_key, next_generation.to_string(), None),
            TxnOp::put(
                claim_key,
                claim_value,
                Some(PutOptions::new().with_lease(lease_id)),
            ),
            TxnOp::put(
                source_key,
                provisional_source_value,
                Some(PutOptions::new().with_lease(lease_id)),
            ),
        ],
    })
}

pub async fn publish_active(
    client: &mut Client,
    claimed: &ClaimedSource,
    str_record: &StrRecord,
) -> Result<(), ClaimError> {
    let active_source_value = serde_json::to_vec(&SourceRecord::from_claim(
        &claimed.claim,
        SourceState::Active,
        str_record,
    ))?;
    let transaction = Txn::new()
        .when(claimed.publish_compares())
        .and_then([TxnOp::put(
            claimed.source_key.clone(),
            active_source_value,
            Some(PutOptions::new().with_lease(claimed.lease_id)),
        )]);

    if client.txn(transaction).await?.succeeded() {
        Ok(())
    } else {
        Err(ClaimError::StaleClaim)
    }
}

pub async fn release_source(
    client: &mut Client,
    claimed: &ClaimedSource,
) -> Result<(), ClaimError> {
    let transaction = Txn::new().when(claimed.claim_compares()).and_then([
        TxnOp::delete(claimed.claim_key.clone(), Some(DeleteOptions::new())),
        TxnOp::delete(claimed.source_key.clone(), Some(DeleteOptions::new())),
    ]);

    if client.txn(transaction).await?.succeeded() {
        Ok(())
    } else {
        Err(ClaimError::StaleClaim)
    }
}

pub async fn release_source_for_connection(
    client: &mut Client,
    cluster_id: &str,
    mountpoint: &str,
    connection_id: &str,
    runtime_epoch: &str,
) -> Result<(), ClaimError> {
    validate_identifier(cluster_id, "cluster ID")?;
    validate_mountpoint(mountpoint)?;
    let claim_key = format!("/rcaster/{cluster_id}/runtime/source-claims/{mountpoint}");
    let source_key = format!("/rcaster/{cluster_id}/runtime/sources/{mountpoint}");
    let response = client.get(claim_key.clone(), None).await?;
    let value = response.kvs().first().ok_or(ClaimError::StaleClaim)?;
    let claim: SourceClaim = serde_json::from_slice(value.value())?;
    if claim.connection_id != connection_id || claim.runtime_epoch != runtime_epoch {
        return Err(ClaimError::StaleClaim);
    }
    let transaction = Txn::new()
        .when([
            Compare::mod_revision(claim_key.clone(), CompareOp::Equal, value.mod_revision()),
            Compare::value(claim_key.clone(), CompareOp::Equal, value.value().to_vec()),
        ])
        .and_then([
            TxnOp::delete(claim_key, Some(DeleteOptions::new())),
            TxnOp::delete(source_key, Some(DeleteOptions::new())),
        ]);
    if client.txn(transaction).await?.succeeded() {
        Ok(())
    } else {
        Err(ClaimError::StaleClaim)
    }
}

pub async fn activate_source_for_connection(
    client: &mut Client,
    cluster_id: &str,
    mountpoint: &str,
    connection_id: &str,
    runtime_epoch: &str,
    str_record: &StrRecord,
) -> Result<(), ClaimError> {
    validate_identifier(cluster_id, "cluster ID")?;
    validate_mountpoint(mountpoint)?;
    let claim_key = format!("/rcaster/{cluster_id}/runtime/source-claims/{mountpoint}");
    let source_key = format!("/rcaster/{cluster_id}/runtime/sources/{mountpoint}");
    let claim_response = client.get(claim_key.clone(), None).await?;
    let claim_kv = claim_response.kvs().first().ok_or(ClaimError::StaleClaim)?;
    let claim: SourceClaim = serde_json::from_slice(claim_kv.value())?;
    if claim.connection_id != connection_id || claim.runtime_epoch != runtime_epoch {
        return Err(ClaimError::StaleClaim);
    }
    let source_response = client.get(source_key.clone(), None).await?;
    let source_kv = source_response
        .kvs()
        .first()
        .ok_or(ClaimError::StaleClaim)?;
    let active = serde_json::to_vec(&SourceRecord::from_claim(
        &claim,
        SourceState::Active,
        str_record,
    ))?;
    let transaction = Txn::new()
        .when([
            Compare::mod_revision(claim_key, CompareOp::Equal, claim_kv.mod_revision()),
            Compare::value(
                format!("/rcaster/{cluster_id}/runtime/source-claims/{mountpoint}"),
                CompareOp::Equal,
                claim_kv.value().to_vec(),
            ),
            Compare::mod_revision(
                source_key.clone(),
                CompareOp::Equal,
                source_kv.mod_revision(),
            ),
            Compare::value(
                source_key.clone(),
                CompareOp::Equal,
                source_kv.value().to_vec(),
            ),
        ])
        .and_then([TxnOp::put(
            source_key,
            active,
            Some(PutOptions::new().with_lease(source_kv.lease())),
        )]);
    if client.txn(transaction).await?.succeeded() {
        Ok(())
    } else {
        Err(ClaimError::StaleClaim)
    }
}

impl ClaimedSource {
    fn claim_compares(&self) -> [Compare; 3] {
        [
            Compare::mod_revision(
                self.claim_key.clone(),
                CompareOp::Equal,
                self.claim_revision,
            ),
            Compare::value(
                self.claim_key.clone(),
                CompareOp::Equal,
                self.claim_value.clone(),
            ),
            Compare::lease(self.claim_key.clone(), CompareOp::Equal, self.lease_id),
        ]
    }

    fn publish_compares(&self) -> [Compare; 5] {
        [
            Compare::mod_revision(
                self.claim_key.clone(),
                CompareOp::Equal,
                self.claim_revision,
            ),
            Compare::value(
                self.claim_key.clone(),
                CompareOp::Equal,
                self.claim_value.clone(),
            ),
            Compare::lease(self.claim_key.clone(), CompareOp::Equal, self.lease_id),
            Compare::mod_revision(
                self.source_key.clone(),
                CompareOp::Equal,
                self.source_revision,
            ),
            Compare::value(
                self.source_key.clone(),
                CompareOp::Equal,
                self.provisional_source_value.clone(),
            ),
        ]
    }
}

fn validate_identifier(value: &str, name: &'static str) -> Result<(), ClaimError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ClaimError::InvalidArgument(name));
    }
    Ok(())
}

fn validate_mountpoint(value: &str) -> Result<(), ClaimError> {
    if value.is_empty()
        || value.len() > 100
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ClaimError::InvalidArgument("invalid mountpoint"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ClaimError, validate_mountpoint};

    #[test]
    fn rejects_mountpoints_that_escape_the_etcd_key_segment() {
        assert!(matches!(
            validate_mountpoint("C63/extra"),
            Err(ClaimError::InvalidArgument("invalid mountpoint"))
        ));
        assert!(matches!(
            validate_mountpoint(""),
            Err(ClaimError::InvalidArgument("invalid mountpoint"))
        ));
    }

    #[test]
    fn accepts_the_documented_mountpoint_character_set() {
        validate_mountpoint("C63-east_1.test").expect("valid mountpoint");
    }
}
