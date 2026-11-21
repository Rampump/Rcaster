use std::time::Duration;

use etcd_client::{Client, Compare, CompareOp, GetOptions, PutOptions, Txn, TxnOp};
use rand::Rng;
use tokio::time::sleep;

use crate::{
    admission::{
        AdmissionError, ClientAdmissionRequest, ClientPermit, SourceAdmissionRequest, SourcePermit,
        config_compare, ip_session_key, read_ip_session_count, read_user_session_count,
        user_session_key, validate_key_segment, validate_session,
    },
    config::load_config_snapshot,
    source_claim::{ClaimError, claim_source_with_guards},
};

const RECORD_VALUE: &[u8] = br#"{"schema_version":1}"#;
const MAX_RECORD_RETRIES: usize = 128;

#[derive(Clone, Debug)]
struct RecordSnapshot {
    key: String,
    revision: Option<i64>,
}

impl RecordSnapshot {
    fn compare(&self) -> Compare {
        match self.revision {
            Some(revision) => Compare::mod_revision(self.key.clone(), CompareOp::Equal, revision),
            None => Compare::create_revision(self.key.clone(), CompareOp::Equal, 0),
        }
    }

    fn put_operation(&self) -> TxnOp {
        TxnOp::put(self.key.clone(), RECORD_VALUE.to_vec(), None)
    }
}

pub async fn admit_client_revision(
    client: &mut Client,
    lease_id: i64,
    cluster_id: &str,
    request: ClientAdmissionRequest,
) -> Result<ClientPermit, AdmissionError> {
    validate_key_segment(cluster_id, "cluster ID")?;
    validate_session(&request.session)?;
    if request.ip_max_sessions.is_some() && request.session.client_ip.is_none() {
        return Err(AdmissionError::MissingClientIp);
    }

    let user_record_key = user_record_key(cluster_id, &request.session.user_id)?;
    let session_key = user_session_key(cluster_id, &request.session);
    let session_value = serde_json::to_vec(&request.session)?;

    for _ in 0..MAX_RECORD_RETRIES {
        let user_record = load_record(client, user_record_key.clone()).await?;
        let current_sessions =
            read_user_session_count(client, cluster_id, &request.session.user_id).await?;
        if current_sessions >= request.max_sessions {
            return Err(AdmissionError::UserQuotaExceeded);
        }

        let mut compares = vec![
            user_record.compare(),
            config_compare(cluster_id, &request.config),
        ];
        let mut operations = vec![
            user_record.put_operation(),
            TxnOp::put(
                session_key.clone(),
                session_value.clone(),
                Some(PutOptions::new().with_lease(lease_id)),
            ),
        ];

        if let Some(ip_max_sessions) = request.ip_max_sessions {
            let client_ip = request
                .session
                .client_ip
                .as_deref()
                .expect("validated above");
            let ip_record_key = ip_record_key(cluster_id, client_ip)?;
            let ip_record = load_record(client, ip_record_key).await?;
            let current_ips = read_ip_session_count(client, cluster_id, client_ip).await?;
            if current_ips >= ip_max_sessions {
                return Err(AdmissionError::IpQuotaExceeded);
            }
            compares.push(ip_record.compare());
            operations.push(ip_record.put_operation());
            operations.push(TxnOp::put(
                ip_session_key(cluster_id, client_ip, &request.session.session_id),
                session_value.clone(),
                Some(PutOptions::new().with_lease(lease_id)),
            ));
        }

        let transaction = Txn::new().when(compares).and_then(operations);
        if client.txn(transaction).await?.succeeded() {
            return Ok(ClientPermit {
                session: request.session,
            });
        }

        if load_config_snapshot(client, cluster_id).await?.revision != request.config.revision {
            return Err(AdmissionError::ConfigStale);
        }
        retry_delay().await;
    }

    Err(AdmissionError::AdmissionConflict)
}

pub async fn admit_source_revision(
    client: &mut Client,
    lease_id: i64,
    cluster_id: &str,
    request: SourceAdmissionRequest,
) -> Result<SourcePermit, AdmissionError> {
    validate_key_segment(cluster_id, "cluster ID")?;
    validate_session(&request.session)?;
    if request.session.user_id != request.source_claim.effective_user_id()
        || request.session.mountpoint != request.source_claim.mountpoint()
    {
        return Err(AdmissionError::SessionIdentityMismatch);
    }

    let user_record_key = user_record_key(cluster_id, &request.session.user_id)?;
    let session_key = user_session_key(cluster_id, &request.session);
    let session_value = serde_json::to_vec(&request.session)?;

    for _ in 0..MAX_RECORD_RETRIES {
        let user_record = load_record(client, user_record_key.clone()).await?;
        let current_sessions =
            read_user_session_count(client, cluster_id, &request.session.user_id).await?;
        if current_sessions >= request.max_sessions {
            return Err(AdmissionError::UserQuotaExceeded);
        }

        let claim = claim_source_with_guards(
            client,
            lease_id,
            cluster_id,
            request.source_claim.clone(),
            vec![
                user_record.compare(),
                config_compare(cluster_id, &request.config),
            ],
            vec![
                user_record.put_operation(),
                TxnOp::put(
                    session_key.clone(),
                    session_value.clone(),
                    Some(PutOptions::new().with_lease(lease_id)),
                ),
            ],
        )
        .await;

        match claim {
            Ok(claimed_source) => {
                return Ok(SourcePermit {
                    session: request.session,
                    claimed_source,
                });
            }
            Err(ClaimError::GuardFailed) => {
                if load_config_snapshot(client, cluster_id).await?.revision
                    != request.config.revision
                {
                    return Err(AdmissionError::ConfigStale);
                }
                retry_delay().await;
            }
            Err(error) => return Err(AdmissionError::Source(error)),
        }
    }

    Err(AdmissionError::AdmissionConflict)
}

async fn load_record(client: &mut Client, key: String) -> Result<RecordSnapshot, AdmissionError> {
    let revision = client
        .get(key.clone(), Some(GetOptions::new()))
        .await?
        .kvs()
        .first()
        .map(|value| value.mod_revision());
    Ok(RecordSnapshot { key, revision })
}

async fn retry_delay() {
    let delay_ms = rand::rng().random_range(1..=8);
    sleep(Duration::from_millis(delay_ms)).await;
}

fn user_record_key(cluster_id: &str, user_id: &str) -> Result<String, AdmissionError> {
    validate_key_segment(cluster_id, "cluster ID")?;
    validate_key_segment(user_id, "user ID")?;
    Ok(format!(
        "/rcaster/{cluster_id}/runtime/admission/users/{user_id}"
    ))
}

fn ip_record_key(cluster_id: &str, client_ip: &str) -> Result<String, AdmissionError> {
    validate_key_segment(cluster_id, "cluster ID")?;
    validate_key_segment(client_ip, "client IP")?;
    Ok(format!(
        "/rcaster/{cluster_id}/runtime/admission/ips/{client_ip}"
    ))
}

#[cfg(test)]
mod tests {
    use super::{ip_record_key, user_record_key};

    #[test]
    fn record_keys_are_scoped_by_cluster_user_and_ip() {
        assert_eq!(
            user_record_key("east", "u_01j").expect("user key"),
            "/rcaster/east/runtime/admission/users/u_01j"
        );
        assert_eq!(
            ip_record_key("east", "2001:db8::1").expect("IP key"),
            "/rcaster/east/runtime/admission/ips/2001:db8::1"
        );
    }

    #[test]
    fn record_keys_reject_path_separators() {
        assert!(user_record_key("east", "u/01j").is_err());
        assert!(ip_record_key("east", "203.0.113.0/24").is_err());
    }
}
