use std::fmt;

use etcd_client::{Client, Compare, CompareOp, DeleteOptions, Txn, TxnOp, TxnOpResponse};
use rcaster_core::account::AccountSnapshot;
use serde::Serialize;

use crate::{
    account_auth::validate_password_hash,
    account_directory::{AccountDirectoryError, load_accounts},
    config::{ConfigError, config_key, load_config_snapshot},
};

#[derive(Debug, Serialize)]
pub struct RecoveryReport {
    pub config_generation: u64,
    pub config_revision: i64,
    pub users: usize,
    pub mountpoints: usize,
    pub deleted_runtime_keys: i64,
}

#[derive(Debug)]
pub enum RecoveryError {
    Etcd(etcd_client::Error),
    Config(ConfigError),
    Accounts(AccountDirectoryError),
    InvalidPasswordHash(String),
    ConfigChanged,
    RuntimeStateRemaining,
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Etcd(error) => write!(formatter, "etcd recovery error: {error}"),
            Self::Config(error) => write!(formatter, "config recovery error: {error}"),
            Self::Accounts(error) => write!(formatter, "account recovery error: {error}"),
            Self::InvalidPasswordHash(record) => {
                write!(formatter, "invalid password hash in {record}")
            }
            Self::ConfigChanged => formatter.write_str("caster config changed during recovery"),
            Self::RuntimeStateRemaining => {
                formatter.write_str("runtime state remains after recovery purge")
            }
        }
    }
}

impl std::error::Error for RecoveryError {}

impl From<etcd_client::Error> for RecoveryError {
    fn from(error: etcd_client::Error) -> Self {
        Self::Etcd(error)
    }
}

impl From<ConfigError> for RecoveryError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<AccountDirectoryError> for RecoveryError {
    fn from(error: AccountDirectoryError) -> Self {
        Self::Accounts(error)
    }
}

pub async fn prepare_restored_cluster(
    client: &mut Client,
    cluster_id: &str,
) -> Result<RecoveryReport, RecoveryError> {
    let config = load_config_snapshot(client, cluster_id).await?;
    let accounts = load_accounts(client, cluster_id).await?;
    validate_account_hashes(&accounts)?;

    let root = format!("/rcaster/{cluster_id}/");
    let prefixes = [
        format!("{root}runtime/"),
        format!("{root}online/"),
        format!("{root}locks/"),
    ];
    let config_key = config_key(cluster_id)?;
    let response =
        client
            .txn(
                Txn::new()
                    .when([Compare::mod_revision(
                        config_key,
                        CompareOp::Equal,
                        config.revision,
                    )])
                    .and_then(prefixes.clone().map(|prefix| {
                        TxnOp::delete(prefix, Some(DeleteOptions::new().with_prefix()))
                    })),
            )
            .await?;
    if !response.succeeded() {
        return Err(RecoveryError::ConfigChanged);
    }
    let deleted_runtime_keys = response
        .op_responses()
        .into_iter()
        .map(|response| match response {
            TxnOpResponse::Delete(response) => response.deleted(),
            _ => 0,
        })
        .sum();

    for prefix in prefixes {
        let remaining = client
            .get(prefix, Some(etcd_client::GetOptions::new().with_prefix()))
            .await?;
        if !remaining.kvs().is_empty() {
            return Err(RecoveryError::RuntimeStateRemaining);
        }
    }

    Ok(RecoveryReport {
        config_generation: config.config.generation,
        config_revision: config.revision,
        users: accounts.users().count(),
        mountpoints: accounts.mountpoints().count(),
        deleted_runtime_keys,
    })
}

fn validate_account_hashes(accounts: &AccountSnapshot) -> Result<(), RecoveryError> {
    for user in accounts.users() {
        validate_password_hash(&user.password_hash)
            .map_err(|_| RecoveryError::InvalidPasswordHash(format!("user {}", user.user_id)))?;
    }
    for mountpoint in accounts.mountpoints() {
        if let Some(hash) = &mountpoint.ntrip1_upload_password_hash {
            validate_password_hash(hash).map_err(|_| {
                RecoveryError::InvalidPasswordHash(format!(
                    "mountpoint {} NTRIP1 upload password",
                    mountpoint.mountpoint
                ))
            })?;
        }
    }
    Ok(())
}
