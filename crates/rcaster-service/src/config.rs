use std::fmt;

use etcd_client::{Client, Compare, CompareOp, Txn, TxnOp};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct CasterConfig {
    pub schema_version: u8,
    pub generation: u64,
}

impl CasterConfig {
    pub fn new(generation: u64) -> Self {
        Self {
            schema_version: 1,
            generation,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigSnapshot {
    pub config: CasterConfig,
    pub revision: i64,
}

#[derive(Debug)]
pub enum ConfigError {
    InvalidIdentifier(&'static str),
    InvalidConfig,
    UnsupportedSchema(u8),
    GenerationNotMonotonic,
    ConfigConflict,
    Etcd(etcd_client::Error),
    Json(serde_json::Error),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentifier(name) => write!(formatter, "invalid {name}"),
            Self::InvalidConfig => formatter.write_str("caster config is missing or invalid"),
            Self::UnsupportedSchema(version) => {
                write!(formatter, "unsupported caster config schema {version}")
            }
            Self::GenerationNotMonotonic => {
                formatter.write_str("caster config generation must increase")
            }
            Self::ConfigConflict => formatter.write_str("caster config changed concurrently"),
            Self::Etcd(error) => write!(formatter, "etcd error: {error}"),
            Self::Json(error) => write!(formatter, "json error: {error}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<etcd_client::Error> for ConfigError {
    fn from(error: etcd_client::Error) -> Self {
        Self::Etcd(error)
    }
}

impl From<serde_json::Error> for ConfigError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

pub async fn publish_config(
    client: &mut Client,
    cluster_id: &str,
    config: CasterConfig,
) -> Result<ConfigSnapshot, ConfigError> {
    let key = config_key(cluster_id)?;
    let current = client.get(key.clone(), None).await?.kvs().first().cloned();
    let compare = match &current {
        Some(value) => {
            let old = decode_config(value.value())?;
            if config.generation <= old.generation {
                return Err(ConfigError::GenerationNotMonotonic);
            }
            Compare::mod_revision(key.clone(), CompareOp::Equal, value.mod_revision())
        }
        None => Compare::create_revision(key.clone(), CompareOp::Equal, 0),
    };
    let value = serde_json::to_vec(&config)?;
    let transaction = Txn::new()
        .when([compare])
        .and_then([TxnOp::put(key, value, None)]);
    let response = client.txn(transaction).await?;
    if !response.succeeded() {
        return Err(ConfigError::ConfigConflict);
    }
    let revision = response
        .header()
        .ok_or(ConfigError::InvalidConfig)?
        .revision();

    Ok(ConfigSnapshot { config, revision })
}

pub async fn load_config_snapshot(
    client: &mut Client,
    cluster_id: &str,
) -> Result<ConfigSnapshot, ConfigError> {
    let key = config_key(cluster_id)?;
    let value = client
        .get(key, None)
        .await?
        .kvs()
        .first()
        .cloned()
        .ok_or(ConfigError::InvalidConfig)?;
    let config = decode_config(value.value())?;
    Ok(ConfigSnapshot {
        config,
        revision: value.mod_revision(),
    })
}

fn config_key(cluster_id: &str) -> Result<String, ConfigError> {
    if cluster_id.is_empty()
        || !cluster_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ConfigError::InvalidIdentifier("cluster ID"));
    }
    Ok(format!("/rcaster/{cluster_id}/config/caster"))
}

fn decode_config(value: &[u8]) -> Result<CasterConfig, ConfigError> {
    let config: CasterConfig = serde_json::from_slice(value)?;
    if config.schema_version != 1 {
        return Err(ConfigError::UnsupportedSchema(config.schema_version));
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::{config_key, decode_config};

    #[test]
    fn rejects_cluster_ids_that_escape_the_config_key() {
        assert!(config_key("cluster-east").is_ok());
        assert!(config_key("cluster/extra").is_err());
        assert!(config_key("").is_err());
    }

    #[test]
    fn rejects_invalid_config_json() {
        assert!(decode_config(b"{not-json}").is_err());
    }
}
