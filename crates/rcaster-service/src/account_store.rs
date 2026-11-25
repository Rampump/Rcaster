use std::{
    fmt,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use rcaster_core::account::{AccountError, AccountSnapshot};
use redb::{Database, ReadableTable, TableDefinition};

use crate::{
    admin_auth::ApiToken,
    blacklist::{BlacklistError, BlacklistRule, validate_rule},
    session_limit::IpNetwork,
};

const USERS: TableDefinition<&str, &[u8]> = TableDefinition::new("users");
const MOUNTPOINTS: TableDefinition<&str, &[u8]> = TableDefinition::new("mountpoints");
const BLACKLIST: TableDefinition<&str, &[u8]> = TableDefinition::new("blacklist");
const API_TOKENS: TableDefinition<&str, &[u8]> = TableDefinition::new("api_tokens");
const API_TOKEN_LAST_USED: TableDefinition<&str, &[u8]> =
    TableDefinition::new("api_token_last_used");

pub struct AccountStore {
    database: Database,
}

#[derive(Debug)]
pub enum StoreError {
    Database(String),
    Json(serde_json::Error),
    Account(AccountError),
    Blacklist(BlacklistError),
}

impl AccountStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let database =
            Database::create(path).map_err(|error| StoreError::Database(error.to_string()))?;
        ensure_tables(&database)?;
        Ok(Self { database })
    }

    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let database =
            Database::open(path).map_err(|error| StoreError::Database(error.to_string()))?;
        ensure_tables(&database)?;
        Ok(Self { database })
    }

    pub fn replace(&self, snapshot: &AccountSnapshot) -> Result<(), StoreError> {
        let transaction = self
            .database
            .begin_write()
            .map_err(|error| StoreError::Database(error.to_string()))?;
        {
            let mut users = transaction
                .open_table(USERS)
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let stale_users = users
                .iter()
                .map_err(|error| StoreError::Database(error.to_string()))?
                .map(|entry| {
                    entry
                        .map(|(key, _)| key.value().to_owned())
                        .map_err(|error| StoreError::Database(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            for key in stale_users {
                users
                    .remove(key.as_str())
                    .map_err(|error| StoreError::Database(error.to_string()))?;
            }
            for user in snapshot.users() {
                let value = serde_json::to_vec(user)?;
                users
                    .insert(user.user_id.as_str(), value.as_slice())
                    .map_err(|error| StoreError::Database(error.to_string()))?;
            }

            let mut mountpoints = transaction
                .open_table(MOUNTPOINTS)
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let stale_mountpoints = mountpoints
                .iter()
                .map_err(|error| StoreError::Database(error.to_string()))?
                .map(|entry| {
                    entry
                        .map(|(key, _)| key.value().to_owned())
                        .map_err(|error| StoreError::Database(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            for key in stale_mountpoints {
                mountpoints
                    .remove(key.as_str())
                    .map_err(|error| StoreError::Database(error.to_string()))?;
            }
            for mountpoint in snapshot.mountpoints() {
                let value = serde_json::to_vec(mountpoint)?;
                mountpoints
                    .insert(mountpoint.mountpoint.as_str(), value.as_slice())
                    .map_err(|error| StoreError::Database(error.to_string()))?;
            }
        }
        transaction
            .commit()
            .map_err(|error| StoreError::Database(error.to_string()))
    }

    pub fn load(&self) -> Result<AccountSnapshot, StoreError> {
        let transaction = self
            .database
            .begin_read()
            .map_err(|error| StoreError::Database(error.to_string()))?;
        let users = transaction
            .open_table(USERS)
            .map_err(|error| StoreError::Database(error.to_string()))?
            .iter()
            .map_err(|error| StoreError::Database(error.to_string()))?
            .map(|entry| {
                let (_, value) = entry.map_err(|error| StoreError::Database(error.to_string()))?;
                serde_json::from_slice(value.value()).map_err(StoreError::Json)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mountpoints = transaction
            .open_table(MOUNTPOINTS)
            .map_err(|error| StoreError::Database(error.to_string()))?
            .iter()
            .map_err(|error| StoreError::Database(error.to_string()))?
            .map(|entry| {
                let (_, value) = entry.map_err(|error| StoreError::Database(error.to_string()))?;
                serde_json::from_slice(value.value()).map_err(StoreError::Json)
            })
            .collect::<Result<Vec<_>, _>>()?;
        AccountSnapshot::new(users, mountpoints).map_err(StoreError::Account)
    }

    pub fn blacklist_rules(&self) -> Result<Vec<BlacklistRule>, BlacklistError> {
        let transaction = self.database.begin_read().map_err(storage_error)?;
        let table = transaction.open_table(BLACKLIST).map_err(storage_error)?;
        let mut rules = table
            .iter()
            .map_err(storage_error)?
            .map(|entry| {
                let (_, value) = entry.map_err(storage_error)?;
                serde_json::from_slice::<BlacklistRule>(value.value()).map_err(json_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
        rules.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(rules)
    }

    pub fn api_tokens(&self) -> Result<Vec<ApiToken>, StoreError> {
        let transaction = self
            .database
            .begin_read()
            .map_err(|error| StoreError::Database(error.to_string()))?;
        let table = transaction
            .open_table(API_TOKENS)
            .map_err(|error| StoreError::Database(error.to_string()))?;
        table
            .iter()
            .map_err(|error| StoreError::Database(error.to_string()))?
            .map(|entry| {
                let (_, value) = entry.map_err(|error| StoreError::Database(error.to_string()))?;
                serde_json::from_slice(value.value()).map_err(StoreError::Json)
            })
            .collect()
    }

    pub fn api_token(&self, token_id: &str) -> Result<Option<ApiToken>, StoreError> {
        let transaction = self
            .database
            .begin_read()
            .map_err(|error| StoreError::Database(error.to_string()))?;
        let table = transaction
            .open_table(API_TOKENS)
            .map_err(|error| StoreError::Database(error.to_string()))?;
        table
            .get(token_id)
            .map_err(|error| StoreError::Database(error.to_string()))?
            .map(|value| serde_json::from_slice(value.value()).map_err(StoreError::Json))
            .transpose()
    }

    pub fn put_api_token(&self, token: &ApiToken) -> Result<(), StoreError> {
        let transaction = self
            .database
            .begin_write()
            .map_err(|error| StoreError::Database(error.to_string()))?;
        {
            let mut table = transaction
                .open_table(API_TOKENS)
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let value = serde_json::to_vec(token)?;
            table
                .insert(token.token_id.as_str(), value.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?;
        }
        transaction
            .commit()
            .map_err(|error| StoreError::Database(error.to_string()))
    }

    pub fn delete_api_token(&self, token_id: &str) -> Result<bool, StoreError> {
        let transaction = self
            .database
            .begin_write()
            .map_err(|error| StoreError::Database(error.to_string()))?;
        let removed = {
            let mut tokens = transaction
                .open_table(API_TOKENS)
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let removed = tokens
                .remove(token_id)
                .map_err(|error| StoreError::Database(error.to_string()))?
                .is_some();
            let mut last_used = transaction
                .open_table(API_TOKEN_LAST_USED)
                .map_err(|error| StoreError::Database(error.to_string()))?;
            last_used
                .remove(token_id)
                .map_err(|error| StoreError::Database(error.to_string()))?;
            removed
        };
        transaction
            .commit()
            .map_err(|error| StoreError::Database(error.to_string()))?;
        Ok(removed)
    }

    pub fn api_token_last_used(&self, token_id: &str) -> Result<Option<u64>, StoreError> {
        let transaction = self
            .database
            .begin_read()
            .map_err(|error| StoreError::Database(error.to_string()))?;
        let table = transaction
            .open_table(API_TOKEN_LAST_USED)
            .map_err(|error| StoreError::Database(error.to_string()))?;
        table
            .get(token_id)
            .map_err(|error| StoreError::Database(error.to_string()))?
            .map(|value| {
                std::str::from_utf8(value.value())
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .ok_or_else(|| StoreError::Database("invalid API token timestamp".to_owned()))
            })
            .transpose()
    }

    pub fn put_api_token_last_used(
        &self,
        token_id: &str,
        timestamp: u64,
    ) -> Result<(), StoreError> {
        let transaction = self
            .database
            .begin_write()
            .map_err(|error| StoreError::Database(error.to_string()))?;
        {
            let mut table = transaction
                .open_table(API_TOKEN_LAST_USED)
                .map_err(|error| StoreError::Database(error.to_string()))?;
            table
                .insert(token_id, timestamp.to_string().as_bytes())
                .map_err(|error| StoreError::Database(error.to_string()))?;
        }
        transaction
            .commit()
            .map_err(|error| StoreError::Database(error.to_string()))
    }

    pub fn put_blacklist_rule(
        &self,
        rule: &BlacklistRule,
        expected_revision: Option<u64>,
    ) -> Result<u64, BlacklistError> {
        validate_rule(rule)?;
        let network = IpNetwork::parse(&rule.cidr).ok_or(BlacklistError::InvalidConfig)?;
        let transaction = self.database.begin_write().map_err(storage_error)?;
        {
            let mut table = transaction.open_table(BLACKLIST).map_err(storage_error)?;
            let rules = table
                .iter()
                .map_err(storage_error)?
                .map(|entry| {
                    let (_, value) = entry.map_err(storage_error)?;
                    serde_json::from_slice::<BlacklistRule>(value.value()).map_err(json_error)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if rules.iter().any(|existing| {
                existing.id != rule.id
                    && existing.enabled
                    && rule.enabled
                    && IpNetwork::parse(&existing.cidr).is_some_and(|existing| {
                        existing.normalized_key() == network.normalized_key()
                    })
            }) {
                return Err(BlacklistError::InvalidConfig);
            }
            let current = rules.iter().find(|existing| existing.id == rule.id);
            match (expected_revision, current) {
                (Some(expected), Some(current)) if current.mod_revision == expected => {}
                (Some(_), Some(_)) => return Err(BlacklistError::Conflict),
                (Some(_), None) => return Err(BlacklistError::NotFound),
                (None, Some(_)) => return Err(BlacklistError::Conflict),
                (None, None) => {}
            }
            let mut stored = rule.clone();
            stored.mod_revision = current
                .map(|current| current.mod_revision.saturating_add(1))
                .unwrap_or(1);
            if stored.updated_at.is_empty() {
                stored.updated_at = now_string();
            }
            let value = serde_json::to_vec(&stored).map_err(json_error)?;
            table
                .insert(stored.id.as_str(), value.as_slice())
                .map_err(storage_error)?;
            drop(table);
            transaction.commit().map_err(storage_error)?;
            Ok(stored.mod_revision)
        }
    }

    pub fn delete_blacklist_rule(
        &self,
        id: &str,
        expected_revision: u64,
    ) -> Result<(), BlacklistError> {
        let transaction = self.database.begin_write().map_err(storage_error)?;
        {
            let mut table = transaction.open_table(BLACKLIST).map_err(storage_error)?;
            let current = table.get(id).map_err(storage_error)?.map(|value| {
                serde_json::from_slice::<BlacklistRule>(value.value()).map_err(json_error)
            });
            let Some(current) = current else {
                return Err(BlacklistError::NotFound);
            };
            if current?.mod_revision != expected_revision {
                return Err(BlacklistError::Conflict);
            }
            table.remove(id).map_err(storage_error)?;
            drop(table);
            transaction.commit().map_err(storage_error)?;
        }
        Ok(())
    }
}

fn ensure_tables(database: &Database) -> Result<(), StoreError> {
    let transaction = database
        .begin_write()
        .map_err(|error| StoreError::Database(error.to_string()))?;
    {
        transaction
            .open_table(USERS)
            .map_err(|error| StoreError::Database(error.to_string()))?;
        transaction
            .open_table(MOUNTPOINTS)
            .map_err(|error| StoreError::Database(error.to_string()))?;
        transaction
            .open_table(BLACKLIST)
            .map_err(|error| StoreError::Database(error.to_string()))?;
        transaction
            .open_table(API_TOKENS)
            .map_err(|error| StoreError::Database(error.to_string()))?;
        transaction
            .open_table(API_TOKEN_LAST_USED)
            .map_err(|error| StoreError::Database(error.to_string()))?;
    }
    transaction
        .commit()
        .map_err(|error| StoreError::Database(error.to_string()))
}

fn storage_error(error: impl std::fmt::Display) -> BlacklistError {
    BlacklistError::Storage(error.to_string())
}

fn json_error(error: serde_json::Error) -> BlacklistError {
    BlacklistError::Json(error.to_string())
}

fn now_string() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "account database error: {error}"),
            Self::Json(error) => write!(formatter, "account JSON error: {error}"),
            Self::Account(error) => write!(formatter, "account snapshot error: {error}"),
            Self::Blacklist(error) => write!(formatter, "account blacklist error: {error}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<serde_json::Error> for StoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}
