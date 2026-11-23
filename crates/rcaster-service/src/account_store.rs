use std::{fmt, path::Path};

use rcaster_core::account::{AccountError, AccountSnapshot};
use redb::{Database, ReadableTable, TableDefinition};

const USERS: TableDefinition<&str, &[u8]> = TableDefinition::new("users");
const MOUNTPOINTS: TableDefinition<&str, &[u8]> = TableDefinition::new("mountpoints");

pub struct AccountStore {
    database: Database,
}

#[derive(Debug)]
pub enum StoreError {
    Database(String),
    Json(serde_json::Error),
    Account(AccountError),
}

impl AccountStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let database =
            Database::create(path).map_err(|error| StoreError::Database(error.to_string()))?;
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
        }
        transaction
            .commit()
            .map_err(|error| StoreError::Database(error.to_string()))?;
        Ok(Self { database })
    }

    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let database =
            Database::open(path).map_err(|error| StoreError::Database(error.to_string()))?;
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
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "account database error: {error}"),
            Self::Json(error) => write!(formatter, "account JSON error: {error}"),
            Self::Account(error) => write!(formatter, "account snapshot error: {error}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<serde_json::Error> for StoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}
