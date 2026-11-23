use std::fmt;

use rcaster_core::account::{AccountError, AccountSnapshot, Mountpoint, User};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AccountFile {
    pub users: Vec<User>,
    pub mountpoints: Vec<Mountpoint>,
}

#[derive(Debug)]
pub enum AccountFileError {
    Json(serde_json::Error),
    Account(AccountError),
}

impl AccountFile {
    pub fn from_slice(input: &[u8]) -> Result<Self, AccountFileError> {
        Ok(serde_json::from_slice(input)?)
    }

    pub fn into_snapshot(self) -> Result<AccountSnapshot, AccountFileError> {
        AccountSnapshot::new(self.users, self.mountpoints).map_err(AccountFileError::Account)
    }
}

impl fmt::Display for AccountFileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(error) => write!(formatter, "account file JSON error: {error}"),
            Self::Account(error) => write!(formatter, "account file error: {error}"),
        }
    }
}

impl std::error::Error for AccountFileError {}

impl From<serde_json::Error> for AccountFileError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}
