use std::{
    collections::{BTreeMap, HashSet},
    fmt,
};

use serde::{Deserialize, Serialize};

use crate::ntrip::validate_mountpoint;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UserRole {
    User,
    Admin,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct User {
    pub schema_version: u8,
    pub mod_revision: u64,
    pub user_id: String,
    pub username: String,
    pub password_hash: String,
    pub auth_version: u64,
    pub enabled: bool,
    pub ntrip_enabled: bool,
    pub web_enabled: bool,
    pub role: UserRole,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Mountpoint {
    pub schema_version: u8,
    pub mod_revision: u64,
    pub mountpoint: String,
    pub owner_user_id: String,
    pub enabled: bool,
    pub ntrip1_upload_password_hash: Option<String>,
    pub advertise: bool,
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_expectation: Option<SourceExpectation>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct SourceExpectation {
    pub formats: Vec<String>,
    #[serde(default)]
    pub msm_versions: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub struct AccountSnapshot {
    users_by_id: BTreeMap<String, User>,
    users_by_name: BTreeMap<String, String>,
    mountpoints: BTreeMap<String, Mountpoint>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccountError {
    UnsupportedSchema(u8),
    InvalidUser,
    InvalidMountpoint,
    DuplicateUser,
    DuplicateMountpoint,
    MissingOwner,
}

impl AccountSnapshot {
    pub fn new(
        users: impl IntoIterator<Item = User>,
        mountpoints: impl IntoIterator<Item = Mountpoint>,
    ) -> Result<Self, AccountError> {
        let mut users_by_id = BTreeMap::new();
        let mut users_by_name = BTreeMap::new();
        for user in users {
            validate_user(&user)?;
            if users_by_id.contains_key(&user.user_id) || users_by_name.contains_key(&user.username)
            {
                return Err(AccountError::DuplicateUser);
            }
            users_by_name.insert(user.username.clone(), user.user_id.clone());
            users_by_id.insert(user.user_id.clone(), user);
        }

        let mut mounts_by_name = BTreeMap::new();
        let mut seen = HashSet::new();
        for mountpoint in mountpoints {
            if mountpoint.schema_version != 1 {
                return Err(AccountError::UnsupportedSchema(mountpoint.schema_version));
            }
            validate_mountpoint(&mountpoint.mountpoint)
                .map_err(|_| AccountError::InvalidMountpoint)?;
            validate_source_expectation(mountpoint.source_expectation.as_ref())?;
            if !seen.insert(mountpoint.mountpoint.clone()) {
                return Err(AccountError::DuplicateMountpoint);
            }
            if !users_by_id.contains_key(&mountpoint.owner_user_id) {
                return Err(AccountError::MissingOwner);
            }
            mounts_by_name.insert(mountpoint.mountpoint.clone(), mountpoint);
        }

        Ok(Self {
            users_by_id,
            users_by_name,
            mountpoints: mounts_by_name,
        })
    }

    pub fn user_by_id(&self, user_id: &str) -> Option<&User> {
        self.users_by_id.get(user_id)
    }

    pub fn user_by_username(&self, username: &str) -> Option<&User> {
        self.users_by_name
            .get(username)
            .and_then(|user_id| self.users_by_id.get(user_id))
    }

    pub fn mountpoint(&self, mountpoint: &str) -> Option<&Mountpoint> {
        self.mountpoints.get(mountpoint)
    }

    pub fn users(&self) -> impl Iterator<Item = &User> {
        self.users_by_id.values()
    }

    pub fn mountpoints(&self) -> impl Iterator<Item = &Mountpoint> {
        self.mountpoints.values()
    }
}

impl fmt::Display for AccountError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchema(version) => {
                write!(formatter, "unsupported account schema {version}")
            }
            Self::InvalidUser => formatter.write_str("invalid user record"),
            Self::InvalidMountpoint => formatter.write_str("invalid mountpoint record"),
            Self::DuplicateUser => formatter.write_str("duplicate user ID or username"),
            Self::DuplicateMountpoint => formatter.write_str("duplicate mountpoint"),
            Self::MissingOwner => formatter.write_str("mountpoint owner does not exist"),
        }
    }
}

impl std::error::Error for AccountError {}

fn validate_user(user: &User) -> Result<(), AccountError> {
    if user.schema_version != 1 {
        return Err(AccountError::UnsupportedSchema(user.schema_version));
    }
    if !valid_identifier(&user.user_id)
        || user.username.is_empty()
        || user.username.len() > 128
        || user
            .username
            .bytes()
            .any(|byte| byte == b':' || byte == 0 || byte < b' ' || byte == 0x7f)
        || user.password_hash.is_empty()
    {
        return Err(AccountError::InvalidUser);
    }
    Ok(())
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn validate_source_expectation(
    expectation: Option<&SourceExpectation>,
) -> Result<(), AccountError> {
    let Some(expectation) = expectation else {
        return Ok(());
    };
    if expectation.formats.is_empty()
        || expectation.formats.iter().any(|format| {
            !matches!(
                format.as_str(),
                "rtcm3" | "rtcm2" | "binex" | "cmr" | "nmea"
            )
        })
        || expectation
            .msm_versions
            .iter()
            .any(|version| !(1..=7).contains(version))
    {
        return Err(AccountError::InvalidMountpoint);
    }
    Ok(())
}
