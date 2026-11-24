use std::time::{SystemTime, UNIX_EPOCH};

use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::{
    account_auth::{hash_password, rehash_password_if_needed, verify_password},
    account_directory::{AccountDirectory, AccountDirectoryError},
    auth::AuthConcurrencyLimiter,
};

pub const PUBLIC_SESSION_COOKIE: &str = "rcaster_web_session";
pub const PUBLIC_SESSION_TTL_S: i64 = 3600;
pub const MONITOR_TICKET_TTL_S: i64 = 60;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PublicSession {
    pub schema_version: u8,
    pub session_id: String,
    pub token_hash: String,
    pub user_id: String,
    pub username: String,
    pub auth_version: u64,
    pub csrf_token: String,
    pub created_at: u64,
    pub expires_at: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MonitorTicket {
    pub schema_version: u8,
    pub ticket_id: String,
    pub token_hash: String,
    pub user_id: String,
    pub username: String,
    pub auth_version: u64,
    pub session_id: String,
    pub mountpoint: String,
    pub client_ip: String,
    #[serde(default)]
    pub source_generation: Option<u64>,
    pub created_at: u64,
    pub expires_at: u64,
}

#[derive(Clone, Debug)]
pub struct PublicIdentity {
    pub user_id: String,
    pub username: String,
    pub auth_version: u64,
    pub session_id: String,
    pub csrf_token: String,
}

pub struct IssuedPublicSession {
    pub identity: PublicIdentity,
    pub cookie_value: String,
    pub expires_at: u64,
}

#[derive(Debug)]
pub enum PublicAuthError {
    Invalid,
    Unavailable,
    NotFound,
}

impl From<AccountDirectoryError> for PublicAuthError {
    fn from(error: AccountDirectoryError) -> Self {
        match error {
            AccountDirectoryError::NotFound => Self::NotFound,
            _ => Self::Unavailable,
        }
    }
}

pub async fn login(
    directory: &AccountDirectory,
    auth_limiter: &AuthConcurrencyLimiter,
    username: &str,
    password: &str,
) -> Result<IssuedPublicSession, PublicAuthError> {
    let user = directory
        .linear_user_by_username(username)
        .await?
        .ok_or(PublicAuthError::Invalid)?;
    if !user.enabled || !user.web_enabled {
        return Err(PublicAuthError::Invalid);
    }
    let user_hash = user.password_hash.clone();
    let supplied = password.to_owned();
    let _permit = auth_limiter
        .try_acquire()
        .map_err(|_| PublicAuthError::Unavailable)?;
    if !tokio::task::spawn_blocking(move || verify_password(&user_hash, &supplied))
        .await
        .map_err(|_| PublicAuthError::Unavailable)?
    {
        return Err(PublicAuthError::Invalid);
    }
    rehash_password_if_needed(directory, &user, password).await;
    let session_id = random_hex();
    let secret = random_hex();
    let csrf_token = random_hex();
    let token_hash = hash_secret(secret.clone()).await?;
    let now = unix_time();
    let expires_at = now.saturating_add(PUBLIC_SESSION_TTL_S as u64);
    let session = PublicSession {
        schema_version: 1,
        session_id: session_id.clone(),
        token_hash,
        user_id: user.user_id.clone(),
        username: user.username.clone(),
        auth_version: user.auth_version,
        csrf_token: csrf_token.clone(),
        created_at: now,
        expires_at,
    };
    let encoded = serde_json::to_vec(&session).map_err(|_| PublicAuthError::Unavailable)?;
    if directory.is_clustered() {
        let lease_id = directory.etcd_lease(PUBLIC_SESSION_TTL_S).await?;
        directory
            .etcd_put(
                &session_key(directory, &session_id)?,
                encoded,
                Some(lease_id),
            )
            .await?;
    } else {
        directory.local_public_session_put(&session_id, encoded, expires_at);
    }
    Ok(IssuedPublicSession {
        identity: PublicIdentity {
            user_id: user.user_id,
            username: user.username,
            auth_version: user.auth_version,
            session_id,
            csrf_token,
        },
        cookie_value: format!("{}.{}", session.session_id, secret),
        expires_at,
    })
}

pub async fn authenticate(
    directory: &AccountDirectory,
    cookie: &str,
) -> Result<PublicIdentity, PublicAuthError> {
    let (session_id, secret) = split_token(cookie).ok_or(PublicAuthError::Invalid)?;
    let stored = if directory.is_clustered() {
        directory
            .etcd_get(&session_key(directory, session_id)?)
            .await?
            .ok_or(PublicAuthError::Invalid)?
            .0
    } else {
        directory
            .local_public_session_get(session_id)
            .ok_or(PublicAuthError::Invalid)?
    };
    let session: PublicSession =
        serde_json::from_slice(&stored).map_err(|_| PublicAuthError::Invalid)?;
    if session.schema_version != 1 || session.expires_at <= unix_time() {
        return Err(PublicAuthError::Invalid);
    }
    verify_secret(session.token_hash.clone(), secret.to_owned()).await?;
    current_user(directory, &session.user_id, session.auth_version).await?;
    Ok(PublicIdentity {
        user_id: session.user_id,
        username: session.username,
        auth_version: session.auth_version,
        session_id: session.session_id,
        csrf_token: session.csrf_token,
    })
}

pub async fn logout(
    directory: &AccountDirectory,
    identity: &PublicIdentity,
) -> Result<(), PublicAuthError> {
    if directory.is_clustered() {
        directory
            .etcd_delete(&session_key(directory, &identity.session_id)?)
            .await?;
    } else {
        directory.local_public_session_delete(&identity.session_id);
    }
    Ok(())
}

pub async fn issue_ticket(
    directory: &AccountDirectory,
    identity: &PublicIdentity,
    mountpoint: &str,
    client_ip: &str,
    source_generation: Option<u64>,
) -> Result<String, PublicAuthError> {
    let ticket_id = random_hex();
    let secret = random_hex();
    let token_hash = hash_secret(secret.clone()).await?;
    let now = unix_time();
    let ticket = MonitorTicket {
        schema_version: 1,
        ticket_id: ticket_id.clone(),
        token_hash,
        user_id: identity.user_id.clone(),
        username: identity.username.clone(),
        auth_version: identity.auth_version,
        session_id: identity.session_id.clone(),
        mountpoint: mountpoint.to_owned(),
        client_ip: client_ip.to_owned(),
        source_generation,
        created_at: now,
        expires_at: now.saturating_add(MONITOR_TICKET_TTL_S as u64),
    };
    let encoded = serde_json::to_vec(&ticket).map_err(|_| PublicAuthError::Unavailable)?;
    if directory.is_clustered() {
        let lease_id = directory.etcd_lease(MONITOR_TICKET_TTL_S).await?;
        directory
            .etcd_put(&ticket_key(directory, &ticket_id)?, encoded, Some(lease_id))
            .await?;
    } else {
        directory.local_monitor_ticket_put(&ticket_id, encoded, ticket.expires_at);
    }
    Ok(format!("{ticket_id}.{secret}"))
}

pub async fn consume_ticket(
    directory: &AccountDirectory,
    ticket: &str,
    mountpoint: &str,
    client_ip: &str,
) -> Result<(PublicIdentity, Option<u64>), PublicAuthError> {
    let (ticket_id, secret) = split_token(ticket).ok_or(PublicAuthError::Invalid)?;
    let (stored, revision) = if directory.is_clustered() {
        let key = ticket_key(directory, ticket_id)?;
        let (stored, revision) = directory
            .etcd_get(&key)
            .await?
            .ok_or(PublicAuthError::Invalid)?;
        (stored, Some((key, revision)))
    } else {
        (
            directory
                .local_monitor_ticket_take(ticket_id)
                .ok_or(PublicAuthError::Invalid)?,
            None,
        )
    };
    let ticket: MonitorTicket =
        serde_json::from_slice(&stored).map_err(|_| PublicAuthError::Invalid)?;
    if ticket.schema_version != 1
        || ticket.expires_at <= unix_time()
        || ticket.mountpoint != mountpoint
        || ticket.client_ip != client_ip
    {
        return Err(PublicAuthError::Invalid);
    }
    verify_secret(ticket.token_hash.clone(), secret.to_owned()).await?;
    if let Some((key, revision)) = revision
        && !directory.etcd_delete_if_revision(&key, revision).await?
    {
        return Err(PublicAuthError::Invalid);
    }
    let user = current_user(directory, &ticket.user_id, ticket.auth_version).await?;
    Ok((
        PublicIdentity {
            user_id: user.user_id,
            username: user.username,
            auth_version: user.auth_version,
            session_id: ticket.session_id,
            csrf_token: String::new(),
        },
        ticket.source_generation,
    ))
}

pub async fn still_authorized(directory: &AccountDirectory, identity: &PublicIdentity) -> bool {
    current_user(directory, &identity.user_id, identity.auth_version)
        .await
        .is_ok()
}

async fn current_user(
    directory: &AccountDirectory,
    user_id: &str,
    auth_version: u64,
) -> Result<rcaster_core::account::User, PublicAuthError> {
    directory
        .linear_user_by_id(user_id)
        .await?
        .filter(|user| user.enabled && user.web_enabled && user.auth_version == auth_version)
        .ok_or(PublicAuthError::Invalid)
}

async fn verify_secret(hash: String, secret: String) -> Result<(), PublicAuthError> {
    if tokio::task::spawn_blocking(move || verify_password(&hash, &secret))
        .await
        .map_err(|_| PublicAuthError::Unavailable)?
    {
        Ok(())
    } else {
        Err(PublicAuthError::Invalid)
    }
}

async fn hash_secret(secret: String) -> Result<String, PublicAuthError> {
    tokio::task::spawn_blocking(move || hash_password(&secret))
        .await
        .map_err(|_| PublicAuthError::Unavailable)?
        .map_err(|_| PublicAuthError::Unavailable)
}

pub fn session_key(
    directory: &AccountDirectory,
    session_id: &str,
) -> Result<String, PublicAuthError> {
    if !valid_id(session_id) {
        return Err(PublicAuthError::Invalid);
    }
    Ok(format!(
        "/rcaster/{}/runtime/web_sessions/{session_id}",
        directory.cluster_id().map_err(PublicAuthError::from)?
    ))
}

fn ticket_key(directory: &AccountDirectory, ticket_id: &str) -> Result<String, PublicAuthError> {
    if !valid_id(ticket_id) {
        return Err(PublicAuthError::Invalid);
    }
    Ok(format!(
        "/rcaster/{}/runtime/monitor_tickets/{ticket_id}",
        directory.cluster_id().map_err(PublicAuthError::from)?
    ))
}

fn split_token(value: &str) -> Option<(&str, &str)> {
    let (id, secret) = value.split_once('.')?;
    (valid_id(id) && valid_id(secret)).then_some((id, secret))
}

fn valid_id(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn random_hex() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
