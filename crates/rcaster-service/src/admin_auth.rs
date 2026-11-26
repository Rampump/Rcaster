use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rand::Rng;
use rcaster_core::account::UserRole;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::{
    account_auth::{rehash_password_if_needed, verify_password},
    account_directory::{AccountDirectory, AccountDirectoryError},
    auth::AuthConcurrencyLimiter,
    cluster_runtime::SharedClusterRuntime,
};

pub const ADMIN_SESSION_TTL_S: i64 = 3600;
pub const ADMIN_SESSION_COOKIE: &str = "rcaster_admin_session";
const TOKEN_PREFIX: &str = "rcaster_";
const LOGIN_WINDOW: Duration = Duration::from_secs(60);
const LOGIN_ATTEMPTS_PER_WINDOW: u8 = 10;
const LOGIN_MAX_FAILURES: u8 = 5;
const LOGIN_LOCKOUT: Duration = Duration::from_secs(300);
const TOKEN_LAST_USED_UPDATE_S: u64 = 3600;
const TOKEN_LAST_USED_TTL_S: i64 = 31 * 86_400;
const MAX_LOGIN_LIMITER_ENTRIES: usize = 4096;

#[derive(Clone, Default)]
pub struct LoginLimiter {
    inner: Arc<Mutex<HashMap<String, LoginState>>>,
}

#[derive(Default)]
struct LoginState {
    window_started: Option<Instant>,
    attempts: u8,
    failures: u8,
    locked_until: Option<Instant>,
    last_seen: Option<Instant>,
}

impl LoginLimiter {
    pub async fn retry_after_cluster(
        &self,
        runtime: Option<&SharedClusterRuntime>,
        key: &str,
    ) -> Option<u64> {
        let local = self.retry_after(key);
        let shared = if let Some(runtime) = runtime {
            runtime.auth_failure_retry_after(key).await.ok().flatten()
        } else {
            None
        };
        local.or(shared)
    }

    pub async fn failure_cluster(
        &self,
        runtime: Option<&SharedClusterRuntime>,
        key: &str,
    ) -> Option<u64> {
        let local = self.failure(key);
        let shared = if let Some(runtime) = runtime {
            runtime.record_auth_failure(key).await.ok().flatten()
        } else {
            None
        };
        local.or(shared)
    }

    pub async fn success_cluster(&self, runtime: Option<&SharedClusterRuntime>, key: &str) {
        self.success(key);
        if let Some(runtime) = runtime {
            let _ = runtime.clear_auth_failures(key).await;
        }
    }

    pub fn retry_after(&self, key: &str) -> Option<u64> {
        let now = Instant::now();
        let mut states = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        prune_login_states(&mut states, now);
        let state = states.entry(key.to_owned()).or_default();
        state.last_seen = Some(now);
        if let Some(until) = state.locked_until {
            if until > now {
                return Some(until.duration_since(now).as_secs().max(1));
            }
            *state = LoginState::default();
        }
        if state
            .window_started
            .is_none_or(|started| now.duration_since(started) >= LOGIN_WINDOW)
        {
            state.window_started = Some(now);
            state.attempts = 0;
        }
        if state.attempts >= LOGIN_ATTEMPTS_PER_WINDOW {
            return Some(
                state
                    .window_started
                    .map(|started| {
                        LOGIN_WINDOW
                            .saturating_sub(now.duration_since(started))
                            .as_secs()
                            .max(1)
                    })
                    .unwrap_or(1),
            );
        }
        state.attempts = state.attempts.saturating_add(1);
        None
    }

    pub fn failure(&self, key: &str) -> Option<u64> {
        let now = Instant::now();
        let mut states = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        prune_login_states(&mut states, now);
        let state = states.entry(key.to_owned()).or_default();
        state.last_seen = Some(now);
        state.failures = state.failures.saturating_add(1);
        if state.failures >= LOGIN_MAX_FAILURES {
            state.locked_until = Some(now + LOGIN_LOCKOUT);
            Some(LOGIN_LOCKOUT.as_secs())
        } else {
            None
        }
    }

    pub fn success(&self, key: &str) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(key);
    }
}

fn prune_login_states(states: &mut HashMap<String, LoginState>, now: Instant) {
    if states.len() < MAX_LOGIN_LIMITER_ENTRIES {
        return;
    }
    states.retain(|_, state| {
        let expired_lock = state.locked_until.is_some_and(|until| until <= now);
        let expired_window = state
            .window_started
            .is_none_or(|started| now.duration_since(started) >= LOGIN_LOCKOUT);
        !(expired_lock && expired_window)
    });
    if states.len() >= MAX_LOGIN_LIMITER_ENTRIES
        && let Some(oldest) = states
            .iter()
            .min_by_key(|(_, state)| state.last_seen.unwrap_or(now - LOGIN_LOCKOUT))
            .map(|(key, _)| key.clone())
    {
        states.remove(&oldest);
    }
}

pub fn valid_login_username(username: &str) -> bool {
    !username.is_empty()
        && username.len() <= 128
        && username
            .bytes()
            .all(|byte| byte != b':' && byte >= b' ' && byte != 0x7f)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AdminSession {
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
pub struct ApiToken {
    pub schema_version: u8,
    pub token_id: String,
    pub name: String,
    pub token_hash: String,
    pub user_id: String,
    pub username: String,
    pub auth_version: u64,
    #[serde(default = "default_admin_scopes")]
    pub scopes: Vec<String>,
    pub created_at: u64,
    #[serde(default)]
    pub expires_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ApiTokenView {
    pub token_id: String,
    pub name: String,
    pub user_id: String,
    pub username: String,
    pub scopes: Vec<String>,
    pub created_at: u64,
    pub expires_at: Option<u64>,
    pub last_used_at: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdminAuthKind {
    Session,
    ApiToken,
}

#[derive(Clone, Debug)]
pub struct AdminIdentity {
    pub user_id: String,
    pub username: String,
    pub auth_version: u64,
    pub scopes: Vec<String>,
    pub expires_at: Option<u64>,
    pub kind: AdminAuthKind,
    pub session_id: Option<String>,
    pub csrf_token: Option<String>,
}

pub struct IssuedSession {
    pub identity: AdminIdentity,
    pub cookie_value: String,
    pub expires_at: u64,
}

#[derive(Debug)]
pub enum AdminAuthError {
    Invalid,
    Unavailable,
    Conflict,
    NotFound,
}

impl From<AccountDirectoryError> for AdminAuthError {
    fn from(error: AccountDirectoryError) -> Self {
        match error {
            AccountDirectoryError::NotFound => Self::NotFound,
            AccountDirectoryError::Conflict => Self::Conflict,
            _ => Self::Unavailable,
        }
    }
}

pub async fn login(
    directory: &AccountDirectory,
    auth_limiter: &AuthConcurrencyLimiter,
    username: &str,
    password: &str,
) -> Result<IssuedSession, AdminAuthError> {
    if !valid_login_username(username) {
        return Err(AdminAuthError::Invalid);
    }
    let user = directory
        .linear_user_by_username(username)
        .await?
        .ok_or(AdminAuthError::Invalid)?;
    if !user.enabled || !user.web_enabled || user.role != UserRole::Admin {
        return Err(AdminAuthError::Invalid);
    }
    let user_password_hash = user.password_hash.clone();
    let supplied_password = password.to_owned();
    let _permit = auth_limiter
        .try_acquire()
        .map_err(|_| AdminAuthError::Unavailable)?;
    if !tokio::task::spawn_blocking(move || {
        verify_password(&user_password_hash, &supplied_password)
    })
    .await
    .map_err(|_| AdminAuthError::Unavailable)?
    {
        return Err(AdminAuthError::Invalid);
    }
    rehash_password_if_needed(directory, &user, password).await;

    let session_id = random_hex();
    let secret = random_hex();
    let csrf_token = random_hex();
    let secret_hash = digest_secret(&secret);
    let now = unix_time();
    let expires_at = now.saturating_add(ADMIN_SESSION_TTL_S as u64);
    let session = AdminSession {
        schema_version: 1,
        session_id: session_id.clone(),
        token_hash: secret_hash,
        user_id: user.user_id.clone(),
        username: user.username.clone(),
        auth_version: user.auth_version,
        csrf_token: csrf_token.clone(),
        created_at: now,
        expires_at,
    };
    let session_value = serde_json::to_vec(&session).map_err(|_| AdminAuthError::Unavailable)?;
    if directory.is_clustered() {
        let lease_id = directory.etcd_lease(ADMIN_SESSION_TTL_S).await?;
        directory
            .etcd_put(
                &session_key(directory, &session_id)?,
                session_value,
                Some(lease_id),
            )
            .await?;
    } else {
        directory.local_admin_session_put(&session_id, session_value, expires_at);
    }
    Ok(IssuedSession {
        identity: AdminIdentity {
            user_id: user.user_id,
            username: user.username,
            auth_version: user.auth_version,
            scopes: default_admin_scopes(),
            expires_at: Some(expires_at),
            kind: AdminAuthKind::Session,
            session_id: Some(session_id),
            csrf_token: Some(csrf_token),
        },
        cookie_value: format!("{}.{}", session.session_id, secret),
        expires_at,
    })
}

pub async fn authenticate(
    directory: &AccountDirectory,
    bearer: Option<&str>,
    cookie: Option<&str>,
) -> Result<AdminIdentity, AdminAuthError> {
    if let Some(token) = bearer.and_then(|value| value.strip_prefix(TOKEN_PREFIX)) {
        return authenticate_api_token(directory, token).await;
    }
    if let Some(cookie) = cookie {
        return authenticate_session(directory, cookie).await;
    }
    Err(AdminAuthError::Invalid)
}

pub async fn logout(
    directory: &AccountDirectory,
    identity: &AdminIdentity,
) -> Result<(), AdminAuthError> {
    if let Some(session_id) = &identity.session_id {
        if directory.is_clustered() {
            directory
                .etcd_delete(&session_key(directory, session_id)?)
                .await?;
        } else {
            directory.local_admin_session_delete(session_id);
        }
    }
    Ok(())
}

pub async fn create_api_token(
    directory: &AccountDirectory,
    identity: &AdminIdentity,
    name: &str,
    scopes: Vec<String>,
    expires_in_s: Option<u64>,
) -> Result<(String, ApiTokenView), AdminAuthError> {
    if scopes.is_empty()
        || scopes.iter().any(|scope| {
            !matches!(
                scope.as_str(),
                "admin:read"
                    | "admin:write"
                    | "operations:write"
                    | "upgrade:write"
                    | "cluster:admin"
            )
        })
        || scopes
            .iter()
            .any(|scope| !identity.scopes.iter().any(|owned| owned == scope))
    {
        return Err(AdminAuthError::Invalid);
    }
    let token_id = random_hex();
    let secret = random_hex();
    let token_hash = digest_secret(&secret);
    let expires_at = expires_in_s.map(|seconds| unix_time().saturating_add(seconds));
    let token = ApiToken {
        schema_version: 1,
        token_id: token_id.clone(),
        name: name.to_owned(),
        token_hash,
        user_id: identity.user_id.clone(),
        username: identity.username.clone(),
        auth_version: identity.auth_version,
        scopes: scopes.clone(),
        created_at: unix_time(),
        expires_at,
    };
    if directory.is_clustered() {
        let lease_id = match expires_in_s {
            Some(seconds) => Some(directory.etcd_lease(seconds as i64).await?),
            None => None,
        };
        directory
            .etcd_put(
                &api_token_key(directory, &token_id)?,
                serde_json::to_vec(&token).map_err(|_| AdminAuthError::Unavailable)?,
                lease_id,
            )
            .await?;
    } else {
        directory.local_api_token_put(&token).await?;
    }
    Ok((
        format!("{TOKEN_PREFIX}{token_id}.{secret}"),
        ApiTokenView {
            token_id,
            name: token.name,
            user_id: token.user_id,
            username: token.username,
            scopes: token.scopes,
            created_at: token.created_at,
            expires_at: token.expires_at,
            last_used_at: None,
        },
    ))
}

pub async fn list_api_tokens(
    directory: &AccountDirectory,
) -> Result<Vec<ApiTokenView>, AdminAuthError> {
    if !directory.is_clustered() {
        let now = unix_time();
        directory.local_expired_api_tokens_delete(now).await?;
        let mut tokens = directory
            .local_api_tokens()
            .await?
            .into_iter()
            .filter(|(token, _)| token.expires_at.is_none_or(|expires_at| expires_at > now))
            .map(|(token, last_used_at)| ApiTokenView {
                token_id: token.token_id,
                name: token.name,
                user_id: token.user_id,
                username: token.username,
                scopes: token.scopes,
                created_at: token.created_at,
                expires_at: token.expires_at,
                last_used_at,
            })
            .collect::<Vec<_>>();
        tokens.sort_by(|left, right| left.token_id.cmp(&right.token_id));
        return Ok(tokens);
    }
    let prefix = api_token_prefix(directory)?;
    let last_used_prefix = api_token_last_used_prefix(directory)?;
    let last_used = directory
        .etcd_list(&last_used_prefix)
        .await?
        .into_iter()
        .filter_map(|(key, value, _)| {
            let token_id = key.strip_prefix(&last_used_prefix)?;
            let timestamp = std::str::from_utf8(&value).ok()?.parse::<u64>().ok()?;
            Some((token_id.to_owned(), timestamp))
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut tokens = directory
        .etcd_list(&prefix)
        .await?
        .into_iter()
        .filter_map(|(_, value, _)| serde_json::from_slice::<ApiToken>(&value).ok())
        .map(|token| ApiTokenView {
            token_id: token.token_id.clone(),
            name: token.name,
            user_id: token.user_id,
            username: token.username,
            scopes: token.scopes,
            created_at: token.created_at,
            expires_at: token.expires_at,
            last_used_at: last_used.get(&token.token_id).copied(),
        })
        .collect::<Vec<_>>();
    tokens.sort_by(|left, right| left.token_id.cmp(&right.token_id));
    Ok(tokens)
}

pub async fn delete_api_token(
    directory: &AccountDirectory,
    token_id: &str,
) -> Result<(), AdminAuthError> {
    if token_id.is_empty() || !valid_hex_id(token_id) {
        return Err(AdminAuthError::NotFound);
    }
    if !directory.is_clustered() {
        return if directory.local_api_token_delete(token_id).await? {
            Ok(())
        } else {
            Err(AdminAuthError::NotFound)
        };
    }
    let key = api_token_key(directory, token_id)?;
    if directory.etcd_get(&key).await?.is_none() {
        return Err(AdminAuthError::NotFound);
    }
    directory.etcd_delete(&key).await?;
    let _ = directory
        .etcd_delete(&api_token_last_used_key(directory, token_id)?)
        .await;
    Ok(())
}

pub async fn still_authorized(directory: &AccountDirectory, identity: &AdminIdentity) -> bool {
    if identity
        .expires_at
        .is_some_and(|expires_at| expires_at <= unix_time())
    {
        return false;
    }
    directory
        .linear_user_by_id(&identity.user_id)
        .await
        .ok()
        .flatten()
        .is_some_and(|user| {
            user.enabled
                && user.web_enabled
                && user.role == UserRole::Admin
                && user.auth_version == identity.auth_version
        })
}

fn default_admin_scopes() -> Vec<String> {
    vec![
        "admin:read".to_owned(),
        "admin:write".to_owned(),
        "operations:write".to_owned(),
        "upgrade:write".to_owned(),
        "cluster:admin".to_owned(),
    ]
}

async fn authenticate_session(
    directory: &AccountDirectory,
    cookie: &str,
) -> Result<AdminIdentity, AdminAuthError> {
    let (session_id, secret) = split_token(cookie).ok_or(AdminAuthError::Invalid)?;
    let stored = if directory.is_clustered() {
        directory
            .etcd_get(&session_key(directory, session_id)?)
            .await?
            .ok_or(AdminAuthError::Invalid)?
            .0
    } else {
        directory
            .local_admin_session_get(session_id)
            .ok_or(AdminAuthError::Invalid)?
    };
    let session: AdminSession =
        serde_json::from_slice(&stored).map_err(|_| AdminAuthError::Invalid)?;
    if session.schema_version != 1 || session.expires_at <= unix_time() {
        return Err(AdminAuthError::Invalid);
    }
    if !verify_secret_hash(session.token_hash, secret.to_owned()).await? {
        return Err(AdminAuthError::Invalid);
    }
    let user = current_admin(directory, &session.user_id, session.auth_version).await?;
    Ok(AdminIdentity {
        user_id: user.user_id,
        username: user.username,
        auth_version: user.auth_version,
        scopes: default_admin_scopes(),
        expires_at: Some(session.expires_at),
        kind: AdminAuthKind::Session,
        session_id: Some(session.session_id),
        csrf_token: Some(session.csrf_token),
    })
}

async fn authenticate_api_token(
    directory: &AccountDirectory,
    token: &str,
) -> Result<AdminIdentity, AdminAuthError> {
    let (token_id, secret) = split_token(token).ok_or(AdminAuthError::Invalid)?;
    let token = if directory.is_clustered() {
        let stored = directory
            .etcd_get(&api_token_key(directory, token_id)?)
            .await?
            .ok_or(AdminAuthError::Invalid)?
            .0;
        serde_json::from_slice(&stored).map_err(|_| AdminAuthError::Invalid)?
    } else {
        directory
            .local_api_token(token_id)
            .await?
            .ok_or(AdminAuthError::Invalid)?
    };
    if token
        .expires_at
        .is_some_and(|expires_at| expires_at <= unix_time())
    {
        if !directory.is_clustered() {
            let _ = directory.local_api_token_delete(&token.token_id).await;
        }
        return Err(AdminAuthError::Invalid);
    }
    if !verify_secret_hash(token.token_hash, secret.to_owned()).await? {
        return Err(AdminAuthError::Invalid);
    }
    let user = current_admin(directory, &token.user_id, token.auth_version).await?;
    let _ = touch_api_token_last_used(directory, &token.token_id).await;
    Ok(AdminIdentity {
        user_id: user.user_id,
        username: user.username,
        auth_version: user.auth_version,
        scopes: token.scopes,
        expires_at: token.expires_at,
        kind: AdminAuthKind::ApiToken,
        session_id: None,
        csrf_token: None,
    })
}

async fn current_admin(
    directory: &AccountDirectory,
    user_id: &str,
    auth_version: u64,
) -> Result<rcaster_core::account::User, AdminAuthError> {
    directory
        .linear_user_by_id(user_id)
        .await?
        .filter(|user| {
            user.enabled
                && user.web_enabled
                && user.role == UserRole::Admin
                && user.auth_version == auth_version
        })
        .ok_or(AdminAuthError::Invalid)
}

fn digest_secret(secret: &str) -> String {
    let digest = Sha256::digest(secret.as_bytes());
    format!(
        "sha256:{}",
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

async fn verify_secret_hash(expected: String, secret: String) -> Result<bool, AdminAuthError> {
    if let Some(expected) = expected.strip_prefix("sha256:") {
        let actual = digest_secret(&secret);
        let actual = actual.strip_prefix("sha256:").unwrap_or_default();
        return Ok(expected.len() == actual.len()
            && bool::from(expected.as_bytes().ct_eq(actual.as_bytes())));
    }
    tokio::task::spawn_blocking(move || verify_password(&expected, &secret))
        .await
        .map_err(|_| AdminAuthError::Unavailable)
}

fn session_key(directory: &AccountDirectory, session_id: &str) -> Result<String, AdminAuthError> {
    if !valid_hex_id(session_id) {
        return Err(AdminAuthError::Invalid);
    }
    Ok(format!(
        "/rcaster/{}/runtime/admin_sessions/{session_id}",
        directory.cluster_id().map_err(AdminAuthError::from)?
    ))
}

async fn touch_api_token_last_used(
    directory: &AccountDirectory,
    token_id: &str,
) -> Result<(), AdminAuthError> {
    let now = unix_time();
    if !directory.is_clustered() {
        if directory
            .local_api_token_last_used(token_id)
            .await?
            .is_some_and(|last_used| now.saturating_sub(last_used) < TOKEN_LAST_USED_UPDATE_S)
        {
            return Ok(());
        }
        directory
            .local_api_token_last_used_put(token_id, now)
            .await?;
        return Ok(());
    }
    let key = api_token_last_used_key(directory, token_id)?;
    if let Some((value, _)) = directory.etcd_get(&key).await?
        && std::str::from_utf8(&value)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|last_used| now.saturating_sub(last_used) < TOKEN_LAST_USED_UPDATE_S)
    {
        return Ok(());
    }
    let lease_id = directory.etcd_lease(TOKEN_LAST_USED_TTL_S).await?;
    directory
        .etcd_put(&key, now.to_string().into_bytes(), Some(lease_id))
        .await?;
    Ok(())
}

fn api_token_prefix(directory: &AccountDirectory) -> Result<String, AdminAuthError> {
    Ok(format!(
        "/rcaster/{}/config/api_tokens/",
        directory.cluster_id().map_err(AdminAuthError::from)?
    ))
}

fn api_token_last_used_prefix(directory: &AccountDirectory) -> Result<String, AdminAuthError> {
    Ok(format!(
        "/rcaster/{}/runtime/api-token-last-used/",
        directory.cluster_id().map_err(AdminAuthError::from)?
    ))
}

fn api_token_last_used_key(
    directory: &AccountDirectory,
    token_id: &str,
) -> Result<String, AdminAuthError> {
    Ok(format!(
        "{}{token_id}",
        api_token_last_used_prefix(directory)?
    ))
}

fn api_token_key(directory: &AccountDirectory, token_id: &str) -> Result<String, AdminAuthError> {
    Ok(format!("{}{token_id}", api_token_prefix(directory)?))
}

fn split_token(value: &str) -> Option<(&str, &str)> {
    let (token_id, secret) = value.split_once('.')?;
    (valid_hex_id(token_id) && valid_hex_id(secret)).then_some((token_id, secret))
}

fn valid_hex_id(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn random_hex() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill(&mut bytes);
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::{LoginLimiter, MAX_LOGIN_LIMITER_ENTRIES, digest_secret, verify_secret_hash};
    use crate::account_auth::hash_password;

    #[test]
    fn login_limiter_locks_repeated_failures_and_clears_on_success() {
        let limiter = LoginLimiter::default();
        for _ in 0..4 {
            assert!(limiter.retry_after("user:admin").is_none());
            assert!(limiter.failure("user:admin").is_none());
        }
        assert!(limiter.retry_after("user:admin").is_none());
        assert!(limiter.failure("user:admin").is_some());
        assert!(limiter.retry_after("user:admin").is_some());
        limiter.success("user:admin");
        assert!(limiter.retry_after("user:admin").is_none());
    }

    #[test]
    fn login_limiter_memory_is_capacity_bounded() {
        let limiter = LoginLimiter::default();
        for index in 0..MAX_LOGIN_LIMITER_ENTRIES + 500 {
            limiter.failure(&format!("user:random-{index}"));
        }
        let states = limiter
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(states.len() <= MAX_LOGIN_LIMITER_ENTRIES);
    }

    #[tokio::test]
    async fn high_entropy_secrets_use_sha256_with_legacy_argon2_fallback() {
        let secret = "0123456789abcdef0123456789abcdef";
        let digest = digest_secret(secret);
        assert!(digest.starts_with("sha256:"));
        assert!(
            verify_secret_hash(digest.clone(), secret.to_owned())
                .await
                .unwrap()
        );
        assert!(
            !verify_secret_hash(digest, "wrong".to_owned())
                .await
                .unwrap()
        );

        let legacy = hash_password(secret).unwrap();
        assert!(verify_secret_hash(legacy, secret.to_owned()).await.unwrap());
    }
}
