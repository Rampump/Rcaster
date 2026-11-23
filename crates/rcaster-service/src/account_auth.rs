use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier, password_hash::SaltString};
use rcaster_core::account::{AccountSnapshot, User};

use crate::{
    account_directory::AccountDirectory,
    auth::{
        AuthConcurrencyLimiter, AuthError, AuthorizedUser, ClientAuthorizer, ClientCredentials,
        SourceAuthorizer, SourceCredentials,
    },
};

#[derive(Clone)]
pub struct AccountAuthorizer {
    directory: AccountDirectory,
    auth_limiter: AuthConcurrencyLimiter,
}

impl AccountAuthorizer {
    pub fn new(accounts: AccountSnapshot) -> Self {
        Self::from_directory_with_limit(AccountDirectory::static_snapshot(accounts), 4)
            .expect("default auth concurrency")
    }

    pub fn from_directory(directory: AccountDirectory) -> Self {
        Self::from_directory_with_limit(directory, 4).expect("default auth concurrency")
    }

    pub fn from_directory_with_limit(
        directory: AccountDirectory,
        max_auth_concurrency: usize,
    ) -> Result<Self, &'static str> {
        if max_auth_concurrency == 0 {
            return Err("auth concurrency must be positive");
        }
        Ok(Self {
            directory,
            auth_limiter: AuthConcurrencyLimiter::new(max_auth_concurrency)?,
        })
    }

    pub fn from_directory_and_limiter(
        directory: AccountDirectory,
        auth_limiter: AuthConcurrencyLimiter,
    ) -> Self {
        Self {
            directory,
            auth_limiter,
        }
    }
}

impl SourceAuthorizer for AccountAuthorizer {
    fn authorize_source<'a>(
        &'a self,
        mountpoint: &'a str,
        credentials: SourceCredentials<'a>,
    ) -> impl std::future::Future<Output = Result<AuthorizedUser, AuthError>> + Send + 'a {
        let directory = self.directory.clone();
        let auth_limiter = self.auth_limiter.clone();
        let mountpoint = mountpoint.to_owned();
        let credentials = OwnedSourceCredentials::from(credentials);
        async move {
            if !directory.is_healthy() {
                return Err(AuthError::Unavailable);
            }
            let accounts = directory.snapshot();
            let _permit = auth_limiter.try_acquire()?;
            let rehash_target = source_rehash_target(&accounts, &mountpoint, &credentials);
            let mountpoint_for_auth = mountpoint.clone();
            let mut result = tokio::task::spawn_blocking(move || {
                authorize(&accounts, &mountpoint_for_auth, &credentials)
            })
            .await
            .map_err(|_| AuthError::Unavailable)??;
            if let Some(target) = rehash_target {
                rehash_target_if_needed(&directory, target).await;
            }
            refresh_authorized_user(&directory, &mountpoint, &mut result, true)?;
            Ok(result)
        }
    }
}

impl ClientAuthorizer for AccountAuthorizer {
    fn authorize_client<'a>(
        &'a self,
        mountpoint: &'a str,
        credentials: Option<ClientCredentials<'a>>,
    ) -> impl std::future::Future<Output = Result<AuthorizedUser, AuthError>> + Send + 'a {
        let directory = self.directory.clone();
        let auth_limiter = self.auth_limiter.clone();
        let mountpoint = mountpoint.to_owned();
        let credentials = credentials.map(OwnedClientCredentials::from);
        async move {
            if !directory.is_healthy() {
                return Err(AuthError::Unavailable);
            }
            let accounts = directory.snapshot();
            let _permit = auth_limiter.try_acquire()?;
            let rehash_target = client_rehash_target(&accounts, &credentials);
            let mountpoint_for_auth = mountpoint.clone();
            let mut result = tokio::task::spawn_blocking(move || {
                authorize_client(&accounts, &mountpoint_for_auth, credentials.as_ref())
            })
            .await
            .map_err(|_| AuthError::Unavailable)??;
            if let Some(target) = rehash_target {
                rehash_target_if_needed(&directory, target).await;
            }
            refresh_authorized_user(&directory, &mountpoint, &mut result, false)?;
            Ok(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AccountAuthorizer;
    use crate::{account_directory::AccountDirectory, auth::AuthError};
    use rcaster_core::account::AccountSnapshot;

    #[test]
    fn auth_concurrency_rejects_excess_work_without_queueing() {
        let authorizer = AccountAuthorizer::from_directory_with_limit(
            AccountDirectory::static_snapshot(AccountSnapshot::default()),
            1,
        )
        .expect("authorizer");
        let permit = authorizer.auth_limiter.try_acquire().expect("first auth");
        assert!(matches!(
            authorizer.auth_limiter.try_acquire(),
            Err(AuthError::Unavailable)
        ));
        drop(permit);
        let _permit = authorizer
            .auth_limiter
            .try_acquire()
            .expect("released auth");
    }
}

pub fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error> {
    let salt = SaltString::encode_b64(&rand::random::<[u8; 16]>())?;
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &salt)?
        .to_string())
}

pub fn validate_password_hash(hash: &str) -> Result<(), argon2::password_hash::Error> {
    PasswordHash::new(hash).map(|_| ())
}

#[derive(Clone)]
enum OwnedSourceCredentials {
    Ntrip1(String),
    Ntrip2 { username: String, password: String },
}

#[derive(Clone)]
enum OwnedClientCredentials {
    Basic { username: String, password: String },
}

enum RehashTarget {
    User {
        user: User,
        password: String,
    },
    Mountpoint {
        mountpoint: String,
        password_hash: String,
        password: String,
    },
}

fn source_rehash_target(
    accounts: &AccountSnapshot,
    mountpoint: &str,
    credentials: &OwnedSourceCredentials,
) -> Option<RehashTarget> {
    match credentials {
        OwnedSourceCredentials::Ntrip1(password) => Some(RehashTarget::Mountpoint {
            mountpoint: mountpoint.to_owned(),
            password_hash: accounts
                .mountpoint(mountpoint)?
                .ntrip1_upload_password_hash
                .clone()?,
            password: password.clone(),
        }),
        OwnedSourceCredentials::Ntrip2 { username, password } => Some(RehashTarget::User {
            user: accounts.user_by_username(username)?.clone(),
            password: password.clone(),
        }),
    }
}

fn client_rehash_target(
    accounts: &AccountSnapshot,
    credentials: &Option<OwnedClientCredentials>,
) -> Option<RehashTarget> {
    let OwnedClientCredentials::Basic { username, password } = credentials.as_ref()?;
    Some(RehashTarget::User {
        user: accounts.user_by_username(username)?.clone(),
        password: password.clone(),
    })
}

async fn rehash_target_if_needed(directory: &AccountDirectory, target: RehashTarget) {
    match target {
        RehashTarget::User { user, password } => {
            rehash_password_if_needed(directory, &user, &password).await;
        }
        RehashTarget::Mountpoint {
            mountpoint,
            password_hash,
            password,
        } => {
            rehash_mountpoint_password_if_needed(directory, &mountpoint, &password_hash, &password)
                .await;
        }
    }
}

impl From<ClientCredentials<'_>> for OwnedClientCredentials {
    fn from(credentials: ClientCredentials<'_>) -> Self {
        match credentials {
            ClientCredentials::Basic { username, password } => Self::Basic {
                username: username.to_owned(),
                password: password.to_owned(),
            },
        }
    }
}

impl From<SourceCredentials<'_>> for OwnedSourceCredentials {
    fn from(credentials: SourceCredentials<'_>) -> Self {
        match credentials {
            SourceCredentials::Ntrip1 { password } => Self::Ntrip1(password.to_owned()),
            SourceCredentials::Ntrip2 { username, password } => Self::Ntrip2 {
                username: username.to_owned(),
                password: password.to_owned(),
            },
        }
    }
}

fn authorize(
    accounts: &AccountSnapshot,
    mountpoint_name: &str,
    credentials: &OwnedSourceCredentials,
) -> Result<AuthorizedUser, AuthError> {
    let mountpoint = accounts
        .mountpoint(mountpoint_name)
        .filter(|mountpoint| mountpoint.enabled)
        .ok_or(AuthError::Unauthorized)?;
    let user = match credentials {
        OwnedSourceCredentials::Ntrip1(password) => {
            let hash = mountpoint
                .ntrip1_upload_password_hash
                .as_deref()
                .ok_or(AuthError::Unauthorized)?;
            if !verify_password(hash, password) {
                return Err(AuthError::Unauthorized);
            }
            accounts
                .user_by_id(&mountpoint.owner_user_id)
                .ok_or(AuthError::Unauthorized)?
        }
        OwnedSourceCredentials::Ntrip2 { username, password } => {
            let user = accounts
                .user_by_username(username)
                .filter(|user| user.enabled && user.ntrip_enabled)
                .ok_or(AuthError::Unauthorized)?;
            if user.user_id != mountpoint.owner_user_id
                || !verify_password(&user.password_hash, password)
            {
                return Err(AuthError::Unauthorized);
            }
            user
        }
    };
    if !user.enabled || !user.ntrip_enabled {
        return Err(AuthError::Unauthorized);
    }
    Ok(AuthorizedUser {
        user_id: user.user_id.clone(),
        username: user.username.clone(),
        auth_version: user.auth_version,
        user_mod_revision: user.mod_revision,
        mountpoint_mod_revision: mountpoint.mod_revision,
        source_expectation: mountpoint.source_expectation.clone(),
    })
}

fn authorize_client(
    accounts: &AccountSnapshot,
    mountpoint: &str,
    credentials: Option<&OwnedClientCredentials>,
) -> Result<AuthorizedUser, AuthError> {
    let mountpoint = accounts
        .mountpoint(mountpoint)
        .filter(|mountpoint| mountpoint.enabled)
        .ok_or(AuthError::Unauthorized)?;
    let Some(OwnedClientCredentials::Basic { username, password }) = credentials else {
        return Err(AuthError::Unauthorized);
    };
    let user = accounts
        .user_by_username(username)
        .filter(|user| user.enabled && user.ntrip_enabled)
        .ok_or(AuthError::Unauthorized)?;
    if !verify_password(&user.password_hash, password) {
        return Err(AuthError::Unauthorized);
    }
    Ok(AuthorizedUser {
        user_id: user.user_id.clone(),
        username: user.username.clone(),
        auth_version: user.auth_version,
        user_mod_revision: user.mod_revision,
        mountpoint_mod_revision: mountpoint.mod_revision,
        source_expectation: None,
    })
}

fn refresh_authorized_user(
    directory: &AccountDirectory,
    mountpoint: &str,
    authorized: &mut AuthorizedUser,
    require_owner: bool,
) -> Result<(), AuthError> {
    let accounts = directory.snapshot();
    let mount = accounts
        .mountpoint(mountpoint)
        .filter(|mountpoint| mountpoint.enabled)
        .ok_or(AuthError::Unauthorized)?;
    if require_owner && mount.owner_user_id != authorized.user_id {
        return Err(AuthError::Unauthorized);
    }
    let user = accounts
        .user_by_id(&authorized.user_id)
        .filter(|user| user.enabled && user.ntrip_enabled)
        .ok_or(AuthError::Unauthorized)?;
    authorized.auth_version = user.auth_version;
    authorized.user_mod_revision = user.mod_revision;
    authorized.mountpoint_mod_revision = mount.mod_revision;
    if require_owner {
        authorized.source_expectation = mount.source_expectation.clone();
    }
    Ok(())
}

pub fn verify_password(hash: &str, password: &str) -> bool {
    let Ok(hash) = PasswordHash::new(hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &hash)
        .is_ok()
}

pub fn password_needs_rehash(hash: &str) -> bool {
    let Ok(hash) = PasswordHash::new(hash) else {
        return true;
    };
    hash.algorithm.as_str() != "argon2id"
        || hash.params.get_decimal("m") != Some(argon2::Params::DEFAULT_M_COST)
        || hash.params.get_decimal("t") != Some(argon2::Params::DEFAULT_T_COST)
        || hash.params.get_decimal("p") != Some(argon2::Params::DEFAULT_P_COST)
        || hash.salt.as_ref().is_none_or(|salt| salt.len() < 16)
        || hash.hash.as_ref().is_none_or(|value| value.len() < 32)
}

pub async fn rehash_password_if_needed(
    directory: &AccountDirectory,
    verified_user: &User,
    password: &str,
) {
    if !directory.is_writable() || !password_needs_rehash(&verified_user.password_hash) {
        return;
    }
    let password = password.to_owned();
    let Ok(new_hash) = tokio::task::spawn_blocking(move || hash_password(&password)).await else {
        return;
    };
    let Ok(new_hash) = new_hash else {
        return;
    };
    let _ = directory
        .update_user_password_hash(
            &verified_user.user_id,
            &verified_user.password_hash,
            new_hash,
        )
        .await;
}

pub async fn rehash_mountpoint_password_if_needed(
    directory: &AccountDirectory,
    mountpoint_name: &str,
    verified_hash: &str,
    password: &str,
) {
    if !directory.is_writable() || !password_needs_rehash(verified_hash) {
        return;
    }
    let password = password.to_owned();
    let Ok(new_hash) = tokio::task::spawn_blocking(move || hash_password(&password)).await else {
        return;
    };
    let Ok(new_hash) = new_hash else {
        return;
    };
    let _ = directory
        .update_mountpoint_password_hash(mountpoint_name, verified_hash, new_hash)
        .await;
}
