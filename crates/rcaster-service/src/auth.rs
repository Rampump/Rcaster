#[derive(Clone, Copy, Debug)]
pub enum SourceCredentials<'a> {
    Ntrip1 {
        password: &'a str,
    },
    Ntrip2 {
        username: &'a str,
        password: &'a str,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum ClientCredentials<'a> {
    Basic {
        username: &'a str,
        password: &'a str,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizedUser {
    pub user_id: String,
    pub username: String,
    pub auth_version: u64,
    pub user_mod_revision: u64,
    pub mountpoint_mod_revision: u64,
    pub source_expectation: Option<SourceExpectation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthError {
    Unauthorized,
    Unavailable,
}

pub trait SourceAuthorizer: Send + Sync {
    fn authorize_source<'a>(
        &'a self,
        mountpoint: &'a str,
        credentials: SourceCredentials<'a>,
    ) -> impl std::future::Future<Output = Result<AuthorizedUser, AuthError>> + Send + 'a;
}

pub trait ClientAuthorizer: Send + Sync {
    fn authorize_client<'a>(
        &'a self,
        mountpoint: &'a str,
        credentials: Option<ClientCredentials<'a>>,
    ) -> impl std::future::Future<Output = Result<AuthorizedUser, AuthError>> + Send + 'a;
}
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct AuthConcurrencyLimiter {
    permits: Arc<Semaphore>,
}

impl AuthConcurrencyLimiter {
    pub fn new(max_concurrency: usize) -> Result<Self, &'static str> {
        if max_concurrency == 0 {
            return Err("auth concurrency must be positive");
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(max_concurrency)),
        })
    }

    pub fn try_acquire(&self) -> Result<OwnedSemaphorePermit, AuthError> {
        Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| AuthError::Unavailable)
    }
}

impl Default for AuthConcurrencyLimiter {
    fn default() -> Self {
        Self::new(4).expect("default auth concurrency")
    }
}
use rcaster_core::account::SourceExpectation;
