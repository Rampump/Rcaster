use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct SessionLimitConfig {
    #[serde(default = "default_max_sessions")]
    pub default_max_sessions: i32,
    #[serde(default)]
    pub per_username: BTreeMap<String, i32>,
    #[serde(default = "default_max_ip_sessions")]
    pub default_max_ip_sessions: i32,
    #[serde(default)]
    pub per_ip: BTreeMap<String, i32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionKind {
    Client,
    Source,
}

#[derive(Clone, Copy, Debug)]
pub struct SessionUser<'a> {
    pub user_id: &'a str,
    pub username: &'a str,
}

#[derive(Clone, Copy, Debug)]
pub struct SessionRequest<'a> {
    pub user: Option<SessionUser<'a>>,
    pub kind: SessionKind,
    pub remote_ip: IpAddr,
}

#[derive(Clone, Debug)]
pub struct LocalSessionLimiter {
    inner: Arc<LimiterInner>,
}

#[derive(Debug)]
struct LimiterInner {
    user_counts: Mutex<HashMap<String, u32>>,
    ip_counts: Mutex<HashMap<IpAddr, u32>>,
    config: SessionLimitConfig,
    ip_rules: Vec<IpRule>,
}

pub struct SessionPermit {
    inner: Arc<LimiterInner>,
    user_id: Option<String>,
    remote_ip: Option<IpAddr>,
}

#[derive(Clone, Debug)]
pub struct ConnectionLimiter {
    active: Arc<AtomicUsize>,
    accepts: Arc<Mutex<AcceptBucket>>,
    max: usize,
}

pub struct ConnectionSlot {
    active: Arc<AtomicUsize>,
}

impl ConnectionLimiter {
    pub fn new(max: usize) -> Result<Self, SessionLimitError> {
        Self::with_accept_rate(max, 1000)
    }

    pub fn with_accept_rate(
        max: usize,
        accept_rate_per_second: u32,
    ) -> Result<Self, SessionLimitError> {
        if max == 0 || accept_rate_per_second == 0 {
            return Err(SessionLimitError::InvalidConfig);
        }
        Ok(Self {
            active: Arc::new(AtomicUsize::new(0)),
            accepts: Arc::new(Mutex::new(AcceptBucket {
                tokens: f64::from(accept_rate_per_second),
                capacity: f64::from(accept_rate_per_second),
                refill_per_second: f64::from(accept_rate_per_second),
                last_refill: Instant::now(),
            })),
            max,
        })
    }

    pub fn try_accept(&self) -> bool {
        self.try_accept_at(Instant::now())
    }

    fn try_accept_at(&self, now: Instant) -> bool {
        let mut bucket = self
            .accepts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * bucket.refill_per_second).min(bucket.capacity);
        bucket.last_refill = now;
        if bucket.tokens < 1.0 {
            return false;
        }
        bucket.tokens -= 1.0;
        true
    }

    pub fn acquire(&self) -> Option<ConnectionSlot> {
        let mut current = self.active.load(Ordering::Acquire);
        loop {
            if current >= self.max {
                return None;
            }
            match self.active.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(ConnectionSlot {
                        active: Arc::clone(&self.active),
                    });
                }
                Err(next) => current = next,
            }
        }
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
struct AcceptBucket {
    tokens: f64,
    capacity: f64,
    refill_per_second: f64,
    last_refill: Instant,
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionLimitError {
    InvalidConfig,
    Exceeded,
}

#[derive(Clone, Copy, Debug)]
struct IpRule {
    network: IpNetwork,
    quota: i32,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum IpNetwork {
    V4 { network: u32, prefix: u8 },
    V6 { network: u128, prefix: u8 },
}

impl Default for SessionLimitConfig {
    fn default() -> Self {
        Self {
            default_max_sessions: default_max_sessions(),
            per_username: BTreeMap::new(),
            default_max_ip_sessions: default_max_ip_sessions(),
            per_ip: BTreeMap::new(),
        }
    }
}

impl LocalSessionLimiter {
    pub fn new(config: SessionLimitConfig) -> Result<Self, SessionLimitError> {
        if config.default_max_sessions < -1
            || config.default_max_ip_sessions < -1
            || config
                .per_username
                .values()
                .chain(config.per_ip.values())
                .any(|quota| *quota < -1)
        {
            return Err(SessionLimitError::InvalidConfig);
        }

        let mut normalized = HashSet::new();
        let mut ip_rules = Vec::with_capacity(config.per_ip.len());
        for (rule, quota) in &config.per_ip {
            let network = IpNetwork::parse(rule).ok_or(SessionLimitError::InvalidConfig)?;
            if !normalized.insert(network.normalized_key()) {
                return Err(SessionLimitError::InvalidConfig);
            }
            ip_rules.push(IpRule {
                network,
                quota: *quota,
            });
        }

        Ok(Self {
            inner: Arc::new(LimiterInner {
                user_counts: Mutex::new(HashMap::new()),
                ip_counts: Mutex::new(HashMap::new()),
                config,
                ip_rules,
            }),
        })
    }

    pub fn is_ip_denied(&self, remote_ip: IpAddr) -> bool {
        self.inner.ip_quota(remote_ip) == 0
    }

    pub fn max_sessions(&self, username: &str) -> usize {
        let quota = self
            .inner
            .config
            .per_username
            .get(username)
            .copied()
            .unwrap_or(self.inner.config.default_max_sessions);
        if quota < 0 {
            usize::MAX
        } else {
            quota as usize
        }
    }

    pub fn max_ip_sessions(&self, remote_ip: IpAddr) -> Option<usize> {
        let quota = self.inner.ip_quota(remote_ip);
        (quota >= 0).then_some(quota as usize)
    }

    pub fn try_acquire(
        &self,
        request: SessionRequest<'_>,
    ) -> Result<SessionPermit, SessionLimitError> {
        self.try_acquire_with_ip_quota(request, None)
    }

    pub fn try_acquire_with_ip_quota(
        &self,
        request: SessionRequest<'_>,
        ip_quota_override: Option<i32>,
    ) -> Result<SessionPermit, SessionLimitError> {
        let user = request.user;
        let user_quota = user.map(|user| {
            self.inner
                .config
                .per_username
                .get(user.username)
                .copied()
                .unwrap_or(self.inner.config.default_max_sessions)
        });
        let ip_quota = (request.kind == SessionKind::Client)
            .then(|| ip_quota_override.unwrap_or_else(|| self.inner.ip_quota(request.remote_ip)));

        if user_quota.is_some_and(|quota| quota == 0) || ip_quota.is_some_and(|quota| quota == 0) {
            return Err(SessionLimitError::Exceeded);
        }

        let mut users = self.inner.user_counts();
        if let (Some(user), Some(quota)) = (user, user_quota)
            && quota > 0
        {
            let count = users.entry(user.user_id.to_owned()).or_default();
            if *count >= quota as u32 {
                return Err(SessionLimitError::Exceeded);
            }
        }
        let mut ips = self.inner.ip_counts();
        if let Some(quota) = ip_quota
            && quota > 0
        {
            let count = ips.entry(request.remote_ip).or_default();
            if *count >= quota as u32 {
                return Err(SessionLimitError::Exceeded);
            }
        }

        let user_id = user.filter(|_| user_quota != Some(-1)).map(|user| {
            *users.entry(user.user_id.to_owned()).or_default() += 1;
            user.user_id.to_owned()
        });
        let remote_ip = ip_quota.filter(|quota| *quota != -1).map(|_| {
            *ips.entry(request.remote_ip).or_default() += 1;
            request.remote_ip
        });

        Ok(SessionPermit {
            inner: Arc::clone(&self.inner),
            user_id,
            remote_ip,
        })
    }
}

impl Drop for SessionPermit {
    fn drop(&mut self) {
        if let Some(user_id) = &self.user_id {
            let mut counts = self.inner.user_counts();
            if let Some(count) = counts.get_mut(user_id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    counts.remove(user_id);
                }
            }
        }
        if let Some(remote_ip) = self.remote_ip {
            let mut counts = self.inner.ip_counts();
            if let Some(count) = counts.get_mut(&remote_ip) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    counts.remove(&remote_ip);
                }
            }
        }
    }
}

impl LimiterInner {
    fn user_counts(&self) -> MutexGuard<'_, HashMap<String, u32>> {
        self.user_counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn ip_counts(&self) -> MutexGuard<'_, HashMap<IpAddr, u32>> {
        self.ip_counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn ip_quota(&self, remote_ip: IpAddr) -> i32 {
        self.ip_rules
            .iter()
            .filter(|rule| rule.network.contains(remote_ip))
            .max_by_key(|rule| rule.network.prefix())
            .map(|rule| rule.quota)
            .unwrap_or(self.config.default_max_ip_sessions)
    }
}

impl IpNetwork {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        let (address, prefix) = value.split_once('/').unwrap_or((value, ""));
        let address = address.parse::<IpAddr>().ok()?;
        match address {
            IpAddr::V4(address) => {
                let prefix = if prefix.is_empty() {
                    32
                } else {
                    prefix.parse::<u8>().ok()?
                };
                if prefix > 32 {
                    return None;
                }
                let address = u32::from(address);
                let mask = prefix_mask_v4(prefix);
                Some(Self::V4 {
                    network: address & mask,
                    prefix,
                })
            }
            IpAddr::V6(address) => {
                let prefix = if prefix.is_empty() {
                    128
                } else {
                    prefix.parse::<u8>().ok()?
                };
                if prefix > 128 {
                    return None;
                }
                let address = u128::from(address);
                let mask = prefix_mask_v6(prefix);
                Some(Self::V6 {
                    network: address & mask,
                    prefix,
                })
            }
        }
    }

    pub(crate) fn contains(self, address: IpAddr) -> bool {
        match (self, address) {
            (Self::V4 { network, prefix }, IpAddr::V4(address)) => {
                u32::from(address) & prefix_mask_v4(prefix) == network
            }
            (Self::V6 { network, prefix }, IpAddr::V6(address)) => {
                u128::from(address) & prefix_mask_v6(prefix) == network
            }
            _ => false,
        }
    }

    pub(crate) fn prefix(self) -> u8 {
        match self {
            Self::V4 { prefix, .. } | Self::V6 { prefix, .. } => prefix,
        }
    }

    pub(crate) fn normalized_key(self) -> String {
        match self {
            Self::V4 { network, prefix } => {
                format!("{}/{}", Ipv4Addr::from(network), prefix)
            }
            Self::V6 { network, prefix } => {
                format!("{}/{}", Ipv6Addr::from(network), prefix)
            }
        }
    }
}

impl fmt::Display for SessionLimitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig => formatter.write_str("invalid session quota"),
            Self::Exceeded => formatter.write_str("session quota exceeded"),
        }
    }
}

impl std::error::Error for SessionLimitError {}

fn prefix_mask_v4(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    }
}

fn prefix_mask_v6(prefix: u8) -> u128 {
    if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - prefix)
    }
}

fn default_max_sessions() -> i32 {
    3
}

fn default_max_ip_sessions() -> i32 {
    16
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use std::net::{IpAddr, Ipv4Addr};

    use super::{
        ConnectionLimiter, LocalSessionLimiter, SessionKind, SessionLimitConfig, SessionRequest,
    };

    #[test]
    fn connection_limiter_releases_capacity() {
        let limiter = ConnectionLimiter::new(2).expect("connection limiter");
        let first = limiter.acquire().expect("first");
        let second = limiter.acquire().expect("second");
        assert!(limiter.acquire().is_none());
        assert_eq!(limiter.active(), 2);
        drop(first);
        assert_eq!(limiter.active(), 1);
        let replacement = limiter.acquire().expect("replacement");
        drop((second, replacement));
        assert_eq!(limiter.active(), 0);
    }

    #[test]
    fn connection_limiter_enforces_accept_rate_and_refills() {
        let limiter = ConnectionLimiter::with_accept_rate(2, 1).expect("connection limiter");
        let start = Instant::now();
        assert!(limiter.try_accept_at(start));
        assert!(!limiter.try_accept_at(start));
        assert!(limiter.try_accept_at(start + Duration::from_secs(2)));
    }

    #[test]
    fn standalone_blacklist_quota_override_denies_client_before_auth() {
        let limiter =
            LocalSessionLimiter::new(SessionLimitConfig::default()).expect("session limiter");
        assert!(
            limiter
                .try_acquire_with_ip_quota(
                    SessionRequest {
                        user: None,
                        kind: SessionKind::Client,
                        remote_ip: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
                    },
                    Some(0),
                )
                .is_err()
        );
    }
}
