use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use etcd_client::{Client, Compare, CompareOp, EventType, GetOptions, Txn, TxnOp, WatchOptions};
use rcaster_core::account::{AccountError, AccountSnapshot, Mountpoint, User};
use tokio::{
    sync::{Mutex, oneshot},
    task::JoinHandle,
};

use crate::{
    ETCD_WATCH_IDLE_TIMEOUT,
    account_store::{AccountStore, StoreError},
    blacklist::{BlacklistError, BlacklistRule},
    etcd_watch_metrics::{EtcdWatchMetrics, WatchLane},
};

#[derive(Clone)]
pub struct AccountDirectory {
    snapshot: Arc<RwLock<AccountSnapshot>>,
    healthy: Arc<AtomicBool>,
    client: Option<Arc<Mutex<Client>>>,
    cluster_id: Option<String>,
    store: Option<Arc<Mutex<AccountStore>>>,
    blacklist: Arc<RwLock<Vec<BlacklistRule>>>,
    local_admin_sessions: Arc<std::sync::Mutex<HashMap<String, LocalAdminSession>>>,
    local_public_sessions: Arc<std::sync::Mutex<HashMap<String, LocalAdminSession>>>,
    local_monitor_tickets: Arc<std::sync::Mutex<HashMap<String, LocalAdminSession>>>,
    watch_metrics: Arc<EtcdWatchMetrics>,
}

#[derive(Clone)]
struct LocalAdminSession {
    value: Vec<u8>,
    expires_at: u64,
}

pub struct AccountWatcher {
    directory: AccountDirectory,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

#[derive(Debug)]
pub enum AccountDirectoryError {
    InvalidCluster,
    InvalidRecord,
    Conflict,
    NotFound,
    InUse,
    Etcd(etcd_client::Error),
    Json(serde_json::Error),
    Account(AccountError),
    Store(StoreError),
    Blacklist(BlacklistError),
}

impl fmt::Display for AccountDirectoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCluster => formatter.write_str("invalid cluster ID"),
            Self::InvalidRecord => formatter.write_str("invalid account record"),
            Self::Conflict => formatter.write_str("account revision conflict"),
            Self::NotFound => formatter.write_str("account record not found"),
            Self::InUse => formatter.write_str("account record is still in use"),
            Self::Etcd(error) => write!(formatter, "etcd error: {error}"),
            Self::Json(error) => write!(formatter, "account JSON error: {error}"),
            Self::Account(error) => write!(formatter, "account snapshot error: {error}"),
            Self::Store(error) => write!(formatter, "account store error: {error}"),
            Self::Blacklist(error) => write!(formatter, "blacklist error: {error}"),
        }
    }
}

impl std::error::Error for AccountDirectoryError {}

impl From<etcd_client::Error> for AccountDirectoryError {
    fn from(error: etcd_client::Error) -> Self {
        Self::Etcd(error)
    }
}

impl From<serde_json::Error> for AccountDirectoryError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<AccountError> for AccountDirectoryError {
    fn from(error: AccountError) -> Self {
        Self::Account(error)
    }
}

impl From<StoreError> for AccountDirectoryError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<BlacklistError> for AccountDirectoryError {
    fn from(error: BlacklistError) -> Self {
        Self::Blacklist(error)
    }
}

impl AccountDirectory {
    pub fn static_snapshot(snapshot: AccountSnapshot) -> Self {
        Self {
            snapshot: Arc::new(RwLock::new(snapshot)),
            healthy: Arc::new(AtomicBool::new(true)),
            client: None,
            cluster_id: None,
            store: None,
            blacklist: Arc::new(RwLock::new(Vec::new())),
            local_admin_sessions: Arc::new(std::sync::Mutex::new(HashMap::new())),
            local_public_sessions: Arc::new(std::sync::Mutex::new(HashMap::new())),
            local_monitor_tickets: Arc::new(std::sync::Mutex::new(HashMap::new())),
            watch_metrics: Arc::new(EtcdWatchMetrics::default()),
        }
    }

    pub fn with_store(
        snapshot: AccountSnapshot,
        store: AccountStore,
    ) -> Result<Self, AccountDirectoryError> {
        let blacklist = store.blacklist_rules()?;
        Ok(Self {
            snapshot: Arc::new(RwLock::new(snapshot)),
            healthy: Arc::new(AtomicBool::new(true)),
            client: None,
            cluster_id: None,
            store: Some(Arc::new(Mutex::new(store))),
            blacklist: Arc::new(RwLock::new(blacklist)),
            local_admin_sessions: Arc::new(std::sync::Mutex::new(HashMap::new())),
            local_public_sessions: Arc::new(std::sync::Mutex::new(HashMap::new())),
            local_monitor_tickets: Arc::new(std::sync::Mutex::new(HashMap::new())),
            watch_metrics: Arc::new(EtcdWatchMetrics::default()),
        })
    }

    pub fn snapshot(&self) -> AccountSnapshot {
        self.snapshot
            .read()
            .map(|snapshot| snapshot.clone())
            .unwrap_or_default()
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub fn is_writable(&self) -> bool {
        self.client.is_some() || self.store.is_some()
    }

    pub(crate) fn is_clustered(&self) -> bool {
        self.client.is_some()
    }

    pub(crate) fn local_admin_session_put(
        &self,
        session_id: &str,
        value: Vec<u8>,
        expires_at: u64,
    ) {
        self.local_admin_sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                session_id.to_owned(),
                LocalAdminSession { value, expires_at },
            );
    }

    pub(crate) fn local_admin_session_get(&self, session_id: &str) -> Option<Vec<u8>> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut sessions = self
            .local_admin_sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if sessions
            .get(session_id)
            .is_some_and(|session| session.expires_at <= now)
        {
            sessions.remove(session_id);
        }
        sessions
            .get(session_id)
            .map(|session| session.value.clone())
    }

    pub(crate) fn local_admin_session_delete(&self, session_id: &str) {
        self.local_admin_sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
    }

    pub(crate) fn local_public_session_put(
        &self,
        session_id: &str,
        value: Vec<u8>,
        expires_at: u64,
    ) {
        self.local_public_sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                session_id.to_owned(),
                LocalAdminSession { value, expires_at },
            );
    }

    pub(crate) fn local_public_session_get(&self, session_id: &str) -> Option<Vec<u8>> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut sessions = self
            .local_public_sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if sessions
            .get(session_id)
            .is_some_and(|session| session.expires_at <= now)
        {
            sessions.remove(session_id);
        }
        sessions
            .get(session_id)
            .map(|session| session.value.clone())
    }

    pub(crate) fn local_public_session_delete(&self, session_id: &str) {
        self.local_public_sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
    }

    pub(crate) fn local_monitor_ticket_put(
        &self,
        ticket_id: &str,
        value: Vec<u8>,
        expires_at: u64,
    ) {
        self.local_monitor_tickets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                ticket_id.to_owned(),
                LocalAdminSession { value, expires_at },
            );
    }

    pub(crate) fn local_monitor_ticket_take(&self, ticket_id: &str) -> Option<Vec<u8>> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut tickets = self
            .local_monitor_tickets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        tickets.retain(|_, ticket| ticket.expires_at > now);
        tickets.remove(ticket_id).map(|ticket| ticket.value)
    }

    pub fn blacklist_rules(&self) -> Vec<BlacklistRule> {
        self.blacklist
            .read()
            .map(|rules| rules.clone())
            .unwrap_or_default()
    }

    pub fn blacklist_quota_for_ip(&self, address: std::net::IpAddr) -> Option<i32> {
        let rules = self.blacklist.read().ok()?;
        rules
            .iter()
            .filter(|rule| rule.enabled)
            .filter_map(|rule| {
                let network = crate::session_limit::IpNetwork::parse(&rule.cidr)?;
                network
                    .contains(address)
                    .then_some((network.prefix(), rule.quota))
            })
            .max_by_key(|(prefix, _)| *prefix)
            .map(|(_, quota)| quota)
    }

    pub async fn put_blacklist_rule(
        &self,
        rule: &BlacklistRule,
        expected_revision: Option<u64>,
    ) -> Result<u64, AccountDirectoryError> {
        let Some(store) = &self.store else {
            return Err(AccountDirectoryError::InvalidCluster);
        };
        let store = store.lock().await;
        let revision = store.put_blacklist_rule(rule, expected_revision)?;
        replace_blacklist(&self.blacklist, store.blacklist_rules()?);
        Ok(revision)
    }

    pub async fn delete_blacklist_rule(
        &self,
        id: &str,
        expected_revision: u64,
    ) -> Result<(), AccountDirectoryError> {
        let Some(store) = &self.store else {
            return Err(AccountDirectoryError::InvalidCluster);
        };
        let store = store.lock().await;
        store.delete_blacklist_rule(id, expected_revision)?;
        replace_blacklist(&self.blacklist, store.blacklist_rules()?);
        Ok(())
    }

    pub async fn refresh(&self) -> Result<AccountSnapshot, AccountDirectoryError> {
        let (Some(client), Some(cluster_id)) = (&self.client, &self.cluster_id) else {
            return Ok(self.snapshot());
        };
        let snapshot = load_accounts(&mut *client.lock().await, cluster_id).await?;
        replace_snapshot(&self.snapshot, &snapshot);
        Ok(snapshot)
    }

    pub(crate) async fn linear_user_by_id(
        &self,
        user_id: &str,
    ) -> Result<Option<User>, AccountDirectoryError> {
        let (Some(client), Some(cluster_id)) = (&self.client, &self.cluster_id) else {
            return Ok(self.snapshot().user_by_id(user_id).cloned());
        };
        let key = user_key(cluster_id, user_id)?;
        let Some(value) = client
            .lock()
            .await
            .get(key, None)
            .await?
            .kvs()
            .first()
            .cloned()
        else {
            return Ok(None);
        };
        let mut user: User = serde_json::from_slice(value.value())?;
        user.mod_revision = value.mod_revision() as u64;
        Ok(Some(user))
    }

    pub(crate) async fn linear_user_by_username(
        &self,
        username: &str,
    ) -> Result<Option<User>, AccountDirectoryError> {
        let (Some(client), Some(cluster_id)) = (&self.client, &self.cluster_id) else {
            return Ok(self.snapshot().user_by_username(username).cloned());
        };
        let index_key = username_key(cluster_id, username)?;
        let Some(value) = client
            .lock()
            .await
            .get(index_key, None)
            .await?
            .kvs()
            .first()
            .cloned()
        else {
            return Ok(load_accounts(&mut *client.lock().await, cluster_id)
                .await?
                .user_by_username(username)
                .cloned());
        };
        let Ok(user_id) = std::str::from_utf8(value.value()) else {
            return Err(AccountDirectoryError::InvalidRecord);
        };
        self.linear_user_by_id(user_id).await
    }

    pub(crate) async fn etcd_get(
        &self,
        key: &str,
    ) -> Result<Option<(Vec<u8>, i64)>, AccountDirectoryError> {
        let (client, _) = self.writable()?;
        Ok(client
            .lock()
            .await
            .get(key, None)
            .await?
            .kvs()
            .first()
            .map(|value| (value.value().to_vec(), value.mod_revision())))
    }

    pub(crate) async fn etcd_list(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>, i64)>, AccountDirectoryError> {
        let (client, _) = self.writable()?;
        Ok(client
            .lock()
            .await
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await?
            .kvs()
            .iter()
            .map(|value| {
                (
                    String::from_utf8_lossy(value.key()).into_owned(),
                    value.value().to_vec(),
                    value.mod_revision(),
                )
            })
            .collect())
    }

    pub(crate) async fn etcd_lease(&self, ttl_s: i64) -> Result<i64, AccountDirectoryError> {
        let (client, _) = self.writable()?;
        Ok(client.lock().await.lease_grant(ttl_s, None).await?.id())
    }

    pub(crate) async fn etcd_put(
        &self,
        key: &str,
        value: Vec<u8>,
        lease_id: Option<i64>,
    ) -> Result<u64, AccountDirectoryError> {
        let (client, _) = self.writable()?;
        let options = lease_id.map(|lease_id| etcd_client::PutOptions::new().with_lease(lease_id));
        let response = client.lock().await.put(key, value, options).await?;
        response
            .header()
            .map(|header| header.revision() as u64)
            .ok_or(AccountDirectoryError::InvalidRecord)
    }

    pub(crate) async fn etcd_keep_alive_once(
        &self,
        lease_id: i64,
    ) -> Result<(), AccountDirectoryError> {
        let (client, _) = self.writable()?;
        let (mut keeper, _stream) = client.lock().await.lease_keep_alive(lease_id).await?;
        keeper.keep_alive().await?;
        Ok(())
    }

    pub(crate) async fn etcd_revoke_lease(
        &self,
        lease_id: i64,
    ) -> Result<(), AccountDirectoryError> {
        let (client, _) = self.writable()?;
        client.lock().await.lease_revoke(lease_id).await?;
        Ok(())
    }

    pub(crate) async fn etcd_put_if_revision(
        &self,
        key: &str,
        value: Vec<u8>,
        revision: i64,
    ) -> Result<bool, AccountDirectoryError> {
        let (client, _) = self.writable()?;
        Ok(client
            .lock()
            .await
            .txn(
                Txn::new()
                    .when([Compare::mod_revision(key, CompareOp::Equal, revision)])
                    .and_then([TxnOp::put(key, value, None)]),
            )
            .await?
            .succeeded())
    }

    pub async fn update_user_password_hash(
        &self,
        user_id: &str,
        verified_hash: &str,
        new_hash: String,
    ) -> Result<bool, AccountDirectoryError> {
        if let Some(store) = &self.store {
            let store = store.lock().await;
            let snapshot = store.load()?;
            let Some(current) = snapshot.user_by_id(user_id).cloned() else {
                return Ok(false);
            };
            if current.password_hash != verified_hash {
                return Ok(false);
            }
            let mut updated = current.clone();
            updated.password_hash = new_hash;
            updated.mod_revision = current.mod_revision.saturating_add(1);
            updated.updated_at = now_string();
            let users = snapshot
                .users()
                .map(|user| {
                    if user.user_id == user_id {
                        updated.clone()
                    } else {
                        user.clone()
                    }
                })
                .collect::<Vec<_>>();
            let next = AccountSnapshot::new(users, snapshot.mountpoints().cloned())?;
            store.replace(&next)?;
            replace_snapshot(&self.snapshot, &next);
            return Ok(true);
        }
        let Some(current) = self.linear_user_by_id(user_id).await? else {
            return Ok(false);
        };
        if current.password_hash != verified_hash {
            return Ok(false);
        }
        let mut updated = current.clone();
        updated.password_hash = new_hash;
        updated.mod_revision = current.mod_revision.saturating_add(1);
        updated.updated_at = now_string();
        self.upsert_user(updated, Some(current.mod_revision))
            .await?;
        Ok(true)
    }

    pub async fn update_mountpoint_password_hash(
        &self,
        mountpoint_name: &str,
        verified_hash: &str,
        new_hash: String,
    ) -> Result<bool, AccountDirectoryError> {
        if let Some(store) = &self.store {
            let store = store.lock().await;
            let snapshot = store.load()?;
            let Some(current) = snapshot.mountpoint(mountpoint_name).cloned() else {
                return Ok(false);
            };
            if current.ntrip1_upload_password_hash.as_deref() != Some(verified_hash) {
                return Ok(false);
            }
            let mut updated = current.clone();
            updated.ntrip1_upload_password_hash = Some(new_hash);
            updated.mod_revision = current.mod_revision.saturating_add(1);
            updated.updated_at = now_string();
            let mountpoints = snapshot
                .mountpoints()
                .map(|mountpoint| {
                    if mountpoint.mountpoint == mountpoint_name {
                        updated.clone()
                    } else {
                        mountpoint.clone()
                    }
                })
                .collect::<Vec<_>>();
            let next = AccountSnapshot::new(snapshot.users().cloned(), mountpoints)?;
            store.replace(&next)?;
            replace_snapshot(&self.snapshot, &next);
            return Ok(true);
        }
        let snapshot = self.snapshot();
        let Some(current) = snapshot.mountpoint(mountpoint_name).cloned() else {
            return Ok(false);
        };
        if current.ntrip1_upload_password_hash.as_deref() != Some(verified_hash) {
            return Ok(false);
        }
        let mut updated = current.clone();
        updated.ntrip1_upload_password_hash = Some(new_hash);
        updated.mod_revision = current.mod_revision.saturating_add(1);
        updated.updated_at = now_string();
        self.upsert_mountpoint(updated, Some(current.mod_revision))
            .await?;
        Ok(true)
    }

    pub(crate) async fn etcd_delete(&self, key: &str) -> Result<(), AccountDirectoryError> {
        let (client, _) = self.writable()?;
        client.lock().await.delete(key, None).await?;
        Ok(())
    }

    pub(crate) async fn etcd_delete_if_revision(
        &self,
        key: &str,
        revision: i64,
    ) -> Result<bool, AccountDirectoryError> {
        let (client, _) = self.writable()?;
        Ok(client
            .lock()
            .await
            .txn(
                Txn::new()
                    .when([Compare::mod_revision(key, CompareOp::Equal, revision)])
                    .and_then([TxnOp::delete(key, None)]),
            )
            .await?
            .succeeded())
    }

    pub(crate) fn cluster_id(&self) -> Result<&str, AccountDirectoryError> {
        self.cluster_id
            .as_deref()
            .ok_or(AccountDirectoryError::InvalidCluster)
    }

    pub async fn upsert_user(
        &self,
        user: User,
        expected_revision: Option<u64>,
    ) -> Result<u64, AccountDirectoryError> {
        AccountSnapshot::new([user.clone()], [])?;
        let (client, cluster_id) = self.writable()?;
        let current = self.snapshot().user_by_id(&user.user_id).cloned();
        if let Some(current) = &current
            && current.username != user.username
        {
            return Err(AccountDirectoryError::InvalidRecord);
        }
        let user_key = user_key(cluster_id, &user.user_id)?;
        let username_key = username_key(cluster_id, &user.username)?;
        let compare = match expected_revision {
            Some(revision) => {
                Compare::mod_revision(user_key.clone(), CompareOp::Equal, revision as i64)
            }
            None => Compare::create_revision(user_key.clone(), CompareOp::Equal, 0),
        };
        let mut operations = vec![TxnOp::put(user_key, serde_json::to_vec(&user)?, None)];
        if expected_revision.is_none() {
            operations.push(TxnOp::put(
                username_key.clone(),
                user.user_id.as_bytes(),
                None,
            ));
        }
        let mut compares = vec![compare];
        if expected_revision.is_none() {
            compares.push(Compare::create_revision(username_key, CompareOp::Equal, 0));
        }
        let response = client
            .lock()
            .await
            .txn(Txn::new().when(compares).and_then(operations))
            .await?;
        if !response.succeeded() {
            return Err(AccountDirectoryError::Conflict);
        }
        let revision = response_revision(&response)?;
        self.refresh().await?;
        Ok(revision)
    }

    pub async fn delete_user(
        &self,
        user_id: &str,
        expected_revision: u64,
    ) -> Result<(), AccountDirectoryError> {
        let snapshot = self.snapshot();
        let user = snapshot
            .user_by_id(user_id)
            .ok_or(AccountDirectoryError::NotFound)?;
        if snapshot
            .mountpoints()
            .any(|mountpoint| mountpoint.owner_user_id == user_id)
        {
            return Err(AccountDirectoryError::InUse);
        }
        let (client, cluster_id) = self.writable()?;
        let user_key = user_key(cluster_id, user_id)?;
        let username_key = username_key(cluster_id, &user.username)?;
        let compare =
            Compare::mod_revision(user_key.clone(), CompareOp::Equal, expected_revision as i64);
        let response = client
            .lock()
            .await
            .txn(Txn::new().when([compare]).and_then([
                TxnOp::delete(user_key, None),
                TxnOp::delete(username_key, None),
            ]))
            .await?;
        if !response.succeeded() {
            return Err(AccountDirectoryError::Conflict);
        }
        self.refresh().await?;
        Ok(())
    }

    pub async fn upsert_mountpoint(
        &self,
        mountpoint: Mountpoint,
        expected_revision: Option<u64>,
    ) -> Result<u64, AccountDirectoryError> {
        let snapshot = self.snapshot();
        let owner = snapshot
            .user_by_id(&mountpoint.owner_user_id)
            .filter(|owner| owner.enabled)
            .ok_or(AccountDirectoryError::InvalidRecord)?;
        let mut users = snapshot.users().cloned().collect::<Vec<_>>();
        if !users.iter().any(|user| user.user_id == owner.user_id) {
            users.push(owner.clone());
        }
        AccountSnapshot::new(users, [mountpoint.clone()])?;
        let (client, cluster_id) = self.writable()?;
        let key = mountpoint_key(cluster_id, &mountpoint.mountpoint)?;
        let compare = match expected_revision {
            Some(revision) => Compare::mod_revision(key.clone(), CompareOp::Equal, revision as i64),
            None => Compare::create_revision(key.clone(), CompareOp::Equal, 0),
        };
        let response = client
            .lock()
            .await
            .txn(Txn::new().when([compare]).and_then([TxnOp::put(
                key,
                serde_json::to_vec(&mountpoint)?,
                None,
            )]))
            .await?;
        if !response.succeeded() {
            return Err(AccountDirectoryError::Conflict);
        }
        let revision = response_revision(&response)?;
        self.refresh().await?;
        Ok(revision)
    }

    pub async fn delete_mountpoint(
        &self,
        mountpoint: &str,
        expected_revision: u64,
    ) -> Result<(), AccountDirectoryError> {
        let (client, cluster_id) = self.writable()?;
        let key = mountpoint_key(cluster_id, mountpoint)?;
        let compare =
            Compare::mod_revision(key.clone(), CompareOp::Equal, expected_revision as i64);
        let response = client
            .lock()
            .await
            .txn(
                Txn::new()
                    .when([compare])
                    .and_then([TxnOp::delete(key, None)]),
            )
            .await?;
        if !response.succeeded() {
            return Err(AccountDirectoryError::Conflict);
        }
        self.refresh().await?;
        Ok(())
    }

    fn writable(&self) -> Result<(&Arc<Mutex<Client>>, &str), AccountDirectoryError> {
        self.client
            .as_ref()
            .zip(self.cluster_id.as_deref())
            .ok_or(AccountDirectoryError::InvalidCluster)
    }
}

impl AccountWatcher {
    pub async fn start(
        client: Client,
        cluster_id: &str,
        seed: AccountSnapshot,
    ) -> Result<Self, AccountDirectoryError> {
        Self::start_with_metrics(
            client,
            cluster_id,
            seed,
            Arc::new(EtcdWatchMetrics::default()),
        )
        .await
    }

    pub(crate) async fn start_with_metrics(
        mut client: Client,
        cluster_id: &str,
        seed: AccountSnapshot,
        watch_metrics: Arc<EtcdWatchMetrics>,
    ) -> Result<Self, AccountDirectoryError> {
        validate_cluster(cluster_id)?;
        seed_if_empty(&mut client, cluster_id, &seed).await?;
        let snapshot = load_accounts(&mut client, cluster_id).await?;
        let directory = AccountDirectory {
            snapshot: Arc::new(RwLock::new(snapshot)),
            healthy: Arc::new(AtomicBool::new(false)),
            client: Some(Arc::new(Mutex::new(client.clone()))),
            cluster_id: Some(cluster_id.to_owned()),
            store: None,
            blacklist: Arc::new(RwLock::new(Vec::new())),
            local_admin_sessions: Arc::new(std::sync::Mutex::new(HashMap::new())),
            local_public_sessions: Arc::new(std::sync::Mutex::new(HashMap::new())),
            local_monitor_tickets: Arc::new(std::sync::Mutex::new(HashMap::new())),
            watch_metrics,
        };
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let task_directory = directory.clone();
        let prefix = config_prefix(cluster_id);
        let task = tokio::spawn(async move {
            run_account_watch(client, prefix, task_directory, &mut stop_rx).await;
        });
        Ok(Self {
            directory,
            stop: Some(stop_tx),
            task: Some(task),
        })
    }

    pub fn directory(&self) -> AccountDirectory {
        self.directory.clone()
    }

    pub async fn shutdown(mut self) {
        self.directory.healthy.store(false, Ordering::Release);
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

async fn run_account_watch(
    mut client: Client,
    prefix: String,
    directory: AccountDirectory,
    stop: &mut oneshot::Receiver<()>,
) {
    loop {
        let loaded = tokio::select! {
            _ = &mut *stop => return,
            loaded = load_accounts_prefix(&mut client, &prefix) => loaded,
        };
        let (snapshot, revision) = match loaded {
            Ok(loaded) => loaded,
            Err(_) => {
                directory.watch_metrics.record_recovery(WatchLane::Accounts);
                directory.healthy.store(false, Ordering::Release);
                if stop_or_sleep(stop).await {
                    return;
                }
                continue;
            }
        };
        replace_snapshot(&directory.snapshot, &snapshot);
        directory
            .watch_metrics
            .observe(WatchLane::Accounts, revision, []);
        let initial_revision = revision;
        let stream = tokio::select! {
            _ = &mut *stop => return,
            stream = client.watch(
                prefix.clone(),
                Some(
                    WatchOptions::new()
                        .with_prefix()
                        .with_start_revision(initial_revision + 1)
                        .with_progress_notify(),
                ),
            ) => stream,
        };
        let Ok(mut stream) = stream else {
            directory.watch_metrics.record_recovery(WatchLane::Accounts);
            directory.healthy.store(false, Ordering::Release);
            if stop_or_sleep(stop).await {
                return;
            }
            continue;
        };
        directory.healthy.store(true, Ordering::Release);
        let mut watch_revision = initial_revision;
        loop {
            let response = tokio::select! {
                _ = &mut *stop => return,
                response = tokio::time::timeout(ETCD_WATCH_IDLE_TIMEOUT, stream.message()) => response,
            };
            let Some(Some(response)) = response.ok().and_then(Result::ok) else {
                directory.watch_metrics.record_recovery(WatchLane::Accounts);
                directory.healthy.store(false, Ordering::Release);
                let reconnect = tokio::select! {
                    _ = &mut *stop => return,
                    stream = client.watch(
                        prefix.clone(),
                        Some(
                            WatchOptions::new()
                                .with_prefix()
                                .with_start_revision(watch_revision + 1)
                                .with_progress_notify(),
                        ),
                    ) => stream,
                };
                match reconnect {
                    Ok(next) => {
                        stream = next;
                        directory.healthy.store(true, Ordering::Release);
                        continue;
                    }
                    Err(_) => {
                        directory.watch_metrics.record_recovery(WatchLane::Accounts);
                        break;
                    }
                }
            };
            if let Some(header) = response.header() {
                watch_revision = watch_revision.max(header.revision());
                directory.watch_metrics.observe(
                    WatchLane::Accounts,
                    header.revision(),
                    response
                        .events()
                        .iter()
                        .filter_map(|event| event.kv().map(|value| value.mod_revision())),
                );
            }
            if response.canceled() {
                break;
            }
            let changed = response.events().iter().any(|event| {
                event.event_type() == EventType::Delete
                    || event.kv().is_some_and(|kv| {
                        let key = String::from_utf8_lossy(kv.key());
                        key.starts_with(&format!("{prefix}users/"))
                            || key.starts_with(&format!("{prefix}mounts/"))
                    })
            });
            if changed {
                break;
            }
        }
        directory.healthy.store(false, Ordering::Release);
        if stop_or_sleep(stop).await {
            return;
        }
    }
}

async fn seed_if_empty(
    client: &mut Client,
    cluster_id: &str,
    seed: &AccountSnapshot,
) -> Result<(), AccountDirectoryError> {
    let users = range(client, &format!("{}/users/", config_prefix(cluster_id))).await?;
    let mounts = range(client, &format!("{}/mounts/", config_prefix(cluster_id))).await?;
    if !users.is_empty() || !mounts.is_empty() {
        return Ok(());
    }
    let mut records = Vec::<SeedRecord>::new();
    for user in seed.users() {
        let key = user_key(cluster_id, &user.user_id)?;
        let username_key = username_key(cluster_id, &user.username)?;
        records.push(SeedRecord {
            values: vec![
                (key, serde_json::to_vec(user)?),
                (username_key, user.user_id.as_bytes().to_vec()),
            ],
        });
    }
    for mountpoint in seed.mountpoints() {
        records.push(SeedRecord {
            values: vec![(
                mountpoint_key(cluster_id, &mountpoint.mountpoint)?,
                serde_json::to_vec(mountpoint)?,
            )],
        });
    }
    for batch in records.chunks(32) {
        let compares = batch
            .iter()
            .flat_map(|record| record.values.iter())
            .map(|(key, _)| Compare::create_revision(key.clone(), CompareOp::Equal, 0))
            .collect::<Vec<_>>();
        let operations = batch
            .iter()
            .flat_map(|record| record.values.iter())
            .map(|(key, value)| TxnOp::put(key.clone(), value.clone(), None))
            .collect::<Vec<_>>();
        if client
            .txn(Txn::new().when(compares).and_then(operations))
            .await?
            .succeeded()
        {
            continue;
        }
        for record in batch {
            let compares = record
                .values
                .iter()
                .map(|(key, _)| Compare::create_revision(key.clone(), CompareOp::Equal, 0))
                .collect::<Vec<_>>();
            let operations = record
                .values
                .iter()
                .map(|(key, value)| TxnOp::put(key.clone(), value.clone(), None))
                .collect::<Vec<_>>();
            let _ = client
                .txn(Txn::new().when(compares).and_then(operations))
                .await?;
        }
    }
    Ok(())
}

struct SeedRecord {
    values: Vec<(String, Vec<u8>)>,
}

pub async fn load_accounts(
    client: &mut Client,
    cluster_id: &str,
) -> Result<AccountSnapshot, AccountDirectoryError> {
    load_accounts_prefix(client, &config_prefix(cluster_id))
        .await
        .map(|(snapshot, _)| snapshot)
}

async fn load_accounts_prefix(
    client: &mut Client,
    prefix: &str,
) -> Result<(AccountSnapshot, i64), AccountDirectoryError> {
    let users_prefix = format!("{prefix}users/");
    let mounts_prefix = format!("{prefix}mounts/");
    let response = client
        .get(prefix, Some(GetOptions::new().with_prefix()))
        .await?;
    let revision = response
        .header()
        .map(|header| header.revision())
        .unwrap_or_default();
    let mut users = Vec::new();
    let mut mountpoints = Vec::new();
    for kv in response.kvs() {
        let key = String::from_utf8_lossy(kv.key());
        if key.starts_with(&users_prefix) {
            let mut user: User = serde_json::from_slice(kv.value())?;
            user.mod_revision = kv.mod_revision() as u64;
            users.push(user);
        } else if key.starts_with(&mounts_prefix) {
            let mut mountpoint: Mountpoint = serde_json::from_slice(kv.value())?;
            mountpoint.mod_revision = kv.mod_revision() as u64;
            mountpoints.push(mountpoint);
        }
    }
    Ok((AccountSnapshot::new(users, mountpoints)?, revision))
}

async fn range(
    client: &mut Client,
    prefix: &str,
) -> Result<Vec<etcd_client::KeyValue>, AccountDirectoryError> {
    Ok(client
        .get(prefix, Some(GetOptions::new().with_prefix()))
        .await?
        .kvs()
        .to_vec())
}

fn replace_snapshot(snapshot: &RwLock<AccountSnapshot>, next: &AccountSnapshot) {
    if let Ok(mut current) = snapshot.write() {
        *current = next.clone();
    }
}

fn replace_blacklist(blacklist: &RwLock<Vec<BlacklistRule>>, next: Vec<BlacklistRule>) {
    if let Ok(mut current) = blacklist.write() {
        *current = next;
    }
}

fn response_revision(response: &etcd_client::TxnResponse) -> Result<u64, AccountDirectoryError> {
    response
        .header()
        .map(|header| header.revision() as u64)
        .ok_or(AccountDirectoryError::InvalidRecord)
}

fn now_string() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

fn config_prefix(cluster_id: &str) -> String {
    format!("/rcaster/{cluster_id}/config/")
}

pub(crate) fn user_key(cluster_id: &str, user_id: &str) -> Result<String, AccountDirectoryError> {
    validate_cluster(cluster_id)?;
    if user_id.is_empty()
        || !user_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(AccountDirectoryError::InvalidRecord);
    }
    Ok(format!("{}users/{user_id}", config_prefix(cluster_id)))
}

fn username_key(cluster_id: &str, username: &str) -> Result<String, AccountDirectoryError> {
    validate_cluster(cluster_id)?;
    if username.is_empty() {
        return Err(AccountDirectoryError::InvalidRecord);
    }
    let encoded = hex(username.as_bytes());
    Ok(format!("{}user-names/{encoded}", config_prefix(cluster_id)))
}

pub(crate) fn mountpoint_key(
    cluster_id: &str,
    mountpoint: &str,
) -> Result<String, AccountDirectoryError> {
    validate_cluster(cluster_id)?;
    rcaster_core::ntrip::validate_mountpoint(mountpoint)
        .map_err(|_| AccountDirectoryError::InvalidRecord)?;
    Ok(format!("{}mounts/{mountpoint}", config_prefix(cluster_id)))
}

fn validate_cluster(cluster_id: &str) -> Result<(), AccountDirectoryError> {
    if cluster_id.is_empty()
        || !cluster_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(AccountDirectoryError::InvalidCluster);
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

async fn stop_or_sleep(stop: &mut oneshot::Receiver<()>) -> bool {
    tokio::select! {
        _ = &mut *stop => true,
        _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => false,
    }
}
