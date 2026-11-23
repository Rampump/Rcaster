use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, Compare, CompareOp, DeleteOptions, GetOptions, PutOptions, Txn, TxnOp};
use rand::Rng;
use rcaster_core::{
    account::{AccountSnapshot, Mountpoint, User},
    str_record::StrRecord,
};
use sha2::{Digest, Sha256};
use tokio::{
    sync::{Mutex, oneshot, watch},
    task::JoinHandle,
    time::{sleep, timeout},
};

use crate::{
    account_directory::{AccountDirectory, AccountDirectoryError, AccountWatcher},
    admission::{
        AdmissionError, ClientAdmissionRequest, ClientPermit, SessionRecord,
        SourceAdmissionRequest, SourcePermit, release_session,
    },
    blacklist::{BlacklistRule, BlacklistWatcher, delete_blacklist_rule, upsert_blacklist_rule},
    config::ConfigWatcher,
    coordinator_admission::AdmissionCoordinator,
    coordinator_grpc::GrpcAdmissionClient,
    session_watch::SessionWatcher,
    source_claim::{
        ClaimError, SourceClaimRequest, SourceRoute, activate_source_for_connection,
        publish_active, release_source, release_source_for_connection,
    },
    source_watch::SourceWatcher,
};

const AUTH_FAILURE_WINDOW_S: u64 = 60;
const AUTH_MAX_FAILURES: u64 = 5;
const AUTH_LOCKOUT_S: u64 = 300;

#[derive(Clone, Debug)]
pub struct ClusterRuntimeOptions {
    pub cluster_id: String,
    pub caster_id: String,
    pub runtime_epoch: String,
    pub relay_endpoint: String,
    pub lease_ttl_s: i64,
    pub keep_alive: bool,
}

pub struct ClusterRuntime {
    client: Client,
    backend: AdmissionBackend,
    options: ClusterRuntimeOptions,
    lease_id: Arc<AtomicI64>,
    config: ConfigWatcher,
    blacklist: BlacklistWatcher,
    sources: SourceWatcher,
    session_watch: SessionWatcher,
    accounts: AccountWatcher,
    version_healthy: Arc<AtomicBool>,
    version_maintenance: Arc<AtomicBool>,
    version_lease_id: i64,
    healthy: Arc<AtomicBool>,
    lease_health: watch::Sender<bool>,
    keepalive_stop: Option<oneshot::Sender<()>>,
    keepalive_task: Option<JoinHandle<()>>,
}

enum AdmissionBackend {
    Local(Arc<AdmissionCoordinator>),
    Grpc(GrpcAdmissionClient),
}

pub enum ClusterSourcePermit {
    Local(Box<SourcePermit>),
    Grpc {
        session: Box<SessionRecord>,
        mountpoint: String,
        connection_id: String,
        source_generation: u64,
    },
}

impl ClusterSourcePermit {
    pub fn mountpoint(&self) -> &str {
        match self {
            Self::Local(permit) => &permit.session.mountpoint,
            Self::Grpc { mountpoint, .. } => mountpoint,
        }
    }

    pub fn source_generation(&self) -> u64 {
        match self {
            Self::Local(permit) => permit.claimed_source.claim.source_generation,
            Self::Grpc {
                source_generation, ..
            } => *source_generation,
        }
    }

    pub fn session(&self) -> &SessionRecord {
        match self {
            Self::Local(permit) => &permit.session,
            Self::Grpc { session, .. } => session,
        }
    }

    pub fn connection_id(&self) -> &str {
        match self {
            Self::Local(permit) => &permit.claimed_source.claim.connection_id,
            Self::Grpc { connection_id, .. } => connection_id,
        }
    }
}

#[derive(Clone)]
pub struct SharedClusterRuntime {
    inner: Arc<Mutex<Option<ClusterRuntime>>>,
    auth_failure_fallback: Arc<AtomicU64>,
}

#[derive(Debug)]
pub enum ClusterRuntimeError {
    Admission(AdmissionError),
    Accounts(AccountDirectoryError),
    Blacklist(String),
    Sources(ClaimError),
    WatchesUnavailable,
    InvalidOptions,
}

impl ClusterRuntime {
    pub async fn start(
        client: Client,
        coordinator: Arc<AdmissionCoordinator>,
        options: ClusterRuntimeOptions,
    ) -> Result<Self, ClusterRuntimeError> {
        Self::start_with_accounts(client, coordinator, options, AccountSnapshot::default()).await
    }

    pub async fn start_with_accounts(
        mut client: Client,
        coordinator: Arc<AdmissionCoordinator>,
        options: ClusterRuntimeOptions,
        account_seed: AccountSnapshot,
    ) -> Result<Self, ClusterRuntimeError> {
        if !valid_options(&options) {
            return Err(ClusterRuntimeError::InvalidOptions);
        }
        let config = ConfigWatcher::start(client.clone(), &options.cluster_id)
            .await
            .map_err(|error| ClusterRuntimeError::Admission(AdmissionError::Config(error)))?;
        let blacklist = BlacklistWatcher::start(client.clone(), &options.cluster_id)
            .await
            .map_err(|error| ClusterRuntimeError::Blacklist(error.to_string()))?;
        let sources = SourceWatcher::start(client.clone(), &options.cluster_id)
            .await
            .map_err(ClusterRuntimeError::Sources)?;
        let session_watch = SessionWatcher::start(client.clone(), &options.cluster_id).await?;
        let accounts = AccountWatcher::start(client.clone(), &options.cluster_id, account_seed)
            .await
            .map_err(ClusterRuntimeError::Accounts)?;
        if !wait_for_watchers(&config, &blacklist, &sources, &accounts).await {
            config.shutdown().await;
            blacklist.shutdown().await;
            sources.shutdown().await;
            session_watch.shutdown().await;
            accounts.shutdown().await;
            return Err(ClusterRuntimeError::WatchesUnavailable);
        }
        let lease_id = client
            .lease_grant(options.lease_ttl_s, None)
            .await
            .map_err(AdmissionError::Etcd)?
            .id();
        publish_version_status(&mut client, &options, lease_id, true, false).await?;
        let lease_id = Arc::new(AtomicI64::new(lease_id));
        let healthy = Arc::new(AtomicBool::new(true));
        let version_healthy = Arc::new(AtomicBool::new(true));
        let version_maintenance = Arc::new(AtomicBool::new(false));
        let (lease_health, _) = watch::channel(true);
        let (keepalive_stop, keepalive_task) = if options.keep_alive {
            let keepalive_client = client.clone();
            let keepalive_lease_id = Arc::clone(&lease_id);
            let keepalive_options = options.clone();
            let (stop_tx, stop_rx) = oneshot::channel();
            let task = tokio::spawn(keep_lease_alive(
                keepalive_client,
                keepalive_lease_id,
                keepalive_options,
                Arc::clone(&healthy),
                Arc::clone(&version_healthy),
                Arc::clone(&version_maintenance),
                lease_health.clone(),
                stop_rx,
            ));
            (Some(stop_tx), Some(task))
        } else {
            (None, None)
        };
        Ok(Self {
            client,
            backend: AdmissionBackend::Local(coordinator),
            options,
            lease_id: Arc::clone(&lease_id),
            config,
            blacklist,
            sources,
            session_watch,
            accounts,
            version_healthy,
            version_maintenance,
            version_lease_id: lease_id.load(Ordering::Acquire),
            healthy,
            lease_health,
            keepalive_stop,
            keepalive_task,
        })
    }

    pub async fn start_grpc(
        client: Client,
        coordinator_endpoint: &str,
        options: ClusterRuntimeOptions,
    ) -> Result<Self, ClusterRuntimeError> {
        Self::start_grpc_with_accounts(
            client,
            coordinator_endpoint,
            options,
            AccountSnapshot::default(),
        )
        .await
    }

    pub async fn start_grpc_with_accounts(
        client: Client,
        coordinator_endpoint: &str,
        options: ClusterRuntimeOptions,
        account_seed: AccountSnapshot,
    ) -> Result<Self, ClusterRuntimeError> {
        let admission = GrpcAdmissionClient::connect(coordinator_endpoint.to_owned()).await?;
        Self::start_with_grpc_client_and_accounts(client, admission, options, account_seed).await
    }

    pub async fn start_with_grpc_client(
        client: Client,
        admission: GrpcAdmissionClient,
        options: ClusterRuntimeOptions,
    ) -> Result<Self, ClusterRuntimeError> {
        Self::start_with_grpc_client_and_accounts(
            client,
            admission,
            options,
            AccountSnapshot::default(),
        )
        .await
    }

    pub async fn start_with_grpc_client_and_accounts(
        mut client: Client,
        admission: GrpcAdmissionClient,
        options: ClusterRuntimeOptions,
        account_seed: AccountSnapshot,
    ) -> Result<Self, ClusterRuntimeError> {
        if !valid_options(&options) {
            return Err(ClusterRuntimeError::InvalidOptions);
        }
        let config = ConfigWatcher::start(client.clone(), &options.cluster_id)
            .await
            .map_err(|error| ClusterRuntimeError::Admission(AdmissionError::Config(error)))?;
        let blacklist = BlacklistWatcher::start(client.clone(), &options.cluster_id)
            .await
            .map_err(|error| ClusterRuntimeError::Blacklist(error.to_string()))?;
        let sources = SourceWatcher::start(client.clone(), &options.cluster_id)
            .await
            .map_err(ClusterRuntimeError::Sources)?;
        let session_watch = SessionWatcher::start(client.clone(), &options.cluster_id).await?;
        let accounts = AccountWatcher::start(client.clone(), &options.cluster_id, account_seed)
            .await
            .map_err(ClusterRuntimeError::Accounts)?;
        if !wait_for_watchers(&config, &blacklist, &sources, &accounts).await {
            config.shutdown().await;
            blacklist.shutdown().await;
            sources.shutdown().await;
            session_watch.shutdown().await;
            accounts.shutdown().await;
            return Err(ClusterRuntimeError::WatchesUnavailable);
        }
        let lease_id = client
            .lease_grant(options.lease_ttl_s, None)
            .await
            .map_err(AdmissionError::Etcd)?
            .id();
        publish_version_status(&mut client, &options, lease_id, true, false).await?;
        let lease_id = Arc::new(AtomicI64::new(lease_id));
        let backend = AdmissionBackend::Grpc(admission);
        let healthy = Arc::new(AtomicBool::new(true));
        let version_healthy = Arc::new(AtomicBool::new(true));
        let version_maintenance = Arc::new(AtomicBool::new(false));
        let (lease_health, _) = watch::channel(true);
        let (keepalive_stop, keepalive_task) = if options.keep_alive {
            let keepalive_client = client.clone();
            let keepalive_lease_id = Arc::clone(&lease_id);
            let keepalive_options = options.clone();
            let (stop_tx, stop_rx) = oneshot::channel();
            let task = tokio::spawn(keep_lease_alive(
                keepalive_client,
                keepalive_lease_id,
                keepalive_options,
                Arc::clone(&healthy),
                Arc::clone(&version_healthy),
                Arc::clone(&version_maintenance),
                lease_health.clone(),
                stop_rx,
            ));
            (Some(stop_tx), Some(task))
        } else {
            (None, None)
        };
        Ok(Self {
            client,
            backend,
            options,
            lease_id: Arc::clone(&lease_id),
            config,
            blacklist,
            sources,
            session_watch,
            accounts,
            version_healthy,
            version_maintenance,
            version_lease_id: lease_id.load(Ordering::Acquire),
            healthy,
            lease_health,
            keepalive_stop,
            keepalive_task,
        })
    }

    pub async fn admit_client(
        &mut self,
        user_id: &str,
        mountpoint: &str,
        client_ip: &str,
        max_sessions: usize,
        ip_max_sessions: Option<usize>,
    ) -> Result<ClientPermit, AdmissionError> {
        self.admit_client_with_metadata(
            user_id,
            mountpoint,
            client_ip,
            max_sessions,
            ip_max_sessions,
            None,
            None,
            0,
            0,
            0,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn admit_client_with_metadata(
        &mut self,
        user_id: &str,
        mountpoint: &str,
        client_ip: &str,
        max_sessions: usize,
        ip_max_sessions: Option<usize>,
        ntrip_version: Option<String>,
        software: Option<String>,
        auth_version: u64,
        user_mod_revision: u64,
        mountpoint_mod_revision: u64,
    ) -> Result<ClientPermit, AdmissionError> {
        if !self.is_healthy() {
            return Err(AdmissionError::CoordinatorUnavailable);
        }
        let mut session = SessionRecord::client(
            unique_id("s"),
            user_id,
            mountpoint,
            &self.options.caster_id,
            &self.options.runtime_epoch,
            client_ip,
            now_timestamp(),
        );
        session.ntrip_version = ntrip_version;
        session.software = software;
        session = session.with_account_revisions(
            auth_version,
            user_mod_revision,
            mountpoint_mod_revision,
        );
        let request = ClientAdmissionRequest::new(
            session,
            max_sessions,
            ip_max_sessions,
            self.config.snapshot(),
        );
        match &mut self.backend {
            AdmissionBackend::Local(coordinator) => {
                coordinator
                    .admit_client(self.lease_id.load(Ordering::Acquire), request)
                    .await
            }
            AdmissionBackend::Grpc(client) => {
                client
                    .admit_client(self.lease_id.load(Ordering::Acquire), request)
                    .await
            }
        }
    }

    pub async fn admit_source(
        &mut self,
        user_id: &str,
        mountpoint: &str,
        connection_id: &str,
        max_sessions: usize,
    ) -> Result<ClusterSourcePermit, AdmissionError> {
        self.admit_source_with_metadata(
            user_id,
            mountpoint,
            connection_id,
            max_sessions,
            None,
            None,
            0,
            0,
            0,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn admit_source_with_metadata(
        &mut self,
        user_id: &str,
        mountpoint: &str,
        connection_id: &str,
        max_sessions: usize,
        ntrip_version: Option<String>,
        software: Option<String>,
        auth_version: u64,
        user_mod_revision: u64,
        mountpoint_mod_revision: u64,
    ) -> Result<ClusterSourcePermit, AdmissionError> {
        if !self.is_healthy() {
            return Err(AdmissionError::CoordinatorUnavailable);
        }
        let timestamp = now_timestamp();
        let mut session = SessionRecord::source(
            unique_id("s"),
            user_id,
            mountpoint,
            &self.options.caster_id,
            &self.options.runtime_epoch,
            timestamp.clone(),
        );
        session.ntrip_version = ntrip_version;
        session.software = software;
        session = session.with_account_revisions(
            auth_version,
            user_mod_revision,
            mountpoint_mod_revision,
        );
        let claim = SourceClaimRequest::new(
            mountpoint,
            &self.options.caster_id,
            connection_id,
            &self.options.runtime_epoch,
            &self.options.relay_endpoint,
            user_id,
            timestamp,
        );
        let request =
            SourceAdmissionRequest::new(session, max_sessions, claim, self.config.snapshot());
        match &mut self.backend {
            AdmissionBackend::Local(coordinator) => coordinator
                .admit_source(self.lease_id.load(Ordering::Acquire), request)
                .await
                .map(|permit| ClusterSourcePermit::Local(Box::new(permit))),
            AdmissionBackend::Grpc(client) => {
                let permit = client
                    .admit_source(self.lease_id.load(Ordering::Acquire), request)
                    .await?;
                Ok(ClusterSourcePermit::Grpc {
                    session: Box::new(permit.session),
                    mountpoint: mountpoint.to_owned(),
                    connection_id: connection_id.to_owned(),
                    source_generation: permit.source_generation,
                })
            }
        }
    }

    pub async fn release_client(&mut self, permit: &ClientPermit) -> Result<(), AdmissionError> {
        release_session(&mut self.client, &self.options.cluster_id, &permit.session).await
    }

    pub async fn activate_source(
        &mut self,
        permit: &ClusterSourcePermit,
        str_record: &StrRecord,
    ) -> Result<(), AdmissionError> {
        match permit {
            ClusterSourcePermit::Local(permit) => {
                publish_active(&mut self.client, &permit.claimed_source, str_record)
                    .await
                    .map_err(AdmissionError::Source)
            }
            ClusterSourcePermit::Grpc {
                mountpoint,
                connection_id,
                ..
            } => activate_source_for_connection(
                &mut self.client,
                &self.options.cluster_id,
                mountpoint,
                connection_id,
                &self.options.runtime_epoch,
                str_record,
            )
            .await
            .map_err(AdmissionError::Source),
        }
    }

    pub async fn release_source(
        &mut self,
        permit: &ClusterSourcePermit,
    ) -> Result<(), AdmissionError> {
        let (source, session) = match permit {
            ClusterSourcePermit::Local(permit) => (
                release_source(&mut self.client, &permit.claimed_source)
                    .await
                    .map_err(AdmissionError::Source),
                release_session(&mut self.client, &self.options.cluster_id, &permit.session).await,
            ),
            ClusterSourcePermit::Grpc {
                session,
                mountpoint,
                connection_id,
                ..
            } => (
                release_source_for_connection(
                    &mut self.client,
                    &self.options.cluster_id,
                    mountpoint,
                    connection_id,
                    &self.options.runtime_epoch,
                )
                .await
                .map_err(AdmissionError::Source),
                release_session(&mut self.client, &self.options.cluster_id, session).await,
            ),
        };
        source?;
        session
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
            && self.config.is_healthy()
            && self.blacklist.is_healthy()
            && self.sources.is_healthy()
            && self.accounts.directory().is_healthy()
    }

    pub fn lease_is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub fn config_is_healthy(&self) -> bool {
        self.config.is_healthy()
    }

    pub fn blacklist_is_healthy(&self) -> bool {
        self.blacklist.is_healthy()
    }

    pub fn sources_is_healthy(&self) -> bool {
        self.sources.is_healthy()
    }

    pub fn accounts_is_healthy(&self) -> bool {
        self.accounts.directory().is_healthy()
    }

    pub fn lease_health(&self) -> watch::Receiver<bool> {
        self.lease_health.subscribe()
    }

    pub async fn sourcetable_records(&self) -> Vec<StrRecord> {
        self.sources.sourcetable_records()
    }

    pub fn source_route(&self, mountpoint: &str) -> Option<SourceRoute> {
        self.sources.route(mountpoint)
    }

    pub fn cluster_id(&self) -> &str {
        &self.options.cluster_id
    }

    pub fn caster_id(&self) -> &str {
        &self.options.caster_id
    }

    pub fn account_directory(&self) -> AccountDirectory {
        self.accounts.directory()
    }

    pub async fn upsert_user(
        &self,
        user: User,
        expected_revision: Option<u64>,
    ) -> Result<u64, AccountDirectoryError> {
        self.accounts
            .directory()
            .upsert_user(user, expected_revision)
            .await
    }

    pub async fn delete_user(
        &self,
        user_id: &str,
        expected_revision: u64,
    ) -> Result<(), AccountDirectoryError> {
        self.accounts
            .directory()
            .delete_user(user_id, expected_revision)
            .await
    }

    pub async fn upsert_mountpoint(
        &self,
        mountpoint: Mountpoint,
        expected_revision: Option<u64>,
    ) -> Result<u64, AccountDirectoryError> {
        self.accounts
            .directory()
            .upsert_mountpoint(mountpoint, expected_revision)
            .await
    }

    pub async fn delete_mountpoint(
        &self,
        mountpoint: &str,
        expected_revision: u64,
    ) -> Result<(), AccountDirectoryError> {
        self.accounts
            .directory()
            .delete_mountpoint(mountpoint, expected_revision)
            .await
    }

    pub fn blacklist_rules(&self) -> Vec<BlacklistRule> {
        self.blacklist.snapshot().all_rules().to_vec()
    }

    pub async fn upsert_blacklist_rule(
        &mut self,
        rule: &BlacklistRule,
        expected_revision: Option<u64>,
    ) -> Result<u64, crate::blacklist::BlacklistError> {
        upsert_blacklist_rule(
            &mut self.client,
            &self.options.cluster_id,
            rule,
            expected_revision,
        )
        .await
    }

    pub async fn delete_blacklist_rule(
        &mut self,
        id: &str,
        expected_revision: u64,
    ) -> Result<(), crate::blacklist::BlacklistError> {
        delete_blacklist_rule(
            &mut self.client,
            &self.options.cluster_id,
            id,
            expected_revision,
        )
        .await
    }

    pub async fn publish_config(
        &mut self,
        config: crate::config::CasterConfig,
    ) -> Result<crate::config::ConfigSnapshot, crate::config::ConfigError> {
        crate::config::publish_config(&mut self.client, &self.options.cluster_id, config).await
    }

    pub async fn config_revisions(
        &mut self,
        limit: usize,
    ) -> Result<Vec<crate::config::ConfigRevision>, crate::config::ConfigError> {
        crate::config::load_config_revisions(&mut self.client, &self.options.cluster_id, limit)
            .await
    }

    pub async fn prune_config_revisions(
        &mut self,
        retain: usize,
    ) -> Result<usize, crate::config::ConfigError> {
        crate::config::prune_config_revisions(&mut self.client, &self.options.cluster_id, retain)
            .await
    }

    pub fn config_snapshot(&self) -> crate::config::ConfigSnapshot {
        self.config.snapshot()
    }

    pub fn config_healthy(&self) -> bool {
        self.config.is_healthy()
    }

    pub async fn reload_config(&mut self) -> Result<crate::config::ConfigSnapshot, AdmissionError> {
        let snapshot =
            crate::config::load_config_snapshot(&mut self.client, &self.options.cluster_id)
                .await
                .map_err(AdmissionError::Config)?;
        self.config.replace_snapshot(snapshot.clone());
        Ok(snapshot)
    }

    pub async fn sessions(&mut self) -> Result<Vec<SessionRecord>, AdmissionError> {
        let prefix = format!("/rcaster/{}/online/users/", self.options.cluster_id);
        let response = self
            .client
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await?;
        let mut sessions = std::collections::BTreeMap::new();
        for kv in response.kvs() {
            let session: SessionRecord = serde_json::from_slice(kv.value())?;
            sessions.insert(session.session_id.clone(), session);
        }
        Ok(sessions.into_values().collect())
    }

    pub fn session_count(&self) -> usize {
        self.session_watch.count()
    }

    pub async fn request_session_disconnect(
        &mut self,
        session: &SessionRecord,
    ) -> Result<(), AdmissionError> {
        let command_id = unique_id("cmd");
        let key = format!(
            "/rcaster/{}/online/commands/{}/{command_id}",
            self.options.cluster_id, session.caster_id
        );
        let lease_id = self.client.lease_grant(10, None).await?.id();
        let value = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "command_id": command_id,
            "session_id": session.session_id,
            "runtime_epoch": session.runtime_epoch,
            "created_at": now_timestamp(),
        }))?;
        self.client
            .put(key, value, Some(PutOptions::new().with_lease(lease_id)))
            .await?;
        Ok(())
    }

    pub async fn request_source_disconnect(
        &mut self,
        route: &SourceRoute,
    ) -> Result<(), AdmissionError> {
        let command_id = unique_id("cmd");
        let key = format!(
            "/rcaster/{}/runtime/source-commands/{}/{command_id}",
            self.options.cluster_id, route.mountpoint
        );
        let lease_id = self.client.lease_grant(10, None).await?.id();
        let value = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "command_id": command_id,
            "mountpoint": route.mountpoint,
            "connection_id": route.connection_id,
            "source_generation": route.source_generation,
            "created_at": now_timestamp(),
        }))?;
        self.client
            .put(key, value, Some(PutOptions::new().with_lease(lease_id)))
            .await?;
        Ok(())
    }

    pub async fn source_cooldown_remaining(
        &mut self,
        mountpoint: &str,
        source_ip: &str,
    ) -> Result<Option<u64>, AdmissionError> {
        let key = source_cooldown_key(&self.options.cluster_id, mountpoint, source_ip)?;
        let Some(value) = self.client.get(key, None).await?.kvs().first().cloned() else {
            return Ok(None);
        };
        let cooldown: serde_json::Value = serde_json::from_slice(value.value())?;
        let now = now_timestamp().parse::<u64>().unwrap_or_default();
        Ok(cooldown
            .get("expires_at")
            .and_then(serde_json::Value::as_u64)
            .map(|expires_at| expires_at.saturating_sub(now)))
    }

    pub async fn record_source_cooldown(
        &mut self,
        mountpoint: &str,
        source_ip: &str,
        reason: &str,
    ) -> Result<(), AdmissionError> {
        let key = source_cooldown_key(&self.options.cluster_id, mountpoint, source_ip)?;
        let fingerprint = key.rsplit('/').next().unwrap_or_default().to_owned();
        let lease_id = self.client.lease_grant(30, None).await?.id();
        let now = now_timestamp().parse::<u64>().unwrap_or_default();
        let value = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "fingerprint": fingerprint,
            "mountpoint": mountpoint,
            "source_ip": source_ip,
            "reason": reason,
            "created_at": now,
            "expires_at": now.saturating_add(30),
        }))?;
        self.client
            .put(key, value, Some(PutOptions::new().with_lease(lease_id)))
            .await?;
        Ok(())
    }

    pub async fn auth_failure_retry_after(
        &mut self,
        key: &str,
    ) -> Result<Option<u64>, AdmissionError> {
        let key = auth_failure_key(&self.options.cluster_id, key);
        let Some(value) = self.client.get(key, None).await?.kvs().first().cloned() else {
            return Ok(None);
        };
        let state: serde_json::Value = serde_json::from_slice(value.value())?;
        let now = now_timestamp().parse::<u64>().unwrap_or_default();
        Ok(state
            .get("locked_until")
            .and_then(serde_json::Value::as_u64)
            .filter(|locked_until| *locked_until > now)
            .map(|locked_until| locked_until - now))
    }

    pub async fn record_auth_failure(&mut self, key: &str) -> Result<Option<u64>, AdmissionError> {
        let key = auth_failure_key(&self.options.cluster_id, key);
        for _ in 0..8 {
            let current = self
                .client
                .get(key.clone(), None)
                .await?
                .kvs()
                .first()
                .cloned();
            let now = now_timestamp().parse::<u64>().unwrap_or_default();
            let mut failures = 0_u64;
            let mut window_started = now;
            let mut locked_until = 0_u64;
            if let Some(value) = &current
                && let Ok(state) = serde_json::from_slice::<serde_json::Value>(value.value())
            {
                failures = state
                    .get("failures")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default();
                window_started = state
                    .get("window_started")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(now);
                locked_until = state
                    .get("locked_until")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default();
            }
            if locked_until > now {
                return Ok(Some(locked_until - now));
            }
            if now.saturating_sub(window_started) >= AUTH_FAILURE_WINDOW_S {
                failures = 0;
                window_started = now;
            }
            failures = failures.saturating_add(1);
            if failures >= AUTH_MAX_FAILURES {
                locked_until = now.saturating_add(AUTH_LOCKOUT_S);
            }
            let ttl = if locked_until > now {
                locked_until - now
            } else {
                window_started
                    .saturating_add(AUTH_FAILURE_WINDOW_S)
                    .saturating_sub(now)
                    .max(1)
            };
            let lease_id = self.client.lease_grant(ttl as i64, None).await?.id();
            let value = serde_json::to_vec(&serde_json::json!({
                "schema_version": 1,
                "failures": failures,
                "window_started": window_started,
                "locked_until": locked_until,
            }))?;
            let compare = current.as_ref().map_or_else(
                || Compare::create_revision(key.clone(), CompareOp::Equal, 0),
                |value| Compare::mod_revision(key.clone(), CompareOp::Equal, value.mod_revision()),
            );
            let result = self
                .client
                .txn(Txn::new().when([compare]).and_then([TxnOp::put(
                    key.clone(),
                    value,
                    Some(PutOptions::new().with_lease(lease_id)),
                )]))
                .await?;
            if result.succeeded() {
                return Ok((locked_until > now).then(|| locked_until - now));
            }
            let _ = self.client.lease_revoke(lease_id).await;
        }
        Err(AdmissionError::CoordinatorUnavailable)
    }

    pub async fn clear_auth_failures(&mut self, key: &str) -> Result<(), AdmissionError> {
        self.client
            .delete(
                auth_failure_key(&self.options.cluster_id, key),
                Some(DeleteOptions::new()),
            )
            .await?;
        Ok(())
    }

    pub async fn source_cooldowns(&mut self) -> Result<Vec<serde_json::Value>, AdmissionError> {
        let prefix = format!(
            "/rcaster/{}/runtime/source-cooldowns/",
            self.options.cluster_id
        );
        let response = self
            .client
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await?;
        response
            .kvs()
            .iter()
            .map(|value| serde_json::from_slice(value.value()).map_err(AdmissionError::Json))
            .collect()
    }

    pub async fn node_versions(&mut self) -> Result<Vec<serde_json::Value>, AdmissionError> {
        let prefix = format!(
            "/rcaster/{}/runtime/version-status/",
            self.options.cluster_id
        );
        let response = self
            .client
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await?;
        response
            .kvs()
            .iter()
            .map(|value| serde_json::from_slice(value.value()).map_err(AdmissionError::Json))
            .collect()
    }

    pub async fn update_version_status(&mut self, healthy: bool) -> Result<(), AdmissionError> {
        let lease_id = self.lease_id.load(Ordering::Acquire);
        if self.version_healthy.load(Ordering::Acquire) == healthy
            && self.version_lease_id == lease_id
        {
            return Ok(());
        }
        publish_version_status(
            &mut self.client,
            &self.options,
            lease_id,
            healthy,
            self.version_maintenance.load(Ordering::Acquire),
        )
        .await?;
        self.version_healthy.store(healthy, Ordering::Release);
        self.version_lease_id = lease_id;
        Ok(())
    }

    pub async fn set_maintenance(&mut self, maintenance: bool) -> Result<(), AdmissionError> {
        if self.version_maintenance.load(Ordering::Acquire) == maintenance {
            return Ok(());
        }
        publish_version_status(
            &mut self.client,
            &self.options,
            self.lease_id.load(Ordering::Acquire),
            self.version_healthy.load(Ordering::Acquire),
            maintenance,
        )
        .await?;
        self.version_maintenance
            .store(maintenance, Ordering::Release);
        self.version_lease_id = self.lease_id.load(Ordering::Acquire);
        Ok(())
    }

    pub async fn clear_source_cooldown(&mut self, fingerprint: &str) -> Result<(), AdmissionError> {
        if !fingerprint.len().is_multiple_of(2)
            || !fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(AdmissionError::InvalidIdentifier("source fingerprint"));
        }
        self.client
            .delete(
                format!(
                    "/rcaster/{}/runtime/source-cooldowns/{fingerprint}",
                    self.options.cluster_id
                ),
                None,
            )
            .await?;
        Ok(())
    }

    pub async fn shutdown(mut self) -> Result<(), AdmissionError> {
        self.healthy.store(false, Ordering::Release);
        self.lease_health.send_replace(false);
        if let Some(stop) = self.keepalive_stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.keepalive_task.take() {
            let _ = task.await;
        }
        let lease_id = self.lease_id.load(Ordering::Acquire);
        self.client
            .lease_revoke(lease_id)
            .await
            .map_err(AdmissionError::Etcd)?;
        self.config.shutdown().await;
        self.blacklist.shutdown().await;
        self.sources.shutdown().await;
        self.session_watch.shutdown().await;
        self.accounts.shutdown().await;
        Ok(())
    }
}

impl SharedClusterRuntime {
    pub async fn start(
        client: Client,
        coordinator: Arc<AdmissionCoordinator>,
        options: ClusterRuntimeOptions,
    ) -> Result<Self, ClusterRuntimeError> {
        Self::start_with_accounts(client, coordinator, options, AccountSnapshot::default()).await
    }

    pub async fn start_with_accounts(
        client: Client,
        coordinator: Arc<AdmissionCoordinator>,
        options: ClusterRuntimeOptions,
        account_seed: AccountSnapshot,
    ) -> Result<Self, ClusterRuntimeError> {
        Ok(Self {
            inner: Arc::new(Mutex::new(Some(
                ClusterRuntime::start_with_accounts(client, coordinator, options, account_seed)
                    .await?,
            ))),
            auth_failure_fallback: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn start_grpc(
        client: Client,
        coordinator_endpoint: &str,
        options: ClusterRuntimeOptions,
    ) -> Result<Self, ClusterRuntimeError> {
        Ok(Self {
            inner: Arc::new(Mutex::new(Some(
                ClusterRuntime::start_grpc(client, coordinator_endpoint, options).await?,
            ))),
            auth_failure_fallback: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn start_with_grpc_client(
        client: Client,
        admission: GrpcAdmissionClient,
        options: ClusterRuntimeOptions,
    ) -> Result<Self, ClusterRuntimeError> {
        Self::start_with_grpc_client_and_accounts(
            client,
            admission,
            options,
            AccountSnapshot::default(),
        )
        .await
    }

    pub async fn start_with_grpc_client_and_accounts(
        client: Client,
        admission: GrpcAdmissionClient,
        options: ClusterRuntimeOptions,
        account_seed: AccountSnapshot,
    ) -> Result<Self, ClusterRuntimeError> {
        Ok(Self {
            inner: Arc::new(Mutex::new(Some(
                ClusterRuntime::start_with_grpc_client_and_accounts(
                    client,
                    admission,
                    options,
                    account_seed,
                )
                .await?,
            ))),
            auth_failure_fallback: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn admit_client(
        &self,
        user_id: &str,
        mountpoint: &str,
        client_ip: &str,
        max_sessions: usize,
        ip_max_sessions: Option<usize>,
    ) -> Result<ClientPermit, AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .admit_client(
                user_id,
                mountpoint,
                client_ip,
                max_sessions,
                ip_max_sessions,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn admit_client_with_metadata(
        &self,
        user_id: &str,
        mountpoint: &str,
        client_ip: &str,
        max_sessions: usize,
        ip_max_sessions: Option<usize>,
        ntrip_version: Option<String>,
        software: Option<String>,
        auth_version: u64,
        user_mod_revision: u64,
        mountpoint_mod_revision: u64,
    ) -> Result<ClientPermit, AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .admit_client_with_metadata(
                user_id,
                mountpoint,
                client_ip,
                max_sessions,
                ip_max_sessions,
                ntrip_version,
                software,
                auth_version,
                user_mod_revision,
                mountpoint_mod_revision,
            )
            .await
    }

    pub async fn admit_source(
        &self,
        user_id: &str,
        mountpoint: &str,
        connection_id: &str,
        max_sessions: usize,
    ) -> Result<ClusterSourcePermit, AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .admit_source(user_id, mountpoint, connection_id, max_sessions)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn admit_source_with_metadata(
        &self,
        user_id: &str,
        mountpoint: &str,
        connection_id: &str,
        max_sessions: usize,
        ntrip_version: Option<String>,
        software: Option<String>,
        auth_version: u64,
        user_mod_revision: u64,
        mountpoint_mod_revision: u64,
    ) -> Result<ClusterSourcePermit, AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .admit_source_with_metadata(
                user_id,
                mountpoint,
                connection_id,
                max_sessions,
                ntrip_version,
                software,
                auth_version,
                user_mod_revision,
                mountpoint_mod_revision,
            )
            .await
    }

    pub async fn release_client(&self, permit: &ClientPermit) -> Result<(), AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .release_client(permit)
            .await
    }

    pub async fn activate_source(
        &self,
        permit: &ClusterSourcePermit,
        str_record: &StrRecord,
    ) -> Result<(), AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .activate_source(permit, str_record)
            .await
    }

    pub async fn release_source(&self, permit: &ClusterSourcePermit) -> Result<(), AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .release_source(permit)
            .await
    }

    pub async fn is_healthy(&self) -> bool {
        self.inner
            .lock()
            .await
            .as_ref()
            .is_some_and(ClusterRuntime::is_healthy)
    }

    pub async fn lease_is_healthy(&self) -> bool {
        self.inner
            .lock()
            .await
            .as_ref()
            .is_some_and(ClusterRuntime::lease_is_healthy)
    }

    pub async fn dependency_health(&self) -> Option<[bool; 5]> {
        self.inner.lock().await.as_ref().map(|runtime| {
            [
                runtime.lease_is_healthy(),
                runtime.config_is_healthy(),
                runtime.blacklist_is_healthy(),
                runtime.sources_is_healthy(),
                runtime.accounts_is_healthy(),
            ]
        })
    }

    pub async fn lease_health(&self) -> Option<watch::Receiver<bool>> {
        self.inner
            .lock()
            .await
            .as_ref()
            .map(ClusterRuntime::lease_health)
    }

    pub async fn ip_quota(&self, address: std::net::IpAddr) -> Option<i32> {
        self.inner
            .lock()
            .await
            .as_ref()
            .and_then(|runtime| runtime.blacklist.quota_for_ip(address))
    }

    pub async fn is_ip_denied(&self, address: std::net::IpAddr) -> bool {
        self.ip_quota(address).await == Some(0)
    }

    pub async fn sourcetable_records(&self) -> Vec<StrRecord> {
        self.inner
            .lock()
            .await
            .as_ref()
            .map(|runtime| runtime.sources.sourcetable_records())
            .unwrap_or_default()
    }

    pub async fn source_route(&self, mountpoint: &str) -> Option<SourceRoute> {
        self.inner
            .lock()
            .await
            .as_ref()
            .and_then(|runtime| runtime.source_route(mountpoint))
    }

    pub async fn cluster_id(&self) -> Option<String> {
        self.inner
            .lock()
            .await
            .as_ref()
            .map(|runtime| runtime.cluster_id().to_owned())
    }

    pub async fn caster_id(&self) -> Option<String> {
        self.inner
            .lock()
            .await
            .as_ref()
            .map(|runtime| runtime.caster_id().to_owned())
    }

    pub async fn account_directory(&self) -> Option<AccountDirectory> {
        self.inner
            .lock()
            .await
            .as_ref()
            .map(ClusterRuntime::account_directory)
    }

    pub async fn upsert_user(
        &self,
        user: User,
        expected_revision: Option<u64>,
    ) -> Result<u64, AccountDirectoryError> {
        self.inner
            .lock()
            .await
            .as_ref()
            .ok_or(AccountDirectoryError::InvalidCluster)?
            .upsert_user(user, expected_revision)
            .await
    }

    pub async fn delete_user(
        &self,
        user_id: &str,
        expected_revision: u64,
    ) -> Result<(), AccountDirectoryError> {
        self.inner
            .lock()
            .await
            .as_ref()
            .ok_or(AccountDirectoryError::InvalidCluster)?
            .delete_user(user_id, expected_revision)
            .await
    }

    pub async fn upsert_mountpoint(
        &self,
        mountpoint: Mountpoint,
        expected_revision: Option<u64>,
    ) -> Result<u64, AccountDirectoryError> {
        self.inner
            .lock()
            .await
            .as_ref()
            .ok_or(AccountDirectoryError::InvalidCluster)?
            .upsert_mountpoint(mountpoint, expected_revision)
            .await
    }

    pub async fn delete_mountpoint(
        &self,
        mountpoint: &str,
        expected_revision: u64,
    ) -> Result<(), AccountDirectoryError> {
        self.inner
            .lock()
            .await
            .as_ref()
            .ok_or(AccountDirectoryError::InvalidCluster)?
            .delete_mountpoint(mountpoint, expected_revision)
            .await
    }

    pub async fn blacklist_rules(&self) -> Vec<BlacklistRule> {
        self.inner
            .lock()
            .await
            .as_ref()
            .map(ClusterRuntime::blacklist_rules)
            .unwrap_or_default()
    }

    pub async fn upsert_blacklist_rule(
        &self,
        rule: &BlacklistRule,
        expected_revision: Option<u64>,
    ) -> Result<u64, crate::blacklist::BlacklistError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| {
                crate::blacklist::BlacklistError::Etcd("runtime unavailable".to_owned())
            })?
            .upsert_blacklist_rule(rule, expected_revision)
            .await
    }

    pub async fn delete_blacklist_rule(
        &self,
        id: &str,
        expected_revision: u64,
    ) -> Result<(), crate::blacklist::BlacklistError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| {
                crate::blacklist::BlacklistError::Etcd("runtime unavailable".to_owned())
            })?
            .delete_blacklist_rule(id, expected_revision)
            .await
    }

    pub async fn publish_config(
        &self,
        config: crate::config::CasterConfig,
    ) -> Result<crate::config::ConfigSnapshot, crate::config::ConfigError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(crate::config::ConfigError::InvalidConfig)?
            .publish_config(config)
            .await
    }

    pub async fn config_revisions(
        &self,
        limit: usize,
    ) -> Result<Vec<crate::config::ConfigRevision>, crate::config::ConfigError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(crate::config::ConfigError::InvalidConfig)?
            .config_revisions(limit)
            .await
    }

    pub async fn prune_config_revisions(&self, retain: usize) -> Result<usize, AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .prune_config_revisions(retain)
            .await
            .map_err(AdmissionError::Config)
    }

    pub async fn sessions(&self) -> Result<Vec<SessionRecord>, AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .sessions()
            .await
    }

    pub async fn session_count(&self) -> usize {
        self.inner
            .lock()
            .await
            .as_ref()
            .map(ClusterRuntime::session_count)
            .unwrap_or_default()
    }

    pub async fn request_session_disconnect(
        &self,
        session: &SessionRecord,
    ) -> Result<(), AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .request_session_disconnect(session)
            .await
    }

    pub async fn request_source_disconnect(
        &self,
        route: &SourceRoute,
    ) -> Result<(), AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .request_source_disconnect(route)
            .await
    }

    pub async fn source_cooldown_remaining(
        &self,
        mountpoint: &str,
        source_ip: &str,
    ) -> Result<Option<u64>, AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .source_cooldown_remaining(mountpoint, source_ip)
            .await
    }

    pub async fn record_source_cooldown(
        &self,
        mountpoint: &str,
        source_ip: &str,
        reason: &str,
    ) -> Result<(), AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .record_source_cooldown(mountpoint, source_ip, reason)
            .await
    }

    pub async fn auth_failure_retry_after(&self, key: &str) -> Result<Option<u64>, AdmissionError> {
        let mut inner = self.inner.lock().await;
        let result = match inner.as_mut() {
            Some(runtime) => runtime.auth_failure_retry_after(key).await,
            None => Err(AdmissionError::CoordinatorUnavailable),
        };
        self.auth_failure_result(result)
    }

    pub async fn record_auth_failure(&self, key: &str) -> Result<Option<u64>, AdmissionError> {
        let mut inner = self.inner.lock().await;
        let result = match inner.as_mut() {
            Some(runtime) => runtime.record_auth_failure(key).await,
            None => Err(AdmissionError::CoordinatorUnavailable),
        };
        self.auth_failure_result(result)
    }

    pub async fn clear_auth_failures(&self, key: &str) -> Result<(), AdmissionError> {
        let mut inner = self.inner.lock().await;
        let result = match inner.as_mut() {
            Some(runtime) => runtime.clear_auth_failures(key).await,
            None => Err(AdmissionError::CoordinatorUnavailable),
        };
        self.auth_failure_result(result)
    }

    pub fn auth_failure_fallback_total(&self) -> u64 {
        self.auth_failure_fallback.load(Ordering::Relaxed)
    }

    fn auth_failure_result<T>(
        &self,
        result: Result<T, AdmissionError>,
    ) -> Result<T, AdmissionError> {
        if result.is_err() {
            self.auth_failure_fallback.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    pub async fn source_cooldowns(&self) -> Result<Vec<serde_json::Value>, AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .source_cooldowns()
            .await
    }

    pub async fn node_versions(&self) -> Result<Vec<serde_json::Value>, AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .node_versions()
            .await
    }

    pub async fn update_version_status(&self, healthy: bool) -> Result<(), AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .update_version_status(healthy)
            .await
    }

    pub async fn set_maintenance(&self, maintenance: bool) -> Result<(), AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .set_maintenance(maintenance)
            .await
    }

    pub async fn clear_source_cooldown(&self, fingerprint: &str) -> Result<(), AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .clear_source_cooldown(fingerprint)
            .await
    }

    pub async fn config_snapshot(&self) -> Option<crate::config::ConfigSnapshot> {
        self.inner
            .lock()
            .await
            .as_ref()
            .map(ClusterRuntime::config_snapshot)
    }

    pub async fn config_healthy(&self) -> Option<bool> {
        self.inner
            .lock()
            .await
            .as_ref()
            .map(ClusterRuntime::config_healthy)
    }

    pub async fn reload_config(&self) -> Result<crate::config::ConfigSnapshot, AdmissionError> {
        self.inner
            .lock()
            .await
            .as_mut()
            .ok_or(AdmissionError::CoordinatorUnavailable)?
            .reload_config()
            .await
    }

    pub async fn shutdown(&self) -> Result<(), AdmissionError> {
        let runtime = self.inner.lock().await.take();
        match runtime {
            Some(runtime) => runtime.shutdown().await,
            None => Ok(()),
        }
    }
}

impl fmt::Display for ClusterRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(error) => write!(formatter, "cluster admission error: {error}"),
            Self::Accounts(error) => write!(formatter, "account directory error: {error}"),
            Self::Blacklist(error) => write!(formatter, "blacklist error: {error}"),
            Self::Sources(error) => write!(formatter, "source watch error: {error}"),
            Self::WatchesUnavailable => formatter.write_str("cluster watches are unavailable"),
            Self::InvalidOptions => formatter.write_str("invalid cluster runtime options"),
        }
    }
}

impl std::error::Error for ClusterRuntimeError {}

impl From<AdmissionError> for ClusterRuntimeError {
    fn from(error: AdmissionError) -> Self {
        Self::Admission(error)
    }
}

async fn wait_for_watchers(
    config: &ConfigWatcher,
    blacklist: &BlacklistWatcher,
    sources: &SourceWatcher,
    accounts: &AccountWatcher,
) -> bool {
    timeout(crate::ETCD_WATCH_IDLE_TIMEOUT, async {
        loop {
            if config.is_healthy()
                && blacklist.is_healthy()
                && sources.is_healthy()
                && accounts.directory().is_healthy()
            {
                return;
            }
            sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

fn valid_options(options: &ClusterRuntimeOptions) -> bool {
    options.lease_ttl_s > 0
        && !options.cluster_id.is_empty()
        && !options.caster_id.is_empty()
        && !options.runtime_epoch.is_empty()
        && !options.relay_endpoint.is_empty()
}

fn unique_id(prefix: &str) -> String {
    let mut bytes = [0_u8; 16];
    rand::rng().fill(&mut bytes);
    format!(
        "{prefix}_{}",
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

fn source_cooldown_key(
    cluster_id: &str,
    mountpoint: &str,
    source_ip: &str,
) -> Result<String, AdmissionError> {
    rcaster_core::ntrip::validate_mountpoint(mountpoint)
        .map_err(|_| AdmissionError::InvalidIdentifier("mountpoint"))?;
    let fingerprint = format!("{mountpoint}\0{source_ip}")
        .bytes()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(format!(
        "/rcaster/{cluster_id}/runtime/source-cooldowns/{fingerprint}"
    ))
}

fn auth_failure_key(cluster_id: &str, key: &str) -> String {
    let fingerprint = format!("{:x}", Sha256::digest(key.as_bytes()));
    format!("/rcaster/{cluster_id}/runtime/auth-failures/{fingerprint}")
}

async fn publish_version_status(
    client: &mut Client,
    options: &ClusterRuntimeOptions,
    lease_id: i64,
    healthy: bool,
    maintenance: bool,
) -> Result<(), AdmissionError> {
    let key = format!(
        "/rcaster/{}/runtime/version-status/{}",
        options.cluster_id, options.caster_id
    );
    let value = serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "caster_id": options.caster_id,
        "software_version": env!("CARGO_PKG_VERSION"),
        "git_commit": option_env!("RCASTER_GIT_COMMIT"),
        "build_id": option_env!("RCASTER_BUILD_ID"),
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "config_schema": 1,
        "etcd_schema": 1,
        "api_schema": 1,
        "ntrip_features": ["ntrip1", "ntrip2", "rtcm3", "rtcm2", "binex", "cmr", "nmea", "near", "relay"],
        "runtime_epoch": options.runtime_epoch,
        "relay_endpoint": options.relay_endpoint,
        "started_at": now_timestamp(),
        "healthy": healthy,
        "maintenance": maintenance,
    }))?;
    client
        .put(key, value, Some(PutOptions::new().with_lease(lease_id)))
        .await?;
    Ok(())
}

fn now_timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

#[allow(clippy::too_many_arguments)]
async fn keep_lease_alive(
    client: Client,
    lease_id: Arc<AtomicI64>,
    options: ClusterRuntimeOptions,
    healthy: Arc<AtomicBool>,
    version_healthy: Arc<AtomicBool>,
    version_maintenance: Arc<AtomicBool>,
    lease_health: watch::Sender<bool>,
    mut stop: oneshot::Receiver<()>,
) {
    let interval = std::time::Duration::from_secs((options.lease_ttl_s / 3).max(1) as u64);
    let request_timeout = std::time::Duration::from_secs((options.lease_ttl_s / 6).max(1) as u64);
    loop {
        let current_lease = lease_id.load(Ordering::Acquire);
        let mut check_client = client.clone();
        let check = async {
            let (mut keeper, mut stream) =
                check_client.lease_keep_alive(current_lease).await.ok()?;
            keeper.keep_alive().await.ok()?;
            let response = stream.message().await.ok()??;
            (response.ttl() > 0).then_some(())
        };
        let healthy_lease = tokio::select! {
            _ = &mut stop => return,
            result = timeout(request_timeout, check) => result.ok().flatten().is_some(),
        };
        if healthy_lease {
            set_lease_health(&healthy, &lease_health, true);
            tokio::select! {
                _ = &mut stop => return,
                _ = sleep(interval) => {}
            }
            continue;
        }

        set_lease_health(&healthy, &lease_health, false);
        let mut grant_client = client.clone();
        let replacement = tokio::select! {
            _ = &mut stop => return,
            result = timeout(request_timeout, async {
                let replacement = grant_client
                    .lease_grant(options.lease_ttl_s, None)
                    .await
                    .ok()?
                    .id();
                if publish_version_status(
                    &mut grant_client,
                    &options,
                    replacement,
                    version_healthy.load(Ordering::Acquire),
                    version_maintenance.load(Ordering::Acquire),
                )
                .await
                .is_err()
                {
                    let _ = grant_client.lease_revoke(replacement).await;
                    return None;
                }
                Some(replacement)
            }) => result.ok().flatten(),
        };
        if let Some(replacement) = replacement {
            lease_id.store(replacement, Ordering::Release);
            set_lease_health(&healthy, &lease_health, true);
        }
        tokio::select! {
            _ = &mut stop => return,
            _ = sleep(interval) => {}
        }
    }
}

fn set_lease_health(healthy: &AtomicBool, lease_health: &watch::Sender<bool>, value: bool) {
    if healthy.swap(value, Ordering::AcqRel) != value {
        lease_health.send_replace(value);
    }
}

#[cfg(test)]
mod lease_tests {
    use super::*;
    use etcd_client::ConnectOptions;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn revoked_caster_lease_is_regranted_without_restart() {
        let endpoint = std::env::var("RCASTER_ETCD_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned());
        let mut client = Client::connect([endpoint], Some(ConnectOptions::new()))
            .await
            .expect("etcd must be running");
        let options = ClusterRuntimeOptions {
            cluster_id: unique_id("lease-recovery"),
            caster_id: unique_id("caster"),
            runtime_epoch: unique_id("epoch"),
            relay_endpoint: "127.0.0.1:12102".to_owned(),
            lease_ttl_s: 2,
            keep_alive: true,
        };
        let original = client
            .lease_grant(options.lease_ttl_s, None)
            .await
            .expect("initial lease")
            .id();
        let mut status_client = client.clone();
        publish_version_status(&mut status_client, &options, original, true, false)
            .await
            .expect("initial version status");

        let lease_id = Arc::new(AtomicI64::new(original));
        let healthy = Arc::new(AtomicBool::new(true));
        let version_healthy = Arc::new(AtomicBool::new(true));
        let version_maintenance = Arc::new(AtomicBool::new(false));
        let (lease_health, _) = watch::channel(true);
        let (stop_tx, stop_rx) = oneshot::channel();
        let task = tokio::spawn(keep_lease_alive(
            client.clone(),
            Arc::clone(&lease_id),
            options.clone(),
            Arc::clone(&healthy),
            Arc::clone(&version_healthy),
            Arc::clone(&version_maintenance),
            lease_health,
            stop_rx,
        ));

        client
            .lease_revoke(original)
            .await
            .expect("revoke original lease");
        let replacement = timeout(Duration::from_secs(10), async {
            loop {
                let current = lease_id.load(Ordering::Acquire);
                if current != original {
                    return current;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("lease supervisor must acquire a replacement");
        assert!(healthy.load(Ordering::Acquire));

        let status_key = format!(
            "/rcaster/{}/runtime/version-status/{}",
            options.cluster_id, options.caster_id
        );
        let status = status_client
            .get(status_key, None)
            .await
            .expect("read version status");
        assert_eq!(status.kvs()[0].lease(), replacement);

        stop_tx.send(()).expect("stop lease supervisor");
        task.await.expect("lease supervisor task");
        client
            .lease_revoke(replacement)
            .await
            .expect("revoke replacement lease");
    }
}
