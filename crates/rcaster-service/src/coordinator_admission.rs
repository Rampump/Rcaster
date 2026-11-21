use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use etcd_client::{
    Client, Compare, CompareOp, DeleteOptions, LeaseKeepAliveStream, LeaseKeeper, PutOptions, Txn,
    TxnOp,
};
use rand::Rng;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
    time::{Instant, sleep, sleep_until},
};

use crate::{
    admission::{
        AdmissionError, ClientAdmissionRequest, ClientPermit, config_compare, ip_session_key,
        read_ip_session_count, read_user_session_count, user_session_key, validate_key_segment,
        validate_session,
    },
    config::{ConfigSnapshot, load_config_snapshot},
};

#[derive(Clone, Debug)]
pub struct CoordinatorOptions {
    pub batch_size: usize,
    pub batch_window: Duration,
    pub owner_lease_ttl_s: i64,
    pub keep_alive: bool,
}

impl Default for CoordinatorOptions {
    fn default() -> Self {
        Self {
            batch_size: 64,
            batch_window: Duration::from_millis(5),
            owner_lease_ttl_s: 15,
            keep_alive: true,
        }
    }
}

#[derive(Debug)]
pub struct AdmissionCoordinator {
    sender: mpsc::Sender<PendingAdmission>,
    alive: Arc<AtomicBool>,
    done: oneshot::Receiver<()>,
}

struct PendingAdmission {
    lease_id: i64,
    request: ClientAdmissionRequest,
    response: oneshot::Sender<Result<ClientPermit, AdmissionError>>,
}

#[derive(Clone, Debug)]
struct OwnerIdentity {
    key: String,
    token: String,
    lease_id: i64,
    revision: i64,
}

impl OwnerIdentity {
    fn compares(&self) -> [Compare; 3] {
        [
            Compare::mod_revision(self.key.clone(), CompareOp::Equal, self.revision),
            Compare::value(self.key.clone(), CompareOp::Equal, self.token.clone()),
            Compare::lease(self.key.clone(), CompareOp::Equal, self.lease_id),
        ]
    }
}

impl AdmissionCoordinator {
    pub async fn start(
        client: Client,
        cluster_id: impl Into<String>,
        options: CoordinatorOptions,
    ) -> Result<Self, AdmissionError> {
        if options.batch_size == 0
            || options.batch_window.is_zero()
            || options.owner_lease_ttl_s <= 0
        {
            return Err(AdmissionError::InvalidIdentifier("coordinator options"));
        }

        let cluster_id = cluster_id.into();
        validate_key_segment(&cluster_id, "cluster ID")?;
        let owner = acquire_owner(&client, &cluster_id, options.owner_lease_ttl_s).await?;
        let alive = Arc::new(AtomicBool::new(true));
        let (sender, receiver) = mpsc::channel(4096);
        let (done_tx, done_rx) = oneshot::channel();
        let (keepalive_stop_tx, keepalive_stop_rx) = oneshot::channel();

        if options.keep_alive {
            let mut keepalive_client = client.clone();
            let (keeper, stream) = match keepalive_client.lease_keep_alive(owner.lease_id).await {
                Ok(keepalive) => keepalive,
                Err(error) => {
                    let mut cleanup_client = client.clone();
                    release_owner(&mut cleanup_client, &owner).await;
                    return Err(error.into());
                }
            };
            tokio::spawn(keep_owner_alive(
                keeper,
                stream,
                options.owner_lease_ttl_s,
                alive.clone(),
                keepalive_stop_rx,
            ));
        }

        tokio::spawn(run_coordinator(
            client,
            cluster_id,
            owner,
            options,
            receiver,
            alive.clone(),
            keepalive_stop_tx,
            done_tx,
        ));

        Ok(Self {
            sender,
            alive,
            done: done_rx,
        })
    }

    pub async fn admit_client(
        &self,
        lease_id: i64,
        request: ClientAdmissionRequest,
    ) -> Result<ClientPermit, AdmissionError> {
        if !self.alive.load(Ordering::Acquire) {
            return Err(AdmissionError::CoordinatorUnavailable);
        }
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(PendingAdmission {
                lease_id,
                request,
                response: response_tx,
            })
            .await
            .map_err(|_| AdmissionError::CoordinatorUnavailable)?;
        response_rx
            .await
            .map_err(|_| AdmissionError::CoordinatorUnavailable)?
    }

    pub async fn shutdown(mut self) {
        drop(self.sender);
        let _ = (&mut self.done).await;
    }
}

async fn acquire_owner(
    client: &Client,
    cluster_id: &str,
    lease_ttl_s: i64,
) -> Result<OwnerIdentity, AdmissionError> {
    let mut client = client.clone();
    let lease_id = client.lease_grant(lease_ttl_s, None).await?.id();
    let key = format!("/rcaster/{cluster_id}/runtime/admission-coordinator/owner");
    let token = owner_token();
    let transaction = Txn::new()
        .when([Compare::create_revision(key.clone(), CompareOp::Equal, 0)])
        .and_then([TxnOp::put(
            key.clone(),
            token.clone(),
            Some(PutOptions::new().with_lease(lease_id)),
        )]);
    let response = client.txn(transaction).await?;
    if !response.succeeded() {
        let _ = client.lease_revoke(lease_id).await;
        return Err(AdmissionError::CoordinatorUnavailable);
    }
    let revision = response
        .header()
        .ok_or(AdmissionError::CorruptState)?
        .revision();

    Ok(OwnerIdentity {
        key,
        token,
        lease_id,
        revision,
    })
}

async fn keep_owner_alive(
    mut keeper: LeaseKeeper,
    mut stream: LeaseKeepAliveStream,
    lease_ttl_s: i64,
    alive: Arc<AtomicBool>,
    mut stop: oneshot::Receiver<()>,
) {
    let interval = Duration::from_secs((lease_ttl_s / 3).max(1) as u64);
    loop {
        tokio::select! {
            _ = &mut stop => return,
            _ = sleep(interval) => {}
        }
        if keeper.keep_alive().await.is_err() {
            alive.store(false, Ordering::Release);
            return;
        }
        match stream.message().await {
            Ok(Some(response)) if response.ttl() > 0 => {}
            _ => {
                alive.store(false, Ordering::Release);
                return;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_coordinator(
    mut client: Client,
    cluster_id: String,
    owner: OwnerIdentity,
    options: CoordinatorOptions,
    mut receiver: mpsc::Receiver<PendingAdmission>,
    alive: Arc<AtomicBool>,
    keepalive_stop: oneshot::Sender<()>,
    done: oneshot::Sender<()>,
) {
    while let Some(first) = receiver.recv().await {
        let mut batch = vec![first];
        let deadline = Instant::now() + options.batch_window;
        let mut channel_closed = false;

        while batch.len() < options.batch_size {
            tokio::select! {
                request = receiver.recv() => {
                    match request {
                        Some(request) => batch.push(request),
                        None => {
                            channel_closed = true;
                            break;
                        }
                    }
                }
                _ = sleep_until(deadline) => break,
            }
        }

        process_batch(&mut client, &cluster_id, &owner, batch, alive.as_ref()).await;
        if channel_closed {
            break;
        }
    }

    alive.store(false, Ordering::Release);
    let _ = keepalive_stop.send(());
    release_owner(&mut client, &owner).await;
    let _ = done.send(());
}

async fn process_batch(
    client: &mut Client,
    cluster_id: &str,
    owner: &OwnerIdentity,
    batch: Vec<PendingAdmission>,
    alive: &AtomicBool,
) {
    let mut groups = BTreeMap::<i64, (ConfigSnapshot, Vec<PendingAdmission>)>::new();
    for pending in batch {
        if let Err(error) = validate_request(&pending.request) {
            let _ = pending.response.send(Err(error));
            continue;
        }
        let config = pending.request.config.clone();
        groups
            .entry(config.revision)
            .or_insert_with(|| (config, Vec::new()))
            .1
            .push(pending);
    }

    for (_, (config, group)) in groups {
        if let Err(error) = process_group(client, cluster_id, owner, &config, group, alive).await {
            alive.store(false, Ordering::Release);
            let _ = error;
            return;
        }
    }
}

async fn process_group(
    client: &mut Client,
    cluster_id: &str,
    owner: &OwnerIdentity,
    config: &ConfigSnapshot,
    group: Vec<PendingAdmission>,
    alive: &AtomicBool,
) -> Result<(), AdmissionError> {
    let users = group
        .iter()
        .map(|pending| pending.request.session.user_id.clone())
        .collect::<HashSet<_>>();
    let ips = group
        .iter()
        .filter(|pending| pending.request.ip_max_sessions.is_some())
        .filter_map(|pending| pending.request.session.client_ip.clone())
        .collect::<HashSet<_>>();
    let user_counts = count_users(client, cluster_id, users).await?;
    let ip_counts = count_ips(client, cluster_id, ips).await?;
    let mut accepted = Vec::new();
    let mut used_users = user_counts;
    let mut used_ips = ip_counts;

    for pending in group {
        let user_count = used_users
            .get(&pending.request.session.user_id)
            .copied()
            .unwrap_or(0);
        if user_count >= pending.request.max_sessions {
            let _ = pending
                .response
                .send(Err(AdmissionError::UserQuotaExceeded));
            continue;
        }

        if let Some(ip_max_sessions) = pending.request.ip_max_sessions {
            let client_ip = pending
                .request
                .session
                .client_ip
                .as_deref()
                .expect("validated request");
            let ip_count = used_ips.get(client_ip).copied().unwrap_or(0);
            if ip_count >= ip_max_sessions {
                let _ = pending.response.send(Err(AdmissionError::IpQuotaExceeded));
                continue;
            }
            *used_ips.entry(client_ip.to_owned()).or_default() += 1;
        }

        *used_users
            .entry(pending.request.session.user_id.clone())
            .or_default() += 1;
        accepted.push(pending);
    }

    if accepted.is_empty() {
        return Ok(());
    }

    let mut compares = vec![config_compare(cluster_id, config)];
    compares.extend(owner.compares());
    let mut prepared = Vec::with_capacity(accepted.len());
    let mut operations = Vec::with_capacity(accepted.len() * 2);
    for pending in accepted {
        let session_value = serde_json::to_vec(&pending.request.session)?;
        operations.push(TxnOp::put(
            user_session_key(cluster_id, &pending.request.session),
            session_value.clone(),
            Some(PutOptions::new().with_lease(pending.lease_id)),
        ));
        if pending.request.ip_max_sessions.is_some() {
            let client_ip = pending
                .request
                .session
                .client_ip
                .as_deref()
                .expect("validated request");
            operations.push(TxnOp::put(
                ip_session_key(cluster_id, client_ip, &pending.request.session.session_id),
                session_value,
                Some(PutOptions::new().with_lease(pending.lease_id)),
            ));
        }
        prepared.push(pending);
    }
    if operations.is_empty() {
        return Ok(());
    }

    let transaction = Txn::new().when(compares).and_then(operations);
    if client.txn(transaction).await?.succeeded() {
        for pending in prepared {
            let _ = pending.response.send(Ok(ClientPermit {
                session: pending.request.session,
            }));
        }
        return Ok(());
    }

    let config_stale = load_config_snapshot(client, cluster_id).await?.revision != config.revision;
    for pending in prepared {
        let error = if config_stale {
            AdmissionError::ConfigStale
        } else {
            AdmissionError::CoordinatorUnavailable
        };
        let _ = pending.response.send(Err(error));
    }
    if !config_stale {
        alive.store(false, Ordering::Release);
    }
    Ok(())
}

async fn count_users(
    client: &Client,
    cluster_id: &str,
    users: HashSet<String>,
) -> Result<HashMap<String, usize>, AdmissionError> {
    let mut tasks = JoinSet::new();
    for user_id in users {
        let mut client = client.clone();
        let cluster_id = cluster_id.to_owned();
        tasks.spawn(async move {
            let count = read_user_session_count(&mut client, &cluster_id, &user_id).await;
            (user_id, count)
        });
    }
    collect_counts(tasks).await
}

async fn count_ips(
    client: &Client,
    cluster_id: &str,
    ips: HashSet<String>,
) -> Result<HashMap<String, usize>, AdmissionError> {
    let mut tasks = JoinSet::new();
    for client_ip in ips {
        let mut client = client.clone();
        let cluster_id = cluster_id.to_owned();
        tasks.spawn(async move {
            let count = read_ip_session_count(&mut client, &cluster_id, &client_ip).await;
            (client_ip, count)
        });
    }
    collect_counts(tasks).await
}

async fn collect_counts(
    mut tasks: JoinSet<(String, Result<usize, AdmissionError>)>,
) -> Result<HashMap<String, usize>, AdmissionError> {
    let mut counts = HashMap::new();
    while let Some(joined) = tasks.join_next().await {
        let (key, result) = joined.map_err(|_| AdmissionError::CoordinatorUnavailable)?;
        counts.insert(key, result?);
    }
    Ok(counts)
}

fn validate_request(request: &ClientAdmissionRequest) -> Result<(), AdmissionError> {
    validate_session(&request.session)?;
    if request.ip_max_sessions.is_some() && request.session.client_ip.is_none() {
        return Err(AdmissionError::MissingClientIp);
    }
    Ok(())
}

async fn release_owner(client: &mut Client, owner: &OwnerIdentity) {
    let transaction = Txn::new()
        .when(owner.compares())
        .and_then([TxnOp::delete(owner.key.clone(), Some(DeleteOptions::new()))]);
    let _ = client.txn(transaction).await;
    let _ = client.lease_revoke(owner.lease_id).await;
}

fn owner_token() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
