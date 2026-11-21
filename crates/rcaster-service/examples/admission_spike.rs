use std::{
    collections::BTreeMap,
    env,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, ConnectOptions, PutOptions, Txn, TxnOp};
use rcaster_service::{
    admission::{
        AdmissionError, ClientAdmissionRequest, SessionRecord, admit_client, try_acquire_user_lock,
    },
    config::{CasterConfig, ConfigSnapshot, publish_config},
    coordinator_admission::{AdmissionCoordinator, CoordinatorOptions},
    revision_admission::admit_client_revision,
};
use tokio::{
    sync::{Semaphore, mpsc},
    time::{Instant, sleep, sleep_until},
};

#[derive(Clone, Debug)]
struct SpikeConfig {
    endpoints: Vec<String>,
    rate: u64,
    seconds: u64,
    mode: WorkloadMode,
    concurrency: usize,
    max_lock_retries: usize,
    preload_sessions: usize,
    ip_max_sessions: Option<usize>,
    admission_mode: AdmissionMode,
}

#[derive(Clone, Copy, Debug)]
enum WorkloadMode {
    Unique,
    Hot,
}

#[derive(Clone, Copy, Debug)]
enum AdmissionMode {
    Lock,
    Revision,
    Coordinator,
}

impl AdmissionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Lock => "lock",
            Self::Revision => "revision",
            Self::Coordinator => "coordinator",
        }
    }
}

impl WorkloadMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unique => "unique",
            Self::Hot => "hot",
        }
    }
}

#[derive(Clone)]
struct PreparedCaster {
    client: Client,
    lock_lease: i64,
    session_lease: i64,
    caster_id: String,
    runtime_epoch: String,
}

struct Sample {
    latency: Duration,
    retries: u32,
    result: Result<(), String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = parse_args()?;
    let mut admin = connect(config.endpoints.clone()).await?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_nanos()
        .to_string();
    let cluster = format!("admission-spike-{}-{run_id}", config.mode.as_str());
    let config_snapshot = publish_config(&mut admin, &cluster, CasterConfig::new(1)).await?;
    preload_sessions(&mut admin, &cluster, config.preload_sessions).await?;
    let workers = prepare_workers(config.endpoints.clone(), config.concurrency).await?;
    let coordinator = if matches!(config.admission_mode, AdmissionMode::Coordinator) {
        Some(Arc::new(
            AdmissionCoordinator::start(
                connect(config.endpoints.clone()).await?,
                cluster.clone(),
                CoordinatorOptions::default(),
            )
            .await?,
        ))
    } else {
        None
    };
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let in_flight = Arc::new(Semaphore::new(config.concurrency));
    let worker_cursor = Arc::new(AtomicUsize::new(0));
    let requested = config.rate * config.seconds;
    let started = Instant::now();

    for sequence in 0..requested {
        let target = started + Duration::from_secs_f64(sequence as f64 / config.rate as f64);
        sleep_until(target).await;
        let permit = in_flight.clone().acquire_owned().await?;
        let worker_index = worker_cursor.fetch_add(1, Ordering::Relaxed) % workers.len();
        let worker = workers[worker_index].clone();
        let sender = sender.clone();
        let config_snapshot = config_snapshot.clone();
        let cluster = cluster.clone();
        let user_id = match config.mode {
            WorkloadMode::Unique => format!("u_{sequence}"),
            WorkloadMode::Hot => "u_hot".to_owned(),
        };
        let max_sessions = match config.mode {
            WorkloadMode::Unique => 1,
            WorkloadMode::Hot => requested as usize + config.preload_sessions + 1,
        };
        let max_retries = config.max_lock_retries;
        let ip_max_sessions = config.ip_max_sessions;
        let admission_mode = config.admission_mode;
        let coordinator = coordinator.clone();

        tokio::spawn(async move {
            let request_started = Instant::now();
            let (result, retries) = admit_with_retry(
                worker,
                cluster,
                user_id,
                format!("s_{sequence}"),
                max_sessions,
                config_snapshot,
                max_retries,
                ip_max_sessions,
                admission_mode,
                coordinator,
            )
            .await;
            let _ = sender.send(Sample {
                latency: request_started.elapsed(),
                retries,
                result,
            });
            drop(permit);
        });
    }
    drop(sender);

    let mut latencies = Vec::with_capacity(requested as usize);
    let mut errors = BTreeMap::<String, usize>::new();
    let mut successes = 0_usize;
    let mut lock_retries = 0_u64;

    while let Some(sample) = receiver.recv().await {
        latencies.push(sample.latency);
        lock_retries += u64::from(sample.retries);
        match sample.result {
            Ok(()) => successes += 1,
            Err(class) => *errors.entry(class).or_default() += 1,
        }
    }

    if let Some(coordinator) = coordinator
        && let Ok(coordinator) = Arc::try_unwrap(coordinator)
    {
        coordinator.shutdown().await;
    }

    let elapsed = started.elapsed().as_secs_f64();
    latencies.sort_unstable();
    let failures = requested as usize - successes;
    println!(
        concat!(
            "{{\"mode\":\"{}\",\"target_rate\":{},\"requested\":{},\"seconds\":{},",
            "\"admission\":\"{}\",\"concurrency\":{},\"successes\":{},\"failures\":{},\"achieved_per_s\":{:.2},",
            "\"lock_retries\":{},\"p50_ms\":{:.3},\"p95_ms\":{:.3},\"p99_ms\":{:.3},",
            "\"elapsed_s\":{:.3},\"preload_sessions\":{},\"ip_max\":{:?},\"errors\":{:?}}}"
        ),
        config.mode.as_str(),
        config.rate,
        requested,
        config.seconds,
        config.admission_mode.as_str(),
        config.concurrency,
        successes,
        failures,
        requested as f64 / elapsed.max(f64::EPSILON),
        lock_retries,
        percentile_ms(&latencies, 0.50),
        percentile_ms(&latencies, 0.95),
        percentile_ms(&latencies, 0.99),
        elapsed,
        config.preload_sessions,
        config.ip_max_sessions,
        errors,
    );

    Ok(())
}

async fn prepare_workers(
    endpoints: Vec<String>,
    count: usize,
) -> Result<Vec<PreparedCaster>, Box<dyn Error>> {
    let mut workers = Vec::with_capacity(count);
    for index in 0..count {
        let mut client = connect(endpoints.clone()).await?;
        let lock_lease = client.lease_grant(120, None).await?.id();
        let session_lease = client.lease_grant(120, None).await?.id();
        workers.push(PreparedCaster {
            client,
            lock_lease,
            session_lease,
            caster_id: format!("spike-caster-{index}"),
            runtime_epoch: format!("spike-boot-{index}"),
        });
    }
    Ok(workers)
}

async fn preload_sessions(
    client: &mut Client,
    cluster: &str,
    count: usize,
) -> Result<(), Box<dyn Error>> {
    if count == 0 {
        return Ok(());
    }
    let lease = client.lease_grant(300, None).await?.id();
    let value = br#"{"seeded":true}"#.to_vec();
    for batch_start in (0..count).step_by(128) {
        let batch_end = (batch_start + 128).min(count);
        let operations = (batch_start..batch_end)
            .map(|index| {
                TxnOp::put(
                    format!("/rcaster/{cluster}/online/users/u_hot/sessions/seed_{index}"),
                    value.clone(),
                    Some(PutOptions::new().with_lease(lease)),
                )
            })
            .collect::<Vec<_>>();
        let response = client.txn(Txn::new().and_then(operations)).await?;
        if !response.succeeded() {
            return Err("session preload transaction failed".into());
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn admit_with_retry(
    mut worker: PreparedCaster,
    cluster: String,
    user_id: String,
    session_id: String,
    max_sessions: usize,
    config: ConfigSnapshot,
    max_retries: usize,
    ip_max_sessions: Option<usize>,
    admission_mode: AdmissionMode,
    coordinator: Option<Arc<AdmissionCoordinator>>,
) -> (Result<(), String>, u32) {
    if matches!(admission_mode, AdmissionMode::Coordinator) {
        let request = ClientAdmissionRequest::new(
            SessionRecord::client(
                session_id,
                user_id,
                "C63",
                worker.caster_id,
                worker.runtime_epoch,
                "198.51.100.10",
                "2026-11-22T02:00:00Z",
            ),
            max_sessions,
            ip_max_sessions,
            config,
        );
        let result = coordinator
            .expect("coordinator mode requires a coordinator")
            .admit_client(worker.session_lease, request)
            .await
            .map(|_| ())
            .map_err(classify_error);
        return (result, 0);
    }

    if matches!(admission_mode, AdmissionMode::Revision) {
        let request = ClientAdmissionRequest::new(
            SessionRecord::client(
                session_id,
                user_id,
                "C63",
                worker.caster_id,
                worker.runtime_epoch,
                "198.51.100.10",
                "2026-11-22T02:00:00Z",
            ),
            max_sessions,
            ip_max_sessions,
            config,
        );
        let result =
            admit_client_revision(&mut worker.client, worker.session_lease, &cluster, request)
                .await
                .map(|_| ())
                .map_err(classify_error);
        return (result, 0);
    }

    for attempt in 0..=max_retries {
        match try_acquire_user_lock(&mut worker.client, worker.lock_lease, &cluster, &user_id).await
        {
            Ok(lock) => {
                let request = ClientAdmissionRequest::new(
                    SessionRecord::client(
                        session_id.clone(),
                        user_id.clone(),
                        "C63",
                        worker.caster_id.clone(),
                        worker.runtime_epoch.clone(),
                        "198.51.100.10",
                        "2026-11-22T02:00:00Z",
                    ),
                    max_sessions,
                    ip_max_sessions,
                    config.clone(),
                );
                let result = admit_client(
                    &mut worker.client,
                    worker.session_lease,
                    &cluster,
                    lock,
                    request,
                )
                .await;
                match result {
                    Ok(_) => return (Ok(()), attempt as u32),
                    Err(AdmissionError::IpLockBusy) if attempt < max_retries => {
                        sleep(Duration::from_millis(1)).await;
                    }
                    Err(error) => return (Err(classify_error(error)), attempt as u32),
                }
            }
            Err(AdmissionError::UserLockBusy) if attempt < max_retries => {
                sleep(Duration::from_millis(1)).await;
            }
            Err(error) => return (Err(classify_error(error)), attempt as u32),
        }
    }
    unreachable!("retry loop always returns")
}

fn classify_error(error: AdmissionError) -> String {
    match error {
        AdmissionError::UserLockBusy => "USER_LOCK_BUSY".to_owned(),
        AdmissionError::IpLockBusy => "IP_LOCK_BUSY".to_owned(),
        AdmissionError::UserQuotaExceeded => "USER_QUOTA_EXCEEDED".to_owned(),
        AdmissionError::IpQuotaExceeded => "IP_QUOTA_EXCEEDED".to_owned(),
        AdmissionError::ConfigStale => "CONFIG_STALE".to_owned(),
        AdmissionError::AdmissionConflict => "ADMISSION_CONFLICT".to_owned(),
        AdmissionError::StaleLock => "STALE_LOCK".to_owned(),
        AdmissionError::Source(error) => format!("SOURCE_{error}"),
        other => other.to_string(),
    }
}

async fn connect(endpoints: Vec<String>) -> Result<Client, Box<dyn Error>> {
    Ok(Client::connect(endpoints, Some(ConnectOptions::new())).await?)
}

fn percentile_ms(values: &[Duration], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let index = ((values.len() - 1) as f64 * percentile).round() as usize;
    values[index].as_secs_f64() * 1000.0
}

fn parse_args() -> Result<SpikeConfig, Box<dyn Error>> {
    let mut rate = 100;
    let mut seconds = 5;
    let mut mode = WorkloadMode::Unique;
    let mut concurrency = 64;
    let mut max_lock_retries = 250;
    let mut preload_sessions = 0;
    let mut ip_max_sessions = None;
    let mut admission_mode = AdmissionMode::Lock;
    let args: Vec<String> = env::args().skip(1).collect();
    let mut index = 0;

    while index < args.len() {
        let key = &args[index];
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("missing value for {key}"))?;
        match key.as_str() {
            "--rate" => rate = value.parse()?,
            "--seconds" => seconds = value.parse()?,
            "--mode" => {
                mode = match value.as_str() {
                    "unique" => WorkloadMode::Unique,
                    "hot" => WorkloadMode::Hot,
                    _ => return Err(format!("unknown mode {value}").into()),
                }
            }
            "--concurrency" => concurrency = value.parse()?,
            "--max-lock-retries" => max_lock_retries = value.parse()?,
            "--preload-sessions" => preload_sessions = value.parse()?,
            "--ip-max" => ip_max_sessions = Some(value.parse()?),
            "--admission" => {
                admission_mode = match value.as_str() {
                    "lock" => AdmissionMode::Lock,
                    "revision" => AdmissionMode::Revision,
                    "coordinator" => AdmissionMode::Coordinator,
                    _ => return Err(format!("unknown admission mode {value}").into()),
                }
            }
            _ => return Err(format!("unknown argument {key}").into()),
        }
        index += 2;
    }

    if rate == 0 || seconds == 0 || concurrency == 0 {
        return Err("rate, seconds, and concurrency must be positive".into());
    }
    if preload_sessions > 0 && !matches!(mode, WorkloadMode::Hot) {
        return Err("--preload-sessions requires --mode hot".into());
    }
    let endpoints = env::var("RCASTER_ETCD_ENDPOINTS")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned())
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(ToOwned::to_owned)
        .collect();

    Ok(SpikeConfig {
        endpoints,
        rate,
        seconds,
        mode,
        concurrency,
        max_lock_retries,
        preload_sessions,
        ip_max_sessions,
        admission_mode,
    })
}
