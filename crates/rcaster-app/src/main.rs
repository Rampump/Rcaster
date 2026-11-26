use std::{
    collections::BTreeMap,
    env, error, fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rcaster_app::{
    accounts::AccountFile,
    backup::{prune_etcd_backups, prune_standalone_backups},
    config::{EtcdTlsConfig, StandaloneConfig, validate_private_key_permissions},
};
use rcaster_core::protocol::{AdmissionMode, AdmissionPolicy, DataFormat};
use rcaster_service::{
    account_auth::{AccountAuthorizer, hash_password},
    account_store::AccountStore,
    admin::{AdminState, serve_admin_mtls_reloadable},
    audit::AuditLog,
    auth::{
        AuthConcurrencyLimiter, AuthError, AuthorizedUser, ClientAuthorizer, ClientCredentials,
        SourceAuthorizer, SourceCredentials,
    },
    cluster_runtime::{ClusterRuntimeOptions, SharedClusterRuntime},
    connections::{CommandWatcher, ConnectionController},
    coordinator_admission::{AdmissionCoordinator, CoordinatorOptions},
    coordinator_grpc::{
        AdmissionAccessPolicy, GrpcAdmissionClient, GrpcClientTls, GrpcServerTls, ServiceToken,
        serve_admission_grpc, serve_admission_grpc_mtls,
    },
    etcd_reload::{EtcdTlsSource, ReloadableEtcdClient},
    health::{HealthState, clock_health, process_usage, serve_health_with_token},
    history_store::HistoryStore,
    hub::{HealthThresholds, HubLimits, MountpointHub},
    maintenance,
    ntrip_server::{NtripServerConfig, NtripTlsAcceptor, serve_ntrip, serve_ntrip_tls},
    operations::OperationManager,
    proxy_protocol::ProxyProtocolConfig,
    public::{PublicMapTiles, PublicState, serve_public_tls_reloadable},
    recovery::prepare_restored_cluster,
    relay::RelayManager,
    relay_grpc::serve_relay_grpc_mtls_reloadable_with_limit,
    session_limit::{ConnectionLimiter, LocalSessionLimiter},
    source_admission::SourceAdmissionConfig,
    tls_reload::ReloadableTlsConfig,
    upgrade::{UpgradeCommandWatcher, UpgradeManager, UpgradePolicy},
};
use tokio::{net::TcpListener, sync::watch};

type AppResult<T> = Result<T, Box<dyn error::Error + Send + Sync>>;

#[derive(Clone)]
struct PlaintextTransportAuthorizer<A> {
    inner: A,
    allow_client_auth: bool,
    allow_source_auth: bool,
}

impl<A> SourceAuthorizer for PlaintextTransportAuthorizer<A>
where
    A: SourceAuthorizer,
{
    async fn authorize_source(
        &self,
        mountpoint: &str,
        credentials: SourceCredentials<'_>,
    ) -> Result<AuthorizedUser, AuthError> {
        if !self.allow_source_auth {
            return Err(AuthError::Unauthorized);
        }
        self.inner.authorize_source(mountpoint, credentials).await
    }
}

impl<A> ClientAuthorizer for PlaintextTransportAuthorizer<A>
where
    A: ClientAuthorizer,
{
    async fn authorize_client(
        &self,
        mountpoint: &str,
        credentials: Option<ClientCredentials<'_>>,
    ) -> Result<AuthorizedUser, AuthError> {
        if !self.allow_client_auth {
            return Err(AuthError::Unauthorized);
        }
        self.inner.authorize_client(mountpoint, credentials).await
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("rcaster: {error}");
        std::process::exit(1);
    }
}

async fn run() -> AppResult<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        Some("serve") => serve(&args[1..]).await,
        Some("import-accounts") => import_accounts(&args[1..]),
        Some("backup-standalone") => backup_standalone(&args[1..]),
        Some("hash-password") => hash_password_command(),
        Some("etcd-backup") => etcd_backup(&args[1..]).await,
        Some("etcd-recovery-prepare") => etcd_recovery_prepare(&args[1..]).await,
        Some("coordinator") => serve_coordinator(&args[1..]).await,
        Some("--help" | "-h" | "help") | None => {
            print_usage();
            Ok(())
        }
        Some(command) => Err(format!("unknown command {command}").into()),
    }
}

async fn serve(args: &[String]) -> AppResult<()> {
    let config_path = required_option(args, "--config")?;
    let config = StandaloneConfig::load(&config_path)?;
    let etcd = match &config.cluster {
        Some(cluster) => {
            Some(connect_etcd(cluster.etcd_endpoints.clone(), cluster.etcd_tls.as_ref()).await?)
        }
        None => None,
    };
    let account_store = AccountStore::open(&config.account_db)?;
    let accounts = account_store.load()?;
    let mut coordinator = None;
    let cluster_runtime = if let Some(cluster) = &config.cluster {
        let client = etcd
            .as_ref()
            .ok_or("cluster etcd client unavailable")?
            .client();
        let options = ClusterRuntimeOptions {
            cluster_id: cluster.cluster_id.clone(),
            caster_id: cluster.caster_id.clone(),
            runtime_epoch: cluster.runtime_epoch.clone(),
            relay_endpoint: cluster.relay_endpoint.clone(),
            lease_ttl_s: cluster.lease_ttl_s,
            keep_alive: true,
        };
        let runtime = if let Some(endpoint) = &cluster.coordinator_endpoint {
            let admission = if let Some(tls) = &cluster.coordinator_tls {
                GrpcAdmissionClient::connect_mtls(
                    endpoint.clone(),
                    GrpcClientTls {
                        ca_certificate_pem: fs::read_to_string(&tls.ca_certificate)?,
                        certificate_pem: fs::read_to_string(&tls.certificate)?,
                        private_key_pem: fs::read_to_string(&tls.private_key)?,
                        server_name: tls.server_name.clone(),
                        service_token: tls.service_token.clone(),
                    },
                )
                .await?
            } else {
                GrpcAdmissionClient::connect(endpoint.clone()).await?
            };
            SharedClusterRuntime::start_with_grpc_client_and_accounts(
                client,
                admission,
                options,
                accounts.clone(),
            )
            .await?
        } else {
            let admission = Arc::new(
                AdmissionCoordinator::start(
                    client.clone(),
                    cluster.cluster_id.clone(),
                    CoordinatorOptions::default(),
                )
                .await?,
            );
            let runtime = SharedClusterRuntime::start_with_accounts(
                client,
                Arc::clone(&admission),
                options,
                accounts.clone(),
            )
            .await?;
            coordinator = Some(admission);
            runtime
        };
        Some(runtime)
    } else {
        None
    };
    let account_directory = match &cluster_runtime {
        Some(runtime) => runtime
            .account_directory()
            .await
            .ok_or("cluster account directory unavailable")?,
        None => rcaster_service::account_directory::AccountDirectory::with_store(
            accounts.clone(),
            account_store,
        )?,
    };
    let auth_limiter = AuthConcurrencyLimiter::new(config.auth_max_concurrency)?;
    let connections = Arc::new(ConnectionController::default());
    let command_watcher =
        if let (Some(cluster), Some(_)) = (config.cluster.as_ref(), cluster_runtime.as_ref()) {
            Some(
                CommandWatcher::start(
                    etcd.as_ref()
                        .ok_or("cluster etcd client unavailable")?
                        .client(),
                    &cluster.cluster_id,
                    &cluster.caster_id,
                    Arc::clone(&connections),
                )
                .await?,
            )
        } else {
            None
        };
    let hub = Arc::new(MountpointHub::new_with_health(
        config.subscriber_buffer_events,
        config.health_thresholds()?,
    )?);
    hub.set_limits(HubLimits {
        max_sources: config.max_sources,
        max_validating_sources: config.max_validating_sources,
        max_subscribers: config.max_subscribers,
        max_subscribers_per_source: config.max_subscribers_per_source,
    });
    let history_store = config
        .history_db
        .as_ref()
        .map(HistoryStore::open)
        .transpose()?
        .map(Arc::new);
    if let Some(store) = &history_store {
        for (mountpoint, points) in store.load()? {
            hub.restore_history(&mountpoint, points);
        }
    }
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let standalone_backup_task = if cluster_runtime.is_none() {
        config.standalone_backup_dir.as_ref().map(|backup_dir| {
            let backup_dir = backup_dir.clone();
            let accounts = account_directory.clone();
            let config_path = config_path.clone();
            let interval = Duration::from_secs(config.standalone_backup_interval_s);
            let retention = config.standalone_backup_retention;
            let mut shutdown = shutdown_rx.clone();
            tokio::spawn(async move {
                loop {
                    let elapsed = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default();
                    let output = backup_dir.join(format!(
                        "standalone-{}-{:09}",
                        elapsed.as_secs(),
                        elapsed.subsec_nanos()
                    ));
                    let snapshot = accounts.snapshot();
                    let source = config_path.clone();
                    let result = tokio::task::spawn_blocking(move || {
                        write_standalone_backup(&output, &snapshot, &source).map(|()| output)
                    })
                    .await;
                    match result {
                        Ok(Ok(output)) => {
                            println!("standalone backup written to {}", output.display());
                            match prune_standalone_backups(&backup_dir, retention) {
                                Ok(0) => {}
                                Ok(removed) => println!(
                                    "pruned {removed} standalone backup(s), retention={retention}"
                                ),
                                Err(error) => {
                                    eprintln!("standalone backup pruning failed: {error}")
                                }
                            }
                        }
                        Ok(Err(error)) => eprintln!("standalone backup failed: {error}"),
                        Err(error) => eprintln!("standalone backup task failed: {error}"),
                    }
                    tokio::select! {
                        _ = shutdown.changed() => return,
                        _ = tokio::time::sleep(interval) => {}
                    }
                }
            })
        })
    } else {
        None
    };
    let proxy_protocol = config
        .proxy_protocol
        .as_ref()
        .map(|proxy| ProxyProtocolConfig::new(&proxy.trusted_l4_cidrs))
        .transpose()
        .map_err(|error| format!("invalid PROXY protocol configuration: {error}"))?;
    let sampling_runtime = cluster_runtime.clone();
    let sampling_hub = Arc::clone(&hub);
    let sampling_store = history_store.clone();
    let sampling_shutdown = shutdown_rx.clone();
    let sampling_task = tokio::spawn(async move {
        let mut shutdown = sampling_shutdown;
        loop {
            if let Some(snapshot) = match &sampling_runtime {
                Some(runtime) => runtime.config_snapshot().await,
                None => None,
            } {
                sampling_hub.set_health_thresholds(HealthThresholds {
                    enabled: snapshot.config.source_health.enabled,
                    short_window: Duration::from_secs(snapshot.config.source_health.short_window_s),
                    long_window: Duration::from_secs(snapshot.config.source_health.long_window_s),
                    observation_timeout: Duration::from_secs(
                        snapshot.config.source_health.observation_timeout_s,
                    ),
                    valid_frame_ratio_min: snapshot.config.source_health.valid_frame_ratio_min,
                    max_crc_ratio: snapshot.config.source_health.crc_error_ratio_max,
                    alert_clear: Duration::from_secs(snapshot.config.source_health.alert_clear_s),
                    max_source_bitrate_bps: snapshot.config.limits.max_source_bitrate_bps,
                    station_id_change_alert: snapshot.config.source_health.station_id_change_alert,
                    base_position_jump_m: snapshot.config.source_health.base_position_jump_m,
                });
                sampling_hub.set_limits(HubLimits {
                    max_sources: snapshot.config.limits.max_sources,
                    max_validating_sources: snapshot.config.limits.max_validating_sources,
                    max_subscribers: snapshot.config.limits.max_subscribers,
                    max_subscribers_per_source: snapshot.config.limits.max_subscribers_per_source,
                });
            }
            sampling_hub.record_health_snapshots();
            if let Some(store) = &sampling_store {
                let histories = sampling_hub
                    .active_sources()
                    .into_iter()
                    .map(|mountpoint| {
                        (
                            mountpoint.clone(),
                            sampling_hub.source_history(&mountpoint, 300),
                        )
                    })
                    .collect::<std::collections::BTreeMap<_, _>>();
                let store = Arc::clone(store);
                let _ = tokio::task::spawn_blocking(move || store.save(&histories)).await;
            }
            tokio::select! {
                _ = shutdown.changed() => return,
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    });
    let mut relay_manager = None;
    let relay_task = if let (Some(cluster), Some(bind), Some(runtime)) = (
        config.cluster.as_ref(),
        config
            .cluster
            .as_ref()
            .and_then(|cluster| cluster.relay_bind.as_deref()),
        cluster_runtime.clone(),
    ) {
        let tls = cluster
            .coordinator_tls
            .as_ref()
            .ok_or("relay listener requires coordinator_tls")?;
        let ca_certificate_pem = fs::read_to_string(&tls.ca_certificate)?;
        let certificate_pem = fs::read_to_string(&tls.certificate)?;
        let private_key_pem = fs::read_to_string(&tls.private_key)?;
        let client_tls = GrpcClientTls {
            ca_certificate_pem: ca_certificate_pem.clone(),
            certificate_pem: certificate_pem.clone(),
            private_key_pem: private_key_pem.clone(),
            server_name: tls.server_name.clone(),
            service_token: tls.service_token.clone(),
        };
        relay_manager = Some(Arc::new(RelayManager::with_limits(
            Arc::clone(&hub),
            runtime.clone(),
            client_tls,
            cluster.relay_max_relays,
            cluster.relay_max_starting,
            Duration::from_millis(cluster.relay_idle_timeout_ms),
        )));
        let listener = TcpListener::bind(bind).await?;
        let relay_address = listener.local_addr()?;
        let policy = AdmissionAccessPolicy::new(
            &cluster.cluster_id,
            [ServiceToken::new(
                &tls.service_token,
                &cluster.cluster_id,
                ["relay:read"],
            )],
        );
        let relay_tls = if let Some(responder) = &tls.server_ocsp_responder {
            ReloadableTlsConfig::from_files_with_ocsp_responder_grpc(
                tls.certificate.clone(),
                tls.private_key.clone(),
                Some(tls.ca_certificate.clone()),
                config.relay_client_crl.clone(),
                tls.server_ocsp_response.clone(),
                responder.to_runtime_config(),
                Duration::from_secs(10),
            )?
        } else if let Some(ocsp_response) = &tls.server_ocsp_response {
            ReloadableTlsConfig::from_files_with_client_crl_and_ocsp_grpc(
                tls.certificate.clone(),
                tls.private_key.clone(),
                Some(tls.ca_certificate.clone()),
                config.relay_client_crl.clone(),
                ocsp_response.clone(),
                Duration::from_secs(10),
            )?
        } else {
            ReloadableTlsConfig::from_files_with_client_crl_grpc(
                tls.certificate.clone(),
                tls.private_key.clone(),
                Some(tls.ca_certificate.clone()),
                config.relay_client_crl.clone(),
                Duration::from_secs(10),
            )?
        };
        let task = tokio::spawn(serve_relay_grpc_mtls_reloadable_with_limit(
            listener,
            Arc::clone(&hub),
            runtime,
            relay_tls,
            policy,
            cluster.relay_max_inbound,
            wait_for_shutdown(shutdown_rx.clone()),
        ));
        println!("relay listening on {relay_address}");
        Some(task)
    } else {
        None
    };
    let health_state = Arc::new(HealthState::default());
    health_state.set_ready(true);
    let auth_prune_task = {
        let accounts = account_directory.clone();
        let mut shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            loop {
                accounts.prune_expired_local_auth();
                tokio::select! {
                    _ = shutdown.changed() => return,
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                }
            }
        })
    };
    let listener_tls = match (
        config.tls_certificate.as_ref(),
        config.tls_private_key.as_ref(),
    ) {
        (Some(certificate), Some(private_key)) => Some(match &config.tls_ocsp_responder {
            Some(responder) => ReloadableTlsConfig::from_files_with_ocsp_responder(
                certificate.clone(),
                private_key.clone(),
                None,
                None,
                config.tls_ocsp_response.clone(),
                responder.to_runtime_config(),
                Duration::from_secs(10),
            )?,
            None => match &config.tls_ocsp_response {
                Some(ocsp_response) => ReloadableTlsConfig::from_files_with_ocsp(
                    certificate.clone(),
                    private_key.clone(),
                    None,
                    ocsp_response.clone(),
                    Duration::from_secs(10),
                )?,
                None => ReloadableTlsConfig::from_files(
                    certificate.clone(),
                    private_key.clone(),
                    None,
                    Duration::from_secs(10),
                )?,
            },
        }),
        _ => None,
    };
    let maintenance = config
        .maintenance_state
        .as_ref()
        .map(|path| maintenance::load(path))
        .transpose()?
        .flatten()
        .unwrap_or(false);
    health_state.set_maintenance(maintenance);
    if let Some(runtime) = &cluster_runtime
        && maintenance
    {
        runtime.set_maintenance(true).await?;
    }
    let connection_limiter = Arc::new(ConnectionLimiter::with_accept_rate(
        config.max_connections,
        config.accept_rate_per_second,
    )?);
    let health_task = if let Some(bind) = config.health_bind.as_deref() {
        let listener = TcpListener::bind(bind).await?;
        let address = listener.local_addr()?;
        let task = tokio::spawn(serve_health_with_token(
            listener,
            Arc::clone(&health_state),
            config.health_metrics_token.clone(),
            wait_for_shutdown(shutdown_rx.clone()),
        ));
        let monitor_health = Arc::clone(&health_state);
        let monitor_runtime = cluster_runtime.clone();
        let monitor_hub = Arc::clone(&hub);
        let monitor_relay = relay_manager.clone();
        let monitor_connections = Arc::clone(&connection_limiter);
        let monitor_command_connections = Arc::clone(&connections);
        let monitor_tls = listener_tls.clone();
        let monitor_shutdown = shutdown_rx.clone();
        let monitor = tokio::spawn(async move {
            let mut shutdown = monitor_shutdown;
            let mut source_warning_states = BTreeMap::<String, u8>::new();
            let mut last_clock_sample = None::<std::time::Instant>;
            loop {
                if last_clock_sample
                    .is_none_or(|sampled| sampled.elapsed() >= Duration::from_secs(30))
                {
                    monitor_health.set_clock_health(
                        tokio::task::spawn_blocking(clock_health)
                            .await
                            .ok()
                            .flatten(),
                    );
                    last_clock_sample = Some(std::time::Instant::now());
                }
                let runtime_ready = match &monitor_runtime {
                    Some(runtime) => runtime.is_healthy().await,
                    None => true,
                };
                monitor_health
                    .set_ready(runtime_ready && monitor_command_connections.is_watch_healthy());
                monitor_health.set_dependency_health(match &monitor_runtime {
                    Some(runtime) => runtime.dependency_health().await.unwrap_or([false; 5]),
                    None => [true; 5],
                });
                if let Some(runtime) = &monitor_runtime {
                    let _ = runtime
                        .update_version_status(monitor_health.is_ready())
                        .await;
                    monitor_health.set_auth_failure_fallback(runtime.auth_failure_fallback_total());
                    if let Some(metrics) = runtime.runtime_metrics().await {
                        monitor_health.set_runtime_metrics(
                            metrics.admission_latency_micros,
                            metrics.lease_remaining_seconds,
                            metrics.watch_lag_revisions,
                            metrics.watch_recoveries,
                        );
                    }
                } else {
                    monitor_health.set_runtime_metrics(0, 0, 0, 0);
                }
                let relays = match &monitor_relay {
                    Some(manager) => manager.active_count().await,
                    None => 0,
                };
                match &monitor_relay {
                    Some(manager) => {
                        let metrics = manager.metrics().await;
                        monitor_health.set_relay_metrics(
                            metrics.starting,
                            metrics.failures,
                            metrics.idle_closes,
                        );
                    }
                    None => monitor_health.set_relay_metrics(0, 0, 0),
                }
                monitor_health.set_counts(monitor_hub.active_sources().len(), relays);
                monitor_health.set_connections(monitor_connections.active());
                monitor_health.set_tls_certificate_expiry(
                    monitor_tls
                        .as_ref()
                        .and_then(ReloadableTlsConfig::certificate_expiry),
                );
                monitor_health.set_hub_metrics(monitor_hub.metrics());
                let active_sources = monitor_hub.active_sources();
                for mountpoint in &active_sources {
                    let Some(health) = monitor_hub.source_health(mountpoint) else {
                        continue;
                    };
                    let previous = source_warning_states
                        .insert(mountpoint.clone(), health.warning_codes)
                        .unwrap_or_default();
                    if previous == 0 && health.warning_codes != 0 {
                        monitor_health.record_source_event(
                            mountpoint,
                            "health_alert",
                            Some(&format!("warning_codes={}", health.warning_codes)),
                            None,
                            None,
                        );
                    } else if previous != 0 && health.warning_codes == 0 {
                        monitor_health.record_source_event(
                            mountpoint,
                            "health_clear",
                            None,
                            None,
                            None,
                        );
                    }
                }
                source_warning_states.retain(|mountpoint, _| {
                    active_sources.iter().any(|active| active == mountpoint)
                });
                monitor_health.set_process_usage(process_usage());
                let sessions = match &monitor_runtime {
                    Some(runtime) => runtime.session_count().await,
                    None => 0,
                };
                monitor_health.set_sessions(sessions);
                let generation = match &monitor_runtime {
                    Some(runtime) => runtime
                        .config_snapshot()
                        .await
                        .map(|snapshot| snapshot.config.generation)
                        .unwrap_or_default(),
                    None => 0,
                };
                monitor_health.set_config_generation(generation);
                tokio::select! {
                    _ = shutdown.changed() => return,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        });
        println!("health listening on {address}");
        Some((task, monitor))
    } else {
        None
    };
    let upgrade_watcher = if config.upgrade.is_some() {
        let cluster = config
            .cluster
            .as_ref()
            .ok_or("upgrade support requires cluster")?;
        let client = etcd
            .as_ref()
            .ok_or("cluster etcd client unavailable")?
            .client();
        Some(UpgradeCommandWatcher::start(
            client,
            cluster.cluster_id.clone(),
            cluster.caster_id.clone(),
            Arc::clone(&health_state),
            Some(Arc::clone(&connections)),
            shutdown_tx.clone(),
        ))
    } else {
        None
    };
    let audit = config
        .audit_log
        .as_ref()
        .map(|path| {
            AuditLog::open_with_retention(
                path,
                config.audit_max_bytes,
                Duration::from_secs(config.audit_retention_days * 86_400),
            )
        })
        .transpose()?
        .unwrap_or_default();
    health_state.set_source_event_log(
        audit.clone(),
        config
            .cluster
            .as_ref()
            .map(|cluster| cluster.caster_id.clone())
            .unwrap_or_else(|| "standalone".to_owned()),
    );
    let audit_task = config.audit_log.as_ref().map(|_| {
        let audit = audit.clone();
        let mut shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            loop {
                let _ = audit.prune().await;
                tokio::select! {
                    _ = shutdown.changed() => return,
                    _ = tokio::time::sleep(Duration::from_secs(3600)) => {}
                }
            }
        })
    });
    let admin_task = if let Some(bind) = config.admin_bind.as_deref() {
        let listener = TcpListener::bind(bind).await?;
        let address = listener.local_addr()?;
        let (
            cluster_id,
            caster_id,
            service_token,
            admin_runtime,
            admin_relays,
            relay_tls,
            upgrades,
            reloadable_tls,
        ) = if let Some(cluster) = config.cluster.as_ref() {
            let tls = cluster
                .coordinator_tls
                .as_ref()
                .ok_or("admin API requires coordinator_tls")?;
            let upgrades = if let Some(upgrade) = &config.upgrade {
                let policy =
                    UpgradePolicy::new(&upgrade.trusted_public_keys, &upgrade.artifact_hosts)
                        .map_err(|error| format!("invalid upgrade policy: {error}"))?;
                Some(UpgradeManager::new_with_approval(
                    etcd.as_ref()
                        .ok_or("cluster etcd client unavailable")?
                        .client(),
                    cluster.cluster_id.clone(),
                    policy,
                    upgrade.require_approval,
                ))
            } else {
                None
            };
            let reloadable_tls = match &tls.server_ocsp_responder {
                Some(responder) => ReloadableTlsConfig::from_files_with_ocsp_responder(
                    tls.certificate.clone(),
                    tls.private_key.clone(),
                    Some(tls.ca_certificate.clone()),
                    config.admin_client_crl.clone(),
                    tls.server_ocsp_response.clone(),
                    responder.to_runtime_config(),
                    Duration::from_secs(10),
                )?,
                None => match &tls.server_ocsp_response {
                    Some(ocsp_response) => {
                        ReloadableTlsConfig::from_files_with_client_crl_and_ocsp(
                            tls.certificate.clone(),
                            tls.private_key.clone(),
                            Some(tls.ca_certificate.clone()),
                            config.admin_client_crl.clone(),
                            ocsp_response.clone(),
                            Duration::from_secs(10),
                        )?
                    }
                    None => ReloadableTlsConfig::from_files_with_client_crl(
                        tls.certificate.clone(),
                        tls.private_key.clone(),
                        Some(tls.ca_certificate.clone()),
                        config.admin_client_crl.clone(),
                        Duration::from_secs(10),
                    )?,
                },
            };
            (
                cluster.cluster_id.clone(),
                cluster.caster_id.clone(),
                tls.service_token.clone(),
                cluster_runtime.clone(),
                relay_manager.clone(),
                Some(GrpcClientTls {
                    ca_certificate_pem: fs::read_to_string(&tls.ca_certificate)?,
                    certificate_pem: fs::read_to_string(&tls.certificate)?,
                    private_key_pem: fs::read_to_string(&tls.private_key)?,
                    server_name: tls.server_name.clone(),
                    service_token: tls.service_token.clone(),
                }),
                upgrades,
                reloadable_tls,
            )
        } else {
            let certificate = config
                .tls_certificate
                .clone()
                .ok_or("standalone admin API requires tls_certificate")?;
            let private_key = config
                .tls_private_key
                .clone()
                .ok_or("standalone admin API requires tls_private_key")?;
            let reloadable_tls = match (&config.tls_ocsp_responder, &config.tls_ocsp_response) {
                (Some(responder), _) => ReloadableTlsConfig::from_files_with_ocsp_responder(
                    certificate,
                    private_key,
                    None,
                    None,
                    config.tls_ocsp_response.clone(),
                    responder.to_runtime_config(),
                    Duration::from_secs(10),
                )?,
                (None, Some(ocsp_response)) => ReloadableTlsConfig::from_files_with_ocsp(
                    certificate,
                    private_key,
                    None,
                    ocsp_response.clone(),
                    Duration::from_secs(10),
                )?,
                (None, None) => ReloadableTlsConfig::from_files(
                    certificate,
                    private_key,
                    None,
                    Duration::from_secs(10),
                )?,
            };
            (
                "standalone".to_owned(),
                "standalone".to_owned(),
                String::new(),
                None,
                None,
                None,
                None,
                reloadable_tls,
            )
        };
        let state = AdminState {
            cluster_id,
            caster_id,
            token: service_token,
            ready: Arc::clone(&health_state),
            hub: Arc::clone(&hub),
            runtime: admin_runtime,
            relays: admin_relays,
            near_mountpoint: config
                .near
                .as_ref()
                .filter(|near| near.enabled)
                .map(|near| near.mountpoint.clone()),
            accounts: account_directory.clone(),
            relay_tls,
            upgrades,
            proxy_protocol: proxy_protocol.clone(),
            maintenance_state: config.maintenance_state.clone(),
            listener_tls: listener_tls.clone(),
            config_revision_retention: config.config_revision_retention,
            login_limiter: Default::default(),
            audit: audit.clone(),
            websockets: Default::default(),
            connections: Arc::clone(&connections),
            auth_limiter: auth_limiter.clone(),
        };
        let task = tokio::spawn(serve_admin_mtls_reloadable(
            listener,
            reloadable_tls,
            state,
            wait_for_shutdown(shutdown_rx.clone()),
        ));
        println!("admin listening on {address}");
        Some(task)
    } else {
        None
    };
    let operation_recovery_task = if config.admin_bind.is_some() {
        match (cluster_runtime.clone(), config.cluster.as_ref()) {
            (Some(runtime), Some(cluster)) => {
                let manager = OperationManager::new(
                    runtime,
                    account_directory.clone(),
                    cluster.cluster_id.clone(),
                    cluster.caster_id.clone(),
                    Some(Arc::clone(&health_state)),
                )
                .with_connections(Arc::clone(&connections));
                let mut shutdown = shutdown_rx.clone();
                Some(tokio::spawn(async move {
                    loop {
                        if let Err(error) = manager.recover_stale_operations().await {
                            eprintln!("rcaster: operation recovery failed: {error:?}");
                        }
                        tokio::select! {
                            _ = shutdown.changed() => return,
                            _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                        }
                    }
                }))
            }
            _ => None,
        }
    } else {
        None
    };
    let public_task = if let Some(bind) = config.public_bind.as_deref() {
        let listener = TcpListener::bind(bind).await?;
        let address = listener.local_addr()?;
        let mut public_websockets = rcaster_service::public::PublicMonitorLimiter::default();
        public_websockets.track_metrics(Arc::clone(&health_state));
        let mut monitor_starts = rcaster_service::public::MonitorStartLimiter::default();
        monitor_starts.track_metrics(Arc::clone(&health_state));
        let reloadable_tls = listener_tls
            .clone()
            .ok_or("public API requires tls_certificate")?;
        let task = tokio::spawn(serve_public_tls_reloadable(
            listener,
            reloadable_tls,
            PublicState {
                hub: Arc::clone(&hub),
                runtime: cluster_runtime.clone(),
                accounts: account_directory.clone(),
                relays: relay_manager.clone(),
                audit: audit.clone(),
                websockets: public_websockets,
                login_limiter: Default::default(),
                monitor_starts,
                map_tiles: config
                    .public_map_tiles
                    .as_ref()
                    .map(|tiles| PublicMapTiles {
                        url_template: tiles.url_template.clone(),
                        attribution: tiles.attribution.clone(),
                        min_zoom: tiles.min_zoom,
                        max_zoom: tiles.max_zoom,
                        coordinate_system: tiles.coordinate_system,
                    }),
                proxy_protocol: proxy_protocol.clone(),
                connections: Arc::clone(&connections),
                auth_limiter: auth_limiter.clone(),
            },
            wait_for_shutdown(shutdown_rx.clone()),
        ));
        println!("public API listening on {address}");
        Some(task)
    } else {
        None
    };
    let listener = TcpListener::bind(&config.bind).await?;
    let address = listener.local_addr()?;
    let tls = match (
        config.tls_bind.as_deref(),
        config.tls_certificate.as_deref(),
        config.tls_private_key.as_deref(),
    ) {
        (Some(bind), Some(_certificate), Some(_private_key)) => {
            let acceptor = NtripTlsAcceptor::from_reloadable(
                listener_tls
                    .clone()
                    .ok_or("NTRIP TLS requires tls_certificate")?,
            );
            let listener = TcpListener::bind(bind).await?;
            Some((listener.local_addr()?, listener, acceptor))
        }
        (None, _, _) => None,
        (Some(_), _, _) => return Err("incomplete TLS configuration".into()),
    };
    let source_admission =
        config
            .source_admission
            .as_ref()
            .map(|admission| SourceAdmissionConfig {
                policy: AdmissionPolicy {
                    mode: if admission.mode == "enforce" {
                        AdmissionMode::Enforce
                    } else {
                        AdmissionMode::Observe
                    },
                    allowed_formats: admission
                        .allowed_formats
                        .iter()
                        .map(|format| match format.as_str() {
                            "nmea" => DataFormat::Nmea,
                            "rtcm3" => DataFormat::Rtcm3,
                            "rtcm2" => DataFormat::Rtcm2,
                            "binex" => DataFormat::Binex,
                            "cmr" => DataFormat::Cmr,
                            _ => unreachable!("validated source admission data format"),
                        })
                        .collect(),
                    require_correction_data: admission.require_correction_data,
                    expected_formats: None,
                    allowed_msm_versions: admission
                        .msm
                        .restrict_versions
                        .then(|| admission.msm.allowed_versions.clone()),
                    min_valid_frames: admission.min_valid_frames,
                    min_correction_frames: admission.min_correction_frames,
                    max_crc_error_ratio: admission.max_crc_error_ratio,
                    require_base_position: admission.require_base_position,
                    require_msm: admission.msm.require_any,
                    max_unknown_ratio: admission.max_unknown_ratio,
                    min_observed_bytes: admission.min_observed_bytes,
                },
                max_bytes: admission.max_bytes,
                max_frames: admission.max_frames,
                timeout: Duration::from_millis(admission.timeout_ms),
                read_timeout: Duration::from_millis(admission.read_timeout_ms),
                str_refresh: admission.str_refresh.enabled,
            });
    let server_config = NtripServerConfig {
        head_timeout: Duration::from_millis(config.head_timeout_ms),
        content_length_max_bytes: config.content_length_max_bytes,
        client_auth_required: config.client_auth_required,
        session_limiter: Arc::new(LocalSessionLimiter::new(config.session_limits)?),
        connection_limiter,
        cluster_runtime: cluster_runtime.clone(),
        relay_manager,
        source_admission,
        accounts: Some(account_directory.clone()),
        near: config
            .near
            .as_ref()
            .filter(|near| near.enabled)
            .map(|near| rcaster_service::ntrip_server::NearServerConfig {
                mountpoint: near.mountpoint.clone(),
                selector: rcaster_service::near::NearConfig {
                    max_distance_m: near.max_distance_m,
                    hysteresis_m: near.hysteresis_m,
                    base_position_uncertainty_m: near.base_position_uncertainty_m,
                    hdop_m_per_unit: near.hdop_m_per_unit,
                    max_hdop: near.max_hdop,
                    switch_position_threshold_m: near.switch_position_threshold_m,
                    observe_time: Duration::from_millis(near.switch_observe_ms),
                    min_recompute_interval: Duration::from_millis(near.min_recompute_interval_ms),
                    max_recompute_interval: Duration::from_millis(near.max_recompute_interval_ms),
                    min_recompute_distance_m: near.min_recompute_distance_m,
                    max_spatial_queries_per_second: near.max_spatial_queries_per_second,
                    max_query_concurrency: near.max_query_concurrency,
                    gga_max_input_per_second: near.gga_max_input_per_second,
                    gga_max_jump_speed_mps: near.gga_max_jump_speed_mps,
                    position_ttl_s: near.position_ttl_s,
                },
                query_limiter: Arc::new(
                    rcaster_service::near::NearQueryLimiter::new(
                        near.max_spatial_queries_per_second,
                        near.max_query_concurrency,
                    )
                    .expect("validated NEAR query limiter"),
                ),
                no_source_timeout: Duration::from_millis(near.no_source_timeout_ms),
                advertise: near.advertise,
            }),
        metrics: Some(Arc::clone(&health_state)),
        connections: Some(connections),
        proxy_protocol,
        auth_login_limiter: Some(Default::default()),
    };
    let authorizer = AccountAuthorizer::from_directory_and_limiter(account_directory, auth_limiter);
    let clear_authorizer = PlaintextTransportAuthorizer {
        inner: authorizer.clone(),
        allow_client_auth: !config.client_auth_require_tls,
        allow_source_auth: !config.source_auth_require_tls,
    };
    let clear_task = tokio::spawn(serve_ntrip(
        listener,
        Arc::clone(&hub),
        clear_authorizer,
        server_config.clone(),
        wait_for_shutdown(shutdown_rx.clone()),
    ));
    let tls_task = tls.map(|(address, listener, acceptor)| {
        let task = tokio::spawn(serve_ntrip_tls(
            listener,
            acceptor,
            hub,
            authorizer,
            server_config,
            Duration::from_millis(config.tls_handshake_timeout_ms),
            wait_for_shutdown(shutdown_rx.clone()),
        ));
        (address, task)
    });

    println!("listening on {address}");
    if let Some((address, _)) = &tls_task {
        println!("listening on TLS {address}");
    }
    io::stdout().flush()?;
    tokio::select! {
        _ = wait_for_termination_signal() => {}
        _ = wait_for_shutdown(shutdown_rx.clone()) => {}
    }
    health_state.set_ready(false);
    if let Some((_, monitor)) = &health_task {
        monitor.abort();
    }
    let _ = shutdown_tx.send(true);
    clear_task.await??;
    if let Some((_, task)) = tls_task {
        task.await??;
    }
    if let Some(task) = relay_task {
        task.await??;
    }
    if let Some((task, monitor)) = health_task {
        task.await??;
        monitor.abort();
    }
    sampling_task.await?;
    auth_prune_task.abort();
    let _ = auth_prune_task.await;
    if let Some(task) = standalone_backup_task {
        task.await?;
    }
    if let Some(task) = audit_task {
        task.await?;
    }
    if let Some(task) = admin_task {
        task.await??;
    }
    if let Some(task) = operation_recovery_task {
        task.await?;
    }
    if let Some(task) = public_task {
        task.await??;
    }
    if let Some(watcher) = upgrade_watcher {
        watcher.shutdown().await;
    }
    if let Some(watcher) = command_watcher {
        watcher.shutdown().await;
    }
    if let Some(runtime) = cluster_runtime {
        runtime.shutdown().await?;
    }
    if let Some(coordinator) = coordinator
        && let Ok(coordinator) = Arc::try_unwrap(coordinator)
    {
        coordinator.shutdown().await;
    }
    Ok(())
}

async fn wait_for_shutdown(mut receiver: watch::Receiver<bool>) {
    while !*receiver.borrow() {
        if receiver.changed().await.is_err() {
            break;
        }
    }
}

async fn wait_for_termination_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn import_accounts(args: &[String]) -> AppResult<()> {
    let database = required_option(args, "--database")?;
    let input = required_option(args, "--input")?;
    let account_file = AccountFile::from_slice(&fs::read(input)?)?;
    let snapshot = account_file.into_snapshot()?;
    let users = snapshot.users().count();
    let mountpoints = snapshot.mountpoints().count();
    AccountStore::open(database)?.replace(&snapshot)?;
    println!("imported {users} users and {mountpoints} mountpoints");
    Ok(())
}

fn hash_password_command() -> AppResult<()> {
    let mut password = String::new();
    io::stdin().read_to_string(&mut password)?;
    let password = password.trim_end_matches(['\r', '\n']);
    if password.is_empty() {
        return Err("password must not be empty".into());
    }
    println!(
        "{}",
        hash_password(password).map_err(|error| format!("password hash failed: {error}"))?
    );
    Ok(())
}

async fn serve_coordinator(args: &[String]) -> AppResult<()> {
    let etcd_endpoint = required_string(args, "--etcd-endpoint")?;
    let cluster_id = required_string(args, "--cluster-id")?;
    let bind = required_string(args, "--bind")?;
    let etcd_ca = optional_string(args, "--etcd-ca");
    let etcd_certificate = optional_string(args, "--etcd-certificate");
    let etcd_private_key = optional_string(args, "--etcd-private-key");
    let etcd_domain = optional_string(args, "--etcd-domain");
    let etcd_tls_fields = [
        etcd_ca.is_some(),
        etcd_certificate.is_some(),
        etcd_private_key.is_some(),
        etcd_domain.is_some(),
    ];
    if etcd_tls_fields.iter().any(|configured| *configured)
        && etcd_tls_fields.iter().any(|configured| !*configured)
    {
        return Err("all etcd mTLS options are required together".into());
    }
    let etcd_tls = match (etcd_ca, etcd_certificate, etcd_private_key, etcd_domain) {
        (Some(ca_certificate), Some(certificate), Some(private_key), Some(domain_name)) => {
            Some(EtcdTlsConfig {
                ca_certificate: ca_certificate.into(),
                certificate: certificate.into(),
                private_key: private_key.into(),
                domain_name,
            })
        }
        _ => None,
    };
    if let Some(tls) = &etcd_tls {
        validate_private_key_permissions(&tls.private_key)?;
    }
    let _etcd = connect_etcd([etcd_endpoint], etcd_tls.as_ref()).await?;
    let client = _etcd.client();
    let coordinator = Arc::new(
        AdmissionCoordinator::start(client, cluster_id.clone(), CoordinatorOptions::default())
            .await?,
    );
    let listener = TcpListener::bind(&bind).await?;
    println!("coordinator listening on {}", listener.local_addr()?);
    io::stdout().flush()?;
    let tls_certificate = optional_string(args, "--tls-certificate");
    let tls_private_key = optional_string(args, "--tls-private-key");
    let client_ca = optional_string(args, "--client-ca");
    let service_token = optional_string(args, "--service-token");
    let tls_fields = [
        tls_certificate.is_some(),
        tls_private_key.is_some(),
        client_ca.is_some(),
        service_token.is_some(),
    ];
    if tls_fields.iter().any(|configured| *configured)
        && tls_fields.iter().any(|configured| !*configured)
    {
        return Err("all coordinator mTLS options are required together".into());
    }
    if let Some(private_key) = &tls_private_key {
        validate_private_key_permissions(Path::new(private_key))?;
    }
    let server_coordinator = Arc::clone(&coordinator);
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    if let (Some(certificate), Some(private_key), Some(client_ca), Some(service_token)) =
        (tls_certificate, tls_private_key, client_ca, service_token)
    {
        let policy = AdmissionAccessPolicy::new(
            cluster_id.clone(),
            [ServiceToken::new(
                service_token,
                cluster_id.clone(),
                ["admission:write"],
            )],
        );
        serve_admission_grpc_mtls(
            listener,
            server_coordinator,
            GrpcServerTls {
                certificate_pem: fs::read_to_string(certificate)?,
                private_key_pem: fs::read_to_string(private_key)?,
                client_ca_pem: fs::read_to_string(client_ca)?,
                client_crl_pem: None,
                server_ocsp_der: None,
            },
            policy,
            shutdown,
        )
        .await?;
    } else {
        serve_admission_grpc(listener, server_coordinator, shutdown).await?;
    }
    if let Ok(coordinator) = Arc::try_unwrap(coordinator) {
        coordinator.shutdown().await;
    }
    Ok(())
}

async fn etcd_backup(args: &[String]) -> AppResult<()> {
    let etcd_endpoint = required_string(args, "--etcd-endpoint")?;
    let explicit_output = optional_string(args, "--output").map(PathBuf::from);
    let backup_dir = optional_string(args, "--backup-dir").map(PathBuf::from);
    if explicit_output.is_some() == backup_dir.is_some() {
        return Err("exactly one of --output or --backup-dir is required".into());
    }
    let retention = optional_string(args, "--retention")
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(168);
    if !(1..=10_000).contains(&retention) {
        return Err("--retention must be between 1 and 10000".into());
    }
    if explicit_output.is_some() && optional_string(args, "--retention").is_some() {
        return Err("--retention requires --backup-dir".into());
    }
    let output = match (explicit_output, backup_dir.as_ref()) {
        (Some(output), None) => output,
        (None, Some(directory)) => {
            fs::create_dir_all(directory)?;
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            directory.join(format!("rcaster-{timestamp}.db"))
        }
        _ => unreachable!("output mode was validated"),
    };
    if output.exists() {
        return Err(format!("refusing to overwrite {}", output.display()).into());
    }
    let etcd_tls = etcd_tls_from_args(args)?;
    let _etcd = connect_etcd([etcd_endpoint], etcd_tls.as_ref()).await?;
    let mut client = _etcd.client();
    let mut snapshot = client.snapshot().await?;
    let temporary = PathBuf::from(format!("{}.tmp", output.display()));
    if temporary.exists() {
        return Err(format!(
            "temporary snapshot file already exists: {}",
            temporary.display()
        )
        .into());
    }
    if let Some(parent) = output.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let result = async {
        let mut file = create_private_file(&temporary)?;
        while let Some(response) = snapshot.message().await? {
            file.write_all(response.blob())?;
        }
        file.sync_all()?;
        fs::hard_link(&temporary, &output)?;
        let _ = fs::remove_file(&temporary);
        Ok::<_, Box<dyn error::Error + Send + Sync>>(())
    }
    .await;
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result?;
    println!("etcd snapshot written to {}", output.display());
    if let Some(directory) = backup_dir {
        let removed = prune_etcd_backups(&directory, retention)?;
        println!("etcd backup retention kept {retention} snapshot(s), removed {removed}");
    }
    Ok(())
}

fn backup_standalone(args: &[String]) -> AppResult<()> {
    let database = required_option(args, "--database")?;
    let config = required_option(args, "--config")?;
    let output = required_option(args, "--output-dir")?;
    if output.exists() {
        return Err(format!("refusing to overwrite {}", output.display()).into());
    }
    StandaloneConfig::load(&config)?;
    let accounts = AccountStore::open_existing(&database)?.load()?;
    write_standalone_backup(&output, &accounts, &config)?;
    println!(
        "standalone backup written to {} ({} users, {} mountpoints)",
        output.display(),
        accounts.users().count(),
        accounts.mountpoints().count()
    );
    Ok(())
}

fn write_standalone_backup(
    output: &Path,
    accounts: &rcaster_core::account::AccountSnapshot,
    config: &Path,
) -> AppResult<()> {
    StandaloneConfig::load(config)?;
    if let Some(parent) = output.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let name = output
        .file_name()
        .ok_or("backup output must have a file name")?
        .to_string_lossy();
    let temporary = output.with_file_name(format!(".{name}.tmp"));
    if temporary.exists() {
        return Err(format!("temporary backup directory exists: {}", temporary.display()).into());
    }
    fs::create_dir(&temporary)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o700))?;
    }
    let result = (|| {
        let account_file = serde_json::json!({
            "users": accounts.users().collect::<Vec<_>>(),
            "mountpoints": accounts.mountpoints().collect::<Vec<_>>(),
        });
        write_synced(
            &temporary.join("accounts.json"),
            serde_json::to_vec_pretty(&account_file)?,
        )?;
        write_synced(&temporary.join("caster.json"), fs::read(config)?)?;
        if output.exists() {
            return Err(format!("refusing to overwrite {}", output.display()).into());
        }
        fs::rename(&temporary, output)?;
        Ok::<_, Box<dyn error::Error + Send + Sync>>(())
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&temporary);
    }
    result
}

fn write_synced(path: &Path, bytes: Vec<u8>) -> io::Result<()> {
    let mut file = create_private_file(path)?;
    file.write_all(&bytes)?;
    file.sync_all()
}

fn create_private_file(path: &Path) -> io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

async fn etcd_recovery_prepare(args: &[String]) -> AppResult<()> {
    let etcd_endpoint = required_string(args, "--etcd-endpoint")?;
    let cluster_id = required_string(args, "--cluster-id")?;
    let confirmation = required_string(args, "--confirm")?;
    if confirmation != cluster_id {
        return Err("--confirm must exactly match --cluster-id".into());
    }
    let etcd_tls = etcd_tls_from_args(args)?;
    let _etcd = connect_etcd([etcd_endpoint], etcd_tls.as_ref()).await?;
    let mut client = _etcd.client();
    let report = prepare_restored_cluster(&mut client, &cluster_id).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn etcd_tls_from_args(args: &[String]) -> AppResult<Option<EtcdTlsConfig>> {
    let etcd_ca = optional_string(args, "--etcd-ca");
    let etcd_certificate = optional_string(args, "--etcd-certificate");
    let etcd_private_key = optional_string(args, "--etcd-private-key");
    let etcd_domain = optional_string(args, "--etcd-domain");
    let tls_fields = [
        etcd_ca.is_some(),
        etcd_certificate.is_some(),
        etcd_private_key.is_some(),
        etcd_domain.is_some(),
    ];
    if tls_fields.iter().any(|configured| *configured)
        && tls_fields.iter().any(|configured| !*configured)
    {
        return Err("all etcd mTLS options are required together".into());
    }
    let tls = match (etcd_ca, etcd_certificate, etcd_private_key, etcd_domain) {
        (Some(ca_certificate), Some(certificate), Some(private_key), Some(domain_name)) => {
            Some(EtcdTlsConfig {
                ca_certificate: ca_certificate.into(),
                certificate: certificate.into(),
                private_key: private_key.into(),
                domain_name,
            })
        }
        _ => None,
    };
    if let Some(tls) = &tls {
        validate_private_key_permissions(&tls.private_key)?;
    }
    Ok(tls)
}

fn required_option(args: &[String], name: &str) -> AppResult<PathBuf> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| PathBuf::from(&pair[1]))
        .ok_or_else(|| format!("missing required option {name}").into())
}

fn required_string(args: &[String], name: &str) -> AppResult<String> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
        .ok_or_else(|| format!("missing required option {name}").into())
}

fn optional_string(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
}

async fn connect_etcd(
    endpoints: impl AsRef<[String]>,
    tls: Option<&EtcdTlsConfig>,
) -> AppResult<ReloadableEtcdClient> {
    ReloadableEtcdClient::connect(
        endpoints.as_ref().to_vec(),
        tls.map(|tls| EtcdTlsSource {
            ca_certificate: tls.ca_certificate.clone(),
            certificate: tls.certificate.clone(),
            private_key: tls.private_key.clone(),
            domain_name: tls.domain_name.clone(),
        }),
        Duration::from_secs(10),
    )
    .await
}

fn print_usage() {
    eprintln!(
        "Usage:
  rcaster serve --config <caster.json>
  rcaster import-accounts --database <accounts.redb> --input <accounts.json>
  rcaster backup-standalone --database <accounts.redb> --config <caster.json>
    --output-dir <new-directory>
  rcaster coordinator --etcd-endpoint <url> --cluster-id <id> --bind <address>
    [--etcd-ca <pem> --etcd-certificate <pem> --etcd-private-key <pem>
     --etcd-domain <name>]
    [--tls-certificate <pem> --tls-private-key <pem> --client-ca <pem>
     --service-token <token>]
  rcaster hash-password < password.txt
  rcaster etcd-backup --etcd-endpoint <url>
    (--output <snapshot.db> | --backup-dir <directory> [--retention <count>])
    [--etcd-ca <pem> --etcd-certificate <pem> --etcd-private-key <pem>
     --etcd-domain <name>]
  rcaster etcd-recovery-prepare --etcd-endpoint <url> --cluster-id <id>
    --confirm <id> [--etcd-ca <pem> --etcd-certificate <pem>
    --etcd-private-key <pem> --etcd-domain <name>]
  rcaster --help"
    );
}
