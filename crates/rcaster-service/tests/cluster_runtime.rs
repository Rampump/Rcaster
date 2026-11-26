use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, ConnectOptions};
use rcaster_core::{
    protocol::{DataFormat, DataProfile},
    str_record::StrRecord,
};
use rcaster_service::{
    admission::AdmissionError,
    cluster_runtime::{ClusterRuntime, ClusterRuntimeOptions, SharedClusterRuntime},
    config::{CasterConfig, publish_config},
    coordinator_admission::{AdmissionCoordinator, CoordinatorOptions},
    coordinator_grpc::serve_admission_grpc,
    source_claim::ClaimError,
};
use tokio::{net::TcpListener, sync::oneshot};

#[tokio::test]
async fn concurrent_shared_admissions_share_the_coordinator_batch() {
    let cluster = unique("runtime-batch");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(
            connect().await,
            &cluster,
            CoordinatorOptions {
                batch_window: std::time::Duration::from_millis(100),
                ..CoordinatorOptions::default()
            },
        )
        .await
        .expect("start coordinator"),
    );
    let shared = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-batch", "10.0.0.11:12102"),
    )
    .await
    .expect("shared runtime");
    let before = admin
        .get("/__rcaster_revision_probe", None)
        .await
        .expect("revision read")
        .header()
        .expect("revision header")
        .revision();

    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..8 {
        let shared = shared.clone();
        tasks.spawn(async move {
            shared
                .admit_client(
                    &format!("batch-user-{index}"),
                    "C63",
                    &format!("203.0.113.{}", index + 10),
                    1,
                    None,
                )
                .await
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.expect("admission task").expect("admission");
    }
    let after = admin
        .get("/__rcaster_revision_probe", None)
        .await
        .expect("revision read")
        .header()
        .expect("revision header")
        .revision();
    assert!(
        after.saturating_sub(before) <= 2,
        "eight concurrent admissions should share one etcd batch, revisions advanced by {}",
        after.saturating_sub(before)
    );

    shared.shutdown().await.expect("runtime shutdown");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn source_claim_is_exclusive_and_releasable_across_casters() {
    let cluster = unique("runtime-source");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("start coordinator"),
    );
    let mut first = ClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-east", "10.0.0.11:12102"),
    )
    .await
    .expect("first runtime");
    let mut second = ClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-west", "10.0.0.12:12102"),
    )
    .await
    .expect("second runtime");

    let first_source = first
        .admit_source("owner-id", "C63", "connection-1", 3)
        .await
        .expect("first source");
    first
        .activate_source(&first_source, &default_str("C63"))
        .await
        .expect("activate first source");
    let active = admin
        .get(format!("/rcaster/{cluster}/runtime/sources/C63"), None)
        .await
        .expect("read active source");
    assert!(String::from_utf8_lossy(active.kvs()[0].value()).contains("\"state\":\"ACTIVE\""));
    assert!(matches!(
        second
            .admit_source("owner-id", "C63", "connection-2", 3)
            .await,
        Err(AdmissionError::Source(ClaimError::AlreadyClaimed))
    ));

    first
        .release_source(&first_source)
        .await
        .expect("release source");
    let second_source = second
        .admit_source("owner-id", "C63", "connection-3", 3)
        .await
        .expect("replacement source");
    assert_eq!(second_source.mountpoint(), "C63");

    drop(first);
    drop(second);
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn active_source_str_is_discoverable_by_other_caster() {
    let cluster = unique("runtime-str");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("start coordinator"),
    );
    let mut first = ClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-east", "10.0.0.11:12102"),
    )
    .await
    .expect("first runtime");
    let second = ClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-west", "10.0.0.12:12102"),
    )
    .await
    .expect("second runtime");
    let source = first
        .admit_source("owner-id", "C63", "connection-1", 3)
        .await
        .expect("source claim");
    assert!(second.sourcetable_records().await.is_empty());

    let mut record = StrRecord::from_profile(
        "C63",
        &DataProfile {
            primary_format: Some(DataFormat::Rtcm3),
            valid_frames: 2,
            has_correction_data: true,
            ..DataProfile::default()
        },
    );
    record.identifier = "Shanghai Farm".to_owned();
    record.format_details = "1005,1074".to_owned();
    first
        .activate_source(&source, &record)
        .await
        .expect("activate source");

    let records = wait_for_sourcetable(&second, |records| {
        records.len() == 1 && records[0] == record
    })
    .await;
    assert_eq!(records, vec![record]);

    first.release_source(&source).await.expect("release source");
    wait_for_sourcetable(&second, |records| records.is_empty()).await;

    first.shutdown().await.expect("first shutdown");
    second.shutdown().await.expect("second shutdown");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn source_cooldown_is_shared_listable_and_clearable() {
    let cluster = unique("runtime-cooldown");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("coordinator"),
    );
    let mut runtime = ClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-cooldown", "10.0.0.16:12102"),
    )
    .await
    .expect("runtime");
    runtime
        .record_source_cooldown("C63", "203.0.113.9", "SOURCE_ADMISSION_REJECTED")
        .await
        .expect("record cooldown");
    assert!(
        runtime
            .source_cooldown_remaining("C63", "203.0.113.9")
            .await
            .expect("cooldown remaining")
            .is_some()
    );
    let cooldowns = runtime.source_cooldowns().await.expect("cooldowns");
    assert_eq!(cooldowns.len(), 1);
    let fingerprint = cooldowns[0]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_owned();
    runtime
        .clear_source_cooldown(&fingerprint)
        .await
        .expect("clear cooldown");
    assert!(
        runtime
            .source_cooldown_remaining("C63", "203.0.113.9")
            .await
            .expect("cooldown remaining")
            .is_none()
    );
    runtime.shutdown().await.expect("runtime shutdown");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn authentication_failure_window_is_shared_across_casters() {
    let cluster = unique("runtime-auth-failure");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("coordinator"),
    );
    let mut first = ClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-auth-one", "10.0.0.17:12102"),
    )
    .await
    .expect("first runtime");
    let mut second = ClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-auth-two", "10.0.0.18:12102"),
    )
    .await
    .expect("second runtime");

    for _ in 0..4 {
        assert!(
            first
                .record_auth_failure("source:203.0.113.10")
                .await
                .expect("failure")
                .is_none()
        );
    }
    assert!(
        first
            .record_auth_failure("source:203.0.113.10")
            .await
            .expect("lockout")
            .is_some()
    );
    assert!(
        second
            .auth_failure_retry_after("source:203.0.113.10")
            .await
            .expect("shared retry")
            .is_some()
    );
    first
        .clear_auth_failures("source:203.0.113.10")
        .await
        .expect("clear");
    assert!(
        second
            .auth_failure_retry_after("source:203.0.113.10")
            .await
            .expect("cleared retry")
            .is_none()
    );

    first.shutdown().await.expect("first shutdown");
    second.shutdown().await.expect("second shutdown");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn shared_auth_failure_outage_is_counted() {
    let cluster = unique("runtime-auth-fallback");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("coordinator"),
    );
    let runtime = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-auth-fallback", "10.0.0.19:12102"),
    )
    .await
    .expect("runtime");
    runtime.shutdown().await.expect("runtime shutdown");
    assert!(
        runtime
            .auth_failure_retry_after("source:203.0.113.11")
            .await
            .is_err()
    );
    assert_eq!(runtime.auth_failure_fallback_total(), 1);
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn session_watch_tracks_live_session_count() {
    let cluster = unique("runtime-session-count");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("coordinator"),
    );
    let shared = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-session-count", "10.0.0.17:12102"),
    )
    .await
    .expect("runtime");
    let permit = shared
        .admit_client("watch-user", "C63", "203.0.113.30", 2, None)
        .await
        .expect("client");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while shared.session_count().await == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("session watch add");
    shared.release_client(&permit).await.expect("release");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while shared.session_count().await != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("session watch delete");
    shared.shutdown().await.expect("shutdown");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn caster_publishes_lease_bound_version_status() {
    let cluster = unique("runtime-version-status");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("coordinator"),
    );
    let mut runtime = ClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-version", "10.0.0.18:12102"),
    )
    .await
    .expect("runtime");
    let versions = runtime.node_versions().await.expect("versions");
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0]["caster_id"], "caster-version");
    assert_eq!(versions[0]["healthy"], true);
    assert_eq!(versions[0]["config_schema"], 1);
    runtime
        .set_maintenance(true)
        .await
        .expect("maintenance status");
    let versions = runtime.node_versions().await.expect("versions");
    assert_eq!(versions[0]["maintenance"], true);
    runtime
        .set_maintenance(false)
        .await
        .expect("maintenance clear");
    runtime
        .update_version_status(false)
        .await
        .expect("unhealthy status");
    let versions = runtime.node_versions().await.expect("versions");
    assert_eq!(versions[0]["healthy"], false);
    runtime.shutdown().await.expect("shutdown");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn user_quota_is_shared_across_casters() {
    let cluster = unique("runtime-user");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("start coordinator"),
    );
    let mut first = ClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-east", "10.0.0.11:12102"),
    )
    .await
    .expect("first runtime");
    let mut second = ClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-west", "10.0.0.12:12102"),
    )
    .await
    .expect("second runtime");

    let permit = first
        .admit_client("client-id", "C63", "203.0.113.10", 1, None)
        .await
        .expect("first client");
    assert!(matches!(
        second
            .admit_client("client-id", "C63", "203.0.113.10", 1, None)
            .await,
        Err(AdmissionError::UserQuotaExceeded)
    ));

    first.release_client(&permit).await.expect("release client");
    let replacement = second
        .admit_client("client-id", "C63", "203.0.113.10", 1, None)
        .await
        .expect("replacement client");

    drop(replacement);
    drop(first);
    drop(second);
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn caster_lease_is_renewed_until_shutdown() {
    let cluster = unique("runtime-lease");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("start coordinator"),
    );
    let mut runtime = ClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        ClusterRuntimeOptions {
            lease_ttl_s: 2,
            ..options(&cluster, "caster-lease", "10.0.0.13:12102")
        },
    )
    .await
    .expect("runtime");

    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    assert!(runtime.is_healthy());
    runtime
        .admit_client("lease-user", "C63", "203.0.113.30", 1, None)
        .await
        .expect("admission after renewal");

    runtime.shutdown().await.expect("runtime shutdown");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn shared_runtime_serializes_competing_admissions() {
    let cluster = unique("runtime-shared");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("start coordinator"),
    );
    let shared = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-shared", "10.0.0.14:12102"),
    )
    .await
    .expect("shared runtime");
    let first = tokio::spawn({
        let shared = shared.clone();
        async move {
            shared
                .admit_client("shared-user", "C63", "203.0.113.40", 1, None)
                .await
        }
    });
    let second = tokio::spawn({
        let shared = shared.clone();
        async move {
            shared
                .admit_client("shared-user", "C63", "203.0.113.40", 1, None)
                .await
        }
    });
    let first = first.await.expect("first task");
    let second = second.await.expect("second task");
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);

    shared.shutdown().await.expect("shared shutdown");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn runtime_uses_remote_grpc_coordinator() {
    let cluster = unique("runtime-grpc");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("coordinator"),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("grpc listener");
    let address = listener.local_addr().expect("grpc address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_coordinator = Arc::clone(&coordinator);
    let server = tokio::spawn(async move {
        serve_admission_grpc(listener, server_coordinator, async {
            let _ = shutdown_rx.await;
        })
        .await
        .expect("grpc server");
    });
    let shared = SharedClusterRuntime::start_grpc(
        connect().await,
        &format!("http://{address}"),
        options(&cluster, "caster-grpc", "10.0.0.15:12102"),
    )
    .await
    .expect("grpc runtime");

    let permit = shared
        .admit_client("grpc-user", "C63", "203.0.113.50", 1, None)
        .await
        .expect("grpc admission");
    shared
        .release_client(&permit)
        .await
        .expect("release client");
    let source = shared
        .admit_source("grpc-owner", "C64", "grpc-source", 1)
        .await
        .expect("grpc source");
    shared
        .activate_source(&source, &default_str("C64"))
        .await
        .expect("activate grpc source");
    shared
        .release_source(&source)
        .await
        .expect("release source");
    shared.shutdown().await.expect("runtime shutdown");

    shutdown_tx.send(()).expect("grpc shutdown");
    server.await.expect("grpc server join");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

fn options(cluster: &str, caster_id: &str, relay_endpoint: &str) -> ClusterRuntimeOptions {
    ClusterRuntimeOptions {
        cluster_id: cluster.to_owned(),
        caster_id: caster_id.to_owned(),
        runtime_epoch: unique("epoch"),
        relay_endpoint: relay_endpoint.to_owned(),
        lease_ttl_s: 30,
        keep_alive: true,
    }
}

async fn connect() -> Client {
    let endpoint = std::env::var("RCASTER_ETCD_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned());
    Client::connect([endpoint], Some(ConnectOptions::new()))
        .await
        .expect("etcd must be running")
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    )
}

async fn wait_for_sourcetable(
    runtime: &ClusterRuntime,
    predicate: impl Fn(&[StrRecord]) -> bool,
) -> Vec<StrRecord> {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let records = runtime.sourcetable_records().await;
            if predicate(&records) {
                return records;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("source watch timed out")
}

fn default_str(mountpoint: &str) -> StrRecord {
    StrRecord::from_profile(mountpoint, &DataProfile::default())
}
