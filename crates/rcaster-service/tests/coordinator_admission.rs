use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, ConnectOptions};
use rcaster_service::{
    admission::{
        AdmissionError, ClientAdmissionRequest, SessionRecord, SourceAdmissionRequest,
        read_ip_session_count, read_user_session_count,
    },
    config::{CasterConfig, publish_config},
    coordinator_admission::{AdmissionCoordinator, CoordinatorOptions},
    revision_admission::admit_source_revision,
    source_claim::SourceClaimRequest,
};

#[tokio::test]
async fn coordinator_batches_one_hundred_casters_for_one_slot() {
    let cluster = unique("coordinator-hundred");
    let user_id = "u_coordinator";
    let mut admin = connect().await;
    let config = publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("start coordinator"),
    );
    let mut tasks = Vec::new();

    for index in 0..100 {
        let coordinator = coordinator.clone();
        let config = config.clone();
        let mut client = connect().await;
        tasks.push(tokio::spawn(async move {
            let lease = grant_lease(&mut client).await;
            coordinator
                .admit_client(
                    lease,
                    ClientAdmissionRequest::new(
                        SessionRecord::client(
                            format!("s_{index}"),
                            user_id,
                            "C63",
                            format!("caster-{index}"),
                            format!("boot_{index}"),
                            "203.0.113.80",
                            "2026-11-22T05:00:00Z",
                        ),
                        1,
                        None,
                        config,
                    ),
                )
                .await
        }));
    }

    let mut successes = 0;
    for task in tasks {
        if task.await.expect("admission task").is_ok() {
            successes += 1;
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(
        read_user_session_count(&mut admin, &cluster, user_id)
            .await
            .expect("count sessions"),
        1
    );
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn coordinator_ip_quota_ignores_source_sessions() {
    let cluster = unique("coordinator-ip");
    let user_id = "u_ip";
    let ip = "203.0.113.81";
    let mut client = connect().await;
    let config = publish_config(&mut client, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let source_lease = grant_lease(&mut client).await;
    admit_source_revision(
        &mut client,
        source_lease,
        &cluster,
        SourceAdmissionRequest::new(
            SessionRecord::source(
                "s_source",
                user_id,
                "C63",
                "caster-west-02",
                "boot_west",
                "2026-11-22T05:00:00Z",
            ),
            3,
            SourceClaimRequest::new(
                "C63",
                "caster-west-02",
                "c_source",
                "boot_west",
                "10.20.0.12:12102",
                user_id,
                "2026-11-22T05:00:00Z",
            ),
            config.clone(),
        ),
    )
    .await
    .expect("source admission");
    assert_eq!(
        read_ip_session_count(&mut client, &cluster, ip)
            .await
            .expect("count IP"),
        0
    );

    let coordinator =
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("start coordinator");
    let first = coordinator
        .admit_client(
            grant_lease(&mut client).await,
            ClientAdmissionRequest::new(
                SessionRecord::client(
                    "s_client_1",
                    user_id,
                    "C63",
                    "caster-east-01",
                    "boot_east",
                    ip,
                    "2026-11-22T05:00:01Z",
                ),
                3,
                Some(1),
                config.clone(),
            ),
        )
        .await;
    assert!(first.is_ok());

    let second = coordinator
        .admit_client(
            grant_lease(&mut client).await,
            ClientAdmissionRequest::new(
                SessionRecord::client(
                    "s_client_2",
                    user_id,
                    "C63",
                    "caster-east-01",
                    "boot_east",
                    ip,
                    "2026-11-22T05:00:02Z",
                ),
                3,
                Some(1),
                config,
            ),
        )
        .await;
    assert!(matches!(second, Err(AdmissionError::IpQuotaExceeded)));
    coordinator.shutdown().await;
}

#[tokio::test]
async fn coordinator_rejects_stale_config_without_writes() {
    let cluster = unique("coordinator-config");
    let user_id = "u_config";
    let mut client = connect().await;
    let stale = publish_config(&mut client, &cluster, CasterConfig::new(1))
        .await
        .expect("publish generation 1");
    publish_config(&mut client, &cluster, CasterConfig::new(2))
        .await
        .expect("publish generation 2");
    let coordinator =
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("start coordinator");

    let rejected = coordinator
        .admit_client(
            grant_lease(&mut client).await,
            ClientAdmissionRequest::new(
                SessionRecord::client(
                    "s_stale",
                    user_id,
                    "C63",
                    "caster-east-01",
                    "boot_east",
                    "203.0.113.82",
                    "2026-11-22T05:00:00Z",
                ),
                1,
                None,
                stale,
            ),
        )
        .await;

    assert!(matches!(rejected, Err(AdmissionError::ConfigStale)));
    assert_eq!(
        read_user_session_count(&mut client, &cluster, user_id)
            .await
            .expect("count sessions"),
        0
    );
    coordinator.shutdown().await;
}

#[tokio::test]
async fn coordinator_owner_lease_recovers_after_expiry() {
    let cluster = unique("coordinator-owner");
    let options = CoordinatorOptions {
        owner_lease_ttl_s: 2,
        keep_alive: false,
        ..CoordinatorOptions::default()
    };
    let first = AdmissionCoordinator::start(connect().await, &cluster, options.clone())
        .await
        .expect("first coordinator");
    let second = AdmissionCoordinator::start(connect().await, &cluster, options.clone()).await;
    assert!(matches!(
        second,
        Err(AdmissionError::CoordinatorUnavailable)
    ));

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let replacement = AdmissionCoordinator::start(connect().await, &cluster, options)
        .await
        .expect("replacement coordinator");

    let mut client = connect().await;
    let config = publish_config(&mut client, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let stale = first
        .admit_client(
            grant_lease(&mut client).await,
            ClientAdmissionRequest::new(
                SessionRecord::client(
                    "s_stale_owner",
                    "u_owner",
                    "C63",
                    "caster-east-01",
                    "boot_east",
                    "203.0.113.83",
                    "2026-11-22T05:00:00Z",
                ),
                1,
                None,
                config,
            ),
        )
        .await;
    assert!(matches!(stale, Err(AdmissionError::CoordinatorUnavailable)));

    first.shutdown().await;
    replacement.shutdown().await;
}

async fn connect() -> Client {
    let endpoint = std::env::var("RCASTER_ETCD_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned());
    Client::connect([endpoint], Some(ConnectOptions::new()))
        .await
        .expect("etcd must be running")
}

async fn grant_lease(client: &mut Client) -> i64 {
    client
        .lease_grant(30, None)
        .await
        .expect("grant lease")
        .id()
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    )
}
