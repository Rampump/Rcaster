use std::time::{SystemTime, UNIX_EPOCH};

use etcd_client::{Client, ConnectOptions};
use rcaster_service::{
    admission::{
        AdmissionError, ClientAdmissionRequest, SessionRecord, SourceAdmissionRequest,
        read_ip_session_count, read_user_session_count,
    },
    config::{CasterConfig, load_config_snapshot, publish_config},
    revision_admission::{admit_client_revision, admit_source_revision},
    source_claim::{ClaimError, SourceClaimRequest, claim_source},
};

#[tokio::test]
async fn revision_final_slot_allows_one_client_or_source() {
    let cluster = unique("revision-final-slot");
    let user_id = "u_race";
    let mut config_client = connect().await;
    let config = publish_config(&mut config_client, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let mut client_task = connect().await;
    let mut source_task = connect().await;
    let client_config = config.clone();
    let client_cluster = cluster.clone();
    let client = tokio::spawn(async move {
        let lease = grant_lease(&mut client_task).await;
        admit_client_revision(
            &mut client_task,
            lease,
            &client_cluster,
            ClientAdmissionRequest::new(
                SessionRecord::client(
                    "s_client",
                    user_id,
                    "C63",
                    "caster-east-01",
                    "boot_east",
                    "203.0.113.70",
                    "2026-11-22T04:00:00Z",
                ),
                1,
                None,
                client_config,
            ),
        )
        .await
    });
    let source_cluster = cluster.clone();
    let source = tokio::spawn(async move {
        let lease = grant_lease(&mut source_task).await;
        admit_source_revision(
            &mut source_task,
            lease,
            &source_cluster,
            SourceAdmissionRequest::new(
                SessionRecord::source(
                    "s_source",
                    user_id,
                    "C63",
                    "caster-west-02",
                    "boot_west",
                    "2026-11-22T04:00:00Z",
                ),
                1,
                SourceClaimRequest::new(
                    "C63",
                    "caster-west-02",
                    "c_source",
                    "boot_west",
                    "10.20.0.12:12102",
                    user_id,
                    "2026-11-22T04:00:00Z",
                ),
                config,
            ),
        )
        .await
    });

    let client = client.await.expect("client task");
    let source = source.await.expect("source task");
    assert_eq!(usize::from(client.is_ok()) + usize::from(source.is_ok()), 1);
    let mut client = connect().await;
    assert_eq!(
        read_user_session_count(&mut client, &cluster, user_id)
            .await
            .expect("count sessions"),
        1
    );
}

#[tokio::test]
async fn one_hundred_revisions_compete_for_one_slot() {
    let cluster = unique("revision-hundred");
    let user_id = "u_hundred";
    let mut config_client = connect().await;
    let config = publish_config(&mut config_client, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let mut tasks = Vec::new();

    for index in 0..100 {
        let cluster = cluster.clone();
        let config = config.clone();
        let mut client = connect().await;
        tasks.push(tokio::spawn(async move {
            let lease = grant_lease(&mut client).await;
            admit_client_revision(
                &mut client,
                lease,
                &cluster,
                ClientAdmissionRequest::new(
                    SessionRecord::client(
                        format!("s_{index}"),
                        user_id,
                        "C63",
                        format!("caster-{index}"),
                        format!("boot_{index}"),
                        "203.0.113.71",
                        "2026-11-22T04:00:00Z",
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

    let mut client = connect().await;
    assert_eq!(
        read_user_session_count(&mut client, &cluster, user_id)
            .await
            .expect("count sessions"),
        1
    );
}

#[tokio::test]
async fn revision_ip_quota_ignores_sources() {
    let cluster = unique("revision-ip");
    let user_id = "u_ip";
    let ip = "203.0.113.72";
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
                "2026-11-22T04:00:00Z",
            ),
            3,
            SourceClaimRequest::new(
                "C63",
                "caster-west-02",
                "c_source",
                "boot_west",
                "10.20.0.12:12102",
                user_id,
                "2026-11-22T04:00:00Z",
            ),
            config.clone(),
        ),
    )
    .await
    .expect("source admission");
    assert_eq!(
        read_ip_session_count(&mut client, &cluster, ip)
            .await
            .expect("count IP sessions"),
        0
    );

    let first_client_lease = grant_lease(&mut client).await;
    admit_client_revision(
        &mut client,
        first_client_lease,
        &cluster,
        ClientAdmissionRequest::new(
            SessionRecord::client(
                "s_client_1",
                user_id,
                "C63",
                "caster-east-01",
                "boot_east",
                ip,
                "2026-11-22T04:00:01Z",
            ),
            3,
            Some(1),
            config.clone(),
        ),
    )
    .await
    .expect("first client");
    let second_client_lease = grant_lease(&mut client).await;
    let second = admit_client_revision(
        &mut client,
        second_client_lease,
        &cluster,
        ClientAdmissionRequest::new(
            SessionRecord::client(
                "s_client_2",
                user_id,
                "C63",
                "caster-east-01",
                "boot_east",
                ip,
                "2026-11-22T04:00:02Z",
            ),
            3,
            Some(1),
            config,
        ),
    )
    .await;

    assert!(matches!(second, Err(AdmissionError::IpQuotaExceeded)));
}

#[tokio::test]
async fn revision_stale_config_rejects_without_writes() {
    let cluster = unique("revision-config");
    let user_id = "u_config";
    let mut client = connect().await;
    let stale = publish_config(&mut client, &cluster, CasterConfig::new(1))
        .await
        .expect("publish generation 1");
    publish_config(&mut client, &cluster, CasterConfig::new(2))
        .await
        .expect("publish generation 2");

    let stale_lease = grant_lease(&mut client).await;
    let rejected = admit_source_revision(
        &mut client,
        stale_lease,
        &cluster,
        SourceAdmissionRequest::new(
            SessionRecord::source(
                "s_stale",
                user_id,
                "C63",
                "caster-west-02",
                "boot_west",
                "2026-11-22T04:00:00Z",
            ),
            1,
            SourceClaimRequest::new(
                "C63",
                "caster-west-02",
                "c_stale",
                "boot_west",
                "10.20.0.12:12102",
                user_id,
                "2026-11-22T04:00:00Z",
            ),
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
    assert!(
        client
            .get(
                format!("/rcaster/{cluster}/runtime/source-claims/C63"),
                None
            )
            .await
            .expect("read claim")
            .kvs()
            .is_empty()
    );

    let current = load_config_snapshot(&mut client, &cluster)
        .await
        .expect("load config");
    let current_lease = grant_lease(&mut client).await;
    admit_source_revision(
        &mut client,
        current_lease,
        &cluster,
        SourceAdmissionRequest::new(
            SessionRecord::source(
                "s_current",
                user_id,
                "C63",
                "caster-west-02",
                "boot_west",
                "2026-11-22T04:00:01Z",
            ),
            1,
            SourceClaimRequest::new(
                "C63",
                "caster-west-02",
                "c_current",
                "boot_west",
                "10.20.0.12:12102",
                user_id,
                "2026-11-22T04:00:01Z",
            ),
            current,
        ),
    )
    .await
    .expect("current config admission");
}

#[tokio::test]
async fn revision_source_claim_failure_leaves_no_session() {
    let cluster = unique("revision-rollback");
    let user_id = "u_owner";
    let mut client = connect().await;
    let config = publish_config(&mut client, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let blocker_lease = grant_lease(&mut client).await;
    claim_source(
        &mut client,
        blocker_lease,
        &cluster,
        SourceClaimRequest::new(
            "C63",
            "caster-blocker",
            "c_blocker",
            "boot_blocker",
            "10.20.0.20:12102",
            "u_blocker",
            "2026-11-22T04:00:00Z",
        ),
    )
    .await
    .expect("blocking claim");

    let failed_lease = grant_lease(&mut client).await;
    let failed = admit_source_revision(
        &mut client,
        failed_lease,
        &cluster,
        SourceAdmissionRequest::new(
            SessionRecord::source(
                "s_owner",
                user_id,
                "C63",
                "caster-west-02",
                "boot_west",
                "2026-11-22T04:00:01Z",
            ),
            1,
            SourceClaimRequest::new(
                "C63",
                "caster-west-02",
                "c_owner",
                "boot_west",
                "10.20.0.12:12102",
                user_id,
                "2026-11-22T04:00:01Z",
            ),
            config,
        ),
    )
    .await;

    assert!(matches!(
        failed,
        Err(AdmissionError::Source(ClaimError::AlreadyClaimed))
    ));
    assert_eq!(
        read_user_session_count(&mut client, &cluster, user_id)
            .await
            .expect("count sessions"),
        0
    );
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
