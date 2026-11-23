use std::time::{SystemTime, UNIX_EPOCH};

use etcd_client::{Client, ConnectOptions};
use rcaster_service::admission::{
    AdmissionError, ClientAdmissionRequest, SessionRecord, SourceAdmissionRequest, admit_client,
    admit_source, read_user_session_count, try_acquire_user_lock,
};
use rcaster_service::config::{
    CasterConfig, ConfigError, load_config_revisions, load_config_snapshot, prune_config_revisions,
    publish_config,
};
use rcaster_service::source_claim::SourceClaimRequest;

#[tokio::test]
async fn stale_config_rejects_client_until_reloaded() {
    let cluster = unique("config-client");
    let user_id = "u_config";
    let mut client = connect().await;
    let stale = publish_config(&mut client, &cluster, CasterConfig::new(1))
        .await
        .expect("publish generation 1");
    publish_config(&mut client, &cluster, CasterConfig::new(2))
        .await
        .expect("publish generation 2");

    let lock_lease = grant_lease(&mut client).await;
    let stale_lock = try_acquire_user_lock(&mut client, lock_lease, &cluster, user_id)
        .await
        .expect("stale user lock");
    let stale_session_lease = grant_lease(&mut client).await;
    let stale_admission = admit_client(
        &mut client,
        stale_session_lease,
        &cluster,
        stale_lock,
        ClientAdmissionRequest::new(
            SessionRecord::client(
                "s_stale",
                user_id,
                "C63",
                "caster-east-01",
                "boot_east",
                "203.0.113.40",
                "2026-11-22T01:00:00Z",
            ),
            2,
            None,
            stale,
        ),
    )
    .await;

    assert!(matches!(stale_admission, Err(AdmissionError::ConfigStale)));
    assert_eq!(
        read_user_session_count(&mut client, &cluster, user_id)
            .await
            .expect("count sessions"),
        0
    );

    let current = load_config_snapshot(&mut client, &cluster)
        .await
        .expect("load current config");
    let current_lock_lease = grant_lease(&mut client).await;
    let current_lock = try_acquire_user_lock(&mut client, current_lock_lease, &cluster, user_id)
        .await
        .expect("current user lock");
    let current_session_lease = grant_lease(&mut client).await;
    let admitted = admit_client(
        &mut client,
        current_session_lease,
        &cluster,
        current_lock,
        ClientAdmissionRequest::new(
            SessionRecord::client(
                "s_current",
                user_id,
                "C63",
                "caster-east-01",
                "boot_east",
                "203.0.113.40",
                "2026-11-22T01:00:01Z",
            ),
            2,
            None,
            current,
        ),
    )
    .await;

    assert!(admitted.is_ok());
}

#[tokio::test]
async fn stale_config_rejects_source_without_claim_or_session() {
    let cluster = unique("config-source");
    let user_id = "u_config_source";
    let mut client = connect().await;
    let stale = publish_config(&mut client, &cluster, CasterConfig::new(1))
        .await
        .expect("publish generation 1");
    publish_config(&mut client, &cluster, CasterConfig::new(2))
        .await
        .expect("publish generation 2");

    let stale_lock_lease = grant_lease(&mut client).await;
    let stale_lock = try_acquire_user_lock(&mut client, stale_lock_lease, &cluster, user_id)
        .await
        .expect("stale user lock");
    let stale_session_lease = grant_lease(&mut client).await;
    let stale_admission = admit_source(
        &mut client,
        stale_session_lease,
        &cluster,
        stale_lock,
        SourceAdmissionRequest::new(
            SessionRecord::source(
                "s_stale",
                user_id,
                "C63",
                "caster-west-02",
                "boot_west",
                "2026-11-22T01:00:00Z",
            ),
            1,
            SourceClaimRequest::new(
                "C63",
                "caster-west-02",
                "c_stale",
                "boot_west",
                "10.20.0.12:12102",
                user_id,
                "2026-11-22T01:00:00Z",
            ),
            stale,
        ),
    )
    .await;

    assert!(matches!(stale_admission, Err(AdmissionError::ConfigStale)));
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
            .expect("read source claim")
            .kvs()
            .is_empty()
    );

    let current = load_config_snapshot(&mut client, &cluster)
        .await
        .expect("load current config");
    let current_lock_lease = grant_lease(&mut client).await;
    let current_lock = try_acquire_user_lock(&mut client, current_lock_lease, &cluster, user_id)
        .await
        .expect("current user lock");
    let current_session_lease = grant_lease(&mut client).await;
    let admitted = admit_source(
        &mut client,
        current_session_lease,
        &cluster,
        current_lock,
        SourceAdmissionRequest::new(
            SessionRecord::source(
                "s_current",
                user_id,
                "C63",
                "caster-west-02",
                "boot_west",
                "2026-11-22T01:00:01Z",
            ),
            1,
            SourceClaimRequest::new(
                "C63",
                "caster-west-02",
                "c_current",
                "boot_west",
                "10.20.0.12:12102",
                user_id,
                "2026-11-22T01:00:01Z",
            ),
            current,
        ),
    )
    .await;

    assert!(admitted.is_ok());
}

#[tokio::test]
async fn stale_account_revisions_reject_client_admission() {
    let cluster = unique("account-revision");
    let user_id = "u_account_revision";
    let mountpoint = "C63";
    let mut client = connect().await;
    let current = publish_config(&mut client, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let user_put = client
        .put(
            format!("/rcaster/{cluster}/config/users/{user_id}"),
            br#"{"enabled":true}"#.to_vec(),
            None,
        )
        .await
        .expect("write user");
    let user_revision = user_put.header().expect("user header").revision() as u64;
    let mount_put = client
        .put(
            format!("/rcaster/{cluster}/config/mounts/{mountpoint}"),
            br#"{"enabled":true}"#.to_vec(),
            None,
        )
        .await
        .expect("write mountpoint");
    let mount_revision = mount_put.header().expect("mount header").revision() as u64;

    let lock_lease = grant_lease(&mut client).await;
    let lock = try_acquire_user_lock(&mut client, lock_lease, &cluster, user_id)
        .await
        .expect("user lock");
    client
        .put(
            format!("/rcaster/{cluster}/config/mounts/{mountpoint}"),
            br#"{"enabled":false}"#.to_vec(),
            None,
        )
        .await
        .expect("disable mountpoint");
    let session_lease = grant_lease(&mut client).await;
    let result = admit_client(
        &mut client,
        session_lease,
        &cluster,
        lock,
        ClientAdmissionRequest::new(
            SessionRecord::client(
                "s_account_stale",
                user_id,
                mountpoint,
                "caster-east-01",
                "boot_east",
                "203.0.113.41",
                "2026-11-22T01:00:00Z",
            )
            .with_account_revisions(1, user_revision, mount_revision),
            2,
            None,
            current,
        ),
    )
    .await;

    assert!(matches!(result, Err(AdmissionError::StaleLock)));
    assert_eq!(
        read_user_session_count(&mut client, &cluster, user_id)
            .await
            .expect("count sessions"),
        0
    );
}

#[tokio::test]
async fn published_generation_must_increase() {
    let cluster = unique("config-generation");
    let mut client = connect().await;
    publish_config(&mut client, &cluster, CasterConfig::new(1))
        .await
        .expect("publish generation 1");
    publish_config(&mut client, &cluster, CasterConfig::new(2))
        .await
        .expect("publish generation 2");

    let duplicate = publish_config(&mut client, &cluster, CasterConfig::new(2)).await;

    assert!(matches!(
        duplicate,
        Err(ConfigError::GenerationNotMonotonic)
    ));
}

#[tokio::test]
async fn config_revision_retention_removes_old_generations() {
    let cluster = unique("config-retention");
    let mut client = connect().await;
    for generation in 1..=6 {
        publish_config(&mut client, &cluster, CasterConfig::new(generation))
            .await
            .expect("publish config");
    }
    assert_eq!(
        prune_config_revisions(&mut client, &cluster, 3)
            .await
            .expect("prune revisions"),
        3
    );
    let revisions = load_config_revisions(&mut client, &cluster, 10)
        .await
        .expect("load revisions");
    assert_eq!(
        revisions
            .iter()
            .map(|revision| revision.config.generation)
            .collect::<Vec<_>>(),
        vec![6, 5, 4]
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
