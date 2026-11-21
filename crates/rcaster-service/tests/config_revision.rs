use std::time::{SystemTime, UNIX_EPOCH};

use etcd_client::{Client, ConnectOptions};
use rcaster_service::admission::{
    AdmissionError, ClientAdmissionRequest, SessionRecord, SourceAdmissionRequest, admit_client,
    admit_source, read_user_session_count, try_acquire_user_lock,
};
use rcaster_service::config::{CasterConfig, ConfigError, load_config_snapshot, publish_config};
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
