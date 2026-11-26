use std::time::{SystemTime, UNIX_EPOCH};

use etcd_client::{Client, ConnectOptions};
use rcaster_service::blacklist::{
    BlacklistError, BlacklistRule, delete_blacklist_rule, upsert_blacklist_rule,
};

#[tokio::test]
async fn blacklist_writes_use_unique_cidr_and_revision_cas() {
    let mut client = connect().await;
    let cluster = unique("blacklist-write");
    let mut rule = BlacklistRule {
        schema_version: 1,
        mod_revision: 0,
        id: "blocked-net".to_owned(),
        cidr: "192.0.2.0/24".to_owned(),
        quota: 0,
        enabled: true,
        description: "test network".to_owned(),
        updated_at: String::new(),
    };
    let first = upsert_blacklist_rule(&mut client, &cluster, &rule, None)
        .await
        .expect("create rule");
    assert!(first > 0);

    let duplicate = BlacklistRule {
        id: "duplicate-net".to_owned(),
        ..rule.clone()
    };
    assert_eq!(
        upsert_blacklist_rule(&mut client, &cluster, &duplicate, None).await,
        Err(BlacklistError::InvalidConfig)
    );

    rule.quota = 2;
    let second = upsert_blacklist_rule(&mut client, &cluster, &rule, Some(first))
        .await
        .expect("update rule");
    assert!(second > first);
    assert_eq!(
        upsert_blacklist_rule(&mut client, &cluster, &rule, Some(first)).await,
        Err(BlacklistError::Conflict)
    );
    assert_eq!(
        delete_blacklist_rule(&mut client, &cluster, &rule.id, first).await,
        Err(BlacklistError::Conflict)
    );
    delete_blacklist_rule(&mut client, &cluster, &rule.id, second)
        .await
        .expect("delete rule");
}

#[tokio::test]
async fn concurrent_casters_cannot_reserve_the_same_normalized_cidr() {
    let cluster = unique("blacklist-reservation");
    let mut left = connect().await;
    let mut right = connect().await;
    let rule = |id: &str, cidr: &str| BlacklistRule {
        schema_version: 1,
        mod_revision: 0,
        id: id.to_owned(),
        cidr: cidr.to_owned(),
        quota: 0,
        enabled: true,
        description: String::new(),
        updated_at: String::new(),
    };
    let left_rule = rule("left", "198.51.100.7/24");
    let right_rule = rule("right", "198.51.100.0/24");
    let (left_result, right_result) = tokio::join!(
        upsert_blacklist_rule(&mut left, &cluster, &left_rule, None),
        upsert_blacklist_rule(&mut right, &cluster, &right_rule, None),
    );
    assert!(
        left_result.is_ok() ^ right_result.is_ok(),
        "exactly one CIDR reservation must win: {left_result:?}, {right_result:?}"
    );
}

#[tokio::test]
async fn changing_a_rule_cidr_releases_the_previous_reservation() {
    let mut client = connect().await;
    let cluster = unique("blacklist-reservation-update");
    let mut rule = BlacklistRule {
        schema_version: 1,
        mod_revision: 0,
        id: "moving".to_owned(),
        cidr: "192.0.2.0/24".to_owned(),
        quota: 0,
        enabled: true,
        description: String::new(),
        updated_at: String::new(),
    };
    let revision = upsert_blacklist_rule(&mut client, &cluster, &rule, None)
        .await
        .expect("create");
    rule.cidr = "198.51.100.0/24".to_owned();
    upsert_blacklist_rule(&mut client, &cluster, &rule, Some(revision))
        .await
        .expect("move");

    let replacement = BlacklistRule {
        id: "replacement".to_owned(),
        cidr: "192.0.2.0/24".to_owned(),
        ..rule
    };
    upsert_blacklist_rule(&mut client, &cluster, &replacement, None)
        .await
        .expect("old CIDR is available");
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
