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
