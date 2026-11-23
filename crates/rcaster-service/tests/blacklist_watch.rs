use std::{
    net::{IpAddr, Ipv4Addr},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, ConnectOptions, DeleteOptions, PutOptions, Txn, TxnOp};
use rcaster_service::blacklist::BlacklistWatcher;

#[tokio::test]
async fn blacklist_watch_applies_longest_prefix_and_deletes_rules() {
    let cluster = unique("blacklist-watch");
    let mut admin = connect().await;
    put_rule(&mut admin, &cluster, "wide", "10.0.0.0/8", 0).await;
    put_disabled_rule(&mut admin, &cluster, "disabled", "192.0.2.0/24").await;
    let watcher = BlacklistWatcher::start(connect().await, &cluster)
        .await
        .expect("start blacklist");
    assert!(watcher.is_ip_denied(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))));
    assert!(
        watcher
            .snapshot()
            .all_rules()
            .iter()
            .any(|rule| rule.id == "disabled" && !rule.enabled)
    );
    assert!(
        watcher
            .quota_for_ip(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)))
            .is_none()
    );

    put_rule(&mut admin, &cluster, "specific", "10.1.0.0/16", 5).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if watcher.quota_for_ip(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))) == Some(5) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("specific rule");
    assert!(watcher.is_ip_denied(IpAddr::V4(Ipv4Addr::new(10, 2, 2, 3))));

    admin
        .txn(Txn::new().and_then([TxnOp::delete(
            rule_key(&cluster, "wide"),
            Some(DeleteOptions::new()),
        )]))
        .await
        .expect("delete rule");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if watcher
                .quota_for_ip(IpAddr::V4(Ipv4Addr::new(10, 2, 2, 3)))
                .is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("deleted rule");
    watcher.shutdown().await;
}

async fn put_disabled_rule(client: &mut Client, cluster: &str, id: &str, cidr: &str) {
    let rule = serde_json::json!({
        "schema_version": 1,
        "mod_revision": 0,
        "id": id,
        "cidr": cidr,
        "quota": 0,
        "enabled": false,
        "description": "disabled",
        "updated_at": "2026-11-22T00:00:00Z"
    });
    client
        .txn(Txn::new().and_then([TxnOp::put(
            rule_key(cluster, id),
            serde_json::to_vec(&rule).expect("encode rule"),
            Some(PutOptions::new()),
        )]))
        .await
        .expect("put disabled rule");
}

async fn put_rule(client: &mut Client, cluster: &str, id: &str, cidr: &str, quota: i32) {
    let rule = serde_json::json!({
        "schema_version": 1,
        "mod_revision": 1,
        "id": id,
        "cidr": cidr,
        "quota": quota,
        "enabled": true,
        "description": "test",
        "updated_at": "2026-11-22T00:00:00Z"
    });
    client
        .txn(Txn::new().and_then([TxnOp::put(
            rule_key(cluster, id),
            serde_json::to_vec(&rule).expect("encode rule"),
            Some(PutOptions::new()),
        )]))
        .await
        .expect("put rule");
}

fn rule_key(cluster: &str, id: &str) -> String {
    format!("/rcaster/{cluster}/config/blacklist/{id}")
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
