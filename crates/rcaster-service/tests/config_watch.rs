use std::time::{Duration, SystemTime, UNIX_EPOCH};

use etcd_client::{Client, ConnectOptions, PutOptions, Txn, TxnOp};
use rcaster_service::config::{CasterConfig, ConfigWatcher, publish_config};

#[tokio::test]
async fn config_watcher_advances_generation_and_flags_invalid_updates() {
    let cluster = unique("config-watch");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish generation 1");
    let watcher = ConfigWatcher::start(connect().await, &cluster)
        .await
        .expect("start watcher");
    assert_eq!(watcher.snapshot().config.generation, 1);

    publish_config(&mut admin, &cluster, CasterConfig::new(2))
        .await
        .expect("publish generation 2");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if watcher.snapshot().config.generation == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("watch generation");
    assert!(watcher.is_healthy());

    let key = format!("/rcaster/{cluster}/config/caster");
    admin
        .txn(Txn::new().and_then([TxnOp::put(
            key,
            b"{invalid".as_slice(),
            Some(PutOptions::new()),
        )]))
        .await
        .expect("write invalid config");
    tokio::time::timeout(Duration::from_secs(2), async {
        while watcher.is_healthy() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("invalid config unhealthy");

    admin
        .txn(Txn::new().and_then([TxnOp::put(
            format!("/rcaster/{cluster}/config/caster"),
            serde_json::to_vec(&CasterConfig::new(3)).expect("encode config"),
            Some(PutOptions::new()),
        )]))
        .await
        .expect("write recovered config");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if watcher.is_healthy() && watcher.snapshot().config.generation == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("watch recovery");

    watcher.shutdown().await;
}

#[tokio::test]
async fn config_watcher_converges_after_one_thousand_updates() {
    let cluster = unique("config-watch-soak");
    let mut admin = connect().await;
    let key = format!("/rcaster/{cluster}/config/caster");
    admin
        .put(
            key.clone(),
            serde_json::to_vec(&CasterConfig::new(1)).expect("encode initial config"),
            None,
        )
        .await
        .expect("write initial config");
    let watcher = ConfigWatcher::start(connect().await, &cluster)
        .await
        .expect("start watcher");
    for generation in 2..=1001 {
        admin
            .put(
                key.clone(),
                serde_json::to_vec(&CasterConfig::new(generation)).expect("encode config"),
                None,
            )
            .await
            .expect("write config");
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if watcher.is_healthy() && watcher.snapshot().config.generation == 1001 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("1000-update convergence");
    watcher.shutdown().await;
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
