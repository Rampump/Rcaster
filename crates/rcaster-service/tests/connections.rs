use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, ConnectOptions};
use rcaster_service::connections::{CommandWatcher, ConnectionController, LocalSession};
use tokio::time::timeout;

#[tokio::test]
async fn cancel_all_cancels_every_bound_connection() {
    let controller = ConnectionController::default();
    let mut first = controller.connection();
    first.bind("session:one".to_owned());
    let mut second = controller.connection();
    second.bind("source:C63:two".to_owned());
    controller.cancel_all();
    timeout(Duration::from_secs(1), first.cancelled())
        .await
        .expect("first cancellation");
    timeout(Duration::from_secs(1), second.cancelled())
        .await
        .expect("second cancellation");
}

#[tokio::test]
async fn source_mountpoint_cancels_only_the_matching_local_source() {
    let controller = ConnectionController::default();
    let mut source = controller.connection();
    source.bind("source:C63".to_owned());
    let mut other = controller.connection();
    other.bind("source:C64".to_owned());

    assert!(controller.cancel_source("C63"));
    timeout(Duration::from_secs(1), source.cancelled())
        .await
        .expect("source cancellation");
    assert!(!controller.cancel_source("C65"));
}

#[tokio::test]
async fn local_sessions_are_listed_and_cancelled_by_id() {
    let controller = ConnectionController::default();
    let mut client = controller.connection();
    client.track_session(LocalSession {
        session_id: "local-client".to_owned(),
        session_type: "client".to_owned(),
        user_id: "owner-id".to_owned(),
        mountpoint: "C63".to_owned(),
        ntrip_version: "2".to_owned(),
        client_ip: "127.0.0.1".to_owned(),
        started_at: 1,
        software: Some("test-client".to_owned()),
    });

    assert_eq!(controller.sessions().len(), 1);
    assert_eq!(controller.sessions()[0].user_id, "owner-id");
    assert!(controller.cancel_session("local-client"));
    timeout(Duration::from_secs(1), client.cancelled())
        .await
        .expect("client cancellation");
    assert!(controller.sessions().is_empty());
    assert!(!controller.cancel_session("missing"));
}

#[tokio::test]
async fn owner_commands_cancel_local_sessions_and_sources() {
    let cluster = unique("commands");
    let caster = "caster-a";
    let controller = Arc::new(ConnectionController::default());
    let watcher = CommandWatcher::start(connect().await, &cluster, caster, Arc::clone(&controller))
        .await
        .expect("command watcher");
    let mut session = controller.connection();
    session.bind("session:session-1".to_owned());
    let mut source = controller.connection();
    source.bind("source:C63:connection-1".to_owned());

    let mut etcd = connect().await;
    etcd.put(
        format!("/rcaster/{cluster}/online/commands/{caster}/cmd-1"),
        br#"{"session_id":"session-1"}"#.to_vec(),
        None,
    )
    .await
    .expect("session command");
    timeout(Duration::from_secs(2), session.cancelled())
        .await
        .expect("session cancellation");

    etcd.put(
        format!("/rcaster/{cluster}/runtime/source-commands/C63/cmd-2"),
        br#"{"mountpoint":"C63","connection_id":"connection-1"}"#.to_vec(),
        None,
    )
    .await
    .expect("source command");
    timeout(Duration::from_secs(2), source.cancelled())
        .await
        .expect("source cancellation");

    watcher.shutdown().await;
}

#[tokio::test]
async fn disabled_or_deleted_users_cancel_bound_connections() {
    let cluster = unique("user-cancel");
    let controller = Arc::new(ConnectionController::default());
    let watcher = CommandWatcher::start(
        connect().await,
        &cluster,
        "caster-a",
        Arc::clone(&controller),
    )
    .await
    .expect("command watcher");
    let mut disabled = controller.connection();
    disabled.bind("user:disabled".to_owned());
    let mut disabled_second = controller.connection();
    disabled_second.bind("user:disabled".to_owned());
    let mut ntrip_disabled = controller.connection();
    ntrip_disabled.bind("user:ntrip-disabled".to_owned());
    let mut deleted = controller.connection();
    deleted.bind("user:deleted".to_owned());
    let mut web_updated = controller.connection();
    web_updated.bind("web-user:web-updated".to_owned());

    let mut etcd = connect().await;
    etcd.put(
        format!("/rcaster/{cluster}/config/users/disabled"),
        br#"{"enabled":false}"#.to_vec(),
        None,
    )
    .await
    .expect("disabled user");
    timeout(Duration::from_secs(2), disabled.cancelled())
        .await
        .expect("disabled user cancellation");
    timeout(Duration::from_secs(2), disabled_second.cancelled())
        .await
        .expect("second disabled user cancellation");

    etcd.put(
        format!("/rcaster/{cluster}/config/users/ntrip-disabled"),
        br#"{"enabled":true,"ntrip_enabled":false}"#.to_vec(),
        None,
    )
    .await
    .expect("NTRIP-disabled user");
    timeout(Duration::from_secs(2), ntrip_disabled.cancelled())
        .await
        .expect("NTRIP-disabled user cancellation");

    etcd.put(
        format!("/rcaster/{cluster}/config/users/web-updated"),
        br#"{"enabled":true,"web_enabled":true,"auth_version":2}"#.to_vec(),
        None,
    )
    .await
    .expect("updated web user");
    timeout(Duration::from_secs(2), web_updated.cancelled())
        .await
        .expect("web session cancellation");

    etcd.put(
        format!("/rcaster/{cluster}/config/users/deleted"),
        br#"{"enabled":true}"#.to_vec(),
        None,
    )
    .await
    .expect("active user");
    etcd.delete(format!("/rcaster/{cluster}/config/users/deleted"), None)
        .await
        .expect("delete user");
    timeout(Duration::from_secs(2), deleted.cancelled())
        .await
        .expect("deleted user cancellation");

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
