use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, DeleteOptions, GetOptions};
use rcaster_core::account::{Mountpoint, User, UserRole};
use rcaster_service::{
    account_auth::hash_password,
    account_directory::load_accounts,
    config::{CasterConfig, load_config_snapshot, publish_config},
    recovery::{RecoveryError, prepare_restored_cluster},
};

static NEXT_CLUSTER: AtomicU64 = AtomicU64::new(0);

#[tokio::test]
async fn recovery_keeps_configuration_and_removes_stale_runtime_state() {
    let cluster = unique_cluster("valid");
    let mut client = connect().await;
    write_fixture(
        &mut client,
        &cluster,
        hash_password("secret").expect("password hash"),
    )
    .await;
    for suffix in [
        "runtime/sources/C63",
        "runtime/source-claims/C63",
        "runtime/source-generations/C63",
        "runtime/admin_sessions/session",
        "online/users/owner/sessions/session",
        "locks/users/owner",
    ] {
        client
            .put(format!("/rcaster/{cluster}/{suffix}"), b"stale", None)
            .await
            .expect("write stale runtime key");
    }

    let report = prepare_restored_cluster(&mut client, &cluster)
        .await
        .expect("prepare restored cluster");
    assert_eq!(report.config_generation, 1);
    assert_eq!(report.users, 1);
    assert_eq!(report.mountpoints, 1);
    assert_eq!(report.deleted_runtime_keys, 6);
    assert_eq!(
        load_config_snapshot(&mut client, &cluster)
            .await
            .expect("config remains")
            .config
            .generation,
        1
    );
    assert_eq!(
        load_accounts(&mut client, &cluster)
            .await
            .expect("accounts remain")
            .mountpoint("C63")
            .expect("mountpoint remains")
            .owner_user_id,
        "owner"
    );
    assert!(prefix(&mut client, &cluster, "runtime/").await.is_empty());
    assert!(prefix(&mut client, &cluster, "online/").await.is_empty());
    assert!(prefix(&mut client, &cluster, "locks/").await.is_empty());

    cleanup(&mut client, &cluster).await;
}

#[tokio::test]
async fn recovery_rejects_invalid_password_hashes_without_deleting_runtime_state() {
    let cluster = unique_cluster("invalid");
    let mut client = connect().await;
    write_fixture(&mut client, &cluster, "not-an-argon2-hash".to_owned()).await;
    let stale_key = format!("/rcaster/{cluster}/runtime/sources/C63");
    client
        .put(stale_key.as_str(), b"stale", None)
        .await
        .expect("write stale runtime key");

    let error = prepare_restored_cluster(&mut client, &cluster)
        .await
        .expect_err("invalid hash must fail recovery");
    assert!(matches!(error, RecoveryError::InvalidPasswordHash(_)));
    assert_eq!(
        prefix(&mut client, &cluster, "runtime/").await.len(),
        1,
        "validation must happen before any runtime deletion"
    );

    cleanup(&mut client, &cluster).await;
}

async fn connect() -> Client {
    let endpoint =
        std::env::var("RCASTER_ETCD_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:2379".into());
    Client::connect([endpoint.as_str()], None)
        .await
        .expect("connect etcd")
}

async fn write_fixture(client: &mut Client, cluster: &str, password_hash: String) {
    publish_config(client, cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let user = User {
        schema_version: 1,
        mod_revision: 0,
        user_id: "owner".to_owned(),
        username: "owner".to_owned(),
        password_hash,
        auth_version: 1,
        enabled: true,
        ntrip_enabled: true,
        web_enabled: true,
        role: UserRole::Admin,
        created_at: "2026-11-23T00:00:00Z".to_owned(),
        updated_at: "2026-11-23T00:00:00Z".to_owned(),
    };
    let mountpoint = Mountpoint {
        schema_version: 1,
        mod_revision: 0,
        mountpoint: "C63".to_owned(),
        owner_user_id: "owner".to_owned(),
        enabled: true,
        ntrip1_upload_password_hash: None,
        advertise: true,
        region: Some("test".to_owned()),
        source_expectation: None,
        created_at: "2026-11-23T00:00:00Z".to_owned(),
        updated_at: "2026-11-23T00:00:00Z".to_owned(),
    };
    client
        .put(
            format!("/rcaster/{cluster}/config/users/owner"),
            serde_json::to_vec(&user).expect("encode user"),
            None,
        )
        .await
        .expect("write user");
    client
        .put(
            format!("/rcaster/{cluster}/config/mounts/C63"),
            serde_json::to_vec(&mountpoint).expect("encode mountpoint"),
            None,
        )
        .await
        .expect("write mountpoint");
}

async fn prefix(client: &mut Client, cluster: &str, suffix: &str) -> Vec<etcd_client::KeyValue> {
    client
        .get(
            format!("/rcaster/{cluster}/{suffix}"),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .expect("read prefix")
        .kvs()
        .to_vec()
}

async fn cleanup(client: &mut Client, cluster: &str) {
    client
        .delete(
            format!("/rcaster/{cluster}/"),
            Some(DeleteOptions::new().with_prefix()),
        )
        .await
        .expect("clean test cluster");
}

fn unique_cluster(test: &str) -> String {
    format!(
        "recovery-{test}-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos(),
        NEXT_CLUSTER.fetch_add(1, Ordering::Relaxed)
    )
}
