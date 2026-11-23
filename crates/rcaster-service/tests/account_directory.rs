use std::time::{Duration, SystemTime, UNIX_EPOCH};

use etcd_client::{Client, ConnectOptions};
use rcaster_core::account::{AccountSnapshot, Mountpoint, User, UserRole};
use rcaster_service::account_directory::{AccountDirectoryError, AccountWatcher};

#[tokio::test]
async fn account_directory_writes_are_shared_watched_and_revision_checked() {
    let cluster = unique("account-directory");
    let watcher = AccountWatcher::start(connect().await, &cluster, snapshot())
        .await
        .expect("account watcher");
    let directory = watcher.directory();
    let second = AccountWatcher::start(connect().await, &cluster, AccountSnapshot::default())
        .await
        .expect("second watcher");
    let second_directory = second.directory();

    let user_revision = directory
        .upsert_user(user("editor-id", "editor"), None)
        .await
        .expect("create user");
    let mount_revision = directory
        .upsert_mountpoint(mountpoint("C64", "editor-id"), None)
        .await
        .expect("create mountpoint");
    wait_for(&second_directory, |accounts| {
        accounts.user_by_username("editor").is_some() && accounts.mountpoint("C64").is_some()
    })
    .await;

    let mut disabled = directory
        .snapshot()
        .user_by_id("editor-id")
        .expect("created user")
        .clone();
    disabled.enabled = false;
    let updated_revision = directory
        .upsert_user(disabled, Some(user_revision))
        .await
        .expect("update user");
    assert!(updated_revision > user_revision);
    wait_for(&second_directory, |accounts| {
        accounts
            .user_by_id("editor-id")
            .is_some_and(|user| !user.enabled)
    })
    .await;
    assert!(matches!(
        directory
            .upsert_user(
                user("editor-id", "editor"),
                Some(updated_revision.saturating_sub(1))
            )
            .await,
        Err(AccountDirectoryError::Conflict)
    ));

    assert!(matches!(
        directory.delete_user("editor-id", updated_revision).await,
        Err(AccountDirectoryError::InUse)
    ));
    directory
        .delete_mountpoint("C64", mount_revision)
        .await
        .expect("delete mountpoint");
    directory
        .delete_user("editor-id", updated_revision)
        .await
        .expect("delete user");

    second.shutdown().await;
    watcher.shutdown().await;
}

async fn wait_for(
    directory: &rcaster_service::account_directory::AccountDirectory,
    predicate: impl Fn(&AccountSnapshot) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if predicate(&directory.snapshot()) && directory.is_healthy() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("account watch update");
}

fn snapshot() -> AccountSnapshot {
    AccountSnapshot::new(
        [User {
            role: UserRole::Admin,
            ..user("owner-id", "owner")
        }],
        [Mountpoint {
            ntrip1_upload_password_hash: Some("hash".to_owned()),
            ..mountpoint("C63", "owner-id")
        }],
    )
    .expect("seed snapshot")
}

fn user(user_id: &str, username: &str) -> User {
    User {
        schema_version: 1,
        mod_revision: 0,
        user_id: user_id.to_owned(),
        username: username.to_owned(),
        password_hash: "hash".to_owned(),
        auth_version: 1,
        enabled: true,
        ntrip_enabled: true,
        web_enabled: true,
        role: UserRole::User,
        created_at: "2026-11-22T00:00:00Z".to_owned(),
        updated_at: "2026-11-22T00:00:00Z".to_owned(),
    }
}

fn mountpoint(name: &str, owner_user_id: &str) -> Mountpoint {
    Mountpoint {
        schema_version: 1,
        mod_revision: 0,
        mountpoint: name.to_owned(),
        owner_user_id: owner_user_id.to_owned(),
        enabled: true,
        ntrip1_upload_password_hash: None,
        advertise: true,
        region: None,
        source_expectation: None,
        created_at: "2026-11-22T00:00:00Z".to_owned(),
        updated_at: "2026-11-22T00:00:00Z".to_owned(),
    }
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
