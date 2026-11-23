use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use rcaster_core::account::{AccountSnapshot, Mountpoint, User, UserRole};
use rcaster_service::{account_directory::AccountDirectory, account_store::AccountStore};

#[test]
fn account_store_persists_and_reloads_the_snapshot() {
    let path = TempDatabase::new();
    let snapshot = snapshot("owner-id", "owner", "C63");
    {
        let store = AccountStore::open(path.as_ref()).expect("open store");
        store.replace(&snapshot).expect("replace accounts");
    }

    let store = AccountStore::open(path.as_ref()).expect("reopen store");
    let loaded = store.load().expect("load accounts");

    assert_eq!(
        loaded
            .user_by_username("owner")
            .map(|user| user.user_id.as_str()),
        Some("owner-id")
    );
    assert_eq!(
        loaded
            .mountpoint("C63")
            .map(|mountpoint| mountpoint.owner_user_id.as_str()),
        Some("owner-id")
    );
}

#[test]
fn open_existing_does_not_create_a_missing_database() {
    let path = TempDatabase::new();
    assert!(AccountStore::open_existing(path.as_ref()).is_err());
    assert!(!path.as_ref().exists());
}

#[test]
fn replacing_accounts_removes_stale_records() {
    let path = TempDatabase::new();
    let store = AccountStore::open(path.as_ref()).expect("open store");
    store
        .replace(&snapshot("old-id", "old", "OLD"))
        .expect("initial accounts");
    store
        .replace(&snapshot("new-id", "new", "NEW"))
        .expect("replacement accounts");

    let loaded = store.load().expect("load accounts");

    assert!(loaded.user_by_username("old").is_none());
    assert!(loaded.mountpoint("OLD").is_none());
    assert!(loaded.user_by_username("new").is_some());
    assert!(loaded.mountpoint("NEW").is_some());
}

#[tokio::test]
async fn writable_account_directory_persists_password_rehash() {
    let path = TempDatabase::new();
    let initial = snapshot("owner-id", "owner", "C63");
    let store = AccountStore::open(path.as_ref()).expect("open store");
    store.replace(&initial).expect("initial accounts");
    let directory = AccountDirectory::with_store(store.load().expect("load snapshot"), store);
    assert!(
        directory
            .update_user_password_hash("owner-id", "hash", "new-hash".to_owned())
            .await
            .expect("rehash")
    );
    drop(directory);
    let reloaded = AccountStore::open(path.as_ref())
        .expect("reopen store")
        .load()
        .expect("reload accounts");
    assert_eq!(
        reloaded.user_by_id("owner-id").expect("user").password_hash,
        "new-hash"
    );
}

fn snapshot(user_id: &str, username: &str, mountpoint: &str) -> AccountSnapshot {
    AccountSnapshot::new(
        [User {
            schema_version: 1,
            mod_revision: 1,
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
        }],
        [Mountpoint {
            schema_version: 1,
            mod_revision: 1,
            mountpoint: mountpoint.to_owned(),
            owner_user_id: user_id.to_owned(),
            enabled: true,
            ntrip1_upload_password_hash: None,
            advertise: true,
            region: None,
            source_expectation: None,
            created_at: "2026-11-22T00:00:00Z".to_owned(),
            updated_at: "2026-11-22T00:00:00Z".to_owned(),
        }],
    )
    .expect("account snapshot")
}

struct TempDatabase(PathBuf);

impl TempDatabase {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        Self(std::env::temp_dir().join(format!(
            "rcaster-account-{}-{unique}.redb",
            std::process::id()
        )))
    }
}

impl AsRef<Path> for TempDatabase {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDatabase {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
