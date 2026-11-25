use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use rcaster_core::account::{AccountSnapshot, Mountpoint, User, UserRole};
use rcaster_service::{
    account_directory::AccountDirectory,
    account_store::AccountStore,
    admin_auth::ApiToken,
    blacklist::{BlacklistError, BlacklistRule},
};

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
    let directory = AccountDirectory::with_store(store.load().expect("load snapshot"), store)
        .expect("account directory");
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

#[test]
fn standalone_blacklist_round_trips_across_reopen() {
    let path = TempDatabase::new();
    let first = blacklist_rule("blocked", "192.0.2.0/24");
    {
        let store = AccountStore::open(path.as_ref()).expect("open store");
        let revision = store
            .put_blacklist_rule(&first, None)
            .expect("create blacklist rule");
        assert_eq!(revision, 1);
        assert_eq!(
            store.put_blacklist_rule(&blacklist_rule("duplicate", "192.0.2.0/24"), None),
            Err(BlacklistError::InvalidConfig)
        );
    }

    let store = AccountStore::open(path.as_ref()).expect("reopen store");
    let rules = store.blacklist_rules().expect("load blacklist rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].id, "blocked");
    assert_eq!(rules[0].mod_revision, 1);

    store
        .delete_blacklist_rule("blocked", 1)
        .expect("delete blacklist rule");
    assert!(store.blacklist_rules().expect("reload rules").is_empty());
}

#[test]
fn standalone_api_tokens_round_trip_across_reopen() {
    let path = TempDatabase::new();
    let token = ApiToken {
        schema_version: 1,
        token_id: "a".repeat(64),
        name: "automation".to_owned(),
        token_hash: "sha256:hash".to_owned(),
        user_id: "owner-id".to_owned(),
        username: "owner".to_owned(),
        auth_version: 1,
        scopes: vec!["admin:read".to_owned()],
        created_at: 1_800_000_000,
        expires_at: Some(1_800_000_060),
    };
    {
        let store = AccountStore::open(path.as_ref()).expect("open store");
        store.put_api_token(&token).expect("persist API token");
        store
            .put_api_token_last_used(&token.token_id, 1_800_000_001)
            .expect("persist last-used timestamp");
    }

    let store = AccountStore::open(path.as_ref()).expect("reopen store");
    let tokens = store.api_tokens().expect("load API tokens");
    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].token_id, token.token_id);
    assert_eq!(
        store
            .api_token_last_used(&token.token_id)
            .expect("load last-used timestamp"),
        Some(1_800_000_001)
    );

    assert!(
        store
            .delete_api_token(&token.token_id)
            .expect("delete API token")
    );
    assert!(store.api_tokens().expect("reload API tokens").is_empty());
    assert_eq!(
        store
            .api_token_last_used(&token.token_id)
            .expect("reload last-used timestamp"),
        None
    );
}

#[tokio::test]
async fn standalone_blacklist_directory_reloads_limiter_state() {
    let path = TempDatabase::new();
    let store = AccountStore::open(path.as_ref()).expect("open store");
    let snapshot = snapshot("owner-id", "owner", "C63");
    store.replace(&snapshot).expect("initial accounts");
    let directory = AccountDirectory::with_store(snapshot, store).expect("account directory");

    directory
        .put_blacklist_rule(&blacklist_rule("blocked", "192.0.2.0/24"), None)
        .await
        .expect("persist blacklist rule");

    assert_eq!(
        directory.blacklist_quota_for_ip("192.0.2.10".parse().expect("IP")),
        Some(0)
    );
    assert_eq!(
        directory.blacklist_quota_for_ip("198.51.100.1".parse().expect("IP")),
        None
    );
}

#[tokio::test]
async fn standalone_account_directory_crud_persists_users_and_mountpoints() {
    let path = TempDatabase::new();
    let store = AccountStore::open(path.as_ref()).expect("open store");
    let initial = snapshot("owner-id", "owner", "C63");
    store.replace(&initial).expect("initial accounts");
    let directory = AccountDirectory::with_store(initial, store).expect("account directory");

    let mut user = snapshot("temporary-id", "temporary", "TMP1")
        .users()
        .next()
        .expect("temporary user")
        .clone();
    user.mod_revision = 0;
    assert_eq!(
        directory
            .upsert_user(user.clone(), None)
            .await
            .expect("create user"),
        1
    );

    let mut mountpoint = snapshot("temporary-id", "temporary", "TMP1")
        .mountpoint("TMP1")
        .expect("temporary mountpoint")
        .clone();
    mountpoint.mod_revision = 0;
    assert_eq!(
        directory
            .upsert_mountpoint(mountpoint.clone(), None)
            .await
            .expect("create mountpoint"),
        1
    );
    mountpoint.enabled = false;
    assert_eq!(
        directory
            .upsert_mountpoint(mountpoint, Some(1))
            .await
            .expect("update mountpoint"),
        2
    );

    user.enabled = false;
    assert_eq!(
        directory
            .upsert_user(user, Some(1))
            .await
            .expect("disable user"),
        2
    );
    directory
        .delete_mountpoint("TMP1", 2)
        .await
        .expect("delete mountpoint");
    directory
        .delete_user("temporary-id", 2)
        .await
        .expect("delete user");
    drop(directory);

    let snapshot = AccountStore::open(path.as_ref())
        .expect("reopen store")
        .load()
        .expect("reload accounts");
    assert!(snapshot.user_by_id("temporary-id").is_none());
    assert!(snapshot.mountpoint("TMP1").is_none());
    assert!(snapshot.user_by_id("owner-id").is_some());
    assert!(snapshot.mountpoint("C63").is_some());
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
            latitude: None,
            longitude: None,
            coverage_radius_m: None,
            str_fields: None,
            source_expectation: None,
            created_at: "2026-11-22T00:00:00Z".to_owned(),
            updated_at: "2026-11-22T00:00:00Z".to_owned(),
        }],
    )
    .expect("account snapshot")
}

fn blacklist_rule(id: &str, cidr: &str) -> BlacklistRule {
    BlacklistRule {
        schema_version: 1,
        mod_revision: 0,
        id: id.to_owned(),
        cidr: cidr.to_owned(),
        quota: 0,
        enabled: true,
        description: String::new(),
        updated_at: String::new(),
    }
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
