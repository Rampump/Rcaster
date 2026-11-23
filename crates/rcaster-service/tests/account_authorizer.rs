use argon2::{Algorithm, Argon2, Params, PasswordHasher, Version, password_hash::SaltString};
use etcd_client::{Client, ConnectOptions};
use rcaster_core::account::{
    AccountError, AccountSnapshot, Mountpoint, SourceExpectation, User, UserRole,
};
use rcaster_service::{
    account_auth::{AccountAuthorizer, hash_password, password_needs_rehash, verify_password},
    account_directory::AccountWatcher,
    auth::{AuthError, ClientAuthorizer, ClientCredentials, SourceAuthorizer, SourceCredentials},
};

#[tokio::test]
async fn ntrip1_uses_the_mountpoint_upload_password() {
    let user = test_user(true, "user-hash");
    let mountpoint = test_mountpoint(Some(hash_password("upload").unwrap()));
    let authorizer = AccountAuthorizer::new(
        AccountSnapshot::new(vec![user], vec![mountpoint]).expect("account snapshot"),
    );

    let authorized = authorizer
        .authorize_source("C63", SourceCredentials::Ntrip1 { password: "upload" })
        .await
        .expect("NTRIP1 source");
    assert_eq!(authorized.user_id, "owner-id");

    assert_eq!(
        authorizer
            .authorize_source("C63", SourceCredentials::Ntrip1 { password: "wrong" })
            .await,
        Err(AuthError::Unauthorized)
    );
}

#[tokio::test]
async fn source_authorization_carries_the_mountpoint_expectation() {
    let user = test_user(true, "user-hash");
    let mut mountpoint = test_mountpoint(Some(hash_password("upload").unwrap()));
    let expectation = SourceExpectation {
        formats: vec!["rtcm3".to_owned()],
        msm_versions: vec![4],
    };
    mountpoint.source_expectation = Some(expectation.clone());
    let authorizer = AccountAuthorizer::new(
        AccountSnapshot::new([user], [mountpoint]).expect("account snapshot"),
    );

    let authorized = authorizer
        .authorize_source("C63", SourceCredentials::Ntrip1 { password: "upload" })
        .await
        .expect("NTRIP1 source");

    assert_eq!(authorized.source_expectation, Some(expectation));
}

#[test]
fn account_snapshot_rejects_invalid_source_expectations() {
    for expectation in [
        SourceExpectation {
            formats: vec![],
            msm_versions: vec![],
        },
        SourceExpectation {
            formats: vec!["rtcm4".to_owned()],
            msm_versions: vec![],
        },
        SourceExpectation {
            formats: vec!["rtcm3".to_owned()],
            msm_versions: vec![8],
        },
    ] {
        let mut mountpoint = test_mountpoint(None);
        mountpoint.source_expectation = Some(expectation);
        assert!(matches!(
            AccountSnapshot::new([test_user(true, "user-hash")], [mountpoint]),
            Err(AccountError::InvalidMountpoint)
        ));
    }
}

#[tokio::test]
async fn ntrip2_requires_the_mountpoint_owner_and_user_password() {
    let user = test_user(true, &hash_password("secret").unwrap());
    let mountpoint = test_mountpoint(None);
    let authorizer = AccountAuthorizer::new(
        AccountSnapshot::new(vec![user], vec![mountpoint]).expect("account snapshot"),
    );

    let authorized = authorizer
        .authorize_source(
            "C63",
            SourceCredentials::Ntrip2 {
                username: "owner",
                password: "secret",
            },
        )
        .await
        .expect("NTRIP2 source");
    assert_eq!(authorized.user_id, "owner-id");
}

#[tokio::test]
async fn disabled_users_cannot_publish_sources() {
    let user = test_user(false, &hash_password("secret").unwrap());
    let mountpoint = test_mountpoint(Some(hash_password("upload").unwrap()));
    let authorizer = AccountAuthorizer::new(
        AccountSnapshot::new(vec![user], vec![mountpoint]).expect("account snapshot"),
    );

    assert_eq!(
        authorizer
            .authorize_source(
                "C63",
                SourceCredentials::Ntrip2 {
                    username: "owner",
                    password: "secret",
                },
            )
            .await,
        Err(AuthError::Unauthorized)
    );
}

#[tokio::test]
async fn client_authentication_accepts_any_enabled_ntrip_user() {
    let owner = user("owner-id", "owner", true, &hash_password("owner").unwrap());
    let client = user(
        "client-id",
        "client",
        true,
        &hash_password("client-secret").unwrap(),
    );
    let mountpoint = test_mountpoint(Some(hash_password("upload").unwrap()));
    let authorizer = AccountAuthorizer::new(
        AccountSnapshot::new(vec![owner, client], vec![mountpoint]).expect("account snapshot"),
    );

    let authorized = authorizer
        .authorize_client(
            "C63",
            Some(ClientCredentials::Basic {
                username: "client",
                password: "client-secret",
            }),
        )
        .await
        .expect("client user");
    assert_eq!(authorized.user_id, "client-id");

    assert_eq!(
        authorizer
            .authorize_client(
                "C63",
                Some(ClientCredentials::Basic {
                    username: "client",
                    password: "wrong",
                }),
            )
            .await,
        Err(AuthError::Unauthorized)
    );
}

#[tokio::test]
async fn successful_login_rehashes_legacy_password_parameters() {
    let cluster = format!(
        "rehash-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let legacy_hash = legacy_hash("secret");
    assert!(password_needs_rehash(&legacy_hash));
    let watcher = AccountWatcher::start(
        Client::connect(
            [std::env::var("RCASTER_ETCD_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned())],
            Some(ConnectOptions::new()),
        )
        .await
        .expect("etcd"),
        &cluster,
        AccountSnapshot::new([test_user(true, &legacy_hash)], [test_mountpoint(None)])
            .expect("accounts"),
    )
    .await
    .expect("account watcher");
    wait_for_account_watcher(&watcher).await;
    let directory = watcher.directory();
    let authorizer = AccountAuthorizer::from_directory(directory.clone());
    authorizer
        .authorize_client(
            "C63",
            Some(ClientCredentials::Basic {
                username: "owner",
                password: "secret",
            }),
        )
        .await
        .expect("authenticated");
    let rehashed = directory
        .snapshot()
        .user_by_username("owner")
        .expect("user")
        .password_hash
        .clone();
    assert!(!password_needs_rehash(&rehashed));
    assert!(verify_password(&rehashed, "secret"));
    watcher.shutdown().await;
}

#[tokio::test]
async fn ntrip1_upload_login_rehashes_mountpoint_password_parameters() {
    let cluster = format!(
        "mount-rehash-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let legacy_hash = legacy_hash("upload");
    let watcher = AccountWatcher::start(
        Client::connect(
            [std::env::var("RCASTER_ETCD_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned())],
            Some(ConnectOptions::new()),
        )
        .await
        .expect("etcd"),
        &cluster,
        AccountSnapshot::new(
            [test_user(true, &hash_password("secret").unwrap())],
            [test_mountpoint(Some(legacy_hash.clone()))],
        )
        .expect("accounts"),
    )
    .await
    .expect("account watcher");
    wait_for_account_watcher(&watcher).await;
    let directory = watcher.directory();
    let authorizer = AccountAuthorizer::from_directory(directory.clone());
    authorizer
        .authorize_source("C63", SourceCredentials::Ntrip1 { password: "upload" })
        .await
        .expect("authenticated");
    let rehashed = directory
        .snapshot()
        .mountpoint("C63")
        .and_then(|mountpoint| mountpoint.ntrip1_upload_password_hash.clone())
        .expect("upload password hash");
    assert_ne!(rehashed, legacy_hash);
    assert!(!password_needs_rehash(&rehashed));
    assert!(verify_password(&rehashed, "upload"));
    watcher.shutdown().await;
}

#[test]
fn rejects_duplicate_accounts_and_missing_mountpoint_owner() {
    let user = test_user(true, "hash");
    assert!(matches!(
        AccountSnapshot::new(vec![user.clone(), user], vec![]),
        Err(AccountError::DuplicateUser)
    ));

    assert!(matches!(
        AccountSnapshot::new(vec![], vec![test_mountpoint(None)]),
        Err(AccountError::MissingOwner)
    ));
}

fn test_user(enabled: bool, password_hash: &str) -> User {
    user("owner-id", "owner", enabled, password_hash)
}

fn user(user_id: &str, username: &str, enabled: bool, password_hash: &str) -> User {
    User {
        schema_version: 1,
        mod_revision: 1,
        user_id: user_id.to_owned(),
        username: username.to_owned(),
        password_hash: password_hash.to_owned(),
        auth_version: 1,
        enabled,
        ntrip_enabled: true,
        web_enabled: true,
        role: UserRole::User,
        created_at: "2026-11-22T00:00:00Z".to_owned(),
        updated_at: "2026-11-22T00:00:00Z".to_owned(),
    }
}

fn test_mountpoint(ntrip1_password_hash: Option<String>) -> Mountpoint {
    Mountpoint {
        schema_version: 1,
        mod_revision: 1,
        mountpoint: "C63".to_owned(),
        owner_user_id: "owner-id".to_owned(),
        enabled: true,
        ntrip1_upload_password_hash: ntrip1_password_hash,
        advertise: true,
        region: Some("test".to_owned()),
        source_expectation: None,
        created_at: "2026-11-22T00:00:00Z".to_owned(),
        updated_at: "2026-11-22T00:00:00Z".to_owned(),
    }
}

fn legacy_hash(password: &str) -> String {
    let salt = SaltString::encode_b64(b"0123456789abcdef").unwrap();
    Argon2::new(
        Algorithm::Argon2id,
        Version::V0x13,
        Params::new(8, 1, 1, None).unwrap(),
    )
    .hash_password(password.as_bytes(), &salt)
    .unwrap()
    .to_string()
}

async fn wait_for_account_watcher(watcher: &AccountWatcher) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !watcher.directory().is_healthy() {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("account watcher did not become healthy");
}
