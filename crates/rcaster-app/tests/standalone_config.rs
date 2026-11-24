use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use rcaster_app::config::UpdaterConfig;
use rcaster_app::{
    accounts::AccountFile,
    config::{ConfigError, StandaloneConfig},
};
use rcaster_service::{
    account_auth::AccountAuthorizer,
    auth::{SourceAuthorizer, SourceCredentials},
};

#[test]
fn loads_config_defaults_and_resolves_account_database_relative_to_config() {
    let directory = TempDirectory::new();
    let path = directory.path().join("caster.json");
    fs::write(
        &path,
        r#"{
            "bind": "127.0.0.1:2101",
            "account_db": "accounts.redb",
            "client_auth_require_tls": false,
            "source_auth_require_tls": false
        }"#,
    )
    .expect("write config");

    let config = StandaloneConfig::load(&path).expect("load config");

    assert_eq!(config.bind, "127.0.0.1:2101");
    assert_eq!(config.account_db, directory.path().join("accounts.redb"));
    assert_eq!(config.head_timeout_ms, 10_000);
    assert_eq!(config.max_validating_sources, 64);
    assert_eq!(config.validation_buffer_max_bytes, 64 * 1024 * 1024);
    assert_eq!(config.auth_max_concurrency, 4);
    assert_eq!(config.accept_rate_per_second, 1000);
    assert_eq!(config.content_length_max_bytes, 1_048_576);
    assert_eq!(config.subscriber_buffer_events, 64);
    assert_eq!(config.standalone_backup_interval_s, 3600);
    assert_eq!(config.standalone_backup_retention, 168);
    assert!(!config.client_auth_require_tls);
    assert!(!config.source_auth_require_tls);
}

#[test]
fn resolves_standalone_backup_directory_and_rejects_zero_interval() {
    let directory = TempDirectory::new();
    let path = directory.path().join("caster.json");
    fs::write(
        &path,
        r#"{
            "bind": "127.0.0.1:2101",
            "account_db": "accounts.redb",
            "client_auth_require_tls": false,
            "source_auth_require_tls": false,
            "standalone_backup_dir": "backups",
            "standalone_backup_interval_s": 60,
            "standalone_backup_retention": 24
        }"#,
    )
    .expect("write backup config");
    let config = StandaloneConfig::load(&path).expect("load backup config");
    assert_eq!(
        config.standalone_backup_dir,
        Some(directory.path().join("backups"))
    );
    assert_eq!(config.standalone_backup_interval_s, 60);
    assert_eq!(config.standalone_backup_retention, 24);

    fs::write(
        &path,
        r#"{
            "bind": "127.0.0.1:2101",
            "account_db": "accounts.redb",
            "client_auth_require_tls": false,
            "source_auth_require_tls": false,
            "standalone_backup_dir": "backups",
            "standalone_backup_interval_s": 0,
            "standalone_backup_retention": 0
        }"#,
    )
    .expect("write zero interval config");
    assert!(matches!(
        StandaloneConfig::load(&path),
        Err(ConfigError::InvalidLimits)
    ));
}

#[test]
fn requires_tls_for_authenticated_transports_by_default() {
    let directory = TempDirectory::new();
    let path = directory.path().join("caster.json");
    fs::write(
        &path,
        r#"{
            "bind": "127.0.0.1:2101",
            "account_db": "accounts.redb"
        }"#,
    )
    .expect("write config");

    assert!(matches!(
        StandaloneConfig::load(&path),
        Err(ConfigError::IncompleteTls)
    ));
}

#[test]
fn rejects_empty_bind_address() {
    let directory = TempDirectory::new();
    let path = directory.path().join("caster.json");
    fs::write(
        &path,
        r#"{
            "bind": "",
            "account_db": "accounts.redb"
        }"#,
    )
    .expect("write config");

    assert!(matches!(
        StandaloneConfig::load(&path),
        Err(ConfigError::InvalidBind)
    ));
}

#[test]
fn resolves_tls_files_and_rejects_partial_tls_configuration() {
    let directory = TempDirectory::new();
    let path = directory.path().join("caster.json");
    fs::write(
        &path,
        r#"{
            "bind": "127.0.0.1:2101",
            "account_db": "accounts.redb",
            "tls_bind": "127.0.0.1:2102",
            "tls_certificate": "server.crt",
            "tls_private_key": "server.key",
            "tls_ocsp_response": "server.ocsp",
            "admin_client_crl": "client.crl",
            "relay_client_crl": "relay.crl"
        }"#,
    )
    .expect("write TLS config");
    let private_key = directory.path().join("server.key");
    fs::write(&private_key, "fixture").expect("write private key fixture");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&private_key, fs::Permissions::from_mode(0o600))
            .expect("protect private key fixture");
    }
    let config = StandaloneConfig::load(&path).expect("load TLS config");
    assert_eq!(
        config.tls_certificate,
        Some(directory.path().join("server.crt"))
    );
    assert_eq!(
        config.tls_private_key,
        Some(directory.path().join("server.key"))
    );
    assert_eq!(
        config.tls_ocsp_response,
        Some(directory.path().join("server.ocsp"))
    );
    assert_eq!(
        config.admin_client_crl,
        Some(directory.path().join("client.crl"))
    );
    assert_eq!(
        config.relay_client_crl,
        Some(directory.path().join("relay.crl"))
    );

    fs::write(
        &path,
        r#"{
            "bind": "127.0.0.1:2101",
            "account_db": "accounts.redb",
            "tls_bind": "127.0.0.1:2102"
        }"#,
    )
    .expect("write partial TLS config");
    assert!(matches!(
        StandaloneConfig::load(&path),
        Err(ConfigError::IncompleteTls)
    ));
}

#[test]
fn loads_and_validates_online_ocsp_responders() {
    let directory = TempDirectory::new();
    let path = directory.path().join("caster.json");
    fs::write(
        &path,
        r#"
        {
            "bind": "127.0.0.1:2101",
            "account_db": "accounts.redb",
            "tls_bind": "127.0.0.1:2102",
            "tls_certificate": "server.crt",
            "tls_private_key": "server.key",
            "tls_ocsp_responder": {
                "url": "https://ocsp.example.test",
                "refresh_interval_s": 900
            },
            "cluster": {
                "etcd_endpoints": ["http://127.0.0.1:2379"],
                "cluster_id": "test",
                "caster_id": "caster-1",
                "runtime_epoch": "boot-1",
                "relay_endpoint": "10.0.0.1:12102",
                "coordinator_tls": {
                    "ca_certificate": "ca.crt",
                    "certificate": "server.crt",
                    "private_key": "server.key",
                    "server_name": "coordinator.test",
                    "service_token": "token",
                    "server_ocsp_responder": {
                        "url": "http://ocsp.example.test"
                    }
                }
            }
        }
        "#,
    )
    .expect("write OCSP responder config");
    let private_key = directory.path().join("server.key");
    fs::write(&private_key, "fixture").expect("write private key fixture");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&private_key, fs::Permissions::from_mode(0o600))
            .expect("protect private key fixture");
    }

    let config = StandaloneConfig::load(&path).expect("load OCSP responder config");
    let listener_responder = config
        .tls_ocsp_responder
        .expect("standalone listener responder");
    assert_eq!(listener_responder.url, "https://ocsp.example.test");
    assert_eq!(listener_responder.refresh_interval_s, 900);
    let coordinator_responder = config
        .cluster
        .and_then(|cluster| cluster.coordinator_tls)
        .and_then(|tls| tls.server_ocsp_responder)
        .expect("coordinator responder");
    assert_eq!(coordinator_responder.refresh_interval_s, 3600);

    for responder in [
        r#"{"url": "ftp://ocsp.example.test"}"#,
        r#"{"url": "https://ocsp.example.test", "refresh_interval_s": 0}"#,
        r#"{"url": "https://ocsp.example.test", "refresh_interval_s": 86401}"#,
    ] {
        fs::write(
            &path,
            format!(
                r#"{{
                    "bind": "127.0.0.1:2101",
                    "account_db": "accounts.redb",
                    "tls_bind": "127.0.0.1:2102",
                    "tls_certificate": "server.crt",
                    "tls_private_key": "server.key",
                    "tls_ocsp_responder": {responder}
                }}"#
            ),
        )
        .expect("write invalid OCSP responder config");
        assert!(matches!(
            StandaloneConfig::load(&path),
            Err(ConfigError::InvalidOcspResponder)
        ));
    }
}

#[test]
fn validates_cluster_configuration_and_defaults_lease_ttl() {
    let directory = TempDirectory::new();
    let path = directory.path().join("caster.json");
    fs::write(
        &path,
        r#"{
            "bind": "127.0.0.1:2101",
            "account_db": "accounts.redb",
            "client_auth_require_tls": false,
            "source_auth_require_tls": false,
            "cluster": {
                "etcd_endpoints": ["http://127.0.0.1:2379"],
                "cluster_id": "test",
                "caster_id": "caster-1",
                "runtime_epoch": "boot-1",
                "relay_endpoint": "10.0.0.1:12102"
            }
        }"#,
    )
    .expect("write cluster config");
    let config = StandaloneConfig::load(&path).expect("load cluster config");
    assert_eq!(
        config.cluster.as_ref().map(|cluster| cluster.lease_ttl_s),
        Some(30)
    );
    assert_eq!(
        config
            .cluster
            .as_ref()
            .map(|cluster| cluster.relay_max_inbound),
        Some(500)
    );

    fs::write(
        &path,
        r#"{
            "bind": "127.0.0.1:2101",
            "account_db": "accounts.redb",
            "client_auth_require_tls": false,
            "source_auth_require_tls": false,
            "cluster": {
                "etcd_endpoints": [],
                "cluster_id": "test",
                "caster_id": "caster-1",
                "runtime_epoch": "boot-1",
                "relay_endpoint": "10.0.0.1:12102"
            }
        }"#,
    )
    .expect("write invalid cluster config");
    assert!(matches!(
        StandaloneConfig::load(&path),
        Err(ConfigError::InvalidCluster)
    ));
}

#[test]
fn validates_source_admission_configuration() {
    let directory = TempDirectory::new();
    let path = directory.path().join("caster.json");
    fs::write(
        &path,
        r#"{
            "bind": "127.0.0.1:2101",
            "account_db": "accounts.redb",
            "client_auth_require_tls": false,
            "source_auth_require_tls": false,
            "source_admission": {
                "mode": "enforce",
                "allowed_formats": ["rtcm3"],
                "require_correction_data": true,
                "min_valid_frames": 1,
                "max_bytes": 65536,
                "max_frames": 32,
                "timeout_ms": 5000
            }
        }"#,
    )
    .expect("write source admission config");
    let config = StandaloneConfig::load(&path).expect("load source admission config");
    assert_eq!(
        config
            .source_admission
            .as_ref()
            .map(|admission| admission.mode.as_str()),
        Some("enforce")
    );

    fs::write(
        &path,
        r#"{
            "bind": "127.0.0.1:2101",
            "account_db": "accounts.redb",
            "client_auth_require_tls": false,
            "source_auth_require_tls": false,
            "source_admission": {
                "mode": "enforce",
                "allowed_formats": ["rtcm2", "binex", "cmr", "nmea"],
                "require_correction_data": true,
                "min_valid_frames": 1,
                "max_bytes": 65536,
                "max_frames": 32,
                "timeout_ms": 5000
            }
        }"#,
    )
    .expect("write multi-format source admission config");
    StandaloneConfig::load(&path).expect("load multi-format source admission config");

    fs::write(
        &path,
        r#"{
            "bind": "127.0.0.1:2101",
            "account_db": "accounts.redb",
            "client_auth_require_tls": false,
            "source_auth_require_tls": false,
            "source_admission": {
                "mode": "enforce",
                "allowed_formats": ["rtcm4"],
                "require_correction_data": true,
                "min_valid_frames": 1,
                "max_bytes": 65536,
                "max_frames": 32,
                "timeout_ms": 5000
            }
        }"#,
    )
    .expect("write invalid source admission config");
    assert!(matches!(
        StandaloneConfig::load(&path),
        Err(ConfigError::InvalidSourceAdmission)
    ));

    fs::write(
        &path,
        r#"
            {
                "bind": "127.0.0.1:2101",
                "account_db": "accounts.redb",
                "client_auth_require_tls": false,
                "source_auth_require_tls": false,
                "max_validating_sources": 64,
                "validation_buffer_max_bytes": 1048576,
                "source_admission": {
                    "mode": "enforce",
                    "allowed_formats": ["rtcm3"],
                    "require_correction_data": true,
                    "min_valid_frames": 1,
                    "max_bytes": 65536,
                    "max_frames": 32,
                    "timeout_ms": 5000,
                    "read_timeout_ms": 30000
                }
            }
        "#,
    )
    .expect("write oversized validation budget");
    assert!(matches!(
        StandaloneConfig::load(&path),
        Err(ConfigError::InvalidSourceAdmission)
    ));
}

#[test]
fn loads_msm_version_restrictions() {
    let directory = TempDirectory::new();
    let path = directory.path().join("caster.json");
    fs::write(
        &path,
        r#"{
            "bind": "127.0.0.1:2101",
            "account_db": "accounts.redb",
            "client_auth_require_tls": false,
            "source_auth_require_tls": false,
            "source_admission": {
                "mode": "enforce",
                "allowed_formats": ["rtcm3"],
                "require_correction_data": true,
                "msm": {
                    "restrict_versions": true,
                    "allowed_versions": [4, 7]
                },
                "min_valid_frames": 1,
                "max_bytes": 65536,
                "max_frames": 512,
                "timeout_ms": 5000
            }
        }"#,
    )
    .expect("write MSM config");

    let config = StandaloneConfig::load(&path).expect("load MSM config");
    let msm = config.source_admission.expect("source admission").msm;
    assert!(msm.restrict_versions);
    assert_eq!(msm.allowed_versions, vec![4, 7]);
}

#[test]
fn loads_advanced_source_admission_rules() {
    let directory = TempDirectory::new();
    let path = directory.path().join("caster.json");
    fs::write(
        &path,
        r#"{
            "bind": "127.0.0.1:2101",
            "account_db": "accounts.redb",
            "client_auth_require_tls": false,
            "source_auth_require_tls": false,
            "source_admission": {
                "mode": "enforce",
                "allowed_formats": ["rtcm3"],
                "require_correction_data": true,
                "min_valid_frames": 3,
                "min_correction_frames": 2,
                "max_crc_error_ratio": 0.1,
                "require_base_position": true,
                "msm": {
                    "require_any": true,
                    "required_within_s": 30
                },
                "max_bytes": 65536,
                "max_frames": 200,
                "timeout_ms": 15000
            }
        }"#,
    )
    .expect("write advanced source admission config");

    let admission = StandaloneConfig::load(&path)
        .expect("load advanced source admission config")
        .source_admission
        .expect("source admission");
    assert_eq!(admission.min_correction_frames, 2);
    assert_eq!(admission.max_crc_error_ratio, Some(0.1));
    assert!(admission.require_base_position);
    assert!(admission.msm.require_any);
    assert_eq!(admission.msm.required_within_s, 30);
}

#[test]
fn rejects_invalid_msm_version_restrictions() {
    let directory = TempDirectory::new();
    let path = directory.path().join("caster.json");
    for allowed_versions in ["[]", "[0]", "[8]"] {
        fs::write(
            &path,
            format!(
                r#"{{
                    "bind": "127.0.0.1:2101",
                    "account_db": "accounts.redb",
                    "client_auth_require_tls": false,
                    "source_auth_require_tls": false,
                    "source_admission": {{
                        "mode": "enforce",
                        "allowed_formats": ["rtcm3"],
                        "require_correction_data": true,
                        "msm": {{
                            "restrict_versions": true,
                            "allowed_versions": {allowed_versions}
                        }},
                        "min_valid_frames": 1,
                        "max_bytes": 65536,
                        "max_frames": 512,
                        "timeout_ms": 5000
                    }}
                }}"#
            ),
        )
        .expect("write invalid MSM config");
        assert!(matches!(
            StandaloneConfig::load(&path),
            Err(ConfigError::InvalidSourceAdmission)
        ));
    }
}

#[test]
fn upgrade_requires_approval_by_default() {
    let directory = TempDirectory::new();
    let path = directory.path().join("caster.json");
    fs::write(
        &path,
        r#"{
            "bind": "127.0.0.1:2101",
            "account_db": "accounts.redb",
            "client_auth_require_tls": false,
            "source_auth_require_tls": false,
            "cluster": {
                "etcd_endpoints": ["http://127.0.0.1:2379"],
                "cluster_id": "test",
                "caster_id": "caster-1",
                "runtime_epoch": "boot-1",
                "relay_endpoint": "10.0.0.1:12102"
            },
            "upgrade": {
                "trusted_public_keys": [
                    "0000000000000000000000000000000000000000000000000000000000000000"
                ],
                "artifact_hosts": ["updates.example.test"]
            }
        }"#,
    )
    .expect("write upgrade config");
    assert!(
        StandaloneConfig::load(&path)
            .expect("upgrade config")
            .upgrade
            .expect("upgrade section")
            .require_approval
    );
}

#[cfg(unix)]
#[test]
fn updater_rejects_etcd_private_keys_accessible_by_group_or_other() {
    use std::os::unix::fs::PermissionsExt;

    let directory = TempDirectory::new();
    let path = directory.path().join("updater.json");
    for name in ["ca.pem", "client.pem", "client.key"] {
        fs::write(directory.path().join(name), "fixture").expect("write TLS fixture");
    }
    fs::write(
        &path,
        r#"
            {
                "etcd_endpoints": ["https://127.0.0.1:2379"],
                "etcd_tls": {
                    "ca_certificate": "ca.pem",
                    "certificate": "client.pem",
                    "private_key": "client.key",
                    "domain_name": "etcd.test"
                },
                "cluster_id": "test",
                "caster_id": "caster-1",
                "work_dir": "work",
                "service_program": "rcaster",
                "health_url": "http://127.0.0.1:9100/health/ready",
                "trusted_public_keys": [
                    "0000000000000000000000000000000000000000000000000000000000000000"
                ],
                "artifact_hosts": ["updates.test"]
            }
        "#,
    )
    .expect("write updater config");

    let key = directory.path().join("client.key");
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).expect("secure private key");
    UpdaterConfig::load(&path).expect("load updater config");

    fs::set_permissions(&key, fs::Permissions::from_mode(0o640)).expect("insecure private key");
    assert!(matches!(
        UpdaterConfig::load(&path),
        Err(ConfigError::InsecurePrivateKey)
    ));
}

#[test]
fn account_file_builds_a_valid_snapshot() {
    let account_file = AccountFile::from_slice(
        br#"{
            "users": [{
                "schema_version": 1,
                "mod_revision": 1,
                "user_id": "owner-id",
                "username": "owner",
                "password_hash": "hash",
                "auth_version": 1,
                "enabled": true,
                "ntrip_enabled": true,
                "web_enabled": true,
                "role": "user",
                "created_at": "2026-11-22T00:00:00Z",
                "updated_at": "2026-11-22T00:00:00Z"
            }],
            "mountpoints": [{
                "schema_version": 1,
                "mod_revision": 1,
                "mountpoint": "C63",
                "owner_user_id": "owner-id",
                "enabled": true,
                "ntrip1_upload_password_hash": null,
                "advertise": true,
                "region": "test",
                "created_at": "2026-11-22T00:00:00Z",
                "updated_at": "2026-11-22T00:00:00Z"
            }]
        }"#,
    )
    .expect("parse accounts");

    let snapshot = account_file.into_snapshot().expect("account snapshot");
    assert!(snapshot.user_by_username("owner").is_some());
    assert!(snapshot.mountpoint("C63").is_some());
}

#[tokio::test]
async fn shipped_example_accounts_authenticate() {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/standalone.accounts.json");
    let account_file = AccountFile::from_slice(&fs::read(path).expect("read example accounts"))
        .expect("parse example accounts");
    let authorizer =
        AccountAuthorizer::new(account_file.into_snapshot().expect("example snapshot"));

    let authorized = authorizer
        .authorize_source(
            "C63",
            SourceCredentials::Ntrip2 {
                username: "owner",
                password: "change-me",
            },
        )
        .await
        .expect("example credentials");
    assert_eq!(authorized.user_id, "owner-id");
}

#[test]
fn shipped_caster_examples_load() {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples");
    for name in ["standalone.caster.json", "cluster.caster.json"] {
        let config = StandaloneConfig::load(directory.join(name))
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert!(!config.client_auth_require_tls, "{name}");
        assert!(!config.source_auth_require_tls, "{name}");
    }
}

struct TempDirectory(PathBuf);

static NEXT_TEMP_DIRECTORY: AtomicU64 = AtomicU64::new(1);

impl TempDirectory {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "rcaster-app-{}-{unique}-{}",
            std::process::id(),
            NEXT_TEMP_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("create temp directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
