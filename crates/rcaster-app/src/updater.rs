use std::{
    collections::BTreeSet,
    error, fs,
    io::{self, Read},
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, GetOptions};
use rcaster_service::etcd_reload::{EtcdTlsSource, ReloadableEtcdClient};
use rcaster_service::upgrade::{
    SignedUpgradeManifest, UpgradeError, UpgradePolicy, verify_artifact,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    process::{Child, Command},
    time::{sleep, timeout},
};

use crate::config::UpdaterConfig;

type UpdaterResult<T> = Result<T, Box<dyn error::Error + Send + Sync>>;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Slot {
    Initial,
    A,
    B,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SlotState {
    current: Slot,
    previous: Option<Slot>,
}

impl Default for SlotState {
    fn default() -> Self {
        Self {
            current: Slot::Initial,
            previous: None,
        }
    }
}

pub async fn run_supervisor(config_path: PathBuf) -> UpdaterResult<()> {
    let config = UpdaterConfig::load(config_path)?;
    let etcd = connect_etcd(&config).await?;
    let client = etcd.client();
    let policy = UpgradePolicy::new(&config.trusted_public_keys, &config.artifact_hosts)
        .map_err(|error| format!("invalid updater policy: {error}"))?;
    let mut updater = Updater {
        config,
        client,
        _etcd: etcd,
        policy,
        child: None,
    };
    updater.start().await?;
    let mut seen = BTreeSet::new();
    let mut shutdown = Box::pin(shutdown_signal());
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                updater.stop().await;
                return Ok(());
            }
            _ = sleep(Duration::from_secs(1)) => {}
        }
        if let Err(error) = updater.poll_commands(&mut seen).await {
            eprintln!("rcaster-updater: command poll failed: {error}");
        }
        if let Err(error) = updater.ensure_service().await {
            eprintln!("rcaster-updater: service supervision failed: {error}");
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        if let Ok(mut terminate) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

struct Updater {
    config: UpdaterConfig,
    client: Client,
    _etcd: ReloadableEtcdClient,
    policy: UpgradePolicy,
    child: Option<Child>,
}

impl Updater {
    async fn start(&mut self) -> UpdaterResult<()> {
        self.start_child().await?;
        self.wait_healthy().await
    }

    async fn poll_commands(&mut self, seen: &mut BTreeSet<String>) -> UpdaterResult<()> {
        let prefix = format!(
            "/rcaster/{}/runtime/upgrade-commands/",
            self.config.cluster_id
        );
        let response = self
            .client
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await?;
        let mut commands = response
            .kvs()
            .iter()
            .filter_map(|value| serde_json::from_slice::<Value>(value.value()).ok())
            .filter(|command| command["caster_id"] == self.config.caster_id)
            .collect::<Vec<_>>();
        commands.sort_by_key(|command| command["created_at"].as_u64().unwrap_or_default());
        for command in commands {
            let fingerprint = format!(
                "{}\0{}\0{}",
                command["operation_id"].as_str().unwrap_or_default(),
                command["action"].as_str().unwrap_or_default(),
                command["created_at"].as_u64().unwrap_or_default(),
            );
            if seen.contains(&fingerprint) {
                continue;
            }
            seen.insert(fingerprint);
            if let Err(error) = self.apply_command(&command).await {
                self.write_state(&command, "FAILED", Some(&error.to_string()))
                    .await;
                eprintln!("rcaster-updater: upgrade command failed: {error}");
            }
        }
        Ok(())
    }

    async fn ensure_service(&mut self) -> UpdaterResult<()> {
        if let Some(child) = &mut self.child {
            match child.try_wait()? {
                None => return Ok(()),
                Some(status) => {
                    self.child = None;
                    eprintln!("rcaster-updater: service exited unexpectedly: {status}");
                }
            }
        }
        self.start_child().await?;
        self.wait_healthy().await
    }

    async fn apply_command(&mut self, command: &Value) -> UpdaterResult<()> {
        if command["schema_version"] != 1 {
            return Err("unsupported upgrade command schema".into());
        }
        let operation_id = command["operation_id"]
            .as_str()
            .ok_or("upgrade command has no operation ID")?;
        let action = command["action"]
            .as_str()
            .ok_or("upgrade command has no action")?;
        if !matches!(action, "apply" | "rollback") {
            return Err("upgrade command action is invalid".into());
        }
        let caster_id = self.config.caster_id.clone();
        if self
            .command_has_terminal_state(operation_id, &caster_id)
            .await?
        {
            return Ok(());
        }
        if !self
            .command_is_active(operation_id, action, &caster_id)
            .await
        {
            return Ok(());
        }
        let signed: SignedUpgradeManifest =
            serde_json::from_value(command["signed_manifest"].clone())?;
        let manifest = self.policy.verify(&signed)?;
        let mut state = self.read_state();
        let old = state.current;
        let replacement = if action == "apply" {
            let slot = inactive_slot(old);
            self.stage_artifact(operation_id, slot, &manifest).await?;
            slot
        } else {
            state
                .previous
                .ok_or("no previous upgrade slot is available")?
        };

        self.stop_child().await;
        state.current = replacement;
        state.previous = Some(old);
        self.write_slot_state(&state)?;
        self.start_child().await?;
        if self.wait_healthy().await.is_ok() {
            self.write_state(
                command,
                if action == "apply" {
                    "APPLIED"
                } else {
                    "ROLLED_BACK"
                },
                None,
            )
            .await;
            return Ok(());
        }

        let error = format!(
            "target version {} failed its readiness check",
            manifest.version
        );
        if action == "apply" && command["auto_rollback"] == true {
            self.stop_child().await;
            state.current = old;
            state.previous = Some(replacement);
            self.write_slot_state(&state)?;
            self.start_child().await?;
            if self.wait_healthy().await.is_ok() {
                self.write_state(command, "ROLLED_BACK", Some(&error)).await;
            } else {
                self.write_state(
                    command,
                    "FAILED",
                    Some("both the new binary and the previous slot are unhealthy"),
                )
                .await;
            }
        } else {
            self.write_state(command, "FAILED", Some(&error)).await;
        }
        eprintln!("rcaster-updater: {error}");
        Ok(())
    }

    async fn command_has_terminal_state(
        &mut self,
        operation_id: &str,
        caster_id: &str,
    ) -> UpdaterResult<bool> {
        let key = format!(
            "/rcaster/{}/runtime/upgrade-states/{operation_id}/{caster_id}",
            self.config.cluster_id
        );
        Ok(self
            .client
            .get(key, None)
            .await?
            .kvs()
            .first()
            .and_then(|value| serde_json::from_slice::<Value>(value.value()).ok())
            .is_some_and(|state| terminal_upgrade_state(&state)))
    }

    async fn stage_artifact(
        &self,
        operation_id: &str,
        slot: Slot,
        manifest: &rcaster_service::upgrade::UpgradeManifest,
    ) -> UpdaterResult<()> {
        if matches!(slot, Slot::Initial) {
            return Err("invalid staging slot".into());
        }
        ensure_staging_space(&self.config.work_dir, manifest.artifact_size)?;
        let staging = self
            .config
            .work_dir
            .join(format!(".artifact-{operation_id}.part"));
        if let Some(parent) = staging.parent() {
            fs::create_dir_all(parent)?;
        }
        let url = manifest.artifact_url.clone();
        let expected_size = manifest.artifact_size;
        let path = staging.clone();
        tokio::task::spawn_blocking(move || download_artifact(&url, &path, expected_size))
            .await??;
        verify_artifact(&staging, manifest)?;
        let target = self.slot_program(slot);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        if target.exists() {
            fs::remove_file(&target)?;
        }
        fs::rename(&staging, &target)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&target, fs::Permissions::from_mode(0o755))?;
        }
        Ok(())
    }

    async fn command_is_active(
        &mut self,
        operation_id: &str,
        action: &str,
        caster_id: &str,
    ) -> bool {
        let key = format!(
            "/rcaster/{}/runtime/upgrades/{operation_id}",
            self.config.cluster_id
        );
        let Ok(response) = self.client.get(key, None).await else {
            return false;
        };
        let Some(value) = response.kvs().first() else {
            return false;
        };
        let Ok(operation) = serde_json::from_slice::<Value>(value.value()) else {
            return false;
        };
        let expected_status = if action == "apply" {
            "RUNNING"
        } else {
            "ROLLING_BACK"
        };
        let current_node = operation["current_node"].as_u64().unwrap_or(u64::MAX) as usize;
        operation["status"] == expected_status
            && operation["nodes"]
                .get(current_node)
                .is_some_and(|node| node["caster_id"] == caster_id)
    }

    async fn start_child(&mut self) -> UpdaterResult<()> {
        self.stop_child().await;
        let state = self.read_state();
        let program = [
            state.current,
            state.previous.unwrap_or(Slot::Initial),
            Slot::Initial,
        ]
        .into_iter()
        .map(|slot| self.slot_program(slot))
        .find(|program| program.is_file())
        .ok_or("no service program is available")?;
        if !program.is_file() {
            return Err(format!("service program {} does not exist", program.display()).into());
        }
        let child = Command::new(&program)
            .args(&self.config.service_args)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        self.child = Some(child);
        Ok(())
    }

    async fn stop_child(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        request_graceful_stop(&mut child);
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(self.config.drain_timeout_s);
        loop {
            match child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if tokio::time::Instant::now() < deadline => {
                    sleep(Duration::from_millis(100)).await;
                }
                _ => break,
            }
        }
        let _ = child.kill().await;
        let _ = child.wait().await;
    }

    async fn wait_healthy(&mut self) -> UpdaterResult<()> {
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(self.config.health_timeout_s);
        loop {
            if let Some(child) = &mut self.child
                && let Some(status) = child.try_wait()?
            {
                return Err(format!("service exited during readiness check: {status}").into());
            }
            if probe_health(&self.config.health_url).await {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("service readiness timed out".into());
            }
            sleep(Duration::from_millis(250)).await;
        }
    }

    async fn stop(&mut self) {
        self.stop_child().await;
    }

    async fn write_state(&mut self, command: &Value, status: &str, error: Option<&str>) {
        let Some(operation_id) = command["operation_id"].as_str() else {
            return;
        };
        let key = format!(
            "/rcaster/{}/runtime/upgrade-states/{operation_id}/{}",
            self.config.cluster_id, self.config.caster_id
        );
        let value = json!({
            "schema_version": 1,
            "operation_id": operation_id,
            "caster_id": self.config.caster_id,
            "status": status,
            "target_version": command["target_version"],
            "updated_at": now_timestamp(),
            "error": error,
        });
        if let Ok(value) = serde_json::to_vec(&value) {
            let _ = self.client.put(key, value, None).await;
        }
    }

    fn read_state(&self) -> SlotState {
        let current = self.config.work_dir.join("slots.json");
        let previous = self.config.work_dir.join("slots.old");
        [current, previous]
            .into_iter()
            .find_map(|path| {
                fs::read(path)
                    .ok()
                    .and_then(|value| serde_json::from_slice(&value).ok())
            })
            .unwrap_or_default()
    }

    fn write_slot_state(&self, state: &SlotState) -> UpdaterResult<()> {
        fs::create_dir_all(&self.config.work_dir)?;
        let path = self.config.work_dir.join("slots.json");
        #[cfg(windows)]
        let old = self.config.work_dir.join("slots.old");
        let next = self.config.work_dir.join("slots.next");
        fs::write(&next, serde_json::to_vec(state)?)?;
        #[cfg(windows)]
        {
            if path.exists() {
                let _ = fs::remove_file(&old);
                fs::rename(&path, &old)?;
            }
        }
        if fs::rename(&next, &path).is_err() {
            #[cfg(windows)]
            {
                let _ = fs::rename(&old, &path);
            }
            return Err("cannot persist the active upgrade slot".into());
        }
        #[cfg(windows)]
        let _ = fs::remove_file(old);
        Ok(())
    }

    fn slot_program(&self, slot: Slot) -> PathBuf {
        match slot {
            Slot::Initial => self.config.service_program.clone(),
            Slot::A | Slot::B => {
                let name = self
                    .config
                    .service_program
                    .file_name()
                    .unwrap_or_else(|| std::ffi::OsStr::new("rcaster"));
                self.config
                    .work_dir
                    .join("slots")
                    .join(match slot {
                        Slot::A => "a",
                        Slot::B => "b",
                        Slot::Initial => unreachable!(),
                    })
                    .join(name)
            }
        }
    }
}

fn request_graceful_stop(child: &mut Child) {
    #[cfg(unix)]
    if let Some(process_id) = child.id() {
        // SAFETY: process_id came from the live Child handle. SIGTERM targets
        // only that process; failure means it already exited.
        unsafe {
            libc::kill(process_id as i32, libc::SIGTERM);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = child.start_kill();
    }
}

fn inactive_slot(current: Slot) -> Slot {
    match current {
        Slot::A => Slot::B,
        Slot::B | Slot::Initial => Slot::A,
    }
}

fn terminal_upgrade_state(state: &Value) -> bool {
    matches!(
        state["status"].as_str(),
        Some("APPLIED" | "FAILED" | "ROLLED_BACK")
    )
}

fn ensure_staging_space(path: &Path, artifact_size: u64) -> UpdaterResult<()> {
    fs::create_dir_all(path)?;
    let available = available_disk_bytes(path)?;
    if available < artifact_size {
        return Err(format!(
            "artifact staging requires {artifact_size} bytes, but only {available} bytes are available"
        )
        .into());
    }
    Ok(())
}

#[cfg(unix)]
fn available_disk_bytes(path: &Path) -> io::Result<u64> {
    use std::{ffi::CString, mem::MaybeUninit, os::unix::ffi::OsStrExt};

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains a NUL byte"))?;
    let mut stats = MaybeUninit::<libc::statvfs>::zeroed();
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let stats = unsafe { stats.assume_init() };
    let block_size = if stats.f_frsize == 0 {
        stats.f_bsize
    } else {
        stats.f_frsize
    };
    Ok(u128::from(stats.f_bavail)
        .saturating_mul(u128::from(block_size))
        .min(u128::from(u64::MAX)) as u64)
}

#[cfg(not(unix))]
fn available_disk_bytes(_path: &Path) -> io::Result<u64> {
    Ok(u64::MAX)
}

fn download_artifact(url: &str, path: &Path, expected_size: u64) -> UpdaterResult<()> {
    let agent = ureq::AgentBuilder::new()
        .redirects(0)
        .timeout_connect(Duration::from_secs(10))
        .timeout_read(Duration::from_secs(60))
        .build();
    let response = agent.get(url).call().map_err(|error| {
        UpgradeError::InvalidManifest(format!("artifact download failed: {error}"))
    })?;
    let mut file = fs::File::create(path)?;
    let copied = io::copy(
        &mut response.into_reader().take(expected_size.saturating_add(1)),
        &mut file,
    )?;
    file.sync_all()?;
    if copied != expected_size {
        let _ = fs::remove_file(path);
        return Err(UpgradeError::InvalidManifest(format!(
            "artifact download size mismatch: expected {expected_size}, got {copied}"
        ))
        .into());
    }
    Ok(())
}

async fn probe_health(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("http://") else {
        return false;
    };
    let (authority, path) = rest
        .split_once('/')
        .map(|(authority, path)| (authority, format!("/{path}")))
        .unwrap_or((rest, "/health/ready".to_owned()));
    let Ok(Ok(mut stream)) = timeout(Duration::from_secs(2), TcpStream::connect(authority)).await
    else {
        return false;
    };
    let request = format!("GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).await.is_err() {
        return false;
    }
    let mut response = [0_u8; 4096];
    let Ok(Ok(read)) = timeout(Duration::from_secs(2), stream.read(&mut response)).await else {
        return false;
    };
    response[..read].starts_with(b"HTTP/1.1 200") || response[..read].starts_with(b"HTTP/1.0 200")
}

async fn connect_etcd(config: &UpdaterConfig) -> UpdaterResult<ReloadableEtcdClient> {
    ReloadableEtcdClient::connect(
        config.etcd_endpoints.clone(),
        config.etcd_tls.as_ref().map(|tls| EtcdTlsSource {
            ca_certificate: tls.ca_certificate.clone(),
            certificate: tls.certificate.clone(),
            private_key: tls.private_key.clone(),
            domain_name: tls.domain_name.clone(),
        }),
        Duration::from_secs(10),
    )
    .await
}

fn now_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
        time::Duration,
    };

    use super::{
        Slot, Updater, connect_etcd, download_artifact, ensure_staging_space, inactive_slot,
        probe_health, request_graceful_stop, terminal_upgrade_state,
    };
    use crate::config::UpdaterConfig;
    use rcaster_service::upgrade::UpgradePolicy;

    #[test]
    fn alternates_between_two_slots() {
        assert_eq!(inactive_slot(Slot::Initial), Slot::A);
        assert_eq!(inactive_slot(Slot::A), Slot::B);
        assert_eq!(inactive_slot(Slot::B), Slot::A);
    }

    #[test]
    fn only_updater_completion_states_suppress_command_replay() {
        assert!(!terminal_upgrade_state(
            &serde_json::json!({"status":"DRAINING"})
        ));
        assert!(terminal_upgrade_state(
            &serde_json::json!({"status":"APPLIED"})
        ));
        assert!(terminal_upgrade_state(
            &serde_json::json!({"status":"FAILED"})
        ));
        assert!(terminal_upgrade_state(
            &serde_json::json!({"status":"ROLLED_BACK"})
        ));
        assert!(!terminal_upgrade_state(&serde_json::json!({})));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_artifacts_larger_than_available_staging_space() {
        let path =
            std::env::temp_dir().join(format!("rcaster-updater-space-{}", std::process::id()));
        ensure_staging_space(&path, 0).expect("zero-byte staging");
        assert!(ensure_staging_space(&path, u64::MAX).is_err());
        let _ = std::fs::remove_dir_all(path);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn supervisor_restarts_an_exited_service() {
        use std::{fs, net::TcpListener, os::unix::fs::PermissionsExt};
        use tokio::process::Child;

        let root =
            std::env::temp_dir().join(format!("rcaster-updater-restart-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("health")).expect("health dir");
        fs::write(root.join("health/ready"), b"ready\n").expect("health response");
        let listener = TcpListener::bind("127.0.0.1:0").expect("health port");
        let port = listener.local_addr().expect("health address").port();
        drop(listener);
        let service = root.join("service.sh");
        fs::write(
            &service,
            format!(
                "#!/bin/sh\nexec python3 -m http.server {port} --bind 127.0.0.1 --directory '{}' >/dev/null 2>&1\n",
                root.display()
            ),
        )
        .expect("service script");
        fs::set_permissions(&service, fs::Permissions::from_mode(0o755)).expect("service mode");
        let config = UpdaterConfig {
            etcd_endpoints: vec!["http://127.0.0.1:2379".to_owned()],
            etcd_tls: None,
            cluster_id: format!("updater-restart-{}", std::process::id()),
            caster_id: "edge-1".to_owned(),
            work_dir: root.clone(),
            service_program: service,
            service_args: Vec::new(),
            health_url: format!("http://127.0.0.1:{port}/health/ready"),
            trusted_public_keys: vec![
                "66be7e332c7a453332bd9d0a7f7db055f5c5ef1a06ada66d98b39fb6810c473a".to_owned(),
            ],
            artifact_hosts: vec!["updates.example.test".to_owned()],
            health_timeout_s: 5,
            drain_timeout_s: 2,
        };
        let etcd = connect_etcd(&config).await.expect("etcd");
        let client = etcd.client();
        let policy = UpgradePolicy::new(&config.trusted_public_keys, &config.artifact_hosts)
            .expect("policy");
        let mut updater = Updater {
            config,
            client,
            _etcd: etcd,
            policy,
            child: None,
        };
        updater.start().await.expect("initial service");
        let first_pid = updater
            .child
            .as_ref()
            .and_then(Child::id)
            .expect("first pid");
        let child = updater.child.as_mut().expect("child");
        child.start_kill().expect("kill child");
        let _ = child.wait().await.expect("wait child");
        updater.ensure_service().await.expect("restart service");
        let second_pid = updater
            .child
            .as_ref()
            .and_then(Child::id)
            .expect("second pid");
        assert_ne!(second_pid, first_pid);
        assert!(probe_health(&updater.config.health_url).await);
        updater.stop().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn graceful_stop_sends_sigterm_before_kill() {
        let mut child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg("trap 'exit 0' TERM; while :; do sleep 1; done")
            .spawn()
            .expect("service child");
        tokio::time::sleep(Duration::from_millis(100)).await;
        request_graceful_stop(&mut child);
        let status = tokio::time::timeout(Duration::from_secs(2), child.wait())
            .await
            .expect("service should exit after SIGTERM")
            .expect("service wait");
        assert!(status.success());
    }

    #[tokio::test]
    async fn health_probe_rejects_non_http_urls() {
        assert!(!probe_health("https://127.0.0.1:1/health/ready").await);
    }

    #[test]
    fn downloads_exact_artifact_without_unbounded_buffering() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("artifact listener");
        let address = listener.local_addr().expect("artifact address");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("artifact request");
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\ntest")
                .expect("artifact response");
        });
        let path = std::env::temp_dir().join(format!(
            "rcaster-updater-artifact-{}.bin",
            std::process::id()
        ));
        download_artifact(&format!("http://{address}/artifact"), &path, 4)
            .expect("artifact download");
        assert_eq!(std::fs::read(&path).expect("artifact"), b"test");
        let _ = std::fs::remove_file(path);
        server.join().expect("artifact server");
    }

    #[test]
    fn rejects_artifact_redirects() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("redirect listener");
        let address = listener.local_addr().expect("redirect address");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("redirect request");
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request);
            stream
                .write_all(
                    b"HTTP/1.1 302 Found\r\nLocation: https://evil.example/rcaster\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .expect("redirect response");
        });
        let path = std::env::temp_dir().join(format!(
            "rcaster-updater-redirect-{}.bin",
            std::process::id()
        ));
        let result = download_artifact(&format!("http://{address}/artifact"), &path, 4);
        assert!(result.is_err(), "artifact redirects must not be followed");
        assert!(!path.exists());
        let _ = std::fs::remove_file(path);
        server.join().expect("redirect server");
    }

    #[tokio::test]
    async fn health_probe_accepts_ready_response() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("health listener");
        let address = listener.local_addr().expect("health address");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("health request");
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nready\n",
                )
                .expect("health response");
        });
        assert!(probe_health(&format!("http://{address}/health/ready")).await);
        server.join().expect("health server");
    }

    #[tokio::test]
    async fn health_probe_accepts_http10_ready_response() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("health listener");
        let address = listener.local_addr().expect("health address");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("health request");
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request);
            stream
                .write_all(
                    b"HTTP/1.0 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nready\n",
                )
                .expect("health response");
        });
        assert!(probe_health(&format!("http://{address}/health/ready")).await);
        server.join().expect("health server");
    }
}
