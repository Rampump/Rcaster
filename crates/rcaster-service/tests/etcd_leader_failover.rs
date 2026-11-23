use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, ConnectOptions, Member};
use rcaster_service::config::{CasterConfig, ConfigWatcher, publish_config};
use tokio::{
    process::{Child, Command},
    time::sleep,
};

#[tokio::test]
#[ignore = "requires RCASTER_ETCD_BIN"]
async fn config_watch_recovers_after_a_leader_failure() {
    let etcd = std::env::var("RCASTER_ETCD_BIN").expect("RCASTER_ETCD_BIN");
    let directory = TempDirectory::new();
    let ports = available_ports(6);
    let ports = ports.as_slice();
    let members = (0..3)
        .map(|index| MemberPorts {
            index,
            client: ports[index * 2],
            peer: ports[index * 2 + 1],
        })
        .collect::<Vec<_>>();
    let endpoints = members
        .iter()
        .map(|member| member.client_url())
        .collect::<Vec<_>>();
    let token = format!("rcaster-failover-{}", unique("token"));
    let mut processes = Vec::new();
    for member in &members {
        processes.push(
            start_etcd(&etcd, directory.path(), member, &members, &token, "new")
                .await
                .expect("start etcd member"),
        );
    }

    let mut admin = connect_with_retry(&endpoints).await;
    let cluster = unique("leader-failover");
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish initial config");
    let watcher = ConfigWatcher::start(
        Client::connect(endpoints.clone(), Some(ConnectOptions::new()))
            .await
            .expect("watcher client"),
        &cluster,
    )
    .await
    .expect("start config watcher");
    assert_eq!(watcher.snapshot().config.generation, 1);

    let leader = wait_for_leader(&mut admin, &members).await;
    processes[leader].kill().await.expect("kill leader");
    processes[leader]
        .wait()
        .await
        .expect("wait for killed leader");
    let remaining = endpoints
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != leader)
        .map(|(_, endpoint)| endpoint.clone())
        .collect::<Vec<_>>();
    let mut surviving_admin = connect_with_retry(&remaining).await;
    wait_for_new_leader(&mut surviving_admin, leader).await;
    publish_with_retry(&mut surviving_admin, &cluster, CasterConfig::new(2)).await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if watcher.is_healthy() && watcher.snapshot().config.generation == 2 {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("config watcher did not converge after leader failover");

    watcher.shutdown().await;
    for process in &mut processes {
        let _ = process.kill().await;
        let _ = process.wait().await;
    }
}

#[tokio::test]
#[ignore = "requires RCASTER_ETCD_BIN"]
async fn config_watch_recovers_after_full_quorum_loss() {
    let etcd = std::env::var("RCASTER_ETCD_BIN").expect("RCASTER_ETCD_BIN");
    let directory = TempDirectory::new();
    let ports = available_ports(6);
    let ports = ports.as_slice();
    let members = (0..3)
        .map(|index| MemberPorts {
            index,
            client: ports[index * 2],
            peer: ports[index * 2 + 1],
        })
        .collect::<Vec<_>>();
    let endpoints = members
        .iter()
        .map(|member| member.client_url())
        .collect::<Vec<_>>();
    let token = format!("rcaster-quorum-loss-{}", unique("token"));
    let mut processes = Vec::new();
    for member in &members {
        processes.push(
            start_etcd(&etcd, directory.path(), member, &members, &token, "new")
                .await
                .expect("start etcd member"),
        );
    }

    let mut admin = connect_with_retry(&endpoints).await;
    let cluster = unique("quorum-loss");
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish initial config");
    let watcher = ConfigWatcher::start(
        Client::connect(endpoints.clone(), Some(ConnectOptions::new()))
            .await
            .expect("watcher client"),
        &cluster,
    )
    .await
    .expect("start config watcher");
    let leader = wait_for_leader(&mut admin, &members).await;
    let victims = [(leader + 1) % members.len(), (leader + 2) % members.len()];

    for &victim in &victims {
        processes[victim].kill().await.expect("kill etcd member");
        processes[victim]
            .wait()
            .await
            .expect("wait for killed member");
    }

    tokio::time::timeout(Duration::from_secs(40), async {
        while watcher.is_healthy() {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("config watcher did not detect full quorum loss");

    for &victim in &victims {
        processes[victim] = start_etcd(
            &etcd,
            directory.path(),
            &members[victim],
            &members,
            &token,
            "existing",
        )
        .await
        .expect("restart etcd member");
    }

    let mut recovered_admin = connect_with_retry(&endpoints).await;
    publish_with_retry(&mut recovered_admin, &cluster, CasterConfig::new(2)).await;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if watcher.is_healthy() && watcher.snapshot().config.generation == 2 {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("config watcher did not converge after quorum recovery");

    watcher.shutdown().await;
    for process in &mut processes {
        let _ = process.kill().await;
        let _ = process.wait().await;
    }
}

async fn start_etcd(
    binary: &str,
    directory: &Path,
    member: &MemberPorts,
    members: &[MemberPorts],
    token: &str,
    cluster_state: &str,
) -> std::io::Result<Child> {
    let initial_cluster = members
        .iter()
        .map(|member| format!("{}={}", member.name(), member.peer_url()))
        .collect::<Vec<_>>()
        .join(",");
    Command::new(binary)
        .arg(format!("--name={}", member.name()))
        .arg(format!(
            "--data-dir={}",
            directory.join(member.name()).display()
        ))
        .arg(format!("--listen-client-urls={}", member.client_url()))
        .arg(format!("--advertise-client-urls={}", member.client_url()))
        .arg(format!("--listen-peer-urls={}", member.peer_url()))
        .arg(format!(
            "--initial-advertise-peer-urls={}",
            member.peer_url()
        ))
        .arg(format!("--initial-cluster={initial_cluster}"))
        .arg(format!("--initial-cluster-state={cluster_state}"))
        .arg(format!("--initial-cluster-token={token}"))
        .arg("--log-level=error")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
}

async fn connect_with_retry(endpoints: &[String]) -> Client {
    for _ in 0..100 {
        if let Ok(mut client) =
            Client::connect(endpoints.to_vec(), Some(ConnectOptions::new())).await
            && client.status().await.is_ok()
        {
            return client;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("etcd cluster did not become ready: {endpoints:?}");
}

async fn wait_for_leader(client: &mut Client, members: &[MemberPorts]) -> usize {
    for _ in 0..100 {
        if let Ok(status) = client.status().await
            && status.leader() != 0
            && let Ok(list) = client.member_list().await
            && let Some(member) = list
                .members()
                .iter()
                .find(|member| member.id() == status.leader())
            && let Some(index) = member_index(member, members)
        {
            return index;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("etcd did not elect a leader");
}

async fn wait_for_new_leader(client: &mut Client, old_leader: usize) {
    for _ in 0..150 {
        if let Ok(status) = client.status().await
            && status.leader() != 0
            && let Ok(list) = client.member_list().await
            && let Some(member) = list
                .members()
                .iter()
                .find(|member| member.id() == status.leader())
            && member.name() != format!("member-{old_leader}")
        {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("etcd did not elect a replacement leader");
}

fn member_index(member: &Member, members: &[MemberPorts]) -> Option<usize> {
    members
        .iter()
        .position(|candidate| member.name() == candidate.name())
}

async fn publish_with_retry(client: &mut Client, cluster: &str, config: CasterConfig) {
    let mut last_error = None;
    for _ in 0..100 {
        match publish_config(client, cluster, config.clone()).await {
            Ok(_) => return,
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
    panic!(
        "config publish did not recover: {}",
        last_error.expect("error")
    );
}

struct MemberPorts {
    index: usize,
    client: u16,
    peer: u16,
}

impl MemberPorts {
    fn name(&self) -> String {
        format!("member-{}", self.index)
    }

    fn client_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.client)
    }

    fn peer_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.peer)
    }
}

fn available_ports(count: usize) -> Vec<u16> {
    let listeners = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("bind temporary port"))
        .collect::<Vec<_>>();
    let ports = listeners
        .iter()
        .map(|listener| listener.local_addr().expect("temporary address").port())
        .collect();
    drop(listeners);
    ports
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    )
}

struct TempDirectory(PathBuf);

impl TempDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(unique("rcaster-etcd-failover"));
        fs::create_dir_all(&path).expect("create temporary directory");
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
