use std::{
    net::{SocketAddr, ToSocketAddrs},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, ConnectOptions};
use rcaster_service::config::{CasterConfig, ConfigWatcher, publish_config};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, duplex},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinSet,
    time::sleep,
};

#[tokio::test]
async fn config_watch_recovers_after_tcp_partition() {
    let endpoint =
        std::env::var("RCASTER_ETCD_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:2379".into());
    let backend = etcd_socket_address(&endpoint);
    let proxy_port = available_port();
    let mut proxy = Proxy::start(proxy_port, backend).await;
    let watched_endpoint = format!("http://127.0.0.1:{proxy_port}");
    let cluster = unique("partition-recovery");
    let mut admin = connect(&endpoint).await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish generation 1");
    let watcher = ConfigWatcher::start(connect(&watched_endpoint).await, &cluster)
        .await
        .expect("start config watcher");

    proxy.stop().await;
    wait_until(Duration::from_secs(5), || !watcher.is_healthy()).await;
    publish_config(&mut admin, &cluster, CasterConfig::new(2))
        .await
        .expect("publish generation 2 while partitioned");
    proxy = Proxy::start(proxy_port, backend).await;

    wait_until(Duration::from_secs(10), || {
        watcher.is_healthy() && watcher.snapshot().config.generation == 2
    })
    .await;

    watcher.shutdown().await;
    proxy.stop().await;
    admin
        .delete(
            format!("/rcaster/{cluster}/"),
            Some(etcd_client::DeleteOptions::new().with_prefix()),
        )
        .await
        .expect("delete test cluster");
}

#[tokio::test]
async fn config_watch_detects_silent_stream_stall_and_recovers() {
    let endpoint =
        std::env::var("RCASTER_ETCD_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:2379".into());
    let backend = etcd_socket_address(&endpoint);
    let proxy_port = available_port();
    let proxy = Proxy::start(proxy_port, backend).await;
    let watched_endpoint = format!("http://127.0.0.1:{proxy_port}");
    let cluster = unique("silent-recovery");
    let mut admin = connect(&endpoint).await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish generation 1");
    let watcher = ConfigWatcher::start(connect(&watched_endpoint).await, &cluster)
        .await
        .expect("start config watcher");
    wait_until(Duration::from_secs(5), || watcher.is_healthy()).await;
    publish_config(&mut admin, &cluster, CasterConfig::new(2))
        .await
        .expect("publish generation 2 to establish watch");
    wait_until(Duration::from_secs(5), || {
        watcher.is_healthy() && watcher.snapshot().config.generation == 2
    })
    .await;

    proxy.blackhole();
    publish_config(&mut admin, &cluster, CasterConfig::new(3))
        .await
        .expect("publish generation 3 while stream is silent");
    wait_until(Duration::from_secs(40), || !watcher.is_healthy()).await;

    proxy.resume();
    wait_until(Duration::from_secs(10), || {
        watcher.is_healthy() && watcher.snapshot().config.generation == 3
    })
    .await;

    watcher.shutdown().await;
    proxy.stop().await;
    admin
        .delete(
            format!("/rcaster/{cluster}/"),
            Some(etcd_client::DeleteOptions::new().with_prefix()),
        )
        .await
        .expect("delete test cluster");
}

#[tokio::test]
async fn pausable_proxy_stops_forwarding_while_blackholed() {
    let (mut source, source_reader) = duplex(1024);
    let (sink_writer, mut sink) = duplex(1024);
    let (blocked, blocked_rx) = watch::channel(false);
    let copy = tokio::spawn(copy_pausable(source_reader, sink_writer, blocked_rx));

    source.write_all(b"before").await.expect("write before");
    let mut buffer = [0_u8; 6];
    sink.read_exact(&mut buffer).await.expect("read before");
    assert_eq!(&buffer, b"before");

    blocked.send(true).expect("blackhole");
    sleep(Duration::from_millis(50)).await;
    source.write_all(b"held").await.expect("write held");
    assert!(
        tokio::time::timeout(Duration::from_millis(250), sink.read(&mut buffer))
            .await
            .is_err(),
        "proxy forwarded data while blackholed"
    );

    blocked.send(false).expect("resume");
    let read = tokio::time::timeout(Duration::from_secs(1), sink.read(&mut buffer))
        .await
        .expect("read after resume")
        .expect("resume read");
    assert_eq!(&buffer[..read], b"held");

    drop(source);
    copy.await.expect("copy task");
}

async fn wait_until(limit: Duration, condition: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + limit;
    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition did not become true before timeout"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

async fn connect(endpoint: &str) -> Client {
    for _ in 0..100 {
        if let Ok(client) = Client::connect([endpoint], Some(ConnectOptions::new())).await {
            return client;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("etcd endpoint did not become ready: {endpoint}");
}

fn etcd_socket_address(endpoint: &str) -> SocketAddr {
    let authority = endpoint
        .strip_prefix("http://")
        .expect("partition test requires an http etcd endpoint");
    authority
        .to_socket_addrs()
        .expect("resolve etcd endpoint")
        .next()
        .expect("etcd endpoint address")
}

fn available_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind temporary port")
        .local_addr()
        .expect("temporary address")
        .port()
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

struct Proxy {
    stop: watch::Sender<bool>,
    blocked: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl Proxy {
    async fn start(port: u16, backend: SocketAddr) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("bind proxy listener");
        let (stop, mut stopped) = watch::channel(false);
        let (blocked, blocked_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = stopped.changed() => break,
                    accepted = listener.accept() => {
                        let Ok((client, _)) = accepted else {
                            break;
                        };
                        let blocked = blocked_rx.clone();
                        connections.spawn(async move {
                            if let Ok(server) = TcpStream::connect(backend).await {
                                let (client_reader, client_writer) = client.into_split();
                                let (server_reader, server_writer) = server.into_split();
                                let mut client_to_server = tokio::spawn(copy_pausable(
                                    client_reader,
                                    server_writer,
                                    blocked.clone(),
                                ));
                                let mut server_to_client = tokio::spawn(copy_pausable(
                                    server_reader,
                                    client_writer,
                                    blocked,
                                ));
                                tokio::select! {
                                    _ = &mut client_to_server => {}
                                    _ = &mut server_to_client => {}
                                }
                                client_to_server.abort();
                                server_to_client.abort();
                            }
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Self {
            stop,
            blocked,
            task,
        }
    }

    fn blackhole(&self) {
        let _ = self.blocked.send(true);
    }

    fn resume(&self) {
        let _ = self.blocked.send(false);
    }

    async fn stop(self) {
        let _ = self.stop.send(true);
        let _ = self.task.await;
    }
}

async fn copy_pausable<R, W>(mut reader: R, mut writer: W, mut blocked: watch::Receiver<bool>)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buffer = [0_u8; 8192];
    loop {
        while *blocked.borrow() {
            if blocked.changed().await.is_err() {
                return;
            }
        }
        tokio::select! {
            result = reader.read(&mut buffer) => {
                let Ok(read) = result else {
                    return;
                };
                if read == 0 || writer.write_all(&buffer[..read]).await.is_err() {
                    return;
                }
            }
            result = blocked.changed() => {
                if result.is_err() {
                    return;
                }
            }
        }
    }
}
