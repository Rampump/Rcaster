use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use rcaster_service::{
    auth::{
        AuthError, AuthorizedUser, ClientAuthorizer, ClientCredentials, SourceAuthorizer,
        SourceCredentials,
    },
    health::process_usage,
    hub::{HubLimits, MountpointHub},
    ntrip_server::{NtripServerConfig, serve_ntrip},
    session_limit::{ConnectionLimiter, LocalSessionLimiter, SessionLimitConfig},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket, TcpStream},
    sync::watch,
};

struct TestAuthorizer;

impl SourceAuthorizer for TestAuthorizer {
    async fn authorize_source(
        &self,
        _mountpoint: &str,
        _credentials: SourceCredentials<'_>,
    ) -> Result<AuthorizedUser, AuthError> {
        Ok(user())
    }
}

impl ClientAuthorizer for TestAuthorizer {
    async fn authorize_client(
        &self,
        _mountpoint: &str,
        _credentials: Option<ClientCredentials<'_>>,
    ) -> Result<AuthorizedUser, AuthError> {
        Ok(user())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options::from_args();
    let process_start = process_usage();
    let max_rss = Arc::new(AtomicU64::new(
        process_start.map(|usage| usage.0).unwrap_or_default(),
    ));
    let max_fds = Arc::new(AtomicU64::new(
        process_start.map(|usage| usage.1).unwrap_or_default(),
    ));
    let (usage_stop_tx, usage_stop_rx) = watch::channel(false);
    let usage_monitor = tokio::spawn(monitor_process_usage(
        usage_stop_rx,
        Arc::clone(&max_rss),
        Arc::clone(&max_fds),
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let hub = Arc::new(MountpointHub::new(
        options.subscribers.saturating_mul(8).max(64),
    )?);
    hub.set_limits(HubLimits {
        max_sources: options.sources.max(1),
        max_validating_sources: options.sources.max(1),
        max_subscribers: options.subscribers.max(1),
        max_subscribers_per_source: options.subscribers.max(1),
    });
    let total_connections = options.sources.saturating_add(options.subscribers);
    let config = NtripServerConfig {
        head_timeout: Duration::from_secs(5),
        content_length_max_bytes: 1024 * 1024,
        client_auth_required: false,
        session_limiter: Arc::new(LocalSessionLimiter::new(SessionLimitConfig {
            default_max_sessions: -1,
            default_max_ip_sessions: -1,
            ..SessionLimitConfig::default()
        })?),
        connection_limiter: Arc::new(capacity_connection_limiter(total_connections)?),
        cluster_runtime: None,
        relay_manager: None,
        source_admission: None,
        near: None,
        metrics: None,
        connections: None,
        proxy_protocol: None,
        auth_login_limiter: None,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(serve_ntrip(
        listener,
        Arc::clone(&hub),
        TestAuthorizer,
        config,
        wait_for_stop(shutdown_rx.clone()),
    ));

    let mut source_streams = Vec::new();
    for index in 0..options.sources {
        let mountpoint = format!("S{index}");
        let mut stream = connect_capacity_client(address, index).await?;
        stream
            .write_all(
                format!("SOURCE secret /{mountpoint}\r\nSource-Agent: rcaster-soak\r\n\r\n")
                    .as_bytes(),
            )
            .await?;
        expect_success(read_head(&mut stream).await?)
            .map_err(|error| format!("source S{index}: {error}"))?;
        source_streams.push(stream);
    }

    let mut subscriber_streams = Vec::new();
    for index in 0..options.subscribers {
        let mountpoint = format!("S{}", index % options.sources);
        let mut stream = connect_capacity_client(address, options.sources + index)
            .await
            .map_err(|error| {
                std::io::Error::new(error.kind(), format!("subscriber {index} connect: {error}"))
            })?;
        stream
            .write_all(format!("GET /{mountpoint}\r\nUser-Agent: rcaster-soak\r\n\r\n").as_bytes())
            .await
            .map_err(|error| {
                std::io::Error::new(error.kind(), format!("subscriber {index} request: {error}"))
            })?;
        let head = read_head(&mut stream).await.map_err(|error| {
            std::io::Error::new(
                error.kind(),
                format!("subscriber {index} response: {error}"),
            )
        })?;
        expect_success(head).map_err(|error| format!("subscriber {index}: {error}"))?;
        subscriber_streams.push(stream);
    }

    let (stop_tx, stop_rx) = watch::channel(false);
    let sources = source_streams
        .into_iter()
        .map(|stream| tokio::spawn(source_loop(stream, options.rate_kib, stop_rx.clone())))
        .collect::<Vec<_>>();
    let subscribers = subscriber_streams
        .into_iter()
        .map(|stream| tokio::spawn(subscriber_loop(stream, stop_rx.clone())))
        .collect::<Vec<_>>();

    let started = Instant::now();
    tokio::time::sleep(Duration::from_secs(options.duration_s)).await;
    let elapsed = started.elapsed().as_secs_f64();
    stop_tx.send_replace(true);

    let source_bytes = join_sources(sources).await;
    let subscriber_bytes = join_subscribers(subscribers).await;
    wait_for_idle(&hub, Duration::from_secs(5)).await?;
    usage_stop_tx.send_replace(true);
    usage_monitor.await?;
    let process_end = process_usage();
    shutdown_tx.send_replace(true);
    server.await??;

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "duration_s": elapsed,
            "sources": options.sources,
            "subscribers": options.subscribers,
            "rate_kib_per_source": options.rate_kib,
            "source_bytes": source_bytes,
            "subscriber_bytes": subscriber_bytes,
            "subscriber_mib_per_second": subscriber_bytes as f64 / elapsed / 1_048_576.0,
            "active_sources": hub.active_sources().len(),
            "subscribers_remaining": hub.metrics().subscribers,
            "process_rss_start_bytes": process_start.map(|usage| usage.0),
            "process_rss_max_bytes": (max_rss.load(Ordering::Relaxed) != 0)
                .then(|| max_rss.load(Ordering::Relaxed)),
            "process_fds_start": process_start.map(|usage| usage.1),
            "process_fds_max": (max_fds.load(Ordering::Relaxed) != 0)
                .then(|| max_fds.load(Ordering::Relaxed)),
            "process_rss_end_bytes": process_end.map(|usage| usage.0),
            "process_fds_end": process_end.map(|usage| usage.1),
        }))?
    );
    Ok(())
}

async fn wait_for_idle(
    hub: &MountpointHub,
    limit: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + limit;
    loop {
        let metrics = hub.metrics();
        if metrics.active_sources == 0 && metrics.subscribers == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "connections did not drain: {} sources, {} subscribers",
                metrics.active_sources, metrics.subscribers
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn monitor_process_usage(
    mut stop: watch::Receiver<bool>,
    max_rss: Arc<AtomicU64>,
    max_fds: Arc<AtomicU64>,
) {
    loop {
        if let Some((rss, fds)) = process_usage() {
            max_rss.fetch_max(rss, Ordering::Relaxed);
            max_fds.fetch_max(fds, Ordering::Relaxed);
        }
        tokio::select! {
            _ = stop.changed() => return,
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

async fn source_loop(mut stream: TcpStream, rate_kib: u64, mut stop: watch::Receiver<bool>) -> u64 {
    let chunk = vec![b'x'; usize::try_from(rate_kib * 1024 / 10).unwrap_or(1).max(1)];
    let mut sent = 0_u64;
    loop {
        tokio::select! {
            _ = stop.changed() => return sent,
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
        if stream.write_all(&chunk).await.is_err() {
            return sent;
        }
        sent = sent.saturating_add(chunk.len() as u64);
    }
}

async fn subscriber_loop(mut stream: TcpStream, mut stop: watch::Receiver<bool>) -> u64 {
    let mut buffer = vec![0_u8; 16 * 1024];
    let mut received = 0_u64;
    loop {
        tokio::select! {
            _ = stop.changed() => return received,
            read = stream.read(&mut buffer) => {
                let Ok(read) = read else {
                    return received;
                };
                if read == 0 {
                    return received;
                }
                received = received.saturating_add(read as u64);
            }
        }
    }
}

async fn join_sources(tasks: Vec<tokio::task::JoinHandle<u64>>) -> u64 {
    let mut total = 0_u64;
    for task in tasks {
        total = total.saturating_add(task.await.unwrap_or_default());
    }
    total
}

async fn join_subscribers(tasks: Vec<tokio::task::JoinHandle<u64>>) -> u64 {
    let mut total = 0_u64;
    for task in tasks {
        total = total.saturating_add(task.await.unwrap_or_default());
    }
    total
}

async fn read_head(stream: &mut TcpStream) -> Result<Vec<u8>, std::io::Error> {
    let mut head = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        if head.len() > 8192 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "response head too large",
            ));
        }
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        head.extend_from_slice(&chunk[..read]);
    }
    Ok(head)
}

async fn connect_capacity_client(
    address: std::net::SocketAddr,
    index: usize,
) -> std::io::Result<TcpStream> {
    let socket = TcpSocket::new_v4()?;
    socket.set_zero_linger()?;
    let octet = u8::try_from(index % 254 + 1).expect("loopback octet is bounded");
    socket.bind(std::net::SocketAddr::from(([127, 0, 0, octet], 0)))?;
    socket.connect(address).await
}

fn capacity_connection_limiter(
    total_connections: usize,
) -> Result<ConnectionLimiter, rcaster_service::session_limit::SessionLimitError> {
    let max = total_connections.max(1);
    ConnectionLimiter::with_accept_rate(max, u32::try_from(max).unwrap_or(u32::MAX))
}

fn expect_success(head: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
    if head.starts_with(b"ICY 200") || head.starts_with(b"HTTP/1.1 200") {
        Ok(())
    } else {
        Err(format!(
            "unexpected NTRIP response: {}",
            String::from_utf8_lossy(&head)
        )
        .into())
    }
}

async fn wait_for_stop(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

fn user() -> AuthorizedUser {
    AuthorizedUser {
        user_id: "soak-user".to_owned(),
        username: "soak".to_owned(),
        auth_version: 1,
        user_mod_revision: 0,
        mountpoint_mod_revision: 0,
        source_expectation: None,
    }
}

struct Options {
    sources: usize,
    subscribers: usize,
    duration_s: u64,
    rate_kib: u64,
}

impl Options {
    fn from_args() -> Self {
        let values = std::env::args()
            .skip(1)
            .collect::<Vec<_>>()
            .chunks(2)
            .filter_map(|pair| {
                let name = pair.first()?.strip_prefix("--")?.to_owned();
                Some((name, pair.get(1)?.clone()))
            })
            .collect::<BTreeMap<_, _>>();
        Self {
            sources: parse(&values, "sources", 1).max(1),
            subscribers: parse(&values, "subscribers", 100),
            duration_s: parse(&values, "duration-s", 30).max(1),
            rate_kib: parse(&values, "rate-kib", 100),
        }
    }
}

fn parse<T>(values: &BTreeMap<String, String>, name: &str, default: T) -> T
where
    T: std::str::FromStr + Copy,
{
    values
        .get(name)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::{capacity_connection_limiter, connect_capacity_client};
    use tokio::net::TcpListener;

    #[test]
    fn capacity_limiter_admits_all_connections_without_rate_throttle() {
        let limiter = capacity_connection_limiter(10_000).expect("capacity limiter");
        for _ in 0..10_000 {
            assert!(limiter.try_accept());
        }
    }

    #[tokio::test]
    async fn capacity_clients_use_distinct_loopback_source_addresses() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("capacity listener");
        let address = listener.local_addr().expect("listener address");
        let client = connect_capacity_client(address, 7)
            .await
            .expect("connect capacity client");
        let (_, peer) = listener.accept().await.expect("accept capacity client");
        assert_eq!(
            peer.ip(),
            "127.0.0.8"
                .parse::<std::net::IpAddr>()
                .expect("expected source IP")
        );
        assert_eq!(
            client.linger().expect("client linger"),
            Some(std::time::Duration::ZERO)
        );
        drop(client);
    }
}
