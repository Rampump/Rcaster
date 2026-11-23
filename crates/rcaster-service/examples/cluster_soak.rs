use std::{
    collections::BTreeMap,
    fs,
    net::IpAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use rcaster_service::health::process_usage;
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName, pem::PemObject},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::watch,
    time::timeout,
};
use tokio_rustls::{TlsConnector, client::TlsStream};

const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options::from_args()?;
    let tls = tls_config(&options.ca_certificate)?;
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

    let authorization = format!(
        "Basic {}",
        encode_base64(format!("{}:{}", options.username, options.password).as_bytes())
    );
    let mut source_streams = Vec::with_capacity(options.sources);
    for index in 0..options.sources {
        let endpoint = &options.endpoints[index % options.endpoints.len()];
        let mountpoint = mountpoint(
            &options.mount_prefix,
            options.mount_start.saturating_add(index),
        );
        let mut stream = connect(endpoint, &tls).await?;
        stream
            .write_all(
                format!(
                    "POST /{mountpoint} HTTP/1.1\r\nHost: {endpoint}\r\nNtrip-Version: Ntrip/2.0\r\nAuthorization: {authorization}\r\nContent-Type: gnss/data\r\nUser-Agent: RCaster-cluster-soak\r\n\r\n"
                )
                .as_bytes(),
            )
            .await?;
        let head = timeout(Duration::from_secs(10), read_head(&mut stream))
            .await
            .map_err(|_| format!("source {mountpoint} response timeout"))??;
        expect_success(head).map_err(|error| format!("source {mountpoint}: {error}"))?;
        stream
            .write_all(&rtcm3_chunk(1024))
            .await
            .map_err(|error| format!("source {mountpoint} admission frame: {error}"))?;
        source_streams.push(stream);
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut subscriber_streams = Vec::with_capacity(options.subscribers);
    for index in 0..options.subscribers {
        let mountpoint = mountpoint(
            &options.mount_prefix,
            options.mount_start.saturating_add(index % options.sources),
        );
        let endpoint_index = (index + 1) % options.endpoints.len();
        let endpoint = &options.endpoints[endpoint_index];
        let mut last_error = String::new();
        let mut connected = None;
        for _ in 0..20 {
            let mut stream = match connect(endpoint, &tls).await {
                Ok(stream) => stream,
                Err(error) => {
                    last_error = error.to_string();
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };
            stream
                .write_all(
                    format!(
                        "GET /{mountpoint} HTTP/1.1\r\nHost: {endpoint}\r\nNtrip-Version: Ntrip/2.0\r\nAuthorization: {authorization}\r\nUser-Agent: RCaster-cluster-soak\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await?;
            let head = timeout(Duration::from_secs(10), read_head(&mut stream))
                .await
                .map_err(|_| format!("subscriber {index} response timeout"))??;
            match expect_success(head.clone()) {
                Ok(()) => {
                    connected = Some(stream);
                    break;
                }
                Err(error) => last_error = format!("{error}: {}", String::from_utf8_lossy(&head)),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        subscriber_streams.push(connected.ok_or_else(|| {
            format!("subscriber {index} to {endpoint}/{mountpoint} failed: {last_error}")
        })?);
    }

    let (stop_tx, stop_rx) = watch::channel(false);
    let sources = source_streams
        .into_iter()
        .map(|stream| {
            let stop = stop_rx.clone();
            tokio::spawn(async move { source_loop(stream, options.rate_kib, stop).await })
        })
        .collect::<Vec<_>>();
    let subscribers = subscriber_streams
        .into_iter()
        .map(|stream| {
            let stop = stop_rx.clone();
            tokio::spawn(async move { subscriber_loop(stream, stop).await })
        })
        .collect::<Vec<_>>();

    let started = Instant::now();
    tokio::time::sleep(Duration::from_secs(options.duration_s)).await;
    let elapsed = started.elapsed().as_secs_f64();
    stop_tx.send_replace(true);
    let source_bytes = join_tasks(sources).await?;
    let subscriber_bytes = join_tasks(subscribers).await?;
    usage_stop_tx.send_replace(true);
    usage_monitor.await?;
    let process_end = process_usage();

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "mode": "remote_cluster",
            "duration_s": elapsed,
            "endpoints": options.endpoints,
            "sources": options.sources,
            "subscribers": options.subscribers,
            "rate_kib_per_source": options.rate_kib,
            "source_bytes": source_bytes,
            "subscriber_bytes": subscriber_bytes,
            "subscriber_mib_per_second": subscriber_bytes as f64 / elapsed / 1_048_576.0,
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

async fn source_loop<S>(
    mut stream: S,
    rate_kib: u64,
    mut stop: watch::Receiver<bool>,
) -> Result<u64, String>
where
    S: AsyncWrite + Unpin,
{
    let chunk = rtcm3_chunk(
        usize::try_from(rate_kib.saturating_mul(1024) / 10)
            .unwrap_or(1024)
            .max(rtcm3_1074_frame().len()),
    );
    let mut sent = 0_u64;
    loop {
        tokio::select! {
            _ = stop.changed() => return Ok(sent),
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
        stream
            .write_all(&chunk)
            .await
            .map_err(|error| format!("source write failed: {error}"))?;
        sent = sent.saturating_add(chunk.len() as u64);
    }
}

async fn subscriber_loop<S>(mut stream: S, mut stop: watch::Receiver<bool>) -> Result<u64, String>
where
    S: AsyncRead + Unpin,
{
    let mut buffer = vec![0_u8; 16 * 1024];
    let mut received = 0_u64;
    loop {
        tokio::select! {
            _ = stop.changed() => return Ok(received),
            read = stream.read(&mut buffer) => {
                let read = read.map_err(|error| format!("subscriber read failed: {error}"))?;
                if read == 0 {
                    tokio::select! {
                        _ = stop.changed() => return Ok(received),
                        _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                    }
                    if *stop.borrow() {
                        return Ok(received);
                    }
                    return Err("subscriber closed before stop".to_owned());
                }
                received = received.saturating_add(read as u64);
            }
        }
    }
}

async fn join_tasks(
    tasks: Vec<tokio::task::JoinHandle<Result<u64, String>>>,
) -> Result<u64, String> {
    let mut total = 0_u64;
    for task in tasks {
        total = total.saturating_add(
            task.await
                .map_err(|error| format!("task join failed: {error}"))??,
        );
    }
    Ok(total)
}

async fn read_head<S>(stream: &mut S) -> Result<Vec<u8>, std::io::Error>
where
    S: AsyncRead + Unpin,
{
    let mut head = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        if head.len() > 8192 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "response head too large",
            ));
        }
        let read = match stream.read(&mut chunk).await {
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof && !head.is_empty() => {
                break;
            }
            Err(error) => return Err(error),
        };
        if read == 0 {
            break;
        }
        head.extend_from_slice(&chunk[..read]);
    }
    Ok(head)
}

fn tls_config(ca_certificate: &PathBuf) -> Result<Arc<ClientConfig>, Box<dyn std::error::Error>> {
    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(&fs::read(ca_certificate)?) {
        roots.add(certificate?)?;
    }
    Ok(Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

async fn connect(endpoint: &str, tls: &Arc<ClientConfig>) -> Result<TlsStream<TcpStream>, String> {
    let stream = timeout(Duration::from_secs(10), TcpStream::connect(endpoint))
        .await
        .map_err(|_| format!("connect timeout: {endpoint}"))?
        .map_err(|error| format!("connect {endpoint}: {error}"))?;
    let host = endpoint
        .rsplit_once(':')
        .map(|(host, _)| host.trim_matches(['[', ']']))
        .ok_or_else(|| format!("endpoint is missing a port: {endpoint}"))?;
    let server_name = match host.parse::<IpAddr>() {
        Ok(ip) => ServerName::IpAddress(ip.into()),
        Err(_) => ServerName::try_from(host.to_owned())
            .map_err(|error| format!("invalid TLS server name {host}: {error}"))?,
    };
    timeout(
        Duration::from_secs(10),
        TlsConnector::from(Arc::clone(tls)).connect(server_name, stream),
    )
    .await
    .map_err(|_| format!("TLS handshake timeout: {endpoint}"))?
    .map_err(|error| format!("TLS handshake {endpoint}: {error}"))
}

fn expect_success(head: Vec<u8>) -> Result<(), String> {
    if head.starts_with(b"ICY 200") || head.starts_with(b"HTTP/1.1 200") {
        Ok(())
    } else {
        Err(format!(
            "unexpected NTRIP response: {}",
            String::from_utf8_lossy(&head)
        ))
    }
}

fn rtcm3_chunk(minimum_bytes: usize) -> Vec<u8> {
    let frame = rtcm3_1074_frame();
    let mut chunk = Vec::with_capacity(minimum_bytes.max(frame.len()));
    while chunk.len() < minimum_bytes {
        chunk.extend_from_slice(&frame);
    }
    chunk
}

fn rtcm3_1074_frame() -> Vec<u8> {
    let mut payload = vec![0_u8; 32];
    write_bits(&mut payload, 0, 12, 1074);
    write_bits(&mut payload, 12, 12, 1);
    write_bits(&mut payload, 24, 30, 123_456);
    write_bits(&mut payload, 73, 64, 1_u64 << 63);
    write_bits(&mut payload, 137, 32, 1_u64 << 31);
    write_bits(&mut payload, 169, 1, 1);
    write_bits(&mut payload, 230, 6, 40);
    synthetic_rtcm3_payload(&payload)
}

fn synthetic_rtcm3_payload(payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![0xD3, (payload.len() >> 8) as u8, payload.len() as u8];
    frame.extend_from_slice(payload);
    let crc = crc24q(&frame);
    frame.extend_from_slice(&crc.to_be_bytes()[1..]);
    frame
}

fn write_bits(data: &mut [u8], start: usize, length: usize, value: u64) {
    for offset in 0..length {
        let bit = (value >> (length - 1 - offset)) & 1;
        let target = start + offset;
        data[target / 8] |= (bit as u8) << (7 - target % 8);
    }
}

fn crc24q(data: &[u8]) -> u32 {
    let mut crc = 0_u32;
    for byte in data {
        crc ^= u32::from(*byte) << 16;
        for _ in 0..8 {
            crc <<= 1;
            if crc & 0x100_0000 != 0 {
                crc ^= 0x1864CFB;
            }
        }
    }
    crc & 0xFF_FFFF
}

fn mountpoint(prefix: &str, index: usize) -> String {
    format!("{prefix}{index:03}")
}

fn encode_base64(input: &[u8]) -> String {
    let mut output = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let value = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        output.push(BASE64[((value >> 18) & 0x3F) as usize] as char);
        output.push(BASE64[((value >> 12) & 0x3F) as usize] as char);
        output.push(if chunk.len() > 1 {
            BASE64[((value >> 6) & 0x3F) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            BASE64[(value & 0x3F) as usize] as char
        } else {
            '='
        });
    }
    output
}

struct Options {
    endpoints: Vec<String>,
    sources: usize,
    subscribers: usize,
    duration_s: u64,
    rate_kib: u64,
    mount_prefix: String,
    mount_start: usize,
    username: String,
    password: String,
    ca_certificate: PathBuf,
}

impl Options {
    fn from_args() -> Result<Self, String> {
        let values = std::env::args()
            .skip(1)
            .collect::<Vec<_>>()
            .chunks(2)
            .filter_map(|pair| {
                let name = pair.first()?.strip_prefix("--")?.to_owned();
                Some((name, pair.get(1)?.clone()))
            })
            .collect::<BTreeMap<_, _>>();
        let endpoints = values
            .get("endpoints")
            .ok_or("missing --endpoints host:port[,host:port...]")?
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if endpoints.is_empty() {
            return Err("--endpoints must contain at least one address".to_owned());
        }
        Ok(Self {
            endpoints,
            sources: parse(&values, "sources", 1).max(1),
            subscribers: parse(&values, "subscribers", 100),
            duration_s: parse(&values, "duration-s", 30).max(1),
            rate_kib: parse(&values, "rate-kib", 1).max(1),
            mount_prefix: values
                .get("mount-prefix")
                .cloned()
                .unwrap_or_else(|| "SOAK".to_owned()),
            mount_start: parse(&values, "mount-start", 0),
            username: values
                .get("username")
                .cloned()
                .ok_or("missing --username")?,
            password: values
                .get("password")
                .cloned()
                .ok_or("missing --password")?,
            ca_certificate: values
                .get("ca-certificate")
                .cloned()
                .map(PathBuf::from)
                .ok_or("missing --ca-certificate")?,
        })
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
    use super::{encode_base64, rtcm3_1074_frame, rtcm3_chunk};

    #[test]
    fn encodes_basic_credentials() {
        assert_eq!(encode_base64(b"test:test"), "dGVzdDp0ZXN0");
    }

    #[test]
    fn chunk_contains_complete_rtcm3_frames() {
        let chunk = rtcm3_chunk(1024);
        let frame_len = rtcm3_1074_frame().len();
        assert!(chunk.len() >= 1024);
        assert_eq!(chunk.len() % frame_len, 0);
        assert_eq!(&chunk[..3], &[0xD3, 0x00, 0x20]);
    }
}
