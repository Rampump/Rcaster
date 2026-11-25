use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::{collections::BTreeMap, future::Future, io, io::Write as _, sync::Arc, time::Duration};

use brotli::CompressorWriter;
use flate2::{Compression, write::GzEncoder};
use futures_util::{SinkExt, StreamExt};
use rcaster_core::str_record::StrRecord;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        handshake::derive_accept_key,
        protocol::{Role, WebSocketConfig},
    },
};

use crate::{
    account_directory::AccountDirectory,
    admin_auth::LoginLimiter,
    audit::{AuditEvent, AuditLog},
    auth::AuthConcurrencyLimiter,
    cluster_runtime::SharedClusterRuntime,
    connections::ConnectionController,
    health::HealthState,
    hub::MountpointHub,
    proxy_protocol::{ProxyProtocolConfig, resolve_client_ip},
    public_auth::{
        self, PUBLIC_SESSION_COOKIE, PUBLIC_SESSION_TTL_S, PublicAuthError, PublicIdentity,
    },
    relay::RelayManager,
    tls_reload::ReloadableTlsConfig,
    web_security::websocket_origin_allowed,
};

const MONITOR_TICKET_COOKIE: &str = "rcaster_monitor_ticket";

#[derive(Clone)]
pub struct PublicState {
    pub hub: Arc<MountpointHub>,
    pub runtime: Option<SharedClusterRuntime>,
    pub accounts: AccountDirectory,
    pub relays: Option<Arc<RelayManager>>,
    pub audit: AuditLog,
    pub websockets: PublicMonitorLimiter,
    pub login_limiter: LoginLimiter,
    pub monitor_starts: MonitorStartLimiter,
    pub map_tiles: Option<PublicMapTiles>,
    pub proxy_protocol: Option<ProxyProtocolConfig>,
    pub connections: Arc<ConnectionController>,
    pub auth_limiter: AuthConcurrencyLimiter,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PublicMapTiles {
    pub url_template: String,
    pub attribution: String,
    pub min_zoom: u8,
    pub max_zoom: u8,
    pub coordinate_system: PublicMapCoordinateSystem,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicMapCoordinateSystem {
    #[default]
    Wgs84,
    Gcj02,
}

#[derive(Clone, Default)]
pub struct MonitorStartLimiter {
    inner: Arc<Mutex<BTreeMap<String, MonitorStartState>>>,
    metrics: Option<Arc<HealthState>>,
}

#[derive(Clone, Copy)]
struct MonitorStartState {
    started: Instant,
    count: u8,
}

impl MonitorStartLimiter {
    pub fn track_metrics(&mut self, metrics: Arc<HealthState>) {
        self.metrics = Some(metrics);
    }

    fn allow(&self, key: &str) -> bool {
        let now = Instant::now();
        let mut states = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = states.entry(key.to_owned()).or_insert(MonitorStartState {
            started: now,
            count: 0,
        });
        if now.duration_since(state.started) >= Duration::from_secs(60) {
            state.started = now;
            state.count = 0;
        }
        if state.count >= 30 {
            if let Some(metrics) = &self.metrics {
                metrics.monitor_start_limited();
            }
            return false;
        }
        state.count += 1;
        true
    }
}

#[derive(Clone, Default)]
pub struct PublicMonitorLimiter {
    inner: Arc<PublicMonitorInner>,
    metrics: Option<Arc<HealthState>>,
}

#[derive(Default)]
struct PublicMonitorInner {
    total: AtomicUsize,
    users: Mutex<BTreeMap<String, usize>>,
    ips: Mutex<BTreeMap<String, usize>>,
    mounts: Mutex<BTreeMap<String, usize>>,
}

pub struct PublicMonitorSlot {
    limiter: PublicMonitorLimiter,
    user: String,
    ip: String,
    mountpoint: String,
}

impl PublicMonitorLimiter {
    pub fn track_metrics(&mut self, metrics: Arc<HealthState>) {
        self.metrics = Some(metrics);
    }

    pub fn acquire(&self, user: &str, ip: &str, mountpoint: &str) -> Option<PublicMonitorSlot> {
        let mut users = self
            .inner
            .users
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut ips = self
            .inner
            .ips
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut mounts = self
            .inner
            .mounts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.inner.total.load(Ordering::Acquire) >= 2000
            || users.get(user).copied().unwrap_or_default() >= 2
            || ips.get(ip).copied().unwrap_or_default() >= 4
            || mounts.get(mountpoint).copied().unwrap_or_default() >= 200
        {
            if let Some(metrics) = &self.metrics {
                metrics.monitor_busy();
            }
            return None;
        }
        self.inner.total.fetch_add(1, Ordering::AcqRel);
        if let Some(metrics) = &self.metrics {
            metrics.set_public_monitor_viewers(self.inner.total.load(Ordering::Acquire));
        }
        *users.entry(user.to_owned()).or_default() += 1;
        *ips.entry(ip.to_owned()).or_default() += 1;
        *mounts.entry(mountpoint.to_owned()).or_default() += 1;
        Some(PublicMonitorSlot {
            limiter: self.clone(),
            user: user.to_owned(),
            ip: ip.to_owned(),
            mountpoint: mountpoint.to_owned(),
        })
    }
}

impl Drop for PublicMonitorSlot {
    fn drop(&mut self) {
        self.limiter.inner.total.fetch_sub(1, Ordering::AcqRel);
        if let Some(metrics) = &self.limiter.metrics {
            metrics.set_public_monitor_viewers(self.limiter.inner.total.load(Ordering::Acquire));
        }
        let mut users = self
            .limiter
            .inner
            .users
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = users.get_mut(&self.user) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                users.remove(&self.user);
            }
        }
        let mut ips = self
            .limiter
            .inner
            .ips
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = ips.get_mut(&self.ip) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                ips.remove(&self.ip);
            }
        }
        let mut mounts = self
            .limiter
            .inner
            .mounts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = mounts.get_mut(&self.mountpoint) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                mounts.remove(&self.mountpoint);
            }
        }
    }
}

#[derive(Clone, Serialize)]
struct PublicRecord {
    #[serde(flatten)]
    record: StrRecord,
    region: Option<String>,
    coverage_radius_m: Option<f64>,
    source_generation: Option<u64>,
}

pub async fn serve_public<F>(
    listener: TcpListener,
    state: PublicState,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let mut shutdown = Box::pin(shutdown);
    loop {
        let accepted = tokio::select! {
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => accepted?,
        };
        let state = state.clone();
        tokio::spawn(async move {
            let peer_ip = accepted.1.ip().to_string();
            let mut stream = accepted.0;
            let _ = timeout(
                Duration::from_secs(5),
                handle(&mut stream, &state, &peer_ip),
            )
            .await;
        });
    }
}

pub async fn serve_public_tls<F>(
    listener: TcpListener,
    certificate_pem: String,
    private_key_pem: String,
    state: PublicState,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let tls = ReloadableTlsConfig::from_pem(&certificate_pem, &private_key_pem)?;
    serve_public_tls_reloadable(listener, tls, state, shutdown).await
}

pub async fn serve_public_tls_reloadable<F>(
    listener: TcpListener,
    tls: ReloadableTlsConfig,
    state: PublicState,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let mut shutdown = Box::pin(shutdown);
    loop {
        let accepted = tokio::select! {
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => accepted?,
        };
        let acceptor = tls.current();
        let state = state.clone();
        tokio::spawn(async move {
            let (mut stream, peer) = accepted;
            let Ok(peer_ip) =
                resolve_client_ip(&mut stream, peer.ip(), state.proxy_protocol.as_ref()).await
            else {
                return;
            };
            let acceptor = TlsAcceptor::from(acceptor);
            if let Ok(Ok(mut stream)) =
                timeout(Duration::from_secs(5), acceptor.accept(stream)).await
            {
                let _ = timeout(
                    Duration::from_secs(5),
                    handle(&mut stream, &state, &peer_ip.to_string()),
                )
                .await;
            }
        });
    }
}

async fn handle<S>(stream: &mut S, state: &PublicState, peer_ip: &str) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = match read_public_request(stream).await {
        Ok(request) => request,
        Err(status) => {
            return stream
                .write_all(&response(
                    status,
                    json!({"code":"BAD_REQUEST","message":"invalid or oversized public request"}),
                ))
                .await;
        }
    };
    let request = String::from_utf8_lossy(&request);
    let (head, request_body) = request.split_once("\r\n\r\n").unwrap_or((&request, ""));
    let Some(first) = head.lines().next() else {
        return stream
            .write_all(&response(400, json!({"code":"BAD_REQUEST"})))
            .await;
    };
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();
    let headers = head.lines().skip(1).collect::<Vec<_>>();
    let (path, query) = path.split_once('?').unwrap_or((path, ""));
    let query = parse_query(query);
    if method == "GET" && matches!(path, "/" | "/public/") {
        return stream
            .write_all(&html_response(
                PUBLIC_HTML,
                state.map_tiles.as_ref().and_then(tile_origin),
            ))
            .await;
    }
    if method == "POST" && path == "/api/v1/auth/login" {
        let Ok(body) = serde_json::from_str::<Value>(request_body) else {
            return stream
                .write_all(&response(400, json!({"code":"BAD_REQUEST"})))
                .await;
        };
        let (Some(username), Some(password)) = (
            body.get("username").and_then(Value::as_str),
            body.get("password").and_then(Value::as_str),
        ) else {
            return stream
                .write_all(&response(400, json!({"code":"BAD_REQUEST"})))
                .await;
        };
        let ip_key = format!("ip:{peer_ip}");
        let user_key = format!("user:{username}");
        let mut retry_after = state
            .login_limiter
            .retry_after_cluster(state.runtime.as_ref(), &ip_key)
            .await;
        if retry_after.is_none() {
            retry_after = state
                .login_limiter
                .retry_after_cluster(state.runtime.as_ref(), &user_key)
                .await;
        }
        if let Some(retry_after) = retry_after {
            audit_public_login(state, peer_ip, username, "rate_limited");
            return stream
                .write_all(&response_with_headers(
                    429,
                    json!({"code":"RATE_LIMITED"}),
                    &[("Retry-After", retry_after.to_string())],
                ))
                .await;
        }
        let issued = match public_auth::login(
            &state.accounts,
            &state.auth_limiter,
            username,
            password,
        )
        .await
        {
            Ok(issued) => {
                state
                    .login_limiter
                    .success_cluster(state.runtime.as_ref(), &ip_key)
                    .await;
                state
                    .login_limiter
                    .success_cluster(state.runtime.as_ref(), &user_key)
                    .await;
                audit_public_login(state, peer_ip, username, "success");
                issued
            }
            Err(PublicAuthError::Invalid) => {
                let mut retry_after = state
                    .login_limiter
                    .failure_cluster(state.runtime.as_ref(), &user_key)
                    .await;
                if retry_after.is_none() {
                    retry_after = state
                        .login_limiter
                        .failure_cluster(state.runtime.as_ref(), &ip_key)
                        .await;
                }
                if let Some(retry_after) = retry_after {
                    audit_public_login(state, peer_ip, username, "locked");
                    return stream
                        .write_all(&response_with_headers(
                            429,
                            json!({"code":"RATE_LIMITED"}),
                            &[("Retry-After", retry_after.to_string())],
                        ))
                        .await;
                }
                audit_public_login(state, peer_ip, username, "failure");
                return stream
                    .write_all(&response(401, json!({"code":"UNAUTHENTICATED"})))
                    .await;
            }
            Err(_) => {
                audit_public_login(state, peer_ip, username, "unavailable");
                return stream
                    .write_all(&response(503, json!({"code":"UNAVAILABLE"})))
                    .await;
            }
        };
        return stream
            .write_all(&response_with_headers(
                200,
                json!({
                    "user_id": issued.identity.user_id,
                    "username": issued.identity.username,
                    "csrf_token": issued.identity.csrf_token,
                    "expires_at": issued.expires_at,
                }),
                &[(
                    "Set-Cookie",
                    format!(
                        "{PUBLIC_SESSION_COOKIE}={}; Path=/api/v1; HttpOnly; Secure; SameSite=Strict; Max-Age={PUBLIC_SESSION_TTL_S}",
                        issued.cookie_value
                    ),
                )],
            ))
            .await;
    }
    let cookie = header(&headers, "cookie").and_then(|value| {
        value.split(';').find_map(|part| {
            let (name, value) = part.trim().split_once('=')?;
            (name == PUBLIC_SESSION_COOKIE).then_some(value)
        })
    });
    let identity = match cookie {
        Some(cookie) => public_auth::authenticate(&state.accounts, cookie)
            .await
            .map(Some),
        None => Ok(None),
    };
    if method == "POST" && path == "/api/v1/auth/logout" {
        let Ok(Some(identity)) = identity else {
            return stream
                .write_all(&response(401, json!({"code":"UNAUTHENTICATED"})))
                .await;
        };
        if !csrf_valid(&headers, &identity.csrf_token) {
            return stream
                .write_all(&response(403, json!({"code":"CSRF_INVALID"})))
                .await;
        }
        let _ = public_auth::logout(&state.accounts, &identity).await;
        return stream
            .write_all(&response_with_headers(
                204,
                Value::Null,
                &[(
                    "Set-Cookie",
                    format!(
                        "{PUBLIC_SESSION_COOKIE}=; Path=/api/v1; HttpOnly; Secure; SameSite=Strict; Max-Age=0"
                    ),
                )],
            ))
            .await;
    }
    if method == "GET" && path == "/api/v1/auth/session" {
        return match identity {
            Ok(Some(identity)) => {
                stream
                    .write_all(&response(
                        200,
                        json!({
                            "authenticated": true,
                            "user_id": identity.user_id,
                            "username": identity.username,
                        }),
                    ))
                    .await
            }
            _ => {
                stream
                    .write_all(&response(401, json!({"code":"UNAUTHENTICATED"})))
                    .await
            }
        };
    }
    if method == "GET" && path == "/api/v1/auth/csrf" {
        return match identity {
            Ok(Some(identity)) => {
                stream
                    .write_all(&response(200, json!({"csrf_token": identity.csrf_token})))
                    .await
            }
            _ => {
                stream
                    .write_all(&response(401, json!({"code":"UNAUTHENTICATED"})))
                    .await
            }
        };
    }
    if method == "POST" && path.starts_with("/api/v1/monitors/") && path.ends_with("/ticket") {
        let mountpoint = &path["/api/v1/monitors/".len()..path.len() - "/ticket".len()];
        let Ok(Some(identity)) = identity else {
            return stream
                .write_all(&response(401, json!({"code":"UNAUTHENTICATED"})))
                .await;
        };
        if !csrf_valid(&headers, &identity.csrf_token) {
            return stream
                .write_all(&response(403, json!({"code":"CSRF_INVALID"})))
                .await;
        }
        if !state.monitor_starts.allow(&identity.user_id) {
            return stream
                .write_all(&response_with_headers(
                    429,
                    json!({"code":"MONITOR_RATE_LIMITED"}),
                    &[("Retry-After", "60".to_owned())],
                ))
                .await;
        }
        let records = public_records(state).await;
        let Some(record) = records.get(mountpoint) else {
            return stream
                .write_all(&response(404, json!({"code":"NOT_FOUND"})))
                .await;
        };
        return match public_auth::issue_ticket(
            &state.accounts,
            &identity,
            mountpoint,
            peer_ip,
            record.source_generation,
        )
        .await
        {
            Ok(ticket) => {
                let cookie = format!(
                    "{MONITOR_TICKET_COOKIE}={ticket}; Path=/api/v1/monitors/{mountpoint}/ws; HttpOnly; Secure; SameSite=Strict; Max-Age=60"
                );
                stream
                    .write_all(&response_with_headers(
                        201,
                        json!({"expires_in": 60}),
                        &[("Set-Cookie", cookie)],
                    ))
                    .await
            }
            Err(_) => {
                stream
                    .write_all(&response(503, json!({"code":"UNAVAILABLE"})))
                    .await
            }
        };
    }
    if method == "GET" && path.starts_with("/api/v1/monitors/") && path.ends_with("/ws") {
        if !websocket_origin_allowed(&headers) {
            return stream
                .write_all(&response(
                    403,
                    json!({"code":"FORBIDDEN","message":"WebSocket origin mismatch"}),
                ))
                .await;
        }
        let mountpoint = &path["/api/v1/monitors/".len()..path.len() - "/ws".len()];
        let ticket = header(&headers, "cookie").and_then(|value| {
            value.split(';').find_map(|part| {
                let (name, value) = part.trim().split_once('=')?;
                (name == MONITOR_TICKET_COOKIE).then_some(value)
            })
        });
        let Some(ticket) = ticket else {
            return stream
                .write_all(&response(401, json!({"code":"TICKET_REQUIRED"})))
                .await;
        };
        let (identity, source_generation) =
            match public_auth::consume_ticket(&state.accounts, ticket, mountpoint, peer_ip).await {
                Ok(value) => value,
                Err(_) => {
                    return stream
                        .write_all(&response(401, json!({"code":"INVALID_TICKET"})))
                        .await;
                }
            };
        return serve_public_websocket(
            stream,
            state,
            &headers,
            mountpoint,
            identity,
            peer_ip,
            source_generation,
        )
        .await;
    }
    if method != "GET" {
        return stream
            .write_all(&response(405, json!({"code":"METHOD_NOT_ALLOWED"})))
            .await;
    }
    let records = public_records(state).await;
    if let Some(mountpoint) = path
        .strip_prefix("/api/v1/public/streams/")
        .and_then(|path| path.strip_suffix("/history"))
    {
        if !records.contains_key(mountpoint) {
            return stream
                .write_all(&response(404, json!({"code":"NOT_FOUND"})))
                .await;
        }
        let limit = query
            .get("limit")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(60)
            .clamp(1, 300);
        return stream
            .write_all(
                &public_cached_json(
                    &headers,
                    json!({
                        "mountpoint": mountpoint,
                        "points": state.hub.source_history(mountpoint, limit),
                    }),
                )
                .await,
            )
            .await;
    }
    let bbox = if path == "/api/v1/public/map/stations" {
        match query
            .get("bbox")
            .map(|value| parse_map_bbox(value))
            .transpose()
        {
            Ok(bbox) => bbox,
            Err(message) => {
                return stream
                    .write_all(&response(
                        400,
                        json!({"code":"BAD_REQUEST","message":message}),
                    ))
                    .await;
            }
        }
    } else {
        None
    };
    let body = match path {
        "/api/v1/public/str" => json!({
            "mountpoints": records.values().collect::<Vec<_>>(),
        }),
        "/api/v1/mountpoints" => mountpoints(&records, &query),
        "/api/v1/public/map/stations" => map_stations(&records, bbox),
        "/api/v1/public/map/config" => json!({
            "tiles": state.map_tiles.as_ref(),
        }),
        _ => {
            return stream
                .write_all(&response(
                    404,
                    json!({"code":"NOT_FOUND","message":"unknown public route"}),
                ))
                .await;
        }
    };
    stream
        .write_all(&public_cached_json(&headers, body).await)
        .await
}

fn audit_public_login(state: &PublicState, peer_ip: &str, username: &str, result: &str) {
    state.audit.record(AuditEvent {
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        request_id: format!("public-{}", rand::random::<u64>()),
        actor: username.to_owned(),
        source_ip: Some(peer_ip.to_owned()),
        action: "auth.public_login".to_owned(),
        target: "public monitor session".to_owned(),
        result: result.to_owned(),
        details: json!({}),
    });
}

const PUBLIC_MAX_HEAD_BYTES: usize = 16 * 1024;
const PUBLIC_MAX_BODY_BYTES: usize = 64 * 1024;

async fn read_public_request<S>(stream: &mut S) -> Result<Vec<u8>, u16>
where
    S: AsyncRead + Unpin,
{
    timeout(Duration::from_secs(5), async {
        let mut request = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            let head_end = find_bytes(&request, b"\r\n\r\n");
            let total_length = if let Some(head_end) = head_end {
                if head_end > PUBLIC_MAX_HEAD_BYTES {
                    return Err(431);
                }
                let Ok(head) = std::str::from_utf8(&request[..head_end]) else {
                    return Err(400);
                };
                let mut content_length = 0;
                for line in head.lines().skip(1) {
                    let Some((name, value)) = line.split_once(':') else {
                        continue;
                    };
                    if name.eq_ignore_ascii_case("content-length") {
                        let Ok(length) = value.trim().parse::<usize>() else {
                            return Err(400);
                        };
                        content_length = content_length.max(length);
                    }
                }
                if content_length > PUBLIC_MAX_BODY_BYTES {
                    return Err(413);
                }
                head_end + 4 + content_length
            } else {
                PUBLIC_MAX_HEAD_BYTES
            };
            if request.len() >= total_length {
                request.truncate(total_length);
                return Ok(request);
            }
            let read = stream.read(&mut chunk).await.map_err(|_| 400_u16)?;
            if read == 0 {
                return Err(400);
            }
            request.extend_from_slice(&chunk[..read]);
            if request.len() > PUBLIC_MAX_HEAD_BYTES + PUBLIC_MAX_BODY_BYTES {
                return Err(413);
            }
        }
    })
    .await
    .map_err(|_| 408_u16)?
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

async fn serve_public_websocket<S>(
    stream: &mut S,
    state: &PublicState,
    headers: &[&str],
    mountpoint: &str,
    identity: PublicIdentity,
    peer_ip: &str,
    source_generation: Option<u64>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if state.hub.source_profile(mountpoint).is_none()
        && let Some(relays) = &state.relays
        && relays.ensure(mountpoint).await.is_err()
    {
        return stream
            .write_all(&response(503, json!({"code":"UNAVAILABLE"})))
            .await;
    }
    let Some(_slot) = state
        .websockets
        .acquire(&identity.username, peer_ip, mountpoint)
    else {
        return stream
            .write_all(&response(
                429,
                json!({"code":"MONITOR_BUSY","message":"monitor limit reached"}),
            ))
            .await;
    };
    let Some(key) = header(headers, "sec-websocket-key") else {
        return stream
            .write_all(&response(400, json!({"code":"BAD_REQUEST"})))
            .await;
    };
    if header(headers, "sec-websocket-version") != Some("13") {
        return stream
            .write_all(&response(400, json!({"code":"BAD_REQUEST"})))
            .await;
    }
    let clear_ticket = format!(
        "{MONITOR_TICKET_COOKIE}=; Path=/api/v1/monitors/{mountpoint}/ws; HttpOnly; Secure; SameSite=Strict; Max-Age=0"
    );
    stream
        .write_all(
            format!(
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\nSet-Cookie: {clear_ticket}\r\n\r\n",
                derive_accept_key(key.as_bytes()),
            )
            .as_bytes(),
        )
        .await?;
    let config = WebSocketConfig::default()
        .max_message_size(Some(64 * 1024))
        .max_frame_size(Some(64 * 1024));
    let mut web_cancel = state.connections.connection();
    web_cancel.bind(format!("web-user:{}", identity.user_id));
    let mut websocket = WebSocketStream::from_raw_socket(stream, Role::Server, Some(config)).await;
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    websocket
        .send(Message::Text(
            json!({"type":"monitor_ready","schema_version":1,"mountpoint":mountpoint})
                .to_string()
                .into(),
        ))
        .await
        .map_err(io::Error::other)?;
    loop {
        tokio::select! {
            _ = web_cancel.cancelled() => {
                let _ = websocket
                    .send(Message::Text(
                        json!({"type":"monitor_closed","reason":"AUTH_CHANGED"})
                            .to_string()
                            .into(),
                    ))
                    .await;
                break;
            }
            _ = interval.tick() => {
                if !public_auth::still_authorized(&state.accounts, &identity).await {
                    let _ = websocket
                        .send(Message::Text(
                            json!({"type":"monitor_closed","reason":"AUTH_CHANGED"})
                                .to_string()
                                .into(),
                        ))
                        .await;
                    break;
                }
                let Some((current_generation, snapshot)) =
                    monitor_snapshot(state, mountpoint).await
                else {
                    let _ = websocket
                        .send(Message::Text(
                            json!({"type":"monitor_closed","reason":"SOURCE_CHANGED"})
                                .to_string()
                                .into(),
                        ))
                        .await;
                    break;
                };
                if source_generation.is_some() && current_generation != source_generation {
                    let _ = websocket
                        .send(Message::Text(
                            json!({"type":"monitor_closed","reason":"SOURCE_CHANGED"})
                                .to_string()
                                .into(),
                        ))
                        .await;
                    break;
                }
                if !matches!(
                    timeout(
                        Duration::from_secs(2),
                        websocket.send(Message::Text(
                            json!({
                                "type":"monitor_snapshot",
                                "schema_version":1,
                                "mountpoint":mountpoint,
                                "snapshot":snapshot,
                            })
                            .to_string()
                            .into(),
                        )),
                    )
                    .await,
                    Ok(Ok(()))
                ) {
                    break;
                }
            }
            message = websocket.next() => {
                match message {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {}
                }
            }
        }
    }
    Ok(())
}

async fn monitor_snapshot(state: &PublicState, mountpoint: &str) -> Option<(Option<u64>, Value)> {
    let mut records = public_records(state).await;
    let entry = records.remove(mountpoint)?;
    let source_generation = entry.source_generation;
    let health = state.hub.source_health(mountpoint);
    let profile = state.hub.source_profile(mountpoint);
    Some((
        source_generation,
        json!({
            "str": entry,
            "subscribers": state.hub.subscriber_count(mountpoint).unwrap_or_default(),
            "profile": profile.map(|profile| json!({
                "satellite_count": profile.satellite_count,
                "satellite_ids": profile.satellite_ids.iter().copied().collect::<Vec<_>>(),
                "signal_count": profile.signal_count,
                "signal_ids": profile.signal_ids.iter().copied().collect::<Vec<_>>(),
                "msm_versions": profile.msm_versions.iter().copied().collect::<Vec<_>>(),
                "satellites": profile.satellite_positions.iter().map(|satellite| json!({
                    "system": satellite.system,
                    "prn": satellite.prn,
                    "elevation_deg": satellite.elevation_deg,
                    "azimuth_deg": satellite.azimuth_deg,
                    "snr_db": satellite.snr_db,
                })).collect::<Vec<_>>(),
                "satellite_cn0_db_hz": profile.satellite_cn0_q4.iter().map(|(prn, cn0_q4)| {
                    (prn.to_string(), json!(f64::from(*cn0_q4) / 4.0))
                }).collect::<serde_json::Map<_, _>>(),
                "msm_observation_count": profile.msm_observations.len(),
                "msm_observations": profile.msm_observations.iter().map(observation_view).collect::<Vec<_>>(),
            })),
            "bitrate_bps": health.and_then(|health| health.bitrate_bps),
            "last_packet_age_seconds": health.and_then(|health| health.last_packet_age_seconds),
            "stream_health_warning": health.is_some_and(|health| health.warning_codes != 0),
            "valid_frame_ratio": health.and_then(|health| health.valid_frame_ratio),
            "crc_error_ratio": health.and_then(|health| health.crc_error_ratio),
        }),
    ))
}

fn observation_view(observation: &rcaster_core::protocol::GnssObservation) -> Value {
    json!({
        "system": observation.system.as_str(),
        "message_number": observation.message_number,
        "station_id": observation.station_id,
        "epoch_time_ms": observation.epoch_time_ms,
        "satellite_id": observation.satellite_id,
        "signal_id": observation.signal_id,
        "pseudorange_m": observation.pseudorange_mm.map(|value| value as f64 / 1000.0),
        "carrier_phase_m": observation.carrier_phase_mm.map(|value| value as f64 / 1000.0),
        "doppler_m_s": observation.doppler_mm_s.map(|value| value as f64 / 1000.0),
        "cn0_db_hz": observation.cn0_q4.map(|value| f64::from(value) / 4.0),
        "lock_time_raw": observation.lock_time_raw,
        "half_cycle_ambiguity": observation.half_cycle_ambiguity,
        "pseudorange_ambiguity_raw": observation.pseudorange_ambiguity_raw,
        "glonass_frequency_channel": observation.glonass_frequency_channel,
    })
}

async fn public_records(state: &PublicState) -> BTreeMap<String, PublicRecord> {
    let accounts = state.accounts.snapshot();
    let mut records = state
        .hub
        .sourcetable_records()
        .into_iter()
        .map(|mut record| {
            let configured = accounts.mountpoint(&record.mountpoint);
            let region = configured.and_then(|mountpoint| mountpoint.region.clone());
            let coverage_radius_m = configured.and_then(|mountpoint| mountpoint.coverage_radius_m);
            apply_configured_position(&mut record, configured);
            (
                record.mountpoint.clone(),
                PublicRecord {
                    record,
                    region,
                    coverage_radius_m,
                    source_generation: None,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    if let Some(runtime) = &state.runtime {
        for mut record in runtime.sourcetable_records().await {
            let source_generation = runtime
                .source_route(&record.mountpoint)
                .await
                .map(|route| route.source_generation);
            let configured = accounts.mountpoint(&record.mountpoint);
            let region = configured.and_then(|mountpoint| mountpoint.region.clone());
            let coverage_radius_m = configured.and_then(|mountpoint| mountpoint.coverage_radius_m);
            apply_configured_position(&mut record, configured);
            records
                .entry(record.mountpoint.clone())
                .and_modify(|entry| {
                    entry.record = record.clone();
                    entry.region = region.clone();
                    entry.coverage_radius_m = coverage_radius_m;
                    entry.source_generation = source_generation;
                })
                .or_insert(PublicRecord {
                    record,
                    region,
                    coverage_radius_m,
                    source_generation,
                });
        }
    }
    records.retain(|name, _| {
        accounts
            .mountpoint(name)
            .is_some_and(|mountpoint| mountpoint.enabled && mountpoint.advertise)
    });
    records
}

fn apply_configured_position(
    record: &mut StrRecord,
    configured: Option<&rcaster_core::account::Mountpoint>,
) {
    let missing = record
        .latitude
        .parse::<f64>()
        .ok()
        .zip(record.longitude.parse::<f64>().ok())
        .is_none_or(|(latitude, longitude)| latitude == 0.0 && longitude == 0.0);
    if missing
        && let Some((latitude, longitude)) =
            configured.and_then(|mountpoint| mountpoint.latitude.zip(mountpoint.longitude))
    {
        record.latitude = format!("{latitude:.7}");
        record.longitude = format!("{longitude:.7}");
    }
}

fn mountpoints(
    records: &BTreeMap<String, PublicRecord>,
    query: &BTreeMap<String, String>,
) -> Value {
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 500);
    let cursor = query.get("cursor").map(String::as_str);
    let region = query.get("region").map(String::as_str);
    let filtered = records
        .values()
        .filter(|record| cursor.is_none_or(|cursor| record.record.mountpoint.as_str() > cursor))
        .filter(|record| region.is_none_or(|region| record.region.as_deref() == Some(region)))
        .cloned()
        .collect::<Vec<_>>();
    let next_cursor =
        (filtered.len() > limit).then(|| filtered[limit - 1].record.mountpoint.clone());
    json!({
        "mountpoints": filtered.into_iter().take(limit).collect::<Vec<_>>(),
        "next_cursor": next_cursor,
    })
}

fn map_stations(records: &BTreeMap<String, PublicRecord>, bbox: Option<MapBbox>) -> Value {
    let features = records
        .values()
        .filter_map(|entry| {
            let record = &entry.record;
            let latitude = record.latitude.parse::<f64>().ok()?;
            let longitude = record.longitude.parse::<f64>().ok()?;
            if !(-90.0..=90.0).contains(&latitude)
                || !(-180.0..=180.0).contains(&longitude)
                || latitude == 0.0 && longitude == 0.0
                || bbox.is_some_and(|(min_lon, min_lat, max_lon, max_lat)| {
                    longitude < min_lon
                        || longitude > max_lon
                        || latitude < min_lat
                        || latitude > max_lat
                })
            {
                return None;
            }
            Some(json!({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude],
                },
                "properties": {
                    "mountpoint": record.mountpoint,
                    "identifier": record.identifier,
                    "format": record.format,
                    "network": record.network,
                    "region": entry.region,
                    "coverage_m": entry.coverage_radius_m,
                },
            }))
        })
        .collect::<Vec<_>>();
    json!({"type": "FeatureCollection", "features": features})
}

type MapBbox = (f64, f64, f64, f64);

fn parse_map_bbox(value: &str) -> Result<MapBbox, &'static str> {
    let values = value
        .split(',')
        .map(str::parse::<f64>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| "bbox must contain four decimal coordinates")?;
    let [min_lon, min_lat, max_lon, max_lat] = values.as_slice() else {
        return Err("bbox must contain four decimal coordinates");
    };
    if !(-180.0..=180.0).contains(min_lon)
        || !(-180.0..=180.0).contains(max_lon)
        || !(-90.0..=90.0).contains(min_lat)
        || !(-90.0..=90.0).contains(max_lat)
        || min_lon > max_lon
        || min_lat > max_lat
    {
        return Err("bbox coordinates are invalid or reversed");
    }
    Ok((*min_lon, *min_lat, *max_lon, *max_lat))
}

fn parse_query(query: &str) -> BTreeMap<String, String> {
    query
        .split('&')
        .filter_map(|part| part.split_once('='))
        .map(|(name, value)| (percent_decode(name), percent_decode(value)))
        .collect()
}

fn percent_decode(value: &str) -> String {
    let input = value.as_bytes();
    let mut output = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        match input[index] {
            b'+' => {
                output.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < input.len() => {
                if let (Some(high), Some(low)) =
                    (hex_value(input[index + 1]), hex_value(input[index + 2]))
                {
                    output.push((high << 4) | low);
                    index += 3;
                } else {
                    output.push(input[index]);
                    index += 1;
                }
            }
            byte => {
                output.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&output).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn header<'a>(headers: &'a [&'a str], name: &str) -> Option<&'a str> {
    headers.iter().find_map(|line| {
        line.split_once(':')
            .filter(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim())
    })
}

fn csrf_valid(headers: &[&str], expected: &str) -> bool {
    header(headers, "x-csrf-token").is_some_and(|supplied| {
        supplied.len() == expected.len()
            && subtle::ConstantTimeEq::ct_eq(supplied.as_bytes(), expected.as_bytes()).into()
    })
}

fn response(status: u16, body: Value) -> Vec<u8> {
    response_with_headers(status, body, &[])
}

fn response_with_headers(status: u16, body: Value, extra: &[(&str, String)]) -> Vec<u8> {
    response_text_with_headers(status, body.to_string(), extra)
}

async fn public_cached_json(headers: &[&str], body: Value) -> Vec<u8> {
    let body = Arc::<str>::from(body.to_string());
    let digest = format!("{:x}", Sha256::digest(body.as_bytes()));
    let encoding = if body.len() >= 128 {
        if accepts_encoding(headers, "br") {
            Some("br")
        } else if accepts_encoding(headers, "gzip") {
            Some("gzip")
        } else {
            None
        }
    } else {
        None
    };
    let etag = match encoding {
        Some(encoding) => format!("\"{digest}-{encoding}\""),
        None => format!("\"{digest}\""),
    };
    let mut extra = vec![
        ("ETag", etag.clone()),
        ("Cache-Control", "public, max-age=5".to_owned()),
        ("Vary", "Accept-Encoding".to_owned()),
    ];
    let not_modified = header(headers, "if-none-match").is_some_and(|value| {
        value
            .split(',')
            .map(str::trim)
            .any(|candidate| candidate == "*" || candidate == etag)
    });
    if not_modified {
        return response_bytes_with_headers(304, Vec::new(), &extra);
    }
    if let Some(encoding) = encoding {
        let compressed_body = Arc::clone(&body);
        let compressed = tokio::task::spawn_blocking(move || match encoding {
            "br" => brotli_json(&compressed_body),
            _ => gzip_json(&compressed_body),
        })
        .await
        .ok()
        .and_then(Result::ok);
        if let Some(compressed) = compressed {
            extra.push(("Content-Encoding", encoding.to_owned()));
            return response_bytes_with_headers(200, compressed, &extra);
        }
    }
    response_bytes_with_headers(200, body.as_bytes().to_vec(), &extra)
}

fn response_text_with_headers(status: u16, body: String, extra: &[(&str, String)]) -> Vec<u8> {
    response_bytes_with_headers(status, body.into_bytes(), extra)
}

fn response_bytes_with_headers(
    status: u16,
    mut body: Vec<u8>,
    extra: &[(&str, String)],
) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        413 => "Payload Too Large",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        503 => "Service Unavailable",
        _ => "Error",
    };
    if matches!(status, 204 | 304) {
        body.clear();
    }
    let mut head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
        body.len()
    );
    if !extra
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("cache-control"))
    {
        head.push_str("Cache-Control: no-cache\r\n");
    }
    head.push_str("X-Content-Type-Options: nosniff\r\nStrict-Transport-Security: max-age=31536000\r\nReferrer-Policy: no-referrer\r\nConnection: close\r\n");
    for (name, value) in extra {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    let mut response = head.into_bytes();
    response.extend_from_slice(&body);
    response
}

fn accepts_encoding(headers: &[&str], expected: &str) -> bool {
    header(headers, "accept-encoding").is_some_and(|value| {
        value.split(',').any(|part| {
            let mut fields = part.trim().split(';');
            let encoding = fields.next().unwrap_or_default().trim();
            let enabled = fields.all(|field| {
                field
                    .trim()
                    .strip_prefix("q=")
                    .and_then(|quality| quality.parse::<f32>().ok())
                    .is_none_or(|quality| quality > 0.0)
            });
            encoding.eq_ignore_ascii_case(expected) && enabled
        })
    })
}

fn gzip_json(body: &str) -> io::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(body.as_bytes())?;
    encoder.finish()
}

fn brotli_json(body: &str) -> io::Result<Vec<u8>> {
    let mut encoder = CompressorWriter::new(Vec::new(), 4096, 5, 22);
    encoder.write_all(body.as_bytes())?;
    Ok(encoder.into_inner())
}

fn html_response(body: &str, tile_origin: Option<String>) -> Vec<u8> {
    let image_sources = tile_origin
        .map(|origin| format!("'self' data: {origin}"))
        .unwrap_or_else(|| "'self' data:".to_owned());
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-store\r\nContent-Security-Policy: default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src {image_sources}; connect-src 'self' ws: wss:\r\nX-Content-Type-Options: nosniff\r\nStrict-Transport-Security: max-age=31536000\r\nReferrer-Policy: no-referrer\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

fn tile_origin(tiles: &PublicMapTiles) -> Option<String> {
    let rest = tiles.url_template.strip_prefix("https://")?;
    let end = rest.find(['/', '?']).unwrap_or(rest.len());
    (end > 0).then(|| format!("https://{}", &rest[..end]))
}

const PUBLIC_HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>RCaster Stations</title>
<style>body{font:15px system-ui;margin:0;background:#f5f6f8;color:#17202a}h1{margin:0;padding:18px 24px;background:#17202a;color:white}main{max-width:1100px;margin:20px auto;padding:0 18px}section{background:white;border:1px solid #d9dee5;border-radius:8px;padding:16px;margin-bottom:14px}input,button{padding:8px;margin:4px}table{border-collapse:collapse;width:100%}td,th{text-align:left;padding:7px;border-bottom:1px solid #e5e7eb}pre{white-space:pre-wrap;overflow:auto}svg{width:100%;height:360px;background:#f9fafb;border:1px solid #e5e7eb}#skyplot{max-width:420px;display:block}#map-shell{position:relative;height:360px;overflow:hidden;border:1px solid #e5e7eb;background:#e8eef3}#tile-layer{position:absolute;inset:0;overflow:hidden;background:#e8eef3}#tile-layer img{position:absolute;width:256px;height:256px;user-select:none;pointer-events:none}#map{position:absolute;inset:0;height:100%;border:0;background:transparent;cursor:grab;touch-action:none}#map circle:focus{outline:3px solid #f59e0b;outline-offset:3px}#map-attribution{position:absolute;right:4px;bottom:3px;z-index:3;padding:2px 5px;background:#ffffffd9;font-size:11px;color:#374151}#map-attribution:empty{display:none}canvas{width:100%;height:220px;background:#f9fafb;border:1px solid #e5e7eb}</style></head>
<body><h1>RCaster public stations</h1><main>
<section><input id="username" placeholder="username" autocomplete="username"><input id="password" type="password" placeholder="password" autocomplete="current-password"><button onclick="login()">Sign in</button><button onclick="logout()">Sign out</button><p id="status" role="status" aria-live="polite">Anonymous directory is read-only.</p></section>
<section><h2>Mountpoints</h2><table><thead><tr><th>Mountpoint</th><th>Format</th><th>Region</th><th>Action</th></tr></thead><tbody id="mounts"></tbody></table></section>
<section><h2>Live monitor</h2><div id="satellites">No active source.</div><svg id="skyplot" viewBox="0 0 300 300" role="img" aria-label="Satellite skyplot"></svg><pre id="monitor">Sign in and select Monitor.</pre><canvas id="chart" width="1000" height="220"></canvas></section>
<section><h2>Station map</h2><div id="map-shell"><div id="tile-layer" aria-hidden="true"></div><svg id="map" viewBox="0 0 1000 360" role="img" aria-label="Station map"></svg><div id="map-attribution"></div></div></section>
</main><script>
let csrf='',socket=null,chartTimer=null,currentMount='',mapData=null,tileConfig=null,tileLoaded=false,mapRefreshTimer=null,mapRequest=0;const status=document.getElementById('status');
const mapCamera={centerX:.5,centerY:.5,zoom:3,initialized:false};
async function api(path,options={}){const method=options.method||'GET';const headers={'Content-Type':'application/json',...(options.headers||{})};if(method!=='GET')headers['X-CSRF-Token']=csrf;const r=await fetch(path,{...options,headers});if(!r.ok){const text=await r.text();try{const error=JSON.parse(text);throw new Error(error.message||error.code||text)}catch(error){if(error instanceof SyntaxError)throw new Error(text||('HTTP '+r.status));throw error}}return r.status===204?null:r.json();}
function row(cells,action){const tr=document.createElement('tr');for(const value of cells){const td=document.createElement('td');td.textContent=value??'';tr.append(td)}if(action){const td=document.createElement('td');td.append(action);tr.append(td)}return tr}
async function load(){try{const mountpoints=[];let cursor='';do{const params=new URLSearchParams({limit:'500'});if(cursor)params.set('cursor',cursor);const data=await api('/api/v1/mountpoints?'+params);mountpoints.push(...data.mountpoints);cursor=data.next_cursor||''}while(cursor);document.getElementById('mounts').replaceChildren(...mountpoints.map(m=>{const button=document.createElement('button');button.textContent='Monitor';button.onclick=()=>monitor(m.mountpoint);return row([m.mountpoint,m.format,m.region],button)}));if(!tileLoaded){const mapConfig=await api('/api/v1/public/map/config');tileConfig=mapConfig.tiles||null;tileLoaded=true}await refreshMap()}catch(e){status.textContent=String(e)}}
async function login(){try{const result=await api('/api/v1/auth/login',{method:'POST',body:JSON.stringify({username:document.getElementById('username').value,password:document.getElementById('password').value})});csrf=result.csrf_token;status.textContent='Signed in as '+result.username;}catch(e){status.textContent=String(e)}}
async function logout(){try{await api('/api/v1/auth/logout',{method:'POST'});csrf='';status.textContent='Signed out';}catch(e){status.textContent=String(e)}}
function clamp(value,min,max){return Math.min(max,Math.max(min,value))}
function longitudeX(longitude){return (longitude+180)/360}
function latitudeY(latitude){const bounded=clamp(latitude,-85.05112878,85.05112878),radians=bounded*Math.PI/180;return (1-Math.log(Math.tan(radians)+1/Math.cos(radians))/Math.PI)/2}
function outsideChina(longitude,latitude){return longitude<72.004||longitude>137.8347||latitude<0.8293||latitude>55.8271}
function transformLatitude(longitude,latitude){let value=-100+2*longitude+3*latitude+.2*latitude*latitude+.1*longitude*latitude+.2*Math.sqrt(Math.abs(longitude));value+=(20*Math.sin(6*longitude*Math.PI)+20*Math.sin(2*longitude*Math.PI))*2/3;value+=(20*Math.sin(latitude*Math.PI)+40*Math.sin(latitude/3*Math.PI))*2/3;value+=(160*Math.sin(latitude/12*Math.PI)+320*Math.sin(latitude*Math.PI/30))*2/3;return value}
function transformLongitude(longitude,latitude){let value=300+longitude+2*latitude+.1*longitude*longitude+.1*longitude*latitude+.1*Math.sqrt(Math.abs(longitude));value+=(20*Math.sin(6*longitude*Math.PI)+20*Math.sin(2*longitude*Math.PI))*2/3;value+=(20*Math.sin(longitude*Math.PI)+40*Math.sin(longitude/3*Math.PI))*2/3;value+=(150*Math.sin(longitude/12*Math.PI)+300*Math.sin(longitude/30*Math.PI))*2/3;return value}
function wgs84ToGcj02(longitude,latitude){if(outsideChina(longitude,latitude))return [longitude,latitude];let deltaLatitude=transformLatitude(longitude-105,latitude-35),deltaLongitude=transformLongitude(longitude-105,latitude-35),radians=latitude/180*Math.PI,magic=Math.sin(radians);magic=1-.00669342162296594323*magic*magic;const root=Math.sqrt(magic);deltaLatitude=deltaLatitude*180/(6378245*(1-.00669342162296594323)/(magic*root)*Math.PI);deltaLongitude=deltaLongitude*180/(6378245/root*Math.cos(radians)*Math.PI);return [longitude+deltaLongitude,latitude+deltaLatitude]}
function gcj02ToWgs84(longitude,latitude){if(outsideChina(longitude,latitude))return [longitude,latitude];let resultLongitude=longitude,resultLatitude=latitude;for(let index=0;index<6;index++){const [convertedLongitude,convertedLatitude]=wgs84ToGcj02(resultLongitude,resultLatitude);resultLongitude-=convertedLongitude-longitude;resultLatitude-=convertedLatitude-latitude}return [resultLongitude,resultLatitude]}
function displayPosition(longitude,latitude){return tileConfig?.coordinate_system==='gcj02'?wgs84ToGcj02(longitude,latitude):[longitude,latitude]}
function featurePosition(feature){return displayPosition(feature.geometry.coordinates[0],feature.geometry.coordinates[1])}
function mapZoomBounds(){const min=tileConfig?.min_zoom??0,max=tileConfig?.max_zoom??10;return [min,Math.max(min,max)]}
function fitMap(points){const svg=document.getElementById('map'),width=Math.max(1,svg.clientWidth||1000),height=Math.max(1,svg.clientHeight||360);if(!points.length){mapCamera.zoom=2;mapCamera.centerX=.5;mapCamera.centerY=.5;mapCamera.initialized=true;return}const projected=points.map(featurePosition),xs=projected.map(point=>longitudeX(point[0])),ys=projected.map(point=>latitudeY(point[1])),minX=Math.min(...xs),maxX=Math.max(...xs),minY=Math.min(...ys),maxY=Math.max(...ys),spanX=Math.max(1e-9,maxX-minX),spanY=Math.max(1e-9,maxY-minY),[minZoom,maxZoom]=mapZoomBounds();let zoom=spanX<=1e-9&&spanY<=1e-9?12:Math.floor(Math.log2(Math.min(width*.82/(spanX*256),height*.75/(spanY*256))));mapCamera.zoom=clamp(zoom,minZoom,maxZoom);mapCamera.centerX=(minX+maxX)/2;mapCamera.centerY=(minY+maxY)/2;mapCamera.initialized=true}
function renderMap(data){mapData=data;if(!mapCamera.initialized)fitMap(data.features||[]);drawMap()}
function drawMap(){const shell=document.getElementById('map-shell'),svg=document.getElementById('map'),layer=document.getElementById('tile-layer'),attribution=document.getElementById('map-attribution'),points=mapData?.features||[],width=Math.max(1,shell.clientWidth),height=Math.max(1,shell.clientHeight),[minZoom,maxZoom]=mapZoomBounds();mapCamera.zoom=clamp(Math.round(mapCamera.zoom),minZoom,maxZoom);const size=256*Math.pow(2,mapCamera.zoom),left=mapCamera.centerX*size-width/2,top=mapCamera.centerY*size-height/2;svg.replaceChildren();svg.setAttribute('viewBox','0 0 '+width+' '+height);layer.replaceChildren();attribution.textContent=tileConfig?.attribution||'';if(tileConfig){const tileCount=Math.pow(2,mapCamera.zoom),startX=Math.floor(left/256),endX=Math.ceil((left+width)/256),startY=Math.max(0,Math.floor(top/256)),endY=Math.min(tileCount-1,Math.ceil((top+height)/256));for(let tileY=startY;tileY<=endY;tileY++){for(let tileX=startX;tileX<=endX;tileX++){const wrapped=((tileX%tileCount)+tileCount)%tileCount,image=document.createElement('img');image.alt='';image.draggable=false;image.src=tileConfig.url_template.replace(/\{z\}/g,String(mapCamera.zoom)).replace(/\{x\}/g,String(wrapped)).replace(/\{y\}/g,String(tileY));image.style.left=(tileX*256-left)+'px';image.style.top=(tileY*256-top)+'px';layer.append(image)}}}if(!points.length){const text=document.createElementNS('http://www.w3.org/2000/svg','text');text.setAttribute('x','20');text.setAttribute('y','40');text.textContent='No positioned stations';svg.append(text);return}for(const feature of points){const [longitude,latitude]=feature.geometry.coordinates,[displayLongitude,displayLatitude]=displayPosition(longitude,latitude),x=longitudeX(displayLongitude)*size-left,y=latitudeY(displayLatitude)*size-top;if(x<-30||x>width+30||y<-30||y>height+30)continue;const circle=document.createElementNS('http://www.w3.org/2000/svg','circle');circle.setAttribute('cx',x);circle.setAttribute('cy',y);circle.setAttribute('r','7');circle.setAttribute('fill','#0077b6');circle.setAttribute('stroke','white');circle.setAttribute('stroke-width','2');circle.setAttribute('role','button');circle.setAttribute('tabindex','0');circle.setAttribute('aria-label','Monitor '+feature.properties.mountpoint+' '+feature.properties.format);circle.style.cursor='pointer';circle.dataset.mountpoint=feature.properties.mountpoint;circle.dataset.longitude=String(longitude);circle.dataset.latitude=String(latitude);circle.dataset.displayLongitude=String(displayLongitude);circle.dataset.displayLatitude=String(displayLatitude);const activate=()=>monitor(feature.properties.mountpoint);circle.onclick=event=>{event.stopPropagation();activate()};circle.onkeydown=event=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();activate()}};const title=document.createElementNS('http://www.w3.org/2000/svg','title');title.textContent=feature.properties.mountpoint+' '+feature.properties.format;circle.append(title);svg.append(circle);const label=document.createElementNS('http://www.w3.org/2000/svg','text');label.setAttribute('x',x+10);label.setAttribute('y',y+4);label.setAttribute('paint-order','stroke');label.setAttribute('stroke','white');label.setAttribute('stroke-width','3');label.textContent=feature.properties.mountpoint;svg.append(label)}}
function initializeMapControls(){const svg=document.getElementById('map');let drag=null;svg.addEventListener('wheel',event=>{event.preventDefault();const rect=svg.getBoundingClientRect(),[minZoom,maxZoom]=mapZoomBounds(),oldSize=256*Math.pow(2,mapCamera.zoom),px=event.clientX-rect.left,py=event.clientY-rect.top,worldX=mapCamera.centerX*oldSize+px-rect.width/2,worldY=mapCamera.centerY*oldSize+py-rect.height/2;mapCamera.zoom=clamp(mapCamera.zoom+(event.deltaY>0?-1:1),minZoom,maxZoom);const newSize=256*Math.pow(2,mapCamera.zoom);mapCamera.centerX=(worldX-(px-rect.width/2))/newSize;mapCamera.centerY=(worldY-(py-rect.height/2))/newSize;drawMap()},{passive:false});svg.addEventListener('pointerdown',event=>{drag={x:event.clientX,y:event.clientY,centerX:mapCamera.centerX,centerY:mapCamera.centerY};svg.setPointerCapture(event.pointerId);svg.style.cursor='grabbing'});svg.addEventListener('pointermove',event=>{if(!drag)return;const rect=svg.getBoundingClientRect(),size=256*Math.pow(2,mapCamera.zoom);mapCamera.centerX=drag.centerX-(event.clientX-drag.x)/size;mapCamera.centerY=drag.centerY-(event.clientY-drag.y)/size;drawMap()});const finish=()=>{drag=null;svg.style.cursor='grab'};svg.addEventListener('pointerup',finish);svg.addEventListener('pointercancel',finish);window.addEventListener('resize',drawMap)}
async function drawChart(mountpoint){try{const data=await api('/api/v1/public/streams/'+encodeURIComponent(mountpoint)+'/history?limit=300');const canvas=document.getElementById('chart'),ctx=canvas.getContext('2d'),points=data.points||[];ctx.clearRect(0,0,canvas.width,canvas.height);ctx.strokeStyle='#0077b6';ctx.lineWidth=2;ctx.beginPath();let max=1;for(const p of points)max=Math.max(max,p.bitrate_bps||0);points.forEach((p,i)=>{const x=30+i/Math.max(1,points.length-1)*(canvas.width-60),y=canvas.height-30-(p.bitrate_bps||0)/max*(canvas.height-50);i?ctx.lineTo(x,y):ctx.moveTo(x,y)});ctx.stroke();ctx.fillStyle='#374151';ctx.fillText('bitrate bps, max '+max,10,18);}catch(e){status.textContent=String(e)}}
function renderSatellites(message){const target=document.getElementById('satellites');target.replaceChildren();const sky=document.getElementById('skyplot');sky.replaceChildren();const profile=message.snapshot?.profile;if(!profile){target.textContent='Profile unavailable';return}const cn0=profile.satellite_cn0_db_hz||{};const ring=document.createElementNS('http://www.w3.org/2000/svg','circle');ring.setAttribute('cx','150');ring.setAttribute('cy','150');ring.setAttribute('r','130');ring.setAttribute('fill','none');ring.setAttribute('stroke','#9ca3af');sky.append(ring);for(const sat of profile.satellites||[]){const radius=130*(90-Math.min(90,sat.elevation_deg))/90,angle=(sat.azimuth_deg||0)*Math.PI/180,x=150+Math.sin(angle)*radius,y=150-Math.cos(angle)*radius,signal=sat.snr_db??cn0[sat.prn];const dot=document.createElementNS('http://www.w3.org/2000/svg','circle');dot.setAttribute('cx',x);dot.setAttribute('cy',y);dot.setAttribute('r','9');dot.setAttribute('fill',signal>=35?'#16a34a':signal>=20?'#f59e0b':'#dc2626');const title=document.createElementNS('http://www.w3.org/2000/svg','title');title.textContent=sat.system+sat.prn+' SNR/CN0 '+(signal??'?');dot.append(title);sky.append(dot);const label=document.createElementNS('http://www.w3.org/2000/svg','text');label.setAttribute('x',x+11);label.setAttribute('y',y+4);label.textContent=sat.prn;sky.append(label)}const add=(label,ids)=>{const strong=document.createElement('strong');strong.textContent=label+': ';target.append(strong);for(const id of ids){const badge=document.createElement('span');badge.textContent=String(id);badge.style.cssText='display:inline-block;padding:2px 6px;margin:2px;border-radius:8px;background:#dbeafe';target.append(badge)}};add('Satellites',profile.satellite_ids||[]);target.append(document.createElement('br'));add('Signals',profile.signal_ids||[]);const values=Object.entries(cn0);if(values.length){target.append(document.createElement('br'));const strong=document.createElement('strong');strong.textContent='RTCM CN0 dB-Hz: ';target.append(strong);for(const [prn,value] of values){const badge=document.createElement('span');badge.textContent=prn+'='+value;badge.style.cssText='display:inline-block;padding:2px 6px;margin:2px;border-radius:8px;background:#dcfce7';target.append(badge)}}}
async function monitor(mountpoint){try{await api('/api/v1/monitors/'+encodeURIComponent(mountpoint)+'/ticket',{method:'POST'});if(socket)socket.close();if(chartTimer)clearInterval(chartTimer);currentMount=mountpoint;const scheme=location.protocol==='https:'?'wss':'ws';socket=new WebSocket(scheme+'://'+location.host+'/api/v1/monitors/'+encodeURIComponent(mountpoint)+'/ws');document.getElementById('monitor').textContent='Waiting for '+mountpoint; socket.onmessage=e=>{document.getElementById('monitor').textContent=e.data;try{const message=JSON.parse(e.data);if(message.type==='monitor_snapshot')renderSatellites(message)}catch(_){}drawChart(mountpoint)};socket.onclose=()=>status.textContent='Monitor closed';drawChart(mountpoint);chartTimer=setInterval(()=>drawChart(currentMount),5000);}catch(e){status.textContent=String(e)}}
function mapBbox(){const shell=document.getElementById('map-shell'),width=Math.max(1,shell.clientWidth),height=Math.max(1,shell.clientHeight),[minZoom,maxZoom]=mapZoomBounds();mapCamera.zoom=clamp(Math.round(mapCamera.zoom),minZoom,maxZoom);const size=256*Math.pow(2,mapCamera.zoom),left=mapCamera.centerX-width/(2*size),right=mapCamera.centerX+width/(2*size),top=mapCamera.centerY-height/(2*size),bottom=mapCamera.centerY+height/(2*size),minDisplayLon=clamp(left*360-180,-180,180),maxDisplayLon=clamp(right*360-180,-180,180),minDisplayLat=clamp(Math.atan(Math.sinh(Math.PI*(1-2*clamp(bottom,0,1))))*180/Math.PI,-85.05112878,85.05112878),maxDisplayLat=clamp(Math.atan(Math.sinh(Math.PI*(1-2*clamp(top,0,1))))*180/Math.PI,-85.05112878,85.05112878);if(tileConfig?.coordinate_system==='gcj02'){const [minLon,minLat]=gcj02ToWgs84(minDisplayLon,minDisplayLat),[maxLon,maxLat]=gcj02ToWgs84(maxDisplayLon,maxDisplayLat);return [Math.min(minLon,maxLon),Math.min(minLat,maxLat),Math.max(minLon,maxLon),Math.max(minLat,maxLat)].map(v=>v.toFixed(6)).join(',')}return minDisplayLon<maxDisplayLon&&minDisplayLat<maxDisplayLat?[minDisplayLon,minDisplayLat,maxDisplayLon,maxDisplayLat].map(v=>v.toFixed(6)).join(','):'-180,-85,180,85'}
async function refreshMap(){const id=++mapRequest,data=await api('/api/v1/public/map/stations?bbox='+encodeURIComponent(mapBbox()));if(id===mapRequest)renderMap(data)}
function scheduleMapRefresh(){clearTimeout(mapRefreshTimer);mapRefreshTimer=setTimeout(()=>refreshMap().catch(e=>{status.textContent=String(e)}),180)}
function drawMapClusters(){const shell=document.getElementById('map-shell'),svg=document.getElementById('map'),points=mapData?.features||[],width=Math.max(1,shell.clientWidth),height=Math.max(1,shell.clientHeight);if(mapCamera.zoom>5)return;svg.querySelectorAll('circle,text').forEach(node=>node.remove());const size=256*Math.pow(2,mapCamera.zoom),left=mapCamera.centerX*size-width/2,top=mapCamera.centerY*size-height/2,groups=new Map();for(const feature of points){const [displayLongitude,displayLatitude]=featurePosition(feature),x=longitudeX(displayLongitude)*size-left,y=latitudeY(displayLatitude)*size-top;if(x<-40||x>width+40||y<-40||y>height+40)continue;const key=Math.floor(x/72)+':'+Math.floor(y/72),group=groups.get(key)||{x:0,y:0,features:[]};group.x+=x;group.y+=y;group.features.push(feature);groups.set(key,group)}if(!groups.size){const text=document.createElementNS('http://www.w3.org/2000/svg','text');text.setAttribute('x','20');text.setAttribute('y','40');text.textContent='No positioned stations';svg.append(text);return}for(const group of groups.values()){const count=group.features.length,x=group.x/count,y=group.y/count,circle=document.createElementNS('http://www.w3.org/2000/svg','circle');circle.setAttribute('cx',x);circle.setAttribute('cy',y);circle.setAttribute('r',String(count===1?7:Math.min(22,10+Math.log2(count)*3)));circle.setAttribute('fill',count===1?'#0077b6':'#0f766e');circle.setAttribute('stroke','white');circle.setAttribute('stroke-width','2');circle.setAttribute('role','button');circle.setAttribute('tabindex','0');circle.setAttribute('aria-label',count===1?'Monitor '+group.features[0].properties.mountpoint:'Expand cluster of '+count+' stations');circle.style.cursor='pointer';circle.dataset.clusterCount=String(count);circle.dataset.mountpoints=group.features.map(feature=>feature.properties.mountpoint).join('|');const activate=()=>{if(count===1){monitor(group.features[0].properties.mountpoint);return}mapCamera.centerX=(x+left)/size;mapCamera.centerY=(y+top)/size;mapCamera.zoom=clamp(mapCamera.zoom+2,...mapZoomBounds());drawMap();scheduleMapRefresh()};circle.onclick=event=>{event.stopPropagation();activate()};circle.onkeydown=event=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();activate()}};svg.append(circle);const label=document.createElementNS('http://www.w3.org/2000/svg','text');label.setAttribute('x',x);label.setAttribute('y',y+4);label.setAttribute('text-anchor','middle');label.setAttribute('font-weight','700');label.setAttribute('fill','white');label.style.pointerEvents='none';label.textContent=String(count);svg.append(label)}}
const baseDrawMap=drawMap;drawMap=function(){baseDrawMap();drawMapClusters()};
initializeMapControls();const mapElement=document.getElementById('map');mapElement.addEventListener('wheel',scheduleMapRefresh,{passive:true});mapElement.addEventListener('pointerup',scheduleMapRefresh);mapElement.addEventListener('pointercancel',scheduleMapRefresh);window.addEventListener('resize',scheduleMapRefresh);load();setInterval(load,5000);
</script></body></html>"#;

#[cfg(test)]
mod tests {
    use super::{MonitorStartLimiter, PublicMapTiles, PublicMonitorLimiter, tile_origin};

    #[test]
    fn public_monitor_limiter_enforces_user_and_ip_limits() {
        let limiter = PublicMonitorLimiter::default();
        let first = limiter
            .acquire("viewer", "192.0.2.1", "C63")
            .expect("first");
        let second = limiter
            .acquire("viewer", "192.0.2.1", "C63")
            .expect("second");
        assert!(limiter.acquire("viewer", "192.0.2.1", "C63").is_none());
        drop(first);
        let replacement = limiter
            .acquire("viewer", "192.0.2.1", "C63")
            .expect("replacement");
        let other = limiter.acquire("other", "192.0.2.1", "C63").expect("other");
        let third = limiter.acquire("third", "192.0.2.1", "C63").expect("third");
        assert!(limiter.acquire("fourth", "192.0.2.1", "C63").is_none());
        drop((second, replacement, other, third));
    }

    #[test]
    fn monitor_start_limiter_allows_thirty_starts_per_minute() {
        let limiter = MonitorStartLimiter::default();
        for _ in 0..30 {
            assert!(limiter.allow("user"));
        }
        assert!(!limiter.allow("user"));
        assert!(limiter.allow("other"));
    }

    #[test]
    fn tile_origin_is_limited_to_the_configured_https_host() {
        let tiles = PublicMapTiles {
            url_template: "https://tiles.example.test/{z}/{x}/{y}.png".to_owned(),
            attribution: "Example".to_owned(),
            min_zoom: 0,
            max_zoom: 18,
            coordinate_system: super::PublicMapCoordinateSystem::Wgs84,
        };
        assert_eq!(
            tile_origin(&tiles).as_deref(),
            Some("https://tiles.example.test")
        );
    }
}
