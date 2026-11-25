use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    io,
    sync::Arc,
    time::Duration,
    time::SystemTime,
    time::UNIX_EPOCH,
};

use futures_util::{SinkExt, StreamExt};
use rcaster_core::account::{Mountpoint, MountpointStrConfig, SourceExpectation, User, UserRole};
use serde_json::{Value, json};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
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
    account_auth::hash_password,
    account_directory::{AccountDirectory, AccountDirectoryError},
    admin_auth::{
        self, ADMIN_SESSION_COOKIE, ADMIN_SESSION_TTL_S, AdminAuthError, AdminAuthKind,
        AdminIdentity, LoginLimiter,
    },
    audit::{AuditEvent, AuditLog},
    auth::AuthConcurrencyLimiter,
    cluster_runtime::SharedClusterRuntime,
    connections::ConnectionController,
    coordinator_grpc::{GrpcClientTls, GrpcServerTls, proto::RelayHealthRequest},
    health::HealthState,
    hub::MountpointHub,
    maintenance,
    operations::OperationManager,
    proxy_protocol::{ProxyProtocolConfig, resolve_client_ip},
    relay::RelayManager,
    relay_grpc::RelayGrpcClient,
    source_claim::SourceAdmissionSummary,
    tls_reload::ReloadableTlsConfig,
    upgrade::UpgradeManager,
    web_security::websocket_origin_allowed,
};

#[derive(Clone)]
pub struct AdminState {
    pub cluster_id: String,
    pub caster_id: String,
    pub token: String,
    pub ready: Arc<HealthState>,
    pub hub: Arc<MountpointHub>,
    pub runtime: Option<SharedClusterRuntime>,
    pub relays: Option<Arc<RelayManager>>,
    pub near_mountpoint: Option<String>,
    pub accounts: AccountDirectory,
    pub relay_tls: Option<GrpcClientTls>,
    pub upgrades: Option<UpgradeManager>,
    pub proxy_protocol: Option<ProxyProtocolConfig>,
    pub maintenance_state: Option<std::path::PathBuf>,
    pub listener_tls: Option<ReloadableTlsConfig>,
    pub config_revision_retention: usize,
    pub login_limiter: LoginLimiter,
    pub audit: AuditLog,
    pub websockets: WebSocketLimiter,
    pub connections: Arc<ConnectionController>,
    pub auth_limiter: AuthConcurrencyLimiter,
}

#[derive(Clone, Default)]
pub struct WebSocketLimiter {
    inner: Arc<WebSocketLimitInner>,
}

#[derive(Default)]
struct WebSocketLimitInner {
    total: AtomicUsize,
    next_id: AtomicUsize,
    by_actor: Mutex<BTreeMap<String, usize>>,
    by_mountpoint: Mutex<BTreeMap<usize, (String, tokio::sync::watch::Sender<bool>)>>,
}

pub struct WebSocketSlot {
    id: usize,
    limiter: WebSocketLimiter,
    actor: String,
    cancelled: tokio::sync::watch::Receiver<bool>,
}

impl WebSocketLimiter {
    pub fn acquire(&self, actor: &str, mountpoint: &str) -> Option<WebSocketSlot> {
        let id = self.inner.next_id.fetch_add(1, Ordering::AcqRel);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let mut by_actor = self
            .inner
            .by_actor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.inner.total.load(Ordering::Acquire) >= 4
            || by_actor.get(actor).copied().unwrap_or_default() >= 4
        {
            return None;
        }
        self.inner.total.fetch_add(1, Ordering::AcqRel);
        *by_actor.entry(actor.to_owned()).or_default() += 1;
        self.inner
            .by_mountpoint
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, (mountpoint.to_owned(), cancel_tx));
        Some(WebSocketSlot {
            id,
            limiter: self.clone(),
            actor: actor.to_owned(),
            cancelled: cancel_rx,
        })
    }

    pub fn cancel_mountpoint(&self, mountpoint: &str) -> usize {
        let senders = self
            .inner
            .by_mountpoint
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|(mount, _)| mount == mountpoint)
            .map(|(_, sender)| sender.clone())
            .collect::<Vec<_>>();
        for sender in &senders {
            sender.send_replace(true);
        }
        senders.len()
    }
}

impl Drop for WebSocketSlot {
    fn drop(&mut self) {
        self.limiter.inner.total.fetch_sub(1, Ordering::AcqRel);
        let mut by_actor = self
            .limiter
            .inner
            .by_actor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = by_actor.get_mut(&self.actor) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                by_actor.remove(&self.actor);
            }
        }
        self.limiter
            .inner
            .by_mountpoint
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}

impl WebSocketSlot {
    async fn cancelled(&mut self) {
        while !*self.cancelled.borrow() {
            if self.cancelled.changed().await.is_err() {
                return;
            }
        }
    }
}

#[derive(Clone, Debug)]
enum Access {
    Service,
    Admin(AdminIdentity),
}

tokio::task_local! {
    static REQUEST_ID: String;
}

pub async fn serve_admin_mtls<F>(
    listener: TcpListener,
    tls: GrpcServerTls,
    state: AdminState,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let tls = ReloadableTlsConfig::from_pem_mtls_with_client_crl_and_ocsp(
        &tls.certificate_pem,
        &tls.private_key_pem,
        &tls.client_ca_pem,
        tls.client_crl_pem.as_deref(),
        tls.server_ocsp_der.as_deref(),
    )?;
    serve_admin_mtls_reloadable(listener, tls, state, shutdown).await
}

pub async fn serve_admin_mtls_reloadable<F>(
    listener: TcpListener,
    tls: ReloadableTlsConfig,
    state: AdminState,
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
            let Ok(source_ip) =
                resolve_client_ip(&mut stream, peer.ip(), state.proxy_protocol.as_ref()).await
            else {
                return;
            };
            let acceptor = TlsAcceptor::from(acceptor);
            let Ok(Ok(mut stream)) = timeout(Duration::from_secs(5), acceptor.accept(stream)).await
            else {
                return;
            };
            let request_id = random_request_id();
            let Ok(response) = REQUEST_ID
                .scope(request_id, handle(&mut stream, &state, Some(source_ip)))
                .await
            else {
                return;
            };
            let _ = stream.write_all(&response).await;
        });
    }
}

async fn handle(
    stream: &mut tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    state: &AdminState,
    source_ip: Option<std::net::IpAddr>,
) -> io::Result<Vec<u8>> {
    let request = match read_admin_request(stream).await {
        Ok(request) => request,
        Err(status) => {
            return Ok(response(
                status,
                json!({"code":"BAD_REQUEST","message":"invalid or oversized admin request"}),
            ));
        }
    };
    let request = String::from_utf8_lossy(&request);
    let (head, body) = request.split_once("\r\n\r\n").unwrap_or((&request, ""));
    let mut lines = head.lines();
    let Some(first) = lines.next() else {
        return Ok(response(
            400,
            json!({"code":"BAD_REQUEST","message":"missing request"}),
        ));
    };
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();
    let (path, query) = path.split_once('?').unwrap_or((path, ""));
    let query = parse_query(query);
    let headers = lines.collect::<Vec<_>>();
    if method == "GET" && matches!(path, "/admin" | "/admin/") {
        return Ok(html_response(ADMIN_HTML));
    }
    if method == "GET" && path == "/admin/v1/ws" {
        return serve_admin_websocket(stream, state, &headers, &query).await;
    }
    if method == "POST" && path == "/admin/v1/auth/login" {
        let Ok(body) = serde_json::from_str::<Value>(body) else {
            return Ok(response(
                400,
                json!({"code":"BAD_REQUEST","message":"invalid JSON body"}),
            ));
        };
        let (Some(username), Some(password)) = (
            body.get("username").and_then(Value::as_str),
            body.get("password").and_then(Value::as_str),
        ) else {
            return Ok(response(
                400,
                json!({"code":"BAD_REQUEST","message":"username and password are required"}),
            ));
        };
        let ip_key = format!(
            "ip:{}",
            source_ip.map(|ip| ip.to_string()).unwrap_or_default()
        );
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
            return Ok(response_with_headers(
                429,
                json!({"code":"RATE_LIMITED","message":"too many login attempts"}),
                &[("Retry-After", retry_after.to_string())],
            ));
        }
        let issued = match admin_auth::login(
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
                audit(
                    state,
                    source_ip,
                    username,
                    "auth.login",
                    "administrator session",
                    "success",
                    json!({}),
                );
                issued
            }
            Err(AdminAuthError::Invalid) => {
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
                    audit(
                        state,
                        source_ip,
                        username,
                        "auth.login",
                        "administrator session",
                        "locked",
                        json!({}),
                    );
                    return Ok(response_with_headers(
                        429,
                        json!({"code":"RATE_LIMITED","message":"administrator login locked"}),
                        &[("Retry-After", retry_after.to_string())],
                    ));
                }
                audit(
                    state,
                    source_ip,
                    username,
                    "auth.login",
                    "administrator session",
                    "failure",
                    json!({}),
                );
                return Ok(response(
                    401,
                    json!({"code":"UNAUTHENTICATED","message":"invalid administrator credentials"}),
                ));
            }
            Err(_) => {
                return Ok(response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"administrator authentication unavailable"}),
                ));
            }
        };
        let cookie = format!(
            "{ADMIN_SESSION_COOKIE}={}; Path=/admin; HttpOnly; Secure; SameSite=Strict; Max-Age={ADMIN_SESSION_TTL_S}",
            issued.cookie_value
        );
        return Ok(response_with_headers(
            200,
            json!({
                "user_id": issued.identity.user_id,
                "username": issued.identity.username,
                "csrf_token": issued.identity.csrf_token,
                "expires_at": issued.expires_at,
            }),
            &[("Set-Cookie", cookie)],
        ));
    }
    let bearer = header(&headers, "authorization")
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty());
    let access = if bearer.is_some_and(|token| {
        token.len() == state.token.len()
            && bool::from(token.as_bytes().ct_eq(state.token.as_bytes()))
    }) {
        Access::Service
    } else {
        let cookie = header(&headers, "cookie").and_then(|value| {
            value.split(';').find_map(|part| {
                let (name, value) = part.trim().split_once('=')?;
                (name == ADMIN_SESSION_COOKIE).then_some(value)
            })
        });
        match admin_auth::authenticate(&state.accounts, bearer, cookie).await {
            Ok(identity) => Access::Admin(identity),
            Err(AdminAuthError::Invalid) => {
                return Ok(response(
                    401,
                    json!({"code":"UNAUTHENTICATED","message":"invalid administrator session"}),
                ));
            }
            Err(_) => {
                return Ok(response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"administrator authentication unavailable"}),
                ));
            }
        }
    };
    if let Access::Admin(identity) = &access {
        let required = if method == "GET" {
            &["admin:read"][..]
        } else if path.starts_with("/admin/v1/upgrades") {
            &["upgrade:write"][..]
        } else if path.starts_with("/admin/v1/operations") {
            &["admin:write", "operations:write"][..]
        } else {
            &["admin:write"][..]
        };
        if !required
            .iter()
            .all(|required| identity.scopes.iter().any(|scope| scope == required))
        {
            return Ok(response(
                403,
                json!({"code":"FORBIDDEN","message":"missing API token scope"}),
            ));
        }
    }
    if let Access::Admin(identity) = &access
        && identity.kind == AdminAuthKind::Session
        && method != "GET"
    {
        let Some(supplied) = header(&headers, "x-csrf-token") else {
            return Ok(response(
                403,
                json!({"code":"CSRF_REQUIRED","message":"X-CSRF-Token required"}),
            ));
        };
        let expected = identity.csrf_token.as_deref().unwrap_or_default();
        if supplied.len() != expected.len()
            || !bool::from(supplied.as_bytes().ct_eq(expected.as_bytes()))
        {
            return Ok(response(
                403,
                json!({"code":"CSRF_INVALID","message":"invalid CSRF token"}),
            ));
        }
    }
    if method != "GET" && matches!(&access, Access::Service) {
        return Ok(response(
            403,
            json!({"code":"FORBIDDEN","message":"service token is read-only"}),
        ));
    }
    if state.runtime.is_none()
        && matches!(method, "POST" | "PATCH")
        && path == "/admin/v1/blacklist"
    {
        let Ok(body) = serde_json::from_str::<Value>(body) else {
            return Ok(response(
                400,
                json!({"code":"BAD_REQUEST","message":"invalid JSON body"}),
            ));
        };
        let Some(quota) = body
            .get("quota")
            .and_then(Value::as_i64)
            .and_then(|quota| i32::try_from(quota).ok())
        else {
            return Ok(response(
                400,
                json!({"code":"BAD_REQUEST","message":"invalid quota"}),
            ));
        };
        let rule = crate::blacklist::BlacklistRule {
            schema_version: 1,
            mod_revision: 0,
            id: body
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            cidr: body
                .get("cidr")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            quota,
            enabled: body.get("enabled").and_then(Value::as_bool).unwrap_or(true),
            description: body
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            updated_at: String::new(),
        };
        let expected = header(&headers, "if-match").and_then(parse_revision);
        return match state.accounts.put_blacklist_rule(&rule, expected).await {
            Ok(revision) => {
                audit(
                    state,
                    source_ip,
                    access_actor(&access),
                    "blacklist.upsert",
                    &rule.id,
                    "success",
                    json!({"cidr": rule.cidr, "quota": rule.quota}),
                );
                Ok(response(
                    if method == "POST" { 201 } else { 200 },
                    json!({"revision": revision}),
                ))
            }
            Err(error) => {
                audit(
                    state,
                    source_ip,
                    access_actor(&access),
                    "blacklist.upsert",
                    &rule.id,
                    "failure",
                    json!({"error": error.to_string()}),
                );
                Ok(blacklist_directory_error_response(error))
            }
        };
    }
    if state.runtime.is_none() && method == "DELETE" && path.starts_with("/admin/v1/blacklist/") {
        let id = &path["/admin/v1/blacklist/".len()..];
        let Some(revision) = header(&headers, "if-match").and_then(parse_revision) else {
            return Ok(response(
                428,
                json!({"code":"PRECONDITION_REQUIRED","message":"If-Match required"}),
            ));
        };
        return match state.accounts.delete_blacklist_rule(id, revision).await {
            Ok(()) => {
                audit(
                    state,
                    source_ip,
                    access_actor(&access),
                    "blacklist.delete",
                    id,
                    "success",
                    json!({}),
                );
                Ok(response(204, Value::Null))
            }
            Err(error) => {
                audit(
                    state,
                    source_ip,
                    access_actor(&access),
                    "blacklist.delete",
                    id,
                    "failure",
                    json!({"error": error.to_string()}),
                );
                Ok(blacklist_directory_error_response(error))
            }
        };
    }
    if state.runtime.is_none()
        && let Some(result) =
            standalone_account_write(state, &access, source_ip, method, path, &headers, body).await
    {
        return Ok(result);
    }
    if state.runtime.is_none() && method == "POST" && path == "/admin/v1/maintenance" {
        let Ok(body) = serde_json::from_str::<Value>(body) else {
            return Ok(response(
                400,
                json!({"code":"BAD_REQUEST","message":"invalid JSON body"}),
            ));
        };
        let Some(maintenance) = body.get("maintenance").and_then(Value::as_bool) else {
            return Ok(response(
                400,
                json!({"code":"BAD_REQUEST","message":"maintenance must be a boolean"}),
            ));
        };
        if let Some(path) = &state.maintenance_state
            && let Err(error) = maintenance::save(path, maintenance)
        {
            return Ok(response(
                503,
                json!({
                    "code":"UNAVAILABLE",
                    "message":format!("maintenance state cannot be persisted: {error}"),
                }),
            ));
        }
        state.ready.set_maintenance(maintenance);
        audit(
            state,
            source_ip,
            access_actor(&access),
            "node.maintenance",
            &state.caster_id,
            "success",
            json!({"maintenance": maintenance}),
        );
        return Ok(response(
            200,
            json!({
                "caster_id": state.caster_id,
                "maintenance": maintenance,
                "ready": state.ready.is_ready(),
            }),
        ));
    }
    if method != "GET" {
        if path == "/admin/v1/auth/logout" {
            if let Access::Admin(identity) = &access {
                let _ = admin_auth::logout(&state.accounts, identity).await;
                audit(
                    state,
                    source_ip,
                    &identity.username,
                    "auth.logout",
                    "administrator session",
                    "success",
                    json!({}),
                );
            }
            return Ok(response_with_headers(
                204,
                Value::Null,
                &[(
                    "Set-Cookie",
                    format!(
                        "{ADMIN_SESSION_COOKIE}=; Path=/admin; HttpOnly; Secure; SameSite=Strict; Max-Age=0"
                    ),
                )],
            ));
        }
        if matches!(method, "POST" | "DELETE") && path.starts_with("/admin/v1/api-tokens") {
            let Access::Admin(identity) = &access else {
                return Ok(response(
                    403,
                    json!({"code":"FORBIDDEN","message":"administrator account required"}),
                ));
            };
            if method == "POST" && path == "/admin/v1/api-tokens" {
                let Ok(body) = serde_json::from_str::<Value>(body) else {
                    return Ok(response(
                        400,
                        json!({"code":"BAD_REQUEST","message":"invalid JSON body"}),
                    ));
                };
                let name = body
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|name| !name.is_empty())
                    .unwrap_or("API token");
                let scopes = body
                    .get("scopes")
                    .and_then(Value::as_array)
                    .map(|scopes| {
                        scopes
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_else(|| {
                        vec![
                            "admin:read".to_owned(),
                            "admin:write".to_owned(),
                            "operations:write".to_owned(),
                        ]
                    });
                let expires_in_s = body
                    .get("expires_in_s")
                    .and_then(Value::as_u64)
                    .filter(|seconds| *seconds > 0 && *seconds <= 31_536_000);
                return Ok(
                    match admin_auth::create_api_token(
                        &state.accounts,
                        identity,
                        name,
                        scopes,
                        expires_in_s,
                    )
                    .await
                    {
                        Ok((token, view)) => {
                            audit(
                                state,
                                source_ip,
                                &identity.username,
                                "api_token.create",
                                &view.token_id,
                                "success",
                                json!({"name": view.name}),
                            );
                            response(
                                201,
                                json!({
                                    "token": token,
                                    "token_id": view.token_id,
                                    "name": view.name,
                                    "scopes": view.scopes,
                                    "expires_at": view.expires_at,
                                }),
                            )
                        }
                        Err(AdminAuthError::Invalid) => response(
                            403,
                            json!({"code":"TOKEN_SCOPE_FORBIDDEN","message":"API token scopes exceed caller privileges"}),
                        ),
                        Err(_) => response(
                            503,
                            json!({"code":"UNAVAILABLE","message":"API token store unavailable"}),
                        ),
                    },
                );
            }
            if method == "DELETE" && path.starts_with("/admin/v1/api-tokens/") {
                let token_id = &path["/admin/v1/api-tokens/".len()..];
                return Ok(
                    match admin_auth::delete_api_token(&state.accounts, token_id).await {
                        Ok(()) => {
                            audit(
                                state,
                                source_ip,
                                &identity.username,
                                "api_token.delete",
                                token_id,
                                "success",
                                json!({}),
                            );
                            response(204, Value::Null)
                        }
                        Err(AdminAuthError::NotFound) => response(
                            404,
                            json!({"code":"NOT_FOUND","message":"API token not found"}),
                        ),
                        Err(_) => response(
                            503,
                            json!({"code":"UNAVAILABLE","message":"API token store unavailable"}),
                        ),
                    },
                );
            }
        }
        if state.runtime.is_none() && method == "DELETE" && path.starts_with("/admin/v1/sources/") {
            let mountpoint = &path["/admin/v1/sources/".len()..];
            if state.hub.source_profile(mountpoint).is_none() {
                return Ok(response(
                    404,
                    json!({"code":"NOT_FOUND","message":"source not found"}),
                ));
            }
            if !state.connections.cancel_source(mountpoint) {
                return Ok(response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"source connection not found"}),
                ));
            }
            audit(
                state,
                source_ip,
                access_actor(&access),
                "source.disconnect",
                mountpoint,
                "accepted",
                json!({"caster_id": state.caster_id}),
            );
            return Ok(response(202, json!({"status":"accepted"})));
        }
        if state.runtime.is_none() && method == "DELETE" && path.starts_with("/admin/v1/sessions/")
        {
            let session_id = &path["/admin/v1/sessions/".len()..];
            if !state.connections.cancel_session(session_id) {
                return Ok(response(
                    404,
                    json!({"code":"NOT_FOUND","message":"session not found"}),
                ));
            }
            audit(
                state,
                source_ip,
                access_actor(&access),
                "session.disconnect",
                session_id,
                "accepted",
                json!({"caster_id": state.caster_id}),
            );
            return Ok(response(202, json!({"status":"accepted"})));
        }
        let Some(runtime) = &state.runtime else {
            return Ok(response(
                503,
                json!({"code":"UNAVAILABLE","message":"cluster runtime unavailable"}),
            ));
        };
        if method == "POST" && path == "/admin/v1/config/reload" {
            return Ok(match runtime.reload_config().await {
                Ok(snapshot) => {
                    let pruned = runtime
                        .prune_config_revisions(state.config_revision_retention)
                        .await
                        .unwrap_or_default();
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "config.reload",
                        "caster config",
                        "success",
                        json!({
                            "generation": snapshot.config.generation,
                            "revision": snapshot.revision,
                            "pruned_revisions": pruned,
                        }),
                    );
                    response(
                        200,
                        json!({
                            "generation": snapshot.config.generation,
                            "revision": snapshot.revision,
                        }),
                    )
                }
                Err(_) => response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"config reload failed"}),
                ),
            });
        }
        if method == "POST" && path == "/admin/v1/maintenance" {
            let Ok(body) = serde_json::from_str::<Value>(body) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid JSON body"}),
                ));
            };
            let Some(maintenance) = body.get("maintenance").and_then(Value::as_bool) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"maintenance must be a boolean"}),
                ));
            };
            let previous = state.ready.is_maintenance();
            if let Some(runtime) = &state.runtime
                && runtime.set_maintenance(maintenance).await.is_err()
            {
                return Ok(response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"maintenance state unavailable"}),
                ));
            }
            if let Some(path) = &state.maintenance_state
                && let Err(error) = maintenance::save(path, maintenance)
            {
                if let Some(runtime) = &state.runtime {
                    let _ = runtime.set_maintenance(previous).await;
                }
                return Ok(response(
                    503,
                    json!({
                        "code":"UNAVAILABLE",
                        "message":format!("maintenance state cannot be persisted: {error}"),
                    }),
                ));
            }
            state.ready.set_maintenance(maintenance);
            audit(
                state,
                source_ip,
                access_actor(&access),
                "node.maintenance",
                &state.caster_id,
                "success",
                json!({"maintenance": maintenance}),
            );
            return Ok(response(
                200,
                json!({
                    "caster_id": state.caster_id,
                    "maintenance": maintenance,
                    "ready": state.ready.is_ready(),
                }),
            ));
        }
        if matches!(method, "PUT") && path == "/admin/v1/config"
            || method == "POST" && path == "/admin/v1/config/apply"
        {
            let Ok(config) = serde_json::from_str::<crate::config::CasterConfig>(body) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid Caster Config"}),
                ));
            };
            if config.schema_version != 1
                || !config.source_health.is_valid()
                || config.limits.max_source_bitrate_bps == 0
                || config.limits.max_sources == 0
                || config.limits.max_validating_sources == 0
                || config.limits.max_subscribers == 0
                || config.limits.max_subscribers_per_source == 0
            {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid Caster Config values"}),
                ));
            }
            return Ok(match runtime.publish_config(config.clone()).await {
                Ok(snapshot) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "config.apply",
                        "caster config",
                        "success",
                        json!({
                            "generation": snapshot.config.generation,
                            "revision": snapshot.revision,
                        }),
                    );
                    response(
                        200,
                        json!({
                            "generation": snapshot.config.generation,
                            "revision": snapshot.revision,
                        }),
                    )
                }
                Err(error) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "config.apply",
                        "caster config",
                        "failure",
                        json!({"error": error.to_string()}),
                    );
                    response(
                        409,
                        json!({"code":"CONFIG_CONFLICT","message":error.to_string()}),
                    )
                }
            });
        }
        if method == "POST" && path == "/admin/v1/operations/preview" {
            let Ok(body) = serde_json::from_str::<Value>(body) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid JSON body"}),
                ));
            };
            return Ok(match operation_preview(state, runtime, &body).await {
                Ok(value) => response(200, value),
                Err((status, message)) => response(
                    status,
                    json!({"code":"INVALID_OPERATION","message":message}),
                ),
            });
        }
        if method == "POST" && path == "/admin/v1/operations" {
            let Ok(body) = serde_json::from_str::<Value>(body) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid JSON body"}),
                ));
            };
            let action = body
                .get("action")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if action != "config.rollback" {
                let manager = OperationManager::new(
                    runtime.clone(),
                    state.accounts.clone(),
                    state.cluster_id.clone(),
                    state.caster_id.clone(),
                    Some(Arc::clone(&state.ready)),
                );
                return Ok(
                    match manager
                        .start(
                            &body,
                            upgrade_actor(&access).to_owned(),
                            REQUEST_ID
                                .try_with(Clone::clone)
                                .unwrap_or_else(|_| random_request_id()),
                        )
                        .await
                    {
                        Ok(operation) => {
                            audit(
                                state,
                                source_ip,
                                access_actor(&access),
                                "operation.create",
                                operation["operation_id"].as_str().unwrap_or("operation"),
                                "accepted",
                                json!({"action": action}),
                            );
                            response(202, operation)
                        }
                        Err((status, message)) => response(
                            status,
                            json!({"code":"INVALID_OPERATION","message":message}),
                        ),
                    },
                );
            }
            let preview = match operation_preview(state, runtime, &body).await {
                Ok(value) => value,
                Err((status, message)) => {
                    return Ok(response(
                        status,
                        json!({"code":"INVALID_OPERATION","message":message}),
                    ));
                }
            };
            let target_generation = preview["target_generation"].as_u64().unwrap_or_default();
            let operation_id = random_request_id();
            let key = format!(
                "/rcaster/{}/runtime/operations/{operation_id}",
                state.cluster_id
            );
            let target = runtime
                .config_revisions(500)
                .await
                .unwrap_or_default()
                .into_iter()
                .find(|revision| revision.config.generation == target_generation);
            let Some(target) = target else {
                return Ok(response(
                    404,
                    json!({"code":"NOT_FOUND","message":"target config revision not found"}),
                ));
            };
            let mut config = target.config;
            config.generation = runtime
                .config_snapshot()
                .await
                .map(|snapshot| snapshot.config.generation.saturating_add(1))
                .unwrap_or(1);
            let result = runtime.publish_config(config).await;
            if result.is_ok() {
                let _ = runtime
                    .prune_config_revisions(state.config_revision_retention)
                    .await;
            }
            let status = if result.is_ok() {
                "SUCCEEDED"
            } else {
                "FAILED"
            };
            let operation = json!({
                "schema_version": 1,
                "operation_id": operation_id,
                "type": "config.rollback",
                "status": status,
                "created_by": upgrade_actor(&access),
                "request_id": REQUEST_ID
                    .try_with(Clone::clone)
                    .unwrap_or_else(|_| random_request_id()),
                "target_generation": target_generation,
                "config_generation": result.as_ref().ok().map(|snapshot| snapshot.config.generation),
                "config_revision": result.as_ref().ok().map(|snapshot| snapshot.revision),
                "error": result.as_ref().err().map(ToString::to_string),
                "created_at": now_timestamp(),
                "updated_at": now_timestamp(),
            });
            let _ = state
                .accounts
                .etcd_put(
                    &key,
                    serde_json::to_vec(&operation).unwrap_or_default(),
                    None,
                )
                .await;
            audit(
                state,
                source_ip,
                access_actor(&access),
                "operation.create",
                &operation_id,
                status,
                json!({"type": "config.rollback", "target_generation": target_generation}),
            );
            return Ok(response(if result.is_ok() { 202 } else { 503 }, operation));
        }
        if method == "POST"
            && path.starts_with("/admin/v1/operations/")
            && (path.ends_with("/cancel") || path.ends_with("/retry"))
        {
            let control = if path.ends_with("/retry") {
                "retry"
            } else {
                "cancel"
            };
            let suffix = format!("/{control}");
            let operation_id = &path["/admin/v1/operations/".len()..path.len() - suffix.len()];
            let key = format!(
                "/rcaster/{}/runtime/operations/{operation_id}",
                state.cluster_id
            );
            let (value, revision) = match state.accounts.etcd_get(&key).await {
                Ok(Some(record)) => record,
                Ok(None) => {
                    return Ok(response(
                        404,
                        json!({"code":"NOT_FOUND","message":"operation not found"}),
                    ));
                }
                Err(_) => {
                    return Ok(response(
                        503,
                        json!({"code":"UNAVAILABLE","message":"operation store unavailable"}),
                    ));
                }
            };
            let Ok(mut operation) = serde_json::from_slice::<Value>(&value) else {
                return Ok(response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"operation record is corrupt"}),
                ));
            };
            if !operation_cancel_allowed(&access, &operation) {
                return Ok(response(
                    403,
                    json!({"code":"FORBIDDEN","message":"operation cancellation is not permitted"}),
                ));
            }
            if control == "retry" {
                let manager = OperationManager::new(
                    runtime.clone(),
                    state.accounts.clone(),
                    state.cluster_id.clone(),
                    state.caster_id.clone(),
                    Some(Arc::clone(&state.ready)),
                );
                let result = manager.retry(operation_id).await;
                audit(
                    state,
                    source_ip,
                    access_actor(&access),
                    "operation.retry",
                    operation_id,
                    if result.is_ok() {
                        "accepted"
                    } else {
                        "failure"
                    },
                    json!({"revision": revision}),
                );
                return Ok(match result {
                    Ok(operation) => response(202, operation),
                    Err((status, message)) => response(
                        status,
                        json!({"code":"OPERATION_CONFLICT","message":message}),
                    ),
                });
            }
            if matches!(
                operation["status"].as_str(),
                Some("SUCCEEDED" | "PARTIAL" | "FAILED" | "CANCELED")
            ) {
                return Ok(response(
                    409,
                    json!({"code":"OPERATION_TERMINAL","message":"operation already completed"}),
                ));
            }
            operation["status"] = json!("CANCELED");
            operation["updated_at"] = json!(now_timestamp());
            let result = state
                .accounts
                .etcd_put_if_revision(
                    &key,
                    serde_json::to_vec(&operation).unwrap_or_default(),
                    revision,
                )
                .await;
            audit(
                state,
                source_ip,
                access_actor(&access),
                "operation.cancel",
                operation_id,
                if result.is_ok() {
                    "accepted"
                } else {
                    "failure"
                },
                json!({"revision": revision}),
            );
            return Ok(match result {
                Ok(true) => response(202, operation),
                Ok(false) => response(
                    409,
                    json!({"code":"OPERATION_CONFLICT","message":"operation changed concurrently"}),
                ),
                Err(_) => response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"operation store unavailable"}),
                ),
            });
        }
        if method == "POST" && matches!(path, "/admin/v1/upgrades/preview" | "/admin/v1/upgrades") {
            let Some(upgrades) = &state.upgrades else {
                return Ok(response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"upgrade controller unavailable"}),
                ));
            };
            let Ok(body) = serde_json::from_str::<Value>(body) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid JSON body"}),
                ));
            };
            let result = if path.ends_with("/preview") {
                upgrades.preview(&body).await
            } else {
                upgrades.start_as(&body, upgrade_actor(&access)).await
            };
            return Ok(match result {
                Ok(value) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        if path.ends_with("/preview") {
                            "upgrade.preview"
                        } else {
                            "upgrade.start"
                        },
                        value["operation_id"].as_str().unwrap_or("upgrade preview"),
                        "accepted",
                        json!({
                            "target_version": value["target_version"],
                            "status": value["status"],
                            "created_by": value["created_by"],
                            "approved_by": value["approved_by"],
                        }),
                    );
                    response(if path.ends_with("/preview") { 200 } else { 202 }, value)
                }
                Err(error) => response(
                    error.status(),
                    json!({"code":"UPGRADE_REJECTED","message":error.to_string()}),
                ),
            });
        }
        if method == "POST" && path.starts_with("/admin/v1/upgrades/") {
            let Some(upgrades) = &state.upgrades else {
                return Ok(response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"upgrade controller unavailable"}),
                ));
            };
            let operation_path = &path["/admin/v1/upgrades/".len()..];
            let (operation_id, action) =
                if let Some(operation_id) = operation_path.strip_suffix("/cancel") {
                    (operation_id, "cancel")
                } else if let Some(operation_id) = operation_path.strip_suffix("/approve") {
                    (operation_id, "approve")
                } else if let Some(operation_id) = operation_path.strip_suffix("/rollback") {
                    (operation_id, "rollback")
                } else {
                    return Ok(response(
                        404,
                        json!({"code":"NOT_FOUND","message":"upgrade route not found"}),
                    ));
                };
            let Ok(body) = serde_json::from_str::<Value>(body) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid JSON body"}),
                ));
            };
            let result = match action {
                "cancel" => upgrades.cancel(operation_id).await,
                "approve" => upgrades.approve(operation_id, upgrade_actor(&access)).await,
                _ => {
                    upgrades
                        .rollback_as(operation_id, &body, upgrade_actor(&access))
                        .await
                }
            };
            return Ok(match result {
                Ok(value) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        match action {
                            "cancel" => "upgrade.cancel",
                            "approve" => "upgrade.approve",
                            _ => "upgrade.rollback",
                        },
                        operation_id,
                        "accepted",
                        json!({
                            "status": value["status"],
                            "created_by": value["created_by"],
                            "approved_by": value["approved_by"],
                        }),
                    );
                    response(202, value)
                }
                Err(error) => response(
                    error.status(),
                    json!({"code":"UPGRADE_REJECTED","message":error.to_string()}),
                ),
            });
        }
        if method == "POST" && path.starts_with("/admin/v1/streams/") && path.ends_with("/monitor")
        {
            let mountpoint = &path["/admin/v1/streams/".len()..path.len() - "/monitor".len()];
            if source(state, mountpoint).await.is_none() {
                return Ok(response(
                    404,
                    json!({"code":"NOT_FOUND","message":"stream not found"}),
                ));
            }
            audit(
                state,
                source_ip,
                access_actor(&access),
                "stream.monitor.create",
                mountpoint,
                "accepted",
                json!({}),
            );
            return Ok(response(
                201,
                json!({
                    "mountpoint": mountpoint,
                    "websocket": format!("/admin/v1/ws?mountpoint={mountpoint}"),
                }),
            ));
        }
        if method == "DELETE"
            && path.starts_with("/admin/v1/streams/")
            && path.ends_with("/monitor")
        {
            let mountpoint = &path["/admin/v1/streams/".len()..path.len() - "/monitor".len()];
            let stopped = state.websockets.cancel_mountpoint(mountpoint);
            audit(
                state,
                source_ip,
                access_actor(&access),
                "stream.monitor.stop",
                mountpoint,
                "accepted",
                json!({"stopped": stopped}),
            );
            return Ok(response(
                202,
                json!({"status": "accepted", "stopped": stopped}),
            ));
        }
        if method == "DELETE" && path.starts_with("/admin/v1/sessions/") {
            let session_id = &path["/admin/v1/sessions/".len()..];
            let sessions = runtime.sessions().await.map_err(|_| ());
            let Ok(sessions) = sessions else {
                return Ok(response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"session state unavailable"}),
                ));
            };
            let Some(session) = sessions
                .into_iter()
                .find(|session| session.session_id == session_id)
            else {
                return Ok(response(
                    404,
                    json!({"code":"NOT_FOUND","message":"session not found"}),
                ));
            };
            return Ok(match runtime.request_session_disconnect(&session).await {
                Ok(()) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "session.disconnect",
                        session_id,
                        "accepted",
                        json!({"caster_id": session.caster_id}),
                    );
                    response(202, json!({"status": "accepted"}))
                }
                Err(error) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "session.disconnect",
                        session_id,
                        "failure",
                        json!({"error": error.to_string()}),
                    );
                    response(
                        503,
                        json!({"code":"UNAVAILABLE","message":"disconnect command failed"}),
                    )
                }
            });
        }
        if method == "DELETE" && path.starts_with("/admin/v1/sources/") {
            let mountpoint = &path["/admin/v1/sources/".len()..];
            let Some(route) = runtime.source_route(mountpoint).await else {
                return Ok(response(
                    404,
                    json!({"code":"NOT_FOUND","message":"source not found"}),
                ));
            };
            return Ok(match runtime.request_source_disconnect(&route).await {
                Ok(()) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "source.disconnect",
                        mountpoint,
                        "accepted",
                        json!({"caster_id": route.caster_id}),
                    );
                    response(202, json!({"status": "accepted"}))
                }
                Err(error) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "source.disconnect",
                        mountpoint,
                        "failure",
                        json!({"error": error.to_string()}),
                    );
                    response(
                        503,
                        json!({"code":"UNAVAILABLE","message":"disconnect command failed"}),
                    )
                }
            });
        }
        if method == "DELETE" && path.starts_with("/admin/v1/source-cooldowns/") {
            let fingerprint = &path["/admin/v1/source-cooldowns/".len()..];
            return Ok(match runtime.clear_source_cooldown(fingerprint).await {
                Ok(()) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "source_cooldown.delete",
                        fingerprint,
                        "success",
                        json!({}),
                    );
                    response(204, Value::Null)
                }
                Err(_) => response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid source cooldown"}),
                ),
            });
        }
        if method == "POST" && path == "/admin/v1/users" {
            let Ok(body) = serde_json::from_str::<Value>(body) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid JSON body"}),
                ));
            };
            let Some(user_id) = body.get("user_id").and_then(Value::as_str) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"user_id is required"}),
                ));
            };
            let Some(username) = body.get("username").and_then(Value::as_str) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"username is required"}),
                ));
            };
            let Some(password) = body.get("password").and_then(Value::as_str) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"password is required"}),
                ));
            };
            let password = password.to_owned();
            let Ok(Ok(password_hash)) =
                tokio::task::spawn_blocking(move || hash_password(&password)).await
            else {
                return Ok(response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"password hashing unavailable"}),
                ));
            };
            let now = now_timestamp();
            let role = match body.get("role").and_then(Value::as_str) {
                Some("admin") => UserRole::Admin,
                Some("user") | None => UserRole::User,
                Some(_) => {
                    return Ok(response(
                        400,
                        json!({"code":"BAD_REQUEST","message":"invalid role"}),
                    ));
                }
            };
            let user = User {
                schema_version: 1,
                mod_revision: 0,
                user_id: user_id.to_owned(),
                username: username.to_owned(),
                password_hash,
                auth_version: 1,
                enabled: body.get("enabled").and_then(Value::as_bool).unwrap_or(true),
                ntrip_enabled: body
                    .get("ntrip_enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                web_enabled: body
                    .get("web_enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(role == UserRole::Admin),
                role,
                created_at: now.clone(),
                updated_at: now,
            };
            return Ok(match runtime.upsert_user(user, None).await {
                Ok(revision) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "user.create",
                        user_id,
                        "success",
                        json!({"username": username, "role": role}),
                    );
                    response(201, json!({"revision": revision}))
                }
                Err(error) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "user.create",
                        user_id,
                        "failure",
                        json!({"error": error.to_string()}),
                    );
                    account_error_response(error)
                }
            });
        }
        if method == "PATCH" && path.starts_with("/admin/v1/users/") {
            let user_id = &path["/admin/v1/users/".len()..];
            let Some(revision) = header(&headers, "if-match").and_then(parse_revision) else {
                return Ok(response(
                    428,
                    json!({"code":"PRECONDITION_REQUIRED","message":"If-Match required"}),
                ));
            };
            let Ok(body) = serde_json::from_str::<Value>(body) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid JSON body"}),
                ));
            };
            let accounts = state.accounts.snapshot();
            let Some(current) = accounts.user_by_id(user_id).cloned() else {
                return Ok(response(
                    404,
                    json!({"code":"NOT_FOUND","message":"user not found"}),
                ));
            };
            let mut updated = current.clone();
            if let Some(value) = body.get("user_id")
                && value.as_str() != Some(current.user_id.as_str())
            {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"user_id cannot be changed"}),
                ));
            }
            if let Some(value) = body.get("username")
                && value.as_str() != Some(current.username.as_str())
            {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"username cannot be changed"}),
                ));
            }
            if let Some(password) = body.get("password").and_then(Value::as_str) {
                let password = password.to_owned();
                let Ok(Ok(password_hash)) =
                    tokio::task::spawn_blocking(move || hash_password(&password)).await
                else {
                    return Ok(response(
                        503,
                        json!({"code":"UNAVAILABLE","message":"password hashing unavailable"}),
                    ));
                };
                updated.password_hash = password_hash;
                updated.auth_version = updated.auth_version.saturating_add(1);
            }
            if let Some(role) = body.get("role").and_then(Value::as_str) {
                let role = match role {
                    "admin" => UserRole::Admin,
                    "user" => UserRole::User,
                    _ => {
                        return Ok(response(
                            400,
                            json!({"code":"BAD_REQUEST","message":"invalid role"}),
                        ));
                    }
                };
                if updated.role != role {
                    updated.role = role;
                    updated.auth_version = updated.auth_version.saturating_add(1);
                }
            }
            if let Some(enabled) = body.get("enabled").and_then(Value::as_bool)
                && updated.enabled != enabled
            {
                updated.enabled = enabled;
                updated.auth_version = updated.auth_version.saturating_add(1);
            }
            if let Some(enabled) = body.get("web_enabled").and_then(Value::as_bool)
                && updated.web_enabled != enabled
            {
                updated.web_enabled = enabled;
                updated.auth_version = updated.auth_version.saturating_add(1);
            }
            if let Some(enabled) = body.get("ntrip_enabled").and_then(Value::as_bool) {
                updated.ntrip_enabled = enabled;
            }
            updated.updated_at = now_timestamp();
            return Ok(match runtime.upsert_user(updated, Some(revision)).await {
                Ok(revision) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "user.update",
                        user_id,
                        "success",
                        json!({"revision": revision}),
                    );
                    response(200, json!({"revision": revision}))
                }
                Err(error) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "user.update",
                        user_id,
                        "failure",
                        json!({"error": error.to_string()}),
                    );
                    account_error_response(error)
                }
            });
        }
        if method == "DELETE" && path.starts_with("/admin/v1/users/") {
            let user_id = &path["/admin/v1/users/".len()..];
            let Some(revision) = header(&headers, "if-match").and_then(parse_revision) else {
                return Ok(response(
                    428,
                    json!({"code":"PRECONDITION_REQUIRED","message":"If-Match required"}),
                ));
            };
            return Ok(match runtime.delete_user(user_id, revision).await {
                Ok(()) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "user.delete",
                        user_id,
                        "success",
                        json!({}),
                    );
                    response(204, Value::Null)
                }
                Err(error) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "user.delete",
                        user_id,
                        "failure",
                        json!({"error": error.to_string()}),
                    );
                    account_error_response(error)
                }
            });
        }
        if method == "POST" && path == "/admin/v1/mountpoints" {
            let Ok(body) = serde_json::from_str::<Value>(body) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid JSON body"}),
                ));
            };
            let Some(mountpoint) = body.get("mountpoint").and_then(Value::as_str) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"mountpoint is required"}),
                ));
            };
            let Some(owner_user_id) = body.get("owner_user_id").and_then(Value::as_str) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"owner_user_id is required"}),
                ));
            };
            let upload_password = body
                .get("ntrip1_upload_password")
                .and_then(Value::as_str)
                .filter(|password| !password.is_empty())
                .map(str::to_owned);
            let ntrip1_upload_password_hash = match upload_password {
                Some(password) => {
                    let Ok(Ok(hash)) =
                        tokio::task::spawn_blocking(move || hash_password(&password)).await
                    else {
                        return Ok(response(
                            503,
                            json!({"code":"UNAVAILABLE","message":"password hashing unavailable"}),
                        ));
                    };
                    Some(hash)
                }
                None => None,
            };
            let now = now_timestamp();
            let mountpoint_name = mountpoint.to_owned();
            let source_expectation = match body.get("source_expectation") {
                None | Some(Value::Null) => None,
                Some(value) => match serde_json::from_value::<SourceExpectation>(value.clone()) {
                    Ok(expectation) => Some(expectation),
                    Err(_) => {
                        return Ok(response(
                            400,
                            json!({"code":"BAD_REQUEST","message":"invalid source expectation"}),
                        ));
                    }
                },
            };
            let Ok(latitude) = parse_optional_number(&body, "latitude") else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid latitude"}),
                ));
            };
            let Ok(longitude) = parse_optional_number(&body, "longitude") else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid longitude"}),
                ));
            };
            let Ok(coverage_radius_m) = parse_optional_number(&body, "coverage_radius_m") else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid coverage radius"}),
                ));
            };
            let Ok(str_fields) = parse_optional_str_fields(&body) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid STR fields"}),
                ));
            };
            let mountpoint = Mountpoint {
                schema_version: 1,
                mod_revision: 0,
                mountpoint: mountpoint_name.clone(),
                owner_user_id: owner_user_id.to_owned(),
                enabled: body.get("enabled").and_then(Value::as_bool).unwrap_or(true),
                ntrip1_upload_password_hash,
                advertise: body
                    .get("advertise")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                region: body
                    .get("region")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                latitude,
                longitude,
                coverage_radius_m,
                str_fields,
                source_expectation,
                created_at: now.clone(),
                updated_at: now,
            };
            return Ok(match runtime.upsert_mountpoint(mountpoint, None).await {
                Ok(revision) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "mountpoint.create",
                        &mountpoint_name,
                        "success",
                        json!({"owner_user_id": owner_user_id}),
                    );
                    response(201, json!({"revision": revision}))
                }
                Err(error) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "mountpoint.create",
                        &mountpoint_name,
                        "failure",
                        json!({"error": error.to_string()}),
                    );
                    account_error_response(error)
                }
            });
        }
        if method == "PATCH" && path.starts_with("/admin/v1/mountpoints/") {
            let mountpoint_name = &path["/admin/v1/mountpoints/".len()..];
            let Some(revision) = header(&headers, "if-match").and_then(parse_revision) else {
                return Ok(response(
                    428,
                    json!({"code":"PRECONDITION_REQUIRED","message":"If-Match required"}),
                ));
            };
            let Ok(body) = serde_json::from_str::<Value>(body) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid JSON body"}),
                ));
            };
            let accounts = state.accounts.snapshot();
            let Some(current) = accounts.mountpoint(mountpoint_name).cloned() else {
                return Ok(response(
                    404,
                    json!({"code":"NOT_FOUND","message":"mountpoint not found"}),
                ));
            };
            let active_route = runtime.source_route(mountpoint_name).await;
            let disabling = body.get("enabled").and_then(Value::as_bool) == Some(false);
            if active_route.is_some() && !disabling {
                return Ok(response(
                    409,
                    json!({"code":"ACTIVE_SOURCE","message":"mountpoint has an active source"}),
                ));
            }
            let mut updated = current.clone();
            if let Some(value) = body.get("mountpoint")
                && value.as_str() != Some(current.mountpoint.as_str())
            {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"mountpoint cannot be renamed"}),
                ));
            }
            if let Some(owner_user_id) = body.get("owner_user_id").and_then(Value::as_str) {
                updated.owner_user_id = owner_user_id.to_owned();
            }
            if let Some(enabled) = body.get("enabled").and_then(Value::as_bool) {
                updated.enabled = enabled;
            }
            if let Some(advertise) = body.get("advertise").and_then(Value::as_bool) {
                updated.advertise = advertise;
            }
            if let Some(region) = body.get("region") {
                updated.region = region.as_str().map(str::to_owned);
            }
            if body.get("latitude").is_some() {
                let Ok(latitude) = parse_optional_number(&body, "latitude") else {
                    return Ok(response(
                        400,
                        json!({"code":"BAD_REQUEST","message":"invalid latitude"}),
                    ));
                };
                updated.latitude = latitude;
            }
            if body.get("longitude").is_some() {
                let Ok(longitude) = parse_optional_number(&body, "longitude") else {
                    return Ok(response(
                        400,
                        json!({"code":"BAD_REQUEST","message":"invalid longitude"}),
                    ));
                };
                updated.longitude = longitude;
            }
            if body.get("coverage_radius_m").is_some() {
                let Ok(coverage_radius_m) = parse_optional_number(&body, "coverage_radius_m")
                else {
                    return Ok(response(
                        400,
                        json!({"code":"BAD_REQUEST","message":"invalid coverage radius"}),
                    ));
                };
                updated.coverage_radius_m = coverage_radius_m;
            }
            if body.get("str_fields").is_some() {
                let Ok(str_fields) = parse_optional_str_fields(&body) else {
                    return Ok(response(
                        400,
                        json!({"code":"BAD_REQUEST","message":"invalid STR fields"}),
                    ));
                };
                updated.str_fields = str_fields;
            }
            if let Some(value) = body.get("source_expectation") {
                updated.source_expectation = if value.is_null() {
                    None
                } else {
                    match serde_json::from_value::<SourceExpectation>(value.clone()) {
                        Ok(expectation) => Some(expectation),
                        Err(_) => {
                            return Ok(response(
                                400,
                                json!({"code":"BAD_REQUEST","message":"invalid source expectation"}),
                            ));
                        }
                    }
                };
            }
            if let Some(password) = body
                .get("ntrip1_upload_password")
                .and_then(Value::as_str)
                .filter(|password| !password.is_empty())
            {
                let password = password.to_owned();
                let Ok(Ok(hash)) =
                    tokio::task::spawn_blocking(move || hash_password(&password)).await
                else {
                    return Ok(response(
                        503,
                        json!({"code":"UNAVAILABLE","message":"password hashing unavailable"}),
                    ));
                };
                updated.ntrip1_upload_password_hash = Some(hash);
            }
            if body
                .get("ntrip1_upload_password")
                .is_some_and(Value::is_null)
            {
                updated.ntrip1_upload_password_hash = None;
            }
            updated.updated_at = now_timestamp();
            return Ok(
                match runtime.upsert_mountpoint(updated, Some(revision)).await {
                    Ok(revision) => {
                        let mut affected_connections = 0;
                        let mut disconnect_pending = false;
                        if disabling {
                            if let Ok(sessions) = runtime.sessions().await {
                                affected_connections = sessions
                                    .iter()
                                    .filter(|session| session.mountpoint == *mountpoint_name)
                                    .count();
                            }
                            if let Some(route) = &active_route
                                && runtime.request_source_disconnect(route).await.is_err()
                            {
                                disconnect_pending = true;
                            }
                        }
                        audit(
                            state,
                            source_ip,
                            access_actor(&access),
                            "mountpoint.update",
                            mountpoint_name,
                            if disconnect_pending {
                                "partial"
                            } else {
                                "success"
                            },
                            json!({
                                "revision": revision,
                                "affected_connections": affected_connections,
                                "disconnect_pending": disconnect_pending,
                            }),
                        );
                        response(
                            if disconnect_pending { 503 } else { 200 },
                            json!({
                                "revision": revision,
                                "affected_connections": affected_connections,
                                "disconnect_pending": disconnect_pending,
                            }),
                        )
                    }
                    Err(error) => {
                        audit(
                            state,
                            source_ip,
                            access_actor(&access),
                            "mountpoint.update",
                            mountpoint_name,
                            "failure",
                            json!({"error": error.to_string()}),
                        );
                        account_error_response(error)
                    }
                },
            );
        }
        if method == "DELETE" && path.starts_with("/admin/v1/mountpoints/") {
            let mountpoint = &path["/admin/v1/mountpoints/".len()..];
            let Some(revision) = header(&headers, "if-match").and_then(parse_revision) else {
                return Ok(response(
                    428,
                    json!({"code":"PRECONDITION_REQUIRED","message":"If-Match required"}),
                ));
            };
            if let Some(route) = runtime.source_route(mountpoint).await {
                if !query
                    .get("force")
                    .is_some_and(|value| value.eq_ignore_ascii_case("true"))
                {
                    return Ok(response(
                        409,
                        json!({"code":"ACTIVE_SOURCE","message":"mountpoint has an active source"}),
                    ));
                }
                if runtime.request_source_disconnect(&route).await.is_err() {
                    return Ok(response(
                        503,
                        json!({"code":"DISCONNECT_PENDING","message":"source disconnect command failed"}),
                    ));
                }
                let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                while runtime.source_route(mountpoint).await.is_some()
                    && tokio::time::Instant::now() < deadline
                {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                if runtime.source_route(mountpoint).await.is_some() {
                    return Ok(response(
                        409,
                        json!({"code":"DISCONNECT_PENDING","message":"source has not released mountpoint"}),
                    ));
                }
            }
            return Ok(
                match runtime.delete_mountpoint(mountpoint, revision).await {
                    Ok(()) => {
                        audit(
                            state,
                            source_ip,
                            access_actor(&access),
                            "mountpoint.delete",
                            mountpoint,
                            "success",
                            json!({}),
                        );
                        response(204, Value::Null)
                    }
                    Err(error) => {
                        audit(
                            state,
                            source_ip,
                            access_actor(&access),
                            "mountpoint.delete",
                            mountpoint,
                            "failure",
                            json!({"error": error.to_string()}),
                        );
                        account_error_response(error)
                    }
                },
            );
        }
        if matches!(method, "POST" | "PATCH") && path == "/admin/v1/blacklist" {
            let Ok(body) = serde_json::from_str::<Value>(body) else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid JSON body"}),
                ));
            };
            let Some(quota) = body
                .get("quota")
                .and_then(Value::as_i64)
                .and_then(|quota| i32::try_from(quota).ok())
            else {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid quota"}),
                ));
            };
            let rule = crate::blacklist::BlacklistRule {
                schema_version: 1,
                mod_revision: 0,
                id: body
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                cidr: body
                    .get("cidr")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                quota,
                enabled: body.get("enabled").and_then(Value::as_bool).unwrap_or(true),
                description: body
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                updated_at: String::new(),
            };
            let expected = header(&headers, "if-match").and_then(parse_revision);
            return match runtime.upsert_blacklist_rule(&rule, expected).await {
                Ok(revision) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "blacklist.upsert",
                        &rule.id,
                        "success",
                        json!({"cidr": rule.cidr, "quota": rule.quota}),
                    );
                    Ok(response(
                        if method == "POST" { 201 } else { 200 },
                        json!({"revision": revision}),
                    ))
                }
                Err(error) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "blacklist.upsert",
                        &rule.id,
                        "failure",
                        json!({"error": error.to_string()}),
                    );
                    Ok(match error {
                        crate::blacklist::BlacklistError::Conflict => response(
                            409,
                            json!({"code":"CONFLICT","message":"blacklist revision conflict"}),
                        ),
                        _ => response(
                            400,
                            json!({"code":"BAD_REQUEST","message":"invalid blacklist rule"}),
                        ),
                    })
                }
            };
        }
        if method == "DELETE" && path.starts_with("/admin/v1/blacklist/") {
            let id = &path["/admin/v1/blacklist/".len()..];
            let Some(revision) = header(&headers, "if-match").and_then(parse_revision) else {
                return Ok(response(
                    428,
                    json!({"code":"PRECONDITION_REQUIRED","message":"If-Match required"}),
                ));
            };
            return match runtime.delete_blacklist_rule(id, revision).await {
                Ok(()) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "blacklist.delete",
                        id,
                        "success",
                        json!({}),
                    );
                    Ok(response(204, Value::Null))
                }
                Err(error) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(&access),
                        "blacklist.delete",
                        id,
                        "failure",
                        json!({"error": error.to_string()}),
                    );
                    Ok(match error {
                        crate::blacklist::BlacklistError::Conflict => response(
                            409,
                            json!({"code":"CONFLICT","message":"blacklist revision conflict"}),
                        ),
                        _ => response(
                            404,
                            json!({"code":"NOT_FOUND","message":"blacklist rule not found"}),
                        ),
                    })
                }
            };
        }
        return Ok(response(
            405,
            json!({"code":"METHOD_NOT_ALLOWED","message":"unsupported admin method"}),
        ));
    }
    let value = match path {
        "/admin/v1/auth/session" => match &access {
            Access::Service => json!({
                "kind": "service",
                "authenticated": true,
            }),
            Access::Admin(identity) => json!({
                "kind": match identity.kind {
                    AdminAuthKind::Session => "session",
                    AdminAuthKind::ApiToken => "api_token",
                },
                "authenticated": true,
                "user_id": identity.user_id,
                "username": identity.username,
            }),
        },
        "/admin/v1/auth/csrf" => {
            let Access::Admin(identity) = &access else {
                return Ok(response(
                    403,
                    json!({"code":"FORBIDDEN","message":"administrator session required"}),
                ));
            };
            json!({"csrf_token": identity.csrf_token})
        }
        "/admin/v1/api-tokens" => {
            let Access::Admin(_) = &access else {
                return Ok(response(
                    403,
                    json!({"code":"FORBIDDEN","message":"administrator account required"}),
                ));
            };
            match admin_auth::list_api_tokens(&state.accounts).await {
                Ok(tokens) => json!({"tokens": tokens}),
                Err(_) => {
                    return Ok(response(
                        503,
                        json!({"code":"UNAVAILABLE","message":"API token store unavailable"}),
                    ));
                }
            }
        }
        "/admin/v1/audit" => {
            let limit = query
                .get("limit")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(100)
                .clamp(1, 1000);
            match state.audit.recent(limit).await {
                Ok(events) => json!({"events": events, "limit": limit}),
                Err(_) => {
                    return Ok(response(
                        503,
                        json!({"code":"UNAVAILABLE","message":"audit log unavailable"}),
                    ));
                }
            }
        }
        "/admin/v1/source-events" => {
            let limit = query
                .get("limit")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(100)
                .clamp(1, 1000);
            let before = query.get("before").copied();
            match state.audit.recent(1000).await {
                Ok(events) => {
                    let mut events = events
                        .into_iter()
                        .filter(|event| event.action.starts_with("source."))
                        .collect::<Vec<_>>();
                    if let Some(before) = before {
                        if let Some(position) =
                            events.iter().position(|event| event.request_id == before)
                        {
                            events.drain(..=position);
                        } else {
                            events.clear();
                        }
                    }
                    let next_cursor =
                        (events.len() > limit).then(|| events[limit - 1].request_id.clone());
                    let events = events
                        .into_iter()
                        .take(limit)
                        .map(|event| {
                            json!({
                                "event_id": event.request_id,
                                "mountpoint": event.target,
                                "event": event.action.trim_start_matches("source."),
                                "caster_id": event.details.get("caster_id"),
                                "source_generation": event.details.get("source_generation"),
                                "connection_id": event.details.get("connection_id"),
                                "reason": event.details.get("reason"),
                                "timestamp": event.timestamp,
                            })
                        })
                        .collect::<Vec<_>>();
                    json!({
                        "events": events,
                        "limit": limit,
                        "next_cursor": next_cursor
                    })
                }
                Err(_) => {
                    return Ok(response(
                        503,
                        json!({"code":"UNAVAILABLE","message":"source event log unavailable"}),
                    ));
                }
            }
        }
        "/admin/v1/cluster/status" => {
            let local_sources = state.hub.active_sources();
            let mut cluster_sources = local_sources.iter().cloned().collect::<BTreeSet<_>>();
            if let Some(runtime) = &state.runtime {
                cluster_sources.extend(
                    runtime
                        .sourcetable_records()
                        .await
                        .into_iter()
                        .map(|record| record.mountpoint),
                );
            }
            json!({
                "cluster_id": state.cluster_id,
                "caster_id": state.caster_id,
                "ready": state.ready.is_ready(),
                "local_active_sources": local_sources.len(),
                "cluster_active_sources": cluster_sources.len(),
            })
        }
        "/admin/v1/sources" => match sources(state, &query).await {
            Ok(sources) => sources,
            Err(message) => {
                return Ok(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":message}),
                ));
            }
        },
        "/admin/v1/relays" => json!({
            "relays": match &state.relays {
                Some(relays) => serde_json::to_value(relays.views().await).unwrap_or(Value::Array(Vec::new())),
                None => Value::Array(Vec::new()),
            },
        }),
        "/admin/v1/near" => json!({
            "enabled": state.near_mountpoint.is_some(),
            "mountpoint": state.near_mountpoint,
        }),
        "/admin/v1/maintenance" => json!({
            "caster_id": state.caster_id,
            "maintenance": state.ready.is_maintenance(),
            "ready": state.ready.is_ready(),
        }),
        "/admin/v1/near/clients" => {
            let Some(mountpoint) = state.near_mountpoint.as_deref() else {
                return Ok(response(200, json!({"clients": []})));
            };
            let Some(runtime) = &state.runtime else {
                return Ok(response(200, json!({"clients": []})));
            };
            match runtime.sessions().await {
                Ok(sessions) => json!({
                    "mountpoint": mountpoint,
                    "clients": sessions
                        .into_iter()
                        .filter(|session| session.mountpoint == mountpoint)
                        .collect::<Vec<_>>(),
                }),
                Err(_) => {
                    return Ok(response(
                        503,
                        json!({"code":"UNAVAILABLE","message":"session state unavailable"}),
                    ));
                }
            }
        }
        "/admin/v1/users" => json!({
            "users": state.accounts.snapshot().users().map(user_view).collect::<Vec<_>>(),
        }),
        "/admin/v1/mountpoints" => json!({
            "mountpoints": state.accounts.snapshot().mountpoints().map(mountpoint_view).collect::<Vec<_>>(),
        }),
        "/admin/v1/blacklist" => json!({
            "rules": match &state.runtime {
                Some(runtime) => serde_json::to_value(runtime.blacklist_rules().await).unwrap_or(Value::Array(Vec::new())),
                None => serde_json::to_value(state.accounts.blacklist_rules()).unwrap_or(Value::Array(Vec::new())),
            },
        }),
        path if path.starts_with("/admin/v1/blacklist/") => {
            let id = &path["/admin/v1/blacklist/".len()..];
            if state.runtime.is_none() {
                return match state
                    .accounts
                    .blacklist_rules()
                    .into_iter()
                    .find(|rule| rule.id == id)
                {
                    Some(rule) => Ok(response(200, json!({"rule": rule}))),
                    None => Ok(response(
                        404,
                        json!({"code":"NOT_FOUND","message":"blacklist rule not found"}),
                    )),
                };
            }
            let key = format!("/rcaster/{}/config/blacklist/{id}", state.cluster_id);
            let rule = match state.accounts.etcd_get(&key).await {
                Ok(Some((value, revision))) => {
                    serde_json::from_slice::<crate::blacklist::BlacklistRule>(&value)
                        .ok()
                        .map(|mut rule| {
                            rule.mod_revision = revision as u64;
                            rule
                        })
                }
                _ => None,
            };
            match rule {
                Some(rule) => json!({"rule": rule}),
                None => {
                    return Ok(response(
                        404,
                        json!({"code":"NOT_FOUND","message":"blacklist rule not found"}),
                    ));
                }
            }
        }
        "/admin/v1/sessions" => json!({
            "sessions": match &state.runtime {
                Some(runtime) => match runtime.sessions().await {
                    Ok(sessions) => serde_json::to_value(sessions).unwrap_or(Value::Array(Vec::new())),
                    Err(_) => return Ok(response(503, json!({"code":"UNAVAILABLE","message":"session state unavailable"}))),
                },
                None => Value::Array(
                    state
                        .connections
                        .sessions()
                        .into_iter()
                        .map(|session| json!({
                            "schema_version": 1,
                            "session_id": session.session_id,
                            "session_type": session.session_type,
                            "user_id": session.user_id,
                            "mountpoint": session.mountpoint,
                            "caster_id": state.caster_id,
                            "runtime_epoch": "standalone",
                            "ntrip_version": session.ntrip_version,
                            "client_ip": session.client_ip,
                            "software": session.software,
                            "started_at": session.started_at.to_string(),
                        }))
                        .collect(),
                ),
            },
        }),
        "/admin/v1/source-cooldowns" => match &state.runtime {
            Some(runtime) => match runtime.source_cooldowns().await {
                Ok(cooldowns) => json!({"cooldowns": cooldowns}),
                Err(_) => {
                    return Ok(response(
                        503,
                        json!({"code":"UNAVAILABLE","message":"source cooldowns unavailable"}),
                    ));
                }
            },
            None => json!({"cooldowns": []}),
        },
        "/admin/v1/config/status" => match &state.runtime {
            Some(runtime) => {
                let snapshot = runtime.config_snapshot().await;
                json!({
                    "generation": snapshot.as_ref().map(|snapshot| snapshot.config.generation),
                    "revision": snapshot.as_ref().map(|snapshot| snapshot.revision),
                    "available": snapshot.is_some(),
                    "healthy": runtime.config_healthy().await,
                    "source_health": snapshot.as_ref().map(|snapshot| &snapshot.config.source_health),
                })
            }
            None => json!({"available": false}),
        },
        "/admin/v1/config" => match &state.runtime {
            Some(runtime) => match runtime.config_snapshot().await {
                Some(snapshot) => json!({
                    "config": snapshot.config,
                    "revision": snapshot.revision,
                }),
                None => {
                    return Ok(response(
                        503,
                        json!({"code":"UNAVAILABLE","message":"config unavailable"}),
                    ));
                }
            },
            None => {
                return Ok(response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"cluster runtime unavailable"}),
                ));
            }
        },
        "/admin/v1/config/revisions" => match &state.runtime {
            Some(runtime) => {
                let limit = query
                    .get("limit")
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(50)
                    .clamp(1, 500);
                match runtime.config_revisions(limit).await {
                    Ok(revisions) => json!({"revisions": revisions}),
                    Err(_) => {
                        return Ok(response(
                            503,
                            json!({"code":"UNAVAILABLE","message":"config revisions unavailable"}),
                        ));
                    }
                }
            }
            None => {
                return Ok(response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"cluster runtime unavailable"}),
                ));
            }
        },
        path if path.starts_with("/admin/v1/operations/") => {
            let operation_id = &path["/admin/v1/operations/".len()..];
            let key = format!(
                "/rcaster/{}/runtime/operations/{operation_id}",
                state.cluster_id
            );
            match state.accounts.etcd_get(&key).await {
                Ok(Some((value, _))) => match serde_json::from_slice::<Value>(&value) {
                    Ok(operation) => operation,
                    Err(_) => {
                        return Ok(response(
                            503,
                            json!({"code":"UNAVAILABLE","message":"operation record is corrupt"}),
                        ));
                    }
                },
                Ok(None) => {
                    return Ok(response(
                        404,
                        json!({"code":"NOT_FOUND","message":"operation not found"}),
                    ));
                }
                Err(_) => {
                    return Ok(response(
                        503,
                        json!({"code":"UNAVAILABLE","message":"operation store unavailable"}),
                    ));
                }
            }
        }
        path if path.starts_with("/admin/v1/upgrades/") => {
            let operation_id = &path["/admin/v1/upgrades/".len()..];
            let Some(upgrades) = &state.upgrades else {
                return Ok(response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"upgrade controller unavailable"}),
                ));
            };
            match upgrades.get(operation_id).await {
                Ok(operation) => operation,
                Err(error) => {
                    return Ok(response(
                        error.status(),
                        json!({"code":"UPGRADE_REJECTED","message":error.to_string()}),
                    ));
                }
            }
        }
        "/admin/v1/versions" => {
            let nodes = match &state.runtime {
                Some(runtime) => runtime.node_versions().await.unwrap_or_default(),
                None => Vec::new(),
            };
            json!({
                "caster_version": env!("CARGO_PKG_VERSION"),
                "config_schema": 1,
                "local_caster_id": state.caster_id,
                "tls_certificate_expires_at": state.listener_tls.as_ref().and_then(ReloadableTlsConfig::certificate_expiry),
                "nodes": nodes,
            })
        }
        path if path.starts_with("/admin/v1/users/") && path.ends_with("/sessions") => {
            let user_id = &path["/admin/v1/users/".len()..path.len() - "/sessions".len()];
            let Some(runtime) = &state.runtime else {
                return Ok(response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"cluster runtime unavailable"}),
                ));
            };
            match runtime.sessions().await {
                Ok(sessions) => json!({
                    "sessions": sessions
                        .into_iter()
                        .filter(|session| session.user_id == user_id)
                        .collect::<Vec<_>>(),
                }),
                Err(_) => {
                    return Ok(response(
                        503,
                        json!({"code":"UNAVAILABLE","message":"session state unavailable"}),
                    ));
                }
            }
        }
        path if path.starts_with("/admin/v1/users/") => {
            let user_id = &path["/admin/v1/users/".len()..];
            let accounts = state.accounts.snapshot();
            match accounts.user_by_id(user_id) {
                Some(user) => json!({"user": user_view(user)}),
                None => {
                    return Ok(response(
                        404,
                        json!({"code":"NOT_FOUND","message":"user not found"}),
                    ));
                }
            }
        }
        path if path.starts_with("/admin/v1/mountpoints/") => {
            let mountpoint = &path["/admin/v1/mountpoints/".len()..];
            let accounts = state.accounts.snapshot();
            match accounts.mountpoint(mountpoint) {
                Some(mountpoint) => json!({"mountpoint": mountpoint_view(mountpoint)}),
                None => {
                    return Ok(response(
                        404,
                        json!({"code":"NOT_FOUND","message":"mountpoint not found"}),
                    ));
                }
            }
        }
        path if path.starts_with("/admin/v1/sources/") && !path.ends_with("/health") => {
            let mountpoint = &path["/admin/v1/sources/".len()..];
            match source(state, mountpoint).await {
                Some(record) => json!({"source": record}),
                None => {
                    return Ok(response(
                        404,
                        json!({"code":"NOT_FOUND","message":"source not found"}),
                    ));
                }
            }
        }
        path if path.starts_with("/admin/v1/streams/") && path.ends_with("/summary") => {
            let mountpoint = &path["/admin/v1/streams/".len()..path.len() - "/summary".len()];
            match stream_summary(state, mountpoint).await {
                Some(summary) => summary,
                None => {
                    return Ok(response(
                        404,
                        json!({"code":"NOT_FOUND","message":"stream not found"}),
                    ));
                }
            }
        }
        path if path.starts_with("/admin/v1/streams/") && path.ends_with("/history") => {
            let mountpoint = &path["/admin/v1/streams/".len()..path.len() - "/history".len()];
            if source(state, mountpoint).await.is_none() {
                return Ok(response(
                    404,
                    json!({"code":"NOT_FOUND","message":"stream not found"}),
                ));
            }
            let limit = query
                .get("limit")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(60)
                .clamp(1, 300);
            json!({
                "mountpoint": mountpoint,
                "points": state.hub.source_history(mountpoint, limit),
            })
        }
        path if path.starts_with("/admin/v1/sources/") && path.ends_with("/health") => {
            let mountpoint = &path["/admin/v1/sources/".len()..path.len() - "/health".len()];
            if let Some(profile) = state.hub.source_profile(mountpoint) {
                let health = state.hub.source_health(mountpoint);
                json!({
                    "mountpoint": mountpoint,
                    "profile": profile_view(&profile),
                    "health": health.map(|health| json!({
                        "packets": health.packets,
                        "bytes": health.bytes,
                        "last_packet_age_seconds": health.last_packet_age_seconds,
                        "bitrate_bps": health.bitrate_bps,
                        "stream_health_warning": health.warning_codes != 0,
                        "warning_codes": health.warning_codes,
                        "last_correction_age_seconds": health.last_correction_age_seconds,
                        "valid_frame_ratio": health.valid_frame_ratio,
                        "crc_error_ratio": health.crc_error_ratio,
                    })),
                })
            } else {
                let Some(runtime) = &state.runtime else {
                    return Ok(response(
                        404,
                        json!({"code":"NOT_FOUND","message":"source health not found"}),
                    ));
                };
                let Some(route) = runtime.source_route(mountpoint).await else {
                    return Ok(response(
                        404,
                        json!({"code":"NOT_FOUND","message":"source health not found"}),
                    ));
                };
                let Some(tls) = state.relay_tls.clone() else {
                    return Ok(response(
                        503,
                        json!({"code":"UNAVAILABLE","message":"remote health unavailable"}),
                    ));
                };
                let mut client = match RelayGrpcClient::connect_mtls(
                    format!("https://{}", route.relay_endpoint),
                    tls,
                )
                .await
                {
                    Ok(client) => client,
                    Err(_) => {
                        return Ok(response(
                            503,
                            json!({"code":"UNAVAILABLE","message":"owner unavailable"}),
                        ));
                    }
                };
                match client
                    .health(RelayHealthRequest {
                        cluster_id: state.cluster_id.clone(),
                        mountpoint: route.mountpoint,
                        source_generation: route.source_generation,
                        connection_id: route.connection_id,
                    })
                    .await
                {
                    Ok(health) => {
                        json!({"remote": true, "owner_caster_id": route.caster_id, "health": health})
                    }
                    Err(_) => {
                        return Ok(response(
                            503,
                            json!({"code":"UNAVAILABLE","message":"owner health unavailable"}),
                        ));
                    }
                }
            }
        }
        _ => {
            return Ok(response(
                404,
                json!({"code":"NOT_FOUND","message":"unknown admin route"}),
            ));
        }
    };
    Ok(response(200, value))
}

async fn serve_admin_websocket(
    stream: &mut tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    state: &AdminState,
    headers: &[&str],
    query: &BTreeMap<&str, &str>,
) -> io::Result<Vec<u8>> {
    if !websocket_origin_allowed(headers) {
        return Ok(response(
            403,
            json!({"code":"FORBIDDEN","message":"WebSocket origin mismatch"}),
        ));
    }
    let bearer = header(headers, "authorization")
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty());
    let access = if bearer.is_some_and(|token| {
        token.len() == state.token.len()
            && bool::from(token.as_bytes().ct_eq(state.token.as_bytes()))
    }) {
        Access::Service
    } else {
        let cookie = header(headers, "cookie").and_then(|value| {
            value.split(';').find_map(|part| {
                let (name, value) = part.trim().split_once('=')?;
                (name == ADMIN_SESSION_COOKIE).then_some(value)
            })
        });
        match admin_auth::authenticate(&state.accounts, bearer, cookie).await {
            Ok(identity) => Access::Admin(identity),
            Err(AdminAuthError::Invalid) => {
                return Ok(response(
                    401,
                    json!({"code":"UNAUTHENTICATED","message":"invalid administrator session"}),
                ));
            }
            Err(_) => {
                return Ok(response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"administrator authentication unavailable"}),
                ));
            }
        }
    };
    let mut web_cancel = state.connections.connection();
    if let Access::Admin(identity) = &access {
        web_cancel.bind(format!("web-user:{}", identity.user_id));
    }
    let Some(mountpoint) = query.get("mountpoint").copied() else {
        return Ok(response(
            400,
            json!({"code":"BAD_REQUEST","message":"mountpoint is required"}),
        ));
    };
    if source(state, mountpoint).await.is_none() {
        return Ok(response(
            404,
            json!({"code":"NOT_FOUND","message":"stream not found"}),
        ));
    }
    if state.hub.source_profile(mountpoint).is_none()
        && let Some(relays) = &state.relays
        && relays.ensure(mountpoint).await.is_err()
    {
        return Ok(response(
            503,
            json!({"code":"UNAVAILABLE","message":"stream monitor relay unavailable"}),
        ));
    }
    let actor = access_actor(&access).to_owned();
    let Some(mut slot) = state.websockets.acquire(&actor, mountpoint) else {
        return Ok(response_with_headers(
            429,
            json!({"code":"WS_LIMIT","message":"administrator WebSocket limit reached"}),
            &[("Retry-After", "5".to_owned())],
        ));
    };
    let Some(key) = header(headers, "sec-websocket-key") else {
        return Ok(response(
            400,
            json!({"code":"BAD_REQUEST","message":"Sec-WebSocket-Key required"}),
        ));
    };
    if header(headers, "sec-websocket-version") != Some("13") {
        return Ok(response(
            400,
            json!({"code":"BAD_REQUEST","message":"WebSocket version 13 required"}),
        ));
    }
    stream
        .write_all(
            format!(
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
                derive_accept_key(key.as_bytes())
            )
            .as_bytes(),
        )
        .await?;
    let config = WebSocketConfig::default()
        .max_message_size(Some(64 * 1024))
        .max_frame_size(Some(64 * 1024));
    let mut websocket = WebSocketStream::from_raw_socket(stream, Role::Server, Some(config)).await;
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let ready = json!({
        "type": "monitor_ready",
        "schema_version": 1,
        "mountpoint": mountpoint,
        "actor": actor,
    });
    if !matches!(
        timeout(
            Duration::from_secs(2),
            websocket.send(Message::Text(ready.to_string().into())),
        )
        .await,
        Ok(Ok(()))
    ) {
        return Ok(Vec::new());
    }
    loop {
        tokio::select! {
            _ = web_cancel.cancelled() => {
                let _ = websocket
                    .send(Message::Text(
                        json!({"type":"monitor_closed","mountpoint":mountpoint,"reason":"AUTH_CHANGED"})
                            .to_string()
                            .into(),
                    ))
                    .await;
                let _ = websocket.close(None).await;
                break;
            }
            _ = slot.cancelled() => {
                let _ = websocket
                    .send(Message::Text(
                        json!({"type":"monitor_closed","mountpoint":mountpoint,"reason":"ADMIN_CANCELED"})
                            .to_string()
                            .into(),
                    ))
                    .await;
                let _ = websocket.close(None).await;
                break;
            }
            _ = interval.tick() => {
                if let Access::Admin(identity) = &access
                    && !admin_auth::still_authorized(&state.accounts, identity).await
                {
                    let _ = websocket
                        .send(Message::Text(
                            json!({"type":"monitor_closed","mountpoint":mountpoint,"reason":"AUTH_CHANGED"})
                                .to_string()
                                .into(),
                        ))
                        .await;
                    let _ = websocket.close(None).await;
                    break;
                }
                let Some(summary) = stream_summary(state, mountpoint).await else {
                    let _ = websocket
                        .send(Message::Text(
                            json!({"type":"monitor_closed","mountpoint":mountpoint,"reason":"SOURCE_CHANGED"})
                                .to_string()
                                .into(),
                        ))
                        .await;
                    let _ = websocket.close(None).await;
                    break;
                };
                let message = json!({
                    "type": "monitor_snapshot",
                    "schema_version": 1,
                    "mountpoint": mountpoint,
                    "timestamp": now_timestamp(),
                    "snapshot": summary,
                });
                if !matches!(
                    timeout(
                        Duration::from_secs(2),
                        websocket.send(Message::Text(message.to_string().into())),
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
    Ok(Vec::new())
}

const ADMIN_MAX_HEAD_BYTES: usize = 16 * 1024;
const ADMIN_MAX_BODY_BYTES: usize = 64 * 1024;

async fn read_admin_request(
    stream: &mut tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
) -> Result<Vec<u8>, u16> {
    timeout(Duration::from_secs(5), async {
        let mut request = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            if let Some(head_end) = find_bytes(&request, b"\r\n\r\n") {
                if head_end > ADMIN_MAX_HEAD_BYTES {
                    return Err(431);
                }
                let head = std::str::from_utf8(&request[..head_end]).map_err(|_| 400_u16)?;
                let content_length = header_lines(head)
                    .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .map(|(_, value)| value.trim().parse::<usize>().map_err(|_| 400_u16))
                    .collect::<Result<Vec<_>, _>>()?;
                if content_length.len() > 1 {
                    return Err(400);
                }
                let content_length = content_length.first().copied().unwrap_or_default();
                if content_length > ADMIN_MAX_BODY_BYTES {
                    return Err(413);
                }
                let body_start = head_end + 4;
                if request.len() >= body_start.saturating_add(content_length) {
                    request.truncate(body_start + content_length);
                    return Ok(request);
                }
            } else if request.len() > ADMIN_MAX_HEAD_BYTES {
                return Err(431);
            }
            if request.len() > ADMIN_MAX_HEAD_BYTES + ADMIN_MAX_BODY_BYTES {
                return Err(413);
            }
            let read = stream.read(&mut chunk).await.map_err(|_| 400_u16)?;
            if read == 0 {
                return Err(400);
            }
            request.extend_from_slice(&chunk[..read]);
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

fn header_lines(head: &str) -> impl Iterator<Item = (&str, &str)> {
    head.lines().skip(1).filter_map(|line| line.split_once(':'))
}

fn profile_view(profile: &rcaster_core::protocol::DataProfile) -> Value {
    let integrity_attempts = profile.valid_frames.saturating_add(profile.crc_errors);
    let crc_ratio =
        (integrity_attempts > 0).then_some(profile.crc_errors as f64 / integrity_attempts as f64);
    json!({
        "total_bytes": profile.total_bytes,
        "unknown_bytes": profile.unknown_bytes,
        "valid_frames": profile.valid_frames,
        "crc_errors": profile.crc_errors,
        "crc_error_ratio": crc_ratio,
        "nmea_lines": profile.nmea_lines,
        "message_numbers": profile.message_numbers.iter().copied().collect::<Vec<_>>(),
        "msm_versions": profile.msm_versions.iter().copied().collect::<Vec<_>>(),
        "satellite_count": profile.satellite_count,
        "signal_count": profile.signal_count,
        "satellite_ids": profile.satellite_ids.iter().copied().collect::<Vec<_>>(),
        "signal_ids": profile.signal_ids.iter().copied().collect::<Vec<_>>(),
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
        "has_correction_data": profile.has_correction_data,
        "position_available": profile.position.is_some(),
        "device_available": profile.device.is_some(),
    })
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

fn user_view(user: &rcaster_core::account::User) -> Value {
    json!({
        "user_id": user.user_id,
        "username": user.username,
        "mod_revision": user.mod_revision,
        "auth_version": user.auth_version,
        "enabled": user.enabled,
        "ntrip_enabled": user.ntrip_enabled,
        "web_enabled": user.web_enabled,
        "role": user.role,
        "created_at": user.created_at,
        "updated_at": user.updated_at,
    })
}

fn mountpoint_view(mountpoint: &rcaster_core::account::Mountpoint) -> Value {
    json!({
        "mountpoint": mountpoint.mountpoint,
        "owner_user_id": mountpoint.owner_user_id,
        "mod_revision": mountpoint.mod_revision,
        "enabled": mountpoint.enabled,
        "advertise": mountpoint.advertise,
        "region": mountpoint.region,
        "latitude": mountpoint.latitude,
        "longitude": mountpoint.longitude,
        "coverage_radius_m": mountpoint.coverage_radius_m,
        "str_fields": mountpoint.str_fields,
        "source_expectation": mountpoint.source_expectation,
        "created_at": mountpoint.created_at,
        "updated_at": mountpoint.updated_at,
    })
}

fn parse_optional_number(body: &Value, key: &str) -> Result<Option<f64>, ()> {
    match body.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_f64().map(Some).ok_or(()),
    }
}

fn parse_optional_str_fields(body: &Value) -> Result<Option<MountpointStrConfig>, ()> {
    match body.get("str_fields") {
        None | Some(Value::Null) => Ok(None),
        Some(value) => serde_json::from_value(value.clone())
            .map(Some)
            .map_err(|_| ()),
    }
}

fn account_error_response(error: AccountDirectoryError) -> Vec<u8> {
    match error {
        AccountDirectoryError::Conflict => response(
            409,
            json!({"code":"CONFLICT","message":"account revision conflict"}),
        ),
        AccountDirectoryError::NotFound => response(
            404,
            json!({"code":"NOT_FOUND","message":"account record not found"}),
        ),
        AccountDirectoryError::InUse => response(
            409,
            json!({"code":"ACCOUNT_IN_USE","message":"account record is still in use"}),
        ),
        AccountDirectoryError::InvalidRecord | AccountDirectoryError::Account(_) => response(
            400,
            json!({"code":"BAD_REQUEST","message":"invalid account record"}),
        ),
        _ => response(
            503,
            json!({"code":"UNAVAILABLE","message":"account directory unavailable"}),
        ),
    }
}

async fn standalone_account_write(
    state: &AdminState,
    access: &Access,
    source_ip: Option<std::net::IpAddr>,
    method: &str,
    path: &str,
    headers: &[&str],
    body: &str,
) -> Option<Vec<u8>> {
    if method == "POST" && path == "/admin/v1/users" {
        let body = serde_json::from_str::<Value>(body).ok()?;
        let user_id = body.get("user_id")?.as_str()?.to_owned();
        let username = body.get("username")?.as_str()?.to_owned();
        let password = body.get("password")?.as_str()?.to_owned();
        let password_hash = tokio::task::spawn_blocking(move || hash_password(&password))
            .await
            .ok()?
            .ok()?;
        let role = match body.get("role").and_then(Value::as_str) {
            Some("admin") => UserRole::Admin,
            Some("user") | None => UserRole::User,
            Some(_) => {
                return Some(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid role"}),
                ));
            }
        };
        let now = now_timestamp();
        let user = User {
            schema_version: 1,
            mod_revision: 0,
            user_id: user_id.clone(),
            username,
            password_hash,
            auth_version: 1,
            enabled: body.get("enabled").and_then(Value::as_bool).unwrap_or(true),
            ntrip_enabled: body
                .get("ntrip_enabled")
                .and_then(Value::as_bool)
                .unwrap_or(true),
            web_enabled: body
                .get("web_enabled")
                .and_then(Value::as_bool)
                .unwrap_or(role == UserRole::Admin),
            role,
            created_at: now.clone(),
            updated_at: now,
        };
        return Some(match state.accounts.upsert_user(user, None).await {
            Ok(revision) => {
                audit(
                    state,
                    source_ip,
                    access_actor(access),
                    "user.create",
                    &user_id,
                    "success",
                    json!({"username": body["username"]}),
                );
                response(201, json!({"revision": revision}))
            }
            Err(error) => {
                audit(
                    state,
                    source_ip,
                    access_actor(access),
                    "user.create",
                    &user_id,
                    "failure",
                    json!({"error": error.to_string()}),
                );
                account_error_response(error)
            }
        });
    }
    if method == "PATCH" && path.starts_with("/admin/v1/users/") {
        let user_id = &path["/admin/v1/users/".len()..];
        let Some(revision) = header(headers, "if-match").and_then(parse_revision) else {
            return Some(response(
                428,
                json!({"code":"PRECONDITION_REQUIRED","message":"If-Match required"}),
            ));
        };
        let body = serde_json::from_str::<Value>(body).ok()?;
        let current = state.accounts.snapshot().user_by_id(user_id).cloned()?;
        let mut updated = current.clone();
        if body
            .get("username")
            .and_then(Value::as_str)
            .is_some_and(|username| username != current.username)
        {
            return Some(response(
                400,
                json!({"code":"BAD_REQUEST","message":"username cannot be changed"}),
            ));
        }
        if let Some(password) = body.get("password").and_then(Value::as_str) {
            let password = password.to_owned();
            let password_hash = tokio::task::spawn_blocking(move || hash_password(&password))
                .await
                .ok()?
                .ok()?;
            updated.password_hash = password_hash;
            updated.auth_version = updated.auth_version.saturating_add(1);
        }
        if let Some(role) = body.get("role").and_then(Value::as_str) {
            let role = match role {
                "admin" => UserRole::Admin,
                "user" => UserRole::User,
                _ => {
                    return Some(response(
                        400,
                        json!({"code":"BAD_REQUEST","message":"invalid role"}),
                    ));
                }
            };
            if role != updated.role {
                updated.role = role;
                updated.auth_version = updated.auth_version.saturating_add(1);
            }
        }
        if let Some(enabled) = body.get("enabled").and_then(Value::as_bool)
            && enabled != updated.enabled
        {
            updated.enabled = enabled;
            updated.auth_version = updated.auth_version.saturating_add(1);
        }
        if let Some(enabled) = body.get("ntrip_enabled").and_then(Value::as_bool) {
            updated.ntrip_enabled = enabled;
        }
        if let Some(enabled) = body.get("web_enabled").and_then(Value::as_bool)
            && updated.web_enabled != enabled
        {
            updated.web_enabled = enabled;
            updated.auth_version = updated.auth_version.saturating_add(1);
        }
        updated.updated_at = now_timestamp();
        return Some(
            match state.accounts.upsert_user(updated, Some(revision)).await {
                Ok(revision) => {
                    if body.get("enabled").and_then(Value::as_bool) == Some(false)
                        || body.get("ntrip_enabled").and_then(Value::as_bool) == Some(false)
                    {
                        state.connections.cancel_user(user_id);
                    }
                    if body.get("enabled").and_then(Value::as_bool) == Some(false)
                        || body.get("web_enabled").and_then(Value::as_bool) == Some(false)
                    {
                        state.connections.cancel_web_user(user_id);
                    }
                    audit(
                        state,
                        source_ip,
                        access_actor(access),
                        "user.update",
                        user_id,
                        "success",
                        json!({"revision": revision}),
                    );
                    response(200, json!({"revision": revision}))
                }
                Err(error) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(access),
                        "user.update",
                        user_id,
                        "failure",
                        json!({"error": error.to_string()}),
                    );
                    account_error_response(error)
                }
            },
        );
    }
    if method == "DELETE" && path.starts_with("/admin/v1/users/") {
        let user_id = &path["/admin/v1/users/".len()..];
        let Some(revision) = header(headers, "if-match").and_then(parse_revision) else {
            return Some(response(
                428,
                json!({"code":"PRECONDITION_REQUIRED","message":"If-Match required"}),
            ));
        };
        return Some(match state.accounts.delete_user(user_id, revision).await {
            Ok(()) => {
                state.connections.cancel_user(user_id);
                state.connections.cancel_web_user(user_id);
                audit(
                    state,
                    source_ip,
                    access_actor(access),
                    "user.delete",
                    user_id,
                    "success",
                    json!({}),
                );
                response(204, Value::Null)
            }
            Err(error) => {
                audit(
                    state,
                    source_ip,
                    access_actor(access),
                    "user.delete",
                    user_id,
                    "failure",
                    json!({"error": error.to_string()}),
                );
                account_error_response(error)
            }
        });
    }
    if method == "POST" && path == "/admin/v1/mountpoints" {
        let body = serde_json::from_str::<Value>(body).ok()?;
        let mountpoint_name = body.get("mountpoint")?.as_str()?.to_owned();
        let owner_user_id = body.get("owner_user_id")?.as_str()?.to_owned();
        let ntrip1_upload_password_hash =
            match body.get("ntrip1_upload_password").and_then(Value::as_str) {
                Some(password) if !password.is_empty() => {
                    let password = password.to_owned();
                    Some(
                        tokio::task::spawn_blocking(move || hash_password(&password))
                            .await
                            .ok()?
                            .ok()?,
                    )
                }
                _ => None,
            };
        let now = now_timestamp();
        let mountpoint = Mountpoint {
            schema_version: 1,
            mod_revision: 0,
            mountpoint: mountpoint_name.clone(),
            owner_user_id,
            enabled: body.get("enabled").and_then(Value::as_bool).unwrap_or(true),
            ntrip1_upload_password_hash,
            advertise: body
                .get("advertise")
                .and_then(Value::as_bool)
                .unwrap_or(true),
            region: body
                .get("region")
                .and_then(Value::as_str)
                .map(str::to_owned),
            latitude: parse_optional_number(&body, "latitude").ok()?,
            longitude: parse_optional_number(&body, "longitude").ok()?,
            coverage_radius_m: parse_optional_number(&body, "coverage_radius_m").ok()?,
            str_fields: parse_optional_str_fields(&body).ok()?,
            source_expectation: body
                .get("source_expectation")
                .filter(|value| !value.is_null())
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok()),
            created_at: now.clone(),
            updated_at: now,
        };
        return Some(
            match state.accounts.upsert_mountpoint(mountpoint, None).await {
                Ok(revision) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(access),
                        "mountpoint.create",
                        &mountpoint_name,
                        "success",
                        json!({}),
                    );
                    response(201, json!({"revision": revision}))
                }
                Err(error) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(access),
                        "mountpoint.create",
                        &mountpoint_name,
                        "failure",
                        json!({"error": error.to_string()}),
                    );
                    account_error_response(error)
                }
            },
        );
    }
    if method == "PATCH" && path.starts_with("/admin/v1/mountpoints/") {
        let mountpoint_name = &path["/admin/v1/mountpoints/".len()..];
        let Some(revision) = header(headers, "if-match").and_then(parse_revision) else {
            return Some(response(
                428,
                json!({"code":"PRECONDITION_REQUIRED","message":"If-Match required"}),
            ));
        };
        let body = serde_json::from_str::<Value>(body).ok()?;
        let mut updated = state
            .accounts
            .snapshot()
            .mountpoint(mountpoint_name)?
            .clone();
        let disabling = body.get("enabled").and_then(Value::as_bool) == Some(false);
        if !disabling && state.hub.source_profile(mountpoint_name).is_some() {
            return Some(response(
                409,
                json!({"code":"ACTIVE_SOURCE","message":"mountpoint has an active source"}),
            ));
        }
        if let Some(value) = body.get("mountpoint")
            && value.as_str() != Some(mountpoint_name)
        {
            return Some(response(
                400,
                json!({"code":"BAD_REQUEST","message":"mountpoint cannot be renamed"}),
            ));
        }
        if let Some(owner) = body.get("owner_user_id").and_then(Value::as_str) {
            updated.owner_user_id = owner.to_owned();
        }
        if let Some(enabled) = body.get("enabled").and_then(Value::as_bool) {
            updated.enabled = enabled;
        }
        if let Some(advertise) = body.get("advertise").and_then(Value::as_bool) {
            updated.advertise = advertise;
        }
        if let Some(region) = body.get("region") {
            updated.region = region.as_str().map(str::to_owned);
        }
        if body.get("latitude").is_some() {
            let Ok(latitude) = parse_optional_number(&body, "latitude") else {
                return Some(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid latitude"}),
                ));
            };
            updated.latitude = latitude;
        }
        if body.get("longitude").is_some() {
            let Ok(longitude) = parse_optional_number(&body, "longitude") else {
                return Some(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid longitude"}),
                ));
            };
            updated.longitude = longitude;
        }
        if body.get("coverage_radius_m").is_some() {
            let Ok(coverage_radius_m) = parse_optional_number(&body, "coverage_radius_m") else {
                return Some(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid coverage radius"}),
                ));
            };
            updated.coverage_radius_m = coverage_radius_m;
        }
        if body.get("str_fields").is_some() {
            let Ok(str_fields) = parse_optional_str_fields(&body) else {
                return Some(response(
                    400,
                    json!({"code":"BAD_REQUEST","message":"invalid STR fields"}),
                ));
            };
            updated.str_fields = str_fields;
        }
        if let Some(value) = body.get("source_expectation") {
            updated.source_expectation = if value.is_null() {
                None
            } else {
                match serde_json::from_value::<SourceExpectation>(value.clone()) {
                    Ok(expectation) => Some(expectation),
                    Err(_) => {
                        return Some(response(
                            400,
                            json!({"code":"BAD_REQUEST","message":"invalid source expectation"}),
                        ));
                    }
                }
            };
        }
        if let Some(password) = body
            .get("ntrip1_upload_password")
            .and_then(Value::as_str)
            .filter(|password| !password.is_empty())
        {
            let password = password.to_owned();
            let Ok(Ok(hash)) = tokio::task::spawn_blocking(move || hash_password(&password)).await
            else {
                return Some(response(
                    503,
                    json!({"code":"UNAVAILABLE","message":"password hashing unavailable"}),
                ));
            };
            updated.ntrip1_upload_password_hash = Some(hash);
        }
        if body
            .get("ntrip1_upload_password")
            .is_some_and(Value::is_null)
        {
            updated.ntrip1_upload_password_hash = None;
        }
        updated.updated_at = now_timestamp();
        return Some(
            match state
                .accounts
                .upsert_mountpoint(updated, Some(revision))
                .await
            {
                Ok(revision) => {
                    let disconnect_pending = disabling
                        && state.hub.source_profile(mountpoint_name).is_some()
                        && !state.connections.cancel_source(mountpoint_name);
                    audit(
                        state,
                        source_ip,
                        access_actor(access),
                        "mountpoint.update",
                        mountpoint_name,
                        if disconnect_pending {
                            "partial"
                        } else {
                            "success"
                        },
                        json!({
                            "revision": revision,
                            "disconnect_pending": disconnect_pending,
                        }),
                    );
                    response(
                        if disconnect_pending { 503 } else { 200 },
                        json!({
                            "revision": revision,
                            "disconnect_pending": disconnect_pending,
                        }),
                    )
                }
                Err(error) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(access),
                        "mountpoint.update",
                        mountpoint_name,
                        "failure",
                        json!({"error": error.to_string()}),
                    );
                    account_error_response(error)
                }
            },
        );
    }
    if method == "DELETE" && path.starts_with("/admin/v1/mountpoints/") {
        let mountpoint_name = &path["/admin/v1/mountpoints/".len()..];
        let Some(revision) = header(headers, "if-match").and_then(parse_revision) else {
            return Some(response(
                428,
                json!({"code":"PRECONDITION_REQUIRED","message":"If-Match required"}),
            ));
        };
        if state.hub.source_profile(mountpoint_name).is_some() {
            return Some(response(
                409,
                json!({"code":"ACTIVE_SOURCE","message":"mountpoint has an active source"}),
            ));
        }
        return Some(
            match state
                .accounts
                .delete_mountpoint(mountpoint_name, revision)
                .await
            {
                Ok(()) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(access),
                        "mountpoint.delete",
                        mountpoint_name,
                        "success",
                        json!({}),
                    );
                    response(204, Value::Null)
                }
                Err(error) => {
                    audit(
                        state,
                        source_ip,
                        access_actor(access),
                        "mountpoint.delete",
                        mountpoint_name,
                        "failure",
                        json!({"error": error.to_string()}),
                    );
                    account_error_response(error)
                }
            },
        );
    }
    None
}

fn blacklist_directory_error_response(error: AccountDirectoryError) -> Vec<u8> {
    match error {
        AccountDirectoryError::Blacklist(crate::blacklist::BlacklistError::Conflict) => response(
            409,
            json!({"code":"CONFLICT","message":"blacklist revision conflict"}),
        ),
        AccountDirectoryError::Blacklist(crate::blacklist::BlacklistError::NotFound) => response(
            404,
            json!({"code":"NOT_FOUND","message":"blacklist rule not found"}),
        ),
        AccountDirectoryError::Blacklist(crate::blacklist::BlacklistError::InvalidConfig) => {
            response(
                400,
                json!({"code":"BAD_REQUEST","message":"invalid blacklist rule"}),
            )
        }
        _ => response(
            503,
            json!({"code":"UNAVAILABLE","message":"blacklist store unavailable"}),
        ),
    }
}

fn now_timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

fn random_request_id() -> String {
    rand::random::<[u8; 16]>()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn audit(
    state: &AdminState,
    source_ip: Option<std::net::IpAddr>,
    actor: &str,
    action: &str,
    target: &str,
    result: &str,
    details: Value,
) {
    state.audit.record(AuditEvent {
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        request_id: REQUEST_ID
            .try_with(Clone::clone)
            .unwrap_or_else(|_| random_request_id()),
        actor: actor.to_owned(),
        source_ip: source_ip.map(|ip| ip.to_string()),
        action: action.to_owned(),
        target: target.to_owned(),
        result: result.to_owned(),
        details,
    });
}

fn access_actor(access: &Access) -> &str {
    match access {
        Access::Service => "service",
        Access::Admin(identity) => &identity.username,
    }
}

fn upgrade_actor(access: &Access) -> &str {
    match access {
        Access::Service => "service",
        Access::Admin(identity) => &identity.user_id,
    }
}

fn operation_cancel_allowed(access: &Access, operation: &Value) -> bool {
    let Access::Admin(identity) = access else {
        return false;
    };
    operation["created_by"].as_str() == Some(identity.user_id.as_str())
        || identity.scopes.iter().any(|scope| scope == "cluster:admin")
}

struct SourceEntry {
    record: rcaster_core::str_record::StrRecord,
    caster_id: String,
    region: Option<String>,
    admission: Option<SourceAdmissionSummary>,
}

async fn sources(state: &AdminState, query: &BTreeMap<&str, &str>) -> Result<Value, &'static str> {
    let primary_format = query
        .get("primary_format")
        .map(|value| value.to_ascii_lowercase());
    if primary_format.as_deref().is_some_and(|format| {
        !matches!(
            format,
            "rtcm3" | "rtcm2" | "binex" | "cmr" | "nmea" | "unknown"
        )
    }) {
        return Err("invalid primary_format");
    }
    let limit = query
        .get("limit")
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|_| "invalid limit")?
        .unwrap_or(100);
    if limit == 0 || limit > 500 {
        return Err("limit must be between 1 and 500");
    }
    let validation_result = query.get("validation_result").copied();
    if validation_result.is_some_and(|result| !matches!(result, "pass" | "warn" | "fail")) {
        return Err("invalid validation_result");
    }
    let expected_format_mismatch = match query.get("expected_format_mismatch").copied() {
        None => None,
        Some("true") => Some(true),
        Some("false") => Some(false),
        Some(_) => return Err("invalid expected_format_mismatch"),
    };
    let msm_version = query
        .get("msm_version")
        .map(|value| value.parse::<u8>())
        .transpose()
        .map_err(|_| "invalid msm_version")?;
    if msm_version.is_some_and(|version| !(1..=7).contains(&version)) {
        return Err("msm_version must be between 1 and 7");
    }
    let accounts = state.accounts.snapshot();
    let mut entries = state
        .hub
        .sourcetable_records()
        .into_iter()
        .map(|record| {
            let admission = state
                .hub
                .source_profile(&record.mountpoint)
                .map(|profile| SourceAdmissionSummary::from_profile(&profile, &[]));
            let entry = SourceEntry {
                region: accounts
                    .mountpoint(&record.mountpoint)
                    .and_then(|mountpoint| mountpoint.region.clone()),
                record,
                caster_id: state.caster_id.clone(),
                admission,
            };
            (entry.record.mountpoint.clone(), entry)
        })
        .collect::<BTreeMap<_, _>>();
    if let Some(runtime) = &state.runtime {
        for record in runtime.sourcetable_records().await {
            let route = runtime.source_route(&record.mountpoint).await;
            let caster_id = route
                .as_ref()
                .map(|route| route.caster_id.clone())
                .unwrap_or_default();
            let admission = route.and_then(|route| route.admission);
            let region = accounts
                .mountpoint(&record.mountpoint)
                .and_then(|mountpoint| mountpoint.region.clone());
            entries.insert(
                record.mountpoint.clone(),
                SourceEntry {
                    record,
                    caster_id,
                    region,
                    admission,
                },
            );
        }
    }
    let caster_id = query.get("caster_id").copied();
    let region = query.get("region").copied();
    let cursor = query.get("cursor").copied();
    let filtered = entries
        .into_values()
        .filter(|entry| {
            primary_format
                .as_deref()
                .is_none_or(|format| source_format(&entry.record.format) == format)
                && caster_id.is_none_or(|caster_id| entry.caster_id == caster_id)
                && region.is_none_or(|region| entry.region.as_deref() == Some(region))
                && validation_result.is_none_or(|result| {
                    entry
                        .admission
                        .as_ref()
                        .is_some_and(|admission| admission.validation_result == result)
                })
                && expected_format_mismatch.is_none_or(|expected| {
                    entry
                        .admission
                        .as_ref()
                        .is_some_and(|admission| admission.expected_format_mismatch == expected)
                })
                && msm_version.is_none_or(|version| {
                    entry
                        .admission
                        .as_ref()
                        .is_some_and(|admission| admission.msm_versions.contains(&version))
                })
                && cursor.is_none_or(|cursor| entry.record.mountpoint.as_str() > cursor)
        })
        .collect::<Vec<_>>();
    let next_cursor =
        (filtered.len() > limit).then(|| filtered[limit - 1].record.mountpoint.clone());
    let records = filtered
        .into_iter()
        .take(limit)
        .map(|entry| {
            let mut record = serde_json::to_value(entry.record).unwrap_or(Value::Null);
            if let Some(object) = record.as_object_mut() {
                object.insert("caster_id".to_owned(), Value::String(entry.caster_id));
                object.insert(
                    "region".to_owned(),
                    entry.region.map(Value::String).unwrap_or(Value::Null),
                );
                object.insert(
                    "admission".to_owned(),
                    serde_json::to_value(entry.admission).unwrap_or(Value::Null),
                );
            }
            record
        })
        .collect::<Vec<_>>();
    Ok(json!({"sources": records, "next_cursor": next_cursor}))
}

fn source_format(value: &str) -> &'static str {
    if value.starts_with("RTCM 3") {
        "rtcm3"
    } else if value.starts_with("RTCM 2") {
        "rtcm2"
    } else if value.starts_with("BINEX") {
        "binex"
    } else if value.starts_with("CMR") {
        "cmr"
    } else if value.starts_with("NMEA") {
        "nmea"
    } else {
        "unknown"
    }
}

fn parse_query(query: &str) -> BTreeMap<&str, &str> {
    query
        .split('&')
        .filter(|part| !part.is_empty())
        .filter_map(|part| part.split_once('='))
        .collect()
}

async fn source(
    state: &AdminState,
    mountpoint: &str,
) -> Option<rcaster_core::str_record::StrRecord> {
    if let Some(record) = state
        .hub
        .sourcetable_records()
        .into_iter()
        .find(|record| record.mountpoint == mountpoint)
    {
        return Some(record);
    }
    if let Some(runtime) = &state.runtime {
        return runtime
            .sourcetable_records()
            .await
            .into_iter()
            .find(|record| record.mountpoint == mountpoint);
    }
    None
}

async fn stream_summary(state: &AdminState, mountpoint: &str) -> Option<Value> {
    let record = source(state, mountpoint).await?;
    Some(json!({
        "mountpoint": mountpoint,
        "subscribers": state.hub.subscriber_count(mountpoint).unwrap_or_default(),
        "str": record,
        "profile": state.hub.source_profile(mountpoint).map(|profile| profile_view(&profile)),
        "health": state.hub.source_health(mountpoint).map(|health| json!({
            "packets": health.packets,
            "bytes": health.bytes,
            "last_packet_age_seconds": health.last_packet_age_seconds,
            "bitrate_bps": health.bitrate_bps,
            "stream_health_warning": health.warning_codes != 0,
            "warning_codes": health.warning_codes,
            "last_correction_age_seconds": health.last_correction_age_seconds,
            "valid_frame_ratio": health.valid_frame_ratio,
            "crc_error_ratio": health.crc_error_ratio,
        })),
    }))
}

async fn operation_preview(
    state: &AdminState,
    runtime: &SharedClusterRuntime,
    body: &Value,
) -> Result<Value, (u16, String)> {
    if body.get("action").and_then(Value::as_str) != Some("config.rollback") {
        let manager = OperationManager::new(
            runtime.clone(),
            state.accounts.clone(),
            state.cluster_id.clone(),
            state.caster_id.clone(),
            Some(Arc::clone(&state.ready)),
        );
        return manager.preview(body).await;
    }
    let Some(target_generation) = body.get("generation").and_then(Value::as_u64) else {
        return Err((400, "generation is required".to_owned()));
    };
    let Some(current) = runtime.config_snapshot().await else {
        return Err((503, "config unavailable".to_owned()));
    };
    let Some(target) = runtime
        .config_revisions(500)
        .await
        .ok()
        .and_then(|revisions| {
            revisions
                .into_iter()
                .find(|revision| revision.config.generation == target_generation)
        })
    else {
        return Err((404, "target config revision not found".to_owned()));
    };
    Ok(json!({
        "action": "config.rollback",
        "current_generation": current.config.generation,
        "target_generation": target_generation,
        "result_generation": current.config.generation.saturating_add(1),
        "target_config": target.config,
    }))
}

fn response(status: u16, body: Value) -> Vec<u8> {
    response_with_headers(status, body, &[])
}

fn response_with_headers(status: u16, body: Value, extra: &[(&str, String)]) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        409 => "Conflict",
        413 => "Content Too Large",
        429 => "Too Many Requests",
        428 => "Precondition Required",
        431 => "Request Header Fields Too Large",
        503 => "Service Unavailable",
        _ => "Error",
    };
    let request_id = REQUEST_ID.try_with(Clone::clone).ok();
    let body = if status == 204 {
        String::new()
    } else {
        let mut body = body;
        if status >= 400
            && let Some(object) = body.as_object_mut()
        {
            object.entry("details").or_insert(Value::Null);
            if let Some(request_id) = &request_id {
                object
                    .entry("request_id")
                    .or_insert_with(|| Value::String(request_id.clone()));
            }
        }
        body.to_string()
    };
    let mut response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nStrict-Transport-Security: max-age=31536000\r\nReferrer-Policy: no-referrer\r\nConnection: close\r\n",
        body.len()
    );
    if let Some(request_id) = request_id {
        response.push_str(&format!("X-Request-ID: {request_id}\r\n"));
    }
    for (name, value) in extra {
        response.push_str(&format!("{name}: {value}\r\n"));
    }
    response.push_str("\r\n");
    response.push_str(&body);
    response.into_bytes()
}

fn html_response(body: &str) -> Vec<u8> {
    let request_id = REQUEST_ID
        .try_with(Clone::clone)
        .map(|request_id| format!("X-Request-ID: {request_id}\r\n"))
        .unwrap_or_default();
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-store\r\nContent-Security-Policy: default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'\r\nX-Content-Type-Options: nosniff\r\nStrict-Transport-Security: max-age=31536000\r\nReferrer-Policy: no-referrer\r\n{request_id}Connection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

const ADMIN_HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>RCaster Admin</title>
<style>body{font:15px system-ui;margin:0;background:#f5f6f8;color:#17202a}header{background:#17202a;color:white;padding:16px 24px}main{max-width:1100px;margin:24px auto;padding:0 18px}section{background:white;border:1px solid #d9dee5;border-radius:8px;padding:18px;margin-bottom:16px}input{width:360px;max-width:70%;padding:8px}textarea{width:100%;min-height:260px;font:13px ui-monospace;box-sizing:border-box}button{padding:8px 14px;margin-left:8px}pre{overflow:auto;white-space:pre-wrap}table{border-collapse:collapse;width:100%}td,th{border-bottom:1px solid #e5e7eb;padding:7px;text-align:left}canvas{width:100%;height:auto;border:1px solid #d9dee5}</style></head>
<body><header><strong>RCaster Admin</strong></header><main>
<section id="login"><h3>Administrator login</h3><label>Username <input id="username" autocomplete="username"></label><label>Password <input id="password" type="password" autocomplete="current-password"></label><button onclick="login()">Sign in</button><p id="status">Sign in to continue.</p></section>
<section id="account" hidden><p id="account-name"></p><p id="read-only" hidden><strong>Read-only service session.</strong> Mutations are disabled.</p><button id="logout" onclick="logout()">Sign out</button></section>
<section><h3>Cluster</h3><pre id="cluster">-</pre><h3>Source filters</h3><p><input id="source-format" aria-label="Source format" placeholder="format"><input id="source-validation" aria-label="Validation result" placeholder="pass/warn/fail"><input id="source-caster" aria-label="Caster ID" placeholder="caster id"><select id="source-mismatch" aria-label="Expected format mismatch"><option value="">any mismatch</option><option value="true">mismatch</option><option value="false">match</option></select><input id="source-msm" aria-label="MSM version" type="number" min="1" max="7" placeholder="MSM"><button onclick="loadSources()">Apply</button></p><h3>Sources</h3><table><thead><tr><th>Mountpoint</th><th>Format</th><th>Details</th><th>Caster</th><th>Validation</th><th>MSM</th><th>Action</th></tr></thead><tbody id="sources"></tbody></table></section>
<section><h3>Source health</h3><pre id="source-health">Select Health or History on a source.</pre><canvas id="history-chart" width="1000" height="180" aria-label="Source history chart"></canvas></section>
<section><h3>Relays</h3><table><thead><tr><th>Mountpoint</th><th>Subscribers</th></tr></thead><tbody id="relays"></tbody></table></section>
<section><h3>Users</h3><p class="write-control"><input id="user-id" aria-label="User ID" placeholder="user id"><input id="user-name" aria-label="Username" placeholder="username"><input id="user-password" aria-label="Password" type="password" placeholder="password (blank to keep)"><select id="user-role" aria-label="Role"><option value="user">user</option><option value="admin">admin</option></select><label><input id="user-enabled" type="checkbox" checked> enabled</label><label><input id="user-ntrip-enabled" type="checkbox" checked> NTRIP</label><label><input id="user-web-enabled" type="checkbox"> Web</label><button onclick="saveUser()">Save user</button></p><table><thead><tr><th>Username</th><th>Role</th><th>Enabled</th><th>NTRIP</th><th>Web</th><th>Action</th></tr></thead><tbody id="users"></tbody></table></section>
<section><h3>Mountpoints</h3><p class="write-control"><input id="mount-name" aria-label="Mountpoint" placeholder="mountpoint"><input id="mount-owner" aria-label="Owner user ID" placeholder="owner user id"><input id="mount-password" aria-label="NTRIP1 upload password" type="password" placeholder="upload password (blank to keep)"><input id="mount-region" aria-label="Region" placeholder="region"><input id="mount-latitude" aria-label="Latitude" type="number" step="any" placeholder="latitude"><input id="mount-longitude" aria-label="Longitude" type="number" step="any" placeholder="longitude"><input id="mount-radius" aria-label="Coverage radius (m)" type="number" min="0" step="any" placeholder="coverage radius (m)"><input id="mount-str-fields" aria-label="STR fields JSON" placeholder="STR fields JSON"><input id="mount-source-expectation" aria-label="Source expectation JSON" placeholder="source expectation JSON"><label><input id="mount-enabled" type="checkbox" checked> enabled</label><label><input id="mount-advertise" type="checkbox" checked> advertise</label><button onclick="saveMount()">Save mountpoint</button></p><table><thead><tr><th>Mountpoint</th><th>Owner</th><th>Enabled</th><th>Region</th><th>Latitude</th><th>Longitude</th><th>Radius (m)</th><th>Action</th></tr></thead><tbody id="mountpoints"></tbody></table></section>
<section id="api-tokens-section"><h3>API tokens</h3><p><input id="api-token-name" aria-label="Token name" placeholder="token name"><select id="api-token-scope" aria-label="Scope"><option value="read">read only</option><option value="write" selected>read + write</option></select><input id="api-token-expiry" aria-label="Expiry seconds" type="number" min="0" placeholder="expiry seconds (optional)"><button onclick="createApiToken()">Create token</button></p><pre id="api-token-secret"></pre><table><thead><tr><th>ID</th><th>Name</th><th>Owner</th><th>Scopes</th><th>Expires</th><th>Action</th></tr></thead><tbody id="api-tokens"></tbody></table></section>
<section><h3>Blacklist</h3><p class="write-control"><input id="bl-id" aria-label="Rule ID" placeholder="rule id"><input id="bl-cidr" aria-label="CIDR" placeholder="CIDR"><input id="bl-quota" aria-label="Quota" type="number" value="0" style="width:90px"><input id="bl-description" aria-label="Description" placeholder="description"><button onclick="addBlacklist()">Add rule</button></p><table><thead><tr><th>ID</th><th>CIDR</th><th>Quota</th><th>Enabled</th></tr></thead><tbody id="blacklist"></tbody></table></section>
<section><h3>Sessions</h3><table><thead><tr><th>Type</th><th>User</th><th>Mountpoint</th><th>Caster</th></tr></thead><tbody id="sessions"></tbody></table></section>
<section><h3>Source cooldowns</h3><table><thead><tr><th>Mountpoint</th><th>Source IP</th><th>Reason</th><th>Expires</th><th>Action</th></tr></thead><tbody id="cooldowns"></tbody></table></section>
<section><h3>Source events</h3><table><thead><tr><th>Timestamp</th><th>Mountpoint</th><th>Event</th><th>Reason</th><th>Caster</th></tr></thead><tbody id="source-events"></tbody></table></section>
<section><h3>NEAR clients</h3><table><thead><tr><th>Type</th><th>User</th><th>Mountpoint</th><th>Current source</th><th>Caster</th></tr></thead><tbody id="near-clients"></tbody></table></section>
<section><h3>Maintenance</h3><pre id="maintenance">-</pre><button class="write-control" onclick="setMaintenance(true)">Enter maintenance</button><button class="write-control" onclick="setMaintenance(false)">Leave maintenance</button></section>
<section><h3>Configuration</h3><pre id="config-status">-</pre><textarea id="config-json" aria-label="Caster configuration JSON" spellcheck="false"></textarea><p class="write-control"><button onclick="saveConfig()">Apply config</button><button onclick="reloadConfig()">Reload config</button></p><table><thead><tr><th>Generation</th><th>Revision</th><th>Source health</th></tr></thead><tbody id="config-revisions"></tbody></table></section>
<section><h3>Versions</h3><pre id="versions-summary">-</pre><table><thead><tr><th>Caster</th><th>Version</th><th>Healthy</th><th>Maintenance</th><th>Started</th></tr></thead><tbody id="versions"></tbody></table></section>
<section><h3>Operation lookup</h3><p><input id="operation-id" aria-label="Operation ID" placeholder="operation id"><button onclick="loadOperation()">Load operation</button></p><pre id="operation">-</pre></section>
<section class="write-control"><h3>Batch operations</h3><select id="batch-action" aria-label="Batch action"><option value="users.disable">Disable users</option><option value="sessions.drop">Drop sessions</option></select><textarea id="batch-targets" aria-label="Batch targets" placeholder="one user or session ID per line"></textarea><p><button onclick="previewBatchDisable()">Preview</button><button id="batch-disable-start" onclick="startBatchDisable()" hidden>Start operation</button></p><pre id="batch-operation">-</pre></section>
<section><h3>Upgrade lookup</h3><p><input id="upgrade-id" aria-label="Upgrade ID" placeholder="upgrade id"><button onclick="loadUpgrade()">Load upgrade</button></p><pre id="upgrade">-</pre></section>
<section><h3>Audit</h3><table><thead><tr><th>Timestamp</th><th>Actor</th><th>Action</th><th>Target</th><th>Result</th></tr></thead><tbody id="audit"></tbody></table></section>
</main><script>
const status=document.getElementById('status');
let blacklistRevision=null;
let userRevision=null,mountRevision=null;
let csrfToken='';
let monitorSocket=null;
let serviceReadOnly=false;
let batchDisableDigest='';
const busyActions=new Set();
let requestScope=null;
let requestSignal=null;
let loggingOut=false;
function beginRequestScope(){requestScope?.abort();const controller=new AbortController();requestScope=controller;requestSignal=controller.signal;return controller}
function endRequestScope(controller){if(requestScope===controller)requestScope=null;if(requestSignal===controller.signal)requestSignal=null}
async function api(path,options={}){const method=options.method||'GET';if(serviceReadOnly&&method!=='GET')throw new Error('Read-only service session');const headers={'Content-Type':'application/json',...(options.headers||{})};if(method!=='GET')headers['X-CSRF-Token']=csrfToken;const signal=method==='GET'&&options.signal===undefined?requestSignal:options.signal;const r=await fetch(path,{...options,headers:headers,signal:signal});if(!r.ok)throw new Error(await r.text());return r.status===204?null:r.json();}
async function guarded(key,action){if(busyActions.has(key))return null;busyActions.add(key);try{return await action()}finally{busyActions.delete(key)}}
function row(cells){const tr=document.createElement('tr');for(const value of cells){const td=document.createElement('td');td.textContent=value??'';tr.append(td)}return tr}
async function login(){try{const result=await api('/admin/v1/auth/login',{method:'POST',body:JSON.stringify({username:document.getElementById('username').value,password:document.getElementById('password').value})});csrfToken=result.csrf_token;document.getElementById('login').hidden=true;document.getElementById('account').hidden=false;document.getElementById('account-name').textContent=result.username;await load();}catch(e){status.textContent=String(e)}}
async function logout(){if(loggingOut)return;loggingOut=true;const controller=beginRequestScope();try{await api('/admin/v1/auth/logout',{method:'POST'});csrfToken='';document.getElementById('login').hidden=false;document.getElementById('account').hidden=true;status.textContent='Signed out';}catch(e){status.textContent=String(e)}finally{controller.abort();endRequestScope(controller);loggingOut=false}}
async function load(){if(loggingOut)return;const controller=beginRequestScope();try{status.textContent='Loading...';const session=await api('/admin/v1/auth/session');const service=session.kind==='service';serviceReadOnly=service;csrfToken=service?'':((await api('/admin/v1/auth/csrf')).csrf_token||'');document.getElementById('login').hidden=true;document.getElementById('account').hidden=false;document.getElementById('account-name').textContent=session.username||session.kind;document.getElementById('read-only').hidden=!service;document.getElementById('logout').hidden=service;document.querySelectorAll('.write-control').forEach(element=>element.hidden=service);document.getElementById('api-tokens-section').hidden=service;document.getElementById('config-json').readOnly=service;const c=await api('/admin/v1/cluster/status');document.getElementById('cluster').textContent=JSON.stringify(c,null,2);await loadSources();const r=await api('/admin/v1/relays');document.getElementById('relays').replaceChildren(...r.relays.map(x=>row([x.mountpoint,x.subscribers])));const u=await api('/admin/v1/users');document.getElementById('users').replaceChildren(...u.users.map(userRow));const m=await api('/admin/v1/mountpoints');document.getElementById('mountpoints').replaceChildren(...m.mountpoints.map(mountRow));const b=await api('/admin/v1/blacklist');document.getElementById('blacklist').replaceChildren(...b.rules.map(blacklistRow));const ss=await api('/admin/v1/sessions');document.getElementById('sessions').replaceChildren(...ss.sessions.map(sessionRow));const cd=await api('/admin/v1/source-cooldowns');document.getElementById('cooldowns').replaceChildren(...cd.cooldowns.map(cooldownRow));await loadOperationalViews();if(!service){const at=await api('/admin/v1/api-tokens');document.getElementById('api-tokens').replaceChildren(...at.tokens.map(apiTokenRow));}status.textContent=service?'Connected (service read-only)':'Connected';}catch(e){if(controller.signal.aborted)return;document.getElementById('login').hidden=false;document.getElementById('account').hidden=true;status.textContent=String(e)}finally{endRequestScope(controller)}}
async function loadSources(){const fields=[['primary_format','source-format'],['validation_result','source-validation'],['caster_id','source-caster'],['expected_format_mismatch','source-mismatch'],['msm_version','source-msm']],sources=[];let cursor='';do{const params=new URLSearchParams({limit:'500'});for(const [name,id] of fields){const value=document.getElementById(id).value.trim();if(value)params.set(name,value)}if(cursor)params.set('cursor',cursor);const result=await api('/admin/v1/sources?'+params);sources.push(...result.sources);cursor=result.next_cursor||''}while(cursor);document.getElementById('sources').replaceChildren(...sources.map(sourceRow))}
function sourceRow(source){const admission=source.admission||{};const tr=row([source.mountpoint,source.format,source.format_details,source.caster_id,admission.validation_result||'',(admission.msm_versions||[]).join(',')]);const td=document.createElement('td');const health=document.createElement('button');health.textContent='Health';health.onclick=()=>showSourceHealth(source.mountpoint);const history=document.createElement('button');history.textContent='History';history.onclick=()=>showSourceHistory(source.mountpoint);const live=document.createElement('button');live.textContent='Live';live.onclick=()=>monitorSource(source.mountpoint);td.append(health,history,live);if(!serviceReadOnly){const disconnect=document.createElement('button');disconnect.textContent='Disconnect';disconnect.onclick=()=>disconnectSource(source.mountpoint);td.append(disconnect)}tr.append(td);return tr}
async function showSourceHistory(mountpoint){try{const result=await api('/admin/v1/streams/'+encodeURIComponent(mountpoint)+'/history?limit=120');document.getElementById('source-health').textContent=JSON.stringify(result,null,2);drawHistory(result.points||[]);status.textContent='History loaded for '+mountpoint}catch(e){status.textContent=String(e)}}
function drawHistory(points){const canvas=document.getElementById('history-chart');const ctx=canvas.getContext('2d');ctx.clearRect(0,0,canvas.width,canvas.height);ctx.fillStyle='#f8fafc';ctx.fillRect(0,0,canvas.width,canvas.height);if(!points.length){ctx.fillStyle='#475569';ctx.fillText('No history points',12,24);return}const values=points.map(point=>point.bitrate_bps||0);const max=Math.max(1,...values);ctx.strokeStyle='#2563eb';ctx.lineWidth=2;ctx.beginPath();values.forEach((value,index)=>{const x=12+(canvas.width-24)*index/Math.max(1,values.length-1);const y=canvas.height-12-(canvas.height-24)*value/max;if(index===0)ctx.moveTo(x,y);else ctx.lineTo(x,y)});ctx.stroke();ctx.fillStyle='#475569';ctx.fillText('bitrate max '+max+' bps',12,16)}
async function loadOperationalViews(){const near=await api('/admin/v1/near/clients');document.getElementById('near-clients').replaceChildren(...near.clients.map(client=>row([client.session_type,client.user_id,client.mountpoint,client.near_current_mountpoint,client.caster_id])));const maintenance=await api('/admin/v1/maintenance');document.getElementById('maintenance').textContent=JSON.stringify(maintenance,null,2);const configStatus=await api('/admin/v1/config/status');document.getElementById('config-status').textContent=JSON.stringify(configStatus,null,2);if(configStatus.available&&document.activeElement!==document.getElementById('config-json')){const raw=await api('/admin/v1/config');document.getElementById('config-json').value=JSON.stringify(raw.config,null,2)}try{const revisions=await api('/admin/v1/config/revisions?limit=20');document.getElementById('config-revisions').replaceChildren(...revisions.revisions.map(configRevisionRow))}catch{document.getElementById('config-revisions').replaceChildren(row(['-','-','Config history unavailable']))}const versions=await api('/admin/v1/versions');document.getElementById('versions-summary').textContent=JSON.stringify({caster_version:versions.caster_version,config_schema:versions.config_schema,local_caster_id:versions.local_caster_id,tls_certificate_expires_at:versions.tls_certificate_expires_at},null,2);document.getElementById('versions').replaceChildren(...versions.nodes.map(versionRow));const sourceEvents=await api('/admin/v1/source-events?limit=50');document.getElementById('source-events').replaceChildren(...sourceEvents.events.map(event=>row([event.timestamp,event.mountpoint,event.event,event.reason??'',event.caster_id??''])));const audit=await api('/admin/v1/audit?limit=50');document.getElementById('audit').replaceChildren(...audit.events.map(event=>row([event.timestamp,event.actor,event.action,event.target,event.result])))}
function configRevisionRow(revision){return row([revision.config.generation,revision.revision,JSON.stringify(revision.config.source_health)])}
function versionRow(version){return row([version.caster_id,version.software_version,version.healthy,version.maintenance,version.started_at])}
async function setMaintenance(enabled){try{await api('/admin/v1/maintenance',{method:'POST',body:JSON.stringify({maintenance:enabled})});status.textContent='Maintenance '+(enabled?'enabled':'disabled');await load()}catch(e){status.textContent=String(e)}}
async function reloadConfig(){try{await api('/admin/v1/config/reload',{method:'POST'});status.textContent='Config reload accepted';await load()}catch(e){status.textContent=String(e)}}
async function saveConfig(){try{const config=JSON.parse(document.getElementById('config-json').value);config.generation=Math.max(Number(config.generation||0)+1,1);await api('/admin/v1/config/apply',{method:'POST',body:JSON.stringify(config)});status.textContent='Config applied';await load()}catch(e){status.textContent=String(e)}}
async function loadOperation(){const id=document.getElementById('operation-id').value.trim();if(!id)return;try{document.getElementById('operation').textContent=JSON.stringify(await api('/admin/v1/operations/'+encodeURIComponent(id)),null,2)}catch(e){document.getElementById('operation').textContent=String(e)}}
function batchTargets(){return [...new Set(document.getElementById('batch-targets').value.split(/[\s,]+/).map(value=>value.trim()).filter(Boolean))]}
function batchAction(){return document.getElementById('batch-action').value}
async function previewBatchDisable(){try{const targets=batchTargets();if(!targets.length)throw new Error('Enter at least one target ID');const result=await api('/admin/v1/operations/preview',{method:'POST',body:JSON.stringify({action:batchAction(),targets})});batchDisableDigest=result.preview_digest;document.getElementById('batch-operation').textContent=JSON.stringify(result,null,2);document.getElementById('batch-disable-start').hidden=false;status.textContent='Batch operation preview ready';}catch(e){batchDisableDigest='';document.getElementById('batch-disable-start').hidden=true;document.getElementById('batch-operation').textContent=String(e)}}
async function startBatchDisable(){try{if(!batchDisableDigest)throw new Error('Preview the operation first');const result=await api('/admin/v1/operations',{method:'POST',body:JSON.stringify({action:batchAction(),targets:batchTargets(),preview_digest:batchDisableDigest})});document.getElementById('operation-id').value=result.operation_id;document.getElementById('batch-operation').textContent=JSON.stringify(result,null,2);document.getElementById('batch-disable-start').hidden=true;status.textContent='Batch operation queued';}catch(e){document.getElementById('batch-operation').textContent=String(e)}}
async function loadUpgrade(){const id=document.getElementById('upgrade-id').value.trim();if(!id)return;try{document.getElementById('upgrade').textContent=JSON.stringify(await api('/admin/v1/upgrades/'+encodeURIComponent(id)),null,2)}catch(e){document.getElementById('upgrade').textContent=String(e)}}
function sessionRow(session){const tr=row([session.session_type,session.user_id,session.mountpoint,session.caster_id]);const td=document.createElement('td');if(serviceReadOnly){td.textContent='Read-only'}else{const disconnect=document.createElement('button');disconnect.textContent='Disconnect';disconnect.onclick=()=>disconnectSession(session.session_id);td.append(disconnect)}tr.append(td);return tr}
function cooldownRow(cooldown){const tr=row([cooldown.mountpoint,cooldown.source_ip,cooldown.reason,cooldown.expires_at]);const td=document.createElement('td');if(serviceReadOnly){td.textContent='Read-only'}else{const clear=document.createElement('button');clear.textContent='Clear';clear.onclick=()=>clearCooldown(cooldown.fingerprint);td.append(clear)}tr.append(td);return tr}
async function disconnectSource(mountpoint){if(!confirm('Disconnect source '+mountpoint+'?'))return;try{await api('/admin/v1/sources/'+encodeURIComponent(mountpoint),{method:'DELETE'});status.textContent='Disconnect accepted for '+mountpoint;}catch(e){status.textContent=String(e)}}
async function disconnectSession(sessionId){if(!confirm('Disconnect session '+sessionId+'?'))return;try{await api('/admin/v1/sessions/'+encodeURIComponent(sessionId),{method:'DELETE'});status.textContent='Disconnect accepted for '+sessionId;}catch(e){status.textContent=String(e)}}
async function clearCooldown(fingerprint){if(!confirm('Clear source cooldown?'))return;try{await api('/admin/v1/source-cooldowns/'+encodeURIComponent(fingerprint),{method:'DELETE'});status.textContent='Source cooldown cleared';await load();}catch(e){status.textContent=String(e)}}
function monitorSource(mountpoint){if(monitorSocket)monitorSocket.close();const target=document.getElementById('source-health');const scheme=location.protocol==='https:'?'wss':'ws';const socket=new WebSocket(scheme+'://'+location.host+'/admin/v1/ws?mountpoint='+encodeURIComponent(mountpoint));monitorSocket=socket;status.textContent='Live monitor connecting for '+mountpoint;target.textContent='Waiting for monitor_ready...';socket.onmessage=event=>{try{const message=JSON.parse(event.data);if(message.type==='monitor_snapshot')status.textContent='Live: '+mountpoint;target.textContent=JSON.stringify(message,null,2);}catch(e){target.textContent=String(e)}};socket.onerror=()=>{status.textContent='Live monitor failed';};socket.onclose=()=>{if(monitorSocket===socket)monitorSocket=null;if(status.textContent.startsWith('Live'))status.textContent='Live monitor closed';};}
function userRow(user){const tr=row([user.username,user.role,user.enabled,user.ntrip_enabled,user.web_enabled]);const td=document.createElement('td');if(serviceReadOnly){td.textContent='Read-only'}else{const edit=document.createElement('button');edit.textContent='Edit';edit.onclick=()=>editUser(user);const del=document.createElement('button');del.textContent='Delete';del.onclick=()=>deleteUser(user);td.append(edit,del)}tr.append(td);return tr}
function editUser(user){userRevision=user.mod_revision;document.getElementById('user-id').value=user.user_id;document.getElementById('user-name').value=user.username;document.getElementById('user-password').value='';document.getElementById('user-role').value=user.role;document.getElementById('user-enabled').checked=user.enabled;document.getElementById('user-ntrip-enabled').checked=user.ntrip_enabled;document.getElementById('user-web-enabled').checked=user.web_enabled;status.textContent='Editing '+user.username}
async function saveUser(){try{const body={user_id:document.getElementById('user-id').value,username:document.getElementById('user-name').value,role:document.getElementById('user-role').value,enabled:document.getElementById('user-enabled').checked,ntrip_enabled:document.getElementById('user-ntrip-enabled').checked,web_enabled:document.getElementById('user-web-enabled').checked};const password=document.getElementById('user-password').value;if(password)body.password=password;const headers=userRevision===null?{}:{'If-Match':'"'+userRevision+'"'};await api('/admin/v1/users'+(userRevision===null?'':'/'+encodeURIComponent(body.user_id)),{method:userRevision===null?'POST':'PATCH',headers:headers,body:JSON.stringify(body)});userRevision=null;status.textContent='User saved';await load();}catch(e){status.textContent=String(e)}}
async function deleteUser(user){if(!confirm('Delete user '+user.username+'?'))return;try{await api('/admin/v1/users/'+encodeURIComponent(user.user_id),{method:'DELETE',headers:{'If-Match':'"'+user.mod_revision+'"'}});status.textContent='User deleted';await load();}catch(e){status.textContent=String(e)}}
function mountRow(mount){const tr=row([mount.mountpoint,mount.owner_user_id,mount.enabled,mount.region||'',mount.latitude??'',mount.longitude??'',mount.coverage_radius_m??'']);const td=document.createElement('td');if(serviceReadOnly){td.textContent='Read-only'}else{const edit=document.createElement('button');edit.textContent='Edit';edit.onclick=()=>editMount(mount);const del=document.createElement('button');del.textContent='Delete';del.onclick=()=>deleteMount(mount);td.append(edit,del)}tr.append(td);return tr}
function optionalNumber(id){const value=document.getElementById(id).value.trim();return value===''?null:Number(value)}
function optionalJson(id){const value=document.getElementById(id).value.trim();return value===''?null:JSON.parse(value)}
function editMount(mount){mountRevision=mount.mod_revision;document.getElementById('mount-name').value=mount.mountpoint;document.getElementById('mount-owner').value=mount.owner_user_id;document.getElementById('mount-password').value='';document.getElementById('mount-region').value=mount.region||'';document.getElementById('mount-latitude').value=mount.latitude??'';document.getElementById('mount-longitude').value=mount.longitude??'';document.getElementById('mount-radius').value=mount.coverage_radius_m??'';document.getElementById('mount-str-fields').value=mount.str_fields?JSON.stringify(mount.str_fields):'';document.getElementById('mount-source-expectation').value=mount.source_expectation?JSON.stringify(mount.source_expectation):'';document.getElementById('mount-enabled').checked=mount.enabled;document.getElementById('mount-advertise').checked=mount.advertise;status.textContent='Editing '+mount.mountpoint}
async function saveMount(){try{const body={mountpoint:document.getElementById('mount-name').value,owner_user_id:document.getElementById('mount-owner').value,region:document.getElementById('mount-region').value||null,latitude:optionalNumber('mount-latitude'),longitude:optionalNumber('mount-longitude'),coverage_radius_m:optionalNumber('mount-radius'),str_fields:optionalJson('mount-str-fields'),source_expectation:optionalJson('mount-source-expectation'),enabled:document.getElementById('mount-enabled').checked,advertise:document.getElementById('mount-advertise').checked};const password=document.getElementById('mount-password').value;if(password)body.ntrip1_upload_password=password;const headers=mountRevision===null?{}:{'If-Match':'"'+mountRevision+'"'};await api('/admin/v1/mountpoints'+(mountRevision===null?'':'/'+encodeURIComponent(body.mountpoint)),{method:mountRevision===null?'POST':'PATCH',headers:headers,body:JSON.stringify(body)});mountRevision=null;status.textContent='Mountpoint saved';await load();}catch(e){status.textContent=String(e)}}
async function deleteMount(mount){if(!confirm('Delete mountpoint '+mount.mountpoint+'?'))return;try{await api('/admin/v1/mountpoints/'+encodeURIComponent(mount.mountpoint),{method:'DELETE',headers:{'If-Match':'"'+mount.mod_revision+'"'}});status.textContent='Mountpoint deleted';await load();}catch(e){status.textContent=String(e)}}
async function showSourceHealth(mountpoint){try{document.getElementById('source-health').textContent='Loading...';const health=await api('/admin/v1/sources/'+encodeURIComponent(mountpoint)+'/health');document.getElementById('source-health').textContent=JSON.stringify(health,null,2);status.textContent='Health loaded for '+mountpoint}catch(e){document.getElementById('source-health').textContent=String(e)}}
function blacklistRow(rule){const tr=row([rule.id,rule.cidr,rule.quota,rule.enabled]);const td=document.createElement('td');if(serviceReadOnly){td.textContent='Read-only'}else{const edit=document.createElement('button');edit.textContent='Edit';edit.onclick=()=>editBlacklist(rule);const del=document.createElement('button');del.textContent='Delete';del.onclick=()=>deleteBlacklist(rule);td.append(edit,del)}tr.append(td);return tr}
function editBlacklist(rule){blacklistRevision=rule.mod_revision;document.getElementById('bl-id').value=rule.id;document.getElementById('bl-cidr').value=rule.cidr;document.getElementById('bl-quota').value=rule.quota;document.getElementById('bl-description').value=rule.description;status.textContent='Editing '+rule.id}
async function addBlacklist(){try{const body={id:document.getElementById('bl-id').value,cidr:document.getElementById('bl-cidr').value,quota:Number(document.getElementById('bl-quota').value),enabled:true,description:document.getElementById('bl-description').value};const headers=blacklistRevision===null?{}:{'If-Match':'"'+blacklistRevision+'"'};await api('/admin/v1/blacklist',{method:blacklistRevision===null?'POST':'PATCH',headers:headers,body:JSON.stringify(body)});status.textContent='Blacklist rule saved';blacklistRevision=null;await load();}catch(e){status.textContent=String(e)}}
async function deleteBlacklist(rule){if(!confirm('Delete blacklist rule '+rule.id+'?'))return;try{await api('/admin/v1/blacklist/'+encodeURIComponent(rule.id),{method:'DELETE',headers:{'If-Match':'"'+rule.mod_revision+'"'}});status.textContent='Blacklist rule deleted';await load();}catch(e){status.textContent=String(e)}}
function apiTokenRow(token){const tr=row([token.token_id,token.name,token.username,token.scopes.join(', '),token.expires_at??'never']);const td=document.createElement('td');const del=document.createElement('button');del.textContent='Revoke';del.onclick=()=>deleteApiToken(token);td.append(del);tr.append(td);return tr}
async function createApiToken(){if(busyActions.has('api-token-create'))return;busyActions.add('api-token-create');try{const scope=document.getElementById('api-token-scope').value;const expiry=Number(document.getElementById('api-token-expiry').value);const body={name:document.getElementById('api-token-name').value,scopes:scope==='read'?['admin:read']:['admin:read','admin:write','operations:write']};if(expiry>0)body.expires_in_s=expiry;const result=await api('/admin/v1/api-tokens',{method:'POST',body:JSON.stringify(body)});document.getElementById('api-token-secret').textContent=result.token;status.textContent='API token created; copy it now';await load();}catch(e){status.textContent=String(e)}finally{busyActions.delete('api-token-create')}}
async function deleteApiToken(token){if(!confirm('Revoke API token '+token.name+'?'))return;try{await api('/admin/v1/api-tokens/'+encodeURIComponent(token.token_id),{method:'DELETE'});await load();}catch(e){status.textContent=String(e)}}
load();
setInterval(function(){if(!loggingOut&&!document.getElementById('account').hidden)load()},10000);
</script></body></html>"#;

fn header<'a>(headers: &'a [&'a str], name: &str) -> Option<&'a str> {
    headers.iter().find_map(|line| {
        line.split_once(':')
            .filter(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim())
    })
}

fn parse_revision(value: &str) -> Option<u64> {
    value
        .trim_matches('"')
        .trim_start_matches("W/\"")
        .trim_end_matches('"')
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::WebSocketLimiter;

    #[test]
    fn websocket_limiter_caps_and_releases_connections() {
        let limiter = WebSocketLimiter::default();
        let slots = (0..4)
            .map(|_| limiter.acquire("admin", "C63").expect("websocket slot"))
            .collect::<Vec<_>>();
        assert!(limiter.acquire("admin", "C63").is_none());
        drop(slots.into_iter().next());
        assert!(limiter.acquire("admin", "C63").is_some());
    }

    #[tokio::test]
    async fn cancels_all_websockets_for_one_mountpoint() {
        let limiter = WebSocketLimiter::default();
        let mut first = limiter.acquire("one", "C63").expect("first");
        let mut second = limiter.acquire("two", "C63").expect("second");
        let other = limiter.acquire("three", "C64").expect("other");
        assert_eq!(limiter.cancel_mountpoint("C63"), 2);
        tokio::time::timeout(std::time::Duration::from_secs(1), first.cancelled())
            .await
            .expect("first cancel");
        tokio::time::timeout(std::time::Duration::from_secs(1), second.cancelled())
            .await
            .expect("second cancel");
        assert!(!*other.cancelled.borrow());
    }
}
