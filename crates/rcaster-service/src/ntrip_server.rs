use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    io,
    net::IpAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rand::Rng;
use rcaster_core::account::{Mountpoint, SourceExpectation};
use rcaster_core::ntrip::{
    BodyFraming, Headers, NtripVersion, Request, SourceV1Request, SourceV2Request,
    chunked::ChunkedDecoder,
    response::{Ntrip1Error, Response, encode_ntrip2_error},
};
use rcaster_core::protocol::{AdmissionViolation, DataFormat, ProtocolDemux, parse_nmea_gga};
use rcaster_core::str_record::StrRecord;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    task::JoinSet,
    time::{Instant, timeout, timeout_at},
};
use tokio_rustls::TlsAcceptor;

use crate::{
    admin_auth::LoginLimiter,
    admission::{AdmissionError, ClientPermit},
    cluster_runtime::{ClusterSourcePermit, SharedClusterRuntime},
    connections::{ConnectionCancel, ConnectionController},
    health::HealthState,
    hub::{HubError, MountpointHub, SourceLease},
    near::{
        GgaAcceptance, NearConfig, NearGga, NearIndex, NearQueryLimiter, NearSelector, NearSource,
    },
    ntrip_io::{ReadHeadError, read_request_head},
    proxy_protocol::{ProxyProtocolConfig, resolve_client_ip},
    relay::RelayManager,
    session_limit::{
        ConnectionLimiter, LocalSessionLimiter, SessionKind, SessionPermit, SessionRequest,
        SessionUser,
    },
    source_admission::{SourceAdmissionConfig, SourceAdmissionWindow, WindowOutcome},
    source_claim::SourceAdmissionSummary,
    tls_reload::ReloadableTlsConfig,
};

pub use crate::auth::{
    AuthError, AuthorizedUser, ClientAuthorizer, ClientCredentials, SourceAuthorizer,
    SourceCredentials,
};

const SOURCE_READ_BYTES: usize = 16 * 1024;
const MAX_CHUNK_TRAILER_BYTES: usize = 4096;

#[derive(Clone)]
pub struct NtripServerConfig {
    pub head_timeout: Duration,
    pub content_length_max_bytes: u64,
    pub client_auth_required: bool,
    pub session_limiter: Arc<LocalSessionLimiter>,
    pub connection_limiter: Arc<ConnectionLimiter>,
    pub cluster_runtime: Option<SharedClusterRuntime>,
    pub relay_manager: Option<Arc<RelayManager>>,
    pub source_admission: Option<SourceAdmissionConfig>,
    pub accounts: Option<crate::account_directory::AccountDirectory>,
    pub near: Option<NearServerConfig>,
    pub metrics: Option<Arc<HealthState>>,
    pub connections: Option<Arc<ConnectionController>>,
    pub proxy_protocol: Option<ProxyProtocolConfig>,
    pub auth_login_limiter: Option<LoginLimiter>,
}

#[derive(Clone)]
pub struct NearServerConfig {
    pub mountpoint: String,
    pub selector: NearConfig,
    pub query_limiter: std::sync::Arc<NearQueryLimiter>,
    pub no_source_timeout: Duration,
    pub advertise: bool,
}

#[allow(dead_code)] // Connection guards are held only for their Drop behavior.
enum SessionHold {
    Local(Box<SessionPermit>),
    ClusterClient(Box<ClusterClientHold>),
    ClusterSource(Box<ClusterSourceHold>),
}

struct ClusterClientHold {
    runtime: SharedClusterRuntime,
    permit: Option<ClientPermit>,
}

struct ClusterSourceHold {
    runtime: SharedClusterRuntime,
    permit: Option<ClusterSourcePermit>,
}

impl Drop for ClusterClientHold {
    fn drop(&mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };
        let runtime = self.runtime.clone();
        tokio::spawn(async move {
            let _ = runtime.release_client(&permit).await;
        });
    }
}

impl Drop for ClusterSourceHold {
    fn drop(&mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };
        let runtime = self.runtime.clone();
        tokio::spawn(async move {
            let _ = runtime.release_source(&permit).await;
        });
    }
}

impl SessionHold {
    fn source_generation(&self) -> Option<u64> {
        match self {
            Self::ClusterSource(hold) => hold
                .permit
                .as_ref()
                .map(crate::cluster_runtime::ClusterSourcePermit::source_generation),
            _ => None,
        }
    }

    async fn ensure_source_publishing(&self) -> io::Result<()> {
        if let Self::ClusterSource(hold) = self
            && !hold.runtime.lease_is_healthy().await
        {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "source lease is no longer healthy",
            ));
        }
        Ok(())
    }

    async fn activate_source(&self, str_record: &StrRecord) -> io::Result<()> {
        self.activate_source_with_summary(str_record, None).await
    }

    async fn activate_source_with_summary(
        &self,
        str_record: &StrRecord,
        admission: Option<SourceAdmissionSummary>,
    ) -> io::Result<()> {
        if let Self::ClusterSource(hold) = self
            && let Some(permit) = &hold.permit
        {
            hold.runtime
                .activate_source_with_summary(permit, str_record, admission)
                .await
                .map_err(|error| io::Error::other(error.to_string()))?;
        }
        Ok(())
    }

    async fn refresh_source_str(
        &self,
        str_record: &StrRecord,
        admission: Option<SourceAdmissionSummary>,
    ) -> io::Result<()> {
        if let Self::ClusterSource(hold) = self
            && let Some(permit) = &hold.permit
        {
            hold.runtime
                .update_source_str(permit, str_record, admission)
                .await
                .map_err(|error| io::Error::other(error.to_string()))?;
        }
        Ok(())
    }

    fn bind(&self, cancellation: Option<&mut ConnectionCancel>, user_id: Option<&str>) {
        let Some(cancellation) = cancellation else {
            return;
        };
        if let Some(user_id) = user_id {
            cancellation.bind(format!("user:{user_id}"));
        }
        let session_id = match self {
            Self::ClusterClient(hold) => hold
                .permit
                .as_ref()
                .map(|permit| permit.session.session_id.as_str()),
            Self::ClusterSource(hold) => hold
                .permit
                .as_ref()
                .map(|permit| permit.session().session_id.as_str()),
            Self::Local(_) => None,
        };
        if let Some(session_id) = session_id {
            cancellation.bind(format!("session:{session_id}"));
        }
        if let Self::ClusterSource(hold) = self
            && let Some(permit) = &hold.permit
        {
            cancellation.bind(format!(
                "source:{}:{}",
                permit.mountpoint(),
                permit.connection_id()
            ));
        }
    }
}

#[derive(Clone)]
pub struct NtripTlsAcceptor(ReloadableTlsConfig);

#[derive(Debug)]
pub enum NtripTlsError {
    Certificate(String),
    MissingPrivateKey,
    Rustls(String),
}

pub async fn serve_ntrip<A, F>(
    listener: TcpListener,
    hub: Arc<MountpointHub>,
    authorizer: A,
    config: NtripServerConfig,
    shutdown: F,
) -> io::Result<()>
where
    A: SourceAuthorizer + ClientAuthorizer + 'static,
    F: Future<Output = ()> + Send,
{
    let authorizer = Arc::new(authorizer);
    let (connection_shutdown, connection_shutdown_rx) = tokio::sync::watch::channel(false);
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => {
                let (mut stream, peer) = accepted?;
                if config.metrics.as_ref().is_some_and(|health| !health.is_ready()) {
                    drop(stream);
                    continue;
                }
                if !config.connection_limiter.try_accept() {
                    continue;
                }
                let Some(connection_slot) = config.connection_limiter.acquire() else {
                    continue;
                };
                let hub = Arc::clone(&hub);
                let authorizer = Arc::clone(&authorizer);
                let config = config.clone();
                let connection_shutdown_rx = connection_shutdown_rx.clone();
                connections.spawn(async move {
                    let operation = async move {
                        let _connection_slot = connection_slot;
                        let Ok(source_ip) =
                            resolve_client_ip(&mut stream, peer.ip(), config.proxy_protocol.as_ref())
                                .await
                        else {
                            return;
                        };
                        let cancellation = config
                            .connections
                            .as_ref()
                            .map(|connections| connections.connection());
                        let _ = handle_connection(
                            stream,
                            source_ip,
                            hub,
                            authorizer,
                            config,
                            cancellation,
                        )
                        .await;
                    };
                    tokio::select! {
                        _ = wait_for_connection_stop(connection_shutdown_rx) => {}
                        _ = operation => {}
                    }
                });
            }
        }
    }
    connection_shutdown.send_replace(true);
    while connections.join_next().await.is_some() {}
    Ok(())
}

pub async fn serve_ntrip_tls<A, F>(
    listener: TcpListener,
    acceptor: NtripTlsAcceptor,
    hub: Arc<MountpointHub>,
    authorizer: A,
    config: NtripServerConfig,
    handshake_timeout: Duration,
    shutdown: F,
) -> io::Result<()>
where
    A: SourceAuthorizer + ClientAuthorizer + 'static,
    F: Future<Output = ()> + Send,
{
    let authorizer = Arc::new(authorizer);
    let acceptor = Arc::new(acceptor);
    let (connection_shutdown, connection_shutdown_rx) = tokio::sync::watch::channel(false);
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => {
                let (mut stream, peer) = accepted?;
                if config.metrics.as_ref().is_some_and(|health| !health.is_ready()) {
                    drop(stream);
                    continue;
                }
                if !config.connection_limiter.try_accept() {
                    continue;
                }
                let Some(connection_slot) = config.connection_limiter.acquire() else {
                    continue;
                };
                let hub = Arc::clone(&hub);
                let authorizer = Arc::clone(&authorizer);
                let acceptor = Arc::clone(&acceptor);
                let config = config.clone();
                let connection_shutdown_rx = connection_shutdown_rx.clone();
                connections.spawn(async move {
                    let operation = async move {
                        let _connection_slot = connection_slot;
                        let Ok(source_ip) =
                            resolve_client_ip(&mut stream, peer.ip(), config.proxy_protocol.as_ref())
                                .await
                        else {
                            return;
                        };
                        let acceptor = TlsAcceptor::from(acceptor.0.current());
                        if let Ok(Ok(stream)) =
                            timeout(handshake_timeout, acceptor.accept(stream)).await
                        {
                            let cancellation = config
                                .connections
                                .as_ref()
                                .map(|connections| connections.connection());
                            let _ = handle_connection(
                                stream,
                                source_ip,
                                hub,
                                authorizer,
                                config,
                                cancellation,
                            )
                            .await;
                        }
                    };
                    tokio::select! {
                        _ = wait_for_connection_stop(connection_shutdown_rx) => {}
                        _ = operation => {}
                    }
                });
            }
        }
    }
    connection_shutdown.send_replace(true);
    while connections.join_next().await.is_some() {}
    Ok(())
}

async fn wait_for_connection_stop(mut stop: tokio::sync::watch::Receiver<bool>) {
    let _ = stop.changed().await;
}

impl NtripTlsAcceptor {
    pub fn from_pem(certificate_pem: &str, private_key_pem: &str) -> Result<Self, NtripTlsError> {
        ReloadableTlsConfig::from_pem(certificate_pem, private_key_pem)
            .map(Self)
            .map_err(|error| NtripTlsError::Rustls(error.to_string()))
    }

    pub fn from_files(
        certificate: std::path::PathBuf,
        private_key: std::path::PathBuf,
        reload_interval: Duration,
    ) -> Result<Self, NtripTlsError> {
        ReloadableTlsConfig::from_files(certificate, private_key, None, reload_interval)
            .map(Self)
            .map_err(|error| NtripTlsError::Rustls(error.to_string()))
    }

    pub fn from_reloadable(config: ReloadableTlsConfig) -> Self {
        Self(config)
    }
}

impl fmt::Display for NtripTlsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Certificate(error) => write!(formatter, "TLS certificate error: {error}"),
            Self::MissingPrivateKey => formatter.write_str("TLS private key is missing"),
            Self::Rustls(error) => write!(formatter, "TLS configuration error: {error}"),
        }
    }
}

impl std::error::Error for NtripTlsError {}

async fn handle_connection<A, S>(
    mut stream: S,
    remote_ip: IpAddr,
    hub: Arc<MountpointHub>,
    authorizer: Arc<A>,
    config: NtripServerConfig,
    mut cancellation: Option<ConnectionCancel>,
) -> io::Result<()>
where
    A: SourceAuthorizer + ClientAuthorizer,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buffered = Vec::new();
    loop {
        let parsed = match read_request_head(&mut stream, &mut buffered, config.head_timeout).await
        {
            Ok(parsed) => parsed,
            Err(ReadHeadError::Parse(error)) => {
                if let Some(status) = error.status_code() {
                    stream
                        .write_all(&encode_ntrip2_error(status, None).map_err(invalid_response)?)
                        .await?;
                }
                return Ok(());
            }
            Err(_) => return Ok(()),
        };

        match parsed.request {
            Request::Sourcetable(request) => {
                let body = sourcetable(&sourcetable_records(&hub, &config).await);
                let keep_alive = request
                    .headers
                    .get("connection")
                    .is_some_and(|value| value.eq_ignore_ascii_case("keep-alive"));
                let response = match request.version {
                    NtripVersion::One => Response::Ntrip1Sourcetable { body: &body },
                    NtripVersion::Two => Response::Ntrip2Sourcetable {
                        server_version: env!("CARGO_PKG_VERSION"),
                        date: &http_date_now(),
                        content_type: "gnss/sourcetable",
                        body: &body,
                        keep_alive,
                    },
                };
                stream
                    .write_all(&response.encode().map_err(invalid_response)?)
                    .await?;
                if request.version == NtripVersion::One || !keep_alive {
                    return Ok(());
                }
            }
            Request::Subscribe(request) => {
                let ip_denied = if let Some(runtime) = &config.cluster_runtime {
                    runtime.is_ip_denied(remote_ip).await
                } else {
                    standalone_ip_quota(&config, remote_ip).is_some_and(|quota| quota == 0)
                        || config.session_limiter.is_ip_denied(remote_ip)
                };
                if ip_denied {
                    return send_client_error(&mut stream, 403, None).await;
                }
                let authenticated_user =
                    if config.client_auth_required {
                        let auth_key = format!("client:{remote_ip}");
                        if let Some(retry_after) = auth_retry_after(&config, &auth_key).await {
                            return send_client_error(&mut stream, 429, Some(retry_after)).await;
                        }
                        let credentials = request.credentials.as_ref().map(|credentials| {
                            ClientCredentials::Basic {
                                username: &credentials.username,
                                password: &credentials.password,
                            }
                        });
                        Some(
                            match authorizer
                                .authorize_client(&request.mountpoint, credentials)
                                .await
                            {
                                Ok(user) => {
                                    auth_success(&config, &auth_key).await;
                                    user
                                }
                                Err(AuthError::Unauthorized) => {
                                    auth_failure(&config, &auth_key).await;
                                    return send_client_error(&mut stream, 401, None).await;
                                }
                                Err(AuthError::Unavailable) => {
                                    return send_client_error(&mut stream, 503, Some(5)).await;
                                }
                            },
                        )
                    } else {
                        None
                    };
                let near = config
                    .near
                    .as_ref()
                    .filter(|near| near.mountpoint == request.mountpoint)
                    .cloned();
                let receiver = if near.is_some() {
                    None
                } else {
                    Some(
                        match subscribe_source(&hub, &config, &request.mountpoint).await {
                            Ok(receiver) => receiver,
                            Err(SourceSubscribeError::NotFound) => {
                                let records = sourcetable_records(&hub, &config).await;
                                return send_unavailable(&mut stream, request.version, &records)
                                    .await;
                            }
                            Err(SourceSubscribeError::Unavailable) => {
                                return send_status(&mut stream, request.version, 503, Some(5))
                                    .await;
                            }
                        },
                    )
                };
                let _permit = match (&config.cluster_runtime, &authenticated_user) {
                    (Some(runtime), Some(user)) => {
                        let ip_max_sessions = match runtime.ip_quota(remote_ip).await {
                            Some(-1) => None,
                            Some(quota) => Some(quota.max(0) as usize),
                            None => config.session_limiter.max_ip_sessions(remote_ip),
                        };
                        match runtime
                            .admit_client_with_metadata(
                                &user.user_id,
                                &request.mountpoint,
                                &remote_ip.to_string(),
                                config.session_limiter.max_sessions(&user.username),
                                ip_max_sessions,
                                Some(ntrip_version_name(request.version)),
                                software_header(&request.headers),
                                user.auth_version,
                                user.user_mod_revision,
                                user.mountpoint_mod_revision,
                            )
                            .await
                        {
                            Ok(permit) => SessionHold::ClusterClient(Box::new(ClusterClientHold {
                                runtime: runtime.clone(),
                                permit: Some(permit),
                            })),
                            Err(
                                AdmissionError::UserQuotaExceeded | AdmissionError::IpQuotaExceeded,
                            ) => {
                                return send_client_error(&mut stream, 429, None).await;
                            }
                            Err(_) => return send_client_error(&mut stream, 503, Some(5)).await,
                        }
                    }
                    _ => match config.session_limiter.try_acquire_with_ip_quota(
                        SessionRequest {
                            user: authenticated_user.as_ref().map(|user| SessionUser {
                                user_id: &user.user_id,
                                username: &user.username,
                            }),
                            kind: SessionKind::Client,
                            remote_ip,
                        },
                        standalone_ip_quota(&config, remote_ip),
                    ) {
                        Ok(permit) => SessionHold::Local(Box::new(permit)),
                        Err(_) => return send_client_error(&mut stream, 429, None).await,
                    },
                };
                _permit.bind(
                    cancellation.as_mut(),
                    authenticated_user
                        .as_ref()
                        .map(|user| user.user_id.as_str()),
                );
                send_data_success(&mut stream, request.version).await?;
                let operation = async {
                    if let Some(near) = near {
                        stream_near(stream, hub, config, near, request.headers.get("ntrip-gga"))
                            .await
                    } else {
                        stream_single_source(stream, receiver.expect("ordinary source"), &hub).await
                    }
                };
                return if let Some(cancellation) = &mut cancellation {
                    tokio::select! {
                        result = operation => result,
                        _ = cancellation.cancelled() => Ok(()),
                    }
                } else {
                    operation.await
                };
            }
            Request::SourceV1(request) => {
                let auth_key = format!("source:{remote_ip}");
                if auth_retry_after(&config, &auth_key).await.is_some() {
                    return send_source_auth_error(&mut stream, NtripVersion::One).await;
                }
                let credentials = SourceCredentials::Ntrip1 {
                    password: &request.password,
                };
                let user = match authorizer
                    .authorize_source(&request.mountpoint, credentials)
                    .await
                {
                    Ok(user) => {
                        auth_success(&config, &auth_key).await;
                        user
                    }
                    Err(AuthError::Unauthorized) => {
                        auth_failure(&config, &auth_key).await;
                        return send_source_auth_error(&mut stream, NtripVersion::One).await;
                    }
                    Err(AuthError::Unavailable) => {
                        return send_status(&mut stream, NtripVersion::One, 503, Some(5)).await;
                    }
                };
                let software = software_header(&request.headers);
                return serve_source(
                    stream,
                    hub,
                    config,
                    NtripVersion::One,
                    request,
                    buffered,
                    SourceContext {
                        user,
                        remote_ip,
                        software,
                    },
                    cancellation,
                )
                .await;
            }
            Request::SourceV2(request) => {
                let auth_key = format!("source:{remote_ip}");
                if let Some(retry_after) = auth_retry_after(&config, &auth_key).await {
                    return send_status(&mut stream, NtripVersion::Two, 429, Some(retry_after))
                        .await;
                }
                let credentials = SourceCredentials::Ntrip2 {
                    username: &request.credentials.username,
                    password: &request.credentials.password,
                };
                let user = match authorizer
                    .authorize_source(&request.mountpoint, credentials)
                    .await
                {
                    Ok(user) => {
                        auth_success(&config, &auth_key).await;
                        user
                    }
                    Err(AuthError::Unauthorized) => {
                        auth_failure(&config, &auth_key).await;
                        return send_source_auth_error(&mut stream, NtripVersion::Two).await;
                    }
                    Err(AuthError::Unavailable) => {
                        return send_status(&mut stream, NtripVersion::Two, 503, Some(5)).await;
                    }
                };
                let software = software_header(&request.headers);
                return serve_source(
                    stream,
                    hub,
                    config,
                    NtripVersion::Two,
                    request,
                    buffered,
                    SourceContext {
                        user,
                        remote_ip,
                        software,
                    },
                    cancellation,
                )
                .await;
            }
        }
    }
}

async fn auth_retry_after(config: &NtripServerConfig, key: &str) -> Option<u32> {
    let local = config
        .auth_login_limiter
        .as_ref()
        .and_then(|limiter| limiter.retry_after(key));
    let shared = if let Some(runtime) = &config.cluster_runtime {
        runtime.auth_failure_retry_after(key).await.ok().flatten()
    } else {
        None
    };
    local.or(shared).map(|seconds| seconds.max(1) as u32)
}

async fn auth_failure(config: &NtripServerConfig, key: &str) {
    if let Some(limiter) = &config.auth_login_limiter {
        limiter.failure(key);
    }
    if let Some(runtime) = &config.cluster_runtime {
        let _ = runtime.record_auth_failure(key).await;
    }
}

async fn auth_success(config: &NtripServerConfig, key: &str) {
    if let Some(limiter) = &config.auth_login_limiter {
        limiter.success(key);
    }
    if let Some(runtime) = &config.cluster_runtime {
        let _ = runtime.clear_auth_failures(key).await;
    }
}

async fn send_client_error<S>(
    stream: &mut S,
    status: u16,
    retry_after: Option<u32>,
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    stream
        .write_all(&encode_ntrip2_error(status, retry_after).map_err(invalid_response)?)
        .await
}

#[allow(clippy::too_many_arguments)]
async fn serve_source<S>(
    mut stream: S,
    hub: Arc<MountpointHub>,
    config: NtripServerConfig,
    version: NtripVersion,
    request: impl SourceRequest,
    buffered: Vec<u8>,
    context: SourceContext,
    mut cancellation: Option<ConnectionCancel>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if config
        .near
        .as_ref()
        .is_some_and(|near| near.mountpoint == request.mountpoint())
    {
        return send_status(&mut stream, version, 403, None).await;
    }
    if let BodyFraming::ContentLength(length) = request.body()
        && length > config.content_length_max_bytes
    {
        return send_status(&mut stream, version, 413, None).await;
    }
    if let Some(runtime) = &config.cluster_runtime {
        match runtime
            .source_cooldown_remaining(request.mountpoint(), &context.remote_ip.to_string())
            .await
        {
            Ok(Some(_)) => return send_status(&mut stream, version, 429, Some(30)).await,
            Ok(None) => {}
            Err(_) => return send_status(&mut stream, version, 503, Some(5)).await,
        }
    }
    let source = match if config.source_admission.is_some() {
        hub.claim_validating_source(request.mountpoint())
    } else {
        hub.claim_source(request.mountpoint())
    } {
        Ok(source) => source,
        Err(HubError::SourceAlreadyActive) => {
            return match version {
                NtripVersion::One => {
                    stream
                        .write_all(
                            &Response::Ntrip1Error(Ntrip1Error::MountpointUnavailable)
                                .encode()
                                .map_err(invalid_response)?,
                        )
                        .await
                }
                NtripVersion::Two => send_status(&mut stream, version, 409, None).await,
            };
        }
        Err(_) => return send_status(&mut stream, version, 503, Some(5)).await,
    };
    let source_connection_id = connection_id();
    let _permit = if let Some(runtime) = &config.cluster_runtime {
        match runtime
            .admit_source_with_metadata(
                &context.user.user_id,
                request.mountpoint(),
                &source_connection_id,
                config.session_limiter.max_sessions(&context.user.username),
                Some(ntrip_version_name(version)),
                context.software.clone(),
                context.user.auth_version,
                context.user.user_mod_revision,
                context.user.mountpoint_mod_revision,
            )
            .await
        {
            Ok(permit) => SessionHold::ClusterSource(Box::new(ClusterSourceHold {
                runtime: runtime.clone(),
                permit: Some(permit),
            })),
            Err(AdmissionError::UserQuotaExceeded) => {
                return send_status(&mut stream, version, 429, None).await;
            }
            Err(AdmissionError::Source(crate::source_claim::ClaimError::AlreadyClaimed)) => {
                return send_status(&mut stream, version, 409, None).await;
            }
            Err(_) => return send_status(&mut stream, version, 503, Some(5)).await,
        }
    } else {
        match config.session_limiter.try_acquire(SessionRequest {
            user: Some(SessionUser {
                user_id: &context.user.user_id,
                username: &context.user.username,
            }),
            kind: SessionKind::Source,
            remote_ip: context.remote_ip,
        }) {
            Ok(permit) => SessionHold::Local(Box::new(permit)),
            Err(_) => return send_status(&mut stream, version, 429, None).await,
        }
    };
    _permit.bind(cancellation.as_mut(), Some(&context.user.user_id));
    let lease_health = match &config.cluster_runtime {
        Some(runtime) => runtime.lease_health().await,
        None => None,
    };
    let configured_mountpoint = config
        .accounts
        .as_ref()
        .and_then(|accounts| accounts.snapshot().mountpoint(source.mountpoint()).cloned());

    let operation = async {
        if version == NtripVersion::Two
            && request
                .headers()
                .get("expect")
                .is_some_and(|value| value.eq_ignore_ascii_case("100-continue"))
        {
            stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await?;
        }
        if let Some(admission) = config.source_admission.clone() {
            send_data_success(&mut stream, version).await?;
            forward_admitted_body(
                stream,
                &source,
                &_permit,
                admission,
                context.user.source_expectation.clone(),
                request.body(),
                buffered,
                config.metrics.clone(),
                config.cluster_runtime.clone(),
                context.remote_ip,
                configured_mountpoint.clone(),
                source_connection_id.clone(),
            )
            .await
        } else {
            let profile = rcaster_core::protocol::DataProfile::default();
            let record = record_from_profile(
                source.mountpoint(),
                &profile,
                configured_mountpoint.as_ref(),
            );
            source.set_str_record(record.clone()).map_err(invalid_hub)?;
            source.set_profile(profile).map_err(invalid_hub)?;
            _permit.activate_source(&record).await?;
            if let Some(metrics) = &config.metrics {
                metrics.source_accepted();
                metrics.record_source_event(
                    source.mountpoint(),
                    "accepted",
                    None,
                    _permit.source_generation(),
                    Some(&source_connection_id),
                );
            }
            send_data_success(&mut stream, version).await?;
            forward_source_body(
                &mut stream,
                &source,
                &_permit,
                request.body(),
                buffered,
                configured_mountpoint.as_ref(),
            )
            .await
        }
    };
    let cancellation = async {
        if let Some(cancellation) = &mut cancellation {
            cancellation.cancelled().await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    let result = tokio::select! {
        result = operation => result,
        _ = cancellation => Ok(()),
        _ = wait_for_lease_loss(lease_health) => Ok(()),
    };
    if source.is_active()
        && let Some(metrics) = &config.metrics
    {
        metrics.record_source_event(
            source.mountpoint(),
            "disconnected",
            result.as_ref().err().map(ToString::to_string).as_deref(),
            _permit.source_generation(),
            Some(&source_connection_id),
        );
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn forward_admitted_body<S>(
    stream: S,
    source: &SourceLease,
    permit: &SessionHold,
    mut config: SourceAdmissionConfig,
    source_expectation: Option<SourceExpectation>,
    framing: BodyFraming,
    buffered: Vec<u8>,
    metrics: Option<Arc<HealthState>>,
    runtime: Option<SharedClusterRuntime>,
    source_ip: IpAddr,
    configured_mountpoint: Option<Mountpoint>,
    connection_id: String,
) -> io::Result<()>
where
    S: AsyncRead + Unpin,
{
    if let Some(expectation) = &source_expectation {
        config.policy = config.policy.with_source_expectation(expectation);
    }
    let mut admission = AdmissionFeed::new(
        config.clone(),
        metrics,
        runtime,
        source_ip,
        configured_mountpoint,
        connection_id,
    );
    match framing {
        BodyFraming::Stream => {
            if !buffered.is_empty() && !admission.feed(&buffered, source, permit).await? {
                return Ok(());
            }
            forward_admitted_stream(stream, source, permit, config, admission).await
        }
        BodyFraming::ContentLength(length) => {
            forward_admitted_fixed(stream, source, permit, config, admission, length, buffered)
                .await
        }
        BodyFraming::Chunked => {
            forward_admitted_chunked(stream, source, permit, config, admission, buffered).await
        }
    }
}

async fn forward_admitted_stream<S>(
    mut stream: S,
    source: &SourceLease,
    permit: &SessionHold,
    config: SourceAdmissionConfig,
    mut admission: AdmissionFeed,
) -> io::Result<()>
where
    S: AsyncRead + Unpin,
{
    let admission_deadline = Instant::now() + config.timeout;
    let mut buffer = vec![0_u8; SOURCE_READ_BYTES];
    loop {
        let read = if admission.is_admitted() {
            timeout(config.read_timeout, stream.read(&mut buffer))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "source read timed out"))??
        } else {
            match timeout_at(admission_deadline, stream.read(&mut buffer)).await {
                Ok(result) => result?,
                Err(_) => {
                    admission.finish(source, permit).await?;
                    if admission.is_admitted() {
                        continue;
                    }
                    return Ok(());
                }
            }
        };
        if read == 0 {
            return admission.finish(source, permit).await;
        }
        if !admission.feed(&buffer[..read], source, permit).await? {
            return Ok(());
        }
    }
}

async fn forward_admitted_fixed<S>(
    mut stream: S,
    source: &SourceLease,
    permit: &SessionHold,
    config: SourceAdmissionConfig,
    mut admission: AdmissionFeed,
    length: u64,
    buffered: Vec<u8>,
) -> io::Result<()>
where
    S: AsyncRead + Unpin,
{
    let admission_deadline = Instant::now() + config.timeout;
    let mut remaining = usize::try_from(length)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Content-Length is too large"))?;
    let consumed = remaining.min(buffered.len());
    remaining -= consumed;
    if consumed > 0
        && !admission
            .feed(&buffered[..consumed], source, permit)
            .await?
    {
        return Ok(());
    }
    if buffered.len() > consumed {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bytes follow fixed-length source body",
        ));
    }
    let mut buffer = vec![0_u8; SOURCE_READ_BYTES.min(remaining.max(1))];
    while remaining > 0 {
        let limit = remaining.min(buffer.len());
        let read = if admission.is_admitted() {
            timeout(config.read_timeout, stream.read(&mut buffer[..limit]))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "source read timed out"))??
        } else {
            timeout_at(admission_deadline, stream.read(&mut buffer[..limit]))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "source admission timed out")
                })??
        };
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "source body ended before Content-Length",
            ));
        }
        if !admission.feed(&buffer[..read], source, permit).await? {
            return Ok(());
        }
        remaining -= read;
    }
    admission.finish(source, permit).await
}

async fn forward_admitted_chunked<S>(
    mut stream: S,
    source: &SourceLease,
    permit: &SessionHold,
    config: SourceAdmissionConfig,
    mut admission: AdmissionFeed,
    buffered: Vec<u8>,
) -> io::Result<()>
where
    S: AsyncRead + Unpin,
{
    let admission_deadline = Instant::now() + config.timeout;
    let mut decoder = ChunkedDecoder::new(MAX_CHUNK_TRAILER_BYTES);
    let mut decoded = Vec::new();
    if !buffered.is_empty() {
        decoder
            .decode(&buffered, &mut decoded)
            .map_err(invalid_chunk)?;
        if !decoded.is_empty() && !admission.feed(&decoded, source, permit).await? {
            return Ok(());
        }
    }
    let mut buffer = vec![0_u8; SOURCE_READ_BYTES];
    while !decoder.is_finished() {
        let read = if admission.is_admitted() {
            timeout(config.read_timeout, stream.read(&mut buffer))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "source read timed out"))??
        } else {
            timeout_at(admission_deadline, stream.read(&mut buffer))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "source admission timed out")
                })??
        };
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "source body ended before chunked terminator",
            ));
        }
        decoded.clear();
        decoder
            .decode(&buffer[..read], &mut decoded)
            .map_err(invalid_chunk)?;
        if !decoded.is_empty() && !admission.feed(&decoded, source, permit).await? {
            return Ok(());
        }
    }
    admission.finish(source, permit).await
}

struct AdmissionFeed {
    window: SourceAdmissionWindow,
    demux: Option<ProtocolDemux>,
    rejected: bool,
    metrics: Option<Arc<HealthState>>,
    runtime: Option<SharedClusterRuntime>,
    source_ip: IpAddr,
    configured_mountpoint: Option<Mountpoint>,
    connection_id: String,
    str_refresh: bool,
    last_str: Option<StrRecord>,
    last_admission: Option<SourceAdmissionSummary>,
}

fn record_from_profile(
    mountpoint: &str,
    profile: &rcaster_core::protocol::DataProfile,
    configured: Option<&Mountpoint>,
) -> StrRecord {
    configured.map_or_else(
        || StrRecord::from_profile(mountpoint, profile),
        |configured| StrRecord::from_profile_and_mountpoint(mountpoint, profile, configured),
    )
}

impl AdmissionFeed {
    fn new(
        config: SourceAdmissionConfig,
        metrics: Option<Arc<HealthState>>,
        runtime: Option<SharedClusterRuntime>,
        source_ip: IpAddr,
        configured_mountpoint: Option<Mountpoint>,
        connection_id: String,
    ) -> Self {
        let str_refresh = config.str_refresh;
        Self {
            window: SourceAdmissionWindow::new(config.policy, config.max_bytes, config.max_frames),
            demux: None,
            rejected: false,
            metrics,
            runtime,
            source_ip,
            configured_mountpoint,
            str_refresh,
            last_str: None,
            last_admission: None,
            connection_id,
        }
    }

    fn is_admitted(&self) -> bool {
        self.demux.is_some()
    }

    async fn feed(
        &mut self,
        bytes: &[u8],
        source: &SourceLease,
        permit: &SessionHold,
    ) -> io::Result<bool> {
        permit.ensure_source_publishing().await?;
        if self.rejected {
            return Ok(false);
        }
        if let Some(demux) = &mut self.demux {
            let frames = demux.push(bytes);
            for frame in frames {
                let packet = Arc::from(frame.bytes);
                source
                    .publish_frame(packet, frame.format, frame.message_number)
                    .map_err(invalid_hub)?;
            }
            source
                .set_profile(demux.profile().clone())
                .map_err(invalid_hub)?;
            if self.str_refresh {
                let record = record_from_profile(
                    source.mountpoint(),
                    demux.profile(),
                    self.configured_mountpoint.as_ref(),
                );
                if self.last_str.as_ref() != Some(&record) {
                    source.set_str_record(record.clone()).map_err(invalid_hub)?;
                    if let Some(admission) = &self.last_admission
                        && let Err(error) = permit
                            .refresh_source_str(&record, Some(admission.clone()))
                            .await
                    {
                        eprintln!(
                            "rcaster: failed to refresh STR for {}: {error}",
                            source.mountpoint()
                        );
                        return Err(error);
                    }
                    eprintln!(
                        "rcaster: refreshed STR for {} after profile update",
                        source.mountpoint()
                    );
                    self.last_str = Some(record);
                }
            }
            return Ok(true);
        }
        match self.window.push(bytes) {
            WindowOutcome::Pending => Ok(true),
            WindowOutcome::Accepted { bytes, warnings } => {
                let record = record_from_profile(
                    source.mountpoint(),
                    self.window.profile(),
                    self.configured_mountpoint.as_ref(),
                );
                let admission =
                    SourceAdmissionSummary::from_profile(self.window.profile(), &warnings);
                source.set_str_record(record.clone()).map_err(invalid_hub)?;
                source
                    .set_profile(self.window.profile().clone())
                    .map_err(invalid_hub)?;
                permit
                    .activate_source_with_summary(&record, Some(admission.clone()))
                    .await?;
                self.last_str = Some(record);
                self.last_admission = Some(admission);
                if let Some(metrics) = &self.metrics {
                    metrics.source_accepted();
                    metrics.record_source_event(
                        source.mountpoint(),
                        "accepted",
                        None,
                        permit.source_generation(),
                        Some(&self.connection_id),
                    );
                }
                if self.window.profile().has_correction_data {
                    publish_correction(source, bytes)?;
                } else {
                    publish(source, bytes)?;
                }
                self.demux = Some(self.window.take_demux());
                Ok(true)
            }
            WindowOutcome::Rejected { violations } => {
                self.rejected = true;
                self.record_cooldown(source.mountpoint(), "SOURCE_ADMISSION_REJECTED")
                    .await;
                if let Some(metrics) = &self.metrics {
                    metrics.source_rejected_with_reasons(&violations);
                    metrics.record_source_event(
                        source.mountpoint(),
                        "rejected",
                        Some(&format!("{violations:?}")),
                        permit.source_generation(),
                        Some(&self.connection_id),
                    );
                }
                Ok(false)
            }
        }
    }

    async fn finish(&mut self, source: &SourceLease, permit: &SessionHold) -> io::Result<()> {
        permit.ensure_source_publishing().await?;
        if self.demux.is_none() {
            let outcome = self.window.finish();
            match outcome {
                WindowOutcome::Accepted { bytes, warnings } => {
                    let admission =
                        SourceAdmissionSummary::from_profile(self.window.profile(), &warnings);
                    let record = record_from_profile(
                        source.mountpoint(),
                        self.window.profile(),
                        self.configured_mountpoint.as_ref(),
                    );
                    source.set_str_record(record.clone()).map_err(invalid_hub)?;
                    source
                        .set_profile(self.window.profile().clone())
                        .map_err(invalid_hub)?;
                    permit
                        .activate_source_with_summary(&record, Some(admission.clone()))
                        .await?;
                    self.last_str = Some(record);
                    self.last_admission = Some(admission);
                    if let Some(metrics) = &self.metrics {
                        metrics.source_accepted();
                        metrics.record_source_event(
                            source.mountpoint(),
                            "accepted",
                            None,
                            None,
                            Some(&self.connection_id),
                        );
                    }
                    if self.window.profile().has_correction_data {
                        publish_correction(source, bytes)?;
                    } else {
                        publish(source, bytes)?;
                    }
                    self.demux = Some(self.window.take_demux());
                    return Ok(());
                }
                WindowOutcome::Rejected { violations } => {
                    self.record_cooldown(source.mountpoint(), "SOURCE_ADMISSION_REJECTED")
                        .await;
                    if let Some(metrics) = &self.metrics {
                        metrics.source_rejected_with_reasons(&violations);
                        metrics.record_source_event(
                            source.mountpoint(),
                            "rejected",
                            Some(&format!("{violations:?}")),
                            permit.source_generation(),
                            Some(&self.connection_id),
                        );
                    }
                    return Ok(());
                }
                WindowOutcome::Pending => {
                    self.record_cooldown(source.mountpoint(), "SOURCE_ADMISSION_REJECTED")
                        .await;
                    if let Some(metrics) = &self.metrics {
                        metrics.source_rejected_with_reasons(&[AdmissionViolation::NoValidFrames]);
                        metrics.record_source_event(
                            source.mountpoint(),
                            "rejected",
                            Some("NoValidFrames"),
                            permit.source_generation(),
                            Some(&self.connection_id),
                        );
                    }
                    return Ok(());
                }
            };
        }
        Ok(())
    }

    async fn record_cooldown(&self, mountpoint: &str, reason: &str) {
        if let Some(runtime) = &self.runtime {
            let _ = runtime
                .record_source_cooldown(mountpoint, &self.source_ip.to_string(), reason)
                .await;
        }
    }
}

struct SourceContext {
    user: AuthorizedUser,
    remote_ip: IpAddr,
    software: Option<String>,
}

trait SourceRequest {
    fn mountpoint(&self) -> &str;
    fn body(&self) -> BodyFraming;
    fn headers(&self) -> &Headers;
}

impl SourceRequest for SourceV1Request {
    fn mountpoint(&self) -> &str {
        &self.mountpoint
    }

    fn body(&self) -> BodyFraming {
        self.body
    }

    fn headers(&self) -> &Headers {
        &self.headers
    }
}

impl SourceRequest for SourceV2Request {
    fn mountpoint(&self) -> &str {
        &self.mountpoint
    }

    fn body(&self) -> BodyFraming {
        self.body
    }

    fn headers(&self) -> &Headers {
        &self.headers
    }
}

async fn forward_source_body<S>(
    stream: &mut S,
    source: &SourceLease,
    permit: &SessionHold,
    framing: BodyFraming,
    buffered: Vec<u8>,
    configured_mountpoint: Option<&Mountpoint>,
) -> io::Result<()>
where
    S: AsyncRead + Unpin,
{
    let mut demux = ProtocolDemux::new();
    match framing {
        BodyFraming::Stream => {
            permit.ensure_source_publishing().await?;
            publish_analyzed(source, permit, buffered, &mut demux, configured_mountpoint).await?;
            forward_stream(stream, source, permit, &mut demux, configured_mountpoint).await
        }
        BodyFraming::ContentLength(length) => {
            forward_fixed(
                stream,
                source,
                permit,
                length,
                buffered,
                &mut demux,
                configured_mountpoint,
            )
            .await
        }
        BodyFraming::Chunked => {
            forward_chunked(
                stream,
                source,
                permit,
                buffered,
                &mut demux,
                configured_mountpoint,
            )
            .await
        }
    }
}

async fn wait_for_lease_loss(mut health: Option<tokio::sync::watch::Receiver<bool>>) {
    let Some(health) = health.as_mut() else {
        std::future::pending::<()>().await;
        return;
    };
    if !*health.borrow() {
        return;
    }
    while health.changed().await.is_ok() {
        if !*health.borrow() {
            return;
        }
    }
}

async fn forward_stream<S>(
    stream: &mut S,
    source: &SourceLease,
    permit: &SessionHold,
    demux: &mut ProtocolDemux,
    configured_mountpoint: Option<&Mountpoint>,
) -> io::Result<()>
where
    S: AsyncRead + Unpin,
{
    let mut buffer = vec![0_u8; SOURCE_READ_BYTES];
    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        permit.ensure_source_publishing().await?;
        publish_analyzed(
            source,
            permit,
            buffer[..read].to_vec(),
            demux,
            configured_mountpoint,
        )
        .await?;
    }
}

async fn forward_fixed<S>(
    stream: &mut S,
    source: &SourceLease,
    permit: &SessionHold,
    length: u64,
    mut buffered: Vec<u8>,
    demux: &mut ProtocolDemux,
    configured_mountpoint: Option<&Mountpoint>,
) -> io::Result<()>
where
    S: AsyncRead + Unpin,
{
    let mut remaining = usize::try_from(length)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Content-Length is too large"))?;
    let take = remaining.min(buffered.len());
    permit.ensure_source_publishing().await?;
    publish_analyzed(
        source,
        permit,
        buffered.drain(..take).collect(),
        demux,
        configured_mountpoint,
    )
    .await?;
    remaining -= take;
    if !buffered.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bytes follow fixed-length source body",
        ));
    }

    let mut buffer = vec![0_u8; SOURCE_READ_BYTES.min(remaining.max(1))];
    while remaining > 0 {
        let limit = remaining.min(buffer.len());
        let read = stream.read(&mut buffer[..limit]).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "source body ended before Content-Length",
            ));
        }
        permit.ensure_source_publishing().await?;
        publish_analyzed(
            source,
            permit,
            buffer[..read].to_vec(),
            demux,
            configured_mountpoint,
        )
        .await?;
        remaining -= read;
    }
    Ok(())
}

async fn forward_chunked<S>(
    stream: &mut S,
    source: &SourceLease,
    permit: &SessionHold,
    buffered: Vec<u8>,
    demux: &mut ProtocolDemux,
    configured_mountpoint: Option<&Mountpoint>,
) -> io::Result<()>
where
    S: AsyncRead + Unpin,
{
    let mut decoder = ChunkedDecoder::new(MAX_CHUNK_TRAILER_BYTES);
    let mut decoded = Vec::new();
    if !buffered.is_empty() {
        decoder
            .decode(&buffered, &mut decoded)
            .map_err(invalid_chunk)?;
        permit.ensure_source_publishing().await?;
        publish_analyzed(
            source,
            permit,
            std::mem::take(&mut decoded),
            demux,
            configured_mountpoint,
        )
        .await?;
    }

    let mut buffer = vec![0_u8; SOURCE_READ_BYTES];
    while !decoder.is_finished() {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "source body ended before chunked terminator",
            ));
        }
        decoder
            .decode(&buffer[..read], &mut decoded)
            .map_err(invalid_chunk)?;
        permit.ensure_source_publishing().await?;
        publish_analyzed(
            source,
            permit,
            std::mem::take(&mut decoded),
            demux,
            configured_mountpoint,
        )
        .await?;
    }
    Ok(())
}

async fn stream_single_source<S>(
    mut stream: S,
    mut receiver: tokio::sync::broadcast::Receiver<Arc<[u8]>>,
    hub: &MountpointHub,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut input = [0_u8; 1024];
    loop {
        tokio::select! {
            packet = receiver.recv() => match packet {
                Ok(packet) => stream.write_all(&packet).await?,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    hub.record_backlog_kick();
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "subscriber exceeded its backlog",
                    ));
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            },
            read = stream.read(&mut input) => {
                if read? == 0 {
                    return Ok(());
                }
            }
        }
    }
}

enum SourceSubscribeError {
    NotFound,
    Unavailable,
}

async fn subscribe_source(
    hub: &MountpointHub,
    config: &NtripServerConfig,
    mountpoint: &str,
) -> Result<tokio::sync::broadcast::Receiver<Arc<[u8]>>, SourceSubscribeError> {
    match hub.subscribe(mountpoint) {
        Ok(receiver) => Ok(receiver),
        Err(HubError::SourceNotFound) => {
            let Some(relay) = &config.relay_manager else {
                return Err(SourceSubscribeError::NotFound);
            };
            if let Err(error) = relay.ensure(mountpoint).await {
                eprintln!("rcaster: relay ensure failed for {mountpoint}: {error}");
                return Err(SourceSubscribeError::Unavailable);
            }
            hub.subscribe(mountpoint).map_err(|error| {
                eprintln!("rcaster: relayed source subscribe failed for {mountpoint}: {error}");
                SourceSubscribeError::Unavailable
            })
        }
        Err(_) => Err(SourceSubscribeError::Unavailable),
    }
}

async fn stream_near<S>(
    mut stream: S,
    hub: Arc<MountpointHub>,
    config: NtripServerConfig,
    near: NearServerConfig,
    initial_gga: Option<&str>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut selector = NearSelector::new(near.selector);
    let mut indexed_revision = hub.source_revision();
    let mut near_index = NearIndex::new(&near_candidates(&hub, &config).await);
    let mut receiver = None;
    let mut demux = None;
    let mut pending = Vec::new();
    let mut pending_output = std::collections::VecDeque::new();
    let mut no_source_deadline = Instant::now() + near.no_source_timeout;
    let mut last_query = None;
    let mut last_gga = None;

    if let Some(gga) = initial_gga.and_then(parse_near_gga_header)
        && accept_near_gga(&mut selector, &gga, config.metrics.as_ref())
    {
        last_gga = Some(gga.clone());
        last_query = Some((std::time::Instant::now(), gga.position));
        if let Some(query_permit) = near.query_limiter.try_acquire() {
            if let Some(metrics) = &config.metrics {
                metrics.near_query();
            }
            switch_near_source(
                &mut selector,
                &mut receiver,
                &mut demux,
                &hub,
                &config,
                &near_index,
                gga,
                &mut no_source_deadline,
                near.no_source_timeout,
                &mut pending_output,
                query_permit,
            )
            .await;
        } else if let Some(metrics) = &config.metrics {
            metrics.near_query_limited();
        }
    }

    let mut read_buffer = [0_u8; 4096];
    loop {
        if hub.source_revision() != indexed_revision {
            near_index = NearIndex::new(&near_candidates(&hub, &config).await);
            indexed_revision = hub.source_revision();
        }
        while let Some(frame) = pending_output.pop_front() {
            stream.write_all(&frame).await?;
        }
        if let Some(source) = receiver.as_mut() {
            let mut source_closed = false;
            tokio::select! {
                read = stream.read(&mut read_buffer) => {
                    let read = read?;
                    if read == 0 {
                        return Ok(());
                    }
                    for gga in near_ggas(&mut pending, &read_buffer[..read]) {
                        if !accept_near_gga(&mut selector, &gga, config.metrics.as_ref()) {
                            continue;
                        }
                        last_gga = Some(gga.clone());
                        if !selector.should_query(last_query, &gga, std::time::Instant::now()) {
                            continue;
                        }
                        last_query = Some((std::time::Instant::now(), gga.position));
                        let Some(query_permit) = near.query_limiter.try_acquire() else {
                            if let Some(metrics) = &config.metrics {
                                metrics.near_query_limited();
                            }
                            continue;
                        };
                        if let Some(metrics) = &config.metrics {
                            metrics.near_query();
                        }
                        switch_near_source(
                            &mut selector,
                            &mut receiver,
                            &mut demux,
                            &hub,
                            &config,
                            &near_index,
                            gga,
                            &mut no_source_deadline,
                            near.no_source_timeout,
                            &mut pending_output,
                            query_permit,
                        ).await;
                    }
                }
                packet = source.recv() => {
                    match packet {
                        Ok(packet) => {
                            let packet_demux = demux.get_or_insert_with(ProtocolDemux::new);
                            for frame in packet_demux.push(&packet)
                                .into_iter()
                                .filter(|frame| frame.format == DataFormat::Rtcm3)
                            {
                                stream.write_all(&frame.bytes).await?;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            source_closed = true;
                        }
                    }
                }
            }
            if source_closed {
                receiver = None;
                demux = None;
                pending_output.clear();
                no_source_deadline = Instant::now() + near.no_source_timeout;
                near_index = NearIndex::new(&near_candidates(&hub, &config).await);
                indexed_revision = hub.source_revision();
                if let Some(gga) = last_gga.clone()
                    && let Some(query_permit) = near.query_limiter.try_acquire()
                {
                    if let Some(metrics) = &config.metrics {
                        metrics.near_query();
                    }
                    switch_near_source(
                        &mut selector,
                        &mut receiver,
                        &mut demux,
                        &hub,
                        &config,
                        &near_index,
                        gga,
                        &mut no_source_deadline,
                        near.no_source_timeout,
                        &mut pending_output,
                        query_permit,
                    )
                    .await;
                } else if let Some(metrics) = &config.metrics {
                    metrics.near_query_limited();
                }
            }
        } else {
            let read = timeout_at(no_source_deadline, stream.read(&mut read_buffer))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "no NEAR source"))??;
            if read == 0 {
                return Ok(());
            }
            for gga in near_ggas(&mut pending, &read_buffer[..read]) {
                if !accept_near_gga(&mut selector, &gga, config.metrics.as_ref()) {
                    continue;
                }
                last_gga = Some(gga.clone());
                if !selector.should_query(last_query, &gga, std::time::Instant::now()) {
                    continue;
                }
                last_query = Some((std::time::Instant::now(), gga.position));
                let Some(query_permit) = near.query_limiter.try_acquire() else {
                    if let Some(metrics) = &config.metrics {
                        metrics.near_query_limited();
                    }
                    continue;
                };
                if let Some(metrics) = &config.metrics {
                    metrics.near_query();
                }
                switch_near_source(
                    &mut selector,
                    &mut receiver,
                    &mut demux,
                    &hub,
                    &config,
                    &near_index,
                    gga,
                    &mut no_source_deadline,
                    near.no_source_timeout,
                    &mut pending_output,
                    query_permit,
                )
                .await;
                if receiver.is_some() {
                    break;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn switch_near_source(
    selector: &mut NearSelector,
    receiver: &mut Option<tokio::sync::broadcast::Receiver<Arc<[u8]>>>,
    demux: &mut Option<ProtocolDemux>,
    hub: &Arc<MountpointHub>,
    config: &NtripServerConfig,
    candidates: &NearIndex,
    gga: NearGga,
    no_source_deadline: &mut Instant,
    no_source_timeout: Duration,
    pending_output: &mut std::collections::VecDeque<Arc<[u8]>>,
    query_permit: crate::near::NearQueryPermit,
) {
    let started = Instant::now();
    switch_near_source_inner(
        selector,
        receiver,
        demux,
        hub,
        config,
        candidates,
        gga,
        no_source_deadline,
        no_source_timeout,
        pending_output,
        query_permit,
    )
    .await;
    if let Some(metrics) = &config.metrics {
        metrics.near_query_latency(started.elapsed());
    }
}

#[allow(clippy::too_many_arguments)]
async fn switch_near_source_inner(
    selector: &mut NearSelector,
    receiver: &mut Option<tokio::sync::broadcast::Receiver<Arc<[u8]>>>,
    demux: &mut Option<ProtocolDemux>,
    hub: &Arc<MountpointHub>,
    config: &NtripServerConfig,
    candidates: &NearIndex,
    gga: NearGga,
    no_source_deadline: &mut Instant,
    no_source_timeout: Duration,
    pending_output: &mut std::collections::VecDeque<Arc<[u8]>>,
    _query_permit: crate::near::NearQueryPermit,
) {
    let previous = selector.current().map(str::to_owned);
    let Some(mountpoint) = selector.update_indexed(&gga, candidates, std::time::Instant::now())
    else {
        return;
    };
    if let Ok(next) = subscribe_source(hub, config, &mountpoint).await {
        pending_output.clear();
        pending_output.extend(hub.station_frames(&mountpoint));
        *receiver = Some(next);
        *demux = Some(ProtocolDemux::new());
        *no_source_deadline = Instant::now() + no_source_timeout;
        if previous.as_deref() != Some(mountpoint.as_str())
            && let Some(metrics) = &config.metrics
        {
            metrics.near_switched();
        }
    }
}

fn accept_near_gga(
    selector: &mut NearSelector,
    gga: &NearGga,
    metrics: Option<&Arc<crate::health::HealthState>>,
) -> bool {
    match selector.accept_gga(gga, std::time::Instant::now()) {
        GgaAcceptance::Accepted => true,
        GgaAcceptance::RateLimited => {
            if let Some(metrics) = metrics {
                metrics.near_gga_rate_limited();
            }
            false
        }
        GgaAcceptance::JumpRejected => {
            if let Some(metrics) = metrics {
                metrics.near_gga_jump_rejected();
            }
            false
        }
        GgaAcceptance::Invalid => {
            if let Some(metrics) = metrics {
                metrics.near_gga_invalid();
            }
            false
        }
    }
}

async fn near_candidates(hub: &MountpointHub, config: &NtripServerConfig) -> Vec<NearSource> {
    let position_ttl_s = config
        .near
        .as_ref()
        .map(|near| near.selector.position_ttl_s)
        .unwrap_or_default();
    let records = sourcetable_records(hub, config).await;
    let mut candidates = Vec::new();
    for record in records {
        if config
            .near
            .as_ref()
            .is_some_and(|near| near.mountpoint == record.mountpoint)
        {
            continue;
        }
        if position_ttl_s > 0
            && !near_position_is_fresh(hub, config, &record.mountpoint, position_ttl_s).await
        {
            continue;
        }
        if let Some(candidate) = NearSource::from_str_record(&record) {
            candidates.push(candidate);
        }
    }
    candidates
}

async fn near_position_is_fresh(
    hub: &MountpointHub,
    config: &NtripServerConfig,
    mountpoint: &str,
    ttl_s: u64,
) -> bool {
    if let Some(age) = hub
        .source_health(mountpoint)
        .and_then(|health| health.last_packet_age_seconds)
    {
        return age <= ttl_s;
    }
    let Some(runtime) = &config.cluster_runtime else {
        return false;
    };
    let Some(route) = runtime.source_route(mountpoint).await else {
        return false;
    };
    let Ok(created_at) = route.created_at.parse::<u64>() else {
        return false;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    now.saturating_sub(created_at) <= ttl_s
}

fn standalone_ip_quota(config: &NtripServerConfig, remote_ip: std::net::IpAddr) -> Option<i32> {
    config
        .accounts
        .as_ref()
        .and_then(|accounts| accounts.blacklist_quota_for_ip(remote_ip))
}

fn parse_near_gga_header(value: &str) -> Option<NearGga> {
    let value = value.trim();
    let sentence = if value.starts_with('$') {
        value.to_owned()
    } else {
        format!("${value}")
    };
    parse_nmea_gga(format!("{sentence}\r\n").as_bytes()).map(|gga| NearGga {
        position: gga.position,
        fix_quality: gga.fix_quality,
        hdop: gga.hdop,
    })
}

fn near_ggas(pending: &mut Vec<u8>, input: &[u8]) -> Vec<NearGga> {
    pending.extend_from_slice(input);
    let mut ggas = Vec::new();
    while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
        let line = pending.drain(..=newline).collect::<Vec<_>>();
        if let Some(gga) = parse_nmea_gga(&line) {
            ggas.push(NearGga {
                position: gga.position,
                fix_quality: gga.fix_quality,
                hdop: gga.hdop,
            });
        }
    }
    if pending.len() > 1024 {
        pending.clear();
    }
    ggas
}

async fn send_unavailable<S>(
    stream: &mut S,
    version: NtripVersion,
    mountpoints: &[StrRecord],
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    if version == NtripVersion::Two {
        return send_status(stream, version, 503, Some(5)).await;
    }
    let body = sourcetable(mountpoints);
    stream
        .write_all(
            &Response::Ntrip1Sourcetable { body: &body }
                .encode()
                .map_err(invalid_response)?,
        )
        .await
}

async fn send_source_auth_error<S>(stream: &mut S, version: NtripVersion) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    match version {
        NtripVersion::One => {
            stream
                .write_all(
                    &Response::Ntrip1Error(Ntrip1Error::BadPassword)
                        .encode()
                        .map_err(invalid_response)?,
                )
                .await
        }
        NtripVersion::Two => send_status(stream, version, 401, None).await,
    }
}

async fn send_data_success<S>(stream: &mut S, version: NtripVersion) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let response = match version {
        NtripVersion::One => Response::Ntrip1Data,
        NtripVersion::Two => Response::Ntrip2Data {
            server_version: env!("CARGO_PKG_VERSION"),
            date: &http_date_now(),
        },
    };
    stream
        .write_all(&response.encode().map_err(invalid_response)?)
        .await
}

async fn send_status<S>(
    stream: &mut S,
    version: NtripVersion,
    status: u16,
    retry_after: Option<u32>,
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    if version == NtripVersion::One {
        let error = if matches!(status, 401 | 403) {
            Ntrip1Error::BadPassword
        } else {
            Ntrip1Error::MountpointUnavailable
        };
        return stream
            .write_all(
                &Response::Ntrip1Error(error)
                    .encode()
                    .map_err(invalid_response)?,
            )
            .await;
    }
    stream
        .write_all(&encode_ntrip2_error(status, retry_after).map_err(invalid_response)?)
        .await
}

fn publish(source: &SourceLease, packet: Vec<u8>) -> io::Result<()> {
    if packet.is_empty() {
        return Ok(());
    }
    source.publish(Arc::from(packet)).map_err(invalid_hub)
}

fn publish_correction(source: &SourceLease, packet: Vec<u8>) -> io::Result<()> {
    if packet.is_empty() {
        return Ok(());
    }
    source
        .publish_correction(Arc::from(packet))
        .map_err(invalid_hub)
}

async fn publish_analyzed(
    source: &SourceLease,
    permit: &SessionHold,
    packet: Vec<u8>,
    demux: &mut ProtocolDemux,
    configured_mountpoint: Option<&Mountpoint>,
) -> io::Result<()> {
    if packet.is_empty() {
        return Ok(());
    }
    source
        .publish_analyzed(Arc::from(packet), demux)
        .map_err(invalid_hub)?;
    let record = record_from_profile(source.mountpoint(), demux.profile(), configured_mountpoint);
    if source
        .refresh_str_record(record.clone())
        .map_err(invalid_hub)?
    {
        permit.refresh_source_str(&record, None).await?;
    }
    Ok(())
}

fn connection_id() -> String {
    let mut bytes = [0_u8; 16];
    rand::rng().fill(&mut bytes);
    format!(
        "c_{}",
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

fn ntrip_version_name(version: NtripVersion) -> String {
    match version {
        NtripVersion::One => "1",
        NtripVersion::Two => "2",
    }
    .to_owned()
}

fn software_header(headers: &Headers) -> Option<String> {
    headers
        .get("user-agent")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.chars().take(256).collect())
}

async fn sourcetable_records(hub: &MountpointHub, config: &NtripServerConfig) -> Vec<StrRecord> {
    let mut by_mountpoint = hub
        .sourcetable_records()
        .into_iter()
        .map(|record| (record.mountpoint.clone(), record))
        .collect::<BTreeMap<_, _>>();
    if let Some(runtime) = &config.cluster_runtime {
        for record in runtime.sourcetable_records().await {
            by_mountpoint
                .entry(record.mountpoint.clone())
                .or_insert(record);
        }
    }
    if let Some(accounts) = &config.accounts {
        let snapshot = accounts.snapshot();
        by_mountpoint.retain(|mountpoint, _| {
            snapshot
                .mountpoint(mountpoint)
                .is_some_and(|mountpoint| mountpoint.enabled && mountpoint.advertise)
        });
    }
    let near_record = config
        .near
        .as_ref()
        .filter(|near| near.advertise)
        .map(|near| {
            by_mountpoint.remove(&near.mountpoint);
            StrRecord {
                mountpoint: near.mountpoint.clone(),
                identifier: "NEAR".to_owned(),
                format: "RTCM 3".to_owned(),
                format_details: "1005,1074,1084,1094,1124".to_owned(),
                carrier: "2".to_owned(),
                navigation_system: "GPS+GLO+GAL+BDS+QZS".to_owned(),
                network: "RCaster".to_owned(),
                country: "XXX".to_owned(),
                latitude: "0".to_owned(),
                longitude: "0".to_owned(),
                nmea: "1".to_owned(),
                solution: "0".to_owned(),
                generator: "RCaster".to_owned(),
                compression: "none".to_owned(),
                authentication: "N".to_owned(),
                fee: "N".to_owned(),
                bitrate: "0".to_owned(),
                misc: "virtual=near".to_owned(),
            }
        });
    let mut records = by_mountpoint.into_values().collect::<Vec<_>>();
    if let Some(record) = near_record {
        records.insert(0, record);
    }
    records
}

fn sourcetable(mountpoints: &[StrRecord]) -> String {
    let mut body = String::from(
        "CAS;CHN;RCaster;0;0;GNSS correction caster;\r\nNET;RCaster;RCaster;CHN;0;0;https://localhost;\r\n",
    );
    for record in mountpoints {
        body.push_str(&record.to_line());
    }
    body.push_str("ENDSOURCETABLE\r\n");
    body
}

fn http_date_now() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    http_date(seconds)
}

fn http_date(seconds: i64) -> String {
    const WEEKDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let days = seconds.div_euclid(86_400);
    let day_seconds = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_date(days);
    let weekday = (days + 4).rem_euclid(7) as usize;
    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        WEEKDAYS[weekday],
        day,
        MONTHS[month - 1],
        year,
        day_seconds / 3600,
        day_seconds % 3600 / 60,
        day_seconds % 60
    )
}

fn civil_date(days: i64) -> (i64, usize, i64) {
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year, month as usize, day)
}

fn invalid_response(error: impl fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

fn invalid_hub(error: HubError) -> io::Error {
    io::Error::other(error.to_string())
}

fn invalid_chunk(error: impl fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::http_date;

    #[test]
    fn formats_known_http_dates() {
        assert_eq!(http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        assert_eq!(http_date(1_795_341_600), "Sun, 22 Nov 2026 10:00:00 GMT");
    }
}
