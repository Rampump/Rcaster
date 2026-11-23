use std::{
    error::Error,
    fs,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, RwLock},
    time::Duration,
};

use serde_json::{Value, json};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
    time::timeout,
};
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request, Response, Status,
    metadata::MetadataValue,
    service::{Interceptor, interceptor::InterceptedService},
    transport::{Certificate, Channel, ClientTlsConfig, Identity},
};

use crate::{
    cluster_runtime::SharedClusterRuntime,
    coordinator_grpc::{
        AdmissionAccessPolicy, GrpcClientTls, GrpcServerTls,
        proto::{
            RelayChunk, RelayHealthRequest, RelayHealthResponse, RelayOpenRequest,
            relay_service_client::RelayServiceClient,
            relay_service_server::{RelayService, RelayServiceServer},
        },
    },
    coordinator_mtls_server::server_config,
    hub::MountpointHub,
    tls_reload::ReloadableTlsConfig,
};

type RelayStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<RelayChunk, Status>> + Send + 'static>>;

#[derive(Clone)]
struct RelayClientTokenInterceptor {
    token: String,
}

impl Interceptor for RelayClientTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let value = MetadataValue::try_from(format!("Bearer {}", self.token))
            .map_err(|_| Status::internal("invalid authorization metadata"))?;
        request.metadata_mut().insert("authorization", value);
        Ok(request)
    }
}

#[derive(Clone)]
pub struct RelayGrpcClient {
    inner: RelayServiceClient<InterceptedService<Channel, RelayClientTokenInterceptor>>,
}

impl RelayGrpcClient {
    pub async fn connect_mtls(
        endpoint: impl Into<String>,
        tls: GrpcClientTls,
    ) -> Result<Self, Status> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tls_config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(tls.ca_certificate_pem))
            .identity(Identity::from_pem(tls.certificate_pem, tls.private_key_pem))
            .domain_name(tls.server_name);
        let channel = Channel::from_shared(endpoint.into())
            .map_err(|_| Status::invalid_argument("invalid relay endpoint"))?
            .tls_config(tls_config)
            .map_err(|_| Status::invalid_argument("invalid relay TLS configuration"))?
            .connect()
            .await
            .map_err(|_| Status::unavailable("relay endpoint unavailable"))?;
        Ok(Self {
            inner: RelayServiceClient::with_interceptor(
                channel,
                RelayClientTokenInterceptor {
                    token: tls.service_token,
                },
            ),
        })
    }

    pub async fn open(
        &mut self,
        request: RelayOpenRequest,
    ) -> Result<tonic::Streaming<RelayChunk>, Status> {
        Ok(self.inner.open(Request::new(request)).await?.into_inner())
    }

    pub async fn health(&mut self, request: RelayHealthRequest) -> Result<Value, Status> {
        let payload = self
            .inner
            .health(Request::new(request))
            .await?
            .into_inner()
            .payload;
        serde_json::from_slice(&payload)
            .map_err(|_| Status::data_loss("invalid relay health response"))
    }
}

#[derive(Clone)]
struct RelayTokenInterceptor {
    policy: AdmissionAccessPolicy,
}

impl Interceptor for RelayTokenInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let authorization = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("missing service token"))?;
        let token = authorization
            .strip_prefix("Bearer ")
            .ok_or_else(|| Status::unauthenticated("invalid authorization scheme"))?;
        self.policy.authorize(token, "relay:read")?;
        Ok(request)
    }
}

#[derive(Clone)]
pub struct RelayGrpcService {
    hub: Arc<MountpointHub>,
    runtime: SharedClusterRuntime,
    inbound: RelayInboundLimiter,
}

impl RelayGrpcService {
    pub fn new(hub: Arc<MountpointHub>, runtime: SharedClusterRuntime) -> Self {
        Self::with_limit(hub, runtime, 500)
    }

    pub fn with_limit(
        hub: Arc<MountpointHub>,
        runtime: SharedClusterRuntime,
        max_inbound: usize,
    ) -> Self {
        Self {
            hub,
            runtime,
            inbound: RelayInboundLimiter::new(max_inbound),
        }
    }
}

#[derive(Clone)]
struct RelayInboundLimiter {
    permits: Arc<Semaphore>,
}

impl RelayInboundLimiter {
    fn new(max_inbound: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max_inbound)),
        }
    }

    fn acquire(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.permits).try_acquire_owned().ok()
    }
}

#[tonic::async_trait]
impl RelayService for RelayGrpcService {
    type OpenStream = RelayStream;

    async fn open(
        &self,
        request: Request<RelayOpenRequest>,
    ) -> Result<Response<Self::OpenStream>, Status> {
        let request = request.into_inner();
        if self.runtime.cluster_id().await.as_deref() != Some(request.cluster_id.as_str()) {
            return Err(Status::permission_denied("relay cluster mismatch"));
        }
        let route = self
            .runtime
            .source_route(&request.mountpoint)
            .await
            .ok_or_else(|| Status::not_found("source is not active"))?;
        if route.caster_id != self.runtime.caster_id().await.unwrap_or_default()
            || route.source_generation != request.source_generation
            || route.connection_id != request.connection_id
        {
            return Err(Status::failed_precondition("source route changed"));
        }
        let permit = self
            .inbound
            .acquire()
            .ok_or_else(|| Status::resource_exhausted("relay inbound limit reached"))?;
        let mut receiver = self
            .hub
            .subscribe(&request.mountpoint)
            .map_err(|_| Status::not_found("source is not active"))?;
        let (sender, output) = mpsc::channel(64);
        tokio::spawn(async move {
            let _permit = permit;
            loop {
                match receiver.recv().await {
                    Ok(data) => {
                        if sender
                            .send(Ok(RelayChunk {
                                data: data.to_vec(),
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let _ = sender
                            .send(Err(Status::data_loss("relay subscriber lagged")))
                            .await;
                        return;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(output))))
    }

    async fn health(
        &self,
        request: Request<RelayHealthRequest>,
    ) -> Result<Response<RelayHealthResponse>, Status> {
        let request = request.into_inner();
        if self.runtime.cluster_id().await.as_deref() != Some(request.cluster_id.as_str()) {
            return Err(Status::permission_denied("relay cluster mismatch"));
        }
        let route = self
            .runtime
            .source_route(&request.mountpoint)
            .await
            .ok_or_else(|| Status::not_found("source health not found"))?;
        if route.caster_id != self.runtime.caster_id().await.unwrap_or_default()
            || route.source_generation != request.source_generation
            || route.connection_id != request.connection_id
        {
            return Err(Status::failed_precondition("source route changed"));
        }
        let profile = self
            .hub
            .source_profile(&request.mountpoint)
            .ok_or_else(|| Status::not_found("source health not found"))?;
        let health = self.hub.source_health(&request.mountpoint);
        let payload = json!({
            "mountpoint": request.mountpoint,
            "profile": profile_json(&profile),
            "health": health.map(|health| json!({
                "packets": health.packets,
                "bytes": health.bytes,
                "last_packet_age_seconds": health.last_packet_age_seconds,
                "bitrate_bps": health.bitrate_bps,
                "stream_health_warning": health.warning_codes != 0,
                "warning_codes": health.warning_codes,
                "last_correction_age_seconds": health.last_correction_age_seconds,
            }))
        });
        serde_json::to_vec(&payload)
            .map(|payload| Response::new(RelayHealthResponse { payload }))
            .map_err(|_| Status::internal("relay health encode failed"))
    }
}

fn profile_json(profile: &rcaster_core::protocol::DataProfile) -> Value {
    let attempts = profile.valid_frames.saturating_add(profile.crc_errors);
    json!({
        "total_bytes": profile.total_bytes,
        "unknown_bytes": profile.unknown_bytes,
        "valid_frames": profile.valid_frames,
        "crc_errors": profile.crc_errors,
        "crc_error_ratio": (attempts > 0).then_some(profile.crc_errors as f64 / attempts as f64),
        "message_numbers": profile.message_numbers.iter().copied().collect::<Vec<_>>(),
        "msm_versions": profile.msm_versions.iter().copied().collect::<Vec<_>>(),
        "satellite_count": profile.satellite_count,
        "signal_count": profile.signal_count,
        "has_correction_data": profile.has_correction_data,
    })
}

pub async fn serve_relay_grpc_mtls<F>(
    listener: TcpListener,
    hub: Arc<MountpointHub>,
    runtime: SharedClusterRuntime,
    tls: GrpcServerTls,
    policy: AdmissionAccessPolicy,
    shutdown: F,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    F: Future<Output = ()> + Send + 'static,
{
    serve_relay_grpc_mtls_with_limit(listener, hub, runtime, tls, policy, 500, None, shutdown).await
}

#[allow(clippy::too_many_arguments)]
pub async fn serve_relay_grpc_mtls_with_limit<F>(
    listener: TcpListener,
    hub: Arc<MountpointHub>,
    runtime: SharedClusterRuntime,
    tls: GrpcServerTls,
    policy: AdmissionAccessPolicy,
    max_inbound: usize,
    crl_reload: Option<(PathBuf, Duration)>,
    shutdown: F,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    F: Future<Output = ()> + Send + 'static,
{
    let _ = rustls::crypto::ring::default_provider().install_default();
    let tls_config = Arc::new(RwLock::new(server_config(&tls)?));
    let reload_task = crl_reload.map(|(path, interval)| {
        let tls_config = Arc::clone(&tls_config);
        let mut tls = tls.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Ok(client_crl_pem) = fs::read_to_string(&path) else {
                    continue;
                };
                if tls.client_crl_pem.as_deref() == Some(&client_crl_pem) {
                    continue;
                }
                tls.client_crl_pem = Some(client_crl_pem);
                match server_config(&tls) {
                    Ok(config) => {
                        if let Ok(mut current) = tls_config.write() {
                            *current = config;
                        }
                    }
                    Err(error) => {
                        eprintln!("rcaster: relay CRL reload failed, keeping old CRL: {error}");
                    }
                }
            }
        })
    });
    let (stream_sender, stream_receiver) =
        mpsc::channel::<Result<tokio_rustls::server::TlsStream<TcpStream>, std::io::Error>>(256);
    let accept_task = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let Ok(config) = tls_config.read().map(|config| Arc::clone(&config)) else {
                break;
            };
            let sender = stream_sender.clone();
            tokio::spawn(async move {
                let acceptor = TlsAcceptor::from(config);
                if let Ok(Ok(stream)) =
                    timeout(Duration::from_secs(5), acceptor.accept(socket)).await
                {
                    let _ = sender.send(Ok(stream)).await;
                }
            });
        }
    });
    let server = tonic::transport::Server::builder()
        .add_service(RelayServiceServer::with_interceptor(
            RelayGrpcService::with_limit(hub, runtime, max_inbound),
            RelayTokenInterceptor { policy },
        ))
        .serve_with_incoming(ReceiverStream::new(stream_receiver));
    let mut server = tokio::spawn(server);
    tokio::pin!(shutdown);
    let result = tokio::select! {
        _ = &mut shutdown => None,
        result = &mut server => Some(result),
    };
    accept_task.abort();
    let _ = accept_task.await;
    if let Some(reload_task) = reload_task {
        reload_task.abort();
        let _ = reload_task.await;
    }
    let result = match result {
        Some(result) => result,
        None => server.await,
    };
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error.into()),
        Err(error) => Err(Box::new(error)),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn serve_relay_grpc_mtls_reloadable_with_limit<F>(
    listener: TcpListener,
    hub: Arc<MountpointHub>,
    runtime: SharedClusterRuntime,
    tls: ReloadableTlsConfig,
    policy: AdmissionAccessPolicy,
    max_inbound: usize,
    shutdown: F,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    F: Future<Output = ()> + Send + 'static,
{
    let (stream_sender, stream_receiver) =
        mpsc::channel::<Result<tokio_rustls::server::TlsStream<TcpStream>, std::io::Error>>(256);
    let accept_task = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let config = tls.current();
            let sender = stream_sender.clone();
            tokio::spawn(async move {
                let acceptor = TlsAcceptor::from(config);
                if let Ok(Ok(stream)) =
                    timeout(Duration::from_secs(5), acceptor.accept(socket)).await
                {
                    let _ = sender.send(Ok(stream)).await;
                }
            });
        }
    });
    let server = tonic::transport::Server::builder()
        .add_service(RelayServiceServer::with_interceptor(
            RelayGrpcService::with_limit(hub, runtime, max_inbound),
            RelayTokenInterceptor { policy },
        ))
        .serve_with_incoming(ReceiverStream::new(stream_receiver));
    let mut server = tokio::spawn(server);
    tokio::pin!(shutdown);
    let result = tokio::select! {
        _ = &mut shutdown => None,
        result = &mut server => Some(result),
    };
    accept_task.abort();
    let _ = accept_task.await;
    let result = match result {
        Some(result) => result,
        None => server.await,
    };
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error.into()),
        Err(error) => Err(Box::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::RelayInboundLimiter;

    #[test]
    fn inbound_limit_is_released_when_a_stream_finishes() {
        let limiter = RelayInboundLimiter::new(1);
        let permit = limiter.acquire().expect("first inbound stream");
        assert!(limiter.acquire().is_none());
        drop(permit);
        assert!(limiter.acquire().is_some());
    }
}
