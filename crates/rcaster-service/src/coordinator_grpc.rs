use std::{future::Future, sync::Arc};

use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Request, Response, Status,
    metadata::MetadataValue,
    service::{Interceptor, interceptor::InterceptedService},
    transport::{Certificate, Channel, ClientTlsConfig, Identity, ServerTlsConfig},
};

use crate::{
    admission::{
        AdmissionError, ClientAdmissionRequest, ClientPermit, SessionRecord, SourceAdmissionRequest,
    },
    config::{CasterConfig, ConfigSnapshot},
    coordinator_admission::AdmissionCoordinator,
    source_claim::{ClaimError, SourceClaimRequest},
};

pub mod proto {
    tonic::include_proto!("rcaster.admission.v1");
}

use proto::{
    AdmissionError as ProtoAdmissionError, AdmitClientRequest, AdmitClientResponse,
    AdmitClientSuccess, AdmitSourceRequest, AdmitSourceResponse, AdmitSourceSuccess,
    admission_service_client::AdmissionServiceClient,
    admission_service_server::{AdmissionService, AdmissionServiceServer},
    admit_client_response, admit_source_response,
};

#[derive(Clone)]
pub struct GrpcAdmissionClient {
    inner: AdmissionServiceClient<InterceptedService<Channel, ClientTokenInterceptor>>,
}

#[derive(Clone)]
pub struct GrpcClientTls {
    pub ca_certificate_pem: String,
    pub certificate_pem: String,
    pub private_key_pem: String,
    pub server_name: String,
    pub service_token: String,
}

#[derive(Debug)]
pub struct GrpcSourcePermit {
    pub session: SessionRecord,
    pub source_generation: u64,
}

#[derive(Clone)]
pub struct GrpcServerTls {
    pub certificate_pem: String,
    pub private_key_pem: String,
    pub client_ca_pem: String,
}

#[derive(Clone)]
pub struct ServiceToken {
    secret: String,
    cluster_id: String,
    scopes: Vec<String>,
}

impl ServiceToken {
    pub fn new(
        secret: impl Into<String>,
        cluster_id: impl Into<String>,
        scopes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            secret: secret.into(),
            cluster_id: cluster_id.into(),
            scopes: scopes.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone)]
pub struct AdmissionAccessPolicy {
    cluster_id: String,
    tokens: Vec<ServiceToken>,
}

impl AdmissionAccessPolicy {
    pub fn new(
        cluster_id: impl Into<String>,
        tokens: impl IntoIterator<Item = ServiceToken>,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            tokens: tokens.into_iter().collect(),
        }
    }

    fn authorize(&self, presented: &str) -> Result<(), Status> {
        for token in &self.tokens {
            if token.secret.len() == presented.len()
                && bool::from(token.secret.as_bytes().ct_eq(presented.as_bytes()))
            {
                if token.cluster_id != self.cluster_id
                    || !token.scopes.iter().any(|scope| scope == "admission:write")
                {
                    return Err(Status::permission_denied("service token scope denied"));
                }
                return Ok(());
            }
        }
        Err(Status::unauthenticated("invalid service token"))
    }
}

#[derive(Clone)]
struct ClientTokenInterceptor {
    token: Option<String>,
}

impl Interceptor for ClientTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(token) = &self.token {
            let value = MetadataValue::try_from(format!("Bearer {token}"))
                .map_err(|_| Status::internal("invalid authorization metadata"))?;
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }
}

#[derive(Clone)]
struct ServerTokenInterceptor {
    policy: Arc<AdmissionAccessPolicy>,
}

impl Interceptor for ServerTokenInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let authorization = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("missing service token"))?;
        let token = authorization
            .strip_prefix("Bearer ")
            .ok_or_else(|| Status::unauthenticated("invalid authorization scheme"))?;
        self.policy.authorize(token)?;
        Ok(request)
    }
}

impl GrpcAdmissionClient {
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, AdmissionError> {
        let channel = Channel::from_shared(endpoint.into())
            .map_err(|_| AdmissionError::CoordinatorUnavailable)?
            .connect()
            .await
            .map_err(|_| AdmissionError::CoordinatorUnavailable)?;
        Ok(Self {
            inner: AdmissionServiceClient::with_interceptor(
                channel,
                ClientTokenInterceptor { token: None },
            ),
        })
    }

    pub async fn connect_mtls(
        endpoint: impl Into<String>,
        tls: GrpcClientTls,
    ) -> Result<Self, AdmissionError> {
        let tls_config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(tls.ca_certificate_pem))
            .identity(Identity::from_pem(tls.certificate_pem, tls.private_key_pem))
            .domain_name(tls.server_name);
        let channel = Channel::from_shared(endpoint.into())
            .map_err(|_| AdmissionError::CoordinatorUnavailable)?
            .tls_config(tls_config)
            .map_err(|_| AdmissionError::CoordinatorUnavailable)?
            .connect()
            .await
            .map_err(|_| AdmissionError::Unauthenticated)?;
        Ok(Self {
            inner: AdmissionServiceClient::with_interceptor(
                channel,
                ClientTokenInterceptor {
                    token: Some(tls.service_token),
                },
            ),
        })
    }

    pub async fn admit_client(
        &mut self,
        lease_id: i64,
        request: ClientAdmissionRequest,
    ) -> Result<ClientPermit, AdmissionError> {
        let response = self
            .inner
            .admit_client(request_to_proto(lease_id, &request))
            .await
            .map_err(status_to_admission_error)?
            .into_inner();

        match response.result {
            Some(admit_client_response::Result::Success(_)) => Ok(ClientPermit {
                session: request.session,
            }),
            Some(admit_client_response::Result::Error(error)) => Err(error_from_proto(error.code)),
            None => Err(AdmissionError::CoordinatorUnavailable),
        }
    }

    pub async fn admit_source(
        &mut self,
        lease_id: i64,
        request: SourceAdmissionRequest,
    ) -> Result<GrpcSourcePermit, AdmissionError> {
        let response = self
            .inner
            .admit_source(source_request_to_proto(lease_id, &request))
            .await
            .map_err(status_to_admission_error)?
            .into_inner();

        match response.result {
            Some(admit_source_response::Result::Success(success)) => Ok(GrpcSourcePermit {
                session: request.session,
                source_generation: success.source_generation,
            }),
            Some(admit_source_response::Result::Error(error)) => Err(error_from_proto(error.code)),
            None => Err(AdmissionError::CoordinatorUnavailable),
        }
    }
}

pub struct AdmissionGrpcService {
    coordinator: Arc<AdmissionCoordinator>,
}

impl AdmissionGrpcService {
    pub fn new(coordinator: Arc<AdmissionCoordinator>) -> Self {
        Self { coordinator }
    }
}

#[tonic::async_trait]
impl AdmissionService for AdmissionGrpcService {
    async fn admit_client(
        &self,
        request: Request<AdmitClientRequest>,
    ) -> Result<Response<AdmitClientResponse>, Status> {
        let request = request.into_inner();
        let lease_id = request.caster_lease_id;
        let request = request_from_proto(request);
        let response = match self.coordinator.admit_client(lease_id, request).await {
            Ok(_) => AdmitClientResponse {
                result: Some(admit_client_response::Result::Success(
                    AdmitClientSuccess {},
                )),
            },
            Err(error) => AdmitClientResponse {
                result: Some(admit_client_response::Result::Error(error_to_proto(&error))),
            },
        };
        Ok(Response::new(response))
    }

    async fn admit_source(
        &self,
        request: Request<AdmitSourceRequest>,
    ) -> Result<Response<AdmitSourceResponse>, Status> {
        let request = request.into_inner();
        let lease_id = request.caster_lease_id;
        let request = source_request_from_proto(request);
        let response = match self.coordinator.admit_source(lease_id, request).await {
            Ok(permit) => AdmitSourceResponse {
                result: Some(admit_source_response::Result::Success(AdmitSourceSuccess {
                    source_generation: permit.claimed_source.claim.source_generation,
                })),
            },
            Err(error) => AdmitSourceResponse {
                result: Some(admit_source_response::Result::Error(error_to_proto(&error))),
            },
        };
        Ok(Response::new(response))
    }
}

pub async fn serve_admission_grpc<F>(
    listener: TcpListener,
    coordinator: Arc<AdmissionCoordinator>,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    tonic::transport::Server::builder()
        .add_service(AdmissionServiceServer::new(AdmissionGrpcService::new(
            coordinator,
        )))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await
}

pub async fn serve_admission_grpc_mtls<F>(
    listener: TcpListener,
    coordinator: Arc<AdmissionCoordinator>,
    tls: GrpcServerTls,
    policy: AdmissionAccessPolicy,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    let tls_config = ServerTlsConfig::new()
        .identity(Identity::from_pem(tls.certificate_pem, tls.private_key_pem))
        .client_ca_root(Certificate::from_pem(tls.client_ca_pem))
        .client_auth_optional(false);
    tonic::transport::Server::builder()
        .tls_config(tls_config)?
        .add_service(AdmissionServiceServer::with_interceptor(
            AdmissionGrpcService::new(coordinator),
            ServerTokenInterceptor {
                policy: Arc::new(policy),
            },
        ))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await
}

fn request_to_proto(lease_id: i64, request: &ClientAdmissionRequest) -> AdmitClientRequest {
    AdmitClientRequest {
        caster_lease_id: lease_id,
        session_id: request.session.session_id.clone(),
        user_id: request.session.user_id.clone(),
        mountpoint: request.session.mountpoint.clone(),
        caster_id: request.session.caster_id.clone(),
        runtime_epoch: request.session.runtime_epoch.clone(),
        client_ip: request.session.client_ip.clone().unwrap_or_default(),
        max_sessions: request.max_sessions as u64,
        ip_max_sessions: request.ip_max_sessions.map(|value| value as u64),
        started_at: request.session.started_at.clone(),
        config_generation: request.config.config.generation,
        config_revision: request.config.revision,
    }
}

fn request_from_proto(request: AdmitClientRequest) -> ClientAdmissionRequest {
    let client_ip = (!request.client_ip.is_empty()).then_some(request.client_ip);
    let mut session = SessionRecord::client(
        request.session_id,
        request.user_id,
        request.mountpoint,
        request.caster_id,
        request.runtime_epoch,
        client_ip.clone().unwrap_or_default(),
        request.started_at,
    );
    session.client_ip = client_ip;
    ClientAdmissionRequest::new(
        session,
        request.max_sessions as usize,
        request.ip_max_sessions.map(|value| value as usize),
        ConfigSnapshot {
            config: CasterConfig {
                schema_version: 1,
                generation: request.config_generation,
            },
            revision: request.config_revision,
        },
    )
}

fn source_request_to_proto(lease_id: i64, request: &SourceAdmissionRequest) -> AdmitSourceRequest {
    AdmitSourceRequest {
        caster_lease_id: lease_id,
        session_id: request.session.session_id.clone(),
        user_id: request.session.user_id.clone(),
        mountpoint: request.session.mountpoint.clone(),
        caster_id: request.session.caster_id.clone(),
        runtime_epoch: request.session.runtime_epoch.clone(),
        max_sessions: request.max_sessions as u64,
        started_at: request.session.started_at.clone(),
        connection_id: request.source_claim.connection_id.clone(),
        relay_endpoint: request.source_claim.relay_endpoint.clone(),
        config_generation: request.config.config.generation,
        config_revision: request.config.revision,
    }
}

fn source_request_from_proto(request: AdmitSourceRequest) -> SourceAdmissionRequest {
    let created_at = request.started_at.clone();
    SourceAdmissionRequest::new(
        SessionRecord::source(
            request.session_id,
            request.user_id.clone(),
            request.mountpoint.clone(),
            request.caster_id.clone(),
            request.runtime_epoch.clone(),
            request.started_at,
        ),
        request.max_sessions as usize,
        SourceClaimRequest::new(
            request.mountpoint,
            request.caster_id,
            request.connection_id,
            request.runtime_epoch,
            request.relay_endpoint,
            request.user_id,
            created_at,
        ),
        ConfigSnapshot {
            config: CasterConfig {
                schema_version: 1,
                generation: request.config_generation,
            },
            revision: request.config_revision,
        },
    )
}

fn error_to_proto(error: &AdmissionError) -> ProtoAdmissionError {
    ProtoAdmissionError {
        code: error_code(error).to_owned(),
        message: error.to_string(),
    }
}

fn error_from_proto(code: String) -> AdmissionError {
    match code.as_str() {
        "USER_QUOTA_EXCEEDED" => AdmissionError::UserQuotaExceeded,
        "IP_QUOTA_EXCEEDED" => AdmissionError::IpQuotaExceeded,
        "CONFIG_STALE" => AdmissionError::ConfigStale,
        "ADMISSION_CONFLICT" => AdmissionError::AdmissionConflict,
        "UNAUTHENTICATED" => AdmissionError::Unauthenticated,
        "PERMISSION_DENIED" => AdmissionError::PermissionDenied,
        "SOURCE_ALREADY_CLAIMED" => AdmissionError::Source(ClaimError::AlreadyClaimed),
        _ => AdmissionError::CoordinatorUnavailable,
    }
}

fn error_code(error: &AdmissionError) -> &'static str {
    match error {
        AdmissionError::UserQuotaExceeded => "USER_QUOTA_EXCEEDED",
        AdmissionError::IpQuotaExceeded => "IP_QUOTA_EXCEEDED",
        AdmissionError::ConfigStale => "CONFIG_STALE",
        AdmissionError::AdmissionConflict => "ADMISSION_CONFLICT",
        AdmissionError::CoordinatorUnavailable => "COORDINATOR_UNAVAILABLE",
        AdmissionError::Unauthenticated => "UNAUTHENTICATED",
        AdmissionError::PermissionDenied => "PERMISSION_DENIED",
        AdmissionError::Source(ClaimError::AlreadyClaimed) => "SOURCE_ALREADY_CLAIMED",
        _ => "ADMISSION_FAILED",
    }
}

fn status_to_admission_error(status: Status) -> AdmissionError {
    match status.code() {
        tonic::Code::Unauthenticated => AdmissionError::Unauthenticated,
        tonic::Code::PermissionDenied => AdmissionError::PermissionDenied,
        _ => AdmissionError::CoordinatorUnavailable,
    }
}
