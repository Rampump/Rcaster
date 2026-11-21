use std::{future::Future, sync::Arc};

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Channel};

use crate::{
    admission::{AdmissionError, ClientAdmissionRequest, ClientPermit, SessionRecord},
    config::{CasterConfig, ConfigSnapshot},
    coordinator_admission::AdmissionCoordinator,
};

pub mod proto {
    tonic::include_proto!("rcaster.admission.v1");
}

use proto::{
    AdmissionError as ProtoAdmissionError, AdmitClientRequest, AdmitClientResponse,
    AdmitClientSuccess,
    admission_service_client::AdmissionServiceClient,
    admission_service_server::{AdmissionService, AdmissionServiceServer},
    admit_client_response,
};

#[derive(Clone)]
pub struct GrpcAdmissionClient {
    inner: AdmissionServiceClient<Channel>,
}

impl GrpcAdmissionClient {
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, AdmissionError> {
        let channel = Channel::from_shared(endpoint.into())
            .map_err(|_| AdmissionError::CoordinatorUnavailable)?
            .connect()
            .await
            .map_err(|_| AdmissionError::CoordinatorUnavailable)?;
        Ok(Self {
            inner: AdmissionServiceClient::new(channel),
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
            .map_err(|_| AdmissionError::CoordinatorUnavailable)?
            .into_inner();

        match response.result {
            Some(admit_client_response::Result::Success(_)) => Ok(ClientPermit {
                session: request.session,
            }),
            Some(admit_client_response::Result::Error(error)) => Err(error_from_proto(error.code)),
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
        _ => "ADMISSION_FAILED",
    }
}
