use std::{
    fmt, io,
    sync::{Arc, RwLock},
};

use rustls::{RootCertStore, ServerConfig, server::WebPkiClientVerifier};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_rustls::{TlsAcceptor, server::TlsStream};
use tokio_stream::wrappers::ReceiverStream;

use crate::coordinator_grpc::{
    AdmissionAccessPolicy, AdmissionGrpcService, GrpcServerTls, ServerTokenInterceptor,
    proto::admission_service_server::AdmissionServiceServer,
};

#[derive(Debug)]
pub enum GrpcTlsError {
    Io(io::Error),
    Certificate(String),
    MissingPrivateKey,
    Rustls(String),
    Task(String),
}

impl fmt::Display for GrpcTlsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::Certificate(message) => write!(formatter, "certificate error: {message}"),
            Self::MissingPrivateKey => formatter.write_str("private key is missing"),
            Self::Rustls(message) => write!(formatter, "TLS error: {message}"),
            Self::Task(message) => write!(formatter, "TLS server task error: {message}"),
        }
    }
}

impl std::error::Error for GrpcTlsError {}

impl From<io::Error> for GrpcTlsError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub struct ReloadableAdmissionMtlsServer {
    current: Arc<RwLock<Arc<ServerConfig>>>,
    shutdown: Option<oneshot::Sender<()>>,
    accept_task: JoinHandle<()>,
    server_task: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl ReloadableAdmissionMtlsServer {
    pub fn start(
        listener: TcpListener,
        coordinator: Arc<crate::coordinator_admission::AdmissionCoordinator>,
        tls: GrpcServerTls,
        policy: AdmissionAccessPolicy,
    ) -> Result<Self, GrpcTlsError> {
        let current = Arc::new(RwLock::new(server_config(&tls)?));
        let (stream_sender, stream_receiver) = mpsc::channel(256);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let accept_task = tokio::spawn(accept_loop(
            listener,
            current.clone(),
            stream_sender,
            shutdown_rx,
        ));
        let service = tonic::transport::Server::builder().add_service(
            AdmissionServiceServer::with_interceptor(
                AdmissionGrpcService::new(coordinator),
                ServerTokenInterceptor {
                    policy: Arc::new(policy),
                },
            ),
        );
        let server_task =
            tokio::spawn(service.serve_with_incoming(ReceiverStream::new(stream_receiver)));

        Ok(Self {
            current,
            shutdown: Some(shutdown_tx),
            accept_task,
            server_task,
        })
    }

    pub fn rotate(&self, tls: GrpcServerTls) -> Result<(), GrpcTlsError> {
        let config = server_config(&tls)?;
        let mut current = self
            .current
            .write()
            .map_err(|_| GrpcTlsError::Rustls("TLS configuration lock poisoned".to_owned()))?;
        *current = config;
        Ok(())
    }

    pub async fn shutdown(mut self) -> Result<(), GrpcTlsError> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let _ = self.accept_task.await;
        match self.server_task.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(GrpcTlsError::Task(error.to_string())),
            Err(error) => Err(GrpcTlsError::Task(error.to_string())),
        }
    }
}

async fn accept_loop(
    listener: TcpListener,
    current: Arc<RwLock<Arc<ServerConfig>>>,
    stream_sender: mpsc::Sender<Result<TlsStream<TcpStream>, io::Error>>,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let Ok((socket, _)) = accepted else {
                    break;
                };
                let Ok(config) = current.read().map(|config| config.clone()) else {
                    break;
                };
                let sender = stream_sender.clone();
                tokio::spawn(async move {
                    if let Ok(stream) = TlsAcceptor::from(config).accept(socket).await {
                        let _ = sender.send(Ok(stream)).await;
                    }
                });
            }
        }
    }
}

fn server_config(tls: &GrpcServerTls) -> Result<Arc<ServerConfig>, GrpcTlsError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certificates = rustls_pemfile::certs(&mut tls.certificate_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| GrpcTlsError::Certificate(error.to_string()))?;
    let private_key = rustls_pemfile::private_key(&mut tls.private_key_pem.as_bytes())
        .map_err(|error| GrpcTlsError::Certificate(error.to_string()))?
        .ok_or(GrpcTlsError::MissingPrivateKey)?;
    let ca_certificates = rustls_pemfile::certs(&mut tls.client_ca_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| GrpcTlsError::Certificate(error.to_string()))?;
    let mut roots = RootCertStore::empty();
    let (_, ignored) = roots.add_parsable_certificates(ca_certificates);
    if ignored > 0 {
        return Err(GrpcTlsError::Certificate(
            "client CA contains invalid certificates".to_owned(),
        ));
    }
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|error| GrpcTlsError::Rustls(error.to_string()))?;
    let mut config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certificates, private_key)
        .map_err(|error| GrpcTlsError::Rustls(error.to_string()))?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(config))
}
