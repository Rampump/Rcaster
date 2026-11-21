use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, ConnectOptions};
use rcaster_service::{
    admission::{AdmissionError, ClientAdmissionRequest, SessionRecord, read_user_session_count},
    config::{CasterConfig, publish_config},
    coordinator_admission::{AdmissionCoordinator, CoordinatorOptions},
    coordinator_grpc::{
        AdmissionAccessPolicy, GrpcAdmissionClient, GrpcClientTls, GrpcServerTls, ServiceToken,
        proto, serve_admission_grpc_mtls,
    },
};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use tokio::{net::TcpListener, sync::oneshot};
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

#[tokio::test]
async fn mtls_and_service_scope_guard_admission() {
    let cluster = unique("grpc-mtls");
    let user_id = "u_secure";
    let mut admin = connect().await;
    let config = publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let pki = TestPki::generate();
    let (coordinator, shutdown, address, server) = start_server(&cluster, &pki).await;
    let endpoint = format!("https://{address}");
    let mut trusted = GrpcAdmissionClient::connect_mtls(
        endpoint.clone(),
        pki.client_tls("admission-good", "localhost"),
    )
    .await
    .expect("trusted mtls client");

    trusted
        .admit_client(
            grant_lease(&mut admin).await,
            ClientAdmissionRequest::new(
                SessionRecord::client(
                    "s_secure",
                    user_id,
                    "C63",
                    "caster-east-01",
                    "boot_east",
                    "203.0.113.100",
                    "2026-11-22T07:00:00Z",
                ),
                3,
                None,
                config.clone(),
            ),
        )
        .await
        .expect("trusted admission");

    let mut invalid = GrpcAdmissionClient::connect_mtls(
        endpoint.clone(),
        pki.client_tls("invalid-token", "localhost"),
    )
    .await
    .expect("invalid-token client");
    let invalid_result = invalid
        .admit_client(
            grant_lease(&mut admin).await,
            secure_request(user_id, "s_invalid", config.clone()),
        )
        .await;
    assert!(matches!(
        invalid_result,
        Err(AdmissionError::Unauthenticated)
    ));

    let mut read_only = GrpcAdmissionClient::connect_mtls(
        endpoint.clone(),
        pki.client_tls("admission-read-only", "localhost"),
    )
    .await
    .expect("read-only client");
    let permission = read_only
        .admit_client(
            grant_lease(&mut admin).await,
            secure_request(user_id, "s_read_only", config.clone()),
        )
        .await;
    assert!(matches!(permission, Err(AdmissionError::PermissionDenied)));

    let mut wrong_cluster = GrpcAdmissionClient::connect_mtls(
        endpoint.clone(),
        pki.client_tls("wrong-cluster", "localhost"),
    )
    .await
    .expect("wrong-cluster client");
    let cluster_permission = wrong_cluster
        .admit_client(
            grant_lease(&mut admin).await,
            secure_request(user_id, "s_wrong_cluster", config),
        )
        .await;
    assert!(matches!(
        cluster_permission,
        Err(AdmissionError::PermissionDenied)
    ));

    let no_client_cert = Channel::from_shared(endpoint)
        .expect("endpoint")
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(pki.ca_certificate.clone()))
                .domain_name("localhost"),
        )
        .expect("tls config")
        .connect()
        .await;
    if let Ok(channel) = no_client_cert {
        let mut client = proto::admission_service_client::AdmissionServiceClient::new(channel);
        let result = client
            .admit_client(proto::AdmitClientRequest::default())
            .await;
        assert!(result.is_err());
    }
    assert_eq!(
        read_user_session_count(&mut admin, &cluster, user_id)
            .await
            .expect("count sessions"),
        1
    );

    drop(trusted);
    drop(invalid);
    drop(read_only);
    drop(wrong_cluster);
    let _ = shutdown.send(());
    server.await.expect("grpc server");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

fn secure_request(
    user_id: &str,
    session_id: &str,
    config: rcaster_service::config::ConfigSnapshot,
) -> ClientAdmissionRequest {
    ClientAdmissionRequest::new(
        SessionRecord::client(
            session_id,
            user_id,
            "C63",
            "caster-east-01",
            "boot_east",
            "203.0.113.101",
            "2026-11-22T07:00:01Z",
        ),
        3,
        None,
        config,
    )
}

async fn start_server(
    cluster: &str,
    pki: &TestPki,
) -> (
    Arc<AdmissionCoordinator>,
    oneshot::Sender<()>,
    std::net::SocketAddr,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind grpc listener");
    let address = listener.local_addr().expect("listener address");
    let coordinator =
        AdmissionCoordinator::start(connect().await, cluster, CoordinatorOptions::default())
            .await
            .expect("start coordinator");
    let coordinator = Arc::new(coordinator);
    let server_coordinator = coordinator.clone();
    let policy = AdmissionAccessPolicy::new(
        cluster,
        [
            ServiceToken::new("admission-good", cluster, ["admission:write"]),
            ServiceToken::new("admission-read-only", cluster, ["admission:read"]),
            ServiceToken::new("wrong-cluster", "other-cluster", ["admission:write"]),
        ],
    );
    let server_tls = pki.server_tls();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        serve_admission_grpc_mtls(
            listener,
            server_coordinator,
            server_tls,
            policy,
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await
        .expect("serve mtls grpc");
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    (coordinator, shutdown_tx, address, task)
}

struct TestPki {
    ca_certificate: String,
    server_certificate: String,
    server_key: String,
    client_certificate: String,
    client_key: String,
}

impl TestPki {
    fn generate() -> Self {
        let mut ca_params = CertificateParams::new(Vec::new()).expect("CA params");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "rcaster-test-ca");
        ca_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let ca_key = KeyPair::generate().expect("CA key");
        let ca_certificate = ca_params.self_signed(&ca_key).expect("CA certificate");

        let server_key = KeyPair::generate().expect("server key");
        let mut server_params =
            CertificateParams::new(vec!["localhost".to_owned()]).expect("server params");
        server_params
            .distinguished_name
            .push(DnType::CommonName, "localhost");
        server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_certificate = server_params
            .signed_by(&server_key, &ca_certificate, &ca_key)
            .expect("server certificate");

        let client_key = KeyPair::generate().expect("client key");
        let mut client_params =
            CertificateParams::new(vec!["rcaster-client".to_owned()]).expect("client params");
        client_params
            .distinguished_name
            .push(DnType::CommonName, "rcaster-client");
        client_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_certificate = client_params
            .signed_by(&client_key, &ca_certificate, &ca_key)
            .expect("client certificate");

        Self {
            ca_certificate: ca_certificate.pem(),
            server_certificate: server_certificate.pem(),
            server_key: server_key.serialize_pem(),
            client_certificate: client_certificate.pem(),
            client_key: client_key.serialize_pem(),
        }
    }

    fn server_tls(&self) -> GrpcServerTls {
        GrpcServerTls {
            certificate_pem: self.server_certificate.clone(),
            private_key_pem: self.server_key.clone(),
            client_ca_pem: self.ca_certificate.clone(),
        }
    }

    fn client_tls(&self, token: &str, server_name: &str) -> GrpcClientTls {
        GrpcClientTls {
            ca_certificate_pem: self.ca_certificate.clone(),
            certificate_pem: self.client_certificate.clone(),
            private_key_pem: self.client_key.clone(),
            server_name: server_name.to_owned(),
            service_token: token.to_owned(),
        }
    }
}

async fn connect() -> Client {
    let endpoint = std::env::var("RCASTER_ETCD_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned());
    Client::connect([endpoint], Some(ConnectOptions::new()))
        .await
        .expect("etcd must be running")
}

async fn grant_lease(client: &mut Client) -> i64 {
    client
        .lease_grant(30, None)
        .await
        .expect("grant lease")
        .id()
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    )
}
