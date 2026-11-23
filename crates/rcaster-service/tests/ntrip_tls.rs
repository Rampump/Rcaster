use std::{sync::Arc, time::Duration};

use rcaster_service::{
    hub::MountpointHub,
    ntrip_server::{
        AuthError, AuthorizedUser, ClientAuthorizer, ClientCredentials, NtripServerConfig,
        NtripTlsAcceptor, SourceAuthorizer, SourceCredentials, serve_ntrip_tls,
    },
    session_limit::{ConnectionLimiter, LocalSessionLimiter, SessionLimitConfig},
};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_rustls::TlsConnector;

struct TestAuthorizer;

impl SourceAuthorizer for TestAuthorizer {
    async fn authorize_source(
        &self,
        _mountpoint: &str,
        credentials: SourceCredentials<'_>,
    ) -> Result<AuthorizedUser, AuthError> {
        if matches!(
            credentials,
            SourceCredentials::Ntrip1 { password: "secret" }
        ) {
            Ok(AuthorizedUser {
                user_id: "owner".to_owned(),
                username: "owner".to_owned(),
                auth_version: 1,
                user_mod_revision: 0,
                mountpoint_mod_revision: 0,
                source_expectation: None,
            })
        } else {
            Err(AuthError::Unauthorized)
        }
    }
}

impl ClientAuthorizer for TestAuthorizer {
    async fn authorize_client(
        &self,
        _mountpoint: &str,
        _credentials: Option<ClientCredentials<'_>>,
    ) -> Result<AuthorizedUser, AuthError> {
        Err(AuthError::Unauthorized)
    }
}

#[tokio::test]
async fn tls_listener_accepts_ntrip1_source() {
    let pki = TestPki::generate();
    let acceptor =
        NtripTlsAcceptor::from_pem(&pki.server_certificate, &pki.server_key).expect("TLS acceptor");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind TLS listener");
    let address = listener.local_addr().expect("listener address");
    let hub = Arc::new(MountpointHub::new(64).expect("hub"));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_ntrip_tls(
        listener,
        acceptor,
        hub,
        TestAuthorizer,
        NtripServerConfig {
            head_timeout: Duration::from_secs(1),
            content_length_max_bytes: 1024,
            client_auth_required: false,
            session_limiter: Arc::new(
                LocalSessionLimiter::new(SessionLimitConfig::default()).expect("session limiter"),
            ),
            connection_limiter: Arc::new(ConnectionLimiter::new(100).expect("connections")),
            cluster_runtime: None,
            relay_manager: None,
            source_admission: None,
            near: None,
            metrics: None,
            connections: None,
            proxy_protocol: None,
            auth_login_limiter: None,
        },
        Duration::from_secs(1),
        async {
            let _ = shutdown_rx.await;
        },
    ));

    let mut roots = RootCertStore::empty();
    let (trusted, ignored) = roots.add_parsable_certificates(pki.ca_der);
    assert_eq!(trusted, 1);
    assert_eq!(ignored, 0);
    let client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client_config));
    let tcp = TcpStream::connect(address).await.expect("TCP connect");
    let mut tls = connector
        .connect(ServerName::try_from("localhost").expect("server name"), tcp)
        .await
        .expect("TLS handshake");
    tls.write_all(b"SOURCE secret /C63\r\n\r\n")
        .await
        .expect("source request");
    let mut response = vec![0_u8; b"ICY 200 OK\r\n\r\n".len()];
    tls.read_exact(&mut response)
        .await
        .expect("source response");
    assert_eq!(response, b"ICY 200 OK\r\n\r\n");

    shutdown_tx.send(()).expect("shutdown");
    server
        .await
        .expect("server join")
        .expect("TLS NTRIP server");
}

struct TestPki {
    ca_der: Vec<rustls::pki_types::CertificateDer<'static>>,
    server_certificate: String,
    server_key: String,
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

        Self {
            ca_der: vec![ca_certificate.der().clone()],
            server_certificate: server_certificate.pem(),
            server_key: server_key.serialize_pem(),
        }
    }
}
