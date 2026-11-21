use std::{
    error::Error,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, ConnectOptions};
use rcaster_service::{
    admission::{ClientAdmissionRequest, SessionRecord},
    config::{CasterConfig, publish_config},
    coordinator_admission::{AdmissionCoordinator, CoordinatorOptions},
    coordinator_grpc::{
        AdmissionAccessPolicy, GrpcAdmissionClient, GrpcClientTls, GrpcServerTls, ServiceToken,
    },
    coordinator_mtls_server::ReloadableAdmissionMtlsServer,
};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoints = std::env::var("RCASTER_ETCD_ENDPOINTS")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned())
        .split(',')
        .map(str::trim)
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let run_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let cluster = format!("admission-rtt-{run_id}");
    let mut admin = connect(endpoints.clone()).await?;
    let config = publish_config(&mut admin, &cluster, CasterConfig::new(1)).await?;
    let lease_id = admin.lease_grant(600, None).await?.id();
    let coordinator = Arc::new(
        AdmissionCoordinator::start(
            connect(endpoints).await?,
            &cluster,
            CoordinatorOptions::default(),
        )
        .await?,
    );
    let pki = TestPki::generate()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_address = listener.local_addr()?;
    let server = ReloadableAdmissionMtlsServer::start(
        listener,
        coordinator,
        pki.server_tls(),
        AdmissionAccessPolicy::new(
            &cluster,
            [ServiceToken::new(
                "rtt-token",
                &cluster,
                ["admission:write"],
            )],
        ),
    )?;

    for rtt_ms in [20, 50, 100, 250] {
        let proxy_port = spawn_proxy(server_address.port(), rtt_ms).await?;
        let mut client = GrpcAdmissionClient::connect_mtls(
            format!("https://127.0.0.1:{proxy_port}"),
            pki.client_tls("rtt-token", "localhost"),
        )
        .await?;
        let user_id = format!("u_rtt_{rtt_ms}");
        let mut latencies = Vec::new();
        let mut failures = 0;

        for sequence in 0..30 {
            let started = Instant::now();
            let result = client
                .admit_client(
                    lease_id,
                    ClientAdmissionRequest::new(
                        SessionRecord::client(
                            format!("s_{rtt_ms}_{sequence}"),
                            &user_id,
                            "C63",
                            "rtt-caster",
                            "rtt-boot",
                            "198.51.100.20",
                            "2026-11-22T09:00:00Z",
                        ),
                        100,
                        None,
                        config.clone(),
                    ),
                )
                .await;
            if result.is_ok() {
                latencies.push(started.elapsed());
            } else {
                failures += 1;
            }
        }
        latencies.sort_unstable();
        println!(
            concat!(
                "{{\"simulated_rtt_ms\":{},\"successes\":{},\"failures\":{},",
                "\"p50_ms\":{:.3},\"p95_ms\":{:.3},\"p99_ms\":{:.3}}}"
            ),
            rtt_ms,
            latencies.len(),
            failures,
            percentile_ms(&latencies, 0.50),
            percentile_ms(&latencies, 0.95),
            percentile_ms(&latencies, 0.99),
        );
    }

    server.shutdown().await?;
    Ok(())
}

async fn spawn_proxy(upstream_port: u16, rtt_ms: u64) -> Result<u16, Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let one_way_delay = Duration::from_secs_f64(rtt_ms as f64 / 2000.0);
    tokio::spawn(async move {
        while let Ok((inbound, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Ok(upstream) = TcpStream::connect(("127.0.0.1", upstream_port)).await else {
                    return;
                };
                let (mut inbound_reader, mut inbound_writer) = inbound.into_split();
                let (mut upstream_reader, mut upstream_writer) = upstream.into_split();
                tokio::join!(
                    forward(&mut inbound_reader, &mut upstream_writer, one_way_delay),
                    forward(&mut upstream_reader, &mut inbound_writer, one_way_delay),
                );
            });
        }
    });
    Ok(port)
}

async fn forward<R, W>(reader: &mut R, writer: &mut W, delay: Duration)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let Ok(read) = reader.read(&mut buffer).await else {
            return;
        };
        if read == 0 {
            return;
        }
        tokio::time::sleep(delay).await;
        if writer.write_all(&buffer[..read]).await.is_err() {
            return;
        }
    }
}

fn percentile_ms(values: &[Duration], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let index = ((values.len() - 1) as f64 * percentile).round() as usize;
    values[index].as_secs_f64() * 1000.0
}

async fn connect(endpoints: Vec<String>) -> Result<Client, Box<dyn Error>> {
    Ok(Client::connect(endpoints, Some(ConnectOptions::new())).await?)
}

struct TestPki {
    ca_certificate: String,
    server_certificate: String,
    server_key: String,
    client_certificate: String,
    client_key: String,
}

impl TestPki {
    fn generate() -> Result<Self, Box<dyn Error>> {
        let mut ca_params = CertificateParams::new(Vec::new())?;
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "rcaster-rtt-ca");
        ca_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let ca_key = KeyPair::generate()?;
        let ca_certificate = ca_params.self_signed(&ca_key)?;

        let server_key = KeyPair::generate()?;
        let mut server_params = CertificateParams::new(vec!["localhost".to_owned()])?;
        server_params
            .distinguished_name
            .push(DnType::CommonName, "localhost");
        server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_certificate = server_params.signed_by(&server_key, &ca_certificate, &ca_key)?;

        let client_key = KeyPair::generate()?;
        let mut client_params = CertificateParams::new(vec!["rcaster-rtt-client".to_owned()])?;
        client_params
            .distinguished_name
            .push(DnType::CommonName, "rcaster-rtt-client");
        client_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_certificate = client_params.signed_by(&client_key, &ca_certificate, &ca_key)?;

        Ok(Self {
            ca_certificate: ca_certificate.pem(),
            server_certificate: server_certificate.pem(),
            server_key: server_key.serialize_pem(),
            client_certificate: client_certificate.pem(),
            client_key: client_key.serialize_pem(),
        })
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
