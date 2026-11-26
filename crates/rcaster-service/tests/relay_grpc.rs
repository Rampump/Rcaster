use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, ConnectOptions};
use rcaster_core::{protocol::DataProfile, str_record::StrRecord};
use rcaster_service::{
    cluster_runtime::{ClusterRuntimeOptions, SharedClusterRuntime},
    config::{CasterConfig, publish_config},
    coordinator_admission::{AdmissionCoordinator, CoordinatorOptions},
    coordinator_grpc::{
        AdmissionAccessPolicy, GrpcClientTls, GrpcServerTls, ServiceToken,
        proto::{RelayHealthRequest, RelayOpenRequest, relay_service_server::RelayService},
    },
    hub::MountpointHub,
    relay::RelayManager,
    relay_grpc::{RelayGrpcClient, RelayGrpcService, serve_relay_grpc_mtls_with_limit},
};
use rcgen::{
    BasicConstraints, CertificateParams, CertificateRevocationListParams, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyIdMethod, KeyPair, KeyUsagePurpose, RevocationReason,
    RevokedCertParams, SerialNumber, date_time_ymd,
};
use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::StreamExt;
use tonic::Request;

#[tokio::test]
async fn relay_requires_current_source_generation_and_connection() {
    let cluster = unique("relay-route");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("coordinator"),
    );
    let runtime = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        ClusterRuntimeOptions {
            cluster_id: cluster.clone(),
            caster_id: "caster-owner".to_owned(),
            runtime_epoch: unique("epoch"),
            relay_endpoint: "127.0.0.1:12102".to_owned(),
            lease_ttl_s: 30,
            keep_alive: true,
        },
    )
    .await
    .expect("runtime");
    let hub = Arc::new(MountpointHub::new(16).expect("hub"));
    let source = hub.claim_source("C63").expect("hub source");
    let record = StrRecord::from_profile("C63", &DataProfile::default());
    source
        .set_str_record(record.clone())
        .expect("activate hub source");
    let permit = runtime
        .admit_source("owner", "C63", "connection-1", 1)
        .await
        .expect("admit source");
    runtime
        .activate_source(&permit, &record)
        .await
        .expect("activate source");
    let route = wait_for_route(&runtime, "C63").await;

    let service = RelayGrpcService::new(Arc::clone(&hub), runtime.clone());
    let invalid = service
        .open(Request::new(RelayOpenRequest {
            cluster_id: cluster.clone(),
            mountpoint: "C63".to_owned(),
            source_generation: route.source_generation + 1,
            connection_id: route.connection_id.clone(),
            requesting_caster_id: "caster-edge".to_owned(),
        }))
        .await;
    assert!(invalid.is_err());

    let mut stream = service
        .open(Request::new(RelayOpenRequest {
            cluster_id: cluster,
            mountpoint: "C63".to_owned(),
            source_generation: route.source_generation,
            connection_id: route.connection_id,
            requesting_caster_id: "caster-edge".to_owned(),
        }))
        .await
        .expect("relay stream")
        .into_inner();
    source
        .publish(Arc::from(&b"\xD3\x00\x00"[..]))
        .expect("publish");
    let chunk = stream
        .next()
        .await
        .expect("relay chunk")
        .expect("relay data");
    assert_eq!(chunk.data, b"\xD3\x00\x00");

    runtime.shutdown().await.expect("runtime shutdown");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn edge_relay_manager_shares_remote_source_over_mtls() {
    let cluster = unique("relay-edge");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("coordinator"),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("relay listener");
    let relay_address = listener.local_addr().expect("relay address");
    let owner = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-owner", &relay_address.to_string()),
    )
    .await
    .expect("owner runtime");
    let edge = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-edge", "127.0.0.1:1"),
    )
    .await
    .expect("edge runtime");
    let owner_hub = Arc::new(MountpointHub::new(16).expect("owner hub"));
    let source = owner_hub.claim_source("C63").expect("source");
    let record = StrRecord::from_profile("C63", &DataProfile::default());
    source
        .set_str_record(record.clone())
        .expect("active owner source");
    source
        .set_profile(DataProfile::default())
        .expect("owner profile");
    let permit = owner
        .admit_source("owner", "C63", "connection-1", 1)
        .await
        .expect("admit source");
    owner
        .activate_source(&permit, &record)
        .await
        .expect("activate source");
    wait_for_route(&owner, "C63").await;

    let pki = TestPki::generate();
    let token = "relay-token";
    let policy = AdmissionAccessPolicy::new(
        &cluster,
        [ServiceToken::new(token, &cluster, ["relay:read"])],
    );
    let (relay_shutdown_tx, relay_shutdown_rx) = oneshot::channel();
    let crl_path = std::env::temp_dir().join(format!(
        "rcaster-relay-crl-{}-{}.pem",
        std::process::id(),
        cluster
    ));
    std::fs::write(&crl_path, &pki.empty_client_crl).expect("empty CRL");
    let mut server_tls = pki.server_tls();
    server_tls.client_crl_pem = None;
    let task = tokio::spawn(serve_relay_grpc_mtls_with_limit(
        listener,
        Arc::clone(&owner_hub),
        owner.clone(),
        server_tls,
        policy,
        500,
        Some((crl_path.clone(), Duration::from_millis(20))),
        async move {
            let _ = relay_shutdown_rx.await;
        },
    ));
    let mut health_client =
        RelayGrpcClient::connect_mtls(format!("https://{relay_address}"), pki.client_tls(token))
            .await
            .expect("relay health client");
    let route = wait_for_route(&owner, "C63").await;
    let mut revoked_client = RelayGrpcClient::connect_mtls(
        format!("https://{relay_address}"),
        pki.revoked_client_tls(token),
    )
    .await
    .expect("revoked channel");
    revoked_client
        .health(RelayHealthRequest {
            cluster_id: cluster.clone(),
            mountpoint: "C63".to_owned(),
            source_generation: route.source_generation,
            connection_id: route.connection_id.clone(),
        })
        .await
        .expect("non-revoked relay client");
    std::fs::write(&crl_path, &pki.client_crl).expect("revoked CRL");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let mut client = RelayGrpcClient::connect_mtls(
                format!("https://{relay_address}"),
                pki.revoked_client_tls(token),
            )
            .await
            .expect("revoked channel");
            if client
                .health(RelayHealthRequest {
                    cluster_id: cluster.clone(),
                    mountpoint: "C63".to_owned(),
                    source_generation: route.source_generation,
                    connection_id: route.connection_id.clone(),
                })
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("relay CRL reload");
    let health = health_client
        .health(
            rcaster_service::coordinator_grpc::proto::RelayHealthRequest {
                cluster_id: cluster.clone(),
                mountpoint: "C63".to_owned(),
                source_generation: route.source_generation,
                connection_id: route.connection_id,
            },
        )
        .await
        .expect("remote health");
    assert_eq!(health["health"]["packets"], 0);
    let edge_hub = Arc::new(MountpointHub::new(16).expect("edge hub"));
    let manager = Arc::new(RelayManager::with_limits(
        Arc::clone(&edge_hub),
        edge.clone(),
        pki.client_tls(token),
        10,
        2,
        Duration::from_millis(100),
    ));
    assert!(manager.ensure("MISSING").await.is_err());
    assert!(
        manager
            .ensure("MISSING")
            .await
            .expect_err("retry should be cooling down")
            .contains("cooldown")
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if edge.source_route("C63").await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("edge source discovery");
    manager.ensure("C63").await.expect("relay manager");
    let mut subscriber = edge_hub.subscribe("C63").expect("relay subscriber");
    source
        .publish(Arc::from(&b"\xD3\x00\x01"[..]))
        .expect("publish");
    let chunk = subscriber.recv().await.expect("relay packet");
    assert_eq!(&*chunk, b"\xD3\x00\x01");
    drop(subscriber);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(edge_hub.subscribe("C63").is_err());

    manager.ensure("C63").await.expect("relay reconnect");
    let mut subscriber = edge_hub.subscribe("C63").expect("reconnected relay");
    source
        .publish(Arc::from(&b"\xD3\x00\x02"[..]))
        .expect("publish after reconnect");
    let chunk = subscriber.recv().await.expect("reconnected packet");
    assert_eq!(&*chunk, b"\xD3\x00\x02");

    owner.shutdown().await.expect("owner shutdown");
    edge.shutdown().await.expect("edge shutdown");
    relay_shutdown_tx.send(()).expect("relay shutdown");
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("relay shutdown timeout")
        .expect("relay task")
        .expect("relay server");
    let _ = std::fs::remove_file(crl_path);
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

#[tokio::test]
async fn failed_relay_start_releases_its_starting_slot() {
    let cluster = unique("relay-start-failure");
    let mut admin = connect().await;
    publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish config");
    let coordinator = Arc::new(
        AdmissionCoordinator::start(connect().await, &cluster, CoordinatorOptions::default())
            .await
            .expect("coordinator"),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve relay address");
    let relay_address = listener.local_addr().expect("relay address");
    drop(listener);
    let owner = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-owner", &relay_address.to_string()),
    )
    .await
    .expect("owner runtime");
    let edge = SharedClusterRuntime::start(
        connect().await,
        Arc::clone(&coordinator),
        options(&cluster, "caster-edge", "127.0.0.1:1"),
    )
    .await
    .expect("edge runtime");
    let owner_hub = Arc::new(MountpointHub::new(16).expect("owner hub"));
    let source = owner_hub.claim_source("C63").expect("source");
    let record = StrRecord::from_profile("C63", &DataProfile::default());
    source.set_str_record(record.clone()).expect("source STR");
    let permit = owner
        .admit_source("owner", "C63", "connection-1", 1)
        .await
        .expect("admit source");
    owner
        .activate_source(&permit, &record)
        .await
        .expect("activate source");
    wait_for_route(&owner, "C63").await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while edge.source_route("C63").await.is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("edge source discovery");

    let manager = RelayManager::with_limits(
        Arc::new(MountpointHub::new(16).expect("edge hub")),
        edge.clone(),
        GrpcClientTls {
            ca_certificate_pem: String::new(),
            certificate_pem: String::new(),
            private_key_pem: String::new(),
            server_name: "localhost".to_owned(),
            service_token: "unused".to_owned(),
        },
        10,
        2,
        Duration::from_millis(100),
    );
    assert!(manager.ensure("C63").await.is_err());
    tokio::time::timeout(Duration::from_secs(2), async {
        while manager.metrics().await.starting != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("starting slot released");

    owner.shutdown().await.expect("owner shutdown");
    edge.shutdown().await.expect("edge shutdown");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

async fn wait_for_route(
    runtime: &SharedClusterRuntime,
    mountpoint: &str,
) -> rcaster_service::source_claim::SourceRoute {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(route) = runtime.source_route(mountpoint).await {
                return route;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("source route timeout")
}

async fn connect() -> Client {
    let endpoint = std::env::var("RCASTER_ETCD_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned());
    Client::connect([endpoint], Some(ConnectOptions::new()))
        .await
        .expect("etcd must be running")
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    )
}

fn options(cluster: &str, caster_id: &str, relay_endpoint: &str) -> ClusterRuntimeOptions {
    ClusterRuntimeOptions {
        cluster_id: cluster.to_owned(),
        caster_id: caster_id.to_owned(),
        runtime_epoch: unique("epoch"),
        relay_endpoint: relay_endpoint.to_owned(),
        lease_ttl_s: 30,
        keep_alive: true,
    }
}

struct TestPki {
    ca_certificate: String,
    server_certificate: String,
    server_key: String,
    client_certificate: String,
    client_key: String,
    revoked_client_certificate: String,
    revoked_client_key: String,
    client_crl: String,
    empty_client_crl: String,
}

impl TestPki {
    fn generate() -> Self {
        let mut ca_params = CertificateParams::new(Vec::new()).expect("CA params");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "relay-test-ca");
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
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_certificate = server_params
            .signed_by(&server_key, &ca_certificate, &ca_key)
            .expect("server certificate");
        let client_key = KeyPair::generate().expect("client key");
        let mut client_params =
            CertificateParams::new(vec!["relay-client".to_owned()]).expect("client params");
        client_params.serial_number = Some(SerialNumber::from(42_u64));
        client_params
            .distinguished_name
            .push(DnType::CommonName, "relay-client");
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_certificate = client_params
            .signed_by(&client_key, &ca_certificate, &ca_key)
            .expect("client certificate");
        let revoked_client_key = KeyPair::generate().expect("revoked client key");
        let mut revoked_client_params = CertificateParams::new(vec!["relay-revoked".to_owned()])
            .expect("revoked client params");
        revoked_client_params.serial_number = Some(SerialNumber::from(43_u64));
        revoked_client_params
            .distinguished_name
            .push(DnType::CommonName, "relay-revoked");
        revoked_client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let revoked_client_certificate = revoked_client_params
            .signed_by(&revoked_client_key, &ca_certificate, &ca_key)
            .expect("revoked client certificate");
        let client_crl = CertificateRevocationListParams {
            this_update: date_time_ymd(2026, 1, 1),
            next_update: date_time_ymd(2030, 1, 1),
            crl_number: SerialNumber::from(1_u64),
            issuing_distribution_point: None,
            revoked_certs: vec![RevokedCertParams {
                serial_number: SerialNumber::from(43_u64),
                revocation_time: date_time_ymd(2026, 11, 23),
                reason_code: Some(RevocationReason::KeyCompromise),
                invalidity_date: None,
            }],
            key_identifier_method: KeyIdMethod::Sha256,
        }
        .signed_by(&ca_certificate, &ca_key)
        .expect("client CRL")
        .pem()
        .expect("client CRL PEM");
        let empty_client_crl = CertificateRevocationListParams {
            this_update: date_time_ymd(2026, 1, 1),
            next_update: date_time_ymd(2030, 1, 1),
            crl_number: SerialNumber::from(1_u64),
            issuing_distribution_point: None,
            revoked_certs: Vec::new(),
            key_identifier_method: KeyIdMethod::Sha256,
        }
        .signed_by(&ca_certificate, &ca_key)
        .expect("empty client CRL")
        .pem()
        .expect("empty client CRL PEM");
        Self {
            ca_certificate: ca_certificate.pem(),
            server_certificate: server_certificate.pem(),
            server_key: server_key.serialize_pem(),
            client_certificate: client_certificate.pem(),
            client_key: client_key.serialize_pem(),
            revoked_client_certificate: revoked_client_certificate.pem(),
            revoked_client_key: revoked_client_key.serialize_pem(),
            client_crl,
            empty_client_crl,
        }
    }

    fn server_tls(&self) -> GrpcServerTls {
        GrpcServerTls {
            certificate_pem: self.server_certificate.clone(),
            private_key_pem: self.server_key.clone(),
            client_ca_pem: self.ca_certificate.clone(),
            client_crl_pem: None,
            server_ocsp_der: None,
        }
    }

    fn client_tls(&self, token: &str) -> GrpcClientTls {
        GrpcClientTls {
            ca_certificate_pem: self.ca_certificate.clone(),
            certificate_pem: self.client_certificate.clone(),
            private_key_pem: self.client_key.clone(),
            server_name: "localhost".to_owned(),
            service_token: token.to_owned(),
        }
    }

    fn revoked_client_tls(&self, token: &str) -> GrpcClientTls {
        GrpcClientTls {
            ca_certificate_pem: self.ca_certificate.clone(),
            certificate_pem: self.revoked_client_certificate.clone(),
            private_key_pem: self.revoked_client_key.clone(),
            server_name: "localhost".to_owned(),
            service_token: token.to_owned(),
        }
    }
}
