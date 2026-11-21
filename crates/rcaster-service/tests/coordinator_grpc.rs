use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use etcd_client::{Client, ConnectOptions};
use rcaster_service::{
    admission::{AdmissionError, ClientAdmissionRequest, SessionRecord, read_user_session_count},
    config::{CasterConfig, publish_config},
    coordinator_admission::{AdmissionCoordinator, CoordinatorOptions},
    coordinator_grpc::{GrpcAdmissionClient, serve_admission_grpc},
};
use tokio::{net::TcpListener, sync::oneshot};

#[tokio::test]
async fn grpc_admission_preserves_quota_and_config_errors() {
    let cluster = unique("grpc-coordinator");
    let user_id = "u_grpc";
    let mut admin = connect().await;
    let stale = publish_config(&mut admin, &cluster, CasterConfig::new(1))
        .await
        .expect("publish generation 1");
    let current = publish_config(&mut admin, &cluster, CasterConfig::new(2))
        .await
        .expect("publish generation 2");
    let (coordinator, shutdown, address, server) = start_server(&cluster).await;
    let mut grpc = GrpcAdmissionClient::connect(format!("http://{address}"))
        .await
        .expect("connect grpc");

    let admitted = grpc
        .admit_client(
            grant_lease(&mut admin).await,
            ClientAdmissionRequest::new(
                SessionRecord::client(
                    "s_grpc_1",
                    user_id,
                    "C63",
                    "caster-east-01",
                    "boot_east",
                    "203.0.113.90",
                    "2026-11-22T06:00:00Z",
                ),
                1,
                None,
                current.clone(),
            ),
        )
        .await;
    assert!(admitted.is_ok());

    let duplicate = grpc
        .admit_client(
            grant_lease(&mut admin).await,
            ClientAdmissionRequest::new(
                SessionRecord::client(
                    "s_grpc_2",
                    user_id,
                    "C63",
                    "caster-east-01",
                    "boot_east",
                    "203.0.113.90",
                    "2026-11-22T06:00:01Z",
                ),
                1,
                None,
                current,
            ),
        )
        .await;
    assert!(matches!(duplicate, Err(AdmissionError::UserQuotaExceeded)));

    let stale_result = grpc
        .admit_client(
            grant_lease(&mut admin).await,
            ClientAdmissionRequest::new(
                SessionRecord::client(
                    "s_grpc_stale",
                    "u_other",
                    "C63",
                    "caster-east-01",
                    "boot_east",
                    "203.0.113.91",
                    "2026-11-22T06:00:02Z",
                ),
                1,
                None,
                stale,
            ),
        )
        .await;
    assert!(matches!(stale_result, Err(AdmissionError::ConfigStale)));
    assert_eq!(
        read_user_session_count(&mut admin, &cluster, user_id)
            .await
            .expect("count sessions"),
        1
    );

    drop(grpc);
    let _ = shutdown.send(());
    server.await.expect("grpc server");
    Arc::try_unwrap(coordinator)
        .expect("coordinator handle")
        .shutdown()
        .await;
}

async fn start_server(
    cluster: &str,
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
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        serve_admission_grpc(listener, server_coordinator, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .expect("serve grpc");
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    (coordinator, shutdown_tx, address, task)
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
