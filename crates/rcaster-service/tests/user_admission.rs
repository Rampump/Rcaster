use std::time::{Duration, SystemTime, UNIX_EPOCH};

use etcd_client::{Client, ConnectOptions};
use rcaster_service::admission::{
    AdmissionError, ClientAdmissionRequest, SessionRecord, SourceAdmissionRequest, admit_client,
    admit_source, read_ip_session_count, read_user_session_count, try_acquire_user_lock,
};
use rcaster_service::source_claim::{ClaimError, SourceClaimRequest, claim_source};

#[tokio::test]
async fn final_user_slot_allows_only_one_of_client_or_source() {
    let cluster = unique("final-slot");
    let user_id = "u_race";
    let client_request = ClientAdmissionRequest::new(
        SessionRecord::client(
            "s_client",
            user_id,
            "C63",
            "caster-east-01",
            "boot_east",
            "203.0.113.8",
            "2026-11-22T00:00:00Z",
        ),
        1,
        None,
    );
    let source_request = SourceAdmissionRequest::new(
        SessionRecord::source(
            "s_source",
            user_id,
            "C63",
            "caster-west-02",
            "boot_west",
            "2026-11-22T00:00:00Z",
        ),
        1,
        SourceClaimRequest::new(
            "C63",
            "caster-west-02",
            "c_source",
            "boot_west",
            "10.20.0.12:12102",
            user_id,
            "2026-11-22T00:00:00Z",
        ),
    );

    let mut client_task = connect().await;
    let mut source_task = connect().await;
    let client_cluster = cluster.clone();
    let client = tokio::spawn(async move {
        let lock_lease = grant_lease(&mut client_task, 30).await;
        let session_lease = grant_lease(&mut client_task, 30).await;
        let lock =
            acquire_user_lock(&mut client_task, lock_lease, &client_cluster, user_id).await?;
        admit_client(
            &mut client_task,
            session_lease,
            &client_cluster,
            lock,
            client_request,
        )
        .await
    });
    let source_cluster = cluster.clone();
    let source = tokio::spawn(async move {
        let lock_lease = grant_lease(&mut source_task, 30).await;
        let session_lease = grant_lease(&mut source_task, 30).await;
        let lock =
            acquire_user_lock(&mut source_task, lock_lease, &source_cluster, user_id).await?;
        admit_source(
            &mut source_task,
            session_lease,
            &source_cluster,
            lock,
            source_request,
        )
        .await
    });

    let client = client.await.expect("client task");
    let source = source.await.expect("source task");
    assert_eq!(usize::from(client.is_ok()) + usize::from(source.is_ok()), 1);

    let mut client = connect().await;
    assert_eq!(
        read_user_session_count(&mut client, &cluster, user_id)
            .await
            .expect("count sessions"),
        1
    );
}

#[tokio::test]
async fn expired_lock_cannot_commit_after_another_caster_takes_over() {
    let cluster = unique("stale-lock");
    let user_id = "u_stale";
    let mut client = connect().await;
    let lock_lease = grant_lease(&mut client, 2).await;
    let stale_lock = try_acquire_user_lock(&mut client, lock_lease, &cluster, user_id)
        .await
        .expect("first lock");
    let replacement_lease = grant_lease(&mut client, 30).await;
    let replacement_lock = acquire_user_lock(&mut client, replacement_lease, &cluster, user_id)
        .await
        .expect("replacement lock");
    let session_lease = grant_lease(&mut client, 30).await;

    admit_client(
        &mut client,
        session_lease,
        &cluster,
        replacement_lock,
        ClientAdmissionRequest::new(
            SessionRecord::client(
                "s_replacement",
                user_id,
                "C63",
                "caster-east-01",
                "boot_east",
                "203.0.113.9",
                "2026-11-22T00:00:01Z",
            ),
            3,
            None,
        ),
    )
    .await
    .expect("replacement admission");

    let stale_session_lease = grant_lease(&mut client, 30).await;
    let stale = admit_client(
        &mut client,
        stale_session_lease,
        &cluster,
        stale_lock,
        ClientAdmissionRequest::new(
            SessionRecord::client(
                "s_stale",
                user_id,
                "C63",
                "caster-west-02",
                "boot_west",
                "203.0.113.10",
                "2026-11-22T00:00:02Z",
            ),
            3,
            None,
        ),
    )
    .await;

    assert!(matches!(stale, Err(AdmissionError::StaleLock)));
    assert_eq!(
        read_user_session_count(&mut client, &cluster, user_id)
            .await
            .expect("count sessions"),
        1
    );
}

#[tokio::test]
async fn failed_source_claim_writes_no_owner_session() {
    let cluster = unique("source-rollback");
    let owner_user = "u_owner";
    let blocking_user = "u_blocker";
    let mut client = connect().await;
    let blocker_lease = grant_lease(&mut client, 30).await;
    claim_source(
        &mut client,
        blocker_lease,
        &cluster,
        SourceClaimRequest::new(
            "C63",
            "caster-blocker",
            "c_blocker",
            "boot_blocker",
            "10.20.0.20:12102",
            blocking_user,
            "2026-11-22T00:00:00Z",
        ),
    )
    .await
    .expect("blocking claim");

    let lock_lease = grant_lease(&mut client, 30).await;
    let session_lease = grant_lease(&mut client, 30).await;
    let lock = try_acquire_user_lock(&mut client, lock_lease, &cluster, owner_user)
        .await
        .expect("owner lock");
    let failed = admit_source(
        &mut client,
        session_lease,
        &cluster,
        lock,
        SourceAdmissionRequest::new(
            SessionRecord::source(
                "s_owner",
                owner_user,
                "C63",
                "caster-west-02",
                "boot_west",
                "2026-11-22T00:00:01Z",
            ),
            1,
            SourceClaimRequest::new(
                "C63",
                "caster-west-02",
                "c_owner",
                "boot_west",
                "10.20.0.12:12102",
                owner_user,
                "2026-11-22T00:00:01Z",
            ),
        ),
    )
    .await;

    assert!(matches!(
        failed,
        Err(AdmissionError::Source(ClaimError::AlreadyClaimed))
    ));
    assert_eq!(
        read_user_session_count(&mut client, &cluster, owner_user)
            .await
            .expect("count owner sessions"),
        0
    );
}

#[tokio::test]
async fn concurrent_casters_do_not_exceed_user_limit() {
    const LIMIT: usize = 3;

    let cluster = unique("cluster-limit");
    let user_id = "u_concurrent";
    let mut tasks = Vec::new();

    for index in 0..10 {
        let cluster = cluster.clone();
        let mut client = connect().await;
        tasks.push(tokio::spawn(async move {
            let lock_lease = grant_lease(&mut client, 30).await;
            let session_lease = grant_lease(&mut client, 30).await;
            let lock = acquire_user_lock(&mut client, lock_lease, &cluster, user_id).await?;
            admit_client(
                &mut client,
                session_lease,
                &cluster,
                lock,
                ClientAdmissionRequest::new(
                    SessionRecord::client(
                        format!("s_{index}"),
                        user_id,
                        "C63",
                        format!("caster-{index}"),
                        format!("boot_{index}"),
                        "203.0.113.20",
                        "2026-11-22T00:00:00Z",
                    ),
                    LIMIT,
                    None,
                ),
            )
            .await
        }));
    }

    let mut successes = 0;
    for task in tasks {
        if task.await.expect("admission task").is_ok() {
            successes += 1;
        }
    }

    assert_eq!(successes, LIMIT);
    let mut client = connect().await;
    assert_eq!(
        read_user_session_count(&mut client, &cluster, user_id)
            .await
            .expect("count sessions"),
        LIMIT
    );
}

#[tokio::test]
async fn sources_do_not_consume_ip_client_quota() {
    let cluster = unique("ip-isolation");
    let user_id = "u_ip";
    let ip = "203.0.113.30";
    let mut client = connect().await;
    let lock_lease = grant_lease(&mut client, 30).await;
    let source_lease = grant_lease(&mut client, 30).await;
    let lock = try_acquire_user_lock(&mut client, lock_lease, &cluster, user_id)
        .await
        .expect("source user lock");
    admit_source(
        &mut client,
        source_lease,
        &cluster,
        lock,
        SourceAdmissionRequest::new(
            SessionRecord::source(
                "s_source",
                user_id,
                "C63",
                "caster-west-02",
                "boot_west",
                "2026-11-22T00:00:00Z",
            ),
            3,
            SourceClaimRequest::new(
                "C63",
                "caster-west-02",
                "c_source",
                "boot_west",
                "10.20.0.12:12102",
                user_id,
                "2026-11-22T00:00:00Z",
            ),
        ),
    )
    .await
    .expect("source admission");
    assert_eq!(
        read_ip_session_count(&mut client, &cluster, ip)
            .await
            .expect("count IP sessions"),
        0
    );

    let lock_lease = grant_lease(&mut client, 30).await;
    let client_lease = grant_lease(&mut client, 30).await;
    let lock = try_acquire_user_lock(&mut client, lock_lease, &cluster, user_id)
        .await
        .expect("client user lock");
    admit_client(
        &mut client,
        client_lease,
        &cluster,
        lock,
        ClientAdmissionRequest::new(
            SessionRecord::client(
                "s_client_1",
                user_id,
                "C63",
                "caster-east-01",
                "boot_east",
                ip,
                "2026-11-22T00:00:01Z",
            ),
            3,
            Some(1),
        ),
    )
    .await
    .expect("first client consumes IP slot");

    let lock_lease = grant_lease(&mut client, 30).await;
    let client_lease = grant_lease(&mut client, 30).await;
    let lock = try_acquire_user_lock(&mut client, lock_lease, &cluster, user_id)
        .await
        .expect("second client user lock");
    let second = admit_client(
        &mut client,
        client_lease,
        &cluster,
        lock,
        ClientAdmissionRequest::new(
            SessionRecord::client(
                "s_client_2",
                user_id,
                "C63",
                "caster-east-01",
                "boot_east",
                ip,
                "2026-11-22T00:00:02Z",
            ),
            3,
            Some(1),
        ),
    )
    .await;

    assert!(matches!(second, Err(AdmissionError::IpQuotaExceeded)));
}

async fn connect() -> Client {
    let endpoint = std::env::var("RCASTER_ETCD_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned());
    Client::connect([endpoint], Some(ConnectOptions::new()))
        .await
        .expect("etcd must be running")
}

async fn grant_lease(client: &mut Client, ttl: i64) -> i64 {
    client
        .lease_grant(ttl, None)
        .await
        .expect("grant lease")
        .id()
}

async fn acquire_user_lock(
    client: &mut Client,
    lease_id: i64,
    cluster: &str,
    user_id: &str,
) -> Result<rcaster_service::admission::UserLock, AdmissionError> {
    for _ in 0..400 {
        match try_acquire_user_lock(client, lease_id, cluster, user_id).await {
            Ok(lock) => return Ok(lock),
            Err(AdmissionError::UserLockBusy) => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(AdmissionError::UserLockBusy)
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
