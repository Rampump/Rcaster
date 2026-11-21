use etcd_client::{Client, ConnectOptions};
use std::time::{SystemTime, UNIX_EPOCH};

use rcaster_service::source_claim::{
    ClaimError, SourceClaim, SourceClaimRequest, claim_source, publish_active, release_source,
};

#[tokio::test]
async fn live_claim_blocks_a_second_caster() {
    let endpoint = std::env::var("RCASTER_ETCD_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned());
    let mut client = Client::connect([endpoint], Some(ConnectOptions::new()))
        .await
        .expect("etcd must be running");
    let lease = client
        .lease_grant(30, None)
        .await
        .expect("grant lease")
        .id();
    let second_lease = client
        .lease_grant(30, None)
        .await
        .expect("grant second lease")
        .id();
    let cluster_id = format!(
        "prototype-live-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    );
    let request = SourceClaimRequest::new(
        "C63",
        "caster-west-02",
        "c_01j",
        "boot_01j",
        "10.20.0.12:12102",
        "u_01j",
        "2026-11-19T10:00:00Z",
    );

    claim_source(&mut client, lease, &cluster_id, request)
        .await
        .expect("first claim must succeed");

    let second_request = SourceClaimRequest::new(
        "C63",
        "caster-east-01",
        "c_02j",
        "boot_02j",
        "10.20.0.13:12102",
        "u_02j",
        "2026-11-19T10:00:01Z",
    );
    let second = claim_source(&mut client, second_lease, &cluster_id, second_request).await;

    assert!(matches!(second, Err(ClaimError::AlreadyClaimed)));
}

#[tokio::test]
async fn takeover_fences_the_previous_caster() {
    let endpoint = std::env::var("RCASTER_ETCD_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned());
    let mut client = Client::connect([endpoint], Some(ConnectOptions::new()))
        .await
        .expect("etcd must be running");
    let cluster_id = format!(
        "prototype-takeover-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    );
    let first_lease = client
        .lease_grant(5, None)
        .await
        .expect("grant first lease")
        .id();
    let first = claim_source(
        &mut client,
        first_lease,
        &cluster_id,
        SourceClaimRequest::new(
            "C63",
            "caster-west-02",
            "c_01j",
            "boot_01j",
            "10.20.0.12:12102",
            "u_01j",
            "2026-11-19T10:00:00Z",
        ),
    )
    .await
    .expect("first claim must succeed");
    assert_eq!(first.claim.source_generation, 1);

    client
        .lease_revoke(first_lease)
        .await
        .expect("revoke first caster lease");
    wait_for_missing_key(
        &mut client,
        &format!("/rcaster/{cluster_id}/runtime/source-claims/C63"),
    )
    .await;

    let second_lease = client
        .lease_grant(30, None)
        .await
        .expect("grant second lease")
        .id();
    let second = claim_source(
        &mut client,
        second_lease,
        &cluster_id,
        SourceClaimRequest::new(
            "C63",
            "caster-east-01",
            "c_02j",
            "boot_02j",
            "10.20.0.13:12102",
            "u_02j",
            "2026-11-19T10:00:01Z",
        ),
    )
    .await
    .expect("second claim must take over after lease expiry");
    assert_eq!(second.claim.source_generation, 2);

    assert!(matches!(
        publish_active(&mut client, &first).await,
        Err(ClaimError::StaleClaim)
    ));
    assert!(matches!(
        release_source(&mut client, &first).await,
        Err(ClaimError::StaleClaim)
    ));

    let current_claim = client
        .get(
            format!("/rcaster/{cluster_id}/runtime/source-claims/C63"),
            None,
        )
        .await
        .expect("read current claim")
        .take_kvs()
        .into_iter()
        .next()
        .expect("second claim must still exist");
    let current_claim: SourceClaim =
        serde_json::from_slice(current_claim.value()).expect("valid claim JSON");
    assert_eq!(current_claim.connection_id, "c_02j");

    publish_active(&mut client, &second)
        .await
        .expect("current caster must publish");
    release_source(&mut client, &second)
        .await
        .expect("current caster must release");
}

async fn wait_for_missing_key(client: &mut Client, key: &str) {
    for _ in 0..100 {
        if client
            .get(key, None)
            .await
            .expect("read source claim")
            .kvs()
            .is_empty()
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("etcd did not delete the expired claim");
}
