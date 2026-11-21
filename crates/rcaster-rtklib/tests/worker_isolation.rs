use std::time::Duration;

use rcaster_rtklib::{DecoderWorker, WorkerError, WorkerLimits};

#[tokio::test]
async fn capability_and_decode_use_versioned_protocol() {
    let mut worker = fixture_worker().await;

    let capability = worker.capability_probe().await.expect("capability probe");
    assert_eq!(capability.engine, "fixture-decoder");
    assert_eq!(capability.formats, ["rtcm3", "rtcm2"]);

    let decoded = worker.decode(b"normal").await.expect("decode");
    assert_eq!(decoded.format, "fixture");
    assert_eq!(decoded.message_numbers, [1005, 1074]);
    worker.stop().await;
}

#[tokio::test]
async fn oversized_input_is_rejected_before_write() {
    let mut worker = fixture_worker().await;

    let error = worker
        .decode(&[0_u8; 32])
        .await
        .expect_err("oversized input");

    assert!(matches!(
        error,
        WorkerError::InputTooLarge {
            size: 32,
            limit: 16
        }
    ));
    worker.stop().await;
}

#[tokio::test]
async fn crashed_worker_restarts_for_the_next_request() {
    let mut worker = fixture_worker().await;

    let error = worker.decode(b"crash").await.expect_err("worker crash");
    assert!(matches!(error, WorkerError::Crashed));
    let decoded = worker.decode(b"normal").await.expect("restarted worker");
    assert_eq!(decoded.format, "fixture");
    worker.stop().await;
}

#[tokio::test]
async fn hung_worker_times_out_and_restarts() {
    let mut worker = fixture_worker().await;

    let error = worker.decode(b"hang").await.expect_err("worker timeout");
    assert!(matches!(error, WorkerError::Timeout(_)));
    let decoded = worker.decode(b"normal").await.expect("restarted worker");
    assert_eq!(decoded.format, "fixture");
    worker.stop().await;
}

#[tokio::test]
async fn oversized_response_is_rejected_and_restarts() {
    let mut worker = fixture_worker().await;

    let error = worker.decode(b"large").await.expect_err("large response");
    assert!(matches!(error, WorkerError::OutputTooLarge { limit: 1024 }));
    let decoded = worker.decode(b"normal").await.expect("restarted worker");
    assert_eq!(decoded.format, "fixture");
    worker.stop().await;
}

async fn fixture_worker() -> DecoderWorker {
    let mut worker = DecoderWorker::new(
        env!("CARGO_BIN_EXE_fixture-decoder-worker"),
        WorkerLimits {
            max_input_bytes: 16,
            max_output_bytes: 1024,
            request_timeout: Duration::from_millis(100),
        },
    );
    worker.capability_probe().await.expect("worker starts");
    worker
}
