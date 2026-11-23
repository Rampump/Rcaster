#![cfg(feature = "rtklib-ffi")]

use std::{path::PathBuf, time::Duration};

use rcaster_rtklib::{DecoderWorker, WorkerLimits};

const RTCM3_1005: &str = "D300133ED0000236FDB80DDE08005B2BC108A7B98D3DD8AB37";

#[tokio::test]
async fn real_rtklib_worker_reports_capability_and_decodes_rtcm3() {
    let mut worker = rtklib_worker().await;

    let capability = worker.capability_probe().await.expect("capability probe");
    assert!(capability.engine.starts_with("RTKLIB 2.4."));
    assert_eq!(capability.formats, ["rtcm3"]);

    let decoded = worker
        .decode(&decode_hex(RTCM3_1005))
        .await
        .expect("RTCM3 1005 decode");
    assert_eq!(decoded.format, "rtcm3");
    assert_eq!(decoded.message_numbers, [1005]);
    worker.stop().await;
}

#[tokio::test]
async fn partial_frame_does_not_leak_into_next_request() {
    let mut worker = rtklib_worker().await;
    let frame = decode_hex(RTCM3_1005);

    let partial = worker.decode(&frame[..10]).await.expect("partial frame");
    assert!(partial.message_numbers.is_empty());
    let decoded = worker.decode(&frame).await.expect("complete frame");
    assert_eq!(decoded.message_numbers, [1005]);
    worker.stop().await;
}

async fn rtklib_worker() -> DecoderWorker {
    let executable = std::env::var_os("RCASTER_RTKLIB_WORKER_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_rtklib-decoder-worker")));
    let mut worker = DecoderWorker::new(
        executable,
        WorkerLimits {
            max_input_bytes: 4096,
            max_output_bytes: 4096,
            request_timeout: Duration::from_secs(2),
        },
    );
    worker.capability_probe().await.expect("worker starts");
    worker
}

fn decode_hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let pair = std::str::from_utf8(pair).expect("ASCII hex");
            u8::from_str_radix(pair, 16).expect("valid hex")
        })
        .collect()
}
