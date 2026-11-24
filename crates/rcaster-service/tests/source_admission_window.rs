use rcaster_core::protocol::{AdmissionMode, AdmissionPolicy, AdmissionViolation, DataFormat};
use rcaster_service::source_admission::{SourceAdmissionWindow, WindowOutcome};

const RTCM3_1005: &str = "D300133ED0000236FDB80DDE08005B2BC108A7B98D3DD8AB37";

#[test]
fn accepts_valid_frame_and_releases_buffered_bytes() {
    let frame = decode_hex(RTCM3_1005);
    let mut window = SourceAdmissionWindow::new(policy(AdmissionMode::Enforce, false), 1024, 8);

    let outcome = window.push(&frame);

    assert_eq!(
        outcome,
        WindowOutcome::Accepted {
            bytes: frame.clone(),
            warnings: Vec::new()
        }
    );
}

#[test]
fn enforce_rejects_missing_correction_data_after_window_finishes() {
    let mut window = SourceAdmissionWindow::new(policy(AdmissionMode::Enforce, true), 1024, 8);

    assert_eq!(window.push(&decode_hex(RTCM3_1005)), WindowOutcome::Pending);
    assert_eq!(
        window.finish(),
        WindowOutcome::Rejected {
            violations: vec![AdmissionViolation::NoObservationOrSsr]
        }
    );
}

#[test]
fn mixed_nmea_before_correction_keeps_window_pending() {
    let mut window = SourceAdmissionWindow::new(
        AdmissionPolicy {
            allowed_formats: vec![DataFormat::Rtcm3, DataFormat::Nmea],
            ..policy(AdmissionMode::Enforce, true)
        },
        1024,
        8,
    );
    let nmea = b"$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47\r\n";
    let correction = synthetic_rtcm3(1074);

    assert_eq!(window.push(nmea), WindowOutcome::Pending);

    let mut expected = nmea.to_vec();
    expected.extend_from_slice(&correction);
    assert_eq!(
        window.push(&correction),
        WindowOutcome::Accepted {
            bytes: expected,
            warnings: Vec::new()
        }
    );
}

#[test]
fn source_expectation_waits_for_correction_after_leading_nmea() {
    let expectation = rcaster_core::account::SourceExpectation {
        formats: vec!["rtcm3".to_owned()],
        msm_versions: vec![4],
    };
    let policy = AdmissionPolicy {
        allowed_formats: vec![DataFormat::Rtcm3, DataFormat::Nmea],
        ..policy(AdmissionMode::Enforce, true)
    }
    .with_source_expectation(&expectation);
    let mut window = SourceAdmissionWindow::new(policy, 1024, 8);
    let nmea = b"$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47\r\n";
    let correction = synthetic_rtcm3(1074);

    assert_eq!(window.push(nmea), WindowOutcome::Pending);

    let mut expected = nmea.to_vec();
    expected.extend_from_slice(&correction);
    assert_eq!(
        window.push(&correction),
        WindowOutcome::Accepted {
            bytes: expected,
            warnings: Vec::new()
        }
    );
}

#[test]
fn observe_accepts_with_warning() {
    let mut window = SourceAdmissionWindow::new(policy(AdmissionMode::Observe, true), 1024, 8);

    assert_eq!(
        window.push(&decode_hex(RTCM3_1005)),
        WindowOutcome::Accepted {
            bytes: decode_hex(RTCM3_1005),
            warnings: vec![AdmissionViolation::NoObservationOrSsr]
        }
    );
}

#[test]
fn observe_accepts_expected_format_mismatch_with_warning() {
    let mut admission = policy(AdmissionMode::Observe, false);
    admission.allowed_formats = vec![DataFormat::Nmea];
    admission.expected_formats = Some(vec![DataFormat::Rtcm3]);
    let mut window = SourceAdmissionWindow::new(admission, 1024, 8);
    let nmea = b"$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47\r\n";

    assert_eq!(
        window.push(nmea),
        WindowOutcome::Accepted {
            bytes: nmea.to_vec(),
            warnings: vec![AdmissionViolation::ExpectedFormatMismatch(DataFormat::Nmea)]
        }
    );
}

#[test]
fn byte_limit_rejects_pending_window() {
    let mut window = SourceAdmissionWindow::new(policy(AdmissionMode::Enforce, true), 4, 8);

    assert_eq!(
        window.push(b"xxxxx"),
        WindowOutcome::Rejected {
            violations: vec![AdmissionViolation::NoValidFrames]
        }
    );
}

#[test]
fn accepts_then_reuses_parser_with_partial_next_frame() {
    let frame = decode_hex(RTCM3_1005);
    let mut input = frame.clone();
    input.extend_from_slice(&frame[..7]);
    let mut window = SourceAdmissionWindow::new(policy(AdmissionMode::Enforce, false), 1024, 8);

    assert_eq!(
        window.push(&input),
        WindowOutcome::Accepted {
            bytes: frame.clone(),
            warnings: Vec::new()
        }
    );

    let mut demux = window.take_demux();
    let frames = demux.push(&frame[7..]);

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].bytes, frame);
}

fn policy(mode: AdmissionMode, require_correction_data: bool) -> AdmissionPolicy {
    AdmissionPolicy {
        mode,
        allowed_formats: vec![DataFormat::Rtcm3],
        require_correction_data,
        expected_formats: None,
        allowed_msm_versions: None,
        min_valid_frames: 1,
        max_unknown_ratio: None,
        min_observed_bytes: 0,
        ..AdmissionPolicy::default()
    }
}

fn decode_hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn synthetic_rtcm3(message_number: u16) -> Vec<u8> {
    let payload = [
        (message_number >> 4) as u8,
        ((message_number & 0x0F) as u8) << 4,
    ];
    let mut frame = vec![0xD3, 0, payload.len() as u8];
    frame.extend_from_slice(&payload);
    let crc = crc24q(&frame);
    frame.extend_from_slice(&crc.to_be_bytes()[1..]);
    frame
}

fn crc24q(data: &[u8]) -> u32 {
    let mut crc = 0_u32;
    for byte in data {
        crc ^= u32::from(*byte) << 16;
        for _ in 0..8 {
            crc <<= 1;
            if crc & 0x100_0000 != 0 {
                crc ^= 0x1864CFB;
            }
        }
    }
    crc & 0xFF_FFFF
}
