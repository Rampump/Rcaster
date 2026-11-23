use std::{fs, path::PathBuf};

use rcaster_core::protocol::{DataFormat, ProtocolDemux};

#[test]
#[ignore = "requires RCASTER_RTKLIB_TEST_DATA"]
fn parses_official_rtklib_rtcm2_and_rtcm3_fixtures() {
    let directory =
        PathBuf::from(std::env::var("RCASTER_RTKLIB_TEST_DATA").expect("RCASTER_RTKLIB_TEST_DATA"));
    for (file, format) in [
        ("testglo.rtcm2", DataFormat::Rtcm2),
        ("testglo.rtcm3", DataFormat::Rtcm3),
        ("GMSD7_20121014.rtcm3", DataFormat::Rtcm3),
    ] {
        let input = fs::read(directory.join(file)).expect("read RTKLIB fixture");
        let mut reference = ProtocolDemux::new();
        let reference_frames = reference.push(&input);
        assert!(!reference_frames.is_empty(), "{file} produced no frames");
        for chunk_size in [1, 127, 4096] {
            let mut demux = ProtocolDemux::new();
            let mut frames = Vec::new();
            for chunk in input.chunks(chunk_size) {
                frames.extend(demux.push(chunk));
            }
            let profile = demux.profile();
            println!(
                "{file}: chunk={chunk_size} bytes={} frames={} valid={} unknown={} crc_errors={}",
                input.len(),
                frames.len(),
                profile.valid_frames,
                profile.unknown_bytes,
                profile.crc_errors
            );
            assert_eq!(
                frames.len(),
                reference_frames.len(),
                "{file} chunk={chunk_size}"
            );
            for (index, (actual, expected)) in frames.iter().zip(&reference_frames).enumerate() {
                assert_eq!(
                    actual,
                    expected,
                    "{file} chunk={chunk_size} first mismatch index={index} actual_len={} expected_len={} actual_type={:?} expected_type={:?}",
                    actual.bytes.len(),
                    expected.bytes.len(),
                    actual.message_number,
                    expected.message_number
                );
            }
            assert!(
                frames.iter().all(|frame| frame.format == format),
                "{file} produced an unexpected format"
            );
            assert_eq!(profile.crc_errors, 0, "{file} CRC/parity errors");
            assert_eq!(
                profile.valid_frames,
                frames.len() as u64,
                "{file} frame counters disagree"
            );
            assert!(profile.has_correction_data, "{file} has no correction data");
        }
        for seed in 0..16_u64 {
            let mut state = seed;
            let mut offset = 0;
            let mut demux = ProtocolDemux::new();
            let mut frames = Vec::new();
            while offset < input.len() {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                let size = 1 + (state as usize % 257);
                let end = (offset + size).min(input.len());
                frames.extend(demux.push(&input[offset..end]));
                offset = end;
            }
            assert_eq!(
                frames, reference_frames,
                "{file} deterministic chunk seed={seed}"
            );
            assert_eq!(demux.profile().crc_errors, 0, "{file} seed={seed}");
        }
    }
}
