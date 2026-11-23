use std::{fs, path::PathBuf};

use rcaster_core::protocol::{DataFormat, ProtocolDemux};

#[test]
#[ignore = "requires RCASTER_BINEX_FIXTURE"]
fn parses_public_binex_stream_fixture() {
    let path =
        PathBuf::from(std::env::var("RCASTER_BINEX_FIXTURE").expect("RCASTER_BINEX_FIXTURE"));
    let input = fs::read(path).expect("read BINEX fixture");
    let mut reference = ProtocolDemux::new();
    let reference_frames = reference.push(&input);
    println!(
        "bytes={} frames={} unknown={} crc_errors={}",
        input.len(),
        reference_frames.len(),
        reference.profile().unknown_bytes,
        reference.profile().crc_errors
    );
    assert!(!reference_frames.is_empty());
    assert_eq!(reference.profile().crc_errors, 0);
    for chunk_size in [127, 4096] {
        let mut demux = ProtocolDemux::new();
        let mut frames = Vec::new();
        for chunk in input.chunks(chunk_size) {
            frames.extend(demux.push(chunk));
        }
        assert_eq!(frames, reference_frames, "chunk={chunk_size}");
        assert_eq!(demux.profile().crc_errors, 0, "chunk={chunk_size}");
    }
    for seed in 0..4_u64 {
        let mut state = seed;
        let mut offset = 0;
        let mut demux = ProtocolDemux::new();
        let mut frames = Vec::new();
        while offset < input.len() {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let size = 1 + (state as usize % 8192);
            let end = (offset + size).min(input.len());
            frames.extend(demux.push(&input[offset..end]));
            offset = end;
        }
        assert_eq!(frames, reference_frames, "deterministic seed={seed}");
        assert_eq!(demux.profile().crc_errors, 0, "seed={seed}");
    }
    assert!(
        reference_frames
            .iter()
            .all(|frame| frame.format == DataFormat::Binex)
    );
    assert!(reference.profile().has_correction_data);
}
