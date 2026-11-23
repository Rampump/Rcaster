use rcaster_core::account::SourceExpectation;
use rcaster_core::protocol::{
    AdmissionMode, AdmissionPolicy, AdmissionProgress, AdmissionViolation, DataFormat, GnssSystem,
    ProtocolDemux, parse_nmea_gga,
};

const RTCM3_1005: &str = "D300133ED0000236FDB80DDE08005B2BC108A7B98D3DD8AB37";
// Invalid prefix byte plus the first 65-byte raw message ending at offset 3552.
const RTCM2_TYPE19: &str = "0A66494340755B7E5F617C565E6A7F4676514C676E4647624564716D7360696648725972494D535F4E43435D48446E476C484E78747F495E76534C7F7A67705B5143";
const BINEX_7F: &str = "E27F030501027A";
const CMR_GPS: &str = "020093022000B503";
const CMR_PLUS: &str = "020094030000009703";

#[test]
fn decodes_rtcm3_frame_and_profile() {
    let mut demux = ProtocolDemux::new();
    let frames = demux.push(&decode_hex(RTCM3_1005));

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].format, DataFormat::Rtcm3);
    assert_eq!(frames[0].message_number, Some(1005));
    let profile = demux.profile();
    assert_eq!(profile.primary_format, Some(DataFormat::Rtcm3));
    assert_eq!(
        profile.message_numbers.iter().copied().collect::<Vec<_>>(),
        vec![1005]
    );
    assert_eq!(profile.valid_frames, 1);
    assert_eq!(profile.crc_errors, 0);
    assert!(!profile.has_correction_data);
    assert!(profile.position.is_some());
    assert!(profile.station_id.is_some());
}

#[test]
fn reassembles_rtcm3_frame_split_across_reads() {
    let data = decode_hex(RTCM3_1005);
    let mut demux = ProtocolDemux::new();
    assert!(demux.push(&data[..7]).is_empty());
    assert!(demux.push(&data[7..15]).is_empty());
    let frames = demux.push(&data[15..]);

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].message_number, Some(1005));
}

#[test]
fn rejects_bad_crc_and_resynchronizes_to_next_frame() {
    let valid = decode_hex(RTCM3_1005);
    let mut bad = valid.clone();
    bad[4] ^= 0x01;
    let mut input = bad;
    input.extend_from_slice(&valid);
    let mut demux = ProtocolDemux::new();
    let frames = demux.push(&input);

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].message_number, Some(1005));
    assert_eq!(demux.profile().crc_errors, 1);
    assert_eq!(demux.profile().valid_frames, 1);
}

#[test]
fn recognizes_nmea_without_correction_data() {
    let mut demux = ProtocolDemux::new();
    let frames =
        demux.push(b"$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47\r\n");

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].format, DataFormat::Nmea);
    assert_eq!(demux.profile().primary_format, Some(DataFormat::Nmea));
    assert!(!demux.profile().has_correction_data);
    assert_eq!(
        demux.profile().position.map(|position| (
            position.latitude_e7,
            position.longitude_e7,
            position.height_cm
        )),
        Some((481_173_000, 115_166_667, 54_540))
    );
}

#[test]
fn extracts_gga_position_quality_and_hdop() {
    let gga =
        parse_nmea_gga(b"$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47\r\n")
            .expect("GGA");

    assert_eq!(gga.fix_quality, 1);
    assert_eq!(gga.hdop, 0.9);
    assert_eq!(gga.position.latitude_e7, 481_173_000);
    assert_eq!(gga.position.longitude_e7, 115_166_667);
}

#[test]
fn extracts_gsv_satellite_sky_positions_and_snr() {
    let mut demux = ProtocolDemux::new();
    demux.push(nmea_line("GPGSV,1,1,2,01,45,120,42,12,10,250,").as_bytes());
    let profile = demux.profile();
    assert_eq!(profile.satellite_positions.len(), 2);
    assert_eq!(profile.satellite_positions[0].system, "GP");
    assert_eq!(profile.satellite_positions[0].prn, 1);
    assert_eq!(profile.satellite_positions[0].elevation_deg, 45);
    assert_eq!(profile.satellite_positions[0].azimuth_deg, 120);
    assert_eq!(profile.satellite_positions[0].snr_db, Some(42));
    assert_eq!(profile.satellite_positions[1].snr_db, None);
}

fn nmea_line(body: &str) -> String {
    let checksum = body.bytes().fold(0_u8, |checksum, byte| checksum ^ byte);
    format!("${body}*{checksum:02X}\r\n")
}

#[test]
fn decodes_rtcm2_type19_with_parity() {
    let mut demux = ProtocolDemux::new();
    let frames = demux.push(&decode_hex(RTCM2_TYPE19));

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].format, DataFormat::Rtcm2);
    assert_eq!(frames[0].message_number, Some(19));
    assert_eq!(demux.profile().primary_format, Some(DataFormat::Rtcm2));
    assert!(demux.profile().has_correction_data);
}

#[test]
fn reassembles_rtcm2_frame_split_at_every_byte_boundary() {
    let data = decode_hex(RTCM2_TYPE19);
    for split in 1..data.len() {
        let mut demux = ProtocolDemux::new();
        let mut frames = demux.push(&data[..split]);
        frames.extend(demux.push(&data[split..]));
        assert_eq!(frames.len(), 1, "split at {split}");
        assert_eq!(frames[0].format, DataFormat::Rtcm2, "split at {split}");
        assert_eq!(frames[0].bytes, data[1..], "split at {split}");
    }
}

#[test]
fn rejects_rtcm2_with_bad_parity() {
    let mut data = decode_hex(RTCM2_TYPE19);
    data[9] ^= 0x01;
    let mut demux = ProtocolDemux::new();
    let frames = demux.push(&data);

    assert!(frames.is_empty());
    assert_eq!(demux.profile().crc_errors, 1);
}

#[test]
fn decodes_binex_record_and_checksum() {
    let mut demux = ProtocolDemux::new();
    let frames = demux.push(&decode_hex(BINEX_7F));

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].format, DataFormat::Binex);
    assert_eq!(frames[0].message_number, Some(0x7F));
    assert_eq!(demux.profile().primary_format, Some(DataFormat::Binex));
    assert!(demux.profile().has_correction_data);
}

#[test]
fn rejects_binex_with_bad_checksum() {
    let mut data = decode_hex(BINEX_7F);
    data[4] ^= 0x01;
    let mut demux = ProtocolDemux::new();
    let frames = demux.push(&data);

    assert!(frames.is_empty());
    assert_eq!(demux.profile().crc_errors, 1);
}

#[test]
fn decodes_binex_with_extended_crc16() {
    let mut data = decode_hex("E27F8102");
    data.resize(data.len() + 130, 0);
    data.extend_from_slice(&[0xA1, 0x5D]);
    let mut demux = ProtocolDemux::new();
    let frames = demux.push(&data);

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].format, DataFormat::Binex);
    assert_eq!(frames[0].message_number, Some(0x7F));
    assert_eq!(frames[0].bytes.len(), 136);
}

#[test]
fn decodes_trimble_cmr_packet() {
    let mut demux = ProtocolDemux::new();
    let frames = demux.push(&decode_hex(CMR_GPS));

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].format, DataFormat::Cmr);
    assert_eq!(frames[0].message_number, Some(0));
    assert_eq!(demux.profile().primary_format, Some(DataFormat::Cmr));
    assert!(demux.profile().has_correction_data);
}

#[test]
fn decodes_trimble_cmr_plus_packet() {
    let mut demux = ProtocolDemux::new();
    let frames = demux.push(&decode_hex(CMR_PLUS));

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].format, DataFormat::Cmr);
    assert_eq!(frames[0].message_number, Some(0x94));
}

#[test]
fn rejects_cmr_with_bad_checksum() {
    let mut data = decode_hex(CMR_GPS);
    data[5] ^= 0x01;
    let mut demux = ProtocolDemux::new();
    let frames = demux.push(&data);

    assert!(frames.is_empty());
    assert_eq!(demux.profile().crc_errors, 1);
}

#[test]
fn reassembles_cmr_packets_split_at_every_byte_boundary() {
    for encoded in [CMR_GPS, CMR_PLUS] {
        let data = decode_hex(encoded);
        for split in 1..data.len() {
            let mut demux = ProtocolDemux::new();
            let mut frames = demux.push(&data[..split]);
            frames.extend(demux.push(&data[split..]));
            assert_eq!(frames.len(), 1, "{encoded} split at {split}");
            assert_eq!(frames[0].format, DataFormat::Cmr);
            assert_eq!(frames[0].bytes, data, "{encoded} split at {split}");
        }
    }
}

#[test]
fn cmr_remains_primary_in_a_mixed_nmea_stream() {
    let mut input = decode_hex(CMR_PLUS);
    input.extend_from_slice(
        nmea_line("GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,").as_bytes(),
    );
    let mut demux = ProtocolDemux::new();
    let frames = demux.push(&input);

    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].format, DataFormat::Cmr);
    assert_eq!(frames[1].format, DataFormat::Nmea);
    assert_eq!(demux.profile().primary_format, Some(DataFormat::Cmr));
    assert_eq!(demux.profile().nmea_lines, 1);
    assert!(demux.profile().has_correction_data);
}

#[test]
fn cmr_checksum_error_resynchronizes_to_the_next_packet() {
    let mut bad = decode_hex(CMR_GPS);
    bad[5] ^= 0x01;
    bad.extend_from_slice(&decode_hex(CMR_PLUS));
    let mut demux = ProtocolDemux::new();
    let frames = demux.push(&bad);

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].format, DataFormat::Cmr);
    assert_eq!(frames[0].message_number, Some(0x94));
    assert_eq!(demux.profile().crc_errors, 1);
}

#[test]
fn rejects_nmea_with_bad_checksum() {
    let mut demux = ProtocolDemux::new();
    let frames =
        demux.push(b"$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*00\r\n");

    assert!(frames.is_empty());
    assert_eq!(demux.profile().nmea_lines, 0);
    assert_eq!(
        demux.profile().unknown_bytes,
        b"$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*00\r\n".len() as u64
    );
}

#[test]
fn counts_unknown_bytes_between_frames() {
    let valid = decode_hex(RTCM3_1005);
    let mut input = b"garbage".to_vec();
    input.extend_from_slice(&valid);
    let mut demux = ProtocolDemux::new();
    let frames = demux.push(&input);

    assert_eq!(frames.len(), 1);
    assert_eq!(demux.profile().unknown_bytes, 7);
}

#[test]
fn incomplete_rtcm2_candidate_does_not_block_later_rtcm3() {
    let mut demux = ProtocolDemux::new();
    let rtcm2 = decode_hex(RTCM2_TYPE19);
    let mut input = vec![0x40];
    input.extend_from_slice(&decode_hex(RTCM3_1005));
    input.extend_from_slice(&rtcm2[..11]);
    let frames = demux.push(&input);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].message_number, Some(1005));
}

#[test]
fn unterminated_nmea_candidate_does_not_block_later_rtcm3() {
    let mut demux = ProtocolDemux::new();
    let mut input = b"$GPGGA,broken".to_vec();
    input.extend_from_slice(&decode_hex(RTCM3_1005));
    let frames = demux.push(&input);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].message_number, Some(1005));
}

#[test]
fn malformed_streams_resynchronize_without_panicking() {
    let valid = decode_hex(RTCM3_1005);
    for (seed, lead) in [(1_u64, 0xD3_u8), (2, 0x66), (3, 0xE2), (4, 0x02), (5, b'$')] {
        let mut state = seed;
        let mut input = vec![lead];
        for _ in 0..16 * 1024 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            input.push(state as u8);
        }
        let mut demux = ProtocolDemux::new();
        for chunk in input.chunks(97) {
            demux.push(chunk);
        }
        let mut anchors = valid.repeat(100);
        anchors.extend_from_slice(&valid);
        let frames = demux.push(&anchors);
        let profile = demux.profile();
        assert!(
            frames.iter().any(|frame| {
                frame.format == DataFormat::Rtcm3 && frame.message_number == Some(1005)
            }),
            "seed {seed} did not resynchronize: frames={}, valid={}, unknown={}, total={}",
            frames.len(),
            profile.valid_frames,
            profile.unknown_bytes,
            profile.total_bytes
        );
    }
}

#[test]
fn identifies_msm_version_as_correction_data() {
    let frame = synthetic_rtcm3(1074);
    let mut demux = ProtocolDemux::new();
    let frames = demux.push(&frame);

    assert_eq!(frames[0].message_number, Some(1074));
    assert!(demux.profile().has_correction_data);
    assert_eq!(
        demux
            .profile()
            .msm_versions
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![4]
    );
}

#[test]
fn extracts_msm_satellite_and_signal_counts() {
    let mut payload = vec![0_u8; 32];
    write_bits(&mut payload, 0, 12, 1074);
    let satellites = (1_u64 << 63) | (1_u64 << 61) | (1_u64 << 59);
    write_bits(&mut payload, 73, 64, satellites);
    write_bits(&mut payload, 137, 32, 0b101);
    let frame = synthetic_rtcm3_payload(&payload);
    let mut demux = ProtocolDemux::new();

    demux.push(&frame);

    assert_eq!(
        demux
            .profile()
            .msm_versions
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![4]
    );
    assert_eq!(demux.profile().satellite_count, 3);
    assert_eq!(demux.profile().signal_count, 2);
    assert_eq!(
        demux
            .profile()
            .satellite_ids
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![1, 3, 5]
    );
    assert_eq!(
        demux
            .profile()
            .signal_ids
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![30, 32]
    );
}

#[test]
fn extracts_msm_cn0_for_each_satellite() {
    let mut payload = vec![0_u8; 64];
    write_bits(&mut payload, 0, 12, 1074);
    write_bits(&mut payload, 12, 12, 42);
    write_bits(&mut payload, 24, 30, 123_456);
    let satellites = (1_u64 << 63) | (1_u64 << 62);
    write_bits(&mut payload, 73, 64, satellites);
    write_bits(&mut payload, 137, 32, (1_u64 << 31) | (1_u64 << 30));
    write_bits(&mut payload, 169, 4, 0b1111);
    let cn0_start = 173 + 2 * 18 + 4 * (15 + 22 + 4 + 1);
    for (index, cn0) in [40, 32, 28, 24].into_iter().enumerate() {
        write_bits(&mut payload, cn0_start + index * 6, 6, cn0);
    }
    let mut demux = ProtocolDemux::new();
    demux.push(&synthetic_rtcm3_payload(&payload));

    assert_eq!(demux.profile().satellite_cn0_q4.get(&1), Some(&160));
    assert_eq!(demux.profile().satellite_cn0_q4.get(&2), Some(&112));
}

#[test]
fn decodes_msm4_pseudorange_phase_lock_and_cn0() {
    let mut payload = vec![0_u8; 64];
    write_msm_header(&mut payload, 1074, 42, 123_456);
    write_bits(&mut payload, 169, 1, 1);
    write_bits(&mut payload, 170, 8, 10);
    write_bits(&mut payload, 178, 10, 512);
    write_bits(&mut payload, 188, 15, 4096);
    write_bits(&mut payload, 203, 22, 1_048_576);
    write_bits(&mut payload, 225, 4, 7);
    write_bits(&mut payload, 229, 1, 1);
    write_bits(&mut payload, 230, 6, 40);
    let mut demux = ProtocolDemux::new();
    demux.push(&synthetic_rtcm3_payload(&payload));

    let observation = demux.profile().msm_observations[0];
    assert_eq!(observation.system, GnssSystem::Gps);
    assert_eq!(observation.satellite_id, 1);
    assert_eq!(observation.signal_id, 1);
    assert_eq!(observation.station_id, 42);
    assert_eq!(observation.epoch_time_ms, 123_456);
    assert_eq!(observation.lock_time_raw, Some(7));
    assert_eq!(observation.half_cycle_ambiguity, Some(true));
    assert_eq!(observation.cn0_q4, Some(160));
    assert!((observation.pseudorange_mm.expect("pseudorange") - 3_147_894_000_518).abs() <= 1);
    assert!((observation.carrier_phase_mm.expect("phase") - 3_148_406_341_145).abs() <= 1);
    assert_eq!(observation.doppler_mm_s, None);
}

#[test]
fn maps_bds_and_navic_msm_message_ranges_to_correct_system() {
    for (message_number, expected) in [(1124, GnssSystem::Bds), (1134, GnssSystem::NavIc)] {
        let mut demux = ProtocolDemux::new();
        demux.push(&msm4_single_for(message_number, 123_456, 1_u64 << 63, 40));

        assert_eq!(demux.profile().msm_observations[0].system, expected);
    }
}

#[test]
fn enforce_rejects_disallowed_msm_version() {
    let mut demux = ProtocolDemux::new();
    demux.push(&msm4_single_for(1077, 123_456, 1_u64 << 63, 40));
    let mut admission = policy(AdmissionMode::Enforce, 1, false);
    admission.allowed_msm_versions = Some(vec![4]);

    assert_eq!(
        admission.evaluate(demux.profile()),
        AdmissionProgress::Rejected {
            violations: vec![AdmissionViolation::MsmVersionNotAllowed(7)]
        }
    );
}

#[test]
fn observe_warns_but_accepts_disallowed_msm_version() {
    let mut demux = ProtocolDemux::new();
    demux.push(&msm4_single_for(1077, 123_456, 1_u64 << 63, 40));
    let mut admission = policy(AdmissionMode::Observe, 1, false);
    admission.allowed_msm_versions = Some(vec![4]);

    assert_eq!(
        admission.evaluate(demux.profile()),
        AdmissionProgress::Accepted {
            warnings: vec![AdmissionViolation::MsmVersionNotAllowed(7)]
        }
    );
}

#[test]
fn source_expectation_rejects_a_mismatched_primary_format() {
    let mut demux = ProtocolDemux::new();
    demux.push(b"$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47\r\n");
    let mut admission = policy(AdmissionMode::Enforce, 1, false);
    admission.expected_formats = Some(vec![DataFormat::Rtcm3]);

    assert_eq!(
        admission.evaluate(demux.profile()),
        AdmissionProgress::Rejected {
            violations: vec![AdmissionViolation::ExpectedFormatMismatch(DataFormat::Nmea)]
        }
    );
}

#[test]
fn source_expectation_narrows_msm_versions() {
    let mut demux = ProtocolDemux::new();
    demux.push(&msm4_single_for(1077, 123_456, 1_u64 << 63, 40));
    let expectation = SourceExpectation {
        formats: vec!["rtcm3".to_owned()],
        msm_versions: vec![4],
    };
    let admission = policy(AdmissionMode::Enforce, 1, false).with_source_expectation(&expectation);

    assert_eq!(admission.expected_formats, Some(vec![DataFormat::Rtcm3]));
    assert_eq!(admission.allowed_msm_versions, Some(vec![4]));
    assert_eq!(
        admission.evaluate(demux.profile()),
        AdmissionProgress::Rejected {
            violations: vec![AdmissionViolation::MsmVersionNotAllowed(7)]
        }
    );
}

#[test]
fn decodes_glonass_msm5_frequency_and_doppler() {
    let mut payload = vec![0_u8; 64];
    write_msm_header(&mut payload, 1085, 17, 654_321);
    write_bits(&mut payload, 169, 1, 1);
    write_bits(&mut payload, 170, 8, 20);
    write_bits(&mut payload, 178, 4, 2);
    write_bits(&mut payload, 182, 10, 0);
    write_signed_bits(&mut payload, 192, 14, -100);
    write_bits(&mut payload, 206, 15, 8192);
    write_bits(&mut payload, 221, 22, 1_048_576);
    write_bits(&mut payload, 243, 4, 9);
    write_bits(&mut payload, 247, 1, 0);
    write_bits(&mut payload, 248, 6, 30);
    write_signed_bits(&mut payload, 254, 15, -250);
    let mut demux = ProtocolDemux::new();
    demux.push(&synthetic_rtcm3_payload(&payload));

    let observation = demux.profile().msm_observations[0];
    assert_eq!(observation.system, GnssSystem::Glonass);
    assert_eq!(observation.glonass_frequency_channel, Some(-5));
    assert_eq!(observation.cn0_q4, Some(120));
    assert_eq!(observation.doppler_mm_s, Some(-100_025));
    assert!(observation.pseudorange_mm.is_some());
    assert!(observation.carrier_phase_mm.is_some());
}

#[test]
fn decodes_msm6_extended_observation_fields() {
    let mut payload = vec![0_u8; 64];
    write_msm_header(&mut payload, 1076, 9, 777_777);
    write_bits(&mut payload, 169, 1, 1);
    write_bits(&mut payload, 170, 8, 75);
    write_bits(&mut payload, 178, 10, 256);
    write_bits(&mut payload, 188, 20, 262_144);
    write_bits(&mut payload, 208, 24, 4_194_304);
    write_bits(&mut payload, 232, 10, 512);
    write_bits(&mut payload, 242, 1, 1);
    write_bits(&mut payload, 243, 10, 357);
    let mut demux = ProtocolDemux::new();
    demux.push(&synthetic_rtcm3_payload(&payload));

    let observation = demux.profile().msm_observations[0];
    assert_eq!(observation.lock_time_raw, Some(512));
    assert_eq!(observation.cn0_q4, Some(357));
    assert_eq!(observation.half_cycle_ambiguity, Some(true));
    assert!(observation.pseudorange_mm.is_some());
    assert_eq!(observation.doppler_mm_s, None);
}

#[test]
fn decodes_msm7_full_observation_fields() {
    let mut payload = vec![0_u8; 64];
    write_msm_header(&mut payload, 1077, 3, 888_888);
    write_bits(&mut payload, 169, 1, 1);
    write_bits(&mut payload, 170, 8, 80);
    write_bits(&mut payload, 178, 10, 768);
    write_signed_bits(&mut payload, 188, 14, 1250);
    write_bits(&mut payload, 202, 20, 524_288);
    write_bits(&mut payload, 222, 24, 4_194_304);
    write_bits(&mut payload, 246, 10, 1023);
    write_bits(&mut payload, 256, 1, 0);
    write_bits(&mut payload, 257, 10, 400);
    write_signed_bits(&mut payload, 267, 15, -1250);
    let mut demux = ProtocolDemux::new();
    demux.push(&synthetic_rtcm3_payload(&payload));

    let observation = demux.profile().msm_observations[0];
    assert_eq!(observation.lock_time_raw, Some(1023));
    assert_eq!(observation.cn0_q4, Some(400));
    assert_eq!(observation.doppler_mm_s, Some(1_249_875));
    assert_eq!(observation.half_cycle_ambiguity, Some(false));
    assert!(observation.carrier_phase_mm.is_some());
}

#[test]
fn decodes_legacy_gps_observation_with_cnr_and_ambiguity() {
    let mut payload = vec![0_u8; 32];
    write_legacy_header(&mut payload, 1002, 42, 321_000, false);
    let mut offset = 64;
    write_legacy(&mut payload, &mut offset, 6, 5);
    write_legacy(&mut payload, &mut offset, 1, 0);
    write_legacy(&mut payload, &mut offset, 24, 1000);
    write_legacy_signed(&mut payload, &mut offset, 20, 2000);
    write_legacy(&mut payload, &mut offset, 7, 12);
    write_legacy(&mut payload, &mut offset, 8, 3);
    write_legacy(&mut payload, &mut offset, 8, 160);
    let mut demux = ProtocolDemux::new();
    demux.push(&synthetic_rtcm3_payload(&payload));

    let observation = demux.profile().msm_observations[0];
    assert_eq!(observation.system, GnssSystem::Gps);
    assert_eq!(observation.satellite_id, 5);
    assert_eq!(observation.signal_id, 1);
    assert_eq!(observation.pseudorange_mm, Some(20_000));
    assert_eq!(observation.carrier_phase_mm, Some(21_000));
    assert_eq!(observation.cn0_q4, Some(160));
    assert_eq!(observation.lock_time_raw, Some(12));
    assert_eq!(observation.pseudorange_ambiguity_raw, Some(3));
    assert_eq!(demux.profile().satellite_cn0_q4.get(&5), Some(&160));
}

#[test]
fn decodes_legacy_gps_l1_l2_observations() {
    let mut payload = vec![0_u8; 40];
    write_legacy_header(&mut payload, 1004, 7, 654_000, false);
    let mut offset = 64;
    write_legacy(&mut payload, &mut offset, 6, 8);
    write_legacy(&mut payload, &mut offset, 1, 0);
    write_legacy(&mut payload, &mut offset, 24, 1500);
    write_legacy_signed(&mut payload, &mut offset, 20, 2000);
    write_legacy(&mut payload, &mut offset, 7, 15);
    write_legacy(&mut payload, &mut offset, 8, 1);
    write_legacy(&mut payload, &mut offset, 8, 160);
    write_legacy(&mut payload, &mut offset, 2, 0);
    write_legacy_signed(&mut payload, &mut offset, 14, -250);
    write_legacy_signed(&mut payload, &mut offset, 20, 1000);
    write_legacy(&mut payload, &mut offset, 7, 22);
    write_legacy(&mut payload, &mut offset, 8, 120);
    let mut demux = ProtocolDemux::new();
    demux.push(&synthetic_rtcm3_payload(&payload));

    let observations = &demux.profile().msm_observations;
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0].signal_id, 1);
    assert_eq!(observations[0].pseudorange_mm, Some(30_000));
    assert_eq!(observations[1].signal_id, 2);
    assert_eq!(observations[1].pseudorange_mm, Some(25_000));
    assert_eq!(observations[1].carrier_phase_mm, Some(25_500));
    assert_eq!(observations[1].cn0_q4, Some(120));
}

#[test]
fn decodes_legacy_glonass_frequency_and_l2() {
    let mut payload = vec![0_u8; 40];
    write_legacy_header(&mut payload, 1012, 9, 777_000, true);
    let mut offset = 61;
    write_legacy(&mut payload, &mut offset, 6, 3);
    write_legacy(&mut payload, &mut offset, 1, 0);
    write_legacy(&mut payload, &mut offset, 5, 9);
    write_legacy(&mut payload, &mut offset, 25, 2000);
    write_legacy_signed(&mut payload, &mut offset, 20, 2000);
    write_legacy(&mut payload, &mut offset, 7, 17);
    write_legacy(&mut payload, &mut offset, 7, 2);
    write_legacy(&mut payload, &mut offset, 8, 120);
    write_legacy(&mut payload, &mut offset, 2, 0);
    write_legacy_signed(&mut payload, &mut offset, 14, -250);
    write_legacy_signed(&mut payload, &mut offset, 20, 1000);
    write_legacy(&mut payload, &mut offset, 7, 19);
    write_legacy(&mut payload, &mut offset, 8, 80);
    let mut demux = ProtocolDemux::new();
    demux.push(&synthetic_rtcm3_payload(&payload));

    let observations = &demux.profile().msm_observations;
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0].system, GnssSystem::Glonass);
    assert_eq!(observations[0].glonass_frequency_channel, Some(2));
    assert_eq!(observations[0].pseudorange_mm, Some(40_000));
    assert_eq!(observations[0].carrier_phase_mm, Some(41_000));
    assert_eq!(observations[0].pseudorange_ambiguity_raw, Some(2));
    assert_eq!(observations[1].pseudorange_mm, Some(35_000));
    assert_eq!(observations[1].carrier_phase_mm, Some(35_500));
}

#[test]
fn merges_msm_parts_for_one_epoch_and_replaces_previous_epoch() {
    let first = msm4_single_satellite(1000, 1_u64 << 63, 40);
    let second = msm4_single_satellite(1000, 1_u64 << 62, 30);
    let next_epoch = msm4_single_satellite(2000, 1_u64 << 63, 20);
    let mut input = first;
    input.extend_from_slice(&second);
    let mut demux = ProtocolDemux::new();
    demux.push(&input);

    assert_eq!(demux.profile().msm_observations.len(), 2);
    assert_eq!(
        demux
            .profile()
            .msm_observations
            .iter()
            .map(|observation| observation.satellite_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    demux.push(&next_epoch);
    assert_eq!(demux.profile().msm_observations.len(), 1);
    assert_eq!(demux.profile().msm_observations[0].epoch_time_ms, 2000);
    assert_eq!(demux.profile().satellite_cn0_q4.get(&1), Some(&80));
}

#[test]
fn admission_waits_for_minimum_frame_count() {
    let mut demux = ProtocolDemux::new();
    demux.push(&decode_hex(RTCM3_1005));
    let policy = policy(AdmissionMode::Enforce, 2, true);

    assert_eq!(policy.evaluate(demux.profile()), AdmissionProgress::Pending);
}

#[test]
fn extracts_1033_device_information() {
    let mut payload = vec![0_u8; 108];
    write_bits(&mut payload, 0, 12, 1033);
    write_bits(&mut payload, 12, 12, 42);
    write_ascii(&mut payload, 24, "ANT-1");
    write_ascii(&mut payload, 160, "ANT-SERIAL");
    write_ascii(&mut payload, 416, "RECEIVER-X");
    write_ascii(&mut payload, 544, "1.2.3");
    write_ascii(&mut payload, 608, "RECEIVER-SERIAL");
    let frame = synthetic_rtcm3_payload(&payload);
    let mut demux = ProtocolDemux::new();

    demux.push(&frame);

    let device = demux.profile().device.as_ref().expect("device info");
    assert_eq!(device.station_id, 42);
    assert_eq!(demux.profile().station_id, Some(42));
    assert_eq!(device.antenna_descriptor, "ANT-1");
    assert_eq!(device.antenna_serial, "ANT-SERIAL");
    assert_eq!(device.receiver_type, "RECEIVER-X");
    assert_eq!(device.receiver_firmware, "1.2.3");
    assert_eq!(device.receiver_serial, "RECEIVER-SERIAL");
}

#[test]
fn enforce_rejects_source_without_correction_data() {
    let mut demux = ProtocolDemux::new();
    demux.push(&decode_hex(RTCM3_1005));
    let policy = policy(AdmissionMode::Enforce, 1, true);

    assert_eq!(
        policy.evaluate(demux.profile()),
        AdmissionProgress::Rejected {
            violations: vec![AdmissionViolation::MissingCorrectionData]
        }
    );
}

#[test]
fn observe_accepts_with_warning() {
    let mut demux = ProtocolDemux::new();
    demux.push(&decode_hex(RTCM3_1005));
    let policy = policy(AdmissionMode::Observe, 1, true);

    assert_eq!(
        policy.evaluate(demux.profile()),
        AdmissionProgress::Accepted {
            warnings: vec![AdmissionViolation::MissingCorrectionData]
        }
    );
}

#[test]
fn enforce_rejects_disallowed_primary_format() {
    let mut demux = ProtocolDemux::new();
    demux.push(b"$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47\r\n");
    let policy = AdmissionPolicy {
        mode: AdmissionMode::Enforce,
        allowed_formats: vec![DataFormat::Rtcm3],
        require_correction_data: false,
        expected_formats: None,
        allowed_msm_versions: None,
        min_valid_frames: 1,
        max_unknown_ratio: None,
        min_observed_bytes: 0,
    };

    assert_eq!(
        policy.evaluate(demux.profile()),
        AdmissionProgress::Rejected {
            violations: vec![AdmissionViolation::FormatNotAllowed(DataFormat::Nmea)]
        }
    );
}

#[test]
fn enforce_rejects_excessive_unknown_data() {
    let mut demux = ProtocolDemux::new();
    let mut input = b"garbage".to_vec();
    input.extend_from_slice(&decode_hex(RTCM3_1005));
    demux.push(&input);
    let policy = AdmissionPolicy {
        mode: AdmissionMode::Enforce,
        allowed_formats: vec![DataFormat::Rtcm3],
        require_correction_data: false,
        expected_formats: None,
        allowed_msm_versions: None,
        min_valid_frames: 1,
        max_unknown_ratio: Some(0.1),
        min_observed_bytes: 1,
    };

    assert_eq!(
        policy.evaluate(demux.profile()),
        AdmissionProgress::Rejected {
            violations: vec![AdmissionViolation::UnknownDataRatioExceeded]
        }
    );
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
    synthetic_rtcm3_payload(&payload)
}

fn synthetic_rtcm3_payload(payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![0xD3, 0, payload.len() as u8];
    let length = payload.len();
    frame[1] = (length >> 8) as u8;
    frame[2] = length as u8;
    frame.extend_from_slice(payload);
    let crc = crc24q(&frame);
    frame.extend_from_slice(&crc.to_be_bytes()[1..]);
    frame
}

fn write_bits(data: &mut [u8], start: usize, length: usize, value: u64) {
    for offset in 0..length {
        let bit = (value >> (length - 1 - offset)) & 1;
        let target = start + offset;
        data[target / 8] |= (bit as u8) << (7 - target % 8);
    }
}

fn write_signed_bits(data: &mut [u8], start: usize, length: usize, value: i64) {
    write_bits(data, start, length, value as u64);
}

fn write_msm_header(data: &mut [u8], message_number: u16, station_id: u16, epoch_ms: u32) {
    write_bits(data, 0, 12, message_number.into());
    write_bits(data, 12, 12, station_id.into());
    write_bits(data, 24, 30, epoch_ms.into());
    write_bits(data, 73, 64, 1_u64 << 63);
    write_bits(data, 137, 32, 1_u64 << 31);
}

fn msm4_single_satellite(epoch_ms: u32, satellite_mask: u64, cn0: u64) -> Vec<u8> {
    msm4_single_for(1074, epoch_ms, satellite_mask, cn0)
}

fn msm4_single_for(message_number: u16, epoch_ms: u32, satellite_mask: u64, cn0: u64) -> Vec<u8> {
    let mut payload = vec![0_u8; 32];
    write_bits(&mut payload, 0, 12, message_number.into());
    write_bits(&mut payload, 12, 12, 1);
    write_bits(&mut payload, 24, 30, epoch_ms.into());
    write_bits(&mut payload, 73, 64, satellite_mask);
    write_bits(&mut payload, 137, 32, 1_u64 << 31);
    write_bits(&mut payload, 169, 1, 1);
    write_bits(&mut payload, 230, 6, cn0);
    synthetic_rtcm3_payload(&payload)
}

fn write_legacy_header(
    data: &mut [u8],
    message_number: u16,
    station_id: u16,
    epoch_ms: u32,
    glonass: bool,
) {
    write_bits(data, 0, 12, message_number.into());
    write_bits(data, 12, 12, station_id.into());
    let epoch_bits = if glonass { 27 } else { 30 };
    write_bits(data, 24, epoch_bits, epoch_ms.into());
    let count_offset = 24 + epoch_bits + 1;
    write_bits(data, count_offset, 5, 1);
}

fn write_legacy(data: &mut [u8], offset: &mut usize, length: usize, value: u64) {
    write_bits(data, *offset, length, value);
    *offset += length;
}

fn write_legacy_signed(data: &mut [u8], offset: &mut usize, length: usize, value: i64) {
    write_signed_bits(data, *offset, length, value);
    *offset += length;
}

fn write_ascii(data: &mut [u8], start: usize, value: &str) {
    for (index, byte) in value.bytes().enumerate() {
        write_bits(data, start + index * 8, 8, u64::from(byte));
    }
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

fn policy(
    mode: AdmissionMode,
    min_valid_frames: u64,
    require_correction_data: bool,
) -> AdmissionPolicy {
    AdmissionPolicy {
        mode,
        allowed_formats: vec![DataFormat::Rtcm3, DataFormat::Nmea],
        require_correction_data,
        expected_formats: None,
        allowed_msm_versions: None,
        min_valid_frames,
        max_unknown_ratio: None,
        min_observed_bytes: 0,
    }
}
