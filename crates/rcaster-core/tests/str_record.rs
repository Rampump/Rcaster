use std::collections::BTreeSet;

use rcaster_core::{
    protocol::{DataFormat, DataProfile, DeviceInfo, GeoPosition},
    str_record::StrRecord,
};

#[test]
fn builds_standard_str_line_from_profile() {
    let profile = DataProfile {
        primary_format: Some(DataFormat::Rtcm3),
        valid_frames: 2,
        message_numbers: BTreeSet::from([1005, 1074]),
        position: Some(GeoPosition {
            latitude_e7: 312_340_000,
            longitude_e7: 1_214_560_000,
            height_cm: 1_200,
        }),
        device: Some(DeviceInfo {
            station_id: 7,
            antenna_descriptor: "ANT;1\r\n".to_owned(),
            antenna_serial: "serial".to_owned(),
            receiver_type: "Receiver".to_owned(),
            receiver_firmware: "1.0".to_owned(),
            receiver_serial: "receiver-serial".to_owned(),
        }),
        has_correction_data: true,
        ..DataProfile::default()
    };

    let line = StrRecord::from_profile("C63", &profile).to_line();

    assert_eq!(line.split(';').count(), 19);
    assert!(line.starts_with("STR;C63;ANT 1;RTCM 3.3;1005,1074;"));
    assert!(line.contains(";31.2340000;121.4560000;"));
    assert!(line.contains(";Receiver 1.0;none;N;N;0;"));
    assert!(line.ends_with("0;\r\n"));
}

#[test]
fn nmea_only_uses_nmea_str_format() {
    let profile = DataProfile {
        primary_format: Some(DataFormat::Nmea),
        valid_frames: 1,
        ..DataProfile::default()
    };

    let record = StrRecord::from_profile("NMEA", &profile);

    assert_eq!(record.format, "NMEA");
    assert_eq!(record.format_details, "");
}

#[test]
fn non_rtcm3_formats_use_str_family_names() {
    for (format, expected) in [
        (DataFormat::Rtcm2, "RTCM 2"),
        (DataFormat::Binex, "BINEX"),
        (DataFormat::Cmr, "CMR/CMR+"),
    ] {
        let profile = DataProfile {
            primary_format: Some(format),
            valid_frames: 1,
            ..DataProfile::default()
        };

        assert_eq!(StrRecord::from_profile("TEST", &profile).format, expected);
    }
}
