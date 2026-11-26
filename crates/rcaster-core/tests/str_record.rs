use std::collections::BTreeSet;

use rcaster_core::{
    account::{Mountpoint, MountpointStrConfig},
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

#[test]
fn configured_mountpoint_fields_survive_when_profile_has_no_position_or_device() {
    let mountpoint = mountpoint_with_str();
    let profile = DataProfile {
        primary_format: Some(DataFormat::Nmea),
        valid_frames: 1,
        ..DataProfile::default()
    };

    let record = StrRecord::from_profile_and_mountpoint("C63", &profile, &mountpoint);

    assert_eq!(record.identifier, "Configured Farm");
    assert_eq!(record.network, "RTK-NET");
    assert_eq!(record.country, "CHN");
    assert_eq!(record.carrier, "1");
    assert_eq!(record.navigation_system, "GPS+GLO");
    assert_eq!(record.latitude, "31.2000000");
    assert_eq!(record.longitude, "121.4000000");
    assert_eq!(record.misc, "nmea_only");
}

#[test]
fn detected_position_overrides_configured_mountpoint_position() {
    let mountpoint = mountpoint_with_str();
    let profile = DataProfile {
        primary_format: Some(DataFormat::Rtcm3),
        valid_frames: 2,
        message_numbers: BTreeSet::from([1005, 1074]),
        position: Some(GeoPosition {
            latitude_e7: 300_000_000,
            longitude_e7: 1_200_000_000,
            height_cm: 0,
        }),
        has_correction_data: true,
        ..DataProfile::default()
    };

    let record = StrRecord::from_profile_and_mountpoint("C63", &profile, &mountpoint);

    assert_eq!(record.latitude, "30.0000000");
    assert_eq!(record.longitude, "120.0000000");
    assert_eq!(record.format_details, "1005,1074");
}

#[test]
fn str_rendering_neutralizes_field_separators_and_control_characters() {
    let mut record = StrRecord::from_profile("C63", &DataProfile::default());
    record.format = "RTCM3;STR;EVIL".to_owned();
    record.network = "NET\r\nSTR;FAKE".to_owned();
    record.country = "CH\0N".to_owned();

    let line = record.to_line();
    assert_eq!(line.split(';').count(), 19);
    assert!(!line.contains('\0'));
    assert_eq!(line.matches("\r\n").count(), 1);
}

fn mountpoint_with_str() -> Mountpoint {
    Mountpoint {
        schema_version: 1,
        mod_revision: 1,
        mountpoint: "C63".to_owned(),
        owner_user_id: "owner-id".to_owned(),
        enabled: true,
        ntrip1_upload_password_hash: None,
        advertise: true,
        region: Some("east".to_owned()),
        latitude: Some(31.2),
        longitude: Some(121.4),
        coverage_radius_m: Some(30_000.0),
        str_fields: Some(MountpointStrConfig {
            identifier: Some("Configured Farm".to_owned()),
            format: Some("CONFIGURED".to_owned()),
            formats: Some("1005,1006".to_owned()),
            carrier: Some("1".to_owned()),
            navigation_system: Some("GPS+GLO".to_owned()),
            network: Some("RTK-NET".to_owned()),
            country: Some("CHN".to_owned()),
            solution: Some("1".to_owned()),
        }),
        source_expectation: None,
        created_at: "2026-11-24T00:00:00Z".to_owned(),
        updated_at: "2026-11-24T00:00:00Z".to_owned(),
    }
}

#[test]
fn configured_str_fields_reject_separators_and_controls() {
    let mut mountpoint = mountpoint_with_str();
    mountpoint.str_fields.as_mut().expect("STR fields").network =
        Some("NET\r\nSTR;FAKE".to_owned());

    assert!(
        rcaster_core::account::AccountSnapshot::new([owner()], [mountpoint]).is_err(),
        "unsafe STR metadata must be rejected"
    );
}

fn owner() -> rcaster_core::account::User {
    rcaster_core::account::User {
        schema_version: 1,
        mod_revision: 0,
        user_id: "owner-id".to_owned(),
        username: "owner".to_owned(),
        password_hash: "hash".to_owned(),
        auth_version: 1,
        enabled: true,
        ntrip_enabled: true,
        web_enabled: true,
        role: rcaster_core::account::UserRole::User,
        created_at: String::new(),
        updated_at: String::new(),
    }
}
