use crate::protocol::{DataFormat, DataProfile, GeoPosition};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct StrRecord {
    pub mountpoint: String,
    pub identifier: String,
    pub format: String,
    pub format_details: String,
    pub carrier: String,
    pub navigation_system: String,
    pub network: String,
    pub country: String,
    pub latitude: String,
    pub longitude: String,
    pub nmea: String,
    pub solution: String,
    pub generator: String,
    pub compression: String,
    pub authentication: String,
    pub fee: String,
    pub bitrate: String,
    pub misc: String,
}

impl StrRecord {
    pub fn from_profile(mountpoint: &str, profile: &DataProfile) -> Self {
        let format = match profile.primary_format {
            Some(DataFormat::Rtcm3) => "RTCM 3.3",
            Some(DataFormat::Rtcm2) => "RTCM 2",
            Some(DataFormat::Binex) => "BINEX",
            Some(DataFormat::Cmr) => "CMR/CMR+",
            Some(DataFormat::Nmea) => "NMEA",
            None => "UNKNOWN",
        };
        let details = profile.format_message_numbers();
        let generator = profile
            .device
            .as_ref()
            .map(|device| format!("{} {}", device.receiver_type, device.receiver_firmware))
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "RCaster".to_owned());
        let (latitude, longitude) = profile
            .position
            .map(position_text)
            .unwrap_or_else(|| ("0".to_owned(), "0".to_owned()));
        Self {
            mountpoint: clean_field(mountpoint),
            identifier: clean_field(
                profile
                    .device
                    .as_ref()
                    .map(|device| device.antenna_descriptor.as_str())
                    .filter(|value| !value.is_empty())
                    .unwrap_or(mountpoint),
            ),
            format: format.to_owned(),
            format_details: clean_field(&details),
            carrier: "2".to_owned(),
            navigation_system: "GPS".to_owned(),
            network: "RCaster".to_owned(),
            country: "XXX".to_owned(),
            latitude,
            longitude,
            nmea: "0".to_owned(),
            solution: "0".to_owned(),
            generator: clean_field(&generator),
            compression: "none".to_owned(),
            authentication: "N".to_owned(),
            fee: "N".to_owned(),
            bitrate: "0".to_owned(),
            misc: String::new(),
        }
    }

    pub fn to_line(&self) -> String {
        format!(
            "STR;{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{}\r\n",
            self.mountpoint,
            self.identifier,
            self.format,
            self.format_details,
            self.carrier,
            self.navigation_system,
            self.network,
            self.country,
            self.latitude,
            self.longitude,
            self.nmea,
            self.solution,
            self.generator,
            self.compression,
            self.authentication,
            self.fee,
            self.bitrate,
            self.misc,
        )
    }
}

fn position_text(position: GeoPosition) -> (String, String) {
    (
        decimal_e7(position.latitude_e7),
        decimal_e7(position.longitude_e7),
    )
}

fn decimal_e7(value: i32) -> String {
    let negative = value < 0;
    let absolute = i64::from(value).abs();
    format!(
        "{}{}.{:07}",
        if negative { "-" } else { "" },
        absolute / 10_000_000,
        absolute % 10_000_000
    )
}

fn clean_field(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character == ';' || character.is_control() {
                ' '
            } else {
                character
            }
        })
        .take(128)
        .collect::<String>()
        .trim()
        .to_owned()
}
