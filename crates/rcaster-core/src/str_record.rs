use crate::{
    account::Mountpoint,
    protocol::{DataFormat, DataProfile, GeoPosition, GnssSystem},
};
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
        Self::from_profile_and_metadata(mountpoint, profile, None)
    }

    pub fn from_profile_and_mountpoint(
        mountpoint: &str,
        profile: &DataProfile,
        configured: &Mountpoint,
    ) -> Self {
        Self::from_profile_and_metadata(mountpoint, profile, Some(configured))
    }

    fn from_profile_and_metadata(
        mountpoint: &str,
        profile: &DataProfile,
        configured: Option<&Mountpoint>,
    ) -> Self {
        let fields = configured.and_then(|mountpoint| mountpoint.str_fields.as_ref());
        let format = detected_format(profile)
            .or_else(|| fields.and_then(|fields| fields.format.as_deref()))
            .unwrap_or("UNKNOWN");
        let detected_details = profile.format_message_numbers();
        let details = if detected_details.is_empty() {
            fields
                .and_then(|fields| fields.formats.clone())
                .unwrap_or_default()
        } else {
            detected_details
        };
        let generator = profile
            .device
            .as_ref()
            .map(|device| format!("{} {}", device.receiver_type, device.receiver_firmware))
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "RCaster".to_owned());
        let (latitude, longitude) = profile
            .position
            .map(position_text)
            .or_else(|| {
                configured.and_then(|mountpoint| {
                    Some((
                        format!("{:.7}", mountpoint.latitude?),
                        format!("{:.7}", mountpoint.longitude?),
                    ))
                })
            })
            .unwrap_or_else(|| ("0".to_owned(), "0".to_owned()));
        let identifier = profile
            .device
            .as_ref()
            .map(|device| device.antenna_descriptor.as_str())
            .filter(|value| !value.is_empty())
            .or_else(|| fields.and_then(|fields| fields.identifier.as_deref()))
            .unwrap_or(mountpoint);
        let navigation_system = navigation_system(profile)
            .or_else(|| fields.and_then(|fields| fields.navigation_system.clone()))
            .unwrap_or_else(|| "GPS".to_owned());
        Self {
            mountpoint: clean_field(mountpoint),
            identifier: clean_field(identifier),
            format: format.to_owned(),
            format_details: clean_field(&details),
            carrier: fields
                .and_then(|fields| fields.carrier.clone())
                .unwrap_or_else(|| "2".to_owned()),
            navigation_system,
            network: fields
                .and_then(|fields| fields.network.clone())
                .unwrap_or_else(|| "RCaster".to_owned()),
            country: fields
                .and_then(|fields| fields.country.clone())
                .unwrap_or_else(|| "XXX".to_owned()),
            latitude,
            longitude,
            nmea: "0".to_owned(),
            solution: fields
                .and_then(|fields| fields.solution.clone())
                .unwrap_or_else(|| "0".to_owned()),
            generator: clean_field(&generator),
            compression: "none".to_owned(),
            authentication: "N".to_owned(),
            fee: "N".to_owned(),
            bitrate: "0".to_owned(),
            misc: if profile.primary_format == Some(DataFormat::Nmea)
                && !profile.has_correction_data
            {
                "nmea_only".to_owned()
            } else {
                String::new()
            },
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

fn detected_format(profile: &DataProfile) -> Option<&'static str> {
    match profile.primary_format {
        Some(DataFormat::Rtcm3) => Some("RTCM 3.3"),
        Some(DataFormat::Rtcm2) => Some("RTCM 2"),
        Some(DataFormat::Binex) => Some("BINEX"),
        Some(DataFormat::Cmr) => Some("CMR/CMR+"),
        Some(DataFormat::Nmea) => Some("NMEA"),
        None => None,
    }
}

fn navigation_system(profile: &DataProfile) -> Option<String> {
    let systems = profile
        .msm_observations
        .iter()
        .map(|observation| match observation.system {
            GnssSystem::Gps => "GPS",
            GnssSystem::Glonass => "GLO",
            GnssSystem::Galileo => "GAL",
            GnssSystem::Sbas => "SBS",
            GnssSystem::Qzss => "QZS",
            GnssSystem::NavIc => "NAVIC",
            GnssSystem::Bds => "BDS",
        })
        .collect::<std::collections::BTreeSet<_>>();
    (!systems.is_empty()).then(|| systems.into_iter().collect::<Vec<_>>().join("+"))
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
