use std::{
    fmt, io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    time::Duration,
};

use tokio::{io::AsyncReadExt, net::TcpStream};

const V1_SIGNATURE: &[u8] = b"PROXY ";
const V2_SIGNATURE: &[u8] = b"\r\n\r\n\0\r\nQUIT\n";
const V1_MAX_LINE: usize = 107;
const V2_MAX_BODY: usize = 536;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyProtocolConfig {
    trusted: Vec<IpCidr>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct IpCidr {
    network: IpAddr,
    prefix: u8,
}

impl ProxyProtocolConfig {
    pub fn new(cidrs: &[String]) -> Result<Self, ProxyProtocolError> {
        if cidrs.is_empty() {
            return Err(ProxyProtocolError::InvalidTrustedCidr(
                "at least one trusted CIDR is required".to_owned(),
            ));
        }
        Ok(Self {
            trusted: cidrs
                .iter()
                .map(|cidr| IpCidr::parse(cidr))
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    pub fn trusts(&self, address: IpAddr) -> bool {
        self.trusted.iter().any(|cidr| cidr.contains(address))
    }
}

#[derive(Debug)]
pub enum ProxyProtocolError {
    InvalidTrustedCidr(String),
    InvalidHeader(&'static str),
    Timeout,
    Io(io::Error),
}

impl fmt::Display for ProxyProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTrustedCidr(message) => {
                write!(formatter, "invalid trusted PROXY CIDR: {message}")
            }
            Self::InvalidHeader(message) => {
                write!(formatter, "invalid PROXY protocol header: {message}")
            }
            Self::Timeout => formatter.write_str("PROXY protocol header timed out"),
            Self::Io(error) => write!(formatter, "PROXY protocol read failed: {error}"),
        }
    }
}

impl std::error::Error for ProxyProtocolError {}

impl From<io::Error> for ProxyProtocolError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub async fn resolve_client_ip(
    stream: &mut TcpStream,
    peer_ip: IpAddr,
    config: Option<&ProxyProtocolConfig>,
) -> Result<IpAddr, ProxyProtocolError> {
    tokio::time::timeout(
        Duration::from_secs(5),
        resolve_client_ip_inner(stream, peer_ip, config),
    )
    .await
    .map_err(|_| ProxyProtocolError::Timeout)?
}

async fn resolve_client_ip_inner(
    stream: &mut TcpStream,
    peer_ip: IpAddr,
    config: Option<&ProxyProtocolConfig>,
) -> Result<IpAddr, ProxyProtocolError> {
    let Some(config) = config else {
        return Ok(peer_ip);
    };
    let Some(prefix) = peek_prefix(stream).await? else {
        return if config.trusts(peer_ip) {
            Err(ProxyProtocolError::InvalidHeader(
                "trusted L4 source did not send a header",
            ))
        } else {
            Ok(peer_ip)
        };
    };
    if !config.trusts(peer_ip) {
        return Err(ProxyProtocolError::InvalidHeader(
            "header came from an untrusted source",
        ));
    }
    if prefix.starts_with(V1_SIGNATURE) {
        return read_v1(stream, peer_ip).await;
    }
    if prefix.starts_with(V2_SIGNATURE) {
        return read_v2(stream, peer_ip).await;
    }
    Err(ProxyProtocolError::InvalidHeader(
        "trusted L4 source sent an unknown signature",
    ))
}

async fn peek_prefix(stream: &TcpStream) -> Result<Option<Vec<u8>>, ProxyProtocolError> {
    let mut buffer = [0_u8; V2_SIGNATURE.len()];
    loop {
        let read = stream.peek(&mut buffer).await?;
        if read == 0 {
            return Ok(None);
        }
        let bytes = &buffer[..read];
        if bytes.starts_with(V1_SIGNATURE) || bytes.starts_with(V2_SIGNATURE) {
            return Ok(Some(bytes[..read].to_vec()));
        }
        let v1_possible = V1_SIGNATURE.starts_with(bytes);
        let v2_possible = V2_SIGNATURE.starts_with(bytes);
        if read >= V1_SIGNATURE.len() && !v1_possible && !v2_possible {
            return Ok(None);
        }
        if !v1_possible && !v2_possible {
            return Ok(None);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn read_v1(stream: &mut TcpStream, peer_ip: IpAddr) -> Result<IpAddr, ProxyProtocolError> {
    let mut line = Vec::with_capacity(V1_MAX_LINE);
    while line.len() < V1_MAX_LINE {
        let byte = stream.read_u8().await?;
        line.push(byte);
        if line.ends_with(b"\r\n") {
            return Ok(parse_v1(&line)?.unwrap_or(peer_ip));
        }
    }
    Err(ProxyProtocolError::InvalidHeader("v1 header is too long"))
}

async fn read_v2(stream: &mut TcpStream, peer_ip: IpAddr) -> Result<IpAddr, ProxyProtocolError> {
    let mut header = [0_u8; 16];
    stream.read_exact(&mut header).await?;
    if &header[..V2_SIGNATURE.len()] != V2_SIGNATURE {
        return Err(ProxyProtocolError::InvalidHeader("v2 signature mismatch"));
    }
    let length = usize::from(u16::from_be_bytes([header[14], header[15]]));
    if length > V2_MAX_BODY {
        return Err(ProxyProtocolError::InvalidHeader("v2 body is too long"));
    }
    let mut body = vec![0_u8; length];
    stream.read_exact(&mut body).await?;
    Ok(parse_v2(&header, &body)?.unwrap_or(peer_ip))
}

fn parse_v1(line: &[u8]) -> Result<Option<IpAddr>, ProxyProtocolError> {
    let line = std::str::from_utf8(line)
        .map_err(|_| ProxyProtocolError::InvalidHeader("v1 header is not ASCII"))?;
    let line = line
        .strip_suffix("\r\n")
        .ok_or(ProxyProtocolError::InvalidHeader("v1 header has no CRLF"))?;
    let fields = line.split(' ').collect::<Vec<_>>();
    match fields.as_slice() {
        ["PROXY", "UNKNOWN", ..] => Ok(None),
        [
            "PROXY",
            "TCP4",
            source,
            _destination,
            source_port,
            destination_port,
        ] if source_port.parse::<u16>().is_ok() && destination_port.parse::<u16>().is_ok() => {
            Ok(Some(source.parse::<Ipv4Addr>().map(IpAddr::V4).map_err(
                |_| ProxyProtocolError::InvalidHeader("invalid TCP4 source address"),
            )?))
        }
        [
            "PROXY",
            "TCP6",
            source,
            _destination,
            source_port,
            destination_port,
        ] if source_port.parse::<u16>().is_ok() && destination_port.parse::<u16>().is_ok() => {
            Ok(Some(source.parse::<Ipv6Addr>().map(IpAddr::V6).map_err(
                |_| ProxyProtocolError::InvalidHeader("invalid TCP6 source address"),
            )?))
        }
        _ => Err(ProxyProtocolError::InvalidHeader("invalid v1 fields")),
    }
}

fn parse_v2(header: &[u8; 16], body: &[u8]) -> Result<Option<IpAddr>, ProxyProtocolError> {
    if header[12] >> 4 != 2 {
        return Err(ProxyProtocolError::InvalidHeader("v2 version mismatch"));
    }
    let command = header[12] & 0x0F;
    let family_protocol = header[13];
    match (command, family_protocol) {
        (0, _) => Ok(None),
        (1, 0x11) if body.len() >= 4 => Ok(Some(IpAddr::V4(Ipv4Addr::new(
            body[0], body[1], body[2], body[3],
        )))),
        (1, 0x21) if body.len() >= 16 => {
            let bytes: [u8; 16] = body[..16].try_into().expect("validated IPv6 length");
            Ok(Some(IpAddr::V6(Ipv6Addr::from(bytes))))
        }
        (1, _) => Err(ProxyProtocolError::InvalidHeader(
            "v2 protocol has no usable transport address",
        )),
        _ => Err(ProxyProtocolError::InvalidHeader("invalid v2 command")),
    }
}

impl IpCidr {
    fn parse(value: &str) -> Result<Self, ProxyProtocolError> {
        let (address, prefix) = value.split_once('/').ok_or_else(|| {
            ProxyProtocolError::InvalidTrustedCidr(format!("{value} has no prefix"))
        })?;
        let address = address.parse::<IpAddr>().map_err(|_| {
            ProxyProtocolError::InvalidTrustedCidr(format!("{address} is not an IP address"))
        })?;
        let prefix = prefix.parse::<u8>().map_err(|_| {
            ProxyProtocolError::InvalidTrustedCidr(format!("{prefix} is not a prefix length"))
        })?;
        let network = match address {
            IpAddr::V4(address) if prefix <= 32 => {
                let mask = if prefix == 0 {
                    0
                } else {
                    u32::MAX << (32 - prefix)
                };
                IpAddr::V4(Ipv4Addr::from(u32::from(address) & mask))
            }
            IpAddr::V6(address) if prefix <= 128 => {
                let mask = if prefix == 0 {
                    0
                } else {
                    u128::MAX << (128 - prefix)
                };
                IpAddr::V6(Ipv6Addr::from(u128::from(address) & mask))
            }
            _ => {
                return Err(ProxyProtocolError::InvalidTrustedCidr(format!(
                    "{value} has an invalid prefix length"
                )));
            }
        };
        Ok(Self { network, prefix })
    }

    fn contains(self, address: IpAddr) -> bool {
        match (self.network, address) {
            (IpAddr::V4(network), IpAddr::V4(address)) => {
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u32::MAX << (32 - self.prefix)
                };
                u32::from(network) & mask == u32::from(address) & mask
            }
            (IpAddr::V6(network), IpAddr::V6(address)) => {
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u128::MAX << (128 - self.prefix)
                };
                u128::from(network) & mask == u128::from(address) & mask
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use super::{ProxyProtocolConfig, parse_v1, parse_v2};

    #[test]
    fn parses_proxy_v1_ipv4_and_ipv6() {
        assert_eq!(
            parse_v1(b"PROXY TCP4 192.0.2.10 198.51.100.1 4000 2101\r\n")
                .unwrap()
                .unwrap(),
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10))
        );
        assert_eq!(
            parse_v1(b"PROXY TCP6 2001:db8::10 2001:db8::1 4000 2101\r\n")
                .unwrap()
                .unwrap(),
            IpAddr::V6("2001:db8::10".parse::<Ipv6Addr>().unwrap())
        );
    }

    #[test]
    fn parses_proxy_v2_ipv4_and_ipv6() {
        let mut ipv4 = [0_u8; 16];
        ipv4[12] = 0x21;
        ipv4[13] = 0x11;
        ipv4[14..].copy_from_slice(&12_u16.to_be_bytes());
        let mut body = vec![192, 0, 2, 20, 198, 51, 100, 2];
        body.extend_from_slice(&4000_u16.to_be_bytes());
        body.extend_from_slice(&2101_u16.to_be_bytes());
        assert_eq!(
            parse_v2(&ipv4, &body).unwrap().unwrap(),
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 20))
        );

        let mut ipv6 = [0_u8; 16];
        ipv6[12] = 0x21;
        ipv6[13] = 0x21;
        ipv6[14..].copy_from_slice(&36_u16.to_be_bytes());
        let mut body = "2001:db8::20"
            .parse::<Ipv6Addr>()
            .unwrap()
            .octets()
            .to_vec();
        body.extend_from_slice(&[0; 16]);
        body.extend_from_slice(&4000_u16.to_be_bytes());
        body.extend_from_slice(&2101_u16.to_be_bytes());
        assert_eq!(
            parse_v2(&ipv6, &body).unwrap().unwrap(),
            IpAddr::V6("2001:db8::20".parse().unwrap())
        );
    }

    #[test]
    fn trusted_cidrs_match_only_their_network_family() {
        let config =
            ProxyProtocolConfig::new(&["10.20.0.0/24".to_owned(), "2001:db8::/32".to_owned()])
                .unwrap();
        assert!(config.trusts("10.20.0.42".parse().unwrap()));
        assert!(!config.trusts("10.21.0.42".parse().unwrap()));
        assert!(config.trusts("2001:db8::42".parse().unwrap()));
        assert!(!config.trusts("2001:db9::42".parse().unwrap()));
    }
}
