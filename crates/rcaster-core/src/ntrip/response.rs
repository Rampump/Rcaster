use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ntrip1Error {
    BadPassword,
    MountpointUnavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Response<'a> {
    Ntrip1Data,
    Ntrip1Sourcetable {
        body: &'a str,
    },
    Ntrip1Error(Ntrip1Error),
    Ntrip2Data {
        server_version: &'a str,
        date: &'a str,
    },
    Ntrip2Sourcetable {
        server_version: &'a str,
        date: &'a str,
        content_type: &'a str,
        body: &'a str,
        keep_alive: bool,
    },
    Ntrip2Error {
        status: u16,
        retry_after: Option<u32>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResponseError {
    InvalidHeaderValue,
    UnsupportedStatus(u16),
}

impl Response<'_> {
    pub fn encode(self) -> Result<Vec<u8>, ResponseError> {
        match self {
            Self::Ntrip1Data => Ok(b"ICY 200 OK\r\n\r\n".to_vec()),
            Self::Ntrip1Sourcetable { body } => Ok(format!(
                "SOURCETABLE 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .into_bytes()),
            Self::Ntrip1Error(error) => Ok(match error {
                Ntrip1Error::BadPassword => b"ERROR - Bad Password\r\n".to_vec(),
                Ntrip1Error::MountpointUnavailable => {
                    b"ERROR - Mount Point Taken or Invalid\r\n".to_vec()
                }
            }),
            Self::Ntrip2Data {
                server_version,
                date,
            } => {
                validate_header_value(server_version)?;
                validate_header_value(date)?;
                Ok(format!(
                    "HTTP/1.1 200 OK\r\nServer: NTRIP RCaster/{server_version}\r\nDate: {date}\r\nNtrip-Version: Ntrip/2.0\r\nContent-Type: gnss/data\r\nCache-Control: no-store, no-cache, max-age=0\r\nPragma: no-cache\r\nConnection: close\r\n\r\n"
                )
                .into_bytes())
            }
            Self::Ntrip2Sourcetable {
                server_version,
                date,
                content_type,
                body,
                keep_alive,
            } => {
                validate_header_value(server_version)?;
                validate_header_value(date)?;
                validate_header_value(content_type)?;
                let connection = if keep_alive { "keep-alive" } else { "close" };
                Ok(format!(
                    "HTTP/1.1 200 OK\r\nServer: NTRIP RCaster/{server_version}\r\nDate: {date}\r\nNtrip-Version: Ntrip/2.0\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: {connection}\r\n\r\n{body}",
                    body.len()
                )
                .into_bytes())
            }
            Self::Ntrip2Error {
                status,
                retry_after,
            } => encode_error(status, retry_after),
        }
    }
}

pub fn encode_ntrip2_error(
    status: u16,
    retry_after: Option<u32>,
) -> Result<Vec<u8>, ResponseError> {
    encode_error(status, retry_after)
}

impl fmt::Display for ResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidHeaderValue => formatter.write_str("invalid response header value"),
            Self::UnsupportedStatus(status) => {
                write!(formatter, "unsupported HTTP status {status}")
            }
        }
    }
}

impl std::error::Error for ResponseError {}

fn encode_error(status: u16, retry_after: Option<u32>) -> Result<Vec<u8>, ResponseError> {
    let reason = status_reason(status).ok_or(ResponseError::UnsupportedStatus(status))?;
    let mut response = format!("HTTP/1.1 {status} {reason}\r\n");
    if status == 401 {
        response.push_str("WWW-Authenticate: Basic realm=\"NTRIP\"\r\n");
    }
    response.push_str("Content-Length: 0\r\n");
    if let Some(seconds) = retry_after {
        response.push_str(&format!("Retry-After: {seconds}\r\n"));
    }
    response.push_str("Connection: close\r\n\r\n");
    Ok(response.into_bytes())
}

fn status_reason(status: u16) -> Option<&'static str> {
    Some(match status {
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        417 => "Expectation Failed",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => return None,
    })
}

fn validate_header_value(value: &str) -> Result<(), ResponseError> {
    if value.is_empty() || value.bytes().any(|byte| byte < b' ' || byte == 0x7f) {
        return Err(ResponseError::InvalidHeaderValue);
    }
    Ok(())
}
