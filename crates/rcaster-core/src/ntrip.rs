use std::{fmt, str};

pub mod chunked;
pub mod response;

const MAX_REQUEST_LINE_BYTES: usize = 4096;
const MAX_HEADER_BYTES: usize = 16384;
const MAX_HEADER_LINE_BYTES: usize = 4096;
const MAX_HEADER_COUNT: usize = 64;
const MAX_BASIC_DECODED_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NtripVersion {
    One,
    Two,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyFraming {
    Stream,
    ContentLength(u64),
    Chunked,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedHead {
    pub request: Request,
    pub consumed: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    SourceV1(SourceV1Request),
    SourceV2(SourceV2Request),
    Subscribe(SubscribeRequest),
    Sourcetable(SourcetableRequest),
}

#[derive(Clone, PartialEq, Eq)]
pub struct SourceV1Request {
    pub password: String,
    pub mountpoint: String,
    pub body: BodyFraming,
    pub headers: Headers,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceV2Request {
    pub mountpoint: String,
    pub credentials: BasicCredentials,
    pub body: BodyFraming,
    pub headers: Headers,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscribeRequest {
    pub version: NtripVersion,
    pub mountpoint: String,
    pub credentials: Option<BasicCredentials>,
    pub headers: Headers,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourcetableRequest {
    pub version: NtripVersion,
    pub headers: Headers,
}

#[derive(Clone, PartialEq, Eq)]
pub struct BasicCredentials {
    pub username: String,
    pub password: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct Headers(Vec<Header>);

impl Headers {
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|header| header.name.eq_ignore_ascii_case(name))
            .map(|header| header.value.as_str())
    }

    pub fn iter(&self) -> impl Iterator<Item = &Header> {
        self.0.iter()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseError {
    Incomplete,
    RequestLineTooLong,
    HeadersTooLarge,
    HeaderLineTooLong,
    TooManyHeaders,
    InvalidRequestLine,
    MethodNotAllowed,
    InvalidTarget,
    InvalidHeader,
    DuplicateHeader(&'static str),
    ConflictingBodyFraming,
    InvalidContentLength,
    UnsupportedTransferEncoding,
    UnsupportedExpectation,
    MissingAuthorization,
    InvalidAuthorization,
    InvalidCredentials,
    InvalidNtripVersion,
}

impl ParseError {
    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::Incomplete => None,
            Self::RequestLineTooLong
            | Self::HeadersTooLarge
            | Self::HeaderLineTooLong
            | Self::TooManyHeaders => Some(431),
            Self::MethodNotAllowed | Self::UnsupportedTransferEncoding => Some(501),
            Self::UnsupportedExpectation => Some(417),
            Self::MissingAuthorization | Self::InvalidAuthorization | Self::InvalidCredentials => {
                Some(401)
            }
            Self::InvalidRequestLine
            | Self::InvalidTarget
            | Self::InvalidHeader
            | Self::DuplicateHeader(_)
            | Self::ConflictingBodyFraming
            | Self::InvalidContentLength
            | Self::InvalidNtripVersion => Some(400),
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete => formatter.write_str("request head is incomplete"),
            Self::RequestLineTooLong => formatter.write_str("request line exceeds 4096 bytes"),
            Self::HeadersTooLarge => formatter.write_str("headers exceed 16384 bytes"),
            Self::HeaderLineTooLong => formatter.write_str("header line exceeds 4096 bytes"),
            Self::TooManyHeaders => formatter.write_str("request contains more than 64 headers"),
            Self::InvalidRequestLine => formatter.write_str("invalid request line"),
            Self::MethodNotAllowed => formatter.write_str("unsupported NTRIP method"),
            Self::InvalidTarget => formatter.write_str("invalid request target"),
            Self::InvalidHeader => formatter.write_str("invalid request header"),
            Self::DuplicateHeader(name) => write!(formatter, "duplicate {name} header"),
            Self::ConflictingBodyFraming => {
                formatter.write_str("Content-Length conflicts with Transfer-Encoding")
            }
            Self::InvalidContentLength => formatter.write_str("invalid Content-Length"),
            Self::UnsupportedTransferEncoding => {
                formatter.write_str("unsupported Transfer-Encoding")
            }
            Self::UnsupportedExpectation => formatter.write_str("unsupported Expect header"),
            Self::MissingAuthorization => formatter.write_str("authorization is required"),
            Self::InvalidAuthorization => formatter.write_str("invalid authorization header"),
            Self::InvalidCredentials => formatter.write_str("invalid Basic credentials"),
            Self::InvalidNtripVersion => formatter.write_str("invalid Ntrip-Version header"),
        }
    }
}

impl std::error::Error for ParseError {}

pub fn parse_head(input: &[u8]) -> Result<ParsedHead, ParseError> {
    let (line_end, after_line) = match next_line(input, 0) {
        Some(line) => line,
        None if input.len() > MAX_REQUEST_LINE_BYTES => {
            return Err(ParseError::RequestLineTooLong);
        }
        None => return Err(ParseError::Incomplete),
    };
    let request_line = &input[..line_end];
    if request_line.len() > MAX_REQUEST_LINE_BYTES {
        return Err(ParseError::RequestLineTooLong);
    }
    if request_line
        .iter()
        .any(|byte| *byte == 0 || (*byte < b' ' && *byte != b'\t'))
    {
        return Err(ParseError::InvalidRequestLine);
    }
    let request_line = str::from_utf8(request_line).map_err(|_| ParseError::InvalidRequestLine)?;
    let mut parts = request_line
        .split([' ', '\t'])
        .filter(|part| !part.is_empty());
    let method = parts.next().ok_or(ParseError::InvalidRequestLine)?;
    let arguments = parts.collect::<Vec<_>>();

    let (headers, consumed) = parse_headers(input, after_line)?;
    validate_headers(&headers)?;

    let request = match method {
        "SOURCE" => match arguments.as_slice() {
            [password, mountpoint] => Request::SourceV1(SourceV1Request {
                password: (*password).to_owned(),
                mountpoint: parse_source_mountpoint(mountpoint)?,
                body: body_framing(&headers)?,
                headers,
            }),
            _ => return Err(ParseError::InvalidRequestLine),
        },
        "POST" => {
            let [target, version] = arguments.as_slice() else {
                return Err(ParseError::InvalidRequestLine);
            };
            validate_http_version(version)?;
            let mountpoint = parse_http_mountpoint(target)?;
            let authorization = headers
                .get("authorization")
                .ok_or(ParseError::MissingAuthorization)?;
            Request::SourceV2(SourceV2Request {
                mountpoint,
                credentials: parse_basic(authorization)?,
                body: body_framing(&headers)?,
                headers,
            })
        }
        "GET" => {
            let (target, version) = match arguments.as_slice() {
                [target] => (*target, None),
                [target, version] => (*target, Some(*version)),
                _ => return Err(ParseError::InvalidRequestLine),
            };
            if let Some(version) = version {
                validate_http_version(version)?;
            }
            let ntrip_version = ntrip_version(&headers)?;
            let path = target.split_once('?').map_or(target, |(path, _query)| path);
            if path == "/" || path.eq_ignore_ascii_case("/sourcetable") {
                Request::Sourcetable(SourcetableRequest {
                    version: ntrip_version,
                    headers,
                })
            } else {
                let credentials = headers.get("authorization").map(parse_basic).transpose()?;
                Request::Subscribe(SubscribeRequest {
                    version: ntrip_version,
                    mountpoint: parse_http_mountpoint(target)?,
                    credentials,
                    headers,
                })
            }
        }
        _ => return Err(ParseError::MethodNotAllowed),
    };

    Ok(ParsedHead { request, consumed })
}

fn parse_headers(input: &[u8], mut cursor: usize) -> Result<(Headers, usize), ParseError> {
    if cursor == input.len() {
        return Ok((Headers::default(), cursor));
    }
    let mut headers = Vec::new();
    let mut header_bytes = 0_usize;
    loop {
        let (line_end, next) = match next_line(input, cursor) {
            Some(line) => line,
            None if input.len() - cursor > MAX_HEADER_LINE_BYTES => {
                return Err(ParseError::HeaderLineTooLong);
            }
            None => return Err(ParseError::Incomplete),
        };
        let line = &input[cursor..line_end];
        if line.len() > MAX_HEADER_LINE_BYTES {
            return Err(ParseError::HeaderLineTooLong);
        }
        header_bytes = header_bytes.saturating_add(next - cursor);
        if header_bytes > MAX_HEADER_BYTES {
            return Err(ParseError::HeadersTooLarge);
        }
        cursor = next;
        if line.is_empty() {
            return Ok((Headers(headers), cursor));
        }
        if headers.len() == MAX_HEADER_COUNT {
            return Err(ParseError::TooManyHeaders);
        }
        headers.push(parse_header(line)?);
    }
}

fn parse_header(line: &[u8]) -> Result<Header, ParseError> {
    if line.first().is_some_and(u8::is_ascii_whitespace) {
        return Err(ParseError::InvalidHeader);
    }
    let colon = line
        .iter()
        .position(|byte| *byte == b':')
        .ok_or(ParseError::InvalidHeader)?;
    let name = &line[..colon];
    if name.is_empty() || !name.iter().all(|byte| is_header_token(*byte)) {
        return Err(ParseError::InvalidHeader);
    }
    let value = trim_ascii(&line[colon + 1..]);
    if value
        .iter()
        .any(|byte| *byte == 0 || (*byte < b' ' && *byte != b'\t') || *byte == 0x7f)
    {
        return Err(ParseError::InvalidHeader);
    }
    Ok(Header {
        name: str::from_utf8(name)
            .map_err(|_| ParseError::InvalidHeader)?
            .to_owned(),
        value: str::from_utf8(value)
            .map_err(|_| ParseError::InvalidHeader)?
            .to_owned(),
    })
}

fn validate_headers(headers: &Headers) -> Result<(), ParseError> {
    for name in [
        "authorization",
        "content-length",
        "transfer-encoding",
        "host",
    ] {
        if headers
            .iter()
            .filter(|header| header.name.eq_ignore_ascii_case(name))
            .count()
            > 1
        {
            return Err(ParseError::DuplicateHeader(name));
        }
    }
    if let Some(expect) = headers.get("expect")
        && !expect.eq_ignore_ascii_case("100-continue")
    {
        return Err(ParseError::UnsupportedExpectation);
    }
    Ok(())
}

fn body_framing(headers: &Headers) -> Result<BodyFraming, ParseError> {
    let content_length = headers.get("content-length");
    let transfer_encoding = headers.get("transfer-encoding");
    match (content_length, transfer_encoding) {
        (Some(_), Some(_)) => Err(ParseError::ConflictingBodyFraming),
        (_, Some(encoding)) if !encoding.eq_ignore_ascii_case("chunked") => {
            Err(ParseError::UnsupportedTransferEncoding)
        }
        (_, Some(_)) => Ok(BodyFraming::Chunked),
        (Some(length), None) => length
            .parse()
            .map(BodyFraming::ContentLength)
            .map_err(|_| ParseError::InvalidContentLength),
        (None, None) => Ok(BodyFraming::Stream),
    }
}

fn ntrip_version(headers: &Headers) -> Result<NtripVersion, ParseError> {
    let Some(version) = headers.get("ntrip-version") else {
        return Ok(NtripVersion::One);
    };
    if version.eq_ignore_ascii_case("Ntrip/2.0") {
        return Ok(NtripVersion::Two);
    }
    if headers
        .get("user-agent")
        .is_some_and(|agent| agent.to_ascii_lowercase().contains("ntrip"))
    {
        return Ok(NtripVersion::One);
    }
    Err(ParseError::InvalidNtripVersion)
}

fn parse_basic(value: &str) -> Result<BasicCredentials, ParseError> {
    let mut parts = value.split_ascii_whitespace();
    let scheme = parts.next().ok_or(ParseError::InvalidAuthorization)?;
    let encoded = parts.next().ok_or(ParseError::InvalidAuthorization)?;
    if !scheme.eq_ignore_ascii_case("basic") || parts.next().is_some() {
        return Err(ParseError::InvalidAuthorization);
    }
    if encoded.len() > MAX_BASIC_DECODED_BYTES.div_ceil(3) * 4 {
        return Err(ParseError::InvalidCredentials);
    }
    let decoded = decode_base64(encoded)?;
    let decoded = str::from_utf8(&decoded).map_err(|_| ParseError::InvalidCredentials)?;
    let (username, password) = decoded
        .split_once(':')
        .ok_or(ParseError::InvalidCredentials)?;
    Ok(BasicCredentials {
        username: username.to_owned(),
        password: password.to_owned(),
    })
}

fn decode_base64(value: &str) -> Result<Vec<u8>, ParseError> {
    let value = value.as_bytes();
    if value.is_empty() || !value.len().is_multiple_of(4) {
        return Err(ParseError::InvalidCredentials);
    }
    let padding = value.iter().rev().take_while(|byte| **byte == b'=').count();
    if padding > 2 || value[..value.len() - padding].contains(&b'=') {
        return Err(ParseError::InvalidCredentials);
    }
    let mut result = Vec::with_capacity(value.len() / 4 * 3);
    let mut accumulator = 0_u32;
    let mut bits = 0_u8;
    for byte in &value[..value.len() - padding] {
        let digit = base64_digit(*byte).ok_or(ParseError::InvalidCredentials)?;
        accumulator = (accumulator << 6) | u32::from(digit);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            result.push((accumulator >> bits) as u8);
            accumulator &= (1 << bits) - 1;
            if result.len() > MAX_BASIC_DECODED_BYTES {
                return Err(ParseError::InvalidCredentials);
            }
        }
    }
    if bits > 0 && accumulator != 0 {
        return Err(ParseError::InvalidCredentials);
    }
    Ok(result)
}

fn parse_http_mountpoint(target: &str) -> Result<String, ParseError> {
    let path = target.split_once('?').map_or(target, |(path, _query)| path);
    let mountpoint = path.strip_prefix('/').ok_or(ParseError::InvalidTarget)?;
    validate_mountpoint(mountpoint)
}

fn parse_source_mountpoint(target: &str) -> Result<String, ParseError> {
    let path = target.split_once('?').map_or(target, |(path, _query)| path);
    validate_mountpoint(path.strip_prefix('/').unwrap_or(path))
}

pub fn validate_mountpoint(mountpoint: &str) -> Result<String, ParseError> {
    if mountpoint.is_empty()
        || mountpoint.len() > 100
        || !mountpoint.is_ascii()
        || !mountpoint
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        || mountpoint.eq_ignore_ascii_case("sourcetable")
    {
        return Err(ParseError::InvalidTarget);
    }
    Ok(mountpoint.to_owned())
}

fn validate_http_version(version: &str) -> Result<(), ParseError> {
    if matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        Ok(())
    } else {
        Err(ParseError::InvalidRequestLine)
    }
}

fn next_line(input: &[u8], start: usize) -> Option<(usize, usize)> {
    let newline = start + input.get(start..)?.iter().position(|byte| *byte == b'\n')?;
    let line_end = if newline > start && input[newline - 1] == b'\r' {
        newline - 1
    } else {
        newline
    };
    Some((line_end, newline + 1))
}

fn trim_ascii(mut value: &[u8]) -> &[u8] {
    while value
        .first()
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
    {
        value = &value[1..];
    }
    while value
        .last()
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
    {
        value = &value[..value.len() - 1];
    }
    value
}

fn is_header_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn base64_digit(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

impl fmt::Debug for SourceV1Request {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceV1Request")
            .field("password", &"***")
            .field("mountpoint", &self.mountpoint)
            .field("body", &self.body)
            .field("headers", &self.headers)
            .finish()
    }
}

impl fmt::Debug for BasicCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BasicCredentials")
            .field("username", &self.username)
            .field("password", &"***")
            .finish()
    }
}

impl fmt::Debug for Headers {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = formatter.debug_map();
        for header in &self.0 {
            let value = if header.name.eq_ignore_ascii_case("authorization") {
                "***"
            } else {
                header.value.as_str()
            };
            map.entry(&header.name, &value);
        }
        map.finish()
    }
}
