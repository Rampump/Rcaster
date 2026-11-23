use rcaster_core::ntrip::{
    BasicCredentials, BodyFraming, NtripVersion, ParseError, Request, SourceV1Request, parse_head,
};

#[test]
fn parses_ntrip1_source_and_redacts_its_password() {
    let input = b"SOURCE upload-secret /C63\r\nUser-Agent: NTRIP TestSource/1.0\r\n\r\n";

    let parsed = parse_head(input).expect("NTRIP1 source");
    assert!(!format!("{:?}", parsed.request).contains("upload-secret"));
    let Request::SourceV1(SourceV1Request {
        password,
        mountpoint,
        headers,
        ..
    }) = parsed.request
    else {
        panic!("expected NTRIP1 source");
    };

    assert_eq!(password, "upload-secret");
    assert_eq!(mountpoint, "C63");
    assert_eq!(headers.get("user-agent"), Some("NTRIP TestSource/1.0"));
    assert_eq!(parsed.consumed, input.len());
}

#[test]
fn parses_ntrip2_post_with_basic_credentials() {
    let input = b"POST /C63 HTTP/1.1\r\nAuthorization: Basic b3duZXI6c2VjcmV0\r\n\r\n";

    let parsed = parse_head(input).expect("NTRIP2 source");
    let Request::SourceV2(request) = parsed.request else {
        panic!("expected NTRIP2 source");
    };

    assert_eq!(request.mountpoint, "C63");
    assert_eq!(request.body, BodyFraming::Stream);
    assert_eq!(
        request.credentials,
        BasicCredentials {
            username: "owner".to_owned(),
            password: "secret".to_owned(),
        }
    );
    assert!(!format!("{:?}", request.credentials).contains("secret"));
}

#[test]
fn parses_versionless_get_as_ntrip1_subscription() {
    let input = b"GET /C63\r\nUser-Agent: NTRIP RTKLIB/2.4.3\r\n\r\n";

    let parsed = parse_head(input).expect("NTRIP1 subscription");
    let Request::Subscribe(request) = parsed.request else {
        panic!("expected subscription");
    };

    assert_eq!(request.version, NtripVersion::One);
    assert_eq!(request.mountpoint, "C63");
}

#[test]
fn detects_ntrip2_headers_case_insensitively() {
    let input = b"GET /C63 HTTP/1.1\r\nnTrIp-VeRsIoN: nTrIp/2.0\r\n\r\n";

    let parsed = parse_head(input).expect("NTRIP2 subscription");
    let Request::Subscribe(request) = parsed.request else {
        panic!("expected subscription");
    };

    assert_eq!(request.version, NtripVersion::Two);
}

#[test]
fn parses_sourcetable_alias_and_ignores_query_string() {
    let input = b"GET /sourceTable?format=text HTTP/1.1\r\n\r\n";

    let parsed = parse_head(input).expect("sourcetable");
    let Request::Sourcetable(request) = parsed.request else {
        panic!("expected sourcetable");
    };

    assert_eq!(request.version, NtripVersion::One);
}

#[test]
fn returns_fixed_and_chunked_body_framing() {
    let fixed = b"POST /C63 HTTP/1.1\r\nAuthorization: Basic b3duZXI6c2VjcmV0\r\nContent-Length: 42\r\n\r\n";
    let Request::SourceV2(fixed) = parse_head(fixed).expect("fixed source").request else {
        panic!("expected NTRIP2 source");
    };
    assert_eq!(fixed.body, BodyFraming::ContentLength(42));

    let chunked = b"POST /C63 HTTP/1.1\r\nAuthorization: Basic b3duZXI6c2VjcmV0\r\nTransfer-Encoding: chunked\r\n\r\n";
    let Request::SourceV2(chunked) = parse_head(chunked).expect("chunked source").request else {
        panic!("expected NTRIP2 source");
    };
    assert_eq!(chunked.body, BodyFraming::Chunked);
}

#[test]
fn rejects_content_length_with_transfer_encoding() {
    let input = b"POST /C63 HTTP/1.1\r\nAuthorization: Basic b3duZXI6c2VjcmV0\r\nContent-Length: 10\r\nTransfer-Encoding: chunked\r\n\r\n";

    let error = parse_head(input).expect_err("ambiguous body framing");
    assert_eq!(error, ParseError::ConflictingBodyFraming);
    assert_eq!(error.status_code(), Some(400));
}

#[test]
fn rejects_duplicate_authorization_headers() {
    let input = b"GET /C63 HTTP/1.0\r\nAuthorization: Basic dXNlcjpwYXNz\r\nAuthorization: Basic dXNlcjpwYXNz\r\n\r\n";

    let error = parse_head(input).expect_err("duplicate authorization");
    assert_eq!(error, ParseError::DuplicateHeader("authorization"));
}

#[test]
fn rejects_oversized_request_line() {
    let mut input = vec![b'G'; 4097];
    input.extend_from_slice(b" /C63 HTTP/1.0\r\n\r\n");

    let error = parse_head(&input).expect_err("oversized request line");
    assert_eq!(error, ParseError::RequestLineTooLong);
    assert_eq!(error.status_code(), Some(431));
}

#[test]
fn rejects_unterminated_oversized_request_line_without_waiting() {
    let input = vec![b'G'; 4097];

    let error = parse_head(&input).expect_err("unterminated request line");
    assert_eq!(error, ParseError::RequestLineTooLong);
}

#[test]
fn rejects_unterminated_oversized_header_line_without_waiting() {
    let mut input = b"GET /C63 HTTP/1.0\r\nX-Long: ".to_vec();
    input.extend(std::iter::repeat_n(b'a', 4090));

    let error = parse_head(&input).expect_err("unterminated header line");
    assert_eq!(error, ParseError::HeaderLineTooLong);
}

#[test]
fn rejects_nested_mountpoint_target() {
    let input = b"GET /nested/C63 HTTP/1.0\r\n\r\n";

    let error = parse_head(input).expect_err("nested mountpoint");
    assert_eq!(error, ParseError::InvalidTarget);
}
