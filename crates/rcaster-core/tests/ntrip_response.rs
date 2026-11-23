use rcaster_core::ntrip::response::{Ntrip1Error, Response, ResponseError, encode_ntrip2_error};

#[test]
fn encodes_ntrip1_success_as_icy() {
    let encoded = Response::Ntrip1Data.encode().expect("NTRIP1 data response");

    assert_eq!(encoded, b"ICY 200 OK\r\n\r\n");
}

#[test]
fn encodes_ntrip1_fixed_error_text() {
    let encoded = Response::Ntrip1Error(Ntrip1Error::BadPassword)
        .encode()
        .expect("NTRIP1 error");

    assert_eq!(encoded, b"ERROR - Bad Password\r\n");
}

#[test]
fn encodes_ntrip1_sourcetable_with_byte_length() {
    let body = "CAS;CHN;RCaster\r\nENDSOURCETABLE\r\n";
    let encoded = Response::Ntrip1Sourcetable { body }
        .encode()
        .expect("NTRIP1 sourcetable");
    let encoded = String::from_utf8(encoded).expect("ASCII response");

    assert_eq!(
        encoded,
        format!(
            "SOURCETABLE 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    );
}

#[test]
fn encodes_ntrip2_data_headers() {
    let encoded = Response::Ntrip2Data {
        server_version: "0.1.0",
        date: "Sat, 22 Nov 2026 10:00:00 GMT",
    }
    .encode()
    .expect("NTRIP2 data response");

    assert_eq!(
        encoded,
        b"HTTP/1.1 200 OK\r\nServer: NTRIP RCaster/0.1.0\r\nDate: Sat, 22 Nov 2026 10:00:00 GMT\r\nNtrip-Version: Ntrip/2.0\r\nContent-Type: gnss/data\r\nCache-Control: no-store, no-cache, max-age=0\r\nPragma: no-cache\r\nConnection: close\r\n\r\n"
    );
}

#[test]
fn encodes_ntrip2_authentication_challenge() {
    let encoded = encode_ntrip2_error(401, None).expect("401 response");

    assert_eq!(
        encoded,
        b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"NTRIP\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
}

#[test]
fn encodes_ntrip2_service_unavailable_with_retry_after() {
    let encoded = encode_ntrip2_error(503, Some(5)).expect("503 response");

    assert_eq!(
        encoded,
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nRetry-After: 5\r\nConnection: close\r\n\r\n"
    );
}

#[test]
fn rejects_header_injection_in_dynamic_values() {
    let error = Response::Ntrip2Data {
        server_version: "0.1.0\r\nX-Injected: yes",
        date: "Sat, 22 Nov 2026 10:00:00 GMT",
    }
    .encode()
    .expect_err("header injection");

    assert_eq!(error, ResponseError::InvalidHeaderValue);
}

#[test]
fn rejects_other_control_bytes_in_dynamic_headers() {
    let error = Response::Ntrip2Data {
        server_version: "0.1.0\t",
        date: "Sat, 22 Nov 2026 10:00:00 GMT",
    }
    .encode()
    .expect_err("control byte");

    assert_eq!(error, ResponseError::InvalidHeaderValue);
}
