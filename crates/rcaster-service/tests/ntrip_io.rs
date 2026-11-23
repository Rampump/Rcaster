use std::time::Duration;

use rcaster_core::ntrip::{BodyFraming, Request};
use rcaster_service::ntrip_io::{ReadHeadError, read_request_head};
use tokio::{
    io::{AsyncWriteExt, duplex},
    net::{TcpListener, TcpStream},
    time::sleep,
};

#[tokio::test]
async fn reads_fragmented_tcp_head_and_preserves_body_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let client = tokio::spawn(async move {
        let mut stream = TcpStream::connect(address).await.expect("connect");
        stream
            .write_all(b"POST /C63 HTTP/1.1\r\nAuthorization: Basic ")
            .await
            .expect("first fragment");
        sleep(Duration::from_millis(20)).await;
        stream
            .write_all(b"b3duZXI6c2VjcmV0\r\n\r\nBODY")
            .await
            .expect("second fragment");
    });
    let (mut server, _) = listener.accept().await.expect("accept");
    let mut buffered = Vec::new();

    let head = read_request_head(&mut server, &mut buffered, Duration::from_secs(1))
        .await
        .expect("request head");

    let Request::SourceV2(request) = head.request else {
        panic!("expected NTRIP2 source");
    };
    assert_eq!(request.mountpoint, "C63");
    assert_eq!(request.body, BodyFraming::Stream);
    assert_eq!(request.credentials.username, "owner");
    assert_eq!(buffered, b"BODY");
    client.await.expect("client task");
}

#[tokio::test]
async fn times_out_an_incomplete_head() {
    let (mut server, _client) = duplex(1024);
    let mut buffered = Vec::new();

    let error = read_request_head(&mut server, &mut buffered, Duration::from_millis(20))
        .await
        .expect_err("head timeout");

    assert!(matches!(error, ReadHeadError::Timeout(_)));
}

#[tokio::test]
async fn rejects_oversized_unterminated_line_before_timeout() {
    let (mut server, mut client) = duplex(8192);
    let writer = tokio::spawn(async move {
        client
            .write_all(&vec![b'G'; 4097])
            .await
            .expect("oversized line");
    });
    let mut buffered = Vec::new();

    let error = read_request_head(&mut server, &mut buffered, Duration::from_secs(1))
        .await
        .expect_err("oversized line");

    assert!(matches!(
        error,
        ReadHeadError::Parse(rcaster_core::ntrip::ParseError::RequestLineTooLong)
    ));
    writer.await.expect("writer task");
}
