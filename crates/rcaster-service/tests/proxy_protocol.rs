use std::{
    net::{IpAddr, Ipv4Addr},
    time::Duration,
};

use rcaster_service::proxy_protocol::{ProxyProtocolConfig, ProxyProtocolError, resolve_client_ip};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

#[tokio::test]
async fn partial_proxy_header_is_bounded_by_the_probe_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("address");
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        stream
            .write_all(b"PROXY TCP4 192.0.2.1 ")
            .await
            .expect("partial header");
        std::future::pending::<()>().await;
    });
    let mut stream = TcpStream::connect(address).await.expect("connect");
    let config = ProxyProtocolConfig::new(&["127.0.0.1/32".to_owned()]).expect("proxy config");

    let result = tokio::time::timeout(
        Duration::from_secs(6),
        resolve_client_ip(&mut stream, IpAddr::V4(Ipv4Addr::LOCALHOST), Some(&config)),
    )
    .await
    .expect("PROXY header timeout");

    assert!(matches!(result, Err(ProxyProtocolError::Timeout)));
    server.abort();
}
