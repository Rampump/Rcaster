use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rcaster_service::etcd_reload::{EtcdTlsSource, ReloadableEtcdClient};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use tokio::{process::Child, time::sleep};

#[tokio::test]
#[ignore = "requires RCASTER_ETCD_BIN and local TLS-capable etcd"]
async fn rotates_etcd_client_credentials_without_restarting_the_client() {
    let etcd_bin = std::env::var("RCASTER_ETCD_BIN").expect("RCASTER_ETCD_BIN");
    let directory = std::env::temp_dir().join(unique("rcaster-etcd-mtls"));
    fs::create_dir_all(&directory).expect("create TLS directory");
    let files = Files::new(&directory);
    let pki = Pki::generate(&files);
    let port = available_port();
    let peer_port = available_port();

    let mut etcd = start_etcd(
        &etcd_bin,
        &directory,
        port,
        peer_port,
        &files.server_certificate,
        &files.server_key,
        &files.trust_both,
        "new",
    )
    .await;
    let mut connected = None;
    for _ in 0..100 {
        match ReloadableEtcdClient::connect(
            vec![format!("https://127.0.0.1:{port}")],
            Some(EtcdTlsSource {
                ca_certificate: files.ca.clone(),
                certificate: files.client_one_certificate.clone(),
                private_key: files.client_one_key.clone(),
                domain_name: "localhost".to_owned(),
            }),
            Duration::from_millis(50),
        )
        .await
        {
            Ok(client) => {
                connected = Some(client);
                break;
            }
            Err(_) => sleep(Duration::from_millis(100)).await,
        }
    }
    let reloadable = connected.expect("connect with first client certificate");

    fs::write(&files.client_one_certificate, &pki.client_two_certificate)
        .expect("replace client certificate");
    fs::write(&files.client_one_key, &pki.client_two_key).expect("replace client key");
    sleep(Duration::from_secs(1)).await;

    let _ = etcd.kill().await;
    let _ = etcd.wait().await;
    sleep(Duration::from_millis(250)).await;
    let mut restarted = start_etcd(
        &etcd_bin,
        &directory,
        port,
        peer_port,
        &files.server_certificate,
        &files.server_key,
        &files.trust_second,
        "existing",
    )
    .await;
    let mut client = reloadable.client();
    let mut last_error = None;
    for _ in 0..50 {
        match client.status().await {
            Ok(_) => {
                last_error = None;
                break;
            }
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
    if let Some(error) = last_error {
        panic!("rotated client did not reconnect: {error}");
    }
    client
        .put("reload-proof", "ok", None)
        .await
        .expect("write after credential rotation");

    let _ = restarted.kill().await;
    let _ = restarted.wait().await;
    let _ = fs::remove_dir_all(directory);
}

struct Files {
    ca: PathBuf,
    trust_both: PathBuf,
    trust_second: PathBuf,
    server_certificate: PathBuf,
    server_key: PathBuf,
    client_one_certificate: PathBuf,
    client_one_key: PathBuf,
}

impl Files {
    fn new(directory: &Path) -> Self {
        Self {
            ca: directory.join("ca.pem"),
            trust_both: directory.join("trust-both.pem"),
            trust_second: directory.join("trust-second.pem"),
            server_certificate: directory.join("server.pem"),
            server_key: directory.join("server-key.pem"),
            client_one_certificate: directory.join("client.pem"),
            client_one_key: directory.join("client-key.pem"),
        }
    }
}

struct Pki {
    client_two_certificate: String,
    client_two_key: String,
}

impl Pki {
    fn generate(files: &Files) -> Self {
        let (ca_one, ca_one_key) = certificate_authority("rcaster-ca-one");
        let (ca_two, ca_two_key) = certificate_authority("rcaster-ca-two");
        let (server_certificate, server_key) = signed_certificate(
            "localhost",
            vec!["localhost".to_owned(), "127.0.0.1".to_owned()],
            ExtendedKeyUsagePurpose::ServerAuth,
            &ca_one,
            &ca_one_key,
        );
        let (client_one_certificate, client_one_key) = signed_certificate(
            "rcaster-client-one",
            vec!["rcaster-client-one".to_owned()],
            ExtendedKeyUsagePurpose::ClientAuth,
            &ca_one,
            &ca_one_key,
        );
        let (client_two_certificate, client_two_key) = signed_certificate(
            "rcaster-client-two",
            vec!["rcaster-client-two".to_owned()],
            ExtendedKeyUsagePurpose::ClientAuth,
            &ca_two,
            &ca_two_key,
        );
        write(&files.ca, ca_one.pem());
        write(
            &files.trust_both,
            format!("{}{}", ca_one.pem(), ca_two.pem()),
        );
        write(&files.trust_second, ca_two.pem());
        write(&files.server_certificate, server_certificate);
        write_private_key(&files.server_key, &server_key);
        write(&files.client_one_certificate, client_one_certificate);
        write_private_key(&files.client_one_key, &client_one_key);
        Self {
            client_two_certificate,
            client_two_key,
        }
    }
}

fn certificate_authority(name: &str) -> (rcgen::Certificate, KeyPair) {
    let key = KeyPair::generate().expect("CA key");
    let mut params = CertificateParams::new(vec![name.to_owned()]).expect("CA params");
    params.distinguished_name.push(DnType::CommonName, name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let certificate = params.self_signed(&key).expect("CA certificate");
    (certificate, key)
}

fn signed_certificate(
    name: &str,
    names: Vec<String>,
    usage: ExtendedKeyUsagePurpose,
    ca: &rcgen::Certificate,
    ca_key: &KeyPair,
) -> (String, String) {
    let key = KeyPair::generate().expect("certificate key");
    let mut params = CertificateParams::new(names).expect("certificate params");
    params.distinguished_name.push(DnType::CommonName, name);
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![usage];
    let certificate = params
        .signed_by(&key, ca, ca_key)
        .expect("signed certificate");
    (certificate.pem(), key.serialize_pem())
}

#[allow(clippy::too_many_arguments)]
async fn start_etcd(
    etcd_bin: &str,
    directory: &Path,
    port: u16,
    peer_port: u16,
    certificate: &Path,
    private_key: &Path,
    trusted_ca: &Path,
    cluster_state: &str,
) -> Child {
    let client_url = format!("https://127.0.0.1:{port}");
    let peer_url = format!("http://127.0.0.1:{peer_port}");
    tokio::process::Command::new(etcd_bin)
        .arg("--name=rcaster-reload")
        .arg(format!(
            "--data-dir={}",
            directory.join("etcd-data").display()
        ))
        .arg(format!("--listen-client-urls={client_url}"))
        .arg(format!("--advertise-client-urls={client_url}"))
        .arg(format!("--listen-peer-urls={peer_url}"))
        .arg(format!("--initial-advertise-peer-urls={peer_url}"))
        .arg(format!("--initial-cluster=rcaster-reload={peer_url}"))
        .arg(format!("--initial-cluster-state={cluster_state}"))
        .arg("--initial-cluster-token=rcaster-reload")
        .arg(format!("--cert-file={}", certificate.display()))
        .arg(format!("--key-file={}", private_key.display()))
        .arg(format!("--trusted-ca-file={}", trusted_ca.display()))
        .arg("--client-cert-auth")
        .arg("--log-level=error")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start etcd")
}

fn available_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind temporary port")
        .local_addr()
        .expect("temporary address")
        .port()
}

fn write(path: &Path, value: impl AsRef<[u8]>) {
    fs::write(path, value).expect("write TLS fixture");
}

fn write_private_key(path: &Path, value: &str) {
    write(path, value);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("secure private key");
    }
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    )
}
