use std::{
    io::{self, Read},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU8, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use der::{Decode, Encode};
use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{CertificateDer, CertificateRevocationListDer, PrivateKeyDer, pem::PemObject},
    server::WebPkiClientVerifier,
};
use sha1::{Digest, Sha1};
use tokio::{sync::oneshot, task::JoinHandle};
use x509_ocsp::{BasicOcspResponse, CertStatus, OcspResponse, OcspResponseStatus, ResponderId};

const MAX_OCSP_RESPONSE_BYTES: usize = 64 * 1024;
const OCSP_BASIC_RESPONSE_OID: &[u8] = &[0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x30, 0x01, 0x01];
const SHA1_OID: &[u8] = &[0x2b, 0x0e, 0x03, 0x02, 0x1a];

fn grpc_alpn() -> Vec<Vec<u8>> {
    vec![b"h2".to_vec()]
}

#[derive(Clone)]
pub struct ReloadableTlsConfig {
    inner: Arc<Inner>,
}

#[derive(Clone, Debug)]
pub struct OcspResponderConfig {
    pub url: String,
    pub refresh_interval: Duration,
}

struct Inner {
    current: RwLock<Arc<ServerConfig>>,
    expires_at: RwLock<Option<i64>>,
    expiry_level: AtomicU8,
    source: Option<TlsSource>,
    stop: Mutex<Option<oneshot::Sender<()>>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone)]
struct TlsSource {
    certificate: PathBuf,
    private_key: PathBuf,
    client_ca: Option<PathBuf>,
    client_crl: Option<PathBuf>,
    ocsp_response: Option<PathBuf>,
    ocsp_responder: Option<OcspResponderSource>,
    alpn_protocols: Vec<Vec<u8>>,
    interval: Duration,
}

#[derive(Clone)]
struct OcspResponderSource {
    config: OcspResponderConfig,
    cache: Arc<Mutex<OcspCache>>,
}

#[derive(Default)]
struct OcspCache {
    response: Option<Vec<u8>>,
    fetched_at: Option<Instant>,
}

impl ReloadableTlsConfig {
    pub fn from_pem(certificate_pem: &str, private_key_pem: &str) -> io::Result<Self> {
        Ok(Self::static_config(build_server_config(
            certificate_pem,
            private_key_pem,
            None,
            None,
            None,
            &[],
        )?))
    }

    pub fn from_pem_mtls(
        certificate_pem: &str,
        private_key_pem: &str,
        client_ca_pem: &str,
    ) -> io::Result<Self> {
        Self::from_pem_mtls_with_client_crl(certificate_pem, private_key_pem, client_ca_pem, None)
    }

    pub fn from_pem_mtls_with_client_crl(
        certificate_pem: &str,
        private_key_pem: &str,
        client_ca_pem: &str,
        client_crl_pem: Option<&str>,
    ) -> io::Result<Self> {
        Ok(Self::static_config(build_server_config(
            certificate_pem,
            private_key_pem,
            Some(client_ca_pem),
            client_crl_pem,
            None,
            &[],
        )?))
    }

    pub fn from_pem_mtls_with_client_crl_and_ocsp(
        certificate_pem: &str,
        private_key_pem: &str,
        client_ca_pem: &str,
        client_crl_pem: Option<&str>,
        ocsp_response: Option<&[u8]>,
    ) -> io::Result<Self> {
        Ok(Self::static_config(build_server_config(
            certificate_pem,
            private_key_pem,
            Some(client_ca_pem),
            client_crl_pem,
            ocsp_response,
            &[],
        )?))
    }

    pub fn from_files(
        certificate: PathBuf,
        private_key: PathBuf,
        client_ca: Option<PathBuf>,
        interval: Duration,
    ) -> io::Result<Self> {
        Self::from_files_with_client_crl(certificate, private_key, client_ca, None, interval)
    }

    pub fn from_files_with_client_crl(
        certificate: PathBuf,
        private_key: PathBuf,
        client_ca: Option<PathBuf>,
        client_crl: Option<PathBuf>,
        interval: Duration,
    ) -> io::Result<Self> {
        Self::from_files_with_options(
            certificate,
            private_key,
            client_ca,
            client_crl,
            None,
            None,
            Vec::new(),
            interval,
        )
    }

    pub fn from_files_with_ocsp(
        certificate: PathBuf,
        private_key: PathBuf,
        client_ca: Option<PathBuf>,
        ocsp_response: PathBuf,
        interval: Duration,
    ) -> io::Result<Self> {
        Self::from_files_with_options(
            certificate,
            private_key,
            client_ca,
            None,
            Some(ocsp_response),
            None,
            Vec::new(),
            interval,
        )
    }

    pub fn from_files_with_client_crl_and_ocsp(
        certificate: PathBuf,
        private_key: PathBuf,
        client_ca: Option<PathBuf>,
        client_crl: Option<PathBuf>,
        ocsp_response: PathBuf,
        interval: Duration,
    ) -> io::Result<Self> {
        Self::from_files_with_options(
            certificate,
            private_key,
            client_ca,
            client_crl,
            Some(ocsp_response),
            None,
            Vec::new(),
            interval,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_files_with_ocsp_responder(
        certificate: PathBuf,
        private_key: PathBuf,
        client_ca: Option<PathBuf>,
        client_crl: Option<PathBuf>,
        ocsp_response: Option<PathBuf>,
        ocsp_responder: OcspResponderConfig,
        interval: Duration,
    ) -> io::Result<Self> {
        Self::from_files_with_options(
            certificate,
            private_key,
            client_ca,
            client_crl,
            ocsp_response,
            Some(ocsp_responder),
            Vec::new(),
            interval,
        )
    }

    pub fn from_files_with_client_crl_grpc(
        certificate: PathBuf,
        private_key: PathBuf,
        client_ca: Option<PathBuf>,
        client_crl: Option<PathBuf>,
        interval: Duration,
    ) -> io::Result<Self> {
        Self::from_files_with_options(
            certificate,
            private_key,
            client_ca,
            client_crl,
            None,
            None,
            grpc_alpn(),
            interval,
        )
    }

    pub fn from_files_with_client_crl_and_ocsp_grpc(
        certificate: PathBuf,
        private_key: PathBuf,
        client_ca: Option<PathBuf>,
        client_crl: Option<PathBuf>,
        ocsp_response: PathBuf,
        interval: Duration,
    ) -> io::Result<Self> {
        Self::from_files_with_options(
            certificate,
            private_key,
            client_ca,
            client_crl,
            Some(ocsp_response),
            None,
            grpc_alpn(),
            interval,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_files_with_ocsp_responder_grpc(
        certificate: PathBuf,
        private_key: PathBuf,
        client_ca: Option<PathBuf>,
        client_crl: Option<PathBuf>,
        ocsp_response: Option<PathBuf>,
        ocsp_responder: OcspResponderConfig,
        interval: Duration,
    ) -> io::Result<Self> {
        Self::from_files_with_options(
            certificate,
            private_key,
            client_ca,
            client_crl,
            ocsp_response,
            Some(ocsp_responder),
            grpc_alpn(),
            interval,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_files_with_options(
        certificate: PathBuf,
        private_key: PathBuf,
        client_ca: Option<PathBuf>,
        client_crl: Option<PathBuf>,
        ocsp_response: Option<PathBuf>,
        ocsp_responder: Option<OcspResponderConfig>,
        alpn_protocols: Vec<Vec<u8>>,
        interval: Duration,
    ) -> io::Result<Self> {
        if interval.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TLS reload interval must be positive",
            ));
        }
        if client_crl.is_some() && client_ca.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "client CRL requires a client CA",
            ));
        }
        if ocsp_responder
            .as_ref()
            .is_some_and(|responder| responder.refresh_interval.is_zero())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "OCSP refresh interval must be positive",
            ));
        }
        let tick = ocsp_responder.as_ref().map_or(interval, |responder| {
            interval.min(responder.refresh_interval)
        });
        let source = TlsSource {
            certificate,
            private_key,
            client_ca,
            client_crl,
            ocsp_response,
            ocsp_responder: ocsp_responder.map(|config| OcspResponderSource {
                config,
                cache: Arc::new(Mutex::new(OcspCache::default())),
            }),
            alpn_protocols,
            interval: tick,
        };
        let loaded = load_config(&source)?;
        let inner = Arc::new(Inner {
            current: RwLock::new(loaded.config),
            expires_at: RwLock::new(loaded.expires_at),
            expiry_level: AtomicU8::new(0),
            source: Some(source.clone()),
            stop: Mutex::new(None),
            task: Mutex::new(None),
        });
        update_expiry_level(&inner, loaded.expires_at);
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let weak = Arc::downgrade(&inner);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stop_rx => return,
                    _ = tokio::time::sleep(source.interval) => {}
                }
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let reload_inner_config = Arc::clone(&inner);
                let reload_source = source.clone();
                match tokio::task::spawn_blocking(move || {
                    reload_inner(&reload_inner_config, &reload_source)
                })
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        eprintln!(
                            "rcaster: TLS reload failed, keeping existing certificate: {error}"
                        );
                    }
                    Err(error) => {
                        eprintln!("rcaster: TLS reload task failed: {error}");
                    }
                }
            }
        });
        *inner
            .stop
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(stop_tx);
        *inner
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(task);
        Ok(Self { inner })
    }

    pub fn current(&self) -> Arc<ServerConfig> {
        Arc::clone(
            &self
                .inner
                .current
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    pub fn certificate_expiry(&self) -> Option<i64> {
        *self
            .inner
            .expires_at
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn reload_now(&self) -> io::Result<bool> {
        let Some(source) = self.inner.source.as_ref() else {
            return Ok(false);
        };
        reload_inner(&self.inner, source)?;
        Ok(true)
    }

    fn static_config(loaded: LoadedTls) -> Self {
        let inner = Inner {
            current: RwLock::new(loaded.config),
            expires_at: RwLock::new(loaded.expires_at),
            expiry_level: AtomicU8::new(0),
            source: None,
            stop: Mutex::new(None),
            task: Mutex::new(None),
        };
        update_expiry_level(&inner, loaded.expires_at);
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(stop) = self
            .stop
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = stop.send(());
        }
        if let Some(task) = self
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
    }
}

fn reload_inner(inner: &Inner, source: &TlsSource) -> io::Result<()> {
    let next = load_config(source)?;
    *inner
        .current
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = next.config;
    *inner
        .expires_at
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = next.expires_at;
    update_expiry_level(inner, next.expires_at);
    Ok(())
}

struct LoadedTls {
    config: Arc<ServerConfig>,
    expires_at: Option<i64>,
}

fn update_expiry_level(inner: &Inner, expires_at: Option<i64>) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let remaining = expires_at.map(|expiry| expiry.saturating_sub(now));
    let level = match remaining {
        None => {
            inner.expiry_level.store(0, Ordering::Release);
            return;
        }
        Some(seconds) if seconds <= 0 => 5,
        Some(seconds) if seconds <= 7 * 86_400 => 4,
        Some(seconds) if seconds <= 14 * 86_400 => 3,
        Some(seconds) if seconds <= 30 * 86_400 => 2,
        Some(_) => 1,
    };
    let previous = inner.expiry_level.swap(level, Ordering::AcqRel);
    if previous == level {
        return;
    }
    match level {
        2 => eprintln!(
            "rcaster: TLS certificate WARNING: expires in {} days",
            remaining.unwrap_or_default() / 86_400
        ),
        3 => eprintln!(
            "rcaster: TLS certificate CRITICAL: expires in {} days",
            remaining.unwrap_or_default() / 86_400
        ),
        4 => eprintln!(
            "rcaster: TLS certificate CRITICAL: expires in {} days",
            remaining.unwrap_or_default() / 86_400
        ),
        5 => eprintln!("rcaster: TLS certificate CRITICAL: certificate has expired"),
        _ => {}
    }
}

fn load_config(source: &TlsSource) -> io::Result<LoadedTls> {
    validate_private_key_permissions(&source.private_key)?;
    let certificate_pem = std::fs::read_to_string(&source.certificate)?;
    let private_key_pem = std::fs::read_to_string(&source.private_key)?;
    let client_ca_pem = source
        .client_ca
        .as_ref()
        .map(std::fs::read_to_string)
        .transpose()?;
    let client_crl_pem = source
        .client_crl
        .as_ref()
        .map(std::fs::read_to_string)
        .transpose()?;
    let ocsp_response = load_ocsp_response(source, &certificate_pem)?;
    if ocsp_response.as_ref().is_some_and(Vec::is_empty) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "OCSP response file is empty",
        ));
    }
    build_server_config(
        &certificate_pem,
        &private_key_pem,
        client_ca_pem.as_deref(),
        client_crl_pem.as_deref(),
        ocsp_response.as_deref(),
        &source.alpn_protocols,
    )
}

fn load_ocsp_response(source: &TlsSource, certificate_pem: &str) -> io::Result<Option<Vec<u8>>> {
    let mut refresh_error = None;
    if let Some(responder) = &source.ocsp_responder {
        if let Err(error) = responder.refresh_if_due(certificate_pem) {
            refresh_error = Some(error);
        }
        if let Some(response) = responder.cached() {
            return Ok(Some(response));
        }
    }

    if let Some(path) = &source.ocsp_response {
        return std::fs::read(path).map(Some);
    }
    match refresh_error {
        Some(error) => Err(error),
        None => Ok(None),
    }
}

impl OcspResponderSource {
    fn cached(&self) -> Option<Vec<u8>> {
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .response
            .clone()
    }

    fn refresh_if_due(&self, certificate_pem: &str) -> io::Result<()> {
        let due = {
            let cache = self
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            cache.response.is_none()
                || cache
                    .fetched_at
                    .is_none_or(|fetched_at| fetched_at.elapsed() >= self.config.refresh_interval)
        };
        if !due {
            return Ok(());
        }
        let response = fetch_ocsp_response(&self.config.url, certificate_pem)?;
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.response = Some(response);
        cache.fetched_at = Some(Instant::now());
        Ok(())
    }
}

fn fetch_ocsp_response(url: &str, certificate_pem: &str) -> io::Result<Vec<u8>> {
    let request = ocsp_request(certificate_pem)?;
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(5))
        .build();
    let response = agent
        .post(url)
        .set("Content-Type", "application/ocsp-request")
        .send_bytes(&request)
        .map_err(|error| io::Error::other(format!("OCSP request failed: {error}")))?;
    let mut response_bytes = Vec::new();
    response
        .into_reader()
        .take((MAX_OCSP_RESPONSE_BYTES + 1) as u64)
        .read_to_end(&mut response_bytes)?;
    if response_bytes.len() > MAX_OCSP_RESPONSE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "OCSP response exceeds size limit",
        ));
    }
    validate_ocsp_response(&response_bytes, certificate_pem)?;
    Ok(response_bytes)
}

fn ocsp_request(certificate_pem: &str) -> io::Result<Vec<u8>> {
    let certificates = CertificateDer::pem_slice_iter(certificate_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let [leaf, issuer, ..] = certificates.as_slice() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "OCSP responder requires a leaf and issuer certificate",
        ));
    };
    let (_, leaf) = x509_parser::parse_x509_certificate(leaf)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let (_, issuer) = x509_parser::parse_x509_certificate(issuer)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

    let sha1_algorithm = der(
        0x30,
        &[0x06, 0x05, 0x2b, 0x0e, 0x03, 0x02, 0x1a, 0x05, 0x00],
    );
    let issuer_name_hash = Sha1::digest(issuer.tbs_certificate.subject().as_raw());
    let issuer_key_hash = Sha1::digest(
        issuer
            .tbs_certificate
            .public_key()
            .subject_public_key
            .data
            .as_ref(),
    );

    let mut cert_id = sha1_algorithm;
    cert_id.extend(der(0x04, &issuer_name_hash));
    cert_id.extend(der(0x04, &issuer_key_hash));
    cert_id.extend(der(0x02, leaf.tbs_certificate.raw_serial()));
    let request = der(0x30, &der(0x30, &cert_id));
    Ok(der(0x30, &der(0x30, &request)))
}

pub(crate) fn validate_ocsp_response(response: &[u8], certificate_pem: &str) -> io::Result<()> {
    let certificates = CertificateDer::pem_slice_iter(certificate_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let [leaf_der, issuer_der, ..] = certificates.as_slice() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "OCSP validation requires a leaf and issuer certificate",
        ));
    };
    let (_, leaf) = x509_parser::parse_x509_certificate(leaf_der)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let (_, issuer) = x509_parser::parse_x509_certificate(issuer_der)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

    let ocsp = OcspResponse::from_der(response)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if ocsp.response_status != OcspResponseStatus::Successful {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "OCSP responder did not return a successful response",
        ));
    }
    let response_bytes = ocsp.response_bytes.as_ref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "OCSP response is missing response bytes",
        )
    })?;
    if response_bytes.response_type.as_bytes() != OCSP_BASIC_RESPONSE_OID {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "OCSP response is not a basic response",
        ));
    }
    let basic = BasicOcspResponse::from_der(response_bytes.response.as_bytes())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
        .as_secs() as i64;
    let produced_at = basic
        .tbs_response_data
        .produced_at
        .0
        .to_unix_duration()
        .as_secs() as i64;
    if produced_at > now {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "OCSP response was produced in the future",
        ));
    }

    let issuer_name_hash = Sha1::digest(issuer.subject().as_raw());
    let issuer_key_hash = Sha1::digest(issuer.public_key().subject_public_key.data.as_ref());
    let leaf_serial = trim_leading_zeroes(leaf.tbs_certificate.raw_serial());
    let mut matched = false;
    for single in &basic.tbs_response_data.responses {
        let cert_id = &single.cert_id;
        if cert_id.hash_algorithm.oid.as_bytes() != SHA1_OID
            || cert_id.issuer_name_hash.as_bytes() != issuer_name_hash.as_slice()
            || cert_id.issuer_key_hash.as_bytes() != issuer_key_hash.as_slice()
            || trim_leading_zeroes(cert_id.serial_number.as_bytes()) != leaf_serial
        {
            continue;
        }
        matched = true;
        if !matches!(single.cert_status, CertStatus::Good(_)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "OCSP response does not report the certificate as good",
            ));
        }
        let this_update = single.this_update.0.to_unix_duration().as_secs() as i64;
        if this_update > now {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "OCSP thisUpdate is in the future",
            ));
        }
        if single
            .next_update
            .is_some_and(|next_update| (next_update.0.to_unix_duration().as_secs() as i64) < now)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "OCSP nextUpdate has expired",
            ));
        }
    }
    if !matched {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "OCSP response does not match the server certificate",
        ));
    }

    let tbs = basic
        .tbs_response_data
        .to_der()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let signature_algorithm_der = basic
        .signature_algorithm
        .to_der()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let (_, signature_algorithm): (_, x509_parser::prelude::AlgorithmIdentifier<'_>) =
        x509_parser::prelude::FromDer::from_der(&signature_algorithm_der)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let signature_bytes = basic.signature.as_bytes().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "OCSP signature is not octet-aligned",
        )
    })?;
    let signature = asn1_rs::BitString::new(0, signature_bytes);
    let responder_matches =
        |certificate: &x509_parser::prelude::X509Certificate<'_>| -> io::Result<bool> {
            match &basic.tbs_response_data.responder_id {
                ResponderId::ByName(name) => {
                    let name = name
                        .to_der()
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                    Ok(certificate.subject().as_raw() == name)
                }
                ResponderId::ByKey(key) => Ok(key.as_bytes()
                    == sha1::Sha1::digest(
                        certificate.public_key().subject_public_key.data.as_ref(),
                    )
                    .as_slice()),
            }
        };
    let signature_matches = |certificate: &x509_parser::prelude::X509Certificate<'_>| {
        x509_parser::verify::verify_signature(
            certificate.public_key(),
            &signature_algorithm,
            &signature,
            &tbs,
        )
        .is_ok()
    };

    let mut verified = responder_matches(&issuer)? && signature_matches(&issuer);
    if !verified && let Some(certs) = &basic.certs {
        for certificate in certs {
            let der = certificate
                .to_der()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            let (_, delegated) = x509_parser::parse_x509_certificate(&der)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            if delegated.validity().not_before.timestamp() > now
                || delegated.validity().not_after.timestamp() < now
                || delegated
                    .verify_signature(Some(issuer.public_key()))
                    .is_err()
                || !delegated
                    .extended_key_usage()
                    .ok()
                    .flatten()
                    .is_some_and(|usage| usage.value.ocsp_signing)
            {
                continue;
            }
            if responder_matches(&delegated)? && signature_matches(&delegated) {
                verified = true;
                break;
            }
        }
    }
    if !verified {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "OCSP response signature is not valid for the certificate issuer",
        ));
    }
    Ok(())
}

fn trim_leading_zeroes(bytes: &[u8]) -> &[u8] {
    let first = bytes
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(bytes.len());
    &bytes[first..]
}

fn der(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut output = vec![tag];
    der_length(content.len(), &mut output);
    output.extend_from_slice(content);
    output
}

fn der_length(length: usize, output: &mut Vec<u8>) {
    if length < 128 {
        output.push(length as u8);
        return;
    }
    let bytes = length.to_be_bytes();
    let first = bytes
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(bytes.len() - 1);
    let significant = &bytes[first..];
    output.push(0x80 | significant.len() as u8);
    output.extend_from_slice(significant);
}

pub fn validate_private_key_permissions(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = std::fs::metadata(path)?.permissions().mode();
        if !private_key_mode_is_safe(mode) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "private key file permissions allow group or other access",
            ));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[cfg(any(unix, test))]
fn private_key_mode_is_safe(mode: u32) -> bool {
    mode & 0o077 == 0
}

fn build_server_config(
    certificate_pem: &str,
    private_key_pem: &str,
    client_ca_pem: Option<&str>,
    client_crl_pem: Option<&str>,
    ocsp_response: Option<&[u8]>,
    alpn_protocols: &[Vec<u8>],
) -> io::Result<LoadedTls> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certificates = CertificateDer::pem_slice_iter(certificate_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if certificates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "certificate PEM contains no certificates",
        ));
    }
    let expires_at = certificates.first().and_then(|certificate| {
        match x509_parser::parse_x509_certificate(certificate) {
            Ok((_, certificate)) => Some(certificate.validity().not_after.timestamp()),
            Err(error) => {
                eprintln!("rcaster: TLS certificate expiry cannot be parsed: {error}");
                None
            }
        }
    });
    let private_key = PrivateKeyDer::from_pem_slice(private_key_pem.as_bytes())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let builder = ServerConfig::builder();
    let builder = if let Some(client_ca_pem) = client_ca_pem {
        let ca = CertificateDer::pem_slice_iter(client_ca_pem.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let mut roots = RootCertStore::empty();
        roots.add_parsable_certificates(ca);
        let mut verifier = WebPkiClientVerifier::builder(Arc::new(roots));
        if let Some(client_crl_pem) = client_crl_pem {
            let crls = CertificateRevocationListDer::pem_slice_iter(client_crl_pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            if crls.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "client CRL PEM contains no CRLs",
                ));
            }
            verifier = verifier.with_crls(crls);
        }
        let verifier = verifier
            .build()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    let mut config = if let Some(ocsp_response) = ocsp_response {
        validate_ocsp_response(ocsp_response, certificate_pem)?;
        builder.with_single_cert_with_ocsp(certificates, private_key, ocsp_response.to_vec())
    } else {
        builder.with_single_cert(certificates, private_key)
    }
    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    config.alpn_protocols = alpn_protocols.to_vec();
    Ok(LoadedTls {
        config: Arc::new(config),
        expires_at,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs, io,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, SystemTime},
    };

    use der::{
        Decode, Encode,
        asn1::{BitString, Null, ObjectIdentifier, OctetString},
    };
    use rcgen::{
        BasicConstraints, Certificate, CertificateParams, CertificateRevocationListParams, DnType,
        ExtendedKeyUsagePurpose, IsCa, KeyIdMethod, KeyPair, KeyUsagePurpose, RevocationReason,
        RevokedCertParams, SerialNumber, date_time_ymd,
    };
    use rustls::{
        ClientConfig, RootCertStore,
        pki_types::{PrivateKeyDer, ServerName, pem::PemObject},
    };
    use sha1::{Digest, Sha1};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_rustls::{TlsAcceptor, TlsConnector};
    use x509_cert::{
        AlgorithmIdentifier, Certificate as X509Certificate, der::asn1::GeneralizedTime,
    };
    use x509_ocsp::{
        BasicOcspResponse, CertId, CertStatus, OcspGeneralizedTime, OcspResponse, ResponderId,
        ResponseData, RevokedInfo, SingleResponse,
    };

    use super::ReloadableTlsConfig;

    const CLIENT_SERIAL: u64 = 42;

    #[test]
    fn ocsp_request_hashes_issuer_subject_name() {
        let (root_certificate, root_key) = certificate_authority();
        let intermediate_key = KeyPair::generate().unwrap();
        let mut intermediate_params = CertificateParams::new(Vec::new()).unwrap();
        intermediate_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        intermediate_params
            .distinguished_name
            .push(DnType::CommonName, "rcaster-test-intermediate");
        intermediate_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let intermediate_certificate = intermediate_params
            .signed_by(&intermediate_key, &root_certificate, &root_key)
            .unwrap();
        let (leaf_certificate, _) =
            signed_certificate("localhost", &intermediate_certificate, &intermediate_key, 1);
        let request = super::ocsp_request(&format!(
            "{}{}{}",
            leaf_certificate.pem(),
            intermediate_certificate.pem(),
            root_certificate.pem()
        ))
        .unwrap();
        let (_, intermediate) =
            x509_parser::parse_x509_certificate(intermediate_certificate.der()).unwrap();
        let (_, root) = x509_parser::parse_x509_certificate(root_certificate.der()).unwrap();
        let expected = sha1::Sha1::digest(intermediate.subject().as_raw());
        let wrong = sha1::Sha1::digest(root.subject().as_raw());
        assert!(
            request
                .windows(expected.len() + 2)
                .any(|window| window[..2] == [0x04, 0x14] && window[2..] == expected[..])
        );
        assert!(
            !request
                .windows(wrong.len() + 2)
                .any(|window| window[..2] == [0x04, 0x14] && window[2..] == wrong[..])
        );
    }

    #[tokio::test]
    async fn invalid_reload_keeps_previous_configuration() {
        let directory =
            std::env::temp_dir().join(format!("rcaster-tls-reload-{}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let certificate_path = directory.join("server.crt");
        let key_path = directory.join("server.key");
        let (certificate, key) = certificate_pair("first.example");
        fs::write(&certificate_path, certificate).unwrap();
        write_private_key(&key_path, &key);
        let config = ReloadableTlsConfig::from_files(
            PathBuf::from(&certificate_path),
            PathBuf::from(&key_path),
            None,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let first = config.current();
        assert!(config.certificate_expiry().is_some());
        fs::write(&certificate_path, "not a certificate").unwrap();
        assert!(config.reload_now().is_err());
        assert!(Arc::ptr_eq(&first, &config.current()));
        let _ = fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn new_files_replace_the_acceptor_for_new_connections() {
        let directory =
            std::env::temp_dir().join(format!("rcaster-tls-reload-success-{}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let certificate_path = directory.join("server.crt");
        let key_path = directory.join("server.key");
        let (certificate, key) = certificate_pair("first.example");
        fs::write(&certificate_path, certificate).unwrap();
        write_private_key(&key_path, &key);
        let config = ReloadableTlsConfig::from_files(
            PathBuf::from(&certificate_path),
            PathBuf::from(&key_path),
            None,
            std::time::Duration::from_millis(20),
        )
        .unwrap();
        let first = config.current();
        let (certificate, key) = certificate_pair("second.example");
        fs::write(&certificate_path, certificate).unwrap();
        write_private_key(&key_path, &key);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if !Arc::ptr_eq(&first, &config.current()) {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("automatic TLS reload");
        let _ = fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn grpc_reloadable_config_advertises_h2() {
        let directory =
            std::env::temp_dir().join(format!("rcaster-tls-grpc-alpn-{}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let certificate_path = directory.join("server.crt");
        let key_path = directory.join("server.key");
        let (certificate, key) = certificate_pair("localhost");
        fs::write(&certificate_path, certificate).unwrap();
        write_private_key(&key_path, &key);
        let config = ReloadableTlsConfig::from_files_with_client_crl_grpc(
            certificate_path.clone(),
            key_path.clone(),
            None,
            None,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        assert_eq!(config.current().alpn_protocols, vec![b"h2".to_vec()]);
        let (certificate, key) = certificate_pair("reloaded.example");
        fs::write(&certificate_path, certificate).unwrap();
        write_private_key(&key_path, &key);
        config.reload_now().unwrap();
        assert_eq!(config.current().alpn_protocols, vec![b"h2".to_vec()]);
        let _ = fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn ocsp_reload_rejects_revoked_status_and_keeps_previous_configuration() {
        let directory =
            std::env::temp_dir().join(format!("rcaster-tls-ocsp-reload-{}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let certificate_path = directory.join("server.crt");
        let key_path = directory.join("server.key");
        let ocsp_path = directory.join("server.ocsp");
        let (ca_certificate, ca_key) = certificate_authority();
        let (server_certificate, server_key) =
            signed_certificate("localhost", &ca_certificate, &ca_key, 1);
        fs::write(
            &certificate_path,
            format!("{}{}", server_certificate.pem(), ca_certificate.pem()),
        )
        .unwrap();
        write_private_key(&key_path, &server_key.serialize_pem());

        let now = SystemTime::now();
        fs::write(
            &ocsp_path,
            ocsp_response(
                &ca_certificate,
                &server_certificate,
                &ca_certificate,
                &ca_key,
                CertStatus::good(),
                now - Duration::from_secs(60),
                Some(now + Duration::from_secs(3_600)),
                &[],
            ),
        )
        .unwrap();
        let config = ReloadableTlsConfig::from_files_with_ocsp(
            certificate_path,
            key_path,
            None,
            ocsp_path.clone(),
            Duration::from_secs(60),
        )
        .unwrap();
        let first = config.current();

        fs::write(
            &ocsp_path,
            ocsp_response(
                &ca_certificate,
                &server_certificate,
                &ca_certificate,
                &ca_key,
                CertStatus::revoked(RevokedInfo {
                    revocation_time: ocsp_time(now - Duration::from_secs(30)),
                    revocation_reason: None,
                }),
                now - Duration::from_secs(60),
                Some(now + Duration::from_secs(3_600)),
                &[],
            ),
        )
        .unwrap();
        assert!(config.reload_now().is_err());
        assert!(Arc::ptr_eq(&first, &config.current()));
        let _ = fs::remove_dir_all(directory);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ocsp_responder_refreshes_without_blocking_tokio_worker() {
        let directory =
            std::env::temp_dir().join(format!("rcaster-tls-ocsp-responder-{}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let certificate_path = directory.join("server.crt");
        let key_path = directory.join("server.key");
        let (ca_certificate, ca_key) = certificate_authority();
        let (server_certificate, server_key) =
            signed_certificate("localhost", &ca_certificate, &ca_key, 1);
        fs::write(
            &certificate_path,
            format!("{}{}", server_certificate.pem(), ca_certificate.pem()),
        )
        .unwrap();
        write_private_key(&key_path, &server_key.serialize_pem());
        let now = SystemTime::now();
        let ocsp_response = ocsp_response(
            &ca_certificate,
            &server_certificate,
            &ca_certificate,
            &ca_key,
            CertStatus::good(),
            now - Duration::from_secs(60),
            Some(now + Duration::from_secs(3_600)),
            &[],
        );

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let heartbeat = Arc::new(AtomicUsize::new(0));
        let heartbeat_during_request = Arc::new(AtomicUsize::new(0));
        let server_requests = Arc::clone(&requests);
        let server_heartbeat = Arc::clone(&heartbeat);
        let server_heartbeat_during_request = Arc::clone(&heartbeat_during_request);
        std::thread::spawn(move || {
            for index in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                server_requests.fetch_add(1, Ordering::SeqCst);
                let mut request = Vec::new();
                let mut chunk = [0_u8; 4096];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = std::io::Read::read(&mut stream, &mut chunk).unwrap();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..read]);
                }
                let head_end = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4)
                    .unwrap_or(request.len());
                let headers = String::from_utf8_lossy(&request).to_ascii_lowercase();
                assert!(headers.contains("content-type: application/ocsp-request"));
                let content_length = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .and_then(|value| value.trim().parse::<usize>().ok())
                    .unwrap_or_default();
                while request.len().saturating_sub(head_end) < content_length {
                    let read = std::io::Read::read(&mut stream, &mut chunk).unwrap();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..read]);
                }
                if index == 1 {
                    std::thread::sleep(Duration::from_millis(300));
                    server_heartbeat_during_request
                        .store(server_heartbeat.load(Ordering::SeqCst), Ordering::SeqCst);
                }
                let body = ocsp_response.as_slice();
                std::io::Write::write_all(
                    &mut stream,
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/ocsp-response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .unwrap();
                std::io::Write::write_all(&mut stream, body).unwrap();
            }
        });

        let config = ReloadableTlsConfig::from_files_with_ocsp_responder(
            certificate_path,
            key_path,
            None,
            None,
            None,
            super::OcspResponderConfig {
                url: format!("http://{address}"),
                refresh_interval: Duration::from_millis(100),
            },
            Duration::from_millis(100),
        )
        .unwrap();
        let first = config.current();
        let heartbeat_task = {
            let heartbeat = Arc::clone(&heartbeat);
            tokio::spawn(async move {
                loop {
                    heartbeat.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
        };
        tokio::time::timeout(Duration::from_millis(500), async {
            while heartbeat.load(Ordering::SeqCst) < 3 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("heartbeat before OCSP refresh");
        heartbeat.store(0, Ordering::SeqCst);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if !Arc::ptr_eq(&first, &config.current()) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("online OCSP refresh");
        assert!(requests.load(Ordering::SeqCst) >= 2);
        assert!(
            heartbeat_during_request.load(Ordering::SeqCst) > 0,
            "OCSP refresh blocked the Tokio worker"
        );
        heartbeat_task.abort();
        let _ = heartbeat_task.await;

        let _ = fs::remove_dir_all(directory);
    }

    fn ocsp_time(time: SystemTime) -> OcspGeneralizedTime {
        OcspGeneralizedTime(GeneralizedTime::from_system_time(time).unwrap())
    }

    #[allow(clippy::too_many_arguments)]
    fn ocsp_response(
        issuer: &Certificate,
        leaf: &Certificate,
        signer: &Certificate,
        signer_key: &KeyPair,
        status: CertStatus,
        this_update: SystemTime,
        next_update: Option<SystemTime>,
        included_certificates: &[&Certificate],
    ) -> Vec<u8> {
        let basic = ocsp_basic_response(
            issuer,
            leaf,
            responder_id_by_name(signer),
            status,
            this_update,
            next_update,
            included_certificates,
        );
        sign_ocsp_response(basic, signer_key)
    }

    fn ocsp_basic_response(
        issuer: &Certificate,
        leaf: &Certificate,
        responder_id: ResponderId,
        status: CertStatus,
        this_update: SystemTime,
        next_update: Option<SystemTime>,
        included_certificates: &[&Certificate],
    ) -> BasicOcspResponse {
        let (_, issuer_parsed) = x509_parser::parse_x509_certificate(issuer.der()).unwrap();
        let leaf_parsed = X509Certificate::from_der(leaf.der()).unwrap();
        let cert_id = CertId {
            hash_algorithm: AlgorithmIdentifier {
                oid: ObjectIdentifier::new_unwrap("1.3.14.3.2.26"),
                parameters: Some(Null.into()),
            },
            issuer_name_hash: OctetString::new(
                Sha1::digest(issuer_parsed.subject().as_raw()).to_vec(),
            )
            .unwrap(),
            issuer_key_hash: OctetString::new(
                Sha1::digest(issuer_parsed.public_key().subject_public_key.data.as_ref()).to_vec(),
            )
            .unwrap(),
            serial_number: leaf_parsed.tbs_certificate().serial_number().clone(),
        };
        let single_response = SingleResponse {
            cert_id,
            cert_status: status,
            this_update: ocsp_time(this_update),
            next_update: next_update.map(ocsp_time),
            single_extensions: None,
        };
        let certs = if included_certificates.is_empty() {
            None
        } else {
            Some(
                included_certificates
                    .iter()
                    .map(|certificate| X509Certificate::from_der(certificate.der()).unwrap())
                    .collect(),
            )
        };
        BasicOcspResponse {
            tbs_response_data: ResponseData {
                version: Default::default(),
                responder_id,
                produced_at: ocsp_time(SystemTime::now()),
                responses: vec![single_response],
                response_extensions: None,
            },
            signature_algorithm: AlgorithmIdentifier {
                oid: ObjectIdentifier::new_unwrap("1.2.840.10045.4.3.2"),
                parameters: None,
            },
            signature: BitString::new(0, Vec::new()).unwrap(),
            certs,
        }
    }

    fn sign_ocsp_response(mut basic: BasicOcspResponse, signer_key: &KeyPair) -> Vec<u8> {
        let tbs = basic.tbs_response_data.to_der().unwrap();
        let rng = ring::rand::SystemRandom::new();
        let key = ring::signature::EcdsaKeyPair::from_pkcs8(
            &ring::signature::ECDSA_P256_SHA256_ASN1_SIGNING,
            &signer_key.serialize_der(),
            &rng,
        )
        .unwrap();
        let signature = key.sign(&rng, &tbs).unwrap();
        basic.signature = BitString::new(0, signature.as_ref().to_vec()).unwrap();
        OcspResponse::successful(basic).unwrap().to_der().unwrap()
    }

    fn responder_id_by_name(certificate: &Certificate) -> ResponderId {
        let certificate = X509Certificate::from_der(certificate.der()).unwrap();
        ResponderId::ByName(certificate.tbs_certificate().subject().clone())
    }

    fn ocsp_fixture() -> (Certificate, KeyPair, Certificate, String) {
        let (issuer, issuer_key) = certificate_authority();
        let (leaf, _) = signed_certificate("localhost", &issuer, &issuer_key, 1);
        let certificate_pem = format!("{}{}", leaf.pem(), issuer.pem());
        (issuer, issuer_key, leaf, certificate_pem)
    }

    fn ocsp_responder_certificate(
        issuer: &Certificate,
        issuer_key: &KeyPair,
        ocsp_signing: bool,
        serial: u64,
    ) -> (Certificate, KeyPair) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::new()).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, "rcaster-test-ocsp-responder");
        params.serial_number = Some(SerialNumber::from(serial));
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        if ocsp_signing {
            params.extended_key_usages = vec![ExtendedKeyUsagePurpose::OcspSigning];
        }
        let certificate = params.signed_by(&key, issuer, issuer_key).unwrap();
        (certificate, key)
    }

    #[test]
    fn ocsp_validation_accepts_valid_and_delegated_responses() {
        let (issuer, issuer_key, leaf, certificate_pem) = ocsp_fixture();
        let now = SystemTime::now();
        let (delegated, delegated_key) = ocsp_responder_certificate(&issuer, &issuer_key, true, 44);

        let issuer_response = ocsp_response(
            &issuer,
            &leaf,
            &issuer,
            &issuer_key,
            CertStatus::good(),
            now - Duration::from_secs(60),
            Some(now + Duration::from_secs(3_600)),
            &[],
        );
        super::validate_ocsp_response(&issuer_response, &certificate_pem).unwrap();

        let delegated_response = ocsp_response(
            &issuer,
            &leaf,
            &delegated,
            &delegated_key,
            CertStatus::good(),
            now - Duration::from_secs(60),
            Some(now + Duration::from_secs(3_600)),
            &[&delegated],
        );
        super::validate_ocsp_response(&delegated_response, &certificate_pem).unwrap();

        let (_, delegated_parsed) = x509_parser::parse_x509_certificate(delegated.der()).unwrap();
        let key_hash = Sha1::digest(
            delegated_parsed
                .public_key()
                .subject_public_key
                .data
                .as_ref(),
        );
        let basic = ocsp_basic_response(
            &issuer,
            &leaf,
            ResponderId::ByKey(OctetString::new(key_hash.to_vec()).unwrap()),
            CertStatus::good(),
            now - Duration::from_secs(60),
            Some(now + Duration::from_secs(3_600)),
            &[&delegated],
        );
        let by_key_response = sign_ocsp_response(basic, &delegated_key);
        super::validate_ocsp_response(&by_key_response, &certificate_pem).unwrap();
    }

    #[test]
    fn ocsp_validation_rejects_response_for_different_certificate_or_issuer() {
        let (issuer, issuer_key, _, certificate_pem) = ocsp_fixture();
        let (other_issuer, other_issuer_key) = certificate_authority();
        let (other_leaf, _) = signed_certificate("other.example", &issuer, &issuer_key, 2);
        let (other_issuer_leaf, _) =
            signed_certificate("other-issuer.example", &other_issuer, &other_issuer_key, 1);
        let now = SystemTime::now();
        let response = ocsp_response(
            &issuer,
            &other_leaf,
            &issuer,
            &issuer_key,
            CertStatus::good(),
            now - Duration::from_secs(60),
            Some(now + Duration::from_secs(3_600)),
            &[],
        );
        assert!(
            super::validate_ocsp_response(&response, &certificate_pem)
                .unwrap_err()
                .to_string()
                .contains("does not match")
        );

        let response = ocsp_response(
            &other_issuer,
            &other_issuer_leaf,
            &other_issuer,
            &other_issuer_key,
            CertStatus::good(),
            now - Duration::from_secs(60),
            Some(now + Duration::from_secs(3_600)),
            &[],
        );
        assert!(
            super::validate_ocsp_response(&response, &certificate_pem)
                .unwrap_err()
                .to_string()
                .contains("does not match")
        );
    }

    #[test]
    fn ocsp_validation_rejects_unknown_and_expired_statuses() {
        let (issuer, issuer_key, leaf, certificate_pem) = ocsp_fixture();
        let now = SystemTime::now();
        let unknown = ocsp_response(
            &issuer,
            &leaf,
            &issuer,
            &issuer_key,
            CertStatus::unknown(),
            now - Duration::from_secs(60),
            Some(now + Duration::from_secs(3_600)),
            &[],
        );
        assert!(
            super::validate_ocsp_response(&unknown, &certificate_pem)
                .unwrap_err()
                .to_string()
                .contains("does not report the certificate as good")
        );

        let expired = ocsp_response(
            &issuer,
            &leaf,
            &issuer,
            &issuer_key,
            CertStatus::good(),
            now - Duration::from_secs(7_200),
            Some(now - Duration::from_secs(60)),
            &[],
        );
        assert!(
            super::validate_ocsp_response(&expired, &certificate_pem)
                .unwrap_err()
                .to_string()
                .contains("nextUpdate has expired")
        );
    }

    #[test]
    fn ocsp_validation_rejects_future_timestamps() {
        let (issuer, issuer_key, leaf, certificate_pem) = ocsp_fixture();
        let now = SystemTime::now();
        let future_this_update = ocsp_response(
            &issuer,
            &leaf,
            &issuer,
            &issuer_key,
            CertStatus::good(),
            now + Duration::from_secs(60),
            Some(now + Duration::from_secs(3_600)),
            &[],
        );
        assert!(
            super::validate_ocsp_response(&future_this_update, &certificate_pem)
                .unwrap_err()
                .to_string()
                .contains("thisUpdate is in the future")
        );

        let mut basic = ocsp_basic_response(
            &issuer,
            &leaf,
            responder_id_by_name(&issuer),
            CertStatus::good(),
            now - Duration::from_secs(60),
            Some(now + Duration::from_secs(3_600)),
            &[],
        );
        basic.tbs_response_data.produced_at = ocsp_time(now + Duration::from_secs(60));
        let future_produced_at = sign_ocsp_response(basic, &issuer_key);
        assert!(
            super::validate_ocsp_response(&future_produced_at, &certificate_pem)
                .unwrap_err()
                .to_string()
                .contains("produced in the future")
        );
    }

    #[test]
    fn ocsp_validation_rejects_invalid_signature() {
        let (issuer, _, leaf, certificate_pem) = ocsp_fixture();
        let (_, wrong_key) = certificate_authority();
        let now = SystemTime::now();
        let response = ocsp_response(
            &issuer,
            &leaf,
            &issuer,
            &wrong_key,
            CertStatus::good(),
            now - Duration::from_secs(60),
            Some(now + Duration::from_secs(3_600)),
            &[],
        );
        assert!(
            super::validate_ocsp_response(&response, &certificate_pem)
                .unwrap_err()
                .to_string()
                .contains("signature is not valid")
        );
    }

    #[test]
    fn ocsp_validation_rejects_delegated_responder_without_ocsp_signing_eku() {
        let (issuer, issuer_key, leaf, certificate_pem) = ocsp_fixture();
        let (delegated, delegated_key) =
            ocsp_responder_certificate(&issuer, &issuer_key, false, 45);
        let now = SystemTime::now();
        let response = ocsp_response(
            &issuer,
            &leaf,
            &delegated,
            &delegated_key,
            CertStatus::good(),
            now - Duration::from_secs(60),
            Some(now + Duration::from_secs(3_600)),
            &[&delegated],
        );
        assert!(
            super::validate_ocsp_response(&response, &certificate_pem)
                .unwrap_err()
                .to_string()
                .contains("signature is not valid")
        );
    }

    #[test]
    fn ocsp_validation_rejects_mismatched_responder_id() {
        let (issuer, issuer_key, leaf, certificate_pem) = ocsp_fixture();
        let (other_responder, _) = signed_certificate("other-responder", &issuer, &issuer_key, 46);
        let now = SystemTime::now();
        let basic = ocsp_basic_response(
            &issuer,
            &leaf,
            responder_id_by_name(&other_responder),
            CertStatus::good(),
            now - Duration::from_secs(60),
            Some(now + Duration::from_secs(3_600)),
            &[],
        );
        let response = sign_ocsp_response(basic, &issuer_key);
        assert!(
            super::validate_ocsp_response(&response, &certificate_pem)
                .unwrap_err()
                .to_string()
                .contains("signature is not valid")
        );
    }
    #[test]
    fn private_key_mode_rejects_group_and_other_access() {
        assert!(super::private_key_mode_is_safe(0o600));
        assert!(super::private_key_mode_is_safe(0o400));
        assert!(!super::private_key_mode_is_safe(0o640));
        assert!(!super::private_key_mode_is_safe(0o604));
    }

    #[cfg(unix)]
    #[test]
    fn private_key_permission_check_rejects_insecure_file() {
        use std::os::unix::fs::PermissionsExt;

        let path = std::env::temp_dir().join(format!(
            "rcaster-private-key-permissions-{}",
            std::process::id()
        ));
        fs::write(&path, "fixture").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(
            super::validate_private_key_permissions(&path)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::PermissionDenied
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        super::validate_private_key_permissions(&path).unwrap();
        let _ = fs::remove_file(path);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn insecure_reloaded_private_key_is_rejected() {
        use std::os::unix::fs::PermissionsExt;

        let directory = std::env::temp_dir().join(format!(
            "rcaster-tls-insecure-reload-{}",
            std::process::id()
        ));
        fs::create_dir_all(&directory).unwrap();
        let certificate_path = directory.join("server.crt");
        let key_path = directory.join("server.key");
        let (certificate, key) = certificate_pair("first.example");
        fs::write(&certificate_path, certificate).unwrap();
        write_private_key(&key_path, &key);
        let config = ReloadableTlsConfig::from_files(
            certificate_path,
            key_path.clone(),
            None,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let first = config.current();
        fs::set_permissions(&key_path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(config.reload_now().is_err());
        assert!(Arc::ptr_eq(&first, &config.current()));
        let _ = fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn client_crl_reload_rejects_revoked_certificate() {
        let directory =
            std::env::temp_dir().join(format!("rcaster-tls-client-crl-{}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let (ca_certificate, ca_key) = certificate_authority();
        let (server_certificate, server_key) =
            signed_certificate("localhost", &ca_certificate, &ca_key, 1);
        let (client_certificate, client_key) =
            signed_certificate("caster-client", &ca_certificate, &ca_key, CLIENT_SERIAL);
        let certificate_path = directory.join("server.crt");
        let key_path = directory.join("server.key");
        let ca_path = directory.join("ca.crt");
        let crl_path = directory.join("client.crl");
        fs::write(&certificate_path, server_certificate.pem()).unwrap();
        write_private_key(&key_path, &server_key.serialize_pem());
        fs::write(&ca_path, ca_certificate.pem()).unwrap();
        fs::write(&crl_path, client_crl(&ca_certificate, &ca_key, false)).unwrap();
        let config = ReloadableTlsConfig::from_files_with_client_crl(
            certificate_path,
            key_path,
            Some(ca_path),
            Some(crl_path.clone()),
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let client = client_config(&client_certificate, &client_key, &ca_certificate);
        handshake(&config, Arc::clone(&client))
            .await
            .expect("non-revoked client");

        fs::write(&crl_path, client_crl(&ca_certificate, &ca_key, true)).unwrap();
        config.reload_now().expect("CRL reload");
        assert!(handshake(&config, Arc::clone(&client)).await.is_err());

        fs::write(&crl_path, "not a CRL").unwrap();
        assert!(config.reload_now().is_err());
        assert!(handshake(&config, client).await.is_err());
        let _ = fs::remove_dir_all(directory);
    }

    async fn handshake(config: &ReloadableTlsConfig, client: Arc<ClientConfig>) -> io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server_config = config.current();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await?;
            TlsAcceptor::from(server_config).accept(socket).await?;
            Ok::<(), io::Error>(())
        });
        let socket = TcpStream::connect(address).await?;
        let client = TlsConnector::from(client)
            .connect(ServerName::try_from("localhost").unwrap(), socket)
            .await;
        let server = server.await.unwrap();
        match (client, server) {
            (Ok(_), Ok(())) => Ok(()),
            (client, server) => Err(io::Error::other(format!(
                "mTLS failed: client={client:?}, server={server:?}"
            ))),
        }
    }

    fn certificate_authority() -> (Certificate, KeyPair) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(DnType::CommonName, "rcaster-test-ca");
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let certificate = params.self_signed(&key).unwrap();
        (certificate, key)
    }

    fn signed_certificate(
        name: &str,
        ca_certificate: &Certificate,
        ca_key: &KeyPair,
        serial: u64,
    ) -> (Certificate, KeyPair) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![name.to_owned()]).unwrap();
        params.serial_number = Some(SerialNumber::from(serial));
        let certificate = params.signed_by(&key, ca_certificate, ca_key).unwrap();
        (certificate, key)
    }

    fn client_config(
        certificate: &Certificate,
        key: &KeyPair,
        ca_certificate: &Certificate,
    ) -> Arc<ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots.add(ca_certificate.der().clone()).unwrap();
        let key = PrivateKeyDer::from_pem_slice(key.serialize_pem().as_bytes()).unwrap();
        Arc::new(
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_client_auth_cert(vec![certificate.der().clone()], key)
                .unwrap(),
        )
    }

    fn client_crl(ca_certificate: &Certificate, ca_key: &KeyPair, revoked: bool) -> String {
        let revoked_certs = if revoked {
            vec![RevokedCertParams {
                serial_number: SerialNumber::from(CLIENT_SERIAL),
                revocation_time: date_time_ymd(2026, 11, 22),
                reason_code: Some(RevocationReason::KeyCompromise),
                invalidity_date: None,
            }]
        } else {
            Vec::new()
        };
        CertificateRevocationListParams {
            this_update: date_time_ymd(2026, 1, 1),
            next_update: date_time_ymd(2030, 1, 1),
            crl_number: SerialNumber::from(1),
            issuing_distribution_point: None,
            revoked_certs,
            key_identifier_method: KeyIdMethod::Sha256,
        }
        .signed_by(ca_certificate, ca_key)
        .unwrap()
        .pem()
        .unwrap()
    }

    fn write_private_key(path: &PathBuf, key: &str) {
        fs::write(path, key).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
    }

    fn certificate_pair(name: &str) -> (String, String) {
        let key = KeyPair::generate().unwrap();
        let params = CertificateParams::new(vec![name.to_owned()]).unwrap();
        let certificate = params.self_signed(&key).unwrap();
        (certificate.pem(), key.serialize_pem())
    }
}
