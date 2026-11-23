use std::{
    error,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use etcd_client::{
    Certificate, Channel as EtcdChannel, Client, ConnectOptions, Identity, TlsOptions,
};
use tokio::{
    sync::{mpsc::Sender, oneshot},
    task::JoinHandle,
    time::sleep,
};
use tonic::{
    codegen::http::Uri,
    transport::{Channel, Endpoint, channel::Change},
};

use crate::tls_reload::validate_private_key_permissions;

type EtcdReloadResult<T> = Result<T, Box<dyn error::Error + Send + Sync>>;

#[derive(Clone, Debug)]
pub struct EtcdTlsSource {
    pub ca_certificate: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub domain_name: String,
}

#[derive(Clone)]
pub struct ReloadableEtcdClient {
    client: Client,
    _inner: Arc<Inner>,
}

struct Inner {
    live: Arc<LiveChannel>,
    stop: Mutex<Option<oneshot::Sender<()>>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

struct LiveChannel {
    updater: Sender<Change<Uri, Endpoint>>,
}

#[derive(Clone)]
struct EtcdSource {
    endpoints: Vec<String>,
    tls: EtcdTlsSource,
    interval: Duration,
}

impl ReloadableEtcdClient {
    pub async fn connect(
        endpoints: Vec<String>,
        tls: Option<EtcdTlsSource>,
        interval: Duration,
    ) -> EtcdReloadResult<Self> {
        if endpoints.is_empty() || interval.is_zero() {
            return Err("etcd endpoints and reload interval must be non-empty".into());
        }
        let mut initial_fingerprint = None;
        let (initial, options) = match tls.as_ref() {
            Some(tls) => {
                let (options, fingerprint) = load_tls(tls)?;
                initial_fingerprint = Some(fingerprint);
                let updates = build_endpoints(&endpoints, Some(options.clone()))?;
                (updates, ConnectOptions::new().with_tls(options))
            }
            None => (build_endpoints(&endpoints, None)?, ConnectOptions::new()),
        };
        let (channel, updater) = Channel::balance_channel(64);
        send_updates(&updater, initial)?;
        let mut client =
            Client::from_channel(EtcdChannel::Tonic(channel), Some(options.clone())).await?;
        client.status().await?;

        let live = Arc::new(LiveChannel { updater });
        let inner = Arc::new(Inner {
            live,
            stop: Mutex::new(None),
            task: Mutex::new(None),
        });
        if let Some(tls) = tls {
            let source = EtcdSource {
                endpoints: endpoints.clone(),
                tls,
                interval,
            };
            let weak = Arc::downgrade(&inner);
            let (stop, mut stopped) = oneshot::channel();
            let Some(mut fingerprint) = initial_fingerprint else {
                return Err("missing etcd TLS credential fingerprint".into());
            };
            let task = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = &mut stopped => return,
                        _ = sleep(source.interval) => {}
                    }
                    let Some(inner) = weak.upgrade() else {
                        return;
                    };
                    let (tls, next_fingerprint) = match load_tls(&source.tls) {
                        Ok(loaded) => loaded,
                        Err(error) => {
                            eprintln!(
                                "rcaster: etcd TLS reload failed, keeping existing credentials: {error}"
                            );
                            continue;
                        }
                    };
                    if next_fingerprint == fingerprint {
                        continue;
                    }
                    let updates = match build_endpoints(&source.endpoints, Some(tls.clone())) {
                        Ok(updates) => updates,
                        Err(error) => {
                            eprintln!(
                                "rcaster: etcd TLS reload failed, keeping existing credentials: {error}"
                            );
                            continue;
                        }
                    };
                    let options = ConnectOptions::new().with_tls(tls);
                    if let Err(error) = probe(updates.clone(), options.clone()).await {
                        eprintln!(
                            "rcaster: etcd TLS reload probe failed, keeping existing credentials: {error}"
                        );
                        continue;
                    }
                    if let Err(error) = send_updates_async(&inner.live.updater, updates).await {
                        eprintln!(
                            "rcaster: etcd TLS reload activation failed, keeping existing credentials: {error}"
                        );
                        continue;
                    }
                    fingerprint = next_fingerprint;
                }
            });
            *inner
                .stop
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(stop);
            *inner
                .task
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(task);
        }
        Ok(Self {
            client,
            _inner: inner,
        })
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(stop) = self
            .stop
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = stop.send(());
        }
        if let Some(task) = self
            .task
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
    }
}

fn send_updates(
    updater: &Sender<Change<Uri, Endpoint>>,
    updates: Vec<(Uri, Endpoint)>,
) -> EtcdReloadResult<()> {
    for (uri, endpoint) in updates {
        updater
            .try_send(Change::Insert(uri, endpoint))
            .map_err(|_| "failed to update etcd endpoint")?;
    }
    Ok(())
}

async fn send_updates_async(
    updater: &Sender<Change<Uri, Endpoint>>,
    updates: Vec<(Uri, Endpoint)>,
) -> EtcdReloadResult<()> {
    for (uri, endpoint) in updates {
        updater
            .send(Change::Insert(uri, endpoint))
            .await
            .map_err(|_| "failed to update etcd endpoint")?;
    }
    Ok(())
}

fn load_tls(source: &EtcdTlsSource) -> EtcdReloadResult<(TlsOptions, Vec<u8>)> {
    validate_private_key_permissions(&source.private_key)?;
    let ca = std::fs::read(&source.ca_certificate)?;
    let certificate = std::fs::read(&source.certificate)?;
    let private_key = std::fs::read(&source.private_key)?;
    let mut fingerprint = Vec::with_capacity(ca.len() + certificate.len() + private_key.len());
    fingerprint.extend_from_slice(&ca);
    fingerprint.extend_from_slice(&certificate);
    fingerprint.extend_from_slice(&private_key);
    let options = TlsOptions::new()
        .ca_certificate(Certificate::from_pem(ca))
        .identity(Identity::from_pem(certificate, private_key))
        .domain_name(source.domain_name.clone());
    Ok((options, fingerprint))
}

fn build_endpoints(
    endpoints: &[String],
    tls: Option<TlsOptions>,
) -> EtcdReloadResult<Vec<(Uri, Endpoint)>> {
    endpoints
        .iter()
        .map(|address| {
            let endpoint = build_endpoint(address, tls.clone())?;
            Ok((endpoint.uri().clone(), endpoint))
        })
        .collect()
}

fn build_endpoint(address: &str, tls: Option<TlsOptions>) -> EtcdReloadResult<Endpoint> {
    let uri = if address.starts_with("http://") || address.starts_with("https://") {
        address.parse::<Uri>()?
    } else if tls.is_some() {
        format!("https://{address}").parse::<Uri>()?
    } else {
        format!("http://{address}").parse::<Uri>()?
    };
    let endpoint = Endpoint::from_shared(uri.to_string())?;
    match tls {
        Some(tls) => Ok(endpoint.tls_config(tls)?),
        None => Ok(endpoint),
    }
}

async fn probe(updates: Vec<(Uri, Endpoint)>, options: ConnectOptions) -> EtcdReloadResult<()> {
    let (channel, updater) = Channel::balance_channel(64);
    send_updates_async(&updater, updates).await?;
    let mut client = Client::from_channel(EtcdChannel::Tonic(channel), Some(options)).await?;
    client.status().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::ReloadableEtcdClient;

    #[tokio::test]
    async fn connects_with_a_reloadable_channel() {
        let endpoint = std::env::var("RCASTER_ETCD_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned());
        let reloadable =
            ReloadableEtcdClient::connect(vec![endpoint], None, Duration::from_secs(60))
                .await
                .expect("connect reloadable etcd client");
        reloadable
            .client()
            .status()
            .await
            .expect("etcd status through reloadable channel");
    }
}
