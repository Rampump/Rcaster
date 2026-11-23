use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use etcd_client::{Client, EventType, GetOptions, WatchOptions};
use tokio::{sync::oneshot, task::JoinHandle};

use crate::{
    ETCD_WATCH_IDLE_TIMEOUT,
    admission::{AdmissionError, SessionRecord},
};

pub struct SessionWatcher {
    sessions: Arc<RwLock<BTreeMap<String, SessionRecord>>>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl SessionWatcher {
    pub async fn start(mut client: Client, cluster_id: &str) -> Result<Self, AdmissionError> {
        let prefix = format!("/rcaster/{cluster_id}/online/users/");
        let (loaded, revision) = load_sessions(&mut client, &prefix).await?;
        let sessions = Arc::new(RwLock::new(loaded));
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let task_sessions = Arc::clone(&sessions);
        let task = tokio::spawn(async move {
            run_session_watch(client, prefix, revision + 1, task_sessions, &mut stop_rx).await;
        });
        Ok(Self {
            sessions,
            stop: Some(stop_tx),
            task: Some(task),
        })
    }

    pub fn count(&self) -> usize {
        self.sessions
            .read()
            .map(|sessions| sessions.len())
            .unwrap_or_default()
    }

    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

async fn run_session_watch(
    mut client: Client,
    prefix: String,
    mut start_revision: i64,
    sessions: Arc<RwLock<BTreeMap<String, SessionRecord>>>,
    stop: &mut oneshot::Receiver<()>,
) {
    loop {
        let loaded = tokio::select! {
            _ = &mut *stop => return,
            loaded = load_sessions(&mut client, &prefix) => loaded,
        };
        let Ok((snapshot, revision)) = loaded else {
            tokio::select! {
                _ = &mut *stop => return,
                _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => continue,
            }
        };
        if let Ok(mut current) = sessions.write() {
            *current = snapshot;
        } else {
            return;
        }
        start_revision = start_revision.max(revision + 1);
        let stream = tokio::select! {
            _ = &mut *stop => return,
            stream = client.watch(
                prefix.clone(),
                Some(
                    WatchOptions::new()
                        .with_prefix()
                        .with_start_revision(start_revision)
                        .with_progress_notify(),
                ),
            ) => stream,
        };
        let Ok(mut stream) = stream else {
            tokio::select! {
                _ = &mut *stop => return,
                _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => continue,
            }
        };
        loop {
            let response = tokio::select! {
                _ = &mut *stop => return,
                response = tokio::time::timeout(ETCD_WATCH_IDLE_TIMEOUT, stream.message()) => response,
            };
            let Some(Some(response)) = response.ok().and_then(Result::ok) else {
                break;
            };
            if response.canceled() {
                break;
            }
            let Ok(mut current) = sessions.write() else {
                return;
            };
            for event in response.events() {
                let Some(kv) = event.kv() else {
                    continue;
                };
                start_revision = start_revision.max(kv.mod_revision() + 1);
                if event.event_type() == EventType::Delete {
                    if let Ok(key) = std::str::from_utf8(kv.key())
                        && let Some(session_id) = key.rsplit('/').next()
                    {
                        current.remove(session_id);
                    }
                } else {
                    let Ok(session) = serde_json::from_slice::<SessionRecord>(kv.value()) else {
                        continue;
                    };
                    current.insert(session.session_id.clone(), session);
                }
            }
        }
    }
}

async fn load_sessions(
    client: &mut Client,
    prefix: &str,
) -> Result<(BTreeMap<String, SessionRecord>, i64), AdmissionError> {
    let response = client
        .get(prefix.to_owned(), Some(GetOptions::new().with_prefix()))
        .await?;
    let revision = response
        .header()
        .map(|header| header.revision())
        .unwrap_or_default();
    let mut sessions = BTreeMap::new();
    for kv in response.kvs() {
        let session: SessionRecord = serde_json::from_slice(kv.value())?;
        sessions.insert(session.session_id.clone(), session);
    }
    Ok((sessions, revision))
}
