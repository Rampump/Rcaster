use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use etcd_client::{Client, EventType, GetOptions, WatchOptions};
use serde::Serialize;
use serde_json::Value;
use tokio::{sync::watch, task::JoinHandle};

#[derive(Clone, Debug, Serialize)]
pub struct LocalSession {
    pub session_id: String,
    pub session_type: String,
    pub user_id: String,
    pub mountpoint: String,
    pub ntrip_version: String,
    pub client_ip: String,
    pub started_at: u64,
    pub software: Option<String>,
}

#[derive(Clone, Default)]
pub struct ConnectionController {
    inner: Arc<Mutex<HashMap<String, Vec<watch::Sender<bool>>>>>,
    sessions: Arc<Mutex<HashMap<String, LocalSession>>>,
}

pub struct ConnectionCancel {
    controller: ConnectionController,
    sender: watch::Sender<bool>,
    receiver: watch::Receiver<bool>,
    keys: Vec<String>,
    session_ids: Vec<String>,
}

pub struct CommandWatcher {
    tasks: Vec<JoinHandle<()>>,
}

impl ConnectionController {
    pub fn connection(&self) -> ConnectionCancel {
        let (sender, receiver) = watch::channel(false);
        ConnectionCancel {
            controller: self.clone(),
            sender,
            receiver,
            keys: Vec::new(),
            session_ids: Vec::new(),
        }
    }

    fn cancel(&self, key: &str) -> bool {
        let Some(senders) = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .cloned()
        else {
            return false;
        };
        for sender in senders {
            sender.send_replace(true);
        }
        true
    }

    pub fn cancel_user(&self, user_id: &str) -> bool {
        self.cancel(&format!("user:{user_id}"))
    }

    pub fn cancel_web_user(&self, user_id: &str) -> bool {
        self.cancel(&format!("web-user:{user_id}"))
    }

    pub fn cancel_source(&self, mountpoint: &str) -> bool {
        self.cancel(&format!("source:{mountpoint}"))
    }

    pub fn cancel_session(&self, session_id: &str) -> bool {
        if !self.cancel(&format!("session:{session_id}")) {
            return false;
        }
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
        true
    }

    pub fn sessions(&self) -> Vec<LocalSession> {
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        sessions.sort_by_key(|session| session.started_at);
        sessions
    }

    pub fn cancel_all(&self) {
        for senders in self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
        {
            for sender in senders {
                sender.send_replace(true);
            }
        }
    }
}

impl ConnectionCancel {
    pub fn bind(&mut self, key: String) {
        self.controller
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(key.clone())
            .or_default()
            .push(self.sender.clone());
        self.keys.push(key);
    }

    pub fn track_session(&mut self, session: LocalSession) {
        let session_id = session.session_id.clone();
        self.bind(format!("session:{session_id}"));
        self.controller
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(session_id.clone(), session);
        self.session_ids.push(session_id);
    }

    pub async fn cancelled(&mut self) {
        loop {
            if *self.receiver.borrow() {
                return;
            }
            if self.receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Drop for ConnectionCancel {
    fn drop(&mut self) {
        let mut connections = self
            .controller
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for key in &self.keys {
            let Some(senders) = connections.get_mut(key) else {
                continue;
            };
            senders.retain(|sender| !sender.same_channel(&self.sender));
            if senders.is_empty() {
                connections.remove(key);
            }
        }
        let mut sessions = self
            .controller
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for session_id in &self.session_ids {
            sessions.remove(session_id);
        }
    }
}

impl CommandWatcher {
    pub async fn start(
        client: Client,
        cluster_id: &str,
        caster_id: &str,
        controller: Arc<ConnectionController>,
    ) -> Result<Self, etcd_client::Error> {
        let session_prefix = format!("/rcaster/{cluster_id}/online/commands/{caster_id}/");
        let source_prefix = format!("/rcaster/{cluster_id}/runtime/source-commands/");
        let user_prefix = format!("/rcaster/{cluster_id}/config/users/");
        Ok(Self {
            tasks: vec![
                tokio::spawn(watch_commands(
                    client.clone(),
                    session_prefix,
                    "session",
                    Arc::clone(&controller),
                )),
                tokio::spawn(watch_commands(
                    client.clone(),
                    source_prefix,
                    "source",
                    Arc::clone(&controller),
                )),
                tokio::spawn(watch_user_access(client, user_prefix, controller)),
            ],
        })
    }

    pub async fn shutdown(self) {
        for task in self.tasks {
            task.abort();
            let _ = task.await;
        }
    }
}

async fn watch_user_access(
    mut client: Client,
    prefix: String,
    controller: Arc<ConnectionController>,
) {
    loop {
        let response = client
            .get(prefix.clone(), Some(GetOptions::new().with_prefix()))
            .await;
        let Ok(response) = response else {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            continue;
        };
        let revision = response
            .header()
            .map(|header| header.revision())
            .unwrap_or_default();
        for kv in response.kvs() {
            cancel_disabled_user(kv.key(), kv.value(), &controller);
        }
        let Ok(mut stream) = client
            .watch(
                prefix.clone(),
                Some(
                    WatchOptions::new()
                        .with_prefix()
                        .with_start_revision(revision + 1)
                        .with_progress_notify(),
                ),
            )
            .await
        else {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            continue;
        };
        loop {
            let Ok(Some(response)) = stream.message().await else {
                break;
            };
            if response.canceled() {
                break;
            }
            for event in response.events() {
                let Some(kv) = event.kv() else {
                    continue;
                };
                if event.event_type() == EventType::Delete {
                    if let Some(user_id) = user_id_from_key(kv.key()) {
                        controller.cancel_user(user_id);
                        controller.cancel_web_user(user_id);
                    }
                } else {
                    if let Some(user_id) = user_id_from_key(kv.key()) {
                        controller.cancel_web_user(user_id);
                    }
                    cancel_disabled_user(kv.key(), kv.value(), &controller);
                }
            }
        }
    }
}

fn cancel_disabled_user(key: &[u8], value: &[u8], controller: &ConnectionController) {
    let user = serde_json::from_slice::<Value>(value).ok();
    let disabled = user.as_ref().is_some_and(|user| {
        user.get("enabled").and_then(Value::as_bool) == Some(false)
            || user.get("ntrip_enabled").and_then(Value::as_bool) == Some(false)
    });
    if disabled && let Some(user_id) = user_id_from_key(key) {
        controller.cancel_user(user_id);
    }
}

fn user_id_from_key(key: &[u8]) -> Option<&str> {
    std::str::from_utf8(key).ok()?.rsplit('/').next()
}

async fn watch_commands(
    mut client: Client,
    prefix: String,
    kind: &'static str,
    controller: Arc<ConnectionController>,
) {
    loop {
        let response = client
            .get(prefix.clone(), Some(GetOptions::new().with_prefix()))
            .await;
        let Ok(response) = response else {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            continue;
        };
        let revision = response
            .header()
            .map(|header| header.revision())
            .unwrap_or_default();
        for kv in response.kvs() {
            process_command(&mut client, kv.key(), kv.value(), kind, &controller).await;
        }
        let Ok(mut stream) = client
            .watch(
                prefix.clone(),
                Some(
                    WatchOptions::new()
                        .with_prefix()
                        .with_start_revision(revision + 1)
                        .with_progress_notify(),
                ),
            )
            .await
        else {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            continue;
        };
        loop {
            let Ok(Some(response)) = stream.message().await else {
                break;
            };
            if response.canceled() {
                break;
            }
            for event in response.events() {
                let Some(kv) = event.kv() else {
                    continue;
                };
                process_command(&mut client, kv.key(), kv.value(), kind, &controller).await;
            }
        }
    }
}

async fn process_command(
    client: &mut Client,
    key: &[u8],
    value: &[u8],
    kind: &str,
    controller: &ConnectionController,
) {
    let Ok(command) = serde_json::from_slice::<Value>(value) else {
        return;
    };
    let target = if kind == "session" {
        command
            .get("session_id")
            .and_then(Value::as_str)
            .map(|session_id| format!("session:{session_id}"))
    } else {
        command
            .get("mountpoint")
            .and_then(Value::as_str)
            .zip(command.get("connection_id").and_then(Value::as_str))
            .map(|(mountpoint, connection_id)| format!("source:{mountpoint}:{connection_id}"))
    };
    if target
        .as_deref()
        .is_some_and(|target| controller.cancel(target))
    {
        let _ = client.delete(key.to_vec(), None).await;
    }
}
