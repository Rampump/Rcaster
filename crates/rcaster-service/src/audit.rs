use std::{
    collections::VecDeque,
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Default)]
pub struct AuditLog {
    inner: Option<Arc<AuditInner>>,
}

struct AuditInner {
    path: PathBuf,
    write_lock: Mutex<()>,
    max_bytes: u64,
    retention: Duration,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AuditEvent {
    pub timestamp: u64,
    pub request_id: String,
    pub actor: String,
    pub source_ip: Option<String>,
    pub action: String,
    pub target: String,
    pub result: String,
    pub details: Value,
}

impl AuditLog {
    pub fn open(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        Self::open_with_retention(path, 16 * 1024 * 1024, Duration::from_secs(90 * 86_400))
    }

    pub fn open_with_retention(
        path: impl Into<PathBuf>,
        max_bytes: u64,
        retention: Duration,
    ) -> std::io::Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }
        OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            inner: Some(Arc::new(AuditInner {
                path,
                write_lock: Mutex::new(()),
                max_bytes,
                retention,
            })),
        })
    }

    pub fn record(&self, event: AuditEvent) {
        let Some(inner) = &self.inner else {
            return;
        };
        let inner = Arc::clone(inner);
        tokio::task::spawn_blocking(move || {
            let _guard = inner
                .write_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            rotate_if_needed(&inner);
            let Ok(mut file) = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&inner.path)
            else {
                return;
            };
            if let Ok(mut line) = serde_json::to_vec(&event) {
                line.push(b'\n');
                let _ = file.write_all(&line);
            }
        });
    }

    pub async fn recent(&self, limit: usize) -> std::io::Result<Vec<AuditEvent>> {
        let Some(inner) = self.inner.clone() else {
            return Ok(Vec::new());
        };
        tokio::task::spawn_blocking(move || {
            let _guard = inner
                .write_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            read_recent(&inner.path, limit)
        })
        .await
        .map_err(std::io::Error::other)?
    }

    pub async fn prune(&self) -> std::io::Result<usize> {
        let Some(inner) = self.inner.clone() else {
            return Ok(0);
        };
        tokio::task::spawn_blocking(move || {
            let _guard = inner
                .write_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Ok(prune_rotated(&inner.path, inner.retention))
        })
        .await
        .map_err(std::io::Error::other)?
    }
}

fn read_recent(path: &Path, limit: usize) -> std::io::Result<Vec<AuditEvent>> {
    let mut paths = rotated_paths(path);
    paths.push(path.to_path_buf());
    let mut events = VecDeque::with_capacity(limit);
    for path in paths {
        let file = match OpenOptions::new().read(true).open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        for line in BufReader::new(file).lines() {
            let line = line?;
            if let Ok(event) = serde_json::from_str::<AuditEvent>(&line) {
                if events.len() == limit {
                    events.pop_front();
                }
                events.push_back(event);
            }
        }
    }
    Ok(events.into_iter().rev().collect())
}

fn rotate_if_needed(inner: &AuditInner) {
    let Ok(metadata) = fs::metadata(&inner.path) else {
        return;
    };
    if metadata.len() < inner.max_bytes {
        return;
    }
    let rotated = rotated_path(&inner.path, unix_nanos());
    if fs::rename(&inner.path, &rotated).is_err() {
        return;
    }
    let _ = prune_rotated(&inner.path, inner.retention);
}

fn rotated_paths(path: &Path) -> Vec<PathBuf> {
    let Some(parent) = path.parent() else {
        return Vec::new();
    };
    let Some(base) = path.file_name().and_then(|name| name.to_str()) else {
        return Vec::new();
    };
    let prefix = format!("{base}.");
    let mut paths = fs::read_dir(parent)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_str()?;
            name.strip_prefix(&prefix)?;
            Some((entry.metadata().ok()?.modified().ok()?, entry.path()))
        })
        .collect::<Vec<_>>();
    paths.sort_by_key(|(modified, _)| *modified);
    paths.into_iter().map(|(_, path)| path).collect()
}

fn prune_rotated(path: &Path, retention: Duration) -> usize {
    let cutoff = SystemTime::now()
        .checked_sub(retention)
        .unwrap_or(UNIX_EPOCH);
    let mut removed = 0;
    for rotated in rotated_paths(path) {
        if fs::metadata(&rotated)
            .and_then(|metadata| metadata.modified())
            .is_ok_and(|modified| modified < cutoff)
            && fs::remove_file(rotated).is_ok()
        {
            removed += 1;
        }
    }
    removed
}

fn rotated_path(path: &Path, timestamp: u128) -> PathBuf {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return path.with_extension(timestamp.to_string());
    };
    path.with_file_name(format!("{name}.{timestamp}"))
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use std::{fs, time::Duration};

    use super::{AuditEvent, AuditLog};

    #[tokio::test]
    async fn rotates_large_logs_and_keeps_recent_events_visible() {
        let path = std::env::temp_dir().join(format!(
            "rcaster-audit-rotation-{}.jsonl",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        let log = AuditLog::open_with_retention(&path, 250, Duration::from_secs(86_400)).unwrap();
        for index in 0..8 {
            log.record(event(index));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let recent = log.recent(3).await.unwrap();
        assert_eq!(
            recent
                .iter()
                .map(|event| event.target.as_str())
                .collect::<Vec<_>>(),
            vec!["7", "6", "5"]
        );
        let rotated = fs::read_dir(path.parent().unwrap())
            .unwrap()
            .flatten()
            .any(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("rcaster-audit-rotation-")
                    && entry.path() != path
            });
        assert!(rotated);
        for entry in fs::read_dir(path.parent().unwrap()).unwrap().flatten() {
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with("rcaster-audit-rotation-")
            {
                let _ = fs::remove_file(entry.path());
            }
        }
    }

    #[tokio::test]
    async fn periodic_prune_removes_old_rotations_without_new_events() {
        let path =
            std::env::temp_dir().join(format!("rcaster-audit-prune-{}.jsonl", std::process::id()));
        let _ = fs::remove_file(&path);
        let rotated = path.with_file_name(format!(
            "{}.old",
            path.file_name().unwrap().to_string_lossy()
        ));
        fs::write(&rotated, b"{}\n").unwrap();
        let log = AuditLog::open_with_retention(&path, 1024, Duration::ZERO).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(log.prune().await.unwrap(), 1);
        assert!(!rotated.exists());
        let _ = fs::remove_file(path);
    }

    fn event(index: usize) -> AuditEvent {
        AuditEvent {
            timestamp: index as u64,
            request_id: format!("request-{index}"),
            actor: "admin".to_owned(),
            source_ip: None,
            action: "test".to_owned(),
            target: index.to_string(),
            result: "success".to_owned(),
            details: serde_json::json!({"padding": "x".repeat(100)}),
        }
    }
}
