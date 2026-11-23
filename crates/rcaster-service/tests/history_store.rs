use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use rcaster_service::{history_store::HistoryStore, hub::StreamHistoryPoint};

#[test]
fn history_store_round_trips_recent_points() {
    let path = TempHistory::new();
    let mut histories = BTreeMap::new();
    histories.insert(
        "C63".to_owned(),
        vec![StreamHistoryPoint {
            timestamp: 42,
            packets: 7,
            bytes: 128,
            bitrate_bps: Some(1024),
            warning_codes: 0,
        }],
    );
    {
        let store = HistoryStore::open(path.as_ref()).expect("open");
        store.save(&histories).expect("save");
    }
    let store = HistoryStore::open(path.as_ref()).expect("reopen");
    assert_eq!(store.load().expect("load"), histories);
}

struct TempHistory(PathBuf);

impl TempHistory {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        Self(std::env::temp_dir().join(format!(
            "rcaster-history-{}-{unique}.redb",
            std::process::id()
        )))
    }
}

impl AsRef<std::path::Path> for TempHistory {
    fn as_ref(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempHistory {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
