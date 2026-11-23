use std::{collections::BTreeMap, fmt, path::Path};

use redb::{Database, ReadableTable, TableDefinition};

use crate::hub::StreamHistoryPoint;

const HISTORY: TableDefinition<&str, &[u8]> = TableDefinition::new("stream_history");

pub struct HistoryStore {
    database: Database,
}

#[derive(Debug)]
pub enum HistoryStoreError {
    Database(String),
    Json(serde_json::Error),
}

impl fmt::Display for HistoryStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "history database error: {error}"),
            Self::Json(error) => write!(formatter, "history JSON error: {error}"),
        }
    }
}

impl std::error::Error for HistoryStoreError {}

impl HistoryStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, HistoryStoreError> {
        let database = Database::create(path)
            .map_err(|error| HistoryStoreError::Database(error.to_string()))?;
        let transaction = database
            .begin_write()
            .map_err(|error| HistoryStoreError::Database(error.to_string()))?;
        transaction
            .open_table(HISTORY)
            .map_err(|error| HistoryStoreError::Database(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| HistoryStoreError::Database(error.to_string()))?;
        Ok(Self { database })
    }

    pub fn load(&self) -> Result<BTreeMap<String, Vec<StreamHistoryPoint>>, HistoryStoreError> {
        let transaction = self
            .database
            .begin_read()
            .map_err(|error| HistoryStoreError::Database(error.to_string()))?;
        let table = transaction
            .open_table(HISTORY)
            .map_err(|error| HistoryStoreError::Database(error.to_string()))?;
        let mut histories = BTreeMap::new();
        for entry in table
            .iter()
            .map_err(|error| HistoryStoreError::Database(error.to_string()))?
        {
            let (key, value) =
                entry.map_err(|error| HistoryStoreError::Database(error.to_string()))?;
            if let Ok(points) = serde_json::from_slice::<Vec<StreamHistoryPoint>>(value.value()) {
                histories.insert(key.value().to_owned(), points);
            }
        }
        Ok(histories)
    }

    pub fn save(
        &self,
        histories: &BTreeMap<String, Vec<StreamHistoryPoint>>,
    ) -> Result<(), HistoryStoreError> {
        let transaction = self
            .database
            .begin_write()
            .map_err(|error| HistoryStoreError::Database(error.to_string()))?;
        {
            let mut table = transaction
                .open_table(HISTORY)
                .map_err(|error| HistoryStoreError::Database(error.to_string()))?;
            for (mountpoint, points) in histories {
                let value = serde_json::to_vec(points)?;
                table
                    .insert(mountpoint.as_str(), value.as_slice())
                    .map_err(|error| HistoryStoreError::Database(error.to_string()))?;
            }
        }
        transaction
            .commit()
            .map_err(|error| HistoryStoreError::Database(error.to_string()))
    }
}

impl From<serde_json::Error> for HistoryStoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}
