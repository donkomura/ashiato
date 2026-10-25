use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

mod backend;
pub mod fixture;

pub use backend::DuckDbBackend;

const SCHEMA_SQL: &str = include_str!("../sql/schema.sql");
const STAGE_RECORDS_SQL: &str = include_str!("../sql/stage_records.sql");
const NORMALIZE_EVENTS_SQL: &str = include_str!("../sql/normalize_events.sql");
const LOAD_FILE_SQL: &str = "CREATE OR REPLACE TEMP TABLE loaded_records AS \
     SELECT unnest(Records) AS record \
     FROM read_json(?, columns = {'Records': 'JSON[]'}, compression = 'gzip')";
const LOAD_RECORDS_SQL: &str = "CREATE OR REPLACE TEMP TABLE loaded_records AS \
     SELECT unnest(CAST(CAST(? AS JSON) AS JSON[])) AS record";

const EXCLUDED_DIRECTORIES: [&str; 2] = ["CloudTrail-Digest", "CloudTrail-Insight"];

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("failed to read {path}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestSummary {
    pub files: usize,
    pub skipped_files: usize,
    pub events: usize,
}

pub struct EventStore {
    connection: duckdb::Connection,
}

impl EventStore {
    pub fn open(path: &Path) -> Result<Self, IngestError> {
        let connection = duckdb::Connection::open(path)?;
        connection.execute_batch(SCHEMA_SQL)?;
        Ok(Self { connection })
    }

    pub(crate) fn open_in_memory() -> Result<Self, IngestError> {
        let connection = duckdb::Connection::open_in_memory()?;
        connection.execute_batch(SCHEMA_SQL)?;
        Ok(Self { connection })
    }

    pub(crate) fn into_backend(self) -> DuckDbBackend {
        DuckDbBackend::from_connection(self.connection)
    }

    pub(crate) fn ingest_records(
        &self,
        records: &[serde_json::Value],
    ) -> Result<usize, IngestError> {
        let records = serde_json::Value::from(records).to_string();
        let transaction = self.connection.unchecked_transaction()?;
        transaction.execute(LOAD_RECORDS_SQL, [&records])?;
        let events = insert_loaded_records(&transaction)?;
        transaction.commit()?;
        Ok(events)
    }

    pub fn ingest_directory(&self, dir: &Path) -> Result<IngestSummary, IngestError> {
        let mut summary = IngestSummary {
            files: 0,
            skipped_files: 0,
            events: 0,
        };
        for file in log_files(dir)? {
            match self.ingest_file(&file)? {
                Some(events) => {
                    summary.files += 1;
                    summary.events += events;
                }
                None => summary.skipped_files += 1,
            }
        }
        Ok(summary)
    }

    fn ingest_file(&self, file: &Path) -> Result<Option<usize>, IngestError> {
        let content = fs::read(file).map_err(|source| IngestError::Io {
            path: file.to_path_buf(),
            source,
        })?;
        let sha256: String = Sha256::digest(&content)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let already_ingested: bool = self.connection.query_row(
            "SELECT count(*) > 0 FROM ingested_files WHERE sha256 = ?",
            [&sha256],
            |row| row.get(0),
        )?;
        if already_ingested {
            return Ok(None);
        }

        let uri = file.to_string_lossy();
        let transaction = self.connection.unchecked_transaction()?;
        transaction.execute(LOAD_FILE_SQL, [&uri])?;
        let events = insert_loaded_records(&transaction)?;
        transaction.execute(
            "INSERT INTO ingested_files VALUES (?, ?, ?, current_timestamp)",
            duckdb::params![sha256, uri, content.len()],
        )?;
        transaction.commit()?;
        Ok(Some(events))
    }
}

fn insert_loaded_records(transaction: &duckdb::Transaction<'_>) -> Result<usize, IngestError> {
    transaction.execute(STAGE_RECORDS_SQL, [])?;
    let events = transaction.execute(
        "INSERT INTO events_raw \
         SELECT json_extract_string(record, '$.eventID'), record FROM staged_records",
        [],
    )?;
    transaction.execute(NORMALIZE_EVENTS_SQL, [])?;
    transaction.execute_batch("DROP TABLE staged_records; DROP TABLE loaded_records;")?;
    Ok(events)
}

fn log_files(dir: &Path) -> Result<Vec<PathBuf>, IngestError> {
    let mut files = Vec::new();
    collect_log_files(dir, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_log_files(dir: &Path, files: &mut Vec<PathBuf>) -> Result<(), IngestError> {
    let io_error = |source| IngestError::Io {
        path: dir.to_path_buf(),
        source,
    };
    for entry in fs::read_dir(dir).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        let path = entry.path();
        if entry.file_type().map_err(io_error)?.is_dir() {
            if !EXCLUDED_DIRECTORIES
                .iter()
                .any(|name| entry.file_name() == *name)
            {
                collect_log_files(&path, files)?;
            }
        } else if path.to_string_lossy().ends_with(".json.gz") {
            files.push(path);
        }
    }
    Ok(())
}
