use std::fs;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};
use std::thread;

use sha2::{Digest, Sha256};

mod backend;
pub mod fixture;

pub use backend::DuckDbBackend;

const SCHEMA_SQL: &str = include_str!("../sql/schema.sql");
const STAGE_RECORDS_SQL: &str = include_str!("../sql/stage_records.sql");
const NORMALIZE_EVENTS_SQL: &str = include_str!("../sql/normalize_events.sql");
// A log file is a single {"Records": [...]} object, and read_json rejects objects over
// 16 MiB by default. The flaws.cloud dataset has files of about 140 MB once decompressed,
// so the limit is raised to 256 MiB. DuckDB allocates twice the limit for each read, so a
// larger limit runs out of memory when several files are ingested in parallel.
const LOAD_FILE_SQL: &str = "CREATE OR REPLACE TEMP TABLE loaded_records AS \
     SELECT unnest(Records) AS record \
     FROM read_json(?, columns = {'Records': 'JSON[]'}, compression = 'gzip', \
     maximum_object_size = 268435456)";
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IngestSummary {
    pub files: usize,
    pub skipped_files: usize,
    pub events: usize,
}

impl IngestSummary {
    fn merge(self, other: Self) -> Self {
        Self {
            files: self.files + other.files,
            skipped_files: self.skipped_files + other.skipped_files,
            events: self.events + other.events,
        }
    }
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
        self.ingest_directory_with_jobs(dir, NonZeroUsize::MIN)
    }

    pub fn ingest_directory_with_jobs(
        &self,
        dir: &Path,
        jobs: NonZeroUsize,
    ) -> Result<IngestSummary, IngestError> {
        let files = log_files(dir)?;
        let connections = (0..jobs.get().min(files.len()))
            .map(|_| self.connection.try_clone())
            .collect::<Result<Vec<_>, _>>()?;
        let (files, next_file, write_lock) = (&files, &AtomicUsize::new(0), &Mutex::new(()));
        thread::scope(|scope| {
            let workers: Vec<_> = connections
                .into_iter()
                .map(|connection| {
                    scope.spawn(move || {
                        let mut summary = IngestSummary::default();
                        while let Some(file) = files.get(next_file.fetch_add(1, Ordering::Relaxed))
                        {
                            match ingest_file(&connection, file, write_lock) {
                                Ok(Some(events)) => {
                                    summary.files += 1;
                                    summary.events += events;
                                }
                                Ok(None) => summary.skipped_files += 1,
                                Err(error) => {
                                    next_file.store(files.len(), Ordering::Relaxed);
                                    return Err(error);
                                }
                            }
                        }
                        Ok(summary)
                    })
                })
                .collect();
            workers
                .into_iter()
                .try_fold(IngestSummary::default(), |total, worker| {
                    let summary = worker.join().expect("an ingest worker panicked")?;
                    Ok(total.merge(summary))
                })
        })
    }
}

fn ingest_file(
    connection: &duckdb::Connection,
    file: &Path,
    write_lock: &Mutex<()>,
) -> Result<Option<usize>, IngestError> {
    let content = fs::read(file).map_err(|source| IngestError::Io {
        path: file.to_path_buf(),
        source,
    })?;
    let sha256: String = Sha256::digest(&content)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if is_ingested(connection, &sha256)? {
        return Ok(None);
    }

    let uri = file.to_string_lossy();
    connection.execute(LOAD_FILE_SQL, [&uri])?;
    let _write_guard = write_lock.lock().unwrap_or_else(PoisonError::into_inner);
    if is_ingested(connection, &sha256)? {
        connection.execute_batch("DROP TABLE loaded_records;")?;
        return Ok(None);
    }
    let transaction = connection.unchecked_transaction()?;
    let events = insert_loaded_records(&transaction)?;
    transaction.execute(
        "INSERT INTO ingested_files VALUES (?, ?, ?, current_timestamp)",
        duckdb::params![sha256, uri, content.len()],
    )?;
    transaction.commit()?;
    Ok(Some(events))
}

fn is_ingested(connection: &duckdb::Connection, sha256: &str) -> Result<bool, IngestError> {
    Ok(connection.query_row(
        "SELECT count(*) > 0 FROM ingested_files WHERE sha256 = ?",
        [sha256],
        |row| row.get(0),
    )?)
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
