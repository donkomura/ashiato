use std::fs;
use std::path::{Path, PathBuf};

const SCHEMA_SQL: &str = include_str!("../sql/schema.sql");
const NORMALIZE_EVENTS_SQL: &str = include_str!("../sql/normalize_events.sql");

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

    pub fn ingest_directory(&self, dir: &Path) -> Result<IngestSummary, IngestError> {
        let files = log_files(dir)?;
        let mut events = 0;
        for file in &files {
            events += self.ingest_file(file)?;
        }
        Ok(IngestSummary {
            files: files.len(),
            skipped_files: 0,
            events,
        })
    }

    fn ingest_file(&self, file: &Path) -> Result<usize, IngestError> {
        self.connection.execute(
            "CREATE OR REPLACE TEMP TABLE staged_records AS \
             SELECT unnest(Records) AS record \
             FROM read_json(?, columns = {'Records': 'JSON[]'}, compression = 'gzip')",
            [file.to_string_lossy()],
        )?;
        let events = self.connection.execute(
            "INSERT INTO events_raw \
             SELECT json_extract_string(record, '$.eventID'), record FROM staged_records",
            [],
        )?;
        self.connection.execute(NORMALIZE_EVENTS_SQL, [])?;
        self.connection.execute("DROP TABLE staged_records", [])?;
        Ok(events)
    }
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
