use std::path::{Path, PathBuf};

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
    pub events: usize,
}

pub struct EventStore {
    connection: duckdb::Connection,
}

impl EventStore {
    pub fn open(path: &Path) -> Result<Self, IngestError> {
        let _ = path;
        todo!()
    }

    pub fn ingest_directory(&self, dir: &Path) -> Result<IngestSummary, IngestError> {
        let _ = (dir, &self.connection);
        todo!()
    }
}
