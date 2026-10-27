use std::path::PathBuf;
use std::sync::Arc;

use ashiato_pipeline::{
    ManifestEntry, PartitionId, PartitionManifest, PartitionWriter, Position, TransportError,
};
use async_trait::async_trait;
use chrono::TimeDelta;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerSettings {
    pub allowed_lateness: TimeDelta,
    pub max_message_size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionAssignment {
    pub index: u32,
    pub manifest: PartitionManifest,
    pub start: Position,
}

#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error("failed to read {path}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[async_trait]
pub trait FileSource: Send + Sync {
    async fn read(&self, entry: &ManifestEntry) -> Result<Vec<u8>, SourceError>;
}

pub struct LocalSource;

#[async_trait]
impl FileSource for LocalSource {
    async fn read(&self, entry: &ManifestEntry) -> Result<Vec<u8>, SourceError> {
        let path = PathBuf::from(&entry.uri);
        tokio::fs::read(&path)
            .await
            .map_err(|source| SourceError::Io { path, source })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("failed to read file {file_seq} ({uri}) of partition {partition}")]
    Source {
        partition: PartitionId,
        file_seq: u64,
        uri: String,
        #[source]
        source: SourceError,
    },
    #[error("failed to send a message of partition {partition}")]
    Transport {
        partition: PartitionId,
        #[source]
        source: TransportError,
    },
    #[error("an ingest task stopped unexpectedly")]
    Task(#[source] tokio::task::JoinError),
}

pub async fn run_worker(
    _assignments: Vec<PartitionAssignment>,
    _source: Arc<dyn FileSource>,
    _writer: Arc<dyn PartitionWriter>,
    _settings: WorkerSettings,
) -> Result<(), WorkerError> {
    todo!()
}
