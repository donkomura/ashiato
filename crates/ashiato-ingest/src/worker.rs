use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::RecordBatch;
use ashiato_pipeline::{
    EventTime, IngestedFile, ManifestEntry, PartitionId, PartitionManifest, PartitionMessage,
    PartitionWriter, Position, TransportError, events_schema, file_id, file_watermark,
};
use async_trait::async_trait;
use chrono::TimeDelta;
use tokio::sync::Semaphore;
use tokio::task::{JoinHandle, JoinSet};

use crate::normalize::{
    ContentError, EventRow, FileDigest, decompress, estimated_size, normalize_records, record_batch,
};

// Leaves room in each message for the headers that describe the chunk (5.3 節).
const HEADER_ALLOWANCE_BYTES: usize = 4096;

pub const DEFAULT_PREFETCH_FILES: NonZeroUsize = NonZeroUsize::new(16).unwrap();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerSettings {
    pub allowed_lateness: TimeDelta,
    pub max_message_size: usize,
    pub decode_threads: NonZeroUsize,
    pub prefetch_files: NonZeroUsize,
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
    #[error(transparent)]
    S3(Box<crate::s3::S3Error>),
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

// The decode threads are taken from the blocking pool of the runtime, and a semaphore with
// decode_threads permits lets only that many files of the worker be decoded at once, so that the
// partitions of a worker share decode_threads threads (6.1 節).
pub async fn run_worker(
    assignments: Vec<PartitionAssignment>,
    source: Arc<dyn FileSource>,
    writer: Arc<dyn PartitionWriter>,
    settings: WorkerSettings,
) -> Result<(), WorkerError> {
    let decoders = Arc::new(Semaphore::new(settings.decode_threads.get()));
    let mut tasks = JoinSet::new();
    for assignment in assignments {
        tasks.spawn(ingest_partition(
            assignment,
            Arc::clone(&source),
            Arc::clone(&writer),
            Arc::clone(&decoders),
            settings,
        ));
    }
    while let Some(result) = tasks.join_next().await {
        result.map_err(WorkerError::Task)??;
    }
    Ok(())
}

pub async fn run_workers(
    workers: Vec<Vec<PartitionAssignment>>,
    source: Arc<dyn FileSource>,
    writer: Arc<dyn PartitionWriter>,
    settings: WorkerSettings,
) -> Result<(), WorkerError> {
    let mut tasks = JoinSet::new();
    for assignments in workers {
        tasks.spawn(run_worker(
            assignments,
            Arc::clone(&source),
            Arc::clone(&writer),
            settings,
        ));
    }
    // Returning on the first error drops the other workers and their writers. Otherwise the
    // writers they hold would keep the channels of the failed partitions open, and the analyzer
    // would wait for those partitions forever.
    while let Some(result) = tasks.join_next().await {
        result.map_err(WorkerError::Task)??;
    }
    Ok(())
}

struct DecodedFile {
    digest: FileDigest,
    rows: Result<Vec<EventRow>, ContentError>,
}

type FileTask = JoinHandle<Result<DecodedFile, WorkerError>>;

struct Prefetched<'a>(VecDeque<(&'a ManifestEntry, FileTask)>);

// Stops the reads and decodes still in flight when the partition stops with an error.
impl Drop for Prefetched<'_> {
    fn drop(&mut self) {
        for (_, task) in &self.0 {
            task.abort();
        }
    }
}

async fn ingest_partition(
    assignment: PartitionAssignment,
    source: Arc<dyn FileSource>,
    writer: Arc<dyn PartitionWriter>,
    decoders: Arc<Semaphore>,
    settings: WorkerSettings,
) -> Result<(), WorkerError> {
    let partition = &assignment.manifest.partition;
    let send = |message| {
        let writer = &writer;
        async move {
            writer
                .put(message)
                .await
                .map_err(|source| WorkerError::Transport {
                    partition: partition.clone(),
                    source,
                })
        }
    };
    let mut entries = assignment
        .manifest
        .files
        .iter()
        .filter(|entry| entry.file_seq >= assignment.start.file_seq);
    let mut prefetched = Prefetched(VecDeque::new());
    loop {
        // A slot is freed only after the messages of its file are sent, and the messages are
        // sent here rather than on a decode thread. A partition whose sends wait thus holds at
        // most prefetch_files files and no decode thread (streaming-implementation-plan.md 4.9 節).
        while prefetched.0.len() < settings.prefetch_files.get() {
            let Some(entry) = entries.next() else {
                break;
            };
            let task = tokio::spawn(fetch_and_decode(
                partition.clone(),
                entry.clone(),
                Arc::clone(&source),
                Arc::clone(&decoders),
            ));
            prefetched.0.push_back((entry, task));
        }
        let Some((entry, task)) = prefetched.0.pop_front() else {
            break;
        };
        let DecodedFile { digest, rows } = task.await.map_err(WorkerError::Task)??;
        let watermark = file_watermark(entry.file_time, settings.allowed_lateness);
        let skip = if entry.file_seq == assignment.start.file_seq {
            assignment.start.record_offset
        } else {
            0
        };
        let messages = match rows {
            Ok(rows) => file_chunks(
                &assignment,
                entry,
                &digest,
                &rows,
                skip,
                watermark,
                settings,
            ),
            Err(error) => vec![PartitionMessage::FileError {
                partition: partition.clone(),
                position: Position::new(entry.file_seq, 0),
                file: ingested_file(entry, &digest, 0),
                reason: error_chain(&error),
                watermark,
            }],
        };
        for message in messages {
            send(message).await?;
        }
    }
    let file_count = assignment.manifest.files.len() as u64;
    send(PartitionMessage::EndOfPartition {
        partition: partition.clone(),
        position: Position::new(file_count, 0),
    })
    .await
}

async fn fetch_and_decode(
    partition: PartitionId,
    entry: ManifestEntry,
    source: Arc<dyn FileSource>,
    decoders: Arc<Semaphore>,
) -> Result<DecodedFile, WorkerError> {
    let content = source
        .read(&entry)
        .await
        .map_err(|source| WorkerError::Source {
            partition,
            file_seq: entry.file_seq,
            uri: entry.uri.clone(),
            source,
        })?;
    let permit = decoders
        .acquire_owned()
        .await
        .expect("the semaphore of the decode threads is never closed");
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        DecodedFile {
            digest: FileDigest::of(&content),
            rows: decompress(&content).and_then(|text| normalize_records(&text)),
        }
    })
    .await
    .map_err(WorkerError::Task)
}

fn file_chunks(
    assignment: &PartitionAssignment,
    entry: &ManifestEntry,
    digest: &FileDigest,
    rows: &[EventRow],
    skip: u64,
    watermark: EventTime,
    settings: WorkerSettings,
) -> Vec<PartitionMessage> {
    let file = ingested_file(entry, digest, rows.len() as u64);
    let chunk = |offset: usize, events: RecordBatch| PartitionMessage::FileChunk {
        partition: assignment.manifest.partition.clone(),
        position: Position::new(entry.file_seq, offset as u64),
        record_count: u32::try_from(events.num_rows()).expect("a chunk fits in a message"),
        file: file.clone(),
        events,
        watermark,
    };
    if rows.is_empty() {
        return vec![chunk(0, RecordBatch::new_empty(events_schema()))];
    }
    let id = file_id(assignment.index, entry.file_seq);
    let budget = settings
        .max_message_size
        .saturating_sub(HEADER_ALLOWANCE_BYTES)
        .max(1);
    let mut messages = Vec::new();
    let mut start = usize::try_from(skip).unwrap_or(usize::MAX).min(rows.len());
    while start < rows.len() {
        let mut end = start + 1;
        let mut size = estimated_size(&rows[start]);
        while end < rows.len() && size + estimated_size(&rows[end]) <= budget {
            size += estimated_size(&rows[end]);
            end += 1;
        }
        messages.push(chunk(
            start,
            record_batch(&rows[start..end], id, start as u64),
        ));
        start = end;
    }
    messages
}

fn ingested_file(entry: &ManifestEntry, digest: &FileDigest, record_count: u64) -> IngestedFile {
    IngestedFile {
        uri: entry.uri.clone(),
        version_id: entry.version_id.clone(),
        etag: entry.etag.clone(),
        size: digest.size,
        sha256: digest.sha256.clone(),
        file_time: entry.file_time,
        record_count,
    }
}

fn error_chain(error: &ContentError) -> String {
    let mut message = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}
