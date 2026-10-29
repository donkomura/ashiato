use std::io::Write;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use ashiato_ingest::{
    DEFAULT_PREFETCH_FILES, FileSource, LocalSource, PartitionAssignment, SourceError, WorkerError,
    WorkerSettings, run_worker, run_workers,
};
use ashiato_pipeline::{
    ChannelReader, ChannelWriter, ManifestEntry, PartitionId, PartitionManifest, PartitionMessage,
    PartitionReader, PartitionWriter, Position, TransportError, WorkerMetrics, file_id,
    partition_channels,
};
use ashiato_testkit::record::RecordBuilder;
use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::sync::{Semaphore, mpsc};

const LATENESS_MINUTES: i64 = 30;
const PATIENCE: Duration = Duration::from_secs(10);

fn settings(max_message_size: usize) -> WorkerSettings {
    WorkerSettings {
        allowed_lateness: TimeDelta::minutes(LATENESS_MINUTES),
        max_message_size,
        decode_threads: NonZeroUsize::new(2).unwrap(),
        prefetch_files: DEFAULT_PREFETCH_FILES,
    }
}

fn parallel_settings(decode_threads: usize, prefetch_files: usize) -> WorkerSettings {
    WorkerSettings {
        decode_threads: NonZeroUsize::new(decode_threads).unwrap(),
        prefetch_files: NonZeroUsize::new(prefetch_files).unwrap(),
        ..settings(1 << 20)
    }
}

fn file_time(minute: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 1, 0, minute, 0).unwrap()
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn records(count: usize) -> Vec<Value> {
    (0..count)
        .map(|index| {
            RecordBuilder::new()
                .set("eventID", format!("event-{index:04}"))
                .set("requestParameters", json!({ "padding": "x".repeat(200) }))
                .build()
        })
        .collect()
}

struct Files {
    dir: TempDir,
    entries: Vec<ManifestEntry>,
}

impl Files {
    fn new() -> Self {
        Self {
            dir: TempDir::new().unwrap(),
            entries: Vec::new(),
        }
    }

    fn add_bytes(&mut self, name: &str, content: &[u8]) -> PathBuf {
        let path = self.dir.path().join(name);
        std::fs::write(&path, content).unwrap();
        let file_seq = self.entries.len() as u64;
        self.entries.push(ManifestEntry {
            file_seq,
            uri: path.to_string_lossy().into_owned(),
            version_id: None,
            etag: None,
            size: content.len() as u64,
            file_time: file_time(5 * file_seq as u32),
        });
        path
    }

    fn add_records(&mut self, name: &str, records: Vec<Value>) -> PathBuf {
        let content = gzip(json!({ "Records": records }).to_string().as_bytes());
        self.add_bytes(name, &content)
    }

    fn assignment(
        &self,
        index: u32,
        partition: &PartitionId,
        start: Position,
    ) -> PartitionAssignment {
        PartitionAssignment {
            index,
            manifest: PartitionManifest {
                partition: partition.clone(),
                files: self.entries.clone(),
            },
            start,
        }
    }
}

fn partition(region: &str) -> PartitionId {
    PartitionId::new("111111111111", region)
}

async fn ingest(
    assignments: Vec<PartitionAssignment>,
    settings: WorkerSettings,
) -> Result<Vec<PartitionMessage>, WorkerError> {
    ingest_from(
        assignments,
        Arc::new(LocalSource),
        4,
        |writer| Arc::new(writer),
        settings,
    )
    .await
}

async fn ingest_from(
    assignments: Vec<PartitionAssignment>,
    source: Arc<dyn FileSource>,
    capacity: usize,
    writer: impl FnOnce(ChannelWriter) -> Arc<dyn PartitionWriter>,
    settings: WorkerSettings,
) -> Result<Vec<PartitionMessage>, WorkerError> {
    let partitions: Vec<PartitionId> = assignments
        .iter()
        .map(|assignment| assignment.manifest.partition.clone())
        .collect();
    let (channel, reader) = partition_channels(partitions.clone(), capacity);
    let worker = tokio::spawn(run_worker(
        assignments,
        source,
        writer(channel),
        settings,
        Arc::default(),
    ));
    let messages = read_to_end(reader, &partitions).await;
    worker.await.unwrap()?;
    Ok(messages)
}

async fn read_to_end(
    mut reader: ChannelReader,
    partitions: &[PartitionId],
) -> Vec<PartitionMessage> {
    let mut messages = Vec::new();
    for partition in partitions {
        loop {
            let deliveries = reader.fetch(partition, 16).await.unwrap();
            if deliveries.is_empty() {
                break;
            }
            let ended = deliveries.iter().any(|delivery| {
                matches!(delivery.message, PartitionMessage::EndOfPartition { .. })
            });
            messages.extend(deliveries.into_iter().map(|delivery| delivery.message));
            if ended {
                break;
            }
        }
    }
    messages
}

fn event_ids(message: &PartitionMessage) -> Vec<String> {
    let PartitionMessage::FileChunk { events, .. } = message else {
        return Vec::new();
    };
    events
        .column_by_name("event_id")
        .unwrap()
        .as_string::<i32>()
        .iter()
        .map(|id| id.unwrap().to_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn chunks_continue_each_other_up_to_the_record_count_of_the_file() {
    let east = partition("us-east-1");
    let mut files = Files::new();
    files.add_records("a.json.gz", records(50));

    let messages = ingest(
        vec![files.assignment(2, &east, Position::START)],
        settings(8 * 1024),
    )
    .await
    .unwrap();

    let chunks: Vec<&PartitionMessage> = messages
        .iter()
        .filter(|message| matches!(message, PartitionMessage::FileChunk { .. }))
        .collect();
    assert!(chunks.len() > 1, "a small max_message_size splits the file");
    let mut next = Position::START;
    for chunk in &chunks {
        let PartitionMessage::FileChunk {
            position,
            record_count,
            events,
            file,
            ..
        } = chunk
        else {
            unreachable!()
        };
        assert_eq!(*position, next);
        assert_eq!(events.num_rows(), *record_count as usize);
        let indexes: Vec<i64> = events
            .column_by_name("record_index")
            .unwrap()
            .as_primitive::<Int64Type>()
            .values()
            .to_vec();
        let expected: Vec<i64> = (position.record_offset as i64..)
            .take(*record_count as usize)
            .collect();
        assert_eq!(indexes, expected);
        let ids = events
            .column_by_name("file_id")
            .unwrap()
            .as_primitive::<Int64Type>();
        assert!(ids.values().iter().all(|id| *id == file_id(2, 0)));
        assert_eq!(file.record_count, 50);
        next = chunk.end_position();
    }
    assert_eq!(next, Position::new(0, 50));
    let all: Vec<String> = chunks.iter().flat_map(|chunk| event_ids(chunk)).collect();
    let expected: Vec<String> = (0..50).map(|index| format!("event-{index:04}")).collect();
    assert_eq!(all, expected);
    assert_eq!(
        messages.last().unwrap(),
        &PartitionMessage::EndOfPartition {
            partition: east,
            position: Position::new(1, 0),
        }
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_is_described_by_its_digest_and_watermark() {
    let east = partition("us-east-1");
    let mut files = Files::new();
    let path = files.add_records("a.json.gz", records(3));
    let content = std::fs::read(&path).unwrap();

    let messages = ingest(
        vec![files.assignment(0, &east, Position::START)],
        settings(1 << 20),
    )
    .await
    .unwrap();

    let PartitionMessage::FileChunk {
        file, watermark, ..
    } = &messages[0]
    else {
        panic!("expected a chunk, but got {:?}", messages[0]);
    };
    let sha256: String = Sha256::digest(&content)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(file.sha256, sha256);
    assert_eq!(file.size, content.len() as u64);
    assert_eq!(file.uri, path.to_string_lossy());
    assert_eq!(file.file_time, file_time(0));
    assert_eq!(file.record_count, 3);
    assert_eq!(
        *watermark,
        file_time(0) - TimeDelta::minutes(LATENESS_MINUTES)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_without_records_is_sent_as_one_empty_chunk() {
    let east = partition("us-east-1");
    let mut files = Files::new();
    files.add_records("empty.json.gz", Vec::new());

    let messages = ingest(
        vec![files.assignment(0, &east, Position::START)],
        settings(1 << 20),
    )
    .await
    .unwrap();

    assert_eq!(messages.len(), 2);
    let PartitionMessage::FileChunk {
        position,
        record_count,
        events,
        file,
        ..
    } = &messages[0]
    else {
        panic!("expected a chunk, but got {:?}", messages[0]);
    };
    assert_eq!(*position, Position::START);
    assert_eq!(*record_count, 0);
    assert_eq!(events.num_rows(), 0);
    assert_eq!(file.record_count, 0);
    assert_eq!(messages[1].position(), Position::new(1, 0));
}

#[tokio::test(flavor = "multi_thread")]
async fn broken_files_are_reported_as_file_errors_and_the_partition_continues() {
    let east = partition("us-east-1");
    let mut files = Files::new();
    files.add_bytes("broken-gzip.json.gz", b"not a gzip stream");
    files.add_bytes("broken-json.json.gz", &gzip(b"{\"Records\": [ {"));
    files.add_bytes("no-records.json.gz", &gzip(b"{\"Events\": []}"));
    files.add_bytes("records-not-array.json.gz", &gzip(b"{\"Records\": 1}"));
    files.add_records("good.json.gz", records(1));

    let messages = ingest(
        vec![files.assignment(0, &east, Position::START)],
        settings(1 << 20),
    )
    .await
    .unwrap();

    let kinds: Vec<(&str, Position)> = messages
        .iter()
        .map(|message| {
            let kind = match message {
                PartitionMessage::FileChunk { .. } => "chunk",
                PartitionMessage::FileError { .. } => "error",
                PartitionMessage::EndOfPartition { .. } => "end",
            };
            (kind, message.position())
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            ("error", Position::new(0, 0)),
            ("error", Position::new(1, 0)),
            ("error", Position::new(2, 0)),
            ("error", Position::new(3, 0)),
            ("chunk", Position::new(4, 0)),
            ("end", Position::new(5, 0)),
        ]
    );
    let PartitionMessage::FileError { reason, file, .. } = &messages[0] else {
        unreachable!()
    };
    assert!(!reason.is_empty());
    assert_eq!(file.record_count, 0);
    assert_eq!(file.size, b"not a gzip stream".len() as u64);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_listed_file_missing_when_it_is_read_stops_the_worker() {
    let east = partition("us-east-1");
    let mut files = Files::new();
    let path = files.add_records("a.json.gz", records(1));
    std::fs::remove_file(&path).unwrap();

    let error = ingest(
        vec![files.assignment(0, &east, Position::START)],
        settings(1 << 20),
    )
    .await
    .unwrap_err();

    assert!(
        matches!(&error, WorkerError::Source { partition, file_seq: 0, .. } if *partition == east),
        "{error:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn resuming_in_the_middle_of_a_file_sends_only_the_records_from_there() {
    let east = partition("us-east-1");
    let mut files = Files::new();
    files.add_records("a.json.gz", records(4));
    files.add_records("b.json.gz", records(10));

    let messages = ingest(
        vec![files.assignment(0, &east, Position::new(1, 7))],
        settings(1 << 20),
    )
    .await
    .unwrap();

    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].position(), Position::new(1, 7));
    assert_eq!(
        event_ids(&messages[0]),
        vec!["event-0007", "event-0008", "event-0009"]
    );
    assert_eq!(messages[1].position(), Position::new(2, 0));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_partition_without_files_only_ends() {
    let east = partition("us-east-1");
    let files = Files::new();

    let messages = ingest(
        vec![files.assignment(0, &east, Position::START)],
        settings(1 << 20),
    )
    .await
    .unwrap();

    assert_eq!(
        messages,
        vec![PartitionMessage::EndOfPartition {
            partition: east,
            position: Position::START,
        }]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_partition_waiting_for_its_reader_does_not_hold_back_the_others() {
    let east = partition("us-east-1");
    let west = partition("us-west-2");
    let mut east_files = Files::new();
    let mut west_files = Files::new();
    for index in 0..4 {
        east_files.add_records(&format!("east-{index}.json.gz"), records(1));
        west_files.add_records(&format!("west-{index}.json.gz"), records(1));
    }
    let (writer, mut reader) = partition_channels([east.clone(), west.clone()], 1);
    let _worker = tokio::spawn(run_worker(
        vec![
            east_files.assignment(0, &east, Position::START),
            west_files.assignment(1, &west, Position::START),
        ],
        Arc::new(LocalSource) as Arc<dyn FileSource>,
        Arc::new(writer),
        settings(1 << 20),
        Arc::default(),
    ));

    let mut west_messages = Vec::new();
    while !matches!(
        west_messages.last(),
        Some(PartitionMessage::EndOfPartition { .. })
    ) {
        let deliveries =
            tokio::time::timeout(std::time::Duration::from_secs(10), reader.fetch(&west, 1))
                .await
                .expect("the other partition must keep going while one partition is not read")
                .unwrap();
        assert!(
            !deliveries.is_empty(),
            "the worker stopped before the end of the partition"
        );
        west_messages.extend(deliveries.into_iter().map(|delivery| delivery.message));
    }
    assert_eq!(west_messages.len(), 5);
}

// Holds the read of each file until the test opens its gate, and reports the files whose read
// has finished, so that a test decides the order in which the reads finish.
struct GatedSource {
    gates: Vec<Semaphore>,
    finished: mpsc::UnboundedSender<u64>,
}

impl GatedSource {
    fn new(file_count: usize) -> (Arc<Self>, mpsc::UnboundedReceiver<u64>) {
        let (finished, receiver) = mpsc::unbounded_channel();
        let gates = (0..file_count).map(|_| Semaphore::new(0)).collect();
        (Arc::new(Self { gates, finished }), receiver)
    }

    fn open(&self, file_seq: u64) {
        self.gates[usize::try_from(file_seq).unwrap()].add_permits(1);
    }
}

#[async_trait]
impl FileSource for GatedSource {
    async fn read(&self, entry: &ManifestEntry) -> Result<Vec<u8>, SourceError> {
        self.gates[usize::try_from(entry.file_seq).unwrap()]
            .acquire()
            .await
            .unwrap()
            .forget();
        let content = LocalSource.read(entry).await;
        self.finished.send(entry.file_seq).unwrap();
        content
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn files_whose_reads_finish_in_reverse_order_are_sent_in_the_order_of_the_files() {
    let east = partition("us-east-1");
    let mut files = Files::new();
    for index in 0..4 {
        files.add_records(&format!("{index}.json.gz"), records(index + 1));
    }
    let (source, mut finished) = GatedSource::new(4);

    let opener = {
        let source = Arc::clone(&source);
        tokio::spawn(async move {
            let mut order = Vec::new();
            for file_seq in (0..4).rev() {
                source.open(file_seq);
                let done = tokio::time::timeout(PATIENCE, finished.recv())
                    .await
                    .expect("the worker must read the later files before the earlier ones end")
                    .unwrap();
                order.push(done);
            }
            order
        })
    };
    let messages = tokio::time::timeout(
        PATIENCE,
        ingest_from(
            vec![files.assignment(0, &east, Position::START)],
            source,
            4,
            |writer| Arc::new(writer),
            parallel_settings(4, 4),
        ),
    )
    .await
    .expect("the worker must finish")
    .unwrap();

    assert_eq!(opener.await.unwrap(), vec![3, 2, 1, 0]);
    let positions: Vec<Position> = messages.iter().map(PartitionMessage::position).collect();
    assert_eq!(
        positions,
        vec![
            Position::new(0, 0),
            Position::new(1, 0),
            Position::new(2, 0),
            Position::new(3, 0),
            Position::new(4, 0),
        ]
    );
    let counts: Vec<usize> = messages
        .iter()
        .map(|message| event_ids(message).len())
        .collect();
    assert_eq!(counts, vec![1, 2, 3, 4, 0]);
}

// Records the files whose messages have been sent, so that a source can check what was sent
// before each read starts.
struct RecordingWriter {
    inner: ChannelWriter,
    sent: Arc<Mutex<Vec<u64>>>,
}

#[async_trait]
impl PartitionWriter for RecordingWriter {
    async fn put(&self, message: PartitionMessage) -> Result<(), TransportError> {
        let file_seq = message.position().file_seq;
        self.inner.put(message).await?;
        self.sent.lock().unwrap().push(file_seq);
        Ok(())
    }
}

struct PrefetchCheckingSource {
    prefetch: u64,
    sent: Arc<Mutex<Vec<u64>>>,
    early_reads: Mutex<Vec<u64>>,
}

#[async_trait]
impl FileSource for PrefetchCheckingSource {
    async fn read(&self, entry: &ManifestEntry) -> Result<Vec<u8>, SourceError> {
        if let Some(released) = entry.file_seq.checked_sub(self.prefetch)
            && !self.sent.lock().unwrap().contains(&released)
        {
            self.early_reads.lock().unwrap().push(entry.file_seq);
        }
        LocalSource.read(entry).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_is_read_only_after_the_file_that_many_files_before_it_has_been_sent() {
    let east = partition("us-east-1");
    let mut files = Files::new();
    for index in 0..12 {
        files.add_records(&format!("{index:02}.json.gz"), records(1));
    }
    let sent = Arc::new(Mutex::new(Vec::new()));
    let source = Arc::new(PrefetchCheckingSource {
        prefetch: 3,
        sent: Arc::clone(&sent),
        early_reads: Mutex::new(Vec::new()),
    });

    let messages = ingest_from(
        vec![files.assignment(0, &east, Position::START)],
        Arc::clone(&source) as Arc<dyn FileSource>,
        1,
        |inner| Arc::new(RecordingWriter { inner, sent }),
        parallel_settings(4, 3),
    )
    .await
    .unwrap();

    assert_eq!(messages.len(), 13);
    assert_eq!(*source.early_reads.lock().unwrap(), Vec::<u64>::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn with_one_decode_thread_a_partition_keeps_going_while_the_others_are_not_read() {
    let stalled = [partition("us-east-1"), partition("us-west-2")];
    let north = partition("eu-north-1");
    let mut stalled_files = Files::new();
    let mut north_files = Files::new();
    for index in 0..6 {
        stalled_files.add_records(&format!("stalled-{index}.json.gz"), records(1));
        north_files.add_records(&format!("north-{index}.json.gz"), records(1));
    }
    let (writer, mut reader) =
        partition_channels([stalled[0].clone(), stalled[1].clone(), north.clone()], 1);
    let _worker = tokio::spawn(run_worker(
        vec![
            stalled_files.assignment(0, &stalled[0], Position::START),
            stalled_files.assignment(1, &stalled[1], Position::START),
            north_files.assignment(2, &north, Position::START),
        ],
        Arc::new(LocalSource) as Arc<dyn FileSource>,
        Arc::new(writer),
        parallel_settings(1, 2),
        Arc::default(),
    ));

    let mut north_messages = Vec::new();
    while !matches!(
        north_messages.last(),
        Some(PartitionMessage::EndOfPartition { .. })
    ) {
        let deliveries = tokio::time::timeout(PATIENCE, reader.fetch(&north, 1))
            .await
            .expect("the shared decode thread must not wait for the full channels")
            .unwrap();
        assert!(
            !deliveries.is_empty(),
            "the worker stopped before the end of the partition"
        );
        north_messages.extend(deliveries.into_iter().map(|delivery| delivery.message));
    }
    assert_eq!(north_messages.len(), 7);
}

#[tokio::test(flavor = "multi_thread")]
async fn several_workers_send_the_same_messages_as_one_worker() {
    let regions = ["ap-northeast-1", "eu-west-1", "us-east-1"];
    let partitions: Vec<PartitionId> = regions.iter().map(|region| partition(region)).collect();
    let mut files = Files::new();
    for index in 0..5 {
        files.add_records(&format!("{index}.json.gz"), records(index * 7 % 5));
    }
    files.add_bytes("broken.json.gz", b"not a gzip stream");
    let assignment =
        |index: usize| files.assignment(index as u32, &partitions[index], Position::START);

    let (writer, reader) = partition_channels(partitions.clone(), 2);
    let one = tokio::spawn(run_worker(
        (0..3).map(assignment).collect(),
        Arc::new(LocalSource) as Arc<dyn FileSource>,
        Arc::new(writer),
        settings(8 * 1024),
        Arc::default(),
    ));
    let expected = read_to_end(reader, &partitions).await;
    one.await.unwrap().unwrap();

    let (writer, reader) = partition_channels(partitions.clone(), 2);
    let several = tokio::spawn(run_workers(
        vec![
            (vec![assignment(2), assignment(0)], Arc::default()),
            (vec![assignment(1)], Arc::default()),
            (vec![], Arc::default()),
        ],
        Arc::new(LocalSource) as Arc<dyn FileSource>,
        Arc::new(writer),
        settings(8 * 1024),
    ));
    let messages = read_to_end(reader, &partitions).await;
    several.await.unwrap().unwrap();

    assert_eq!(messages.len(), expected.len());
    assert_eq!(messages, expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_worker_that_fails_stops_the_other_workers() {
    let failing = partition("us-east-1");
    let waiting = partition("us-west-2");
    let mut missing = Files::new();
    let path = missing.add_records("missing.json.gz", records(1));
    std::fs::remove_file(&path).unwrap();
    let mut many = Files::new();
    for index in 0..6 {
        many.add_records(&format!("{index}.json.gz"), records(1));
    }
    let (writer, _reader) = partition_channels([failing.clone(), waiting.clone()], 1);

    let error = tokio::time::timeout(
        PATIENCE,
        run_workers(
            vec![
                (
                    vec![missing.assignment(0, &failing, Position::START)],
                    Arc::default(),
                ),
                (
                    vec![many.assignment(1, &waiting, Position::START)],
                    Arc::default(),
                ),
            ],
            Arc::new(LocalSource) as Arc<dyn FileSource>,
            Arc::new(writer),
            settings(1 << 20),
        ),
    )
    .await
    .expect("the workers must stop when one of them fails")
    .unwrap_err();

    assert!(
        matches!(&error, WorkerError::Source { partition, .. } if *partition == failing),
        "{error:?}"
    );
}

async fn ingest_measured(
    assignments: Vec<PartitionAssignment>,
    metrics: &Arc<WorkerMetrics>,
) -> Vec<PartitionMessage> {
    let partitions: Vec<PartitionId> = assignments
        .iter()
        .map(|assignment| assignment.manifest.partition.clone())
        .collect();
    let (writer, reader) = partition_channels(partitions.clone(), 4);
    let worker = tokio::spawn(run_worker(
        assignments,
        Arc::new(LocalSource) as Arc<dyn FileSource>,
        Arc::new(writer),
        settings(1 << 20),
        Arc::clone(metrics),
    ));
    let messages = read_to_end(reader, &partitions).await;
    worker.await.unwrap().unwrap();
    messages
}

fn partition_counts(metrics: &WorkerMetrics) -> Vec<(String, u64, u64, u64)> {
    metrics.line(0, true)["partitions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|partition| {
            (
                partition["partition"].as_str().unwrap().to_owned(),
                partition["files"].as_u64().unwrap(),
                partition["bytes"].as_u64().unwrap(),
                partition["events"].as_u64().unwrap(),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_worker_counts_the_files_bytes_and_events_it_sent_for_each_partition() {
    let (east, west) = (partition("us-east-1"), partition("us-west-2"));
    let mut files = Files::new();
    files.add_records("a.json.gz", records(4));
    files.add_records("b.json.gz", records(3));
    files.add_bytes("broken.json.gz", b"not a gzip stream");
    let bytes: u64 = files.entries.iter().map(|entry| entry.size).sum();
    let metrics = Arc::new(WorkerMetrics::new(2));

    ingest_measured(
        vec![
            files.assignment(0, &east, Position::START),
            Files::new().assignment(1, &west, Position::START),
        ],
        &metrics,
    )
    .await;

    assert_eq!(
        partition_counts(&metrics),
        [
            ("111111111111/us-east-1".to_owned(), 3, bytes, 7),
            ("111111111111/us-west-2".to_owned(), 0, 0, 0),
        ]
    );
    assert!(metrics.fetch_wait.get() > Duration::ZERO);
    assert!(metrics.decode.get() > Duration::ZERO);
    assert_eq!(metrics.line(0, true)["worker_id"], 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resumed_worker_counts_only_the_files_and_events_it_sent_in_this_start() {
    let east = partition("us-east-1");
    let mut files = Files::new();
    files.add_records("a.json.gz", records(4));
    files.add_records("b.json.gz", records(10));
    files.add_records("c.json.gz", records(2));
    let bytes = files.entries[1].size + files.entries[2].size;
    let metrics = Arc::new(WorkerMetrics::new(0));

    ingest_measured(
        vec![files.assignment(0, &east, Position::new(1, 7))],
        &metrics,
    )
    .await;

    assert_eq!(
        partition_counts(&metrics),
        [("111111111111/us-east-1".to_owned(), 2, bytes, 5)]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_wait_of_a_full_channel_is_counted_as_rejected() {
    let east = partition("us-east-1");
    let mut files = Files::new();
    for index in 0..4 {
        files.add_records(&format!("{index}.json.gz"), records(1));
    }
    let (writer, mut reader) = partition_channels([east.clone()], 1);
    let metrics = Arc::new(WorkerMetrics::new(0));
    let worker = tokio::spawn(run_worker(
        vec![files.assignment(0, &east, Position::START)],
        Arc::new(LocalSource) as Arc<dyn FileSource>,
        Arc::new(writer),
        settings(1 << 20),
        Arc::clone(&metrics),
    ));

    tokio::time::sleep(Duration::from_millis(200)).await;
    while !reader.fetch(&east, 16).await.unwrap().is_empty() {}
    worker.await.unwrap().unwrap();

    assert!(
        metrics.send.rejected_wait.get() >= Duration::from_millis(150),
        "{metrics:?}"
    );
}
