use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use ashiato_ingest::{
    FileSource, LocalSource, PartitionAssignment, WorkerError, WorkerSettings, run_worker,
};
use ashiato_pipeline::{
    ManifestEntry, PartitionId, PartitionManifest, PartitionMessage, PartitionReader, Position,
    file_id, partition_channels,
};
use ashiato_testkit::record::RecordBuilder;
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const LATENESS_MINUTES: i64 = 30;

fn settings(max_message_size: usize) -> WorkerSettings {
    WorkerSettings {
        allowed_lateness: TimeDelta::minutes(LATENESS_MINUTES),
        max_message_size,
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
    let partitions: Vec<PartitionId> = assignments
        .iter()
        .map(|assignment| assignment.manifest.partition.clone())
        .collect();
    let (writer, mut reader) = partition_channels(partitions.clone(), 4);
    let worker = tokio::spawn(run_worker(
        assignments,
        Arc::new(LocalSource) as Arc<dyn FileSource>,
        Arc::new(writer),
        settings,
    ));
    let mut messages = Vec::new();
    for partition in &partitions {
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
    worker.await.unwrap()?;
    Ok(messages)
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
