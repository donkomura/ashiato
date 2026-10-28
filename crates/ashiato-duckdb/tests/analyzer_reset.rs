mod common;

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use ashiato_duckdb::{AnalyzerError, AnalyzerSettings};
use ashiato_ingest::{
    DEFAULT_PREFETCH_FILES, FileSource, LocalSource, PartitionAssignment, WorkerSettings,
    run_worker,
};
use ashiato_pipeline::{
    Delivery, DeliveryHandle, Manifest, PartitionId, PartitionMessage, PartitionReader,
    PartitionWriter, Position, TransportError,
};
use ashiato_testkit::record::RecordBuilder;
use async_trait::async_trait;
use chrono::TimeDelta;
use common::{LATENESS_MINUTES, Workspace, at, log, open, rows, run_to_end, settings};
use serde_json::Value;

const TABLES: [&str; 6] = [
    "pending",
    "ingested_files",
    "partition_progress",
    "late_events",
    "recent_event_ids",
    "batches",
];

fn event(id: &str, minute: u32) -> Value {
    RecordBuilder::new()
        .set("eventID", id)
        .set("eventTime", at(1, minute / 60, minute % 60).to_rfc3339())
        .build()
}

fn fixture() -> Workspace {
    let workspace = Workspace::new();
    for account in ["111111111111", "222222222222"] {
        for step in 0..8_u32 {
            let minute = 10 + step * 15;
            workspace.write(&log(
                account,
                at(1, minute / 60, minute % 60),
                vec![
                    event(&format!("{account}-{step}-a"), minute - 2),
                    event(&format!("{account}-{step}-b"), minute - 1),
                ],
            ));
        }
    }
    workspace
}

#[derive(Default)]
struct CollectingWriter {
    messages: Mutex<Vec<PartitionMessage>>,
}

#[async_trait]
impl PartitionWriter for CollectingWriter {
    async fn put(&self, message: PartitionMessage) -> Result<(), TransportError> {
        self.messages.lock().unwrap().push(message);
        Ok(())
    }
}

async fn worker_messages(manifest: &Manifest) -> HashMap<PartitionId, Vec<PartitionMessage>> {
    let writer = Arc::new(CollectingWriter::default());
    let assignments = manifest
        .partitions
        .iter()
        .enumerate()
        .map(|(index, partition)| PartitionAssignment {
            index: u32::try_from(index).unwrap(),
            manifest: partition.clone(),
            start: Position::START,
        })
        .collect();
    run_worker(
        assignments,
        Arc::new(LocalSource) as Arc<dyn FileSource>,
        Arc::clone(&writer) as Arc<dyn PartitionWriter>,
        WorkerSettings {
            allowed_lateness: TimeDelta::minutes(LATENESS_MINUTES),
            max_message_size: 1 << 20,
            decode_threads: NonZeroUsize::new(2).unwrap(),
            prefetch_files: DEFAULT_PREFETCH_FILES,
        },
    )
    .await
    .unwrap();
    let mut streams: HashMap<PartitionId, Vec<PartitionMessage>> = HashMap::new();
    for message in std::mem::take(&mut *writer.messages.lock().unwrap()) {
        streams
            .entry(message.partition().clone())
            .or_default()
            .push(message);
    }
    for messages in streams.values_mut() {
        messages.sort_by_key(PartitionMessage::position);
    }
    streams
}

// Behaves as a work queue with one consumer for each partition: an acknowledged message is
// removed, and a new consumer delivers the messages that remain from the start of the stream.
// A held back message is delivered after the ones behind it, as a redelivery after ack_wait
// can be; a lost message is never delivered, as a message removed from the stream.
struct WorkQueueReader {
    streams: HashMap<PartitionId, Vec<PartitionMessage>>,
    cursors: HashMap<PartitionId, usize>,
    acked: Mutex<HashSet<(PartitionId, usize)>>,
    handles: Mutex<HashMap<u64, (PartitionId, usize)>>,
    next_handle: u64,
    held_back: Vec<(PartitionId, usize)>,
    lost: Option<(PartitionId, usize)>,
    resets: Vec<PartitionId>,
}

impl WorkQueueReader {
    fn new(streams: HashMap<PartitionId, Vec<PartitionMessage>>) -> Self {
        Self {
            cursors: streams.keys().map(|id| (id.clone(), 0)).collect(),
            streams,
            acked: Mutex::new(HashSet::new()),
            handles: Mutex::new(HashMap::new()),
            next_handle: 0,
            held_back: Vec::new(),
            lost: None,
            resets: Vec::new(),
        }
    }

    fn take(&mut self, partition: &PartitionId, max: usize) -> Vec<Delivery> {
        let mut deliveries = Vec::new();
        let length = self.streams[partition].len();
        while deliveries.len() < max {
            let cursor = self.cursors[partition];
            if cursor >= length {
                break;
            }
            self.cursors.insert(partition.clone(), cursor + 1);
            let key = (partition.clone(), cursor);
            if self.acked.lock().unwrap().contains(&key) || self.lost.as_ref() == Some(&key) {
                continue;
            }
            if let Some(held) = self.held_back.iter().position(|held| *held == key) {
                self.held_back.remove(held);
                continue;
            }
            let handle = self.next_handle;
            self.next_handle += 1;
            self.handles.lock().unwrap().insert(handle, key);
            deliveries.push(Delivery {
                message: self.streams[partition][cursor].clone(),
                handle: DeliveryHandle::new(handle),
            });
        }
        deliveries
    }
}

#[async_trait]
impl PartitionReader for WorkQueueReader {
    async fn fetch(
        &mut self,
        partition: &PartitionId,
        max: usize,
    ) -> Result<Vec<Delivery>, TransportError> {
        Ok(self.take(partition, max))
    }

    async fn fetch_ready(
        &mut self,
        partitions: &[PartitionId],
        max: usize,
    ) -> Result<(usize, Vec<Delivery>), TransportError> {
        for (index, partition) in partitions.iter().enumerate() {
            let deliveries = self.take(partition, max);
            if !deliveries.is_empty() {
                return Ok((index, deliveries));
            }
        }
        Ok((0, Vec::new()))
    }

    async fn ack(&self, deliveries: &[DeliveryHandle]) -> Result<(), TransportError> {
        let handles = self.handles.lock().unwrap();
        let mut acked = self.acked.lock().unwrap();
        for handle in deliveries {
            acked.insert(handles[&handle.id()].clone());
        }
        Ok(())
    }

    async fn reset(&mut self, partition: &PartitionId) -> Result<(), TransportError> {
        self.resets.push(partition.clone());
        self.cursors.insert(partition.clone(), 0);
        Ok(())
    }
}

fn batched_settings() -> AnalyzerSettings {
    AnalyzerSettings {
        fetch_max: 2,
        commit_every: 3,
        ..settings()
    }
}

async fn reference(workspace: &Workspace, manifest: &Manifest) -> Vec<Vec<String>> {
    let database = workspace.database("reference");
    run_to_end(&database, manifest, batched_settings()).await;
    TABLES.iter().map(|table| rows(&database, table)).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gap_that_a_new_consumer_fills_does_not_change_the_tables() {
    let workspace = fixture();
    let manifest = workspace.manifest();
    let expected = reference(&workspace, &manifest).await;
    let partition = manifest.partitions[0].partition.clone();
    let mut reader = WorkQueueReader::new(worker_messages(&manifest).await);
    reader.held_back = vec![(partition.clone(), 2)];
    let database = workspace.database("reset");
    let mut analyzer = open(&database, &manifest, batched_settings());

    analyzer.run(&mut reader).await.unwrap();
    drop(analyzer);

    assert_eq!(reader.resets, vec![partition]);
    let tables: Vec<Vec<String>> = TABLES.iter().map(|table| rows(&database, table)).collect();
    assert_eq!(tables, expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gap_after_the_positions_continued_again_gets_another_new_consumer() {
    let workspace = fixture();
    let manifest = workspace.manifest();
    let expected = reference(&workspace, &manifest).await;
    let partition = manifest.partitions[0].partition.clone();
    let mut reader = WorkQueueReader::new(worker_messages(&manifest).await);
    reader.held_back = vec![(partition.clone(), 1), (partition.clone(), 5)];
    let database = workspace.database("reset");
    let mut analyzer = open(&database, &manifest, batched_settings());

    analyzer.run(&mut reader).await.unwrap();
    drop(analyzer);

    assert_eq!(reader.resets, vec![partition.clone(), partition]);
    let tables: Vec<Vec<String>> = TABLES.iter().map(|table| rows(&database, table)).collect();
    assert_eq!(tables, expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gap_that_remains_with_a_new_consumer_is_an_error() {
    let workspace = fixture();
    let manifest = workspace.manifest();
    let partition = manifest.partitions[0].partition.clone();
    let mut reader = WorkQueueReader::new(worker_messages(&manifest).await);
    reader.lost = Some((partition.clone(), 2));
    let mut analyzer = open(&workspace.database("lost"), &manifest, batched_settings());

    let error = analyzer.run(&mut reader).await.unwrap_err();

    assert!(
        matches!(
            &error,
            AnalyzerError::Gap { partition: gap, expected, received }
                if *gap == partition
                    && *expected == Position::new(2, 0)
                    && *received == Position::new(3, 0)
        ),
        "{error:?}"
    );
    assert_eq!(reader.resets, vec![partition]);
}
