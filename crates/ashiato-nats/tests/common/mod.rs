#![allow(dead_code)]

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{Arc, Mutex};

use ashiato_ingest::{
    DEFAULT_PREFETCH_FILES, FileSource, ListScope, LocalSource, PartitionAssignment,
    WorkerSettings, list_local, run_worker,
};
use ashiato_pipeline::{
    EventTime, Manifest, PartitionMessage, PartitionWriter, Position, TransportError,
};
use ashiato_testkit::log_file::{LogFile, write_log_file};
use ashiato_testkit::record::RecordBuilder;
use async_trait::async_trait;
use chrono::{TimeDelta, TimeZone, Utc};
use serde_json::Value;
use tempfile::TempDir;

pub const LATENESS_MINUTES: i64 = 30;
pub const ACCOUNTS: [&str; 2] = ["111111111111", "222222222222"];

pub fn at(day: u32, hour: u32, minute: u32) -> EventTime {
    Utc.with_ymd_and_hms(2026, 10, day, hour, minute, 0)
        .unwrap()
}

pub fn event(id: &str, time: EventTime) -> Value {
    RecordBuilder::new()
        .set("eventID", id)
        .set("eventTime", time.to_rfc3339())
        .build()
}

pub struct Workspace {
    logs: TempDir,
    databases: TempDir,
}

impl Workspace {
    pub fn new() -> Self {
        Self {
            logs: TempDir::new().unwrap(),
            databases: TempDir::new().unwrap(),
        }
    }

    pub fn write(&self, log: &LogFile) {
        write_log_file(self.logs.path(), log);
    }

    pub fn write_bytes(&self, log: &LogFile, content: &[u8]) {
        let path = self.logs.path().join(log.relative_path());
        write_log_file(self.logs.path(), log);
        std::fs::write(path, content).unwrap();
    }

    pub fn manifest(&self) -> Manifest {
        let scope = ListScope {
            accounts: None,
            regions: None,
            period: Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap()..at(31, 0, 0),
        };
        list_local(self.logs.path(), &scope).unwrap().manifest
    }

    pub fn database(&self, name: &str) -> std::path::PathBuf {
        self.databases.path().join(format!("{name}.duckdb"))
    }

    pub fn logs(&self) -> &Path {
        self.logs.path()
    }
}

// Two partitions of several files each, with a corrupt file and an empty one, so that every
// kind of message is sent and the events span several batches.
pub fn fixture() -> Workspace {
    let workspace = Workspace::new();
    for (number, account) in ACCOUNTS.iter().enumerate() {
        for step in 0..12_u32 {
            let file_time = at(1, step / 4, (step % 4) * 15 + 5);
            let records = (0..3)
                .map(|index| {
                    event(
                        &format!("{number}-{step}-{index}"),
                        file_time - TimeDelta::minutes(i64::from(index) + 1),
                    )
                })
                .collect();
            workspace.write(&LogFile {
                records,
                ..LogFile::new(*account, "us-east-1", file_time)
            });
        }
    }
    workspace.write(&LogFile::new(ACCOUNTS[0], "us-east-1", at(1, 3, 10)));
    workspace.write_bytes(
        &LogFile::new(ACCOUNTS[1], "us-east-1", at(1, 3, 10)),
        b"not a gzip stream",
    );
    workspace
}

pub fn worker_settings(max_message_size: usize) -> WorkerSettings {
    WorkerSettings {
        allowed_lateness: TimeDelta::minutes(LATENESS_MINUTES),
        max_message_size,
        decode_threads: NonZeroUsize::new(2).unwrap(),
        prefetch_files: DEFAULT_PREFETCH_FILES,
    }
}

pub fn assignments(manifest: &Manifest, starts: &[Position]) -> Vec<PartitionAssignment> {
    manifest
        .partitions
        .iter()
        .zip(starts)
        .enumerate()
        .map(|(index, (partition, start))| PartitionAssignment {
            index: u32::try_from(index).unwrap(),
            manifest: partition.clone(),
            start: *start,
        })
        .collect()
}

#[derive(Default)]
pub struct CollectingWriter {
    pub messages: Mutex<Vec<PartitionMessage>>,
}

#[async_trait]
impl PartitionWriter for CollectingWriter {
    async fn put(&self, message: PartitionMessage) -> Result<(), TransportError> {
        self.messages.lock().unwrap().push(message);
        Ok(())
    }
}

// The messages that the worker sends for the manifest, in the order of each partition.
pub async fn worker_messages(
    manifest: &Manifest,
    max_message_size: usize,
) -> Vec<PartitionMessage> {
    let writer = Arc::new(CollectingWriter::default());
    run_worker(
        assignments(manifest, &vec![Position::START; manifest.partitions.len()]),
        Arc::new(LocalSource) as Arc<dyn FileSource>,
        Arc::clone(&writer) as Arc<dyn PartitionWriter>,
        worker_settings(max_message_size),
        Arc::default(),
    )
    .await
    .unwrap();
    let mut messages = std::mem::take(&mut *writer.messages.lock().unwrap());
    messages.sort_by_key(|message| (message.partition().clone(), message.position()));
    messages
}
