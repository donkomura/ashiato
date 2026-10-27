#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ashiato_duckdb::{Analyzer, AnalyzerSettings, PipelineStore};
use ashiato_ingest::{
    FileSource, ListScope, LocalSource, PartitionAssignment, WorkerError, WorkerSettings,
    list_local, run_worker,
};
use ashiato_pipeline::{ChannelReader, EventTime, Manifest, Position, partition_channels};
use ashiato_testkit::log_file::{LogFile, write_log_file};
use chrono::{TimeDelta, TimeZone, Utc};
use serde_json::Value;
use tempfile::TempDir;
use tokio::task::JoinHandle;

pub const LATENESS_MINUTES: i64 = 30;

pub fn at(day: u32, hour: u32, minute: u32) -> EventTime {
    Utc.with_ymd_and_hms(2026, 10, day, hour, minute, 0)
        .unwrap()
}

pub fn settings() -> AnalyzerSettings {
    AnalyzerSettings {
        batch: TimeDelta::hours(1),
        allowed_lateness: TimeDelta::minutes(LATENESS_MINUTES),
        read_since: at(1, 0, 0),
        fetch_max: 1,
        commit_every: 1,
    }
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

    pub fn logs(&self) -> &Path {
        self.logs.path()
    }

    pub fn write(&self, log: &LogFile) -> PathBuf {
        write_log_file(self.logs.path(), log)
    }

    pub fn write_bytes(&self, log: &LogFile, content: &[u8]) -> PathBuf {
        let path = self.write(log);
        std::fs::write(&path, content).unwrap();
        path
    }

    pub fn manifest(&self) -> Manifest {
        let scope = ListScope {
            accounts: None,
            regions: None,
            period: at(1, 0, 0)..at(31, 0, 0),
        };
        list_local(self.logs.path(), &scope).unwrap().manifest
    }

    pub fn database(&self, name: &str) -> PathBuf {
        self.databases.path().join(format!("{name}.duckdb"))
    }
}

pub fn log(account: &str, time: EventTime, records: Vec<Value>) -> LogFile {
    LogFile {
        records,
        ..LogFile::new(account, "us-east-1", time)
    }
}

pub fn open(database: &Path, manifest: &Manifest, settings: AnalyzerSettings) -> Analyzer {
    Analyzer::open(
        PipelineStore::open(database).unwrap(),
        manifest.clone(),
        settings,
    )
    .unwrap()
}

pub fn start_worker(
    manifest: &Manifest,
    starts: &[Position],
) -> (ChannelReader, JoinHandle<Result<(), WorkerError>>) {
    let assignments: Vec<PartitionAssignment> = manifest
        .partitions
        .iter()
        .zip(starts)
        .enumerate()
        .map(|(index, (partition, start))| PartitionAssignment {
            index: u32::try_from(index).unwrap(),
            manifest: partition.clone(),
            start: *start,
        })
        .collect();
    let (writer, reader) = partition_channels(
        manifest
            .partitions
            .iter()
            .map(|partition| partition.partition.clone()),
        4,
    );
    let worker = tokio::spawn(run_worker(
        assignments,
        Arc::new(LocalSource) as Arc<dyn FileSource>,
        Arc::new(writer),
        WorkerSettings {
            allowed_lateness: TimeDelta::minutes(LATENESS_MINUTES),
            max_message_size: 1 << 20,
        },
    ));
    (reader, worker)
}

pub async fn run_to_end(database: &Path, manifest: &Manifest, settings: AnalyzerSettings) {
    let mut analyzer = open(database, manifest, settings);
    let (mut reader, worker) = start_worker(manifest, &analyzer.resume_positions());
    analyzer.run(&mut reader).await.unwrap();
    drop(analyzer);
    drop(reader);
    worker.abort();
}

pub fn rows(database: &Path, table: &str) -> Vec<String> {
    let connection = duckdb::Connection::open(database).unwrap();
    let mut statement = connection
        .prepare(&format!(
            "SELECT CAST(to_json(t) AS VARCHAR) AS row FROM {table} t ORDER BY row"
        ))
        .unwrap();
    statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

pub fn query_strings(database: &Path, sql: &str) -> Vec<String> {
    let connection = duckdb::Connection::open(database).unwrap();
    let mut statement = connection.prepare(sql).unwrap();
    statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}
