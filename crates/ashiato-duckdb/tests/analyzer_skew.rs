mod common;

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ashiato_core::AnalysisWindow;
use ashiato_core::rule::{Rule, RuleSource, Rules, SignalRule};
use ashiato_duckdb::{Analyzer, AnalyzerSettings, PipelineStore};
use ashiato_ingest::{
    DEFAULT_PREFETCH_FILES, FileSource, LocalSource, PartitionAssignment, SourceError,
    WorkerSettings, run_worker,
};
use ashiato_pipeline::{
    ChannelReader, Delivery, DeliveryHandle, EventTime, Manifest, ManifestEntry, PartitionId,
    PartitionReader, TransportError, partition_channels,
};
use ashiato_testkit::record::RecordBuilder;
use async_trait::async_trait;
use chrono::TimeDelta;
use common::{LATENESS_MINUTES, Workspace, at, log, rows};
use serde_json::{Value, json};
use tokio::sync::watch;

const FAST: [&str; 2] = ["111111111111", "222222222222"];
const SLOW: &str = "333333333333";
const FILES_PER_PARTITION: i64 = 144;
const FILE_INTERVAL_MINUTES: i64 = 5;
const MAX_SKEW_MINUTES: i64 = 30;
const BATCH_MINUTES: i64 = 60;
// The slow partition may read one more file on every SLOW_EVERY-th fetch.
const SLOW_EVERY: usize = 8;
const TABLES: [&str; 9] = [
    "pending",
    "late_events",
    "batches",
    "ingested_files",
    "partition_progress",
    "recent_event_ids",
    "signals",
    "lineage_edges",
    "data_counters",
];

fn record(account: &str, id: String, time: EventTime, login: bool) -> Value {
    let builder = RecordBuilder::new()
        .set("eventID", id)
        .set("eventTime", time.to_rfc3339())
        .set("recipientAccountId", account)
        .set("awsRegion", "us-east-1");
    if login {
        builder
            .set("eventSource", "signin.amazonaws.com")
            .set("eventName", "ConsoleLogin")
            .set("userIdentity.accessKeyId", Value::Null)
            .set("responseElements", json!({ "ConsoleLogin": "Success" }))
            .set("additionalEventData", json!({ "MFAUsed": "No" }))
            .build()
    } else {
        builder.build()
    }
}

// Every partition has a file every five minutes, each with one event ten minutes before the time
// of its file, so that the events of the partitions advance at the same pace.
fn write_files(workspace: &Workspace) {
    for account in FAST.iter().copied().chain([SLOW]) {
        for file in 1..=FILES_PER_PARTITION {
            let file_time = at(1, 0, 0) + TimeDelta::minutes(FILE_INTERVAL_MINUTES * file);
            workspace.write(&log(
                account,
                file_time,
                vec![record(
                    account,
                    format!("{account}-{file:03}"),
                    file_time - TimeDelta::minutes(10),
                    file % 3 == 0,
                )],
            ));
        }
    }
}

// Lets the files of the slow partition be read only as fast as the analyzer releases them.
struct SlowSource {
    released: watch::Receiver<u64>,
}

#[async_trait]
impl FileSource for SlowSource {
    async fn read(&self, entry: &ManifestEntry) -> Result<Vec<u8>, SourceError> {
        if entry.uri.contains(SLOW) {
            let mut released = self.released.clone();
            released
                .wait_for(|released| *released > entry.file_seq)
                .await
                .unwrap();
        }
        LocalSource.read(entry).await
    }
}

// Releases one more file of the slow partition on every SLOW_EVERY-th fetch, and on every fetch
// that may only read the slow partition, so that the slow partition lags behind the others
// without stopping the run.
struct SlowingReader {
    inner: ChannelReader,
    slow: PartitionId,
    release: watch::Sender<u64>,
    fetches: usize,
}

#[async_trait]
impl PartitionReader for SlowingReader {
    async fn fetch(
        &mut self,
        partition: &PartitionId,
        max: usize,
    ) -> Result<Vec<Delivery>, TransportError> {
        self.inner.fetch(partition, max).await
    }

    async fn fetch_ready(
        &mut self,
        partitions: &[PartitionId],
        max: usize,
    ) -> Result<(usize, Vec<Delivery>), TransportError> {
        self.fetches += 1;
        if self.fetches.is_multiple_of(SLOW_EVERY)
            || partitions.iter().all(|partition| *partition == self.slow)
        {
            self.release.send_modify(|released| *released += 1);
        }
        self.inner.fetch_ready(partitions, max).await
    }

    async fn ack(&self, deliveries: &[DeliveryHandle]) -> Result<(), TransportError> {
        self.inner.ack(deliveries).await
    }

    async fn reset(&mut self, partition: &PartitionId) -> Result<(), TransportError> {
        self.inner.reset(partition).await
    }
}

fn window() -> AnalysisWindow {
    AnalysisWindow::new(at(1, 0, 0), at(2, 0, 0), TimeDelta::zero()).unwrap()
}

fn signal_rules() -> Vec<SignalRule> {
    Rules::new([RuleSource::builtin().unwrap()])
        .unwrap()
        .iter()
        .filter_map(|rule| match rule {
            Rule::Signal(rule) => Some(rule.clone()),
            Rule::Scenario(_) => None,
        })
        .collect()
}

// Runs the pipeline with the slow partition, committing and finalizing after every message as
// Analyzer::run does with commit_every 1, and returns the most rows that pending held.
async fn run_lagging(database: &Path, manifest: &Manifest, max_skew: Option<TimeDelta>) -> u64 {
    let settings = AnalyzerSettings {
        batch: TimeDelta::minutes(BATCH_MINUTES),
        allowed_lateness: TimeDelta::minutes(LATENESS_MINUTES),
        read_since: window().baseline_since(),
        fetch_max: 1,
        commit_every: 1,
    };
    let mut analyzer = Analyzer::open(
        PipelineStore::open(database).unwrap(),
        manifest.clone(),
        settings,
    )
    .unwrap()
    .with_queries(signal_rules(), window())
    .with_max_skew(max_skew);
    let partitions: Vec<PartitionId> = manifest
        .partitions
        .iter()
        .map(|partition| partition.partition.clone())
        .collect();
    let (writer, channel) = partition_channels(partitions.clone(), 4);
    let (release, released) = watch::channel(0);
    let assignments = manifest
        .partitions
        .iter()
        .zip(analyzer.resume_positions())
        .enumerate()
        .map(|(index, (partition, start))| PartitionAssignment {
            index: u32::try_from(index).unwrap(),
            manifest: partition.clone(),
            start,
        })
        .collect();
    let worker = tokio::spawn(run_worker(
        assignments,
        Arc::new(SlowSource { released }) as Arc<dyn FileSource>,
        Arc::new(writer),
        WorkerSettings {
            allowed_lateness: TimeDelta::minutes(LATENESS_MINUTES),
            max_message_size: 1 << 20,
            decode_threads: NonZeroUsize::new(2).unwrap(),
            prefetch_files: DEFAULT_PREFETCH_FILES,
        },
        Arc::default(),
    ));
    let mut reader = SlowingReader {
        inner: channel,
        slow: partitions
            .iter()
            .find(|partition| partition.account == SLOW)
            .unwrap()
            .clone(),
        release,
        fetches: 0,
    };

    let most_pending = tokio::time::timeout(Duration::from_secs(120), async {
        let mut most_pending = 0;
        while !analyzer.all_partitions_ended() {
            analyzer.receive(&mut reader).await.unwrap();
            let handles = analyzer.commit().unwrap();
            reader.ack(&handles).await.unwrap();
            analyzer.finalize_batches().unwrap();
            most_pending = most_pending.max(analyzer.pending_rows().unwrap());
        }
        most_pending
    })
    .await
    .expect("the run must not stall while one partition lags");
    worker.await.unwrap().unwrap();
    most_pending
}

#[tokio::test(flavor = "multi_thread")]
async fn max_skew_bounds_pending_while_one_partition_lags_and_keeps_the_output() {
    let workspace = Workspace::new();
    write_files(&workspace);
    let manifest = workspace.manifest();
    assert_eq!(manifest.partitions.len(), 3);
    let unlimited = workspace.database("unlimited");
    let limited = workspace.database("limited");

    let unlimited_pending = run_lagging(&unlimited, &manifest, None).await;
    let limited_pending = run_lagging(
        &limited,
        &manifest,
        Some(TimeDelta::minutes(MAX_SKEW_MINUTES)),
    )
    .await;

    // A partition is read only while its watermark is less than max_skew ahead of the global
    // watermark, and after each commit the batches that end by the global watermark are
    // finalized. The events in pending are therefore within batch before the global watermark
    // and max_skew + allowed_lateness after it, and each partition has one event in every five
    // minutes of that range.
    let range = BATCH_MINUTES + MAX_SKEW_MINUTES + LATENESS_MINUTES;
    let bound = u64::try_from(3 * (range / FILE_INTERVAL_MINUTES + 1)).unwrap();
    assert!(
        limited_pending <= bound,
        "pending held {limited_pending} rows, more than {bound}"
    );
    // Without max_skew the fast partitions run ahead of the slow one, which shows that the slow
    // partition does lag behind in this test.
    assert!(
        unlimited_pending > bound,
        "pending held only {unlimited_pending} rows without max_skew"
    );
    assert!(!rows(&limited, "signals").is_empty());
    for table in TABLES {
        assert_eq!(rows(&limited, table), rows(&unlimited, table), "{table}");
    }
}
