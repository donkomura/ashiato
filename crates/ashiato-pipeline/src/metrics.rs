use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use chrono::SecondsFormat;
use serde_json::{Value, json};

use crate::{EventTime, PartitionId};

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, millis)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

// The time spent in one step, added up over the tasks that take it. The tasks run at the same
// time, so the total can exceed the time the run took (14.1.5 節).
#[derive(Debug, Default)]
pub struct Elapsed(AtomicU64);

impl Elapsed {
    pub fn add(&self, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.0.fetch_add(nanos, Ordering::Relaxed);
    }

    pub fn since(&self, started: Instant) {
        self.add(started.elapsed());
    }

    pub fn get(&self) -> Duration {
        Duration::from_nanos(self.0.load(Ordering::Relaxed))
    }

    fn millis(&self) -> u64 {
        millis(self.get())
    }
}

#[derive(Debug, Default)]
pub struct Moment(OnceLock<u64>);

impl Moment {
    pub fn mark(&self) {
        self.mark_at(unix_ms());
    }

    pub fn mark_at(&self, unix_ms: u64) {
        let _ = self.0.set(unix_ms);
    }

    pub fn get(&self) -> Option<u64> {
        self.0.get().copied()
    }
}

#[derive(Debug, Default)]
pub struct SendTimes {
    pub publish_wait: Elapsed,
    pub rejected_wait: Elapsed,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AnalyzerCounts {
    pub manifest_files: u64,
    pub ingested_files: u64,
    pub global_watermark: Option<EventTime>,
    pub pending_rows: u64,
    pub pending_max_rows: u64,
}

#[derive(Debug, Default)]
pub struct AnalyzerMetrics {
    counts: Mutex<AnalyzerCounts>,
    pub fetch_wait: Elapsed,
    pub commit: Elapsed,
    pub batch: Elapsed,
    pub first_plan_response: Moment,
    pub last_end_committed: Moment,
    pub correlated: Moment,
}

impl AnalyzerMetrics {
    pub fn set_counts(&self, counts: AnalyzerCounts) {
        *self.lock() = counts;
    }

    pub fn counts(&self) -> AnalyzerCounts {
        *self.lock()
    }

    fn lock(&self) -> MutexGuard<'_, AnalyzerCounts> {
        self.counts
            .lock()
            .expect("no thread panics while holding the counts")
    }

    pub fn line(&self, unix_ms: u64, is_final: bool) -> Value {
        let counts = self.counts();
        json!({
            "metrics": "analyzer",
            "unix_ms": unix_ms,
            "final": is_final,
            "manifest_files": counts.manifest_files,
            "ingested_files": counts.ingested_files,
            "global_watermark": counts
                .global_watermark
                .map(|watermark| watermark.to_rfc3339_opts(SecondsFormat::AutoSi, true)),
            "pending_rows": counts.pending_rows,
            "pending_max_rows": counts.pending_max_rows,
            "time_ms": {
                "fetch_wait": self.fetch_wait.millis(),
                "commit": self.commit.millis(),
                "batch": self.batch.millis(),
            },
            "phases_unix_ms": {
                "first_plan_response": self.first_plan_response.get(),
                "last_end_committed": self.last_end_committed.get(),
                "correlated": self.correlated.get(),
            },
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PartitionCounts {
    pub files: u64,
    pub bytes: u64,
    pub events: u64,
}

#[derive(Debug, Default)]
pub struct WorkerMetrics {
    worker_id: u32,
    pub fetch_wait: Elapsed,
    pub decode: Elapsed,
    pub send: SendTimes,
    partitions: Mutex<BTreeMap<PartitionId, PartitionCounts>>,
}

impl WorkerMetrics {
    pub fn new(worker_id: u32) -> Self {
        Self {
            worker_id,
            ..Self::default()
        }
    }

    pub fn add_partition(&self, partition: &PartitionId) {
        self.lock().entry(partition.clone()).or_default();
    }

    pub fn record_file(&self, partition: &PartitionId, bytes: u64, events: u64) {
        let mut partitions = self.lock();
        let counts = partitions.entry(partition.clone()).or_default();
        counts.files += 1;
        counts.bytes += bytes;
        counts.events += events;
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<PartitionId, PartitionCounts>> {
        self.partitions
            .lock()
            .expect("no thread panics while holding the counts")
    }

    pub fn line(&self, unix_ms: u64, is_final: bool) -> Value {
        let partitions: Vec<Value> = self
            .lock()
            .iter()
            .map(|(partition, counts)| {
                json!({
                    "partition": partition.to_string(),
                    "files": counts.files,
                    "bytes": counts.bytes,
                    "events": counts.events,
                })
            })
            .collect();
        json!({
            "metrics": "worker",
            "worker_id": self.worker_id,
            "unix_ms": unix_ms,
            "final": is_final,
            "partitions": partitions,
            "time_ms": {
                "fetch_wait": self.fetch_wait.millis(),
                "decode": self.decode.millis(),
                "publish_wait": self.send.publish_wait.millis(),
                "rejected_wait": self.send.rejected_wait.millis(),
            },
        })
    }
}
