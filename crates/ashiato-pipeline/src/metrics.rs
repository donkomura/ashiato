use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::{EventTime, PartitionId};

pub fn unix_ms() -> u64 {
    todo!()
}

#[derive(Debug, Default)]
pub struct Elapsed(AtomicU64);

impl Elapsed {
    pub fn add(&self, elapsed: Duration) {
        let _ = elapsed;
        todo!()
    }

    pub fn since(&self, started: Instant) {
        let _ = started;
        todo!()
    }

    pub fn get(&self) -> Duration {
        let _ = &self.0;
        todo!()
    }
}

#[derive(Debug, Default)]
pub struct Moment(OnceLock<u64>);

impl Moment {
    pub fn mark(&self) {
        todo!()
    }

    pub fn mark_at(&self, unix_ms: u64) {
        let _ = unix_ms;
        todo!()
    }

    pub fn get(&self) -> Option<u64> {
        let _ = &self.0;
        todo!()
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
        let _ = (counts, &self.counts);
        todo!()
    }

    pub fn counts(&self) -> AnalyzerCounts {
        todo!()
    }

    pub fn line(&self, unix_ms: u64, is_final: bool) -> Value {
        let _ = (unix_ms, is_final);
        todo!()
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
        let _ = worker_id;
        todo!()
    }

    pub fn add_partition(&self, partition: &PartitionId) {
        let _ = (partition, self.worker_id, &self.partitions);
        todo!()
    }

    pub fn record_file(&self, partition: &PartitionId, bytes: u64, events: u64) {
        let _ = (partition, bytes, events);
        todo!()
    }

    pub fn line(&self, unix_ms: u64, is_final: bool) -> Value {
        let _ = (unix_ms, is_final);
        todo!()
    }
}
