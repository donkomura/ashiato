use std::collections::BTreeMap;
use std::time::Duration;

use ashiato_testkit::dataset::DatasetSummary;

use crate::metrics::{AnalyzerMetrics, AnalyzerTimes, WorkerMetrics, WorkerTimes};

// The criterion of 14.1.4 節 is E(N) >= 0.8 for N = 2, 3 and 4. 17 節 marks 0.8 as a provisional
// value to be revised after the first measurement. It is a percentage so that a run exactly on
// the criterion is judged in integer arithmetic.
pub const MIN_EFFICIENCY_PERCENT: u64 = 80;

// The CPU usage of each service in one `docker stats --no-stream` call, in percent of one CPU.
pub type StatsSample = BTreeMap<String, f64>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScalingError {
    #[error("the analyzer metrics have no {0} time")]
    MissingPhase(&'static str),
    #[error("the ingestion ended at {end} ms, which is not after it started at {start} ms")]
    EmptyIngestion { start: i64, end: i64 },
    #[error("{found} of the {workers} workers wrote metrics")]
    MissingWorkers { workers: u32, found: usize },
    #[error("invalid docker stats line {0:?}")]
    InvalidStats(String),
    #[error("no runs were measured")]
    NoRuns,
}

// One measured run, without the warm-up run (14.1.4 節). The metrics are the final lines of the
// analyzer and of every worker, and the stats are taken once a second while the run lasts.
#[derive(Debug, Clone, PartialEq)]
pub struct Run {
    pub workers: u32,
    pub analyzer: AnalyzerMetrics,
    pub worker_metrics: Vec<WorkerMetrics>,
    pub stats: Vec<StatsSample>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Throughput {
    pub bytes_per_second: f64,
    pub events_per_second: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CpuUsage {
    pub mean: f64,
    pub max: f64,
    pub samples: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Level {
    pub workers: u32,
    pub durations: Vec<Duration>,
    pub median: Duration,
    pub throughput: Throughput,
    pub speedup: Option<f64>,
    pub efficiency: Option<f64>,
    pub meets_criterion: Option<bool>,
    pub worker_times: WorkerTimes,
    pub analyzer_times: AnalyzerTimes,
    pub cpu: BTreeMap<String, CpuUsage>,
    pub servers_cpu: Option<CpuUsage>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Scaling {
    pub compressed_bytes: u64,
    pub events: u64,
    pub server_cpu_limit: f64,
    pub levels: Vec<Level>,
}

impl Scaling {
    pub fn passed(&self) -> bool {
        todo!()
    }
}

pub fn ingest_duration(metrics: &AnalyzerMetrics) -> Result<Duration, ScalingError> {
    todo!("{metrics:?}")
}

pub fn median(durations: &[Duration]) -> Option<Duration> {
    todo!("{durations:?}")
}

pub fn rejected_share(times: WorkerTimes) -> Option<f64> {
    todo!("{times:?}")
}

pub fn parse_docker_stats(output: &str, project: &str) -> Result<StatsSample, ScalingError> {
    todo!("{output}{project}")
}

pub fn evaluate(
    runs: &[Run],
    dataset: &DatasetSummary,
    server_cpus: usize,
) -> Result<Scaling, ScalingError> {
    todo!("{runs:?}{dataset:?}{server_cpus}")
}
