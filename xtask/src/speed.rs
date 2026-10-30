use std::path::PathBuf;

use ashiato_testkit::dataset::DatasetSpec;

use crate::cli::ScalingDataset;
use crate::scaling::{Run, ScalingError, StatsSample};

pub const REPORT_FILE: &str = "scaling-report.md";
pub const RUNS_DIR: &str = "scaling";
pub const STATS_LOG: &str = "docker-stats.log";

#[derive(Debug, Clone, PartialEq)]
pub struct Options {
    pub dataset: DatasetSpec,
    pub integration_dir: PathBuf,
    pub workers: Vec<u32>,
    pub repeat: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MeasureError {
    #[error("the analyzer wrote no final metrics")]
    NoAnalyzerMetrics,
    #[error("worker {0} wrote no final metrics")]
    NoWorkerMetrics(u32),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimedSample {
    pub unix_ms: i64,
    pub sample: StatsSample,
}

pub fn dataset_spec(_dataset: ScalingDataset, _scale: f64) -> DatasetSpec {
    todo!()
}

pub fn stats_entry(_unix_ms: i64, _output: &str) -> String {
    todo!()
}

pub fn parse_stats_log(_log: &str, _project: &str) -> Result<Vec<TimedSample>, ScalingError> {
    todo!()
}

pub fn collect_run(
    _analyzer_log: &str,
    _worker_logs: &[String],
    _samples: &[TimedSample],
) -> Result<Run, MeasureError> {
    todo!()
}
