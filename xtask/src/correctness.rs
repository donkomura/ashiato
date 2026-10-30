use std::path::PathBuf;
use std::time::Duration;

use ashiato_testkit::dataset::{DatasetKind, DatasetSpec, DatasetSummary};

use crate::cli::CorrectnessDataset;
use crate::plan::PlanSpec;
use crate::report::Report;

pub const DATASET_SEED: u64 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct Options {
    pub dataset: DatasetSpec,
    pub integration_dir: PathBuf,
}

pub fn integration_dir() -> PathBuf {
    todo!()
}

pub fn dataset_spec(dataset: CorrectnessDataset, scale: f64) -> DatasetSpec {
    let _ = (dataset, scale);
    todo!()
}

pub fn run_id(kind: DatasetKind) -> String {
    let _ = kind;
    todo!()
}

pub fn plan_spec(summary: &DatasetSummary, run_id: &str) -> PlanSpec {
    let _ = (summary, run_id);
    todo!()
}

pub fn run_timeout(scale: f64) -> Duration {
    let _ = scale;
    todo!()
}

pub fn count_lines(output: &[u8]) -> usize {
    let _ = output;
    todo!()
}

pub fn correctness(options: &Options) -> anyhow::Result<Report> {
    let _ = options;
    todo!()
}
