use std::time::Duration;

use ashiato_testkit::dataset::DatasetSummary;

use crate::compare::Comparison;
use crate::schedule::RunSpec;
use crate::topology::{CpuAssignment, Cpuset};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageVersion {
    pub image: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Environment {
    pub cores: Vec<Cpuset>,
    pub cpus: CpuAssignment,
    pub docker: String,
    pub compose: String,
    pub nats: ImageVersion,
    pub minio: ImageVersion,
    pub runtime_image: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Injection {
    pub since_start: Duration,
    pub ingested_files: u64,
    pub manifest_files: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunResult {
    pub spec: RunSpec,
    pub elapsed: Duration,
    pub analyzer_exit: Option<i64>,
    pub output_lines: usize,
    pub injection: Option<Injection>,
    pub comparison: Option<Result<Comparison, String>>,
    pub error: Option<String>,
}

impl RunResult {
    pub fn passed(&self) -> bool {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Report {
    pub dataset: String,
    pub environment: Environment,
    pub summary: DatasetSummary,
    pub runs: Vec<RunResult>,
    pub reference_checks: Option<Vec<ReferenceCheck>>,
}

impl Report {
    pub fn passed(&self) -> bool {
        todo!()
    }

    pub fn markdown(&self) -> String {
        todo!()
    }
}
