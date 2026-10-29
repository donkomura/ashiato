use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetKind {
    A,
    ANoDataEvents,
    B,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DatasetSpec {
    pub kind: DatasetKind,
    pub seed: u64,
    pub scale: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatasetSummary {
    pub kind: DatasetKind,
    pub seed: u64,
    pub scale: f64,
    pub org_id: String,
    pub accounts: Vec<String>,
    pub regions: Vec<String>,
    pub files: u64,
    pub compressed_bytes: u64,
    pub events: u64,
    pub baseline_since: DateTime<Utc>,
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub lookback: String,
    pub batch: String,
    pub allowed_lateness: String,
    pub max_skew: String,
    pub partitions: Vec<PartitionSummary>,
    pub expected_scenarios: Vec<ExpectedScenario>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionSummary {
    pub account: String,
    pub region: String,
    pub files: u64,
    pub compressed_bytes: u64,
    pub events: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedScenario {
    pub scenario_id: String,
    pub origin: ExpectedOrigin,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedOrigin {
    pub id: String,
    pub kind: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DatasetError {
    #[error("scale must be a positive finite number, but got {0}")]
    InvalidScale(f64),
    #[error("output directory {0} is not empty")]
    NotEmpty(PathBuf),
    #[error("failed to write {path}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

pub fn generate(spec: &DatasetSpec, out_dir: &Path) -> Result<DatasetSummary, DatasetError> {
    let _ = (spec, out_dir);
    todo!()
}
