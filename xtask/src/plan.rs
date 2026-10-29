use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use crate::compose::Layout;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanSpec {
    pub run_id: String,
    pub bucket: String,
    pub endpoint_url: String,
    pub baseline_since: DateTime<Utc>,
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
    pub batch: String,
    pub allowed_lateness: String,
    pub max_skew: String,
}

pub fn write_plan(spec: &PlanSpec, layout: Layout, workspace: &Path) -> io::Result<PathBuf> {
    let _ = (spec, layout, workspace);
    todo!()
}
