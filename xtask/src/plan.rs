use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, SecondsFormat, Utc};

use crate::compose::{Layout, NATS_URL};

const PLAN_FILE: &str = "run.yaml";
const CONTEXT_FILE: &str = "context.yaml";
// The default max_bytes of 50 GiB is more than the JetStream store of the test machine can
// reserve. 4 GiB still holds 24 partitions of 64 messages of 1 MiB with room to spare.
const BROKER: &str = "broker:\n  max_bytes: 4GiB\n";

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

// Every run of a dataset gets the same plan except for placement, which the digest leaves
// out, so that the outputs of all runs can be compared byte for byte (4.1 節).
pub fn write_plan(spec: &PlanSpec, layout: Layout, workspace: &Path) -> io::Result<PathBuf> {
    let lookback = (spec.since - spec.baseline_since).num_seconds();
    fs::write(
        workspace.join(CONTEXT_FILE),
        format!("baseline:\n  lookback: {lookback}s\n"),
    )?;
    let placement = match layout {
        Layout::SingleProcess => "placement:\n  mode: single_process\n".to_owned(),
        Layout::Distributed { workers } => format!(
            "placement:\n  mode: distributed\n  nats_url: {NATS_URL}\n  ingest_workers: {workers}\n"
        ),
    };
    let plan = workspace.join(PLAN_FILE);
    fs::write(
        &plan,
        format!(
            "run_id: {}\n\
             source:\n  kind: s3\n  uri: s3://{}/AWSLogs/\n  endpoint_url: {}\n\
             analysis_window:\n  since: {}\n  until: {}\n\
             context: {CONTEXT_FILE}\n\
             rules: [builtin]\n\
             pipeline:\n  batch: {}\n  allowed_lateness: {}\n  max_skew: {}\n\
             {placement}{BROKER}",
            spec.run_id,
            spec.bucket,
            spec.endpoint_url,
            timestamp(spec.since),
            timestamp(spec.until),
            spec.batch,
            spec.allowed_lateness,
            spec.max_skew,
        ),
    )?;
    Ok(plan)
}

fn timestamp(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Secs, true)
}
