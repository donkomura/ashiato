use std::path::Path;

// The tables of 14.1.3 節 that must match the reference run row for row.
pub const TABLES: [&str; 8] = [
    "signals",
    "lineage_edges",
    "baseline",
    "ingested_files",
    "late_events",
    "batches",
    "data_counters",
    "partition_progress",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDifference {
    pub table: String,
    pub only_in_reference: u64,
    pub only_in_run: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceComparison {
    pub tables: Vec<TableDifference>,
    pub pending_rows: u64,
}

impl WorkspaceComparison {
    pub fn matches(&self) -> bool {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Comparison {
    pub output_matches: bool,
    pub workspace: WorkspaceComparison,
    pub stream_removed: bool,
}

impl Comparison {
    pub fn passed(&self) -> bool {
        todo!()
    }
}

pub fn outputs_match(reference: &[u8], run: &[u8]) -> bool {
    let _ = (reference, run);
    todo!()
}

pub fn compare_workspaces(
    reference: &Path,
    run: &Path,
) -> Result<WorkspaceComparison, duckdb::Error> {
    let _ = (reference, run);
    todo!()
}

pub fn stream_left(nats_store: &Path, run_id: &str) -> bool {
    let _ = (nats_store, run_id);
    todo!()
}
