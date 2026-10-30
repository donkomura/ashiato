use std::path::Path;

use ashiato_nats::stream_name;
use duckdb::Connection;

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
        self.pending_rows == 0
            && self
                .tables
                .iter()
                .all(|table| table.only_in_reference == 0 && table.only_in_run == 0)
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
        self.output_matches && self.workspace.matches() && self.stream_removed
    }
}

// Every run of dataset A reports signals, so an empty reference output means that the reference
// run itself failed, and runs that also print nothing would match it without proving anything.
pub fn outputs_match(reference: &[u8], run: &[u8]) -> bool {
    !reference.is_empty() && reference == run
}

pub fn compare_workspaces(
    reference: &Path,
    run: &Path,
) -> Result<WorkspaceComparison, duckdb::Error> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(&format!(
        "ATTACH {} AS ref (READ_ONLY); ATTACH {} AS run (READ_ONLY);",
        literal(reference),
        literal(run)
    ))?;
    let count = |sql: &str| connection.query_row(sql, [], |row| row.get::<_, u64>(0));
    let tables = TABLES
        .iter()
        .map(|table| {
            Ok(TableDifference {
                table: (*table).to_owned(),
                only_in_reference: count(&format!(
                    "SELECT count(*) FROM (SELECT * FROM ref.{table} EXCEPT ALL SELECT * FROM run.{table})"
                ))?,
                only_in_run: count(&format!(
                    "SELECT count(*) FROM (SELECT * FROM run.{table} EXCEPT ALL SELECT * FROM ref.{table})"
                ))?,
            })
        })
        .collect::<Result<_, duckdb::Error>>()?;
    Ok(WorkspaceComparison {
        tables,
        pending_rows: count("SELECT count(*) FROM run.pending")?,
    })
}

pub(crate) fn literal(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "''"))
}

// The NATS configuration of compose.rs defines no accounts, so JetStream keeps every stream in a
// directory of its own under the global account $G of the file store.
pub fn stream_left(nats_store: &Path, run_id: &str) -> bool {
    nats_store
        .join("jetstream")
        .join("$G")
        .join("streams")
        .join(stream_name(run_id))
        .exists()
}
