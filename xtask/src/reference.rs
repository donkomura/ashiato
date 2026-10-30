use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use ashiato_core::analysis::not_streamable_rules;
use ashiato_core::query::query_signal_rule;
use ashiato_core::rule::{Rule, RuleSource, Rules, SignalRule};
use ashiato_core::ruleset::Rulesets;
use ashiato_core::{AnalysisWindow, Duration, SignalRow};
use ashiato_duckdb::{DuckDbBackend, EventStore, IngestError};
use ashiato_testkit::dataset::{DatasetSummary, ExpectedScenario};
use duckdb::Connection;
use duckdb::types::{TimeUnit, Value};

use crate::compare::literal;
use crate::report::ReferenceCheck;

pub const SCENARIO_CHECK: &str = "scenario_match が正解の一覧と一致する";
pub const SIGNAL_CHECK: &str = "signals がバッチ処理と一致する";

const SUMMARY_FILE: &str = "dataset.json";
const BATCH_DATABASE: &str = "batch.duckdb";
const STAGING_DIR: &str = "staging";
const LOG_SUFFIX: &str = ".json.gz";
// The columns of signals except batch_start, which only the pipeline has.
const SIGNAL_COLUMNS: &str = "rule_id, start_time, end_time, event_ids, access_key_id, principal_key, source_ip, \
     account_id, region, attrs";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScenarioComparison {
    pub missing: Vec<ExpectedScenario>,
    pub unexpected: Vec<ExpectedScenario>,
}

impl ScenarioComparison {
    pub fn matches(&self) -> bool {
        self.missing.is_empty() && self.unexpected.is_empty()
    }
}

// Only the fields that the generator writes are compared, as in the dataset test of ashiato-cli.
pub fn compare_scenarios(
    output: &[u8],
    expected: &[ExpectedScenario],
) -> Result<ScenarioComparison, serde_json::Error> {
    let mut missing = expected.to_vec();
    let mut unexpected = Vec::new();
    for line in output
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let record: serde_json::Value = serde_json::from_slice(line)?;
        if record["type"] != "scenario_match" {
            continue;
        }
        let found: ExpectedScenario = serde_json::from_value(record)?;
        match missing.iter().position(|scenario| *scenario == found) {
            Some(index) => {
                missing.remove(index);
            }
            None => unexpected.push(found),
        }
    }
    Ok(ScenarioComparison {
        missing,
        unexpected,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleDifference {
    pub rule_id: String,
    pub only_in_reference: u64,
    pub only_in_batch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalComparison {
    pub batch_signals: u64,
    pub differences: Vec<RuleDifference>,
}

impl SignalComparison {
    // Dataset A always gives signals, so a batch processing that found none proves nothing.
    pub fn matches(&self) -> bool {
        self.batch_signals > 0 && self.differences.is_empty()
    }
}

pub fn scenario_check(compared: Result<ScenarioComparison, String>) -> ReferenceCheck {
    let _ = compared;
    todo!()
}

pub fn signal_check(compared: Result<SignalComparison, String>) -> ReferenceCheck {
    let _ = compared;
    todo!()
}

// The batch processing of 14.1.3 節 puts every event of the dataset into one table and runs the
// signal SQL over the whole analysis window, as `ashiato ingest` and `ashiato analyze` do.
pub fn compare_signals(
    reference: &Path,
    dataset: &Path,
    work_dir: &Path,
) -> anyhow::Result<SignalComparison> {
    let summary_path = dataset.join(SUMMARY_FILE);
    let summary: DatasetSummary = serde_json::from_slice(
        &fs::read(&summary_path)
            .with_context(|| format!("failed to read {}", summary_path.display()))?,
    )?;
    let database = work_dir.join(BATCH_DATABASE);
    if database.exists() {
        bail!("{} is left from an earlier comparison", database.display());
    }
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(&format!(
        "ATTACH {} AS ref (READ_ONLY); CREATE TABLE batch AS SELECT {SIGNAL_COLUMNS} FROM ref.signals LIMIT 0;",
        literal(reference)
    ))?;

    ingest_readable_files(dataset, &work_dir.join(STAGING_DIR), &database)?;
    let window = AnalysisWindow::new(
        summary.since,
        summary.until,
        summary.lookback.parse::<Duration>()?.to_time_delta(),
    )?;
    let rules = streamed_rules(summary.batch.parse()?)?;
    let signals = batch_signals(&database, &window, &rules)?;
    insert_signals(&connection, &signals)?;
    Ok(SignalComparison {
        batch_signals: signals.len() as u64,
        differences: differences(&connection)?,
    })
}

// The same rules as the reference run, whose plan has rules: [builtin] and no rulesets, so
// that `ashiato run` selects the default ruleset and leaves out the rules it cannot stream.
fn streamed_rules(batch: Duration) -> anyhow::Result<Vec<SignalRule>> {
    let rules = Rules::new([RuleSource::builtin()?])?;
    let mut rulesets = Rulesets::builtin()?;
    let unlisted: Vec<String> = rulesets
        .unlisted(&rules)
        .into_iter()
        .map(str::to_owned)
        .collect();
    rulesets.add_to_default(unlisted);
    let selection = rulesets.select(&rules, &[], &[])?;
    let skipped = not_streamable_rules(&selection.rules, batch);
    Ok(selection
        .rules
        .iter()
        .filter_map(|rule| match rule {
            Rule::Signal(rule) => Some(rule),
            Rule::Scenario(_) => None,
        })
        .filter(|rule| skipped.iter().all(|skipped| skipped.rule_id != rule.id))
        .cloned()
        .collect())
}

// Dataset A has broken gzip and JSON files, which make EventStore stop the whole directory. Each
// file is ingested through a link of its own, and a file that DuckDB cannot read is left out,
// just as the pipeline records it as a file error without its events.
fn ingest_readable_files(dataset: &Path, staging: &Path, database: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(staging)?;
    let store = EventStore::open(database)?;
    let link = staging.join(format!("file{LOG_SUFFIX}"));
    for file in log_files(&fs::canonicalize(dataset)?)? {
        std::os::unix::fs::symlink(&file, &link)?;
        let ingested = store.ingest_directory(staging);
        fs::remove_file(&link)?;
        match ingested {
            Ok(_) | Err(IngestError::DuckDb(_)) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn log_files(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut dirs = vec![dir.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.to_string_lossy().ends_with(LOG_SUFFIX) {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn batch_signals(
    database: &Path,
    window: &AnalysisWindow,
    rules: &[SignalRule],
) -> anyhow::Result<Vec<(String, SignalRow)>> {
    let backend = DuckDbBackend::open(database)?;
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let mut signals = Vec::new();
    for rule in rules {
        for mut row in runtime.block_on(query_signal_rule(&backend, rule, window))? {
            // The pipeline sorts the event IDs that array_agg returns in no particular order.
            row.event_ids.sort();
            signals.push((rule.id.clone(), row));
        }
    }
    Ok(signals)
}

fn insert_signals(connection: &Connection, signals: &[(String, SignalRow)]) -> anyhow::Result<()> {
    let transaction = connection.unchecked_transaction()?;
    let mut statement = transaction.prepare(&format!(
        "INSERT INTO batch ({SIGNAL_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ))?;
    for (rule_id, row) in signals {
        statement.execute(duckdb::params![
            rule_id,
            Value::Timestamp(TimeUnit::Microsecond, row.start_time.timestamp_micros()),
            Value::Timestamp(TimeUnit::Microsecond, row.end_time.timestamp_micros()),
            serde_json::to_string(&row.event_ids)?,
            row.access_key_id,
            row.principal_key,
            row.source_ip,
            row.account_id,
            row.region,
            serde_json::to_string(&row.attrs)?,
        ])?;
    }
    drop(statement);
    transaction.commit()?;
    Ok(())
}

fn differences(connection: &Connection) -> Result<Vec<RuleDifference>, duckdb::Error> {
    let mut statement = connection.prepare(&format!(
        "SELECT rule_id, count(*) FILTER (WHERE side = 'ref'), count(*) FILTER (WHERE side = 'batch') \
         FROM (SELECT rule_id, 'ref' AS side FROM \
                   (SELECT {SIGNAL_COLUMNS} FROM ref.signals EXCEPT ALL SELECT {SIGNAL_COLUMNS} FROM batch) \
               UNION ALL \
               SELECT rule_id, 'batch' FROM \
                   (SELECT {SIGNAL_COLUMNS} FROM batch EXCEPT ALL SELECT {SIGNAL_COLUMNS} FROM ref.signals)) \
         GROUP BY rule_id ORDER BY rule_id"
    ))?;
    statement
        .query_map([], |row| {
            Ok(RuleDifference {
                rule_id: row.get(0)?,
                only_in_reference: row.get(1)?,
                only_in_batch: row.get(2)?,
            })
        })?
        .collect()
}
