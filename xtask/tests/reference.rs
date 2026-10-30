use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ashiato_core::analysis::not_streamable_rules;
use ashiato_core::rule::{Rule, RuleSource, Rules, SignalRule};
use ashiato_core::ruleset::Rulesets;
use ashiato_core::{AnalysisWindow, Duration};
use ashiato_duckdb::{Analyzer, AnalyzerSettings, PipelineStore};
use ashiato_ingest::{
    DEFAULT_PREFETCH_FILES, FileSource, ListScope, LocalSource, PartitionAssignment,
    WorkerSettings, list_local, run_workers,
};
use ashiato_pipeline::partition_channels;
use ashiato_testkit::dataset::{
    DatasetKind, DatasetSpec, DatasetSummary, ExpectedOrigin, ExpectedScenario, generate,
};
use chrono::{TimeDelta, TimeZone, Utc};
use serde_json::json;
use tempfile::TempDir;
use xtask::reference::{
    RuleDifference, ScenarioComparison, SignalComparison, compare_scenarios, compare_signals,
};

// Small enough for cargo test, while every scenario and special file is still in the dataset.
const SCALE: f64 = 0.005;
const SEED: u64 = 32;

fn scenario(id: &str, origin: &str, kind: &str, minute: i64) -> ExpectedScenario {
    let start = Utc.with_ymd_and_hms(2026, 9, 9, 10, 0, 0).unwrap() + TimeDelta::minutes(minute);
    ExpectedScenario {
        scenario_id: id.to_owned(),
        origin: ExpectedOrigin {
            id: origin.to_owned(),
            kind: kind.to_owned(),
        },
        start_time: start,
        end_time: start + TimeDelta::minutes(30),
    }
}

// A scenario_match line has more fields than the generator writes, and they are not compared.
fn match_line(scenario: &ExpectedScenario) -> String {
    let mut line = serde_json::to_value(scenario).unwrap();
    line["type"] = json!("scenario_match");
    line["severity"] = json!("high");
    line["steps"] = json!([{ "signal_id": "s", "event_ids": ["e"] }]);
    line["lineage"] = json!([]);
    line.to_string()
}

fn output(lines: &[String]) -> Vec<u8> {
    lines
        .iter()
        .map(|line| format!("{line}\n"))
        .collect::<String>()
        .into_bytes()
}

#[test]
fn scenario_matches_in_any_order_among_other_records_match_the_expected_list() {
    let expected = [
        scenario("console_takeover_mining", "arn:a", "iam_user", 0),
        scenario("role_chain_escalation", "arn:b", "root", 60),
    ];
    let lines = [
        json!({ "type": "pipeline_run", "ingested_files": 3 }).to_string(),
        match_line(&expected[1]),
        json!({ "type": "rule_skipped", "rule_id": "s3_delete_burst" }).to_string(),
        match_line(&expected[0]),
        json!({ "type": "origin_score", "origin": { "id": "arn:a", "kind": "iam_user" } })
            .to_string(),
    ];

    let comparison = compare_scenarios(&output(&lines), &expected).unwrap();

    assert_eq!(
        comparison,
        ScenarioComparison {
            missing: Vec::new(),
            unexpected: Vec::new(),
        }
    );
    assert!(comparison.matches());
}

#[test]
fn scenario_matches_are_compared_with_their_multiplicity_and_every_field() {
    let a = scenario("console_takeover_mining", "arn:a", "iam_user", 0);
    let b = scenario("role_chain_escalation", "arn:b", "root", 60);
    let c = scenario("leaked_key_recon_persistence", "AKIAC", "iam_user", 120);
    let mut other_kind = c.clone();
    other_kind.origin.kind = "unknown".to_owned();
    let mut later_end = b.clone();
    later_end.end_time += TimeDelta::seconds(1);
    let lines = [
        match_line(&a),
        match_line(&other_kind),
        match_line(&a),
        match_line(&later_end),
    ];

    let comparison =
        compare_scenarios(&output(&lines), &[a.clone(), b.clone(), c.clone()]).unwrap();

    assert_eq!(
        comparison,
        ScenarioComparison {
            missing: vec![b, c],
            unexpected: vec![other_kind, a, later_end],
        }
    );
    assert!(!comparison.matches());
}

#[test]
fn an_output_without_scenario_matches_misses_every_expected_scenario() {
    let expected = [scenario("console_takeover_mining", "arn:a", "iam_user", 0)];

    let comparison = compare_scenarios(b"", &expected).unwrap();

    assert_eq!(comparison.missing, expected);
    assert!(comparison.unexpected.is_empty());
    assert!(!comparison.matches());
}

#[test]
fn an_output_that_is_not_json_lines_cannot_be_compared() {
    let expected = [scenario("console_takeover_mining", "arn:a", "iam_user", 0)];

    assert!(compare_scenarios(b"{\"type\": \"run\"}\nnot json\n", &expected).is_err());
    assert!(compare_scenarios(b"{\"type\": \"scenario_match\"}\n", &expected).is_err());
}

#[test]
fn signals_match_only_when_the_batch_processing_found_some_and_no_rule_differs() {
    let difference = RuleDifference {
        rule_id: "root_console_login".to_owned(),
        only_in_reference: 0,
        only_in_batch: 1,
    };
    let cases = [
        (3, Vec::new(), true),
        (0, Vec::new(), false),
        (3, vec![difference], false),
    ];
    for (batch_signals, differences, expected) in cases {
        let comparison = SignalComparison {
            batch_signals,
            differences,
        };
        assert_eq!(comparison.matches(), expected, "{comparison:?}");
    }
}

fn duration(text: &str) -> Duration {
    text.parse().unwrap()
}

// The signal rules that `ashiato run` passes to the analyzer for a plan with rules: [builtin]
// and no rulesets, which is the plan of the reference run.
fn streamed_rules(batch: Duration) -> Vec<SignalRule> {
    let rules = Rules::new([RuleSource::builtin().unwrap()]).unwrap();
    let mut rulesets = Rulesets::builtin().unwrap();
    let unlisted: Vec<String> = rulesets
        .unlisted(&rules)
        .into_iter()
        .map(str::to_owned)
        .collect();
    rulesets.add_to_default(unlisted);
    let selection = rulesets.select(&rules, &[], &[]).unwrap();
    let skipped = not_streamable_rules(&selection.rules, batch);
    selection
        .rules
        .iter()
        .filter_map(|rule| match rule {
            Rule::Signal(rule) => Some(rule),
            Rule::Scenario(_) => None,
        })
        .filter(|rule| skipped.iter().all(|skipped| skipped.rule_id != rule.id))
        .cloned()
        .collect()
}

// The single_process placement of `ashiato run` with the settings that the generator wrote.
async fn reference_run(data: &Path, summary: &DatasetSummary, database: &Path) {
    let batch = duration(&summary.batch);
    let allowed_lateness = duration(&summary.allowed_lateness).to_time_delta();
    let window = AnalysisWindow::new(
        summary.since,
        summary.until,
        duration(&summary.lookback).to_time_delta(),
    )
    .unwrap();
    let scope = ListScope {
        accounts: Some(&summary.accounts),
        regions: Some(&summary.regions),
        period: window.baseline_since()..window.until(),
    };
    let manifest = list_local(data, &scope).unwrap().manifest;
    let settings = AnalyzerSettings {
        batch: batch.to_time_delta(),
        allowed_lateness,
        read_since: window.baseline_since(),
        fetch_max: 16,
        commit_every: 64,
    };
    let mut analyzer = Analyzer::open(
        PipelineStore::open(database).unwrap(),
        manifest.clone(),
        settings,
    )
    .unwrap()
    .with_queries(streamed_rules(batch), window)
    .with_max_skew(Some(duration(&summary.max_skew).to_time_delta()));
    let assignments = manifest
        .partitions
        .iter()
        .zip(analyzer.resume_positions())
        .enumerate()
        .map(|(index, (partition, start))| PartitionAssignment {
            index: u32::try_from(index).unwrap(),
            manifest: partition.clone(),
            start,
        })
        .collect();
    let (writer, mut reader) = partition_channels(
        manifest
            .partitions
            .iter()
            .map(|partition| partition.partition.clone()),
        64,
    );
    let worker = tokio::spawn(run_workers(
        vec![(assignments, Arc::default())],
        Arc::new(LocalSource) as Arc<dyn FileSource>,
        Arc::new(writer),
        WorkerSettings {
            allowed_lateness,
            max_message_size: 1 << 20,
            decode_threads: NonZeroUsize::new(2).unwrap(),
            prefetch_files: DEFAULT_PREFETCH_FILES,
        },
    ));
    analyzer.run(&mut reader).await.unwrap();
    worker.await.unwrap().unwrap();
}

struct Reference {
    dir: TempDir,
}

impl Reference {
    fn run(kind: DatasetKind) -> Self {
        let dir = TempDir::new().unwrap();
        let reference = Self { dir };
        let spec = DatasetSpec {
            kind,
            seed: SEED,
            scale: SCALE,
        };
        let summary = generate(&spec, &reference.data()).unwrap();
        std::fs::create_dir_all(reference.database().parent().unwrap()).unwrap();
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(reference_run(
                &reference.data(),
                &summary,
                &reference.database(),
            ));
        reference
    }

    fn data(&self) -> PathBuf {
        self.dir.path().join("data")
    }

    fn database(&self) -> PathBuf {
        self.dir.path().join("r0").join("ashiato.duckdb")
    }

    fn work_dir(&self) -> PathBuf {
        self.dir.path().join("batch")
    }

    fn execute(&self, sql: &str) {
        duckdb::Connection::open(self.database())
            .unwrap()
            .execute_batch(sql)
            .unwrap();
    }

    fn signal_rule_ids(&self) -> Vec<String> {
        let connection = duckdb::Connection::open(self.database()).unwrap();
        let mut statement = connection
            .prepare("SELECT DISTINCT rule_id FROM signals ORDER BY rule_id")
            .unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    fn signals(&self) -> u64 {
        duckdb::Connection::open(self.database())
            .unwrap()
            .query_row("SELECT count(*) FROM signals", [], |row| row.get(0))
            .unwrap()
    }
}

fn assert_signals_match_the_batch_processing(kind: DatasetKind) {
    let reference = Reference::run(kind);

    let comparison = compare_signals(
        &reference.database(),
        &reference.data(),
        &reference.work_dir(),
    )
    .unwrap();

    assert_eq!(
        comparison,
        SignalComparison {
            batch_signals: reference.signals(),
            differences: Vec::new(),
        }
    );
    assert!(comparison.matches());
}

#[test]
fn the_signals_of_the_reference_run_of_dataset_a_match_the_batch_processing() {
    assert_signals_match_the_batch_processing(DatasetKind::A);
}

#[test]
fn the_signals_of_the_reference_run_of_dataset_a_without_data_events_match_the_batch_processing() {
    assert_signals_match_the_batch_processing(DatasetKind::ANoDataEvents);
}

// batch_start is a column of the pipeline alone, so moving every row to another batch changes
// nothing, while a lost or a repeated row is counted for its rule.
#[test]
fn rows_lost_or_repeated_by_the_reference_run_are_counted_for_each_rule() {
    let reference = Reference::run(DatasetKind::A);
    let rules = reference.signal_rule_ids();
    assert!(rules.len() >= 2, "{rules:?}");
    let (lost, repeated) = (&rules[0], &rules[rules.len() - 1]);
    let batch_signals = reference.signals();
    reference.execute(&format!(
        "UPDATE signals SET batch_start = TIMESTAMP '2000-01-01 00:00:00';
         DELETE FROM signals WHERE rowid = (SELECT min(rowid) FROM signals WHERE rule_id = '{lost}');
         INSERT INTO signals SELECT * FROM signals WHERE rule_id = '{repeated}' LIMIT 1;"
    ));

    let comparison = compare_signals(
        &reference.database(),
        &reference.data(),
        &reference.work_dir(),
    )
    .unwrap();

    assert_eq!(
        comparison,
        SignalComparison {
            batch_signals,
            differences: vec![
                RuleDifference {
                    rule_id: lost.clone(),
                    only_in_reference: 0,
                    only_in_batch: 1,
                },
                RuleDifference {
                    rule_id: repeated.clone(),
                    only_in_reference: 1,
                    only_in_batch: 0,
                },
            ],
        }
    );
    assert!(!comparison.matches());
}

#[test]
fn signals_cannot_be_compared_without_the_reference_or_the_dataset_or_with_a_used_work_dir() {
    let reference = Reference::run(DatasetKind::A);
    let dir = reference.dir.path();
    let missing = dir.join("missing");

    assert!(
        compare_signals(
            &missing.join("ashiato.duckdb"),
            &reference.data(),
            &dir.join("w1")
        )
        .is_err()
    );
    assert!(compare_signals(&reference.database(), &missing, &dir.join("w2")).is_err());
    compare_signals(
        &reference.database(),
        &reference.data(),
        &reference.work_dir(),
    )
    .unwrap();
    assert!(
        compare_signals(
            &reference.database(),
            &reference.data(),
            &reference.work_dir()
        )
        .is_err()
    );
}
