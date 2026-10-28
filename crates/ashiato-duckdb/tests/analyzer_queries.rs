mod common;

use std::path::Path;

use ashiato_core::fixture::fixture_window;
use ashiato_core::lineage::LineageEdge;
use ashiato_core::query::{QueryBackend, query_signal_rule};
use ashiato_core::rule::{Rule, RuleSource, Rules, SignalRule};
use ashiato_core::sequence::ObservedSignal;
use ashiato_duckdb::{Analyzer, AnalyzerSettings, DuckDbBackend, EventStore, PipelineStore};
use ashiato_pipeline::{EventTime, Manifest};
use ashiato_testkit::fixture::builtin_fixtures;
use ashiato_testkit::record::RecordBuilder;
use chrono::TimeDelta;
use common::{LATENESS_MINUTES, Workspace, start_worker, write_records};
use serde_json::{Value, json};

// These rules aggregate without time_bin or read the baseline period of events directly, so
// they cannot be evaluated batch by batch (streaming-pipeline.md 8.4). `rules validate` reports
// them once its streaming checks are added (段階 21).
const NOT_BATCHABLE: [&str; 5] = [
    "cloudtrail_logging_paused",
    "first_seen_account_region_write",
    "role_session_outlier",
    "self_privilege_escalation",
    "temporary_credential_ip_change",
];

fn builtin_rules() -> Rules {
    Rules::new([RuleSource::builtin().unwrap()]).unwrap()
}

// Rules that use the baseline are compared once the baseline table is built (段階 17).
fn batchable_signal_rules(rules: &Rules) -> Vec<SignalRule> {
    rules
        .iter()
        .filter_map(|rule| match rule {
            Rule::Signal(rule) => Some(rule),
            Rule::Scenario(_) => None,
        })
        .filter(|rule| !NOT_BATCHABLE.contains(&rule.id.as_str()) && !rule.sql.contains("baseline"))
        .cloned()
        .collect()
}

fn fixture_records() -> Vec<Value> {
    builtin_fixtures()
        .unwrap()
        .into_iter()
        .enumerate()
        .flat_map(|(fixture, file)| {
            file.records
                .into_iter()
                .enumerate()
                .map(move |(index, mut record)| {
                    record["eventID"] = json!(format!("fixture-{fixture:04}-{index:04}"));
                    record
                })
        })
        .collect()
}

fn settings(batch: TimeDelta, fetch_max: usize) -> AnalyzerSettings {
    AnalyzerSettings {
        batch,
        allowed_lateness: TimeDelta::minutes(LATENESS_MINUTES),
        read_since: fixture_window().baseline_since(),
        fetch_max,
        commit_every: fetch_max,
    }
}

async fn run_pipeline(
    database: &Path,
    manifest: &Manifest,
    settings: AnalyzerSettings,
    rules: Vec<SignalRule>,
) -> PipelineStore {
    let mut analyzer = Analyzer::open(
        PipelineStore::open(database).unwrap(),
        manifest.clone(),
        settings,
    )
    .unwrap()
    .with_queries(rules, fixture_window());
    let (mut reader, worker) = start_worker(manifest, &analyzer.resume_positions());
    analyzer.run(&mut reader).await.unwrap();
    worker.await.unwrap().unwrap();
    analyzer.into_store()
}

// array_agg in the SQL of a rule returns event IDs in no particular order.
fn sorted(signals: &[ObservedSignal]) -> Vec<String> {
    let mut signals: Vec<String> = signals
        .iter()
        .map(|signal| {
            let mut row = signal.row.clone();
            row.event_ids.sort();
            format!("{} {:?}", signal.signal_id, row)
        })
        .collect();
    signals.sort();
    signals
}

async fn batch_signals(logs: &Path, database: &Path, rules: &[SignalRule]) -> Vec<ObservedSignal> {
    EventStore::open(database)
        .unwrap()
        .ingest_directory(logs)
        .unwrap();
    let backend = DuckDbBackend::open(database).unwrap();
    let mut observed = Vec::new();
    for rule in rules {
        for row in query_signal_rule(&backend, rule, &fixture_window())
            .await
            .unwrap()
        {
            observed.push(ObservedSignal {
                signal_id: rule.id.clone(),
                row,
            });
        }
    }
    observed
}

async fn batch_lineage(database: &Path) -> Vec<LineageEdge> {
    DuckDbBackend::open(database)
        .unwrap()
        .query_lineage(&fixture_window())
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_signals_of_each_batch_match_the_signals_of_all_events_at_once() {
    let workspace = Workspace::new();
    write_records(&workspace, fixture_records(), &fixture_window());
    let manifest = workspace.manifest();
    let rules = batchable_signal_rules(&builtin_rules());
    assert!(rules.len() > 20, "{} rules", rules.len());

    let store = run_pipeline(
        &workspace.database("pipeline"),
        &manifest,
        settings(TimeDelta::hours(1), 1),
        rules.clone(),
    )
    .await;
    let streamed = store.observed_signals().unwrap();
    let expected = batch_signals(workspace.logs(), &workspace.database("batch"), &rules).await;

    assert!(!expected.is_empty());
    assert_eq!(sorted(&streamed), sorted(&expected));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_signals_do_not_depend_on_the_batch_length_or_the_order_of_arrival() {
    let workspace = Workspace::new();
    write_records(&workspace, fixture_records(), &fixture_window());
    let manifest = workspace.manifest();
    let rules = batchable_signal_rules(&builtin_rules());

    let hourly = run_pipeline(
        &workspace.database("hourly"),
        &manifest,
        settings(TimeDelta::hours(1), 1),
        rules.clone(),
    )
    .await
    .observed_signals()
    .unwrap();
    let daily = run_pipeline(
        &workspace.database("daily"),
        &manifest,
        settings(TimeDelta::hours(24), 16),
        rules,
    )
    .await
    .observed_signals()
    .unwrap();

    assert_eq!(daily, hourly);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_lineage_edges_of_each_batch_match_the_lineage_of_all_events_at_once() {
    let workspace = Workspace::new();
    write_records(&workspace, fixture_records(), &fixture_window());
    let manifest = workspace.manifest();

    let store = run_pipeline(
        &workspace.database("pipeline"),
        &manifest,
        settings(TimeDelta::hours(1), 1),
        Vec::new(),
    )
    .await;
    let streamed = store.lineage_edges().unwrap();
    EventStore::open(&workspace.database("batch"))
        .unwrap()
        .ingest_directory(workspace.logs())
        .unwrap();
    let expected = batch_lineage(&workspace.database("batch")).await;

    assert!(!expected.is_empty());
    assert_eq!(streamed, expected);
}

fn assume_role(id: &str, time: EventTime, child: &str) -> Value {
    RecordBuilder::new()
        .set("eventID", id)
        .set("eventTime", time.to_rfc3339())
        .set("eventSource", "sts.amazonaws.com")
        .set("eventName", "AssumeRole")
        .set("userIdentity.type", "AssumedRole")
        .set(
            "userIdentity.arn",
            "arn:aws:sts::111111111111:assumed-role/roleA/session",
        )
        .set("userIdentity.accessKeyId", "ASIAPARENT")
        .set(
            "userIdentity.sessionContext.sessionIssuer.arn",
            "arn:aws:iam::111111111111:role/roleA",
        )
        .set(
            "requestParameters",
            json!({ "roleArn": "arn:aws:iam::111111111111:role/roleB", "roleSessionName": "s" }),
        )
        .set(
            "responseElements",
            json!({ "credentials": { "accessKeyId": child } }),
        )
        .build()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_baseline_period_gives_lineage_edges_but_no_signals() {
    let workspace = Workspace::new();
    let window = fixture_window();
    let in_baseline = window.since() - TimeDelta::days(3);
    let in_analysis = window.since() + TimeDelta::hours(2);
    write_records(
        &workspace,
        vec![
            assume_role("in-baseline", in_baseline, "ASIACHILDBASELINE"),
            assume_role("in-analysis", in_analysis, "ASIACHILDANALYSIS"),
        ],
        &window,
    );
    let manifest = workspace.manifest();
    let rules = builtin_rules();
    let Some(Rule::Signal(rule)) = rules.get("assume_role_by_assumed_role") else {
        panic!("assume_role_by_assumed_role is a builtin signal rule");
    };

    let store = run_pipeline(
        &workspace.database("pipeline"),
        &manifest,
        settings(TimeDelta::hours(1), 1),
        vec![rule.clone()],
    )
    .await;

    let signals: Vec<Vec<String>> = store
        .observed_signals()
        .unwrap()
        .into_iter()
        .map(|signal| signal.row.event_ids)
        .collect();
    assert_eq!(signals, vec![vec!["in-analysis".to_owned()]]);
    let edges: Vec<String> = store
        .lineage_edges()
        .unwrap()
        .into_iter()
        .map(|edge| edge.issued_by_event_id)
        .collect();
    assert_eq!(edges, vec!["in-baseline", "in-analysis"]);
}
