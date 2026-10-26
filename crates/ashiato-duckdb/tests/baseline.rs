use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::Write;

use ashiato_core::AnalysisWindow;
use ashiato_core::query::query_signal_rule;
use ashiato_core::record::RecordBuilder;
use ashiato_core::rule::{Rule, RuleFile, RuleSource, SignalRule};
use ashiato_duckdb::{DuckDbBackend, EventStore};
use chrono::TimeDelta;
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use tempfile::TempDir;

const ALICE_ARN: &str = "arn:aws:iam::111111111111:user/alice";
const BOB_ARN: &str = "arn:aws:iam::111111111111:user/bob";
const CAROL_ARN: &str = "arn:aws:iam::111111111111:user/carol";

const SIGNAL_COLUMNS: &str = "\
    TIMESTAMP '2026-10-01 00:00:00' AS start_time,
    TIMESTAMP '2026-10-01 00:00:00' AS end_time,
    ['baseline'] AS event_ids,
    CAST(NULL AS VARCHAR) AS access_key_id,
    principal_key,
    '-' AS source_ip,
    '-' AS account_id,
    '-' AS region";

struct Workspace {
    _dir: TempDir,
    backend: DuckDbBackend,
}

fn workspace(records: &[Value]) -> Workspace {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    let mut encoder = GzEncoder::new(
        File::create(source.join("log.json.gz")).unwrap(),
        Compression::default(),
    );
    encoder
        .write_all(json!({ "Records": records }).to_string().as_bytes())
        .unwrap();
    encoder.finish().unwrap();
    let database = dir.path().join("ashiato.duckdb");
    EventStore::open(&database)
        .unwrap()
        .ingest_directory(&source)
        .unwrap();
    let backend = DuckDbBackend::open(&database).unwrap();
    Workspace { _dir: dir, backend }
}

fn window() -> AnalysisWindow {
    AnalysisWindow::new(
        "2026-10-01T00:00:00Z".parse().unwrap(),
        "2026-10-02T00:00:00Z".parse().unwrap(),
        TimeDelta::days(30),
    )
    .unwrap()
}

fn rule(sql: &str) -> SignalRule {
    let indented: String = sql.lines().map(|line| format!("  {line}\n")).collect();
    let content = format!(
        "kind: signal\nid: baseline_probe\ntitle: test\nseverity: low\n\
         tactics: [TA0007]\nattrs: [dim, value]\nsql: |\n{indented}"
    );
    let set = RuleSource::parse(
        "test",
        vec![RuleFile {
            path: "baseline_probe/rule.yaml".to_owned(),
            content,
        }],
    )
    .unwrap();
    match set.rules() {
        [Rule::Signal(rule)] => rule.clone(),
        other => panic!("expected one signal rule, got {other:?}"),
    }
}

fn event(arn: &str, time: &str) -> RecordBuilder {
    RecordBuilder::new()
        .set("userIdentity.arn", arn)
        .set("eventTime", time)
}

fn records() -> Vec<Value> {
    vec![
        event(ALICE_ARN, "2026-09-15T00:00:00Z")
            .set("eventName", "ListBuckets")
            .set("sourceIPAddress", "198.51.100.1")
            .build(),
        event(ALICE_ARN, "2026-09-20T00:00:00Z")
            .set("eventName", "GetCallerIdentity")
            .set("sourceIPAddress", "198.51.100.2")
            .set("awsRegion", "ap-northeast-1")
            .build(),
        event(ALICE_ARN, "2026-10-01T10:00:00Z")
            .set("eventName", "CreateAccessKey")
            .set("sourceIPAddress", "203.0.113.9")
            .set("userAgent", "curl/8.0")
            .build(),
        event(BOB_ARN, "2026-10-01T10:00:00Z").build(),
        event(CAROL_ARN, "2026-08-01T00:00:00Z").build(),
    ]
}

async fn query(sql: &str) -> Vec<(String, Option<String>, Option<String>)> {
    let workspace = workspace(&records());
    let mut rows: Vec<_> = query_signal_rule(&workspace.backend, &rule(sql), &window())
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.principal_key,
                row.attrs.get("dim").cloned(),
                row.attrs.get("value").cloned(),
            )
        })
        .collect();
    rows.sort();
    rows
}

fn entry(dim: &str, value: &str) -> (String, Option<String>, Option<String>) {
    (
        ALICE_ARN.to_owned(),
        Some(dim.to_owned()),
        Some(value.to_owned()),
    )
}

#[tokio::test]
async fn baseline_lists_the_values_seen_per_principal_in_the_baseline_window() {
    let rows = query(&format!(
        "SELECT {SIGNAL_COLUMNS}, dim AS attr_dim, value AS attr_value FROM baseline"
    ))
    .await;

    let expected: BTreeSet<_> = [
        entry("event_name", "GetCallerIdentity"),
        entry("event_name", "ListBuckets"),
        entry("region", "ap-northeast-1"),
        entry("region", "us-east-1"),
        entry("source_ip", "198.51.100.1"),
        entry("source_ip", "198.51.100.2"),
        entry("user_agent", "aws-cli/2.0.0"),
    ]
    .into_iter()
    .collect();
    assert_eq!(rows.into_iter().collect::<BTreeSet<_>>(), expected);
}

#[tokio::test]
async fn baseline_principals_lists_principals_with_events_in_the_baseline_window() {
    let rows = query(&format!("SELECT {SIGNAL_COLUMNS} FROM baseline_principals")).await;

    assert_eq!(rows, [(ALICE_ARN.to_owned(), None, None)]);
}

#[tokio::test]
async fn rule_with_its_own_with_clause_can_refer_to_the_baseline() {
    let rows = query(&format!(
        "WITH known AS (SELECT principal_key FROM baseline_principals)\n\
         SELECT {SIGNAL_COLUMNS} FROM known"
    ))
    .await;

    assert_eq!(rows, [(ALICE_ARN.to_owned(), None, None)]);
}
