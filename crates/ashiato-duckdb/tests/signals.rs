use std::fs::{self, File};
use std::io::Write;

use ashiato_core::AnalysisWindow;
use ashiato_core::SignalRow;
use ashiato_core::macros::Dialect;
use ashiato_core::query::{
    BackendError, DataAvailability, QueryBackend, SignalQueryError, query_signal_rule,
};
use ashiato_core::rule::{Rule, RuleFile, RuleSource, SignalRule};
use ashiato_duckdb::{DuckDbBackend, EventStore};
use ashiato_testkit::record::RecordBuilder;
use chrono::{DateTime, TimeDelta, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use tempfile::TempDir;

const LOG_DIR: &str = "AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01";

const S3_DELETE_BURST: &str = r#"
kind: signal
id: s3_delete_burst
title: 短時間に集中した S3 オブジェクトの削除
severity: high
tactics: [TA0040]
requires: [s3_data_events]
params:
  min_deletes: 3
  bin: 5m
attrs: [bucket]
sql: |
  SELECT
    min(event_time)          AS start_time,
    max(event_time)          AS end_time,
    array_agg(event_id)      AS event_ids,
    access_key_id,
    principal_key,
    min(source_ip)           AS source_ip,
    recipient_account_id     AS account_id,
    aws_region               AS region,
    {{ json(request_parameters, '$.bucketName') }} AS attr_bucket
  FROM events
  WHERE {{ in_analysis_window(event_time) }}
    AND event_source = 's3.amazonaws.com'
    AND event_name IN ('DeleteObject', 'DeleteObjects')
    AND error_code IS NULL
  GROUP BY access_key_id, principal_key, recipient_account_id, aws_region,
           {{ json(request_parameters, '$.bucketName') }},
           {{ time_bin(event_time, param('bin')) }}
  HAVING count(*) >= {{ param('min_deletes') }}
"#;

struct Workspace {
    _dir: TempDir,
    backend: DuckDbBackend,
}

fn workspace(records: &[Value]) -> Workspace {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("source");
    let log = source.join(LOG_DIR).join("log.json.gz");
    fs::create_dir_all(log.parent().unwrap()).unwrap();
    let mut encoder = GzEncoder::new(File::create(&log).unwrap(), Compression::default());
    encoder
        .write_all(json!({ "Records": records }).to_string().as_bytes())
        .unwrap();
    encoder.finish().unwrap();

    let db_path = dir.path().join("ashiato.duckdb");
    EventStore::open(&db_path)
        .unwrap()
        .ingest_directory(&source)
        .unwrap();
    let backend = DuckDbBackend::open(&db_path).unwrap();
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

fn parse_signal_rule(content: &str) -> SignalRule {
    let set = RuleSource::parse(
        "test",
        vec![RuleFile {
            path: "rule.yaml".to_owned(),
            content: content.to_owned(),
        }],
    )
    .unwrap();
    match set.rules() {
        [Rule::Signal(rule)] => rule.clone(),
        other => panic!("expected one signal rule, got {other:?}"),
    }
}

fn single_event_rule(select: &str) -> SignalRule {
    let indented: String = select.lines().map(|line| format!("  {line}\n")).collect();
    parse_signal_rule(&format!(
        "kind: signal\n\
         id: single_event\n\
         title: test\n\
         severity: low\n\
         tactics: [TA0007]\n\
         sql: |\n{indented}"
    ))
}

fn delete_object(event_id: &str, event_time: &str, bucket: &str) -> Value {
    RecordBuilder::new()
        .set("eventID", event_id)
        .set("eventTime", event_time)
        .set("eventSource", "s3.amazonaws.com")
        .set("eventName", "DeleteObject")
        .set("eventCategory", "Data")
        .set("readOnly", false)
        .set("awsRegion", "ap-northeast-1")
        .set("sourceIPAddress", "203.0.113.7")
        .set(
            "requestParameters",
            json!({ "bucketName": bucket, "key": format!("{event_id}.txt") }),
        )
        .build()
}

async fn s3_delete_burst(records: &[Value]) -> Vec<SignalRow> {
    let Workspace { backend, .. } = workspace(records);
    query_signal_rule(&backend, &parse_signal_rule(S3_DELETE_BURST), &window())
        .await
        .unwrap()
}

#[test]
fn duckdb_backend_uses_the_duckdb_dialect() {
    let Workspace { backend, .. } = workspace(&[]);

    assert_eq!(backend.dialect(), Dialect::DuckDb);
}

#[tokio::test]
async fn s3_delete_burst_is_not_raised_below_the_threshold() {
    let signals = s3_delete_burst(&[
        delete_object("evt-1", "2026-10-01T10:00:00Z", "evidence"),
        delete_object("evt-2", "2026-10-01T10:01:00Z", "evidence"),
    ])
    .await;

    assert_eq!(signals, Vec::new());
}

#[tokio::test]
async fn s3_delete_burst_is_raised_at_the_threshold() {
    let mut signals = s3_delete_burst(&[
        delete_object("evt-1", "2026-10-01T10:00:00Z", "evidence"),
        delete_object("evt-2", "2026-10-01T10:01:00Z", "evidence"),
        delete_object("evt-3", "2026-10-01T10:02:30Z", "evidence"),
    ])
    .await;

    assert_eq!(signals.len(), 1, "unexpected signals: {signals:?}");
    let signal = signals.remove(0);
    let mut event_ids = signal.event_ids.clone();
    event_ids.sort();
    assert_eq!(event_ids, ["evt-1", "evt-2", "evt-3"]);
    assert_eq!(
        signal.start_time,
        "2026-10-01T10:00:00Z".parse::<DateTime<Utc>>().unwrap()
    );
    assert_eq!(
        signal.end_time,
        "2026-10-01T10:02:30Z".parse::<DateTime<Utc>>().unwrap()
    );
    assert_eq!(
        signal.access_key_id.as_deref(),
        Some("AKIAEXAMPLEACCESSKEY")
    );
    assert_eq!(signal.principal_key, "arn:aws:iam::111111111111:user/alice");
    assert_eq!(signal.source_ip, "203.0.113.7");
    assert_eq!(signal.account_id, "111111111111");
    assert_eq!(signal.region, "ap-northeast-1");
    assert_eq!(
        signal.attrs,
        [("bucket".to_owned(), "evidence".to_owned())].into()
    );
}

#[tokio::test]
async fn s3_delete_burst_counts_each_bucket_separately() {
    let signals = s3_delete_burst(&[
        delete_object("evt-1", "2026-10-01T10:00:00Z", "evidence"),
        delete_object("evt-2", "2026-10-01T10:01:00Z", "evidence"),
        delete_object("evt-3", "2026-10-01T10:02:00Z", "backups"),
    ])
    .await;

    assert_eq!(signals, Vec::new());
}

#[tokio::test]
async fn s3_delete_burst_ignores_events_outside_the_analysis_window() {
    let signals = s3_delete_burst(&[
        delete_object("evt-1", "2026-09-30T23:58:00Z", "evidence"),
        delete_object("evt-2", "2026-10-01T00:00:00Z", "evidence"),
        delete_object("evt-3", "2026-10-01T00:01:00Z", "evidence"),
    ])
    .await;

    assert_eq!(signals, Vec::new());
}

#[tokio::test]
async fn null_access_key_id_becomes_none_and_null_attrs_are_omitted() {
    let record = RecordBuilder::new()
        .set("eventID", "evt-service")
        .set("userIdentity.type", "AWSService")
        .set("userIdentity.invokedBy", "cloudtrail.amazonaws.com")
        .set("userIdentity.accessKeyId", Value::Null)
        .build();
    let Workspace { backend, .. } = workspace(&[record]);
    let rule = single_event_rule(
        "SELECT event_time AS start_time, event_time AS end_time, [event_id] AS event_ids,\n\
         access_key_id, principal_key, source_ip, recipient_account_id AS account_id,\n\
         aws_region AS region, invoked_by AS attr_invoked_by, NULL AS attr_missing\n\
         FROM events",
    );

    let signals = query_signal_rule(&backend, &rule, &window()).await.unwrap();

    assert_eq!(signals.len(), 1, "unexpected signals: {signals:?}");
    assert_eq!(signals[0].access_key_id, None);
    assert_eq!(signals[0].principal_key, "service:cloudtrail.amazonaws.com");
    assert_eq!(signals[0].event_ids, ["evt-service"]);
    assert_eq!(
        signals[0].attrs,
        [(
            "invoked_by".to_owned(),
            "cloudtrail.amazonaws.com".to_owned()
        )]
        .into()
    );
}

#[tokio::test]
async fn a_missing_required_column_is_an_error_with_the_rule_id() {
    let Workspace { backend, .. } = workspace(&[RecordBuilder::new().build()]);
    let rule = single_event_rule(
        "SELECT event_time AS start_time, event_time AS end_time, [event_id] AS event_ids,\n\
         access_key_id, principal_key, source_ip, recipient_account_id AS account_id\n\
         FROM events",
    );

    let error = query_signal_rule(&backend, &rule, &window())
        .await
        .unwrap_err();

    assert_eq!(error.rule_id(), "single_event");
    assert!(
        matches!(
            &error,
            SignalQueryError::Backend {
                source: BackendError::MissingColumn { column },
                ..
            } if column == "region"
        ),
        "unexpected error: {error:?}"
    );
}

#[tokio::test]
async fn a_missing_required_column_is_an_error_even_without_rows() {
    let Workspace { backend, .. } = workspace(&[]);
    let rule = single_event_rule("SELECT event_time AS start_time FROM events");

    let error = query_signal_rule(&backend, &rule, &window())
        .await
        .unwrap_err();

    assert!(
        matches!(
            &error,
            SignalQueryError::Backend {
                source: BackendError::MissingColumn { .. },
                ..
            }
        ),
        "unexpected error: {error:?}"
    );
}

#[tokio::test]
async fn invalid_sql_is_a_query_error_with_the_rule_id() {
    let Workspace { backend, .. } = workspace(&[]);
    let rule = single_event_rule("SELEC 1");

    let error = query_signal_rule(&backend, &rule, &window())
        .await
        .unwrap_err();

    assert_eq!(error.rule_id(), "single_event");
    assert!(
        matches!(
            &error,
            SignalQueryError::Backend {
                source: BackendError::Query(_),
                ..
            }
        ),
        "unexpected error: {error:?}"
    );
}

#[tokio::test]
async fn query_signals_can_be_called_on_a_workspace_without_events() {
    let dir = TempDir::new().unwrap();
    let backend = DuckDbBackend::open(&dir.path().join("ashiato.duckdb")).unwrap();

    let signals = backend
        .query_signals(
            "SELECT event_time AS start_time, event_time AS end_time, [event_id] AS event_ids,\n\
             access_key_id, principal_key, source_ip, recipient_account_id AS account_id,\n\
             aws_region AS region FROM events",
        )
        .await
        .unwrap();

    assert_eq!(signals, Vec::new());
}

fn list_buckets(event_id: &str, event_time: &str) -> Value {
    RecordBuilder::new()
        .set("eventID", event_id)
        .set("eventTime", event_time)
        .set("eventSource", "s3.amazonaws.com")
        .set("eventName", "ListBuckets")
        .set("eventCategory", "Management")
        .build()
}

#[tokio::test]
async fn data_availability_counts_the_events_of_the_analysis_window() {
    let Workspace { backend, .. } = workspace(&[
        list_buckets("evt-1", "2026-10-01T10:00:00Z"),
        list_buckets("evt-2", "2026-10-01T11:00:00Z"),
        list_buckets("evt-before", "2026-09-30T23:59:59Z"),
        list_buckets("evt-after", "2026-10-02T00:00:00Z"),
    ]);

    let data = backend.data_availability(&window()).await.unwrap();

    assert_eq!(
        data,
        DataAvailability {
            events: 2,
            s3_data_events: false,
        }
    );
}

#[tokio::test]
async fn s3_data_events_are_available_when_the_window_has_one() {
    let Workspace { backend, .. } = workspace(&[
        list_buckets("evt-list", "2026-10-01T10:00:00Z"),
        delete_object("evt-delete", "2026-10-01T10:01:00Z", "customer-data"),
    ]);

    let data = backend.data_availability(&window()).await.unwrap();

    assert!(data.s3_data_events, "{data:?}");
}

#[tokio::test]
async fn s3_data_events_outside_the_window_are_not_counted() {
    let Workspace { backend, .. } = workspace(&[
        list_buckets("evt-list", "2026-10-01T10:00:00Z"),
        delete_object("evt-delete", "2026-09-30T10:00:00Z", "customer-data"),
    ]);

    let data = backend.data_availability(&window()).await.unwrap();

    assert!(!data.s3_data_events, "{data:?}");
}

#[tokio::test]
async fn data_availability_of_a_workspace_without_events_is_empty() {
    let dir = TempDir::new().unwrap();
    let backend = DuckDbBackend::open(&dir.path().join("ashiato.duckdb")).unwrap();

    let data = backend.data_availability(&window()).await.unwrap();

    assert_eq!(data, DataAvailability::default());
}
