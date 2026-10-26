use std::collections::BTreeMap;
use std::sync::Mutex;

use ashiato_core::lineage::LineageEdge;
use ashiato_core::macros::{Dialect, MacroError};
use ashiato_core::query::{BackendError, QueryBackend, SignalQueryError, query_signal_rule};
use ashiato_core::rule::{Rule, RuleFile, RuleSource, SignalRule};
use ashiato_core::{AnalysisWindow, SignalRow};
use async_trait::async_trait;
use chrono::TimeDelta;

struct FakeBackend {
    received_sql: Mutex<Vec<String>>,
    response: fn() -> Result<Vec<SignalRow>, BackendError>,
}

impl FakeBackend {
    fn returning(response: fn() -> Result<Vec<SignalRow>, BackendError>) -> Self {
        Self {
            received_sql: Mutex::new(Vec::new()),
            response,
        }
    }

    fn received_sql(&self) -> Vec<String> {
        self.received_sql.lock().unwrap().clone()
    }
}

#[async_trait]
impl QueryBackend for FakeBackend {
    fn dialect(&self) -> Dialect {
        Dialect::DuckDb
    }

    async fn query_signals(&self, sql: &str) -> Result<Vec<SignalRow>, BackendError> {
        self.received_sql.lock().unwrap().push(sql.to_owned());
        (self.response)()
    }

    async fn query_lineage(
        &self,
        _window: &AnalysisWindow,
    ) -> Result<Vec<LineageEdge>, BackendError> {
        Ok(Vec::new())
    }
}

fn window() -> AnalysisWindow {
    AnalysisWindow::new(
        "2026-10-01T00:00:00Z".parse().unwrap(),
        "2026-10-02T00:00:00Z".parse().unwrap(),
        TimeDelta::days(30),
    )
    .unwrap()
}

fn signal_rule(sql: &str) -> SignalRule {
    let indented_sql: String = sql.lines().map(|line| format!("  {line}\n")).collect();
    let content = format!(
        "kind: signal\n\
         id: test_signal\n\
         title: test\n\
         severity: low\n\
         tactics: [TA0007]\n\
         params:\n  min_count: 3\n\
         sql: |\n{indented_sql}"
    );
    let set = RuleSource::parse(
        "test",
        vec![RuleFile {
            path: "test_signal/rule.yaml".to_owned(),
            content,
        }],
    )
    .unwrap();
    match set.rules() {
        [Rule::Signal(rule)] => rule.clone(),
        other => panic!("expected one signal rule, got {other:?}"),
    }
}

fn row() -> SignalRow {
    SignalRow {
        start_time: "2026-10-01T00:00:00Z".parse().unwrap(),
        end_time: "2026-10-01T00:01:00Z".parse().unwrap(),
        event_ids: vec!["evt-1".to_owned()],
        access_key_id: Some("AKIAEXAMPLEACCESSKEY".to_owned()),
        principal_key: "arn:aws:iam::111111111111:user/alice".to_owned(),
        source_ip: "198.51.100.1".to_owned(),
        account_id: "111111111111".to_owned(),
        region: "us-east-1".to_owned(),
        attrs: BTreeMap::from([("bucket".to_owned(), "evidence".to_owned())]),
    }
}

#[tokio::test]
async fn query_signal_rule_sends_the_sql_with_macros_expanded() {
    let backend = FakeBackend::returning(|| Ok(Vec::new()));
    let rule = signal_rule(
        "SELECT * FROM events\n\
         WHERE {{ in_analysis_window(event_time) }}\n\
         HAVING count(*) >= {{ param('min_count') }}",
    );

    query_signal_rule(&backend, &rule, &window()).await.unwrap();

    let received = backend.received_sql();
    assert_eq!(received.len(), 1);
    assert!(
        received[0].ends_with(
            "SELECT * FROM events\n\
             WHERE (event_time >= TIMESTAMP '2026-10-01 00:00:00' \
             AND event_time < TIMESTAMP '2026-10-02 00:00:00')\n\
             HAVING count(*) >= 3\n"
        ),
        "{}",
        received[0]
    );
}

#[tokio::test]
async fn query_signal_rule_puts_the_baseline_ctes_before_the_rule_sql() {
    let backend = FakeBackend::returning(|| Ok(Vec::new()));
    let rule = signal_rule("SELECT * FROM baseline");

    query_signal_rule(&backend, &rule, &window()).await.unwrap();

    let sql = &backend.received_sql()[0];
    assert!(sql.starts_with("WITH baseline AS ("), "{sql}");
    assert!(sql.contains("baseline_principals AS ("), "{sql}");
    assert!(
        sql.contains(
            "(event_time >= TIMESTAMP '2026-09-01 00:00:00' \
             AND event_time < TIMESTAMP '2026-10-01 00:00:00')"
        ),
        "the baseline CTEs must be limited to the baseline window: {sql}"
    );
    assert!(!sql.contains("{{"), "{sql}");
    assert!(sql.ends_with("\nSELECT * FROM baseline\n"), "{sql}");
}

#[tokio::test]
async fn query_signal_rule_merges_the_baseline_ctes_into_the_with_clause_of_the_rule() {
    let backend = FakeBackend::returning(|| Ok(Vec::new()));
    let rule = signal_rule("with recent AS (SELECT 1)\nSELECT * FROM recent");

    query_signal_rule(&backend, &rule, &window()).await.unwrap();

    let sql = &backend.received_sql()[0];
    assert!(sql.starts_with("WITH baseline AS ("), "{sql}");
    assert_eq!(sql.to_lowercase().matches("with ").count(), 1, "{sql}");
    assert!(
        sql.ends_with("recent AS (SELECT 1)\nSELECT * FROM recent\n"),
        "{sql}"
    );
}

#[tokio::test]
async fn query_signal_rule_returns_the_rows_from_the_backend() {
    let backend = FakeBackend::returning(|| Ok(vec![row()]));
    let rule = signal_rule("SELECT 1");

    let rows = query_signal_rule(&backend, &rule, &window()).await.unwrap();

    assert_eq!(rows, vec![row()]);
}

#[tokio::test]
async fn query_signal_rule_reports_macro_errors_with_the_rule_id() {
    let backend = FakeBackend::returning(|| Ok(Vec::new()));
    let rule = signal_rule("SELECT {{ param('undefined') }}");

    let error = query_signal_rule(&backend, &rule, &window())
        .await
        .unwrap_err();

    assert_eq!(error.rule_id(), "test_signal");
    assert!(
        matches!(
            &error,
            SignalQueryError::Macro {
                source: MacroError::UndefinedParam { name },
                ..
            } if name == "undefined"
        ),
        "unexpected error: {error:?}"
    );
    assert!(backend.received_sql().is_empty());
}

#[tokio::test]
async fn query_signal_rule_reports_backend_errors_with_the_rule_id() {
    let backend = FakeBackend::returning(|| {
        Err(BackendError::MissingColumn {
            column: "region".to_owned(),
        })
    });
    let rule = signal_rule("SELECT 1");

    let error = query_signal_rule(&backend, &rule, &window())
        .await
        .unwrap_err();

    assert_eq!(error.rule_id(), "test_signal");
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
    assert!(error.to_string().contains("test_signal"));
}
