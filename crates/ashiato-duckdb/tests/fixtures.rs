use ashiato_core::fixture::{Expectation, Fixture, FixtureFile, builtin_fixtures};
use ashiato_core::rule::{RuleFile, RuleSet, Rules};
use ashiato_duckdb::fixture::{FixtureReport, FixtureRunError, Verdict, run_fixture};
use serde_json::{Value, json};

const CONSOLE_LOGIN: &str = r#"
kind: signal
id: console_login
title: コンソールへのログイン
severity: low
tactics: [TA0001]
sql: |
  SELECT
    event_time           AS start_time,
    event_time           AS end_time,
    [event_id]           AS event_ids,
    access_key_id,
    principal_key,
    source_ip,
    recipient_account_id AS account_id,
    aws_region           AS region
  FROM events
  WHERE {{ in_analysis_window(event_time) }}
    AND event_name = 'ConsoleLogin'
"#;

const REPEATED_CONSOLE_LOGIN: &str = r#"
kind: scenario
id: repeated_console_login
title: 繰り返されるコンソールへのログイン
severity: low
tactics: [TA0001]
window: 1h
steps:
  - signal: console_login
  - signal: console_login
"#;

fn rules() -> Rules {
    let set = RuleSet::parse(
        "test",
        vec![
            RuleFile {
                path: "console_login/rule.yaml".to_owned(),
                content: CONSOLE_LOGIN.to_owned(),
            },
            RuleFile {
                path: "repeated_console_login/rule.yaml".to_owned(),
                content: REPEATED_CONSOLE_LOGIN.to_owned(),
            },
        ],
    )
    .unwrap();
    Rules::new([set]).unwrap()
}

fn fixture(path: &str, records: Value) -> Fixture {
    Fixture::parse(&FixtureFile {
        path: path.to_owned(),
        content: json!({ "Records": records }).to_string(),
    })
    .unwrap()
}

fn console_login() -> Value {
    json!({ "eventSource": "signin.amazonaws.com", "eventName": "ConsoleLogin" })
}

fn other_event() -> Value {
    json!({ "eventName": "ListBuckets", "eventSource": "s3.amazonaws.com" })
}

async fn run(path: &str, records: Value) -> FixtureReport {
    run_fixture(&rules(), &fixture(path, records))
        .await
        .unwrap()
}

fn assert_failure_names_rule_and_file(report: &FixtureReport, file: &str) {
    let message = report.to_string();
    assert!(
        message.contains("console_login"),
        "rule id is missing: {message}"
    );
    assert!(message.contains(file), "file name is missing: {message}");
}

#[tokio::test]
async fn positive_fixture_with_a_signal_passes() {
    let report = run(
        "console_login/fixtures/positive/login.json",
        json!([other_event(), console_login()]),
    )
    .await;

    assert_eq!(
        report,
        FixtureReport {
            rule_id: "console_login".to_owned(),
            path: "console_login/fixtures/positive/login.json".to_owned(),
            verdict: Verdict::Passed,
        }
    );
}

#[tokio::test]
async fn negative_fixture_without_signals_passes() {
    let report = run(
        "console_login/fixtures/negative/no_login.json",
        json!([other_event()]),
    )
    .await;

    assert_eq!(report.verdict, Verdict::Passed);
}

#[tokio::test]
async fn positive_fixture_without_signals_fails_with_the_rule_id_and_file_name() {
    let report = run(
        "console_login/fixtures/positive/no_login.json",
        json!([other_event()]),
    )
    .await;

    assert_eq!(
        report.verdict,
        Verdict::Failed {
            expectation: Expectation::Positive,
            signals: 0,
        }
    );
    assert_failure_names_rule_and_file(&report, "no_login.json");
}

#[tokio::test]
async fn negative_fixture_with_signals_fails_with_the_rule_id_and_file_name() {
    let report = run(
        "console_login/fixtures/negative/two_logins.json",
        json!([console_login(), console_login()]),
    )
    .await;

    assert_eq!(
        report.verdict,
        Verdict::Failed {
            expectation: Expectation::Negative,
            signals: 2,
        }
    );
    assert_failure_names_rule_and_file(&report, "two_logins.json");
}

#[tokio::test]
async fn events_outside_the_fixture_window_do_not_raise_signals() {
    let mut login = console_login();
    login["eventTime"] = json!("2026-08-01T00:00:00Z");

    let report = run(
        "console_login/fixtures/negative/old_login.json",
        json!([login]),
    )
    .await;

    assert_eq!(report.verdict, Verdict::Passed);
}

#[tokio::test]
async fn each_fixture_runs_on_its_own_database() {
    run(
        "console_login/fixtures/positive/login.json",
        json!([console_login()]),
    )
    .await;

    let report = run(
        "console_login/fixtures/negative/no_login.json",
        json!([other_event()]),
    )
    .await;

    assert_eq!(report.verdict, Verdict::Passed);
}

#[tokio::test]
async fn scenario_fixtures_are_skipped() {
    let report = run(
        "repeated_console_login/fixtures/positive/two_logins.json",
        json!([console_login(), console_login()]),
    )
    .await;

    assert_eq!(report.verdict, Verdict::Skipped);
}

#[tokio::test]
async fn fixture_of_an_undefined_rule_is_an_error() {
    let error = run_fixture(
        &rules(),
        &fixture("missing_rule/fixtures/positive/login.json", json!([])),
    )
    .await
    .unwrap_err();

    assert!(
        matches!(&error, FixtureRunError::UnknownRule { rule_id, .. } if rule_id == "missing_rule"),
        "unexpected error: {error:?}"
    );
    assert!(error.to_string().contains("login.json"), "{error}");
}

#[tokio::test]
async fn builtin_rule_fixtures_pass() {
    let rules = Rules::new([RuleSet::builtin().unwrap()]).unwrap();
    let mut failures = Vec::new();
    for fixture in builtin_fixtures().unwrap() {
        let report = run_fixture(&rules, &fixture)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        if matches!(report.verdict, Verdict::Failed { .. }) {
            failures.push(report.to_string());
        }
    }

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
