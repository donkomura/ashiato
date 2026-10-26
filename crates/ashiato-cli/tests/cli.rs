use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use ashiato_core::record::RecordBuilder;
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::json;
use tempfile::TempDir;

fn ashiato() -> Command {
    Command::new(env!("CARGO_BIN_EXE_ashiato"))
}

#[test]
fn version_flag_prints_package_version() {
    let output = ashiato().arg("--version").output().unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("ashiato {}\n", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn ingest_creates_workspace_and_loads_local_logs() {
    let dir = TempDir::new().unwrap();
    let source = dir
        .path()
        .join("source/AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01");
    fs::create_dir_all(&source).unwrap();
    let mut encoder = GzEncoder::new(
        File::create(source.join("log.json.gz")).unwrap(),
        Compression::default(),
    );
    let records = [
        RecordBuilder::new().set("eventID", "evt-1").build(),
        RecordBuilder::new().set("eventID", "evt-2").build(),
    ];
    encoder
        .write_all(json!({ "Records": records }).to_string().as_bytes())
        .unwrap();
    encoder.finish().unwrap();
    let workspace = dir.path().join("cases/case-1");

    let output = ashiato()
        .arg("ingest")
        .arg("--workspace")
        .arg(&workspace)
        .arg("--source")
        .arg(dir.path().join("source"))
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let db = duckdb::Connection::open(workspace.join("ashiato.duckdb")).unwrap();
    let events: i64 = db
        .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(events, 2);
}

#[test]
fn ingest_loads_logs_with_the_given_number_of_jobs() {
    let dir = TempDir::new().unwrap();
    for file in 0..3 {
        write_log(
            &dir.path().join(format!("source/log-{file}.json.gz")),
            &[RecordBuilder::new()
                .set("eventID", format!("evt-{file}"))
                .build()],
        );
    }
    let workspace = dir.path().join("case");

    let output = ashiato()
        .arg("ingest")
        .arg("--workspace")
        .arg(&workspace)
        .arg("--source")
        .arg(dir.path().join("source"))
        .args(["--jobs", "2"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let db = duckdb::Connection::open(workspace.join("ashiato.duckdb")).unwrap();
    let events: i64 = db
        .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(events, 3);
}

#[test]
fn ingest_rejects_zero_jobs() {
    let dir = TempDir::new().unwrap();
    fs::create_dir_all(dir.path().join("source")).unwrap();

    let output = ashiato()
        .arg("ingest")
        .arg("--workspace")
        .arg(dir.path().join("case"))
        .arg("--source")
        .arg(dir.path().join("source"))
        .args(["--jobs", "0"])
        .output()
        .unwrap();

    assert!(!output.status.success());
}

#[test]
fn ingest_rejects_s3_source_as_unsupported() {
    let dir = TempDir::new().unwrap();

    let output = ashiato()
        .arg("ingest")
        .arg("--workspace")
        .arg(dir.path().join("case"))
        .arg("--source")
        .arg("s3://trail-bucket/AWSLogs/")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("not supported"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

const CONSOLE_LOGIN_RULE: &str = r#"
kind: signal
id: test_console_login
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

const REPEATED_CONSOLE_LOGIN_RULE: &str = r#"
kind: scenario
id: test_repeated_console_login
title: 繰り返されるコンソールへのログイン
severity: medium
tactics: [TA0001]
window: 1h
steps:
  - signal: test_console_login
  - signal: test_console_login
"#;

struct AnalyzeCase {
    dir: TempDir,
}

impl AnalyzeCase {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        for (id, content) in [
            ("test_console_login", CONSOLE_LOGIN_RULE),
            ("test_repeated_console_login", REPEATED_CONSOLE_LOGIN_RULE),
        ] {
            let rule_dir = dir.path().join("rules").join(id);
            fs::create_dir_all(&rule_dir).unwrap();
            fs::write(rule_dir.join("rule.yaml"), content).unwrap();
        }
        let login = |event_id: &str, time: &str| {
            RecordBuilder::new()
                .set("eventID", event_id)
                .set("eventTime", time)
                .set("eventSource", "signin.amazonaws.com")
                .set("eventName", "ConsoleLogin")
                .set("userIdentity.accessKeyId", serde_json::Value::Null)
                .build()
        };
        write_log(
            &dir.path().join("source/log.json.gz"),
            &[
                login("evt-login-1", "2026-10-01T10:00:00Z"),
                login("evt-login-2", "2026-10-01T10:20:00Z"),
            ],
        );
        let output = ashiato()
            .arg("ingest")
            .arg("--workspace")
            .arg(dir.path().join("case"))
            .arg("--source")
            .arg(dir.path().join("source"))
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        Self { dir }
    }

    fn rules_dir(&self) -> PathBuf {
        self.dir.path().join("rules")
    }

    fn analyze(&self, extra_args: &[&str]) -> Vec<serde_json::Value> {
        let output = ashiato()
            .arg("analyze")
            .arg("--workspace")
            .arg(self.dir.path().join("case"))
            .args(["--since", "2026-10-01", "--until", "2026-10-02"])
            .arg("--rules")
            .arg(self.rules_dir())
            .args(extra_args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

fn write_log(path: &Path, records: &[serde_json::Value]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut encoder = GzEncoder::new(File::create(path).unwrap(), Compression::default());
    encoder
        .write_all(json!({ "Records": records }).to_string().as_bytes())
        .unwrap();
    encoder.finish().unwrap();
}

fn of_type<'a>(lines: &'a [serde_json::Value], kind: &str) -> Vec<&'a serde_json::Value> {
    lines.iter().filter(|line| line["type"] == kind).collect()
}

#[test]
fn analyze_writes_the_run_record_first() {
    let case = AnalyzeCase::new();

    let lines = case.analyze(&[]);

    let run = &lines[0];
    assert_eq!(run["type"], "run", "{lines:#?}");
    assert_eq!(run["tool_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(run["since"], "2026-10-01T00:00:00Z");
    assert_eq!(run["until"], "2026-10-02T00:00:00Z");
    let rulesets = run["rulesets"].as_array().unwrap();
    assert_eq!(rulesets.len(), 2, "{run}");
    assert_eq!(rulesets[0]["id"], "builtin");
    assert_eq!(
        rulesets[1]["id"],
        case.rules_dir().to_string_lossy().as_ref()
    );
    for ruleset in rulesets {
        assert!(
            ruleset["digest"].as_str().unwrap().starts_with("sha256:"),
            "{ruleset}"
        );
    }
    assert_eq!(of_type(&lines, "run").len(), 1);
}

#[test]
fn analyze_writes_scenario_matches_of_the_ingested_events() {
    let case = AnalyzeCase::new();

    let lines = case.analyze(&[]);

    let matches: Vec<_> = of_type(&lines, "scenario_match")
        .into_iter()
        .filter(|line| line["scenario_id"] == "test_repeated_console_login")
        .collect();
    assert_eq!(matches.len(), 1, "{lines:#?}");
    let found = matches[0];
    assert_eq!(found["severity"], "medium");
    assert_eq!(
        found["origin"],
        json!({ "id": "iam_user:arn:aws:iam::111111111111:user/alice", "kind": "iam_user" })
    );
    assert_eq!(found["start_time"], "2026-10-01T10:00:00Z");
    assert_eq!(found["end_time"], "2026-10-01T10:20:00Z");
    assert_eq!(found["lineage"], json!([]));
    let event_ids: Vec<&serde_json::Value> = found["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|step| {
            assert_eq!(step["signal_id"], "test_console_login");
            &step["signals"][0]["event_ids"][0]
        })
        .collect();
    assert_eq!(event_ids, [&json!("evt-login-1"), &json!("evt-login-2")]);
}

#[test]
fn analyze_writes_signals_only_with_emit_signals() {
    let case = AnalyzeCase::new();
    let test_signals = |lines: &[serde_json::Value]| -> Vec<serde_json::Value> {
        of_type(lines, "signal")
            .into_iter()
            .filter(|line| line["signal_id"] == "test_console_login")
            .cloned()
            .collect()
    };

    let without = case.analyze(&[]);
    let with = case.analyze(&["--emit-signals"]);

    assert!(of_type(&without, "signal").is_empty(), "{without:#?}");
    let signals = test_signals(&with);
    assert_eq!(signals.len(), 2, "{with:#?}");
    assert_eq!(signals[0]["event_ids"], json!(["evt-login-1"]));
    assert_eq!(
        signals[0]["origin"]["id"],
        "iam_user:arn:aws:iam::111111111111:user/alice"
    );
    assert_eq!(signals[0]["access_key_id"], serde_json::Value::Null);
}

#[test]
fn analyze_fails_when_the_workspace_has_not_been_ingested() {
    let dir = TempDir::new().unwrap();

    let output = ashiato()
        .arg("analyze")
        .arg("--workspace")
        .arg(dir.path().join("missing"))
        .args(["--since", "2026-10-01", "--until", "2026-10-02"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("ashiato.duckdb"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!dir.path().join("missing").exists());
}

#[test]
fn analyze_rejects_a_period_that_is_not_after_since() {
    let case = AnalyzeCase::new();

    let output = ashiato()
        .arg("analyze")
        .arg("--workspace")
        .arg(case.dir.path().join("case"))
        .args(["--since", "2026-10-02", "--until", "2026-10-01"])
        .output()
        .unwrap();

    assert!(!output.status.success());
}

fn write_rule(rules: &Path, id: &str, content: &str) {
    let dir = rules.join(id);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("rule.yaml"), content).unwrap();
}

fn write_fixture(rules: &Path, path: &str, records: serde_json::Value) {
    let path = rules.join(path);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, json!({ "Records": records }).to_string()).unwrap();
}

fn console_login_record() -> serde_json::Value {
    json!({ "eventSource": "signin.amazonaws.com", "eventName": "ConsoleLogin" })
}

fn rules_command(subcommand: &str, rules: Option<&Path>) -> std::process::Output {
    let mut command = ashiato();
    command.args(["rules", subcommand]);
    if let Some(rules) = rules {
        command.arg("--rules").arg(rules);
    }
    command.output().unwrap()
}

#[test]
fn rules_validate_accepts_the_builtin_rules() {
    let output = rules_command("validate", None);

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn rules_validate_accepts_valid_rules_under_rules() {
    let dir = TempDir::new().unwrap();
    write_rule(dir.path(), "test_console_login", CONSOLE_LOGIN_RULE);

    let output = rules_command("validate", Some(dir.path()));

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn rules_validate_rejects_a_function_outside_the_allow_list() {
    let dir = TempDir::new().unwrap();
    write_rule(
        dir.path(),
        "test_console_login",
        &CONSOLE_LOGIN_RULE.replace(
            "event_name = 'ConsoleLogin'",
            "regexp_matches(event_name, '^Console')",
        ),
    );

    let output = rules_command("validate", Some(dir.path()));

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("test_console_login"), "stderr: {stderr}");
    assert!(stderr.contains("regexp_matches"), "stderr: {stderr}");
}

#[test]
fn rules_validate_rejects_a_scenario_that_refers_to_an_unknown_signal() {
    let dir = TempDir::new().unwrap();
    write_rule(
        dir.path(),
        "test_repeated_console_login",
        REPEATED_CONSOLE_LOGIN_RULE,
    );

    let output = rules_command("validate", Some(dir.path()));

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("test_console_login"), "stderr: {stderr}");
}

#[test]
fn rules_test_passes_the_builtin_fixtures() {
    let output = rules_command("test", None);

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(
            "PASS console_takeover_mining (console_takeover_mining/fixtures/positive/mining_from_cloudshell.json)"
        ),
        "stdout: {stdout}"
    );
}

#[test]
fn rules_test_runs_the_fixtures_under_rules() {
    let dir = TempDir::new().unwrap();
    write_rule(dir.path(), "test_console_login", CONSOLE_LOGIN_RULE);
    write_fixture(
        dir.path(),
        "test_console_login/fixtures/positive/login.json",
        json!([console_login_record()]),
    );

    let output = rules_command("test", Some(dir.path()));

    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout
            .contains("PASS test_console_login (test_console_login/fixtures/positive/login.json)"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("PASS console_takeover_mining"),
        "stdout: {stdout}"
    );
}

#[test]
fn rules_test_fails_when_a_fixture_fails() {
    let dir = TempDir::new().unwrap();
    write_rule(dir.path(), "test_console_login", CONSOLE_LOGIN_RULE);
    write_fixture(
        dir.path(),
        "test_console_login/fixtures/negative/login.json",
        json!([console_login_record()]),
    );

    let output = rules_command("test", Some(dir.path()));

    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout
            .contains("FAIL test_console_login (test_console_login/fixtures/negative/login.json)"),
        "stdout: {stdout}"
    );
}

#[test]
fn rules_test_skips_the_builtin_fixtures_of_overridden_rules() {
    let dir = TempDir::new().unwrap();
    write_rule(
        dir.path(),
        "cloudshell_activity",
        &CONSOLE_LOGIN_RULE.replace("test_console_login", "cloudshell_activity"),
    );

    let output = rules_command("test", Some(dir.path()));

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "stdout: {stdout}\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !stdout.contains("cloudshell_activity/fixtures"),
        "stdout: {stdout}"
    );
}

#[test]
fn rules_test_rejects_a_fixture_outside_the_fixture_layout() {
    let dir = TempDir::new().unwrap();
    write_rule(dir.path(), "test_console_login", CONSOLE_LOGIN_RULE);
    write_fixture(
        dir.path(),
        "test_console_login/login.json",
        json!([console_login_record()]),
    );

    let output = rules_command("test", Some(dir.path()));

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("test_console_login/login.json"),
        "stderr: {stderr}"
    );
}
