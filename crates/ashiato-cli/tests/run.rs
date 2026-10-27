use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use ashiato_pipeline::RunPlan;
use ashiato_testkit::log_file::{LogFile, write_log_file};
use ashiato_testkit::record::RecordBuilder;
use chrono::{DateTime, TimeZone, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use tempfile::TempDir;

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

const PLAN: &str = r#"
run_id: case-1
source:
  kind: local
  uri: ./logs
analysis_window:
  since: 2026-10-01T00:00:00Z
  until: 2026-10-02T00:00:00Z
rules: [builtin, ./rules]
rule: [test_repeated_console_login]
pipeline:
  batch: 1h
  allowed_lateness: 30m
placement:
  mode: single_process
"#;

fn ashiato() -> Command {
    Command::new(env!("CARGO_BIN_EXE_ashiato"))
}

fn at(hour: u32, minute: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 1, hour, minute, 0).unwrap()
}

fn login(event_id: &str, time: DateTime<Utc>) -> Value {
    RecordBuilder::new()
        .set("eventID", event_id)
        .set("eventTime", time.to_rfc3339())
        .set("eventSource", "signin.amazonaws.com")
        .set("eventName", "ConsoleLogin")
        .set("userIdentity.accessKeyId", Value::Null)
        .build()
}

fn gzip(path: &Path, content: &[u8]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut encoder = GzEncoder::new(File::create(path).unwrap(), Compression::default());
    encoder.write_all(content).unwrap();
    encoder.finish().unwrap();
}

struct RunCase {
    dir: TempDir,
}

impl RunCase {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        for (id, content) in [
            ("test_console_login", CONSOLE_LOGIN_RULE),
            ("test_repeated_console_login", REPEATED_CONSOLE_LOGIN_RULE),
        ] {
            let rule_dir = dir.path().join("case/rules").join(id);
            fs::create_dir_all(&rule_dir).unwrap();
            fs::write(rule_dir.join("rule.yaml"), content).unwrap();
        }
        let case = Self { dir };
        case.write_log(at(10, 5), vec![login("evt-login-1", at(10, 0))]);
        case.write_log(at(10, 25), vec![login("evt-login-2", at(10, 20))]);
        case.write_log(at(12, 0), vec![login("evt-late", at(11, 0))]);
        let broken = case
            .logs()
            .join(LogFile::new("111111111111", "us-east-1", at(12, 5)).relative_path());
        fs::create_dir_all(broken.parent().unwrap()).unwrap();
        fs::write(&broken, b"not a gzip stream").unwrap();
        gzip(
            &case.logs().join("other.json.gz"),
            json!({ "Records": [] }).to_string().as_bytes(),
        );
        case.write_plan(PLAN);
        case
    }

    fn logs(&self) -> PathBuf {
        self.dir.path().join("case/logs")
    }

    fn plan(&self) -> PathBuf {
        self.dir.path().join("case/run.yaml")
    }

    fn rules_dir(&self) -> PathBuf {
        self.dir.path().join("case/rules")
    }

    fn write_log(&self, time: DateTime<Utc>, records: Vec<Value>) {
        write_log_file(
            &self.logs(),
            &LogFile {
                records,
                ..LogFile::new("111111111111", "us-east-1", time)
            },
        );
    }

    fn write_plan(&self, plan: &str) {
        fs::create_dir_all(self.plan().parent().unwrap()).unwrap();
        fs::write(self.plan(), plan).unwrap();
    }

    fn run_output(&self, extra_args: &[&str]) -> Output {
        ashiato()
            .arg("run")
            .arg("--plan")
            .arg(self.plan())
            .args(extra_args)
            .output()
            .unwrap()
    }

    fn run(&self, extra_args: &[&str]) -> Vec<Value> {
        let output = self.run_output(extra_args);
        assert!(
            output.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        lines(&output.stdout)
    }
}

fn lines(stdout: &[u8]) -> Vec<Value> {
    String::from_utf8(stdout.to_vec())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn of_type<'a>(lines: &'a [Value], kind: &str) -> Vec<&'a Value> {
    lines.iter().filter(|line| line["type"] == kind).collect()
}

#[test]
fn run_writes_the_run_record_with_the_plan_digest_and_the_ingest_counts() {
    let case = RunCase::new();

    let lines = case.run(&[]);

    let run = &lines[0];
    assert_eq!(run["type"], "run", "{lines:#?}");
    assert_eq!(run["tool_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(run["since"], "2026-10-01T00:00:00Z");
    assert_eq!(run["until"], "2026-10-02T00:00:00Z");
    assert_eq!(
        run["plan_digest"],
        RunPlan::load(&case.plan()).unwrap().digest()
    );
    assert_eq!(run["ingested_files"], 3, "{run}");
    assert_eq!(run["file_errors"], 1, "{run}");
    assert_eq!(run["late_events"], 1, "{run}");
    let rule_sources = run["rule_sources"].as_array().unwrap();
    assert_eq!(rule_sources[0]["id"], "builtin");
    assert_eq!(
        rule_sources[1]["id"],
        case.rules_dir().to_string_lossy().as_ref()
    );
    assert_eq!(of_type(&lines, "run").len(), 1);
}

#[test]
fn run_writes_the_scenario_matches_of_the_streamed_events() {
    let case = RunCase::new();

    let lines = case.run(&[]);

    let matches = of_type(&lines, "scenario_match");
    assert_eq!(matches.len(), 1, "{lines:#?}");
    let found = matches[0];
    assert_eq!(found["scenario_id"], "test_repeated_console_login");
    assert_eq!(
        found["origin"],
        json!({ "id": "iam_user:arn:aws:iam::111111111111:user/alice", "kind": "iam_user" })
    );
    assert_eq!(found["start_time"], "2026-10-01T10:00:00Z");
    assert_eq!(found["end_time"], "2026-10-01T10:20:00Z");
}

#[test]
fn run_gives_the_same_records_as_ingest_and_analyze() {
    let case = RunCase::new();
    let streamed = case.run(&["--emit-signals"]);

    // The batch ingest has no notion of late events, so it reads only the files whose events
    // all arrived in time.
    let in_time = case.dir.path().join("in-time");
    for time in [at(10, 5), at(10, 25)] {
        let log = LogFile::new("111111111111", "us-east-1", time);
        let target = in_time.join(log.relative_path());
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::copy(case.logs().join(log.relative_path()), target).unwrap();
    }
    let workspace = case.dir.path().join("batch");
    let ingest = ashiato()
        .arg("ingest")
        .arg("--workspace")
        .arg(&workspace)
        .arg("--source")
        .arg(&in_time)
        .output()
        .unwrap();
    assert!(ingest.status.success(), "{ingest:?}");
    let analyze = ashiato()
        .arg("analyze")
        .arg("--workspace")
        .arg(&workspace)
        .args(["--since", "2026-10-01", "--until", "2026-10-02"])
        .arg("--rules")
        .arg(case.rules_dir())
        .args(["--rule", "test_repeated_console_login", "--emit-signals"])
        .output()
        .unwrap();
    assert!(analyze.status.success(), "{analyze:?}");

    let without_run = |lines: Vec<Value>| -> Vec<Value> {
        lines
            .into_iter()
            .filter(|line| line["type"] != "run")
            .collect()
    };
    let batch = without_run(lines(&analyze.stdout));
    assert!(!batch.is_empty());
    assert_eq!(without_run(streamed), batch);
}

#[test]
fn run_writes_signals_only_with_emit_signals() {
    let case = RunCase::new();

    assert!(of_type(&case.run(&[]), "signal").is_empty());
    let signals: Vec<Value> = of_type(&case.run(&["--emit-signals"]), "signal")
        .into_iter()
        .map(|signal| signal["event_ids"].clone())
        .collect();
    assert_eq!(signals, [json!(["evt-login-1"]), json!(["evt-login-2"])]);
}

#[test]
fn run_writes_the_summary_and_the_listed_severities() {
    let case = RunCase::new();

    let summary = case.run(&["--summary"]);
    let rules: Vec<(&Value, &Value)> = of_type(&summary, "summary")
        .into_iter()
        .map(|line| (&line["kind"], &line["rule_id"]))
        .collect();
    assert_eq!(
        rules,
        [
            (
                &json!("scenario_match"),
                &json!("test_repeated_console_login")
            ),
            (&json!("signal"), &json!("test_console_login")),
        ],
        "{summary:#?}"
    );
    assert!(of_type(&summary, "scenario_match").is_empty());

    let high = case.run(&["--severity", "high"]);
    assert!(of_type(&high, "scenario_match").is_empty(), "{high:#?}");
}

#[test]
fn running_again_reads_the_saved_manifest_and_gives_the_same_output() {
    let case = RunCase::new();
    let first = case.run_output(&["--emit-signals"]);
    assert!(first.status.success(), "{first:?}");

    case.write_log(at(10, 45), vec![login("evt-login-3", at(10, 40))]);
    let second = case.run_output(&["--emit-signals"]);

    assert!(second.status.success(), "{second:?}");
    assert_eq!(
        String::from_utf8(second.stdout).unwrap(),
        String::from_utf8(first.stdout).unwrap()
    );
}

#[test]
fn a_different_plan_on_the_same_workspace_is_an_error() {
    let case = RunCase::new();
    case.run(&[]);
    case.write_plan(&PLAN.replace("2026-10-02T00:00:00Z", "2026-10-03T00:00:00Z"));

    let output = case.run_output(&[]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("digest"), "{stderr}");
}

#[test]
fn files_that_do_not_follow_the_cloudtrail_layout_are_reported() {
    let case = RunCase::new();

    let output = case.run_output(&[]);

    assert!(output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("other.json.gz"), "{stderr}");
}

#[test]
fn settings_that_are_not_implemented_yet_are_rejected() {
    for (name, plan, setting) in [
        (
            "distributed",
            PLAN.replace("mode: single_process", "mode: distributed"),
            "distributed",
        ),
        (
            "s3",
            PLAN.replace(
                "kind: local\n  uri: ./logs",
                "kind: s3\n  uri: s3://bucket/AWSLogs/",
            ),
            "s3",
        ),
        (
            "workers",
            PLAN.replace(
                "mode: single_process",
                "mode: single_process\n  ingest_workers: 2",
            ),
            "ingest_workers",
        ),
    ] {
        let case = RunCase::new();
        case.write_plan(&plan);

        let output = case.run_output(&[]);

        assert!(!output.status.success(), "{name}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(setting), "{name}: {stderr}");
        assert!(
            !case.dir.path().join("case/ashiato.duckdb").exists(),
            "{name}"
        );
    }
}
