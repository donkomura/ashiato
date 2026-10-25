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
