use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use ashiato_testkit::record::RecordBuilder;
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

fn assert_ingest_fails(source: &str, extra_args: &[&str], message: &str) {
    let dir = TempDir::new().unwrap();

    let output = ashiato()
        .arg("ingest")
        .arg("--workspace")
        .arg(dir.path().join("case"))
        .arg("--source")
        .arg(source)
        .args(extra_args)
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(message),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn ingest_rejects_s3_filters_for_local_sources() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().to_string_lossy();

    for args in [
        ["--accounts", "111111111111"],
        ["--regions", "us-east-1"],
        ["--from", "2026-10-01"],
        ["--to", "2026-10-01"],
    ] {
        assert_ingest_fails(&source, &args, "only to S3 sources");
    }
}

#[test]
fn ingest_rejects_s3_uri_without_bucket() {
    assert_ingest_fails("s3:///AWSLogs/", &[], "bucket");
}

#[test]
fn ingest_rejects_from_after_to() {
    assert_ingest_fails(
        "s3://trail-bucket/AWSLogs/",
        &["--from", "2026-10-02", "--to", "2026-10-01"],
        "--from must not be after --to",
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

    fn analyze_output(&self, extra_args: &[&str]) -> std::process::Output {
        ashiato()
            .arg("analyze")
            .arg("--workspace")
            .arg(self.dir.path().join("case"))
            .args(["--since", "2026-10-01", "--until", "2026-10-02"])
            .arg("--rules")
            .arg(self.rules_dir())
            .args(extra_args)
            .output()
            .unwrap()
    }

    fn analyze(&self, extra_args: &[&str]) -> Vec<serde_json::Value> {
        let output = self.analyze_output(extra_args);
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
    let rule_sources = run["rule_sources"].as_array().unwrap();
    assert_eq!(rule_sources.len(), 2, "{run}");
    assert_eq!(rule_sources[0]["id"], "builtin");
    assert_eq!(
        rule_sources[1]["id"],
        case.rules_dir().to_string_lossy().as_ref()
    );
    for source in rule_sources {
        assert!(
            source["digest"].as_str().unwrap().starts_with("sha256:"),
            "{source}"
        );
    }
    assert_eq!(of_type(&lines, "run").len(), 1);
}

#[test]
fn analyze_writes_the_data_availability_in_the_run_record() {
    let case = AnalyzeCase::new();

    let lines = case.analyze(&[]);

    assert_eq!(
        lines[0]["data"],
        json!({ "events": 2, "s3_data_events": false }),
        "{lines:#?}"
    );
}

fn skipped_rule_ids(lines: &[serde_json::Value]) -> Vec<&str> {
    let mut ids: Vec<&str> = of_type(lines, "rule_skipped")
        .iter()
        .map(|line| line["rule_id"].as_str().unwrap())
        .collect();
    ids.sort_unstable();
    ids
}

#[test]
fn analyze_writes_the_rules_skipped_for_missing_s3_data_events() {
    let case = AnalyzeCase::new();

    let lines = case.analyze(&[]);

    assert_eq!(
        skipped_rule_ids(&lines),
        [
            "cross_account_s3_destruction",
            "s3_delete_burst",
            "s3_object_read_or_copy",
        ],
        "{lines:#?}"
    );
    for skipped in of_type(&lines, "rule_skipped") {
        assert_eq!(skipped["reason"], "missing_data", "{skipped}");
        assert_eq!(skipped["requires"], json!(["s3_data_events"]), "{skipped}");
    }
    let first_other = lines
        .iter()
        .skip(1)
        .position(|line| line["type"] != "rule_skipped")
        .map_or(lines.len(), |index| index + 1);
    assert!(
        lines[1..first_other]
            .iter()
            .all(|line| line["type"] == "rule_skipped"),
        "{lines:#?}"
    );
    assert_eq!(of_type(&lines, "rule_skipped").len(), first_other - 1);
}

#[test]
fn analyze_writes_the_skipped_rules_with_summary_and_severity() {
    let case = AnalyzeCase::new();

    let lines = case.analyze(&["--summary", "--severity", "low"]);

    assert_eq!(skipped_rule_ids(&lines).len(), 3, "{lines:#?}");
}

#[test]
fn analyze_skips_the_attack_rules_that_need_s3_data_events() {
    let case = AnalyzeCase::new();

    let lines = case.analyze(&["--ruleset", "attack-v20"]);

    let skipped = skipped_rule_ids(&lines);
    for id in [
        "cross_account_s3_destruction",
        "s3_delete_burst",
        "s3_object_read_or_copy",
        "s3_ransom_encryption",
        "s3_ssec_encryption",
    ] {
        assert!(skipped.contains(&id), "{id}: {skipped:?}");
    }
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

fn test_records<'a>(lines: &'a [serde_json::Value], kind: &str) -> Vec<&'a serde_json::Value> {
    of_type(lines, kind)
        .into_iter()
        .filter(|line| {
            [&line["signal_id"], &line["scenario_id"], &line["rule_id"]]
                .iter()
                .any(|id| id.as_str().is_some_and(|id| id.starts_with("test_")))
        })
        .collect()
}

#[test]
fn analyze_writes_only_the_listed_severities() {
    let case = AnalyzeCase::new();

    let low = case.analyze(&["--emit-signals", "--severity", "low"]);
    let medium_and_high = case.analyze(&["--emit-signals", "--severity", "medium,high"]);

    assert_eq!(test_records(&low, "signal").len(), 2, "{low:#?}");
    assert!(test_records(&low, "scenario_match").is_empty(), "{low:#?}");
    for line in low
        .iter()
        .skip(1)
        .filter(|line| !["rule_skipped", "origin_score"].contains(&line["type"].as_str().unwrap()))
    {
        assert_eq!(line["severity"], "low", "{line}");
    }
    assert!(
        test_records(&medium_and_high, "signal").is_empty(),
        "{medium_and_high:#?}"
    );
    assert_eq!(
        test_records(&medium_and_high, "scenario_match").len(),
        1,
        "{medium_and_high:#?}"
    );
    for line in medium_and_high
        .iter()
        .skip(1)
        .filter(|line| !["rule_skipped", "origin_score"].contains(&line["type"].as_str().unwrap()))
    {
        assert!(
            ["medium", "high"].contains(&line["severity"].as_str().unwrap()),
            "{line}"
        );
    }
}

fn alice_origin_score() -> serde_json::Value {
    json!({
        "type": "origin_score",
        "origin": { "id": "iam_user:arn:aws:iam::111111111111:user/alice", "kind": "iam_user" },
        "score": 1,
        "tactics": ["TA0001"],
        "signal_ids": ["test_console_login"],
        "above_threshold": false,
    })
}

#[test]
fn analyze_writes_the_origin_scores_after_the_scenario_matches() {
    let case = AnalyzeCase::new();

    let lines = case.analyze(&["--rule", "test_repeated_console_login"]);

    assert_eq!(
        of_type(&lines, "origin_score"),
        [&alice_origin_score()],
        "{lines:#?}"
    );
    let last_match = lines
        .iter()
        .rposition(|line| line["type"] == "scenario_match")
        .unwrap();
    let first_score = lines
        .iter()
        .position(|line| line["type"] == "origin_score")
        .unwrap();
    assert!(last_match < first_score, "{lines:#?}");
}

#[test]
fn analyze_scores_the_origins_before_filtering_by_severity() {
    let case = AnalyzeCase::new();

    for severity in ["medium", "high"] {
        let lines = case.analyze(&[
            "--rule",
            "test_repeated_console_login",
            "--severity",
            severity,
        ]);

        assert_eq!(
            of_type(&lines, "origin_score"),
            [&alice_origin_score()],
            "{severity}: {lines:#?}"
        );
    }
}

#[test]
fn analyze_summary_does_not_write_the_origin_scores() {
    let case = AnalyzeCase::new();

    let lines = case.analyze(&["--rule", "test_repeated_console_login", "--summary"]);

    assert!(of_type(&lines, "origin_score").is_empty(), "{lines:#?}");
}

#[test]
fn analyze_rejects_an_unknown_severity() {
    let case = AnalyzeCase::new();

    let output = ashiato()
        .arg("analyze")
        .arg("--workspace")
        .arg(case.dir.path().join("case"))
        .args(["--since", "2026-10-01", "--until", "2026-10-02"])
        .args(["--severity", "high,urgent"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("urgent"),
        "{output:?}"
    );
}

#[test]
fn analyze_summary_aggregates_the_records_by_rule_and_origin() {
    let case = AnalyzeCase::new();

    let lines = case.analyze(&["--summary"]);

    assert!(of_type(&lines, "signal").is_empty(), "{lines:#?}");
    assert!(of_type(&lines, "scenario_match").is_empty(), "{lines:#?}");
    let summaries = test_records(&lines, "summary");
    assert_eq!(summaries.len(), 2, "{lines:#?}");
    let scenario = summaries[0];
    assert_eq!(scenario["kind"], "scenario_match");
    assert_eq!(scenario["rule_id"], "test_repeated_console_login");
    assert_eq!(scenario["severity"], "medium");
    assert_eq!(scenario["count"], 1);
    let signal = summaries[1];
    assert_eq!(signal["kind"], "signal");
    assert_eq!(signal["rule_id"], "test_console_login");
    assert_eq!(signal["severity"], "low");
    assert_eq!(
        signal["origin"]["id"],
        "iam_user:arn:aws:iam::111111111111:user/alice"
    );
    assert_eq!(signal["count"], 2);
    assert_eq!(signal["events"], 2);
    assert_eq!(signal["source_ips"], 1);
    assert_eq!(signal["first_seen"], "2026-10-01T10:00:00Z");
    assert_eq!(signal["last_seen"], "2026-10-01T10:20:00Z");
}

#[test]
fn analyze_summary_is_ordered_from_the_most_severe() {
    let case = AnalyzeCase::new();

    let lines = case.analyze(&["--summary"]);

    let rank = |severity: &str| {
        ["critical", "high", "medium", "low"]
            .iter()
            .position(|s| *s == severity)
            .unwrap()
    };
    let ranks: Vec<usize> = of_type(&lines, "summary")
        .iter()
        .map(|line| rank(line["severity"].as_str().unwrap()))
        .collect();
    assert!(ranks.is_sorted(), "{lines:#?}");
}

#[test]
fn analyze_summary_includes_only_the_listed_severities() {
    let case = AnalyzeCase::new();

    let lines = case.analyze(&["--summary", "--severity", "medium"]);

    let summaries = test_records(&lines, "summary");
    assert_eq!(summaries.len(), 1, "{lines:#?}");
    assert_eq!(summaries[0]["rule_id"], "test_repeated_console_login");
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

const BINNED_LOGIN_RULE: &str = r#"
kind: signal
id: test_binned_login
title: 10 分ごとのコンソールへのログイン
severity: low
tactics: [TA0001]
params:
  bin: 10m
sql: |
  SELECT
    min(event_time)      AS start_time,
    max(event_time)      AS end_time,
    array_agg(event_id)  AS event_ids,
    access_key_id,
    principal_key,
    min(source_ip)       AS source_ip,
    recipient_account_id AS account_id,
    min(aws_region)      AS region
  FROM events
  WHERE {{ in_analysis_window(event_time) }}
    AND event_name = 'ConsoleLogin'
  GROUP BY access_key_id, principal_key, recipient_account_id,
           {{ time_bin(event_time, param('bin')) }}
"#;

fn rules_validate_with_batch(rules: Option<&Path>, batch: &str) -> std::process::Output {
    let mut command = ashiato();
    command.args(["rules", "validate", "--batch", batch]);
    if let Some(rules) = rules {
        command.arg("--rules").arg(rules);
    }
    command.output().unwrap()
}

fn warnings(stderr: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(stderr)
        .lines()
        .filter(|line| line.starts_with("warning: "))
        .map(str::to_owned)
        .collect()
}

#[test]
fn rules_validate_warns_about_the_builtin_rules_that_the_pipeline_cannot_evaluate() {
    let output = rules_command("validate", None);

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let warnings = warnings(&output.stderr);
    for rule_id in [
        "cloudtrail_logging_paused",
        "first_seen_account_region_write",
        "role_session_outlier",
        "self_privilege_escalation",
        "temporary_credential_ip_change",
    ] {
        assert!(
            warnings.iter().any(|warning| warning.contains(rule_id)),
            "{rule_id} in {warnings:#?}"
        );
    }
    assert_eq!(
        warnings
            .iter()
            .filter(|warning| warning.contains("in_baseline_window"))
            .count(),
        3,
        "{warnings:#?}"
    );
}

#[test]
fn rules_validate_warns_when_the_batch_is_not_a_multiple_of_the_time_bin() {
    let dir = TempDir::new().unwrap();
    write_rule(dir.path(), "test_binned_login", BINNED_LOGIN_RULE);

    let divisible = rules_validate_with_batch(Some(dir.path()), "30m");
    let indivisible = rules_validate_with_batch(Some(dir.path()), "15m");

    assert!(divisible.status.success());
    assert!(
        !warnings(&divisible.stderr)
            .iter()
            .any(|warning| warning.contains("test_binned_login")),
        "{:#?}",
        warnings(&divisible.stderr)
    );
    assert!(
        indivisible.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&indivisible.stderr)
    );
    let warnings = warnings(&indivisible.stderr);
    let warning = warnings
        .iter()
        .find(|warning| warning.contains("test_binned_login"))
        .unwrap_or_else(|| panic!("{warnings:#?}"));
    assert!(warning.contains("10m"), "{warning}");
    assert!(warning.contains("15m"), "{warning}");
}

#[test]
fn rules_validate_fails_on_errors_even_when_it_also_warns() {
    let dir = TempDir::new().unwrap();
    write_rule(
        dir.path(),
        "test_binned_login",
        &BINNED_LOGIN_RULE.replace(
            "event_name = 'ConsoleLogin'",
            "regexp_matches(event_name, '^Console')",
        ),
    );

    let output = rules_validate_with_batch(Some(dir.path()), "15m");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("error: "), "stderr: {stderr}");
    assert!(stderr.contains("regexp_matches"), "stderr: {stderr}");
    assert!(
        warnings(&output.stderr)
            .iter()
            .any(|warning| warning.contains("test_binned_login")),
        "stderr: {stderr}"
    );
}

#[test]
fn rules_validate_rejects_a_batch_that_is_not_a_duration() {
    let output = rules_validate_with_batch(None, "0h");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("0h"), "stderr: {stderr}");
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

const TEST_RULESET: &str = r#"
id: test_attack
title: 試験用の ATT&CK
framework:
  kind: mitre_attack
  version: "20.0"
rules:
  - id: test_console_login
    refs: [T1078.004]
  - id: test_repeated_console_login
"#;

fn write_ruleset(dir: &Path, content: &str) -> PathBuf {
    let rulesets = dir.join("rulesets");
    fs::create_dir_all(&rulesets).unwrap();
    fs::write(rulesets.join("ruleset.yaml"), content).unwrap();
    rulesets
}

fn rule_ids(lines: &[serde_json::Value]) -> Vec<String> {
    let mut ids: Vec<String> = lines
        .iter()
        .filter_map(|line| {
            line["signal_id"]
                .as_str()
                .or_else(|| line["scenario_id"].as_str())
        })
        .map(str::to_owned)
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

impl AnalyzeCase {
    fn rulesets_dir(&self) -> PathBuf {
        write_ruleset(self.dir.path(), TEST_RULESET)
    }

    fn analyze_failure(&self, extra_args: &[&str]) -> String {
        let output = ashiato()
            .arg("analyze")
            .arg("--workspace")
            .arg(self.dir.path().join("case"))
            .args(["--since", "2026-10-01", "--until", "2026-10-02"])
            .arg("--rules")
            .arg(self.rules_dir())
            .arg("--rulesets")
            .arg(self.rulesets_dir())
            .args(extra_args)
            .output()
            .unwrap();
        assert!(!output.status.success(), "{output:?}");
        String::from_utf8_lossy(&output.stderr).into_owned()
    }

    fn analyze_with_rulesets(&self, extra_args: &[&str]) -> Vec<serde_json::Value> {
        let rulesets = self.rulesets_dir();
        let mut args = vec!["--rulesets", rulesets.to_str().unwrap()];
        args.extend_from_slice(extra_args);
        self.analyze(&args)
    }
}

#[test]
fn analyze_reports_the_default_ruleset_in_the_run_record() {
    let case = AnalyzeCase::new();

    let lines = case.analyze(&[]);

    let rulesets = lines[0]["rulesets"].as_array().unwrap();
    assert_eq!(rulesets.len(), 1, "{}", lines[0]);
    assert_eq!(rulesets[0]["id"], "default");
    assert!(
        rulesets[0]["digest"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
}

#[test]
fn analyze_runs_only_the_rules_of_the_given_ruleset() {
    let case = AnalyzeCase::new();

    let lines = case.analyze_with_rulesets(&["--ruleset", "test_attack", "--emit-signals"]);

    assert_eq!(
        rule_ids(&lines),
        ["test_console_login", "test_repeated_console_login"]
    );
    let rulesets = lines[0]["rulesets"].as_array().unwrap();
    assert_eq!(rulesets.len(), 1);
    assert_eq!(rulesets[0]["id"], "test_attack");
}

#[test]
fn analyze_attaches_the_refs_of_the_selected_rulesets() {
    let case = AnalyzeCase::new();

    let lines = case.analyze_with_rulesets(&["--ruleset", "test_attack", "--emit-signals"]);

    let signal = of_type(&lines, "signal")[0];
    assert_eq!(
        signal["refs"],
        json!([{ "ruleset": "test_attack", "ids": ["T1078.004"] }]),
        "{signal}"
    );
    let found = of_type(&lines, "scenario_match")[0];
    assert!(found.get("refs").is_none(), "{found}");
}

#[test]
fn analyze_omits_refs_for_rulesets_without_a_framework() {
    let case = AnalyzeCase::new();

    let lines = case.analyze(&["--emit-signals"]);

    for line in test_records(&lines, "signal") {
        assert!(line.get("refs").is_none(), "{line}");
    }
}

#[test]
fn analyze_runs_only_the_given_rules_of_the_ruleset() {
    let case = AnalyzeCase::new();

    let lines = case.analyze_with_rulesets(&[
        "--ruleset",
        "test_attack",
        "--rule",
        "test_console_login",
        "--emit-signals",
    ]);

    assert_eq!(rule_ids(&lines), ["test_console_login"]);
}

#[test]
fn analyze_runs_the_signals_of_a_given_scenario() {
    let case = AnalyzeCase::new();

    let lines = case.analyze_with_rulesets(&[
        "--ruleset",
        "test_attack",
        "--rule",
        "test_repeated_console_login",
        "--emit-signals",
    ]);

    assert_eq!(
        rule_ids(&lines),
        ["test_console_login", "test_repeated_console_login"]
    );
}

#[test]
fn analyze_rejects_a_rule_outside_the_given_rulesets() {
    let case = AnalyzeCase::new();

    let stderr =
        case.analyze_failure(&["--ruleset", "test_attack", "--rule", "cloudshell_activity"]);

    assert!(stderr.contains("cloudshell_activity"), "stderr: {stderr}");
}

#[test]
fn analyze_rejects_an_unknown_ruleset() {
    let case = AnalyzeCase::new();

    let stderr = case.analyze_failure(&["--ruleset", "missing"]);

    assert!(stderr.contains("missing"), "stderr: {stderr}");
}

fn rulesets_command(args: &[&str]) -> std::process::Output {
    ashiato().arg("rulesets").args(args).output().unwrap()
}

fn json_lines(output: &std::process::Output) -> Vec<serde_json::Value> {
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[test]
fn rulesets_list_writes_each_ruleset() {
    let dir = TempDir::new().unwrap();
    write_rule(
        &dir.path().join("rules"),
        "test_console_login",
        CONSOLE_LOGIN_RULE,
    );
    write_rule(
        &dir.path().join("rules"),
        "test_repeated_console_login",
        REPEATED_CONSOLE_LOGIN_RULE,
    );
    let rulesets = write_ruleset(dir.path(), TEST_RULESET);

    let lines = json_lines(&rulesets_command(&[
        "list",
        "--rules",
        dir.path().join("rules").to_str().unwrap(),
        "--rulesets",
        rulesets.to_str().unwrap(),
    ]));

    let default = lines.iter().find(|line| line["id"] == "default").unwrap();
    assert!(default["rules"].as_u64().unwrap() > 0, "{default}");
    assert!(default.get("framework").is_none(), "{default}");
    let attack = lines
        .iter()
        .find(|line| line["id"] == "test_attack")
        .unwrap();
    assert_eq!(attack["title"], "試験用の ATT&CK");
    assert_eq!(attack["rules"], 2);
    assert_eq!(
        attack["framework"],
        json!({ "kind": "mitre_attack", "version": "20.0" })
    );
}

#[test]
fn rulesets_show_writes_each_rule_of_the_ruleset() {
    let dir = TempDir::new().unwrap();
    write_rule(
        &dir.path().join("rules"),
        "test_console_login",
        CONSOLE_LOGIN_RULE,
    );
    write_rule(
        &dir.path().join("rules"),
        "test_repeated_console_login",
        REPEATED_CONSOLE_LOGIN_RULE,
    );
    let rulesets = write_ruleset(dir.path(), TEST_RULESET);

    let lines = json_lines(&rulesets_command(&[
        "show",
        "test_attack",
        "--rules",
        dir.path().join("rules").to_str().unwrap(),
        "--rulesets",
        rulesets.to_str().unwrap(),
    ]));

    assert_eq!(
        lines,
        [
            json!({
                "id": "test_console_login",
                "kind": "signal",
                "severity": "low",
                "tactics": ["TA0001"],
                "refs": ["T1078.004"],
            }),
            json!({
                "id": "test_repeated_console_login",
                "kind": "scenario",
                "severity": "medium",
                "tactics": ["TA0001"],
                "refs": [],
            }),
        ]
    );
}

#[test]
fn rulesets_show_writes_the_builtin_default_ruleset() {
    let lines = json_lines(&rulesets_command(&["show", "default"]));

    assert!(
        lines
            .iter()
            .any(|line| line["id"] == "console_takeover_mining" && line["kind"] == "scenario"),
        "{lines:#?}"
    );
}

#[test]
fn rulesets_show_rejects_an_unknown_ruleset() {
    let output = rulesets_command(&["show", "missing"]);

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("missing"));
}

#[test]
fn rules_validate_rejects_a_ruleset_that_lists_an_unknown_rule() {
    let dir = TempDir::new().unwrap();
    let rulesets = write_ruleset(
        dir.path(),
        "id: broken\ntitle: broken\nrules:\n  - id: no_such_rule\n",
    );

    let output = ashiato()
        .args(["rules", "validate", "--rulesets"])
        .arg(&rulesets)
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("no_such_rule"), "stderr: {stderr}");
}

#[test]
fn rules_test_runs_only_the_fixtures_of_the_given_ruleset() {
    let dir = TempDir::new().unwrap();
    let rules = dir.path().join("rules");
    write_rule(&rules, "test_console_login", CONSOLE_LOGIN_RULE);
    write_rule(
        &rules,
        "test_repeated_console_login",
        REPEATED_CONSOLE_LOGIN_RULE,
    );
    write_fixture(
        &rules,
        "test_console_login/fixtures/positive/login.json",
        json!([console_login_record()]),
    );
    let rulesets = write_ruleset(dir.path(), TEST_RULESET);

    let output = ashiato()
        .args(["rules", "test", "--ruleset", "test_attack", "--rules"])
        .arg(&rules)
        .arg("--rulesets")
        .arg(&rulesets)
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "stdout: {stdout}");
    assert!(
        stdout.contains("PASS test_console_login"),
        "stdout: {stdout}"
    );
    assert!(
        !stdout.contains("console_takeover_mining"),
        "stdout: {stdout}"
    );
}

const SUPPRESS_CONSOLE_LOGIN_CONTEXT: &str = r#"
suppress:
  - origin: "iam_user:*"
    signals: [test_console_login]
"#;

const PARAM_CONSOLE_LOGIN_RULE: &str = r#"
kind: signal
id: test_console_login
title: コンソールへのログイン
severity: low
tactics: [TA0001]
params:
  event_name: ConsoleLogin
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
    AND event_name = {{ param('event_name') }}
"#;

const BASELINE_LOGIN_RULE: &str = r#"
kind: signal
id: test_baseline_login
title: ベースライン期間のログイン
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
  WHERE {{ in_baseline_window(event_time) }}
    AND event_name = 'ConsoleLogin'
"#;

impl AnalyzeCase {
    fn write_context(&self, content: &str) -> String {
        let path = self.dir.path().join("context.yaml");
        fs::write(&path, content).unwrap();
        path.to_string_lossy().into_owned()
    }
}

fn context_digest(content: &str) -> String {
    ashiato_core::context::Context::parse(content)
        .unwrap()
        .digest()
        .unwrap()
        .to_owned()
}

#[test]
fn analyze_writes_the_context_digest_only_when_a_context_file_is_given() {
    let case = AnalyzeCase::new();
    let context = case.write_context(SUPPRESS_CONSOLE_LOGIN_CONTEXT);

    let without = case.analyze(&[]);
    let with = case.analyze(&["--context", &context]);

    assert!(without[0].get("context_digest").is_none(), "{}", without[0]);
    assert_eq!(
        with[0]["context_digest"],
        context_digest(SUPPRESS_CONSOLE_LOGIN_CONTEXT),
        "{}",
        with[0]
    );
}

#[test]
fn analyze_leaves_out_the_signals_suppressed_by_the_context() {
    let case = AnalyzeCase::new();
    let context = case.write_context(SUPPRESS_CONSOLE_LOGIN_CONTEXT);

    let without = case.analyze(&["--emit-signals"]);
    let with = case.analyze(&["--emit-signals", "--context", &context]);

    assert_eq!(test_records(&without, "signal").len(), 2, "{without:#?}");
    assert_eq!(test_records(&without, "scenario_match").len(), 1);
    assert!(test_records(&with, "signal").is_empty(), "{with:#?}");
    assert!(
        test_records(&with, "scenario_match").is_empty(),
        "{with:#?}"
    );
}

#[test]
fn analyze_overrides_the_params_of_the_rules_with_the_context() {
    let case = AnalyzeCase::new();
    write_rule(
        &case.rules_dir(),
        "test_console_login",
        PARAM_CONSOLE_LOGIN_RULE,
    );
    let context = case.write_context("params:\n  test_console_login.event_name: CreateUser\n");

    let without = case.analyze(&["--emit-signals"]);
    let with = case.analyze(&["--emit-signals", "--context", &context]);

    assert_eq!(test_records(&without, "signal").len(), 2, "{without:#?}");
    assert!(test_records(&with, "signal").is_empty(), "{with:#?}");
}

#[test]
fn analyze_rejects_a_param_that_the_rule_does_not_declare() {
    let case = AnalyzeCase::new();
    let context = case.write_context("params:\n  test_console_login.min_count: 2\n");

    let output = case.analyze_output(&["--context", &context]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("test_console_login.min_count"), "{stderr}");
}

#[test]
fn analyze_rejects_a_missing_context_file() {
    let case = AnalyzeCase::new();
    let missing = case.dir.path().join("missing.yaml");

    let output = case.analyze_output(&["--context", &missing.to_string_lossy()]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("missing.yaml"), "{stderr}");
}

#[test]
fn analyze_takes_the_baseline_period_from_the_lookback_of_the_context() {
    let case = AnalyzeCase::new();
    write_rule(
        &case.rules_dir(),
        "test_baseline_login",
        BASELINE_LOGIN_RULE,
    );
    write_log(
        &case.dir.path().join("baseline/log.json.gz"),
        &[RecordBuilder::new()
            .set("eventID", "evt-baseline-login")
            .set("eventTime", "2026-09-21T10:00:00Z")
            .set("eventSource", "signin.amazonaws.com")
            .set("eventName", "ConsoleLogin")
            .build()],
    );
    let ingest = ashiato()
        .arg("ingest")
        .arg("--workspace")
        .arg(case.dir.path().join("case"))
        .arg("--source")
        .arg(case.dir.path().join("baseline"))
        .output()
        .unwrap();
    assert!(ingest.status.success(), "{ingest:?}");
    let context = case.write_context("baseline:\n  lookback: 7d\n");
    let baseline_logins = |lines: &[serde_json::Value]| {
        of_type(lines, "signal")
            .into_iter()
            .filter(|line| line["signal_id"] == "test_baseline_login")
            .count()
    };

    let without = case.analyze(&["--emit-signals"]);
    let with = case.analyze(&["--emit-signals", "--context", &context]);

    assert_eq!(baseline_logins(&without), 1, "{without:#?}");
    assert_eq!(baseline_logins(&with), 0, "{with:#?}");
}

#[test]
fn analyze_uses_the_scoring_thresholds_of_the_context() {
    let case = AnalyzeCase::new();
    let context = case.dir.path().join("context.yaml");
    fs::write(&context, "scoring:\n  min_tactics: 1\n  threshold: 1\n").unwrap();

    let lines = case.analyze(&["--context", context.to_str().unwrap()]);

    let mut expected = alice_origin_score();
    expected["above_threshold"] = json!(true);
    assert_eq!(of_type(&lines, "origin_score"), [&expected]);
}
