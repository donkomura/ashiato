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

    fn with_context(self, content: &str) -> Self {
        fs::write(self.dir.path().join("case/context.yaml"), content).unwrap();
        self.write_plan(&format!("{PLAN}context: ./context.yaml\n"));
        self
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

fn alice_origin_score() -> Value {
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
fn run_writes_the_origin_scores_after_the_scenario_matches() {
    let case = RunCase::new();

    let lines = case.run(&[]);

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
fn run_scores_the_origins_before_filtering_by_severity_and_not_in_the_summary() {
    let case = RunCase::new();

    let high = case.run(&["--severity", "high"]);
    let summary = case.run(&["--summary"]);

    assert_eq!(
        of_type(&high, "origin_score"),
        [&alice_origin_score()],
        "{high:#?}"
    );
    assert!(of_type(&summary, "origin_score").is_empty(), "{summary:#?}");
}

#[test]
fn run_writes_the_largest_number_of_pending_rows_to_stderr_as_one_json_line() {
    let case = RunCase::new();

    let output = case.run_output(&[]);

    assert!(output.status.success(), "{output:?}");
    let metrics: Vec<Value> = String::from_utf8_lossy(&output.stderr)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|line| line.get("metrics").is_some())
        .collect();
    assert_eq!(metrics.len(), 1, "{metrics:?}");
    assert_eq!(metrics[0]["metrics"], "run");
    // The few messages of the case are committed at once, so both logins are pending together.
    assert_eq!(metrics[0]["pending_max_rows"], 2, "{}", metrics[0]);
    assert!(
        lines(&output.stdout)
            .iter()
            .all(|line| line.get("metrics").is_none())
    );
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
    let case = RunCase::new();
    case.write_plan(&PLAN.replace("mode: single_process", "mode: distributed"));

    let output = case.run_output(&[]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("distributed"), "{stderr}");
    assert!(!case.dir.path().join("case/ashiato.duckdb").exists());
}

#[test]
fn the_number_of_ingest_workers_does_not_change_the_output() {
    let case = RunCase::new();
    for (account, region) in [
        ("111111111111", "ap-northeast-1"),
        ("222222222222", "us-east-1"),
        ("333333333333", "eu-west-1"),
    ] {
        for (minute, id) in [(5, "a"), (25, "b"), (45, "c")] {
            write_log_file(
                &case.logs(),
                &LogFile {
                    records: vec![login(
                        &format!("evt-{account}-{region}-{id}"),
                        at(10, minute - 5),
                    )],
                    ..LogFile::new(account, region, at(10, minute))
                },
            );
        }
    }
    let output = |workers: u32| {
        let database = case.dir.path().join("case/ashiato.duckdb");
        if database.exists() {
            fs::remove_file(&database).unwrap();
        }
        case.write_plan(&PLAN.replace(
            "mode: single_process",
            &format!("mode: single_process\n  ingest_workers: {workers}"),
        ));
        let output = case.run_output(&["--emit-signals"]);
        assert!(
            output.status.success(),
            "{workers} workers: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    };

    let one = output(1);

    assert_eq!(of_type(&lines(one.as_bytes()), "signal").len(), 11, "{one}");
    for workers in [2, 3, 5] {
        assert_eq!(output(workers), one, "{workers} workers");
    }
}

#[test]
fn max_skew_does_not_change_the_output_except_for_the_plan_digest() {
    let case = RunCase::new();
    let output = |plan: &str| {
        let database = case.dir.path().join("case/ashiato.duckdb");
        if database.exists() {
            fs::remove_file(&database).unwrap();
        }
        case.write_plan(plan);
        let mut lines = case.run(&["--emit-signals"]);
        assert_eq!(lines[0]["type"], "run");
        lines[0]
            .as_object_mut()
            .unwrap()
            .remove("plan_digest")
            .unwrap();
        lines
    };

    let without = output(PLAN);
    let with = output(&PLAN.replace(
        "allowed_lateness: 30m",
        "allowed_lateness: 30m\n  max_skew: 30m",
    ));

    assert!(!of_type(&without, "signal").is_empty());
    assert_eq!(with, without);
}

#[test]
fn an_s3_source_below_the_trail_root_is_rejected_before_the_workspace_is_created() {
    let case = RunCase::new();
    case.write_plan(&PLAN.replace(
        "kind: local\n  uri: ./logs",
        "kind: s3\n  uri: s3://bucket/AWSLogs/111111111111/CloudTrail/",
    ));

    let output = case.run_output(&[]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("AWSLogs/111111111111/CloudTrail/"),
        "{stderr}"
    );
    assert!(!case.dir.path().join("case/ashiato.duckdb").exists());
}

fn skipped_rule_ids(lines: &[Value]) -> Vec<&str> {
    of_type(lines, "rule_skipped")
        .into_iter()
        .filter(|line| line["reason"] == "missing_data")
        .map(|line| line["rule_id"].as_str().unwrap())
        .collect()
}

#[test]
fn run_skips_the_rules_that_need_s3_data_events_after_streaming_all_batches() {
    let case = RunCase::new();
    case.write_plan(&PLAN.replace("rule: [test_repeated_console_login]\n", ""));

    let lines = case.run(&[]);

    assert_eq!(
        lines[0]["data"],
        json!({ "events": 2, "s3_data_events": false })
    );
    let skipped = skipped_rule_ids(&lines);
    for rule_id in [
        "cross_account_s3_destruction",
        "s3_delete_burst",
        "s3_object_read_or_copy",
    ] {
        assert!(skipped.contains(&rule_id), "{rule_id} in {skipped:?}");
    }
    let all_skipped = of_type(&lines, "rule_skipped").len();
    assert!(
        lines[1..=all_skipped]
            .iter()
            .all(|line| line["type"] == "rule_skipped"),
        "{lines:#?}"
    );
    for skipped in of_type(&lines, "rule_skipped") {
        if skipped["reason"] == "missing_data" {
            assert_eq!(skipped["requires"], json!(["s3_data_events"]));
        }
    }
}

#[test]
fn run_does_not_skip_rules_when_s3_data_events_were_streamed() {
    let case = RunCase::new();
    case.write_plan(&PLAN.replace("rule: [test_repeated_console_login]\n", ""));
    case.write_log(
        at(11, 35),
        vec![
            RecordBuilder::new()
                .set("eventTime", at(11, 30).to_rfc3339())
                .set("eventSource", "s3.amazonaws.com")
                .set("eventName", "GetObject")
                .set("eventCategory", "Data")
                .build(),
        ],
    );

    let lines = case.run(&[]);

    assert_eq!(
        lines[0]["data"],
        json!({ "events": 3, "s3_data_events": true })
    );
    assert!(skipped_rule_ids(&lines).is_empty(), "{lines:#?}");
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

#[test]
fn run_writes_the_context_digest_only_when_the_plan_has_a_context_file() {
    let without = RunCase::new().run(&[]);
    let case = RunCase::new().with_context(SUPPRESS_CONSOLE_LOGIN_CONTEXT);

    let with = case.run(&[]);

    assert!(without[0].get("context_digest").is_none(), "{}", without[0]);
    let expected = ashiato_core::context::Context::parse(SUPPRESS_CONSOLE_LOGIN_CONTEXT)
        .unwrap()
        .digest()
        .unwrap()
        .to_owned();
    assert_eq!(with[0]["context_digest"], expected, "{}", with[0]);
    assert_eq!(
        with[0]["plan_digest"],
        RunPlan::load(&case.plan()).unwrap().digest()
    );
}

#[test]
fn run_leaves_out_the_signals_suppressed_by_the_context() {
    let case = RunCase::new().with_context(SUPPRESS_CONSOLE_LOGIN_CONTEXT);

    let lines = case.run(&["--emit-signals"]);

    assert!(of_type(&lines, "signal").is_empty(), "{lines:#?}");
    assert!(of_type(&lines, "scenario_match").is_empty(), "{lines:#?}");
}

#[test]
fn run_overrides_the_params_of_the_rules_with_the_context() {
    let without = RunCase::new();
    let with =
        RunCase::new().with_context("params:\n  test_console_login.event_name: CreateUser\n");
    for case in [&without, &with] {
        fs::write(
            case.rules_dir().join("test_console_login/rule.yaml"),
            PARAM_CONSOLE_LOGIN_RULE,
        )
        .unwrap();
    }

    let without = without.run(&["--emit-signals"]);
    let with = with.run(&["--emit-signals"]);

    assert_eq!(of_type(&without, "signal").len(), 2, "{without:#?}");
    assert!(of_type(&with, "signal").is_empty(), "{with:#?}");
}

#[test]
fn an_edited_context_file_on_the_same_workspace_is_an_error() {
    let case = RunCase::new().with_context(SUPPRESS_CONSOLE_LOGIN_CONTEXT);
    case.run(&[]);
    fs::write(
        case.dir.path().join("case/context.yaml"),
        SUPPRESS_CONSOLE_LOGIN_CONTEXT.replace("iam_user:*", "workload:*"),
    )
    .unwrap();

    let output = case.run_output(&[]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("digest"), "{stderr}");
}

#[test]
fn run_uses_the_scoring_thresholds_of_the_context() {
    let case = RunCase::new().with_context("scoring:\n  min_tactics: 1\n  threshold: 1\n");

    let lines = case.run(&[]);

    let mut expected = alice_origin_score();
    expected["above_threshold"] = json!(true);
    assert_eq!(of_type(&lines, "origin_score"), [&expected]);
}

const UNBINNED_LOGIN_RULE: &str = r#"
kind: signal
id: test_unbinned_login
title: 調査期間全体での主体ごとのコンソールへのログイン
severity: low
tactics: [TA0001]
sql: |
  SELECT
    min(event_time)      AS start_time,
    max(event_time)      AS end_time,
    array_agg(event_id)  AS event_ids,
    min(access_key_id)   AS access_key_id,
    principal_key,
    min(source_ip)       AS source_ip,
    recipient_account_id AS account_id,
    min(aws_region)      AS region
  FROM events
  WHERE {{ in_analysis_window(event_time) }}
    AND event_name = 'ConsoleLogin'
  GROUP BY principal_key, recipient_account_id
"#;

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
    min(access_key_id)   AS access_key_id,
    principal_key,
    min(source_ip)       AS source_ip,
    recipient_account_id AS account_id,
    min(aws_region)      AS region
  FROM events
  WHERE {{ in_analysis_window(event_time) }}
    AND event_name = 'ConsoleLogin'
  GROUP BY principal_key, recipient_account_id,
           {{ time_bin(event_time, param('bin')) }}
"#;

const STREAMING_RULES: &str =
    "rule: [test_repeated_console_login, test_unbinned_login, test_binned_login]";

impl RunCase {
    fn with_streaming_rules(self, context: Option<&str>) -> Self {
        for (id, content) in [
            ("test_unbinned_login", UNBINNED_LOGIN_RULE),
            ("test_binned_login", BINNED_LOGIN_RULE),
        ] {
            let rule_dir = self.rules_dir().join(id);
            fs::create_dir_all(&rule_dir).unwrap();
            fs::write(rule_dir.join("rule.yaml"), content).unwrap();
        }
        let mut plan = PLAN.replace("rule: [test_repeated_console_login]", STREAMING_RULES);
        if let Some(context) = context {
            fs::write(self.dir.path().join("case/context.yaml"), context).unwrap();
            plan.push_str("context: ./context.yaml\n");
        }
        self.write_plan(&plan);
        self
    }
}

fn signal_ids(lines: &[Value]) -> Vec<&str> {
    let mut ids: Vec<&str> = of_type(lines, "signal")
        .into_iter()
        .map(|line| line["signal_id"].as_str().unwrap())
        .collect();
    ids.sort_unstable();
    ids
}

#[test]
fn run_skips_the_rules_that_the_batches_of_the_pipeline_cannot_evaluate() {
    let case = RunCase::new().with_streaming_rules(Some("params:\n  test_binned_login.bin: 7m\n"));

    let output = case.run_output(&["--emit-signals"]);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "stderr: {stderr}");
    let lines = lines(&output.stdout);
    let mut skipped = of_type(&lines, "rule_skipped");
    skipped.sort_by_key(|line| line["rule_id"].as_str().unwrap().to_owned());
    assert_eq!(
        skipped,
        [
            &json!({
                "type": "rule_skipped",
                "rule_id": "test_binned_login",
                "reason": "not_streamable",
                "violations": ["batch_not_multiple_of_time_bin"],
            }),
            &json!({
                "type": "rule_skipped",
                "rule_id": "test_unbinned_login",
                "reason": "not_streamable",
                "violations": ["aggregate_without_time_bin"],
            }),
        ]
    );
    assert!(
        lines[1..=skipped.len()]
            .iter()
            .all(|line| line["type"] == "rule_skipped"),
        "{lines:#?}"
    );
    assert_eq!(
        signal_ids(&lines),
        ["test_console_login", "test_console_login"]
    );
    assert_eq!(of_type(&lines, "scenario_match").len(), 1, "{lines:#?}");
    for rule_id in ["test_binned_login", "test_unbinned_login"] {
        assert!(
            stderr
                .lines()
                .any(|line| line.starts_with("warning: ") && line.contains(rule_id)),
            "{rule_id} in {stderr}"
        );
    }
    assert!(stderr.contains("7m"), "{stderr}");
}

#[test]
fn run_evaluates_the_rules_whose_time_bin_divides_the_batch() {
    let case = RunCase::new().with_streaming_rules(None);

    let lines = case.run(&["--emit-signals"]);

    let skipped: Vec<&str> = of_type(&lines, "rule_skipped")
        .into_iter()
        .map(|line| line["rule_id"].as_str().unwrap())
        .collect();
    assert_eq!(skipped, ["test_unbinned_login"], "{lines:#?}");
    assert_eq!(
        signal_ids(&lines),
        [
            "test_binned_login",
            "test_binned_login",
            "test_console_login",
            "test_console_login"
        ]
    );
}
