use std::fs;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use ashiato_core::Duration;
use ashiato_core::context::Context;
use ashiato_pipeline::{
    BrokerSettings, ByteSizeError, PlacementMode, PlanError, RuleOrigin, RunPlan, SourceLocation,
    parse_byte_size,
};
use chrono::{TimeDelta, TimeZone, Utc};

const BASE_DIR: &str = "/cases/case-1";

const FULL_PLAN: &str = r#"
run_id: case-2026-10-xx
source:
  kind: local
  uri: ./logs
  accounts: ["111111111111", "222222222222"]
  regions: [us-east-1, ap-northeast-1]
analysis_window:
  since: 2026-09-24T00:00:00Z
  until: 2026-10-25T00:00:00Z
rules: [builtin, ./rules, /shared/rules]
ruleset_dirs: [./rulesets]
rulesets: [attack-v20]
rule: [console_takeover_mining]
pipeline:
  batch: 2h
  allowed_lateness: 30m
  max_skew: 3h
placement:
  mode: single_process
  ingest_workers: 1
broker:
  max_bytes: 50GiB
  max_messages_per_partition: 32
  max_message_size: 512KiB
  duplicate_window: 5m
  ack_wait: 15m
"#;

const MINIMAL_PLAN: &str = r#"
run_id: case-1
source:
  kind: local
  uri: logs
analysis_window:
  since: 2026-09-24T00:00:00Z
  until: 2026-10-25T00:00:00Z
pipeline:
  allowed_lateness: 30m
placement:
  mode: single_process
"#;

fn parse(text: &str) -> Result<RunPlan, PlanError> {
    RunPlan::parse(text, Path::new(BASE_DIR))
}

fn duration(text: &str) -> Duration {
    text.parse().unwrap()
}

fn digest_of(text: &str) -> String {
    parse(text).unwrap().digest().to_owned()
}

#[test]
fn full_plan_is_read_into_typed_settings() {
    let plan = parse(FULL_PLAN).unwrap();

    assert_eq!(plan.run_id, "case-2026-10-xx");
    assert_eq!(
        plan.source.location,
        SourceLocation::Local {
            root: PathBuf::from("/cases/case-1/logs")
        }
    );
    assert_eq!(
        plan.source.accounts,
        Some(vec!["111111111111".to_owned(), "222222222222".to_owned()])
    );
    assert_eq!(
        plan.source.regions,
        Some(vec!["us-east-1".to_owned(), "ap-northeast-1".to_owned()])
    );
    assert_eq!(
        plan.rules,
        vec![
            RuleOrigin::Builtin,
            RuleOrigin::Directory(PathBuf::from("/cases/case-1/rules")),
            RuleOrigin::Directory(PathBuf::from("/shared/rules")),
        ]
    );
    assert_eq!(
        plan.ruleset_dirs,
        vec![PathBuf::from("/cases/case-1/rulesets")]
    );
    assert_eq!(plan.rulesets, vec!["attack-v20".to_owned()]);
    assert_eq!(plan.rule, vec!["console_takeover_mining".to_owned()]);
    assert_eq!(plan.pipeline.batch, duration("2h"));
    assert_eq!(plan.pipeline.allowed_lateness, duration("30m"));
    assert_eq!(plan.pipeline.max_skew, Some(duration("3h")));
    assert_eq!(plan.placement.mode, PlacementMode::SingleProcess);
    assert_eq!(plan.placement.nats_url, None);
    assert_eq!(plan.placement.ingest_workers, NonZeroU32::new(1).unwrap());
    assert_eq!(
        plan.broker,
        BrokerSettings {
            max_bytes: 50 << 30,
            max_messages_per_partition: 32,
            max_message_size: 512 << 10,
            duplicate_window: duration("5m"),
            ack_wait: duration("15m"),
        }
    );
}

#[test]
fn omitted_settings_take_their_defaults() {
    let plan = parse(MINIMAL_PLAN).unwrap();

    assert_eq!(plan.source.accounts, None);
    assert_eq!(plan.source.regions, None);
    assert_eq!(plan.rules, vec![RuleOrigin::Builtin]);
    assert!(plan.ruleset_dirs.is_empty());
    assert!(plan.rulesets.is_empty());
    assert!(plan.rule.is_empty());
    assert_eq!(plan.pipeline.batch, duration("1h"));
    assert_eq!(plan.pipeline.max_skew, None);
    assert_eq!(plan.placement.ingest_workers, NonZeroU32::new(1).unwrap());
    assert_eq!(plan.broker, BrokerSettings::default());
    assert_eq!(plan.broker.max_messages_per_partition, 64);
    assert_eq!(plan.broker.max_message_size, 1 << 20);
}

#[test]
fn broker_settings_omitted_individually_take_their_defaults() {
    let plan = parse(&format!(
        "{MINIMAL_PLAN}broker:\n  max_message_size: 1048576\n"
    ))
    .unwrap();

    assert_eq!(
        plan.broker,
        BrokerSettings {
            max_message_size: 1_048_576,
            ..BrokerSettings::default()
        }
    );
}

#[test]
fn features_not_implemented_yet_can_still_be_read() {
    let plan = parse(
        r#"
run_id: case-1
source:
  kind: s3
  uri: s3://trail-bucket/AWSLogs/o-xxxx/
  endpoint_url: http://minio:9000
analysis_window:
  since: 2026-09-24T00:00:00Z
  until: 2026-10-25T00:00:00Z
pipeline:
  allowed_lateness: 30m
placement:
  mode: distributed
  nats_url: nats://10.0.0.10:4222
  ingest_workers: 8
"#,
    )
    .unwrap();

    assert_eq!(
        plan.source.location,
        SourceLocation::S3 {
            uri: "s3://trail-bucket/AWSLogs/o-xxxx/".to_owned(),
            endpoint_url: Some("http://minio:9000".to_owned()),
        }
    );
    assert_eq!(plan.placement.mode, PlacementMode::Distributed);
    assert_eq!(
        plan.placement.nats_url.as_deref(),
        Some("nats://10.0.0.10:4222")
    );
    assert_eq!(plan.placement.ingest_workers, NonZeroU32::new(8).unwrap());
}

#[test]
fn allowed_lateness_must_be_given() {
    let text = MINIMAL_PLAN.replace("  allowed_lateness: 30m\n", "  batch: 1h\n");

    let error = parse(&text).unwrap_err();

    assert!(matches!(error, PlanError::Parse(_)), "{error:?}");
    assert!(
        format!("{:?}", error).contains("allowed_lateness"),
        "{error:?}"
    );
}

#[test]
fn unknown_keys_are_rejected() {
    for text in [
        format!("{MINIMAL_PLAN}contexts: ./context.yaml\n"),
        MINIMAL_PLAN.replace("  uri: logs\n", "  uri: logs\n  prefix: AWSLogs/\n"),
        MINIMAL_PLAN.replace(
            "  mode: single_process\n",
            "  mode: single_process\n  workers: 2\n",
        ),
        format!("{MINIMAL_PLAN}broker:\n  max_bytes: 1GiB\n  max_size: 1MiB\n"),
    ] {
        let error = parse(&text).unwrap_err();
        assert!(matches!(error, PlanError::Parse(_)), "{text}: {error:?}");
    }
}

#[test]
fn invalid_values_are_rejected() {
    for text in [
        MINIMAL_PLAN.replace("allowed_lateness: 30m", "allowed_lateness: 30 minutes"),
        MINIMAL_PLAN.replace("kind: local", "kind: gcs"),
        MINIMAL_PLAN.replace("mode: single_process", "mode: cluster"),
        MINIMAL_PLAN.replace("since: 2026-09-24T00:00:00Z", "since: yesterday"),
        format!("{MINIMAL_PLAN}broker:\n  max_bytes: 50GB\n"),
    ] {
        let error = parse(&text).unwrap_err();
        assert!(matches!(error, PlanError::Parse(_)), "{text}: {error:?}");
    }
}

#[test]
fn endpoint_url_is_rejected_for_a_local_source() {
    let text = MINIMAL_PLAN.replace(
        "  uri: logs\n",
        "  uri: logs\n  endpoint_url: http://minio:9000\n",
    );

    let error = parse(&text).unwrap_err();

    assert!(
        matches!(error, PlanError::EndpointUrlForLocalSource),
        "{error:?}"
    );
}

#[test]
fn analysis_window_reaches_back_by_the_baseline_lookback() {
    let window = parse(MINIMAL_PLAN).unwrap().analysis_window();

    let since = Utc.with_ymd_and_hms(2026, 9, 24, 0, 0, 0).unwrap();
    assert_eq!(window.since(), since);
    assert_eq!(
        window.until(),
        Utc.with_ymd_and_hms(2026, 10, 25, 0, 0, 0).unwrap()
    );
    assert_eq!(window.baseline_since(), since - TimeDelta::days(30));
}

#[test]
fn empty_analysis_window_is_rejected() {
    let text = MINIMAL_PLAN.replace("until: 2026-10-25T00:00:00Z", "until: 2026-09-24T00:00:00Z");

    let error = parse(&text).unwrap_err();

    assert!(matches!(error, PlanError::Window(_)), "{error:?}");
}

#[test]
fn load_resolves_relative_paths_against_the_directory_of_the_plan() {
    let dir = tempfile::tempdir().unwrap();
    let case = dir.path().join("case");
    fs::create_dir(&case).unwrap();
    let path = case.join("run.yaml");
    fs::write(&path, FULL_PLAN).unwrap();

    let plan = RunPlan::load(&path).unwrap();

    assert_eq!(
        plan.source.location,
        SourceLocation::Local {
            root: case.join("logs")
        }
    );
    assert_eq!(
        plan.rules,
        vec![
            RuleOrigin::Builtin,
            RuleOrigin::Directory(case.join("rules")),
            RuleOrigin::Directory(PathBuf::from("/shared/rules")),
        ]
    );
    assert_eq!(plan.ruleset_dirs, vec![case.join("rulesets")]);
    assert_eq!(plan.digest(), digest_of(FULL_PLAN));
}

#[test]
fn load_reports_a_missing_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("run.yaml");

    let error = RunPlan::load(&path).unwrap_err();

    assert!(
        matches!(&error, PlanError::Read { path: reported, .. } if *reported == path),
        "{error:?}"
    );
}

#[test]
fn digest_is_a_sha256_of_the_plan() {
    let digest = digest_of(FULL_PLAN);

    let hex = digest.strip_prefix("sha256:").unwrap();
    assert_eq!(hex.len(), 64);
    assert!(
        hex.bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
}

#[test]
fn digest_ignores_the_yaml_formatting() {
    let reformatted = r#"
# The same plan with other key order, layout and comments.
rule:
  - console_takeover_mining
rulesets:
  - attack-v20
ruleset_dirs:
  - ./rulesets
rules:
  - builtin
  - ./rules
  - /shared/rules
pipeline: {max_skew: 3h, allowed_lateness: 30m, batch: 2h}
analysis_window:
    until: 2026-10-25T00:00:00Z   # end of the investigation
    since: 2026-09-24T00:00:00Z
source:
    regions:
      - us-east-1
      - ap-northeast-1
    accounts:
      - "111111111111"
      - '222222222222'
    uri: ./logs
    kind: local
broker:
  ack_wait: 15m
  duplicate_window: 5m
  max_message_size: 512KiB
  max_messages_per_partition: 32
  max_bytes: 50GiB
placement:
  ingest_workers: 1
  mode: single_process
run_id: "case-2026-10-xx"
"#;

    assert_eq!(digest_of(reformatted), digest_of(FULL_PLAN));
}

#[test]
fn digest_ignores_placement_and_broker() {
    let text = FULL_PLAN
        .replace(
            "  mode: single_process\n  ingest_workers: 1\n",
            "  mode: distributed\n  nats_url: nats://10.0.0.10:4222\n  ingest_workers: 8\n",
        )
        .replace("max_bytes: 50GiB", "max_bytes: 10GiB")
        .replace("ack_wait: 15m", "ack_wait: 1h");
    assert_ne!(text, FULL_PLAN);

    assert_eq!(digest_of(&text), digest_of(FULL_PLAN));
    let without_broker = FULL_PLAN.split("broker:").next().unwrap();
    assert_eq!(digest_of(without_broker), digest_of(FULL_PLAN));
}

#[test]
fn digest_changes_with_settings_that_affect_the_output() {
    let base = digest_of(FULL_PLAN);
    for (from, to) in [
        ("run_id: case-2026-10-xx", "run_id: case-2026-10-yy"),
        ("uri: ./logs", "uri: ./other-logs"),
        ("\"222222222222\"]", "\"333333333333\"]"),
        ("ap-northeast-1]", "eu-west-1]"),
        ("since: 2026-09-24T00:00:00Z", "since: 2026-09-25T00:00:00Z"),
        ("until: 2026-10-25T00:00:00Z", "until: 2026-10-26T00:00:00Z"),
        (
            "rules: [builtin, ./rules, /shared/rules]",
            "rules: [builtin]",
        ),
        ("ruleset_dirs: [./rulesets]", "ruleset_dirs: [./other]"),
        ("rulesets: [attack-v20]", "rulesets: [default]"),
        ("rule: [console_takeover_mining]", "rule: []"),
        ("batch: 2h", "batch: 1h"),
        ("allowed_lateness: 30m", "allowed_lateness: 1h"),
        ("max_skew: 3h", "max_skew: 2h"),
    ] {
        let text = FULL_PLAN.replace(from, to);
        assert_ne!(text, FULL_PLAN, "{from} is not in the plan");
        assert_ne!(digest_of(&text), base, "changing {from} to {to}");
    }
}

#[test]
fn byte_sizes_take_binary_units() {
    for (text, bytes) in [
        ("1024", 1024),
        ("512KiB", 512 << 10),
        ("1MiB", 1 << 20),
        ("50GiB", 50 << 30),
        ("2TiB", 2 << 40),
    ] {
        assert_eq!(parse_byte_size(text), Ok(bytes), "{text}");
    }
}

#[test]
fn malformed_byte_sizes_are_rejected() {
    for text in [
        "",
        "0",
        "0MiB",
        "MiB",
        "1.5MiB",
        "-1",
        "10MB",
        "10 MiB",
        "10mib",
        "1KiBKiB",
        "99999999TiB",
    ] {
        assert_eq!(
            parse_byte_size(text),
            Err(ByteSizeError::Invalid(text.to_owned())),
            "{text}"
        );
    }
}

const CONTEXT: &str = r#"
suppress:
  - origin: "workload:*"
baseline:
  lookback: 7d
"#;

struct ContextCase {
    dir: tempfile::TempDir,
}

impl ContextCase {
    fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
        }
    }

    fn write(&self, relative: &str, content: &str) -> PathBuf {
        let path = self.dir.path().join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, content).unwrap();
        path
    }

    fn load(&self, context: &str, content: &str) -> Result<RunPlan, PlanError> {
        self.write(&format!("case/{context}"), content);
        let plan = self.write(
            "case/run.yaml",
            &format!("{MINIMAL_PLAN}context: ./{context}\n"),
        );
        RunPlan::load(&plan)
    }
}

#[test]
fn load_reads_the_context_file_relative_to_the_plan() {
    let case = ContextCase::new();

    let plan = case.load("context.yaml", CONTEXT).unwrap();

    assert_eq!(plan.context, Context::parse(CONTEXT).unwrap());
    assert!(plan.context.digest().is_some());
    assert_eq!(plan.context.suppress.len(), 1);
}

#[test]
fn a_plan_without_a_context_uses_the_default_context() {
    let plan = parse(MINIMAL_PLAN).unwrap();

    assert_eq!(plan.context, Context::default());
    assert_eq!(plan.context.digest(), None);
}

#[test]
fn the_lookback_of_the_context_sets_the_baseline_period() {
    let case = ContextCase::new();

    let window = case
        .load("context.yaml", CONTEXT)
        .unwrap()
        .analysis_window();

    let since = Utc.with_ymd_and_hms(2026, 9, 24, 0, 0, 0).unwrap();
    assert_eq!(window.since(), since);
    assert_eq!(window.baseline_since(), since - TimeDelta::days(7));
}

#[test]
fn digest_changes_with_the_content_of_the_context_file() {
    let case = ContextCase::new();
    let base = case.load("context.yaml", CONTEXT).unwrap();

    let changed = case
        .load("context.yaml", &CONTEXT.replace("workload:*", "role:*"))
        .unwrap();

    assert_ne!(changed.digest(), base.digest());
    assert_ne!(base.digest(), digest_of(MINIMAL_PLAN));
}

#[test]
fn digest_does_not_depend_on_the_path_of_the_context_file() {
    let first = ContextCase::new().load("context.yaml", CONTEXT).unwrap();

    let moved = ContextCase::new()
        .load("shared/case-context.yaml", CONTEXT)
        .unwrap();

    assert_eq!(moved.digest(), first.digest());
}

#[test]
fn a_missing_context_file_is_an_error() {
    let case = ContextCase::new();
    let plan = case.write(
        "case/run.yaml",
        &format!("{MINIMAL_PLAN}context: ./context.yaml\n"),
    );

    let error = RunPlan::load(&plan).unwrap_err();

    let expected = case.dir.path().join("case/context.yaml");
    assert!(
        matches!(&error, PlanError::ReadContext { path, .. } if *path == expected),
        "{error:?}"
    );
}

#[test]
fn an_invalid_context_file_is_an_error() {
    let case = ContextCase::new();

    let error = case
        .load("context.yaml", "baseline:\n  window: 7d\n")
        .unwrap_err();

    let expected = case.dir.path().join("case/context.yaml");
    assert!(
        matches!(&error, PlanError::Context { path, .. } if *path == expected),
        "{error:?}"
    );
}
