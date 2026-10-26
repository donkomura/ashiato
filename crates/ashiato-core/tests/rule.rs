use ashiato_core::rule::{
    DataRequirement, Rule, RuleError, RuleFile, RuleSource, Rules, ScenarioStep,
};
use ashiato_core::{Duration, Severity, Tactic};
use serde_json::json;

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
  SELECT min(event_time) AS start_time FROM events
"#;

const CROSS_ACCOUNT_S3_DESTRUCTION: &str = r#"
kind: scenario
id: cross_account_s3_destruction
title: クロスアカウントのロール引き受けから S3 データを持ち出して削除する
severity: critical
tactics: [TA0001, TA0007, TA0010, TA0040]
window: 1h
steps:
  - signal: cross_account_assume_role
  - signal: s3_bucket_enumeration
    optional: true
  - signal: s3_object_read_or_copy
    same: [bucket]
  - signal: s3_delete_burst
    max_gap: 30m
    same: [bucket]
    min_count: 2
    distinct_by: bucket
"#;

fn file(path: &str, content: &str) -> RuleFile {
    RuleFile {
        path: path.to_owned(),
        content: content.to_owned(),
    }
}

fn signal(id: &str, attrs: &[&str]) -> String {
    format!(
        "kind: signal\nid: {id}\ntitle: {id}\nseverity: low\ntactics: [TA0007]\nattrs: [{}]\nsql: SELECT 1\n",
        attrs.join(", ")
    )
}

fn scenario(id: &str, steps: &str) -> String {
    format!(
        "kind: scenario\nid: {id}\ntitle: {id}\nseverity: high\ntactics: [TA0001]\nwindow: 1h\nsteps:\n{steps}"
    )
}

fn parse_one(content: &str) -> Result<Rule, RuleError> {
    let set = RuleSource::parse("test", vec![file("rule.yaml", content)])?;
    Ok(set.rules()[0].clone())
}

fn rule_source(id: &str, files: &[(&str, &str)]) -> RuleSource {
    RuleSource::parse(
        id,
        files
            .iter()
            .map(|(path, content)| file(path, content))
            .collect(),
    )
    .unwrap()
}

fn s3_destruction_signals() -> Vec<(String, String)> {
    vec![
        (
            "cross_account_assume_role/rule.yaml".to_owned(),
            signal("cross_account_assume_role", &[]),
        ),
        (
            "s3_bucket_enumeration/rule.yaml".to_owned(),
            signal("s3_bucket_enumeration", &[]),
        ),
        (
            "s3_object_read_or_copy/rule.yaml".to_owned(),
            signal("s3_object_read_or_copy", &["bucket"]),
        ),
        (
            "s3_delete_burst/rule.yaml".to_owned(),
            S3_DELETE_BURST.to_owned(),
        ),
    ]
}

fn sorted_ids(rules: &Rules) -> Vec<String> {
    let mut ids: Vec<String> = rules.iter().map(|rule| rule.id().to_owned()).collect();
    ids.sort();
    ids
}

#[test]
fn signal_rule_is_parsed() {
    let Rule::Signal(rule) = parse_one(S3_DELETE_BURST).unwrap() else {
        panic!("expected a signal rule");
    };

    assert_eq!(rule.id, "s3_delete_burst");
    assert_eq!(rule.title, "短時間に集中した S3 オブジェクトの削除");
    assert_eq!(rule.severity, Severity::High);
    assert_eq!(rule.tactics, [Tactic::Impact]);
    assert_eq!(rule.requires, [DataRequirement::S3DataEvents]);
    assert_eq!(rule.params["min_deletes"], json!(3));
    assert_eq!(rule.params["bin"], json!("5m"));
    assert_eq!(rule.attrs, ["bucket"]);
    assert_eq!(
        rule.sql,
        "SELECT min(event_time) AS start_time FROM events\n"
    );
}

#[test]
fn optional_keys_of_signal_rule_have_defaults() {
    let Rule::Signal(rule) = parse_one(&signal("minimal", &[])).unwrap() else {
        panic!("expected a signal rule");
    };

    assert!(rule.requires.is_empty());
    assert!(rule.params.is_empty());
    assert!(rule.attrs.is_empty());
    assert!(!rule.external_source_only);
}

#[test]
fn external_source_only_is_read_from_signal_rule() {
    let content = signal("probe", &[]) + "external_source_only: true\n";

    let Rule::Signal(rule) = parse_one(&content).unwrap() else {
        panic!("expected a signal rule");
    };

    assert!(rule.external_source_only);
}

#[test]
fn scenario_rule_is_parsed() {
    let Rule::Scenario(rule) = parse_one(CROSS_ACCOUNT_S3_DESTRUCTION).unwrap() else {
        panic!("expected a scenario rule");
    };

    assert_eq!(rule.id, "cross_account_s3_destruction");
    assert_eq!(rule.severity, Severity::Critical);
    assert_eq!(
        rule.tactics,
        [
            Tactic::InitialAccess,
            Tactic::Discovery,
            Tactic::Exfiltration,
            Tactic::Impact
        ]
    );
    assert_eq!(rule.window, "1h".parse::<Duration>().unwrap());
    assert_eq!(
        rule.steps,
        [
            ScenarioStep {
                signal: "cross_account_assume_role".to_owned(),
                optional: false,
                min_count: 1,
                distinct_by: None,
                max_gap: None,
                same: vec![],
            },
            ScenarioStep {
                signal: "s3_bucket_enumeration".to_owned(),
                optional: true,
                min_count: 1,
                distinct_by: None,
                max_gap: None,
                same: vec![],
            },
            ScenarioStep {
                signal: "s3_object_read_or_copy".to_owned(),
                optional: false,
                min_count: 1,
                distinct_by: None,
                max_gap: None,
                same: vec!["bucket".to_owned()],
            },
            ScenarioStep {
                signal: "s3_delete_burst".to_owned(),
                optional: false,
                min_count: 2,
                distinct_by: Some("bucket".to_owned()),
                max_gap: Some("30m".parse().unwrap()),
                same: vec!["bucket".to_owned()],
            },
        ]
    );
}

#[test]
fn unknown_key_is_rejected() {
    let misspelled_signal_key = S3_DELETE_BURST.replace("attrs:", "attributes:");
    let misspelled_step_key = CROSS_ACCOUNT_S3_DESTRUCTION.replace("optional:", "optinal:");

    for content in [misspelled_signal_key, misspelled_step_key] {
        let error = parse_one(&content).unwrap_err();
        assert!(
            matches!(&error, RuleError::Parse { path, .. } if path == "rule.yaml"),
            "unexpected error: {error:?}"
        );
    }
}

#[test]
fn unknown_kind_and_requirement_are_rejected() {
    let unknown_kind = S3_DELETE_BURST.replace("kind: signal", "kind: detector");
    let unknown_requirement = S3_DELETE_BURST.replace("s3_data_events", "lambda_data_events");

    for content in [unknown_kind, unknown_requirement] {
        assert!(matches!(parse_one(&content), Err(RuleError::Parse { .. })));
    }
}

#[test]
fn duplicate_id_in_one_rule_source_is_rejected() {
    let error = RuleSource::parse(
        "builtin",
        vec![
            file("a/rule.yaml", &signal("same_id", &[])),
            file("b/rule.yaml", &signal("same_id", &[])),
        ],
    )
    .unwrap_err();

    assert!(
        matches!(&error, RuleError::DuplicateId { id, rule_source } if id == "same_id" && rule_source == "builtin"),
        "unexpected error: {error:?}"
    );
}

#[test]
fn rules_built_from_valid_rule_source_contain_all_rules() {
    let mut files = s3_destruction_signals();
    files.push((
        "cross_account_s3_destruction/rule.yaml".to_owned(),
        CROSS_ACCOUNT_S3_DESTRUCTION.to_owned(),
    ));
    let files: Vec<(&str, &str)> = files
        .iter()
        .map(|(path, content)| (path.as_str(), content.as_str()))
        .collect();

    let rules = Rules::new([rule_source("builtin", &files)]).unwrap();

    assert_eq!(
        sorted_ids(&rules),
        [
            "cross_account_assume_role",
            "cross_account_s3_destruction",
            "s3_bucket_enumeration",
            "s3_delete_burst",
            "s3_object_read_or_copy",
        ]
    );
    assert!(matches!(
        rules.get("cross_account_s3_destruction"),
        Some(Rule::Scenario(_))
    ));
}

#[test]
fn scenario_referring_to_missing_signal_is_rejected() {
    let missing = scenario("uses_missing", "  - signal: no_such_signal\n");
    let set = rule_source("builtin", &[("uses_missing/rule.yaml", &missing)]);

    let error = Rules::new([set]).unwrap_err();

    assert!(
        matches!(&error, RuleError::UnknownSignal { scenario, signal } if scenario == "uses_missing" && signal == "no_such_signal"),
        "unexpected error: {error:?}"
    );
}

#[test]
fn scenario_referring_to_another_scenario_is_rejected() {
    let inner = scenario("inner", "  - signal: base\n");
    let outer = scenario("outer", "  - signal: inner\n");
    let set = rule_source(
        "builtin",
        &[
            ("base/rule.yaml", &signal("base", &[])),
            ("inner/rule.yaml", &inner),
            ("outer/rule.yaml", &outer),
        ],
    );

    let error = Rules::new([set]).unwrap_err();

    assert!(
        matches!(&error, RuleError::UnknownSignal { scenario, signal } if scenario == "outer" && signal == "inner"),
        "unexpected error: {error:?}"
    );
}

#[test]
fn attribute_used_by_same_or_distinct_by_must_be_declared() {
    let uses_same = scenario(
        "uses_same",
        "  - signal: base\n    same: [bucket]\n  - signal: base\n    same: [bucket]\n",
    );
    let uses_distinct_by = scenario(
        "uses_distinct_by",
        "  - signal: base\n    min_count: 2\n    distinct_by: role_arn\n",
    );

    for (scenario_id, content, attr) in [
        ("uses_same", uses_same, "bucket"),
        ("uses_distinct_by", uses_distinct_by, "role_arn"),
    ] {
        let set = rule_source(
            "builtin",
            &[
                ("base/rule.yaml", &signal("base", &["other"])),
                ("scenario/rule.yaml", &content),
            ],
        );

        let error = Rules::new([set]).unwrap_err();

        assert!(
            matches!(
                &error,
                RuleError::UndeclaredAttribute { scenario, signal, attr: undeclared }
                    if scenario == scenario_id && signal == "base" && undeclared == attr
            ),
            "unexpected error: {error:?}"
        );
    }
}

#[test]
fn custom_rule_replaces_builtin_rule_with_same_id_and_adds_new_ones() {
    let builtin = rule_source(
        "builtin",
        &[
            ("kept/rule.yaml", &signal("kept", &[])),
            ("replaced/rule.yaml", &signal("replaced", &[])),
        ],
    );
    let custom = rule_source(
        "custom",
        &[
            ("replaced/rule.yaml", &signal("replaced", &["bucket"])),
            ("added/rule.yaml", &signal("added", &[])),
        ],
    );

    let rules = Rules::new([builtin, custom]).unwrap();

    assert_eq!(sorted_ids(&rules), ["added", "kept", "replaced"]);
    let Some(Rule::Signal(replaced)) = rules.get("replaced") else {
        panic!("expected the replaced signal rule");
    };
    assert_eq!(replaced.attrs, ["bucket"]);
}

#[test]
fn references_are_checked_after_custom_rules_are_merged() {
    let builtin = rule_source(
        "builtin",
        &[
            ("base/rule.yaml", &signal("base", &["bucket"])),
            (
                "uses_base/rule.yaml",
                &scenario("uses_base", "  - signal: base\n    same: [bucket]\n"),
            ),
        ],
    );
    let custom = rule_source("custom", &[("base/rule.yaml", &signal("base", &[]))]);

    let error = Rules::new([builtin, custom]).unwrap_err();

    assert!(
        matches!(&error, RuleError::UndeclaredAttribute { scenario, .. } if scenario == "uses_base"),
        "unexpected error: {error:?}"
    );
}

#[test]
fn custom_scenario_can_refer_to_builtin_signal() {
    let builtin = rule_source("builtin", &[("base/rule.yaml", &signal("base", &[]))]);
    let custom = rule_source(
        "custom",
        &[(
            "uses_base/rule.yaml",
            &scenario("uses_base", "  - signal: base\n"),
        )],
    );

    let rules = Rules::new([builtin, custom]).unwrap();

    assert_eq!(sorted_ids(&rules), ["base", "uses_base"]);
}

#[test]
fn rules_report_id_and_digest_of_each_rule_source() {
    let builtin = rule_source("builtin", &[("a/rule.yaml", &signal("a", &[]))]);
    let custom = rule_source("custom", &[("b/rule.yaml", &signal("b", &[]))]);
    let expected = [
        ("builtin".to_owned(), builtin.digest().to_owned()),
        ("custom".to_owned(), custom.digest().to_owned()),
    ];

    let rules = Rules::new([builtin, custom]).unwrap();

    let reported: Vec<(String, String)> = rules
        .rule_sources()
        .iter()
        .map(|set| (set.id.clone(), set.digest.clone()))
        .collect();
    assert_eq!(reported, expected);
}

#[test]
fn digest_is_sha256_of_rule_files() {
    let digest = rule_source("builtin", &[("a/rule.yaml", &signal("a", &[]))])
        .digest()
        .to_owned();

    let hex = digest.strip_prefix("sha256:").unwrap();
    assert_eq!(hex.len(), 64);
    assert!(
        hex.chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    );
}

#[test]
fn digest_does_not_depend_on_file_order() {
    let a = ("a/rule.yaml", signal("a", &[]));
    let b = ("b/rule.yaml", signal("b", &[]));

    let forward = rule_source("builtin", &[(a.0, &a.1), (b.0, &b.1)]);
    let backward = rule_source("builtin", &[(b.0, &b.1), (a.0, &a.1)]);

    assert_eq!(forward.digest(), backward.digest());
}

#[test]
fn digest_changes_when_rule_content_or_path_changes() {
    let original = rule_source("builtin", &[("a/rule.yaml", &signal("a", &[]))]);
    let edited = rule_source("builtin", &[("a/rule.yaml", &signal("a", &["bucket"]))]);
    let moved = rule_source("builtin", &[("moved/rule.yaml", &signal("a", &[]))]);

    assert_ne!(original.digest(), edited.digest());
    assert_ne!(original.digest(), moved.digest());
}

#[test]
fn builtin_rules_are_loaded_and_valid() {
    let builtin = RuleSource::builtin().unwrap();

    assert_eq!(builtin.id(), "builtin");
    Rules::new([builtin]).unwrap();
}

#[test]
fn invalid_duration_in_scenario_is_rejected() {
    let invalid_window = CROSS_ACCOUNT_S3_DESTRUCTION.replace("window: 1h", "window: 1hour");
    let invalid_max_gap = CROSS_ACCOUNT_S3_DESTRUCTION.replace("max_gap: 30m", "max_gap: 0m");

    for content in [invalid_window, invalid_max_gap] {
        assert!(matches!(parse_one(&content), Err(RuleError::Parse { .. })));
    }
}
