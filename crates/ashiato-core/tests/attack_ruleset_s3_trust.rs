use std::collections::BTreeMap;

use ashiato_core::rule::{DataRequirement, Rule, RuleSource, Rules, ScenarioRule, SignalRule};
use ashiato_core::ruleset::{DEFAULT_RULESET_ID, Rulesets};
use ashiato_core::{Severity, Tactic};

const ATTACK_RULESET_ID: &str = "attack-v20";

fn builtin_rules() -> Rules {
    Rules::new([RuleSource::builtin().unwrap()]).unwrap()
}

fn signal(rules: &Rules, id: &str) -> SignalRule {
    match rules.get(id) {
        Some(Rule::Signal(rule)) => rule.clone(),
        other => panic!("{id} is not a signal rule: {other:?}"),
    }
}

fn scenario(rules: &Rules, id: &str) -> ScenarioRule {
    match rules.get(id) {
        Some(Rule::Scenario(rule)) => rule.clone(),
        other => panic!("{id} is not a scenario rule: {other:?}"),
    }
}

fn ruleset_refs(ruleset_id: &str) -> BTreeMap<String, Vec<String>> {
    Rulesets::builtin()
        .unwrap()
        .get(ruleset_id)
        .unwrap()
        .rules
        .iter()
        .map(|entry| (entry.id.clone(), entry.refs.clone()))
        .collect()
}

fn cross_account_s3_rules() -> [(&'static str, Vec<&'static str>); 5] {
    [
        ("cross_account_assume_role", vec!["T1199"]),
        ("s3_bucket_enumeration", vec!["T1580"]),
        ("s3_object_read_or_copy", vec!["T1530"]),
        ("s3_delete_burst", vec!["T1485"]),
        (
            "cross_account_s3_destruction",
            vec!["T1199", "T1530", "T1485"],
        ),
    ]
}

fn attack_only_rules() -> [(&'static str, Vec<&'static str>); 3] {
    [
        ("s3_ransom_encryption", vec!["T1486", "T1485.001"]),
        ("trusted_relationship_abuse", vec!["T1199"]),
        ("cloud_service_hijack_probe", vec!["T1496.004"]),
    ]
}

#[test]
fn cross_account_s3_rules_are_in_the_default_ruleset() {
    let default = ruleset_refs(DEFAULT_RULESET_ID);

    for (id, _) in cross_account_s3_rules() {
        assert_eq!(default.get(id), Some(&Vec::new()), "{id}");
    }
}

#[test]
fn attack_ruleset_maps_the_cross_account_s3_rules_to_techniques() {
    let attack = ruleset_refs(ATTACK_RULESET_ID);

    for (id, expected) in cross_account_s3_rules() {
        let expected: Vec<String> = expected.iter().map(|r| (*r).to_owned()).collect();
        assert_eq!(attack.get(id), Some(&expected), "{id}");
    }
}

#[test]
fn attack_ruleset_maps_the_attack_only_rules_to_techniques() {
    let attack = ruleset_refs(ATTACK_RULESET_ID);

    for (id, expected) in attack_only_rules() {
        let expected: Vec<String> = expected.iter().map(|r| (*r).to_owned()).collect();
        assert_eq!(attack.get(id), Some(&expected), "{id}");
    }
}

#[test]
fn attack_only_rules_are_not_in_the_default_ruleset() {
    let default = ruleset_refs(DEFAULT_RULESET_ID);

    for (id, _) in attack_only_rules() {
        assert!(!default.contains_key(id), "{id} is in the default ruleset");
    }
}

#[test]
fn cross_account_s3_signals_follow_design_section_12() {
    let rules = builtin_rules();

    for (id, severity, tactic, requires) in [
        (
            "cross_account_assume_role",
            Severity::Medium,
            Tactic::InitialAccess,
            vec![],
        ),
        (
            "s3_bucket_enumeration",
            Severity::Low,
            Tactic::Discovery,
            vec![],
        ),
        (
            "s3_object_read_or_copy",
            Severity::Medium,
            Tactic::Exfiltration,
            vec![DataRequirement::S3DataEvents],
        ),
        (
            "s3_delete_burst",
            Severity::High,
            Tactic::Impact,
            vec![DataRequirement::S3DataEvents],
        ),
    ] {
        let rule = signal(&rules, id);
        assert_eq!(rule.severity, severity, "{id}");
        assert_eq!(rule.tactics, [tactic], "{id}");
        assert_eq!(rule.requires, requires, "{id}");
    }
}

#[test]
fn cross_account_s3_signals_carry_the_attributes_used_by_the_scenarios() {
    let rules = builtin_rules();

    assert_eq!(
        signal(&rules, "cross_account_assume_role").attrs,
        ["role_arn"]
    );
    assert_eq!(signal(&rules, "s3_object_read_or_copy").attrs, ["bucket"]);
    assert_eq!(signal(&rules, "s3_delete_burst").attrs, ["bucket"]);
}

#[test]
fn cross_account_s3_destruction_follows_design_section_9_1() {
    let rule = scenario(&builtin_rules(), "cross_account_s3_destruction");

    assert_eq!(rule.severity, Severity::Critical);
    assert_eq!(
        rule.tactics,
        [
            Tactic::InitialAccess,
            Tactic::Discovery,
            Tactic::Exfiltration,
            Tactic::Impact,
        ]
    );
    assert_eq!(rule.window.to_string(), "1h");
    let steps: Vec<(&str, bool, Option<String>, Vec<String>)> = rule
        .steps
        .iter()
        .map(|step| {
            (
                step.signal.as_str(),
                step.optional,
                step.max_gap.as_ref().map(ToString::to_string),
                step.same.clone(),
            )
        })
        .collect();
    let bucket = vec!["bucket".to_owned()];
    assert_eq!(
        steps,
        [
            ("cross_account_assume_role", false, None, vec![]),
            ("s3_bucket_enumeration", true, None, vec![]),
            ("s3_object_read_or_copy", false, None, bucket.clone()),
            ("s3_delete_burst", false, Some("30m".to_owned()), bucket),
        ]
    );
}

#[test]
fn s3_ransom_encryption_needs_the_lifecycle_expiration_of_the_encrypted_bucket() {
    let rule = scenario(&builtin_rules(), "s3_ransom_encryption");

    assert_eq!(rule.severity, Severity::Critical);
    assert_eq!(rule.tactics, [Tactic::Impact]);
    assert_eq!(rule.window.to_string(), "24h");
    let steps: Vec<(&str, bool, Vec<String>)> = rule
        .steps
        .iter()
        .map(|step| (step.signal.as_str(), step.optional, step.same.clone()))
        .collect();
    let bucket = vec!["bucket".to_owned()];
    assert_eq!(
        steps,
        [
            ("s3_bucket_enumeration", true, vec![]),
            ("s3_ssec_encryption", false, bucket.clone()),
            ("s3_lifecycle_expiration_set", false, bucket),
        ]
    );
}

#[test]
fn trusted_relationship_abuse_follows_a_cross_account_session_with_persistence() {
    let rule = scenario(&builtin_rules(), "trusted_relationship_abuse");

    assert_eq!(rule.severity, Severity::High);
    assert_eq!(rule.tactics, [Tactic::InitialAccess, Tactic::Persistence]);
    assert_eq!(rule.window.to_string(), "1h");
    let steps: Vec<(&str, Option<String>)> = rule
        .steps
        .iter()
        .map(|step| {
            (
                step.signal.as_str(),
                step.max_gap.as_ref().map(ToString::to_string),
            )
        })
        .collect();
    assert_eq!(
        steps,
        [
            ("cross_account_assume_role", None),
            ("iam_persistence", Some("15m".to_owned())),
        ]
    );
}

#[test]
fn cloud_service_hijack_probe_runs_on_management_events() {
    let rule = signal(&builtin_rules(), "cloud_service_hijack_probe");

    assert_eq!(rule.severity, Severity::Medium);
    assert_eq!(rule.tactics, [Tactic::Impact]);
    assert!(rule.requires.is_empty());
}
