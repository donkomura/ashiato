use std::collections::BTreeMap;

use ashiato_core::Tactic;
use ashiato_core::rule::{Rule, RuleSource, Rules};
use ashiato_core::ruleset::{FrameworkKind, Ruleset, Rulesets};

const ATTACK_RULESET_ID: &str = "attack-v20";

fn builtin_rules() -> Rules {
    Rules::new([RuleSource::builtin().unwrap()]).unwrap()
}

fn attack_ruleset() -> Ruleset {
    Rulesets::builtin()
        .unwrap()
        .get(ATTACK_RULESET_ID)
        .cloned()
        .unwrap()
}

fn tactics(rules: &Rules, id: &str) -> Vec<Tactic> {
    match rules.get(id).unwrap() {
        Rule::Signal(rule) => rule.tactics.clone(),
        Rule::Scenario(rule) => rule.tactics.clone(),
    }
}

#[test]
fn attack_ruleset_declares_attack_v20() {
    let ruleset = attack_ruleset();

    let framework = ruleset.framework.unwrap();
    assert_eq!(framework.kind, FrameworkKind::MitreAttack);
    assert_eq!(framework.version, "20.0");
    assert_eq!(
        framework.url.as_deref(),
        Some("https://attack.mitre.org/versions/v20/")
    );
}

#[test]
fn attack_ruleset_maps_the_existing_rules_to_techniques() {
    let expected: BTreeMap<&str, Vec<&str>> = BTreeMap::from([
        ("console_login_without_mfa", vec!["T1078.004"]),
        (
            "console_login_brute_force",
            vec!["T1110.001", "T1110.003", "T1110.004"],
        ),
        ("root_credential_usage", vec!["T1078.004"]),
        ("cloudshell_activity", vec!["T1059.009"]),
        ("offensive_tool_user_agent", vec!["T1059.009"]),
        ("compute_resource_launch", vec!["T1496.001", "T1578.002"]),
        ("compute_launch_attempts", vec!["T1496.001"]),
        (
            "assume_role_by_assumed_role",
            vec!["T1078.004", "T1548.005"],
        ),
        ("assume_role_brute_force", vec!["T1087.004"]),
        ("iam_privilege_modification", vec!["T1098.003", "T1556.009"]),
        ("self_privilege_escalation", vec!["T1098.003"]),
        ("iam_persistence", vec!["T1136.003", "T1098.001"]),
        (
            "persistence_attempt_denied",
            vec!["T1136.003", "T1098.001", "T1648"],
        ),
        ("first_seen_identity_probe", vec!["T1078.004"]),
        ("temporary_credential_ip_change", vec!["T1550.001"]),
        ("access_denied_enumeration", vec!["T1580", "T1526"]),
        ("broad_service_enumeration", vec!["T1580", "T1526"]),
        ("bucket_name_enumeration", vec!["T1580"]),
        ("novel_api_burst", vec!["T1078.004"]),
        ("role_session_outlier", vec!["T1078.004"]),
        ("first_seen_account_region_write", vec!["T1535"]),
        ("cloudtrail_logging_disabled", vec!["T1685.002"]),
        ("cloudtrail_logging_paused", vec!["T1685.002"]),
        ("cloudtrail_tampering_attempt_denied", vec!["T1685.002"]),
        ("security_monitoring_disabled", vec!["T1685"]),
        ("mfa_device_deactivated", vec!["T1556.006"]),
        ("password_policy_change", vec!["T1556"]),
        ("s3_bucket_public_access_granted", vec!["T1537"]),
        (
            "s3_bucket_permission_change_denied",
            vec!["T1537", "T1491.002"],
        ),
        ("snapshot_shared_externally", vec!["T1537"]),
        ("snapshot_sharing_attempt_denied", vec!["T1537"]),
        ("kms_key_deletion", vec!["T1485"]),
        (
            "console_takeover_mining",
            vec!["T1078.004", "T1059.009", "T1496.001"],
        ),
        ("role_chain_escalation", vec!["T1548.005", "T1098.003"]),
        (
            "leaked_key_recon_persistence",
            vec!["T1078.004", "T1580", "T1098.001"],
        ),
        (
            "leaked_key_compute_hijack_attempt",
            vec!["T1078.004", "T1496.001"],
        ),
    ]);

    let ruleset = attack_ruleset();

    let actual: BTreeMap<&str, Vec<&str>> = ruleset
        .rules
        .iter()
        .filter(|entry| expected.contains_key(entry.id.as_str()))
        .map(|entry| {
            (
                entry.id.as_str(),
                entry.refs.iter().map(String::as_str).collect(),
            )
        })
        .collect();
    assert_eq!(actual, expected);
}

#[test]
fn every_rule_of_the_attack_ruleset_has_refs() {
    for entry in attack_ruleset().rules {
        assert!(!entry.refs.is_empty(), "{} has no refs", entry.id);
    }
}

#[test]
fn rules_that_impair_defenses_belong_to_defense_impairment() {
    let rules = builtin_rules();

    for id in [
        "cloudtrail_logging_disabled",
        "cloudtrail_logging_paused",
        "cloudtrail_tampering_attempt_denied",
        "security_monitoring_disabled",
        "password_policy_change",
    ] {
        assert_eq!(tactics(&rules, id), [Tactic::DefenseImpairment], "{id}");
    }
    assert_eq!(
        tactics(&rules, "mfa_device_deactivated"),
        [Tactic::DefenseImpairment, Tactic::CredentialAccess]
    );
}

#[test]
fn rules_that_hide_activity_stay_in_stealth() {
    let rules = builtin_rules();

    assert_eq!(
        tactics(&rules, "first_seen_account_region_write"),
        [Tactic::Stealth]
    );
}

#[test]
fn denied_bucket_permission_changes_are_exfiltration_and_impact() {
    let rules = builtin_rules();

    assert_eq!(
        tactics(&rules, "s3_bucket_permission_change_denied"),
        [Tactic::Exfiltration, Tactic::Impact]
    );
}
