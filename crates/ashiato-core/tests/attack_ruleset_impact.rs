use std::collections::BTreeMap;

use ashiato_core::rule::{DataRequirement, Rule, RuleSource, Rules};
use ashiato_core::ruleset::Rulesets;
use ashiato_core::{Severity, Tactic};

const ATTACK_RULESET_ID: &str = "attack-v20";

fn builtin_rules() -> Rules {
    Rules::new([RuleSource::builtin().unwrap()]).unwrap()
}

fn attack_refs() -> BTreeMap<String, Vec<String>> {
    Rulesets::builtin()
        .unwrap()
        .get(ATTACK_RULESET_ID)
        .unwrap()
        .rules
        .iter()
        .map(|entry| (entry.id.clone(), entry.refs.clone()))
        .collect()
}

#[test]
fn attack_ruleset_includes_the_impact_rules() {
    let refs = attack_refs();

    for (id, expected) in [
        ("resource_destruction_burst", vec!["T1485"]),
        ("recovery_inhibited", vec!["T1490"]),
        ("backup_inhibit_then_destruction", vec!["T1490", "T1485"]),
        ("s3_lifecycle_expiration_set", vec!["T1485.001"]),
        ("s3_ssec_encryption", vec!["T1486"]),
    ] {
        assert_eq!(
            refs.get(id),
            Some(&expected.iter().map(|r| (*r).to_owned()).collect()),
            "{id}"
        );
    }
}

#[test]
fn default_ruleset_does_not_include_the_impact_rules() {
    let sets = Rulesets::builtin().unwrap();
    let default = sets.get("default").unwrap();

    for id in [
        "resource_destruction_burst",
        "recovery_inhibited",
        "backup_inhibit_then_destruction",
        "s3_lifecycle_expiration_set",
        "s3_ssec_encryption",
    ] {
        assert!(default.rules.iter().all(|entry| entry.id != id), "{id}");
    }
}

#[test]
fn impact_signals_have_the_planned_severity_and_tactics() {
    let rules = builtin_rules();

    for (id, severity) in [
        ("resource_destruction_burst", Severity::High),
        ("recovery_inhibited", Severity::High),
        ("s3_lifecycle_expiration_set", Severity::Medium),
        ("s3_ssec_encryption", Severity::Critical),
    ] {
        let Some(Rule::Signal(rule)) = rules.get(id) else {
            panic!("{id} is not a signal rule");
        };
        assert_eq!(rule.severity, severity, "{id}");
        assert_eq!(rule.tactics, [Tactic::Impact], "{id}");
    }
}

#[test]
fn ssec_encryption_requires_s3_data_events() {
    let rules = builtin_rules();

    let Some(Rule::Signal(rule)) = rules.get("s3_ssec_encryption") else {
        panic!("s3_ssec_encryption is not a signal rule");
    };
    assert_eq!(rule.requires, [DataRequirement::S3DataEvents]);
}

#[test]
fn backup_inhibit_then_destruction_follows_inhibition_with_destruction() {
    let rules = builtin_rules();

    let Some(Rule::Scenario(scenario)) = rules.get("backup_inhibit_then_destruction") else {
        panic!("backup_inhibit_then_destruction is not a scenario rule");
    };
    let steps: Vec<&str> = scenario
        .steps
        .iter()
        .map(|step| step.signal.as_str())
        .collect();
    assert_eq!(steps, ["recovery_inhibited", "resource_destruction_burst"]);
    assert_eq!(scenario.tactics, [Tactic::Impact]);
}
