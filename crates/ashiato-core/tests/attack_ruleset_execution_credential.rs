use std::collections::BTreeMap;

use ashiato_core::rule::{Rule, RuleSource, Rules};
use ashiato_core::ruleset::{DEFAULT_RULESET_ID, Ruleset, Rulesets};
use ashiato_core::{Severity, Tactic};

const ATTACK_RULESET_ID: &str = "attack-v20";

const RULES: [&str; 8] = [
    "ssm_remote_command",
    "ec2_instance_connect_key_push",
    "secrets_bulk_retrieval",
    "serverless_function_change",
    "federation_token_issued",
    "traffic_mirroring_created",
    "snapshot_restore_rollback",
    "image_registered",
];

fn builtin_rules() -> Rules {
    Rules::new([RuleSource::builtin().unwrap()]).unwrap()
}

fn ruleset(id: &str) -> Ruleset {
    Rulesets::builtin().unwrap().get(id).cloned().unwrap()
}

#[test]
fn attack_ruleset_maps_the_execution_and_credential_rules_to_techniques() {
    let expected: BTreeMap<&str, Vec<&str>> = BTreeMap::from([
        ("ssm_remote_command", vec!["T1651", "T1021.008"]),
        (
            "ec2_instance_connect_key_push",
            vec!["T1098.004", "T1021.008"],
        ),
        ("secrets_bulk_retrieval", vec!["T1555.006"]),
        ("serverless_function_change", vec!["T1648", "T1546"]),
        ("federation_token_issued", vec!["T1550.001"]),
        ("traffic_mirroring_created", vec!["T1040", "T1020.001"]),
        ("snapshot_restore_rollback", vec!["T1578.001", "T1578.004"]),
        ("image_registered", vec!["T1525"]),
    ]);

    let attack = ruleset(ATTACK_RULESET_ID);

    let actual: BTreeMap<&str, Vec<&str>> = attack
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
fn execution_and_credential_rules_have_the_planned_severity_and_tactics() {
    let expected: [(&str, Severity, Vec<Tactic>); 8] = [
        (
            "ssm_remote_command",
            Severity::Medium,
            vec![Tactic::Execution, Tactic::LateralMovement],
        ),
        (
            "ec2_instance_connect_key_push",
            Severity::Medium,
            vec![Tactic::Persistence, Tactic::LateralMovement],
        ),
        (
            "secrets_bulk_retrieval",
            Severity::High,
            vec![Tactic::CredentialAccess],
        ),
        (
            "serverless_function_change",
            Severity::Medium,
            vec![Tactic::Execution, Tactic::Persistence],
        ),
        (
            "federation_token_issued",
            Severity::Medium,
            vec![Tactic::Persistence, Tactic::LateralMovement],
        ),
        (
            "traffic_mirroring_created",
            Severity::Medium,
            vec![Tactic::CredentialAccess, Tactic::Exfiltration],
        ),
        (
            "snapshot_restore_rollback",
            Severity::Low,
            vec![Tactic::DefenseImpairment],
        ),
        ("image_registered", Severity::Low, vec![Tactic::Persistence]),
    ];
    let rules = builtin_rules();

    for (id, severity, tactics) in expected {
        let Some(Rule::Signal(rule)) = rules.get(id) else {
            panic!("{id} is not a signal rule");
        };
        assert_eq!(rule.severity, severity, "{id}");
        assert_eq!(rule.tactics, tactics, "{id}");
    }
}

#[test]
fn execution_and_credential_rules_are_not_in_the_default_ruleset() {
    let default = ruleset(DEFAULT_RULESET_ID);

    for id in RULES {
        assert!(
            default.rules.iter().all(|entry| entry.id != id),
            "{id} is in the default ruleset"
        );
    }
}

#[test]
fn attack_ruleset_lists_rules_in_id_order() {
    let attack = ruleset(ATTACK_RULESET_ID);

    let ids: Vec<&str> = attack.rules.iter().map(|entry| entry.id.as_str()).collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted);
}
