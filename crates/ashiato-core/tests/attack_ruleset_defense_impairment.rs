use std::collections::BTreeMap;

use ashiato_core::ruleset::{DEFAULT_RULESET_ID, Rulesets};

const ATTACK_RULESET_ID: &str = "attack-v20";

fn defense_impairment_refs() -> BTreeMap<&'static str, Vec<&'static str>> {
    BTreeMap::from([
        ("cloud_log_scope_reduced", vec!["T1685.002"]),
        ("security_alerting_disabled", vec!["T1685"]),
        ("organization_left", vec!["T1666"]),
        ("cloud_firewall_opened", vec!["T1686.001"]),
        (
            "identity_provider_trust_modified",
            vec!["T1484.002", "T1556.007"],
        ),
        ("service_quota_increase", vec!["T1578.005"]),
        ("quota_raise_then_mining", vec!["T1578.005", "T1496.001"]),
    ])
}

#[test]
fn attack_ruleset_maps_the_defense_impairment_rules_to_techniques() {
    let expected = defense_impairment_refs();
    let sets = Rulesets::builtin().unwrap();
    let ruleset = sets.get(ATTACK_RULESET_ID).unwrap();

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
fn defense_impairment_rules_are_not_in_the_default_ruleset() {
    let sets = Rulesets::builtin().unwrap();
    let default = sets.get(DEFAULT_RULESET_ID).unwrap();

    for id in defense_impairment_refs().keys() {
        assert!(
            default.rules.iter().all(|entry| entry.id != *id),
            "{id} is in the default ruleset"
        );
    }
}
