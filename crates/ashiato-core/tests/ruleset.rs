use ashiato_core::rule::{RuleFile, RuleSource, Rules};
use ashiato_core::ruleset::{
    DEFAULT_RULESET_ID, FrameworkKind, RuleRefs, Ruleset, RulesetError, Rulesets,
};

fn file(path: &str, content: &str) -> RuleFile {
    RuleFile {
        path: path.to_owned(),
        content: content.to_owned(),
    }
}

fn signal(id: &str) -> String {
    format!(
        "kind: signal\nid: {id}\ntitle: {id}\nseverity: low\ntactics: [TA0007]\nsql: SELECT 1\n"
    )
}

fn scenario(id: &str, signals: &[&str]) -> String {
    let steps: String = signals
        .iter()
        .map(|signal| format!("  - signal: {signal}\n"))
        .collect();
    format!(
        "kind: scenario\nid: {id}\ntitle: {id}\nseverity: high\ntactics: [TA0001]\nwindow: 1h\nsteps:\n{steps}"
    )
}

fn rules() -> Rules {
    let files = vec![
        file("a/rule.yaml", &signal("a")),
        file("b/rule.yaml", &signal("b")),
        file("c/rule.yaml", &signal("c")),
        file("ab/rule.yaml", &scenario("ab", &["a", "b"])),
    ];
    Rules::new([RuleSource::parse("test", files).unwrap()]).unwrap()
}

const ATTACK: &str = r#"
id: attack
title: ATT&CK
framework:
  kind: mitre_attack
  version: "20.0"
  url: https://attack.mitre.org/versions/v20/
rules:
  - id: a
    refs: [T1078.004]
  - id: b
    refs: [T1580, T1526]
  - id: ab
    refs: [T1078]
"#;

const DEFAULT: &str = r#"
id: default
title: 標準のルール
rules:
  - id: a
  - id: c
"#;

fn rulesets(manifests: &[&str]) -> Result<Rulesets, RulesetError> {
    Rulesets::parse(
        manifests
            .iter()
            .enumerate()
            .map(|(index, content)| file(&format!("{index}.yaml"), content))
            .collect(),
    )
}

fn ids(selected: &Rules) -> Vec<String> {
    selected.iter().map(|rule| rule.id().to_owned()).collect()
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

#[test]
fn ruleset_manifest_is_parsed() {
    let ruleset = Ruleset::parse(&file("attack.yaml", ATTACK)).unwrap();

    assert_eq!(ruleset.id, "attack");
    assert_eq!(ruleset.title, "ATT&CK");
    let framework = ruleset.framework.as_ref().unwrap();
    assert_eq!(framework.kind, FrameworkKind::MitreAttack);
    assert_eq!(framework.version, "20.0");
    assert_eq!(
        framework.url.as_deref(),
        Some("https://attack.mitre.org/versions/v20/")
    );
    assert_eq!(ruleset.rules.len(), 3);
    assert_eq!(ruleset.rules[1].id, "b");
    assert_eq!(ruleset.rules[1].refs, strings(&["T1580", "T1526"]));
    assert!(ruleset.digest().starts_with("sha256:"));
}

#[test]
fn ruleset_without_framework_has_entries_without_refs() {
    let ruleset = Ruleset::parse(&file("default.yaml", DEFAULT)).unwrap();

    assert!(ruleset.framework.is_none());
    assert!(ruleset.rules.iter().all(|entry| entry.refs.is_empty()));
}

#[test]
fn unknown_keys_and_frameworks_are_rejected() {
    let unknown_key = DEFAULT.replace("title:", "name: x\ntitle:");
    let unknown_framework = ATTACK.replace("mitre_attack", "cis");

    for content in [unknown_key, unknown_framework] {
        assert!(matches!(
            Ruleset::parse(&file("x.yaml", &content)),
            Err(RulesetError::Parse { .. })
        ));
    }
}

#[test]
fn duplicate_rule_in_a_ruleset_is_rejected() {
    let content = format!("{DEFAULT}  - id: a\n");

    let error = Ruleset::parse(&file("default.yaml", &content)).unwrap_err();

    assert!(
        matches!(&error, RulesetError::DuplicateRule { ruleset, rule } if ruleset == "default" && rule == "a"),
        "{error:?}"
    );
}

#[test]
fn refs_without_framework_are_rejected() {
    let content = DEFAULT.replace("  - id: c", "  - id: c\n    refs: [T1580]");

    let error = Ruleset::parse(&file("default.yaml", &content)).unwrap_err();

    assert!(
        matches!(&error, RulesetError::RefsWithoutFramework { ruleset, rule } if ruleset == "default" && rule == "c"),
        "{error:?}"
    );
}

#[test]
fn attack_refs_must_be_technique_ids() {
    for invalid in [
        "TA0001",
        "T158",
        "T15800",
        "T1580.1",
        "T1580.0001",
        "t1580",
        "T1580.",
    ] {
        let content = ATTACK.replace("[T1580, T1526]", &format!("[{invalid}]"));

        let error = Ruleset::parse(&file("attack.yaml", &content)).unwrap_err();

        assert!(
            matches!(&error, RulesetError::InvalidRef { rule, reference, .. } if rule == "b" && reference == invalid),
            "{invalid}: {error:?}"
        );
    }
}

#[test]
fn digest_changes_with_manifest_content() {
    let original = Ruleset::parse(&file("default.yaml", DEFAULT)).unwrap();
    let edited = Ruleset::parse(&file(
        "default.yaml",
        &DEFAULT.replace("- id: c", "- id: b"),
    ))
    .unwrap();

    assert_ne!(original.digest(), edited.digest());
}

#[test]
fn duplicate_ruleset_ids_are_rejected() {
    let error = rulesets(&[DEFAULT, DEFAULT]).unwrap_err();

    assert!(
        matches!(&error, RulesetError::DuplicateRuleset { ruleset } if ruleset == "default"),
        "{error:?}"
    );
}

#[test]
fn extending_replaces_rulesets_with_the_same_id() {
    let mut sets = rulesets(&[DEFAULT, ATTACK]).unwrap();
    let replacement = DEFAULT.replace("  - id: c\n", "");

    sets.extend(rulesets(&[&replacement]).unwrap());

    let default = sets.get(DEFAULT_RULESET_ID).unwrap();
    assert_eq!(default.rules.len(), 1);
    assert!(sets.get("attack").is_some());
}

#[test]
fn check_rejects_rules_that_do_not_exist() {
    let content = DEFAULT.replace("- id: c", "- id: missing");
    let sets = rulesets(&[&content]).unwrap();

    let error = sets.check(&rules()).unwrap_err();

    assert!(
        matches!(&error, RulesetError::UnknownRule { ruleset, rule } if ruleset == "default" && rule == "missing"),
        "{error:?}"
    );
}

#[test]
fn check_rejects_scenarios_without_their_signals() {
    let content = DEFAULT.replace("- id: c", "- id: ab");
    let sets = rulesets(&[&content]).unwrap();

    let error = sets.check(&rules()).unwrap_err();

    assert!(
        matches!(&error, RulesetError::MissingScenarioSignal { ruleset, scenario, signal } if ruleset == "default" && scenario == "ab" && signal == "b"),
        "{error:?}"
    );
}

#[test]
fn check_accepts_consistent_rulesets() {
    rulesets(&[DEFAULT, ATTACK])
        .unwrap()
        .check(&rules())
        .unwrap();
}

#[test]
fn unlisted_returns_rules_in_no_ruleset() {
    let sets = rulesets(&[DEFAULT]).unwrap();
    let rules = rules();

    assert_eq!(sets.unlisted(&rules), vec!["ab", "b"]);
}

#[test]
fn rules_added_to_default_become_selected_by_default() {
    let mut sets = rulesets(&[DEFAULT]).unwrap();
    sets.add_to_default(strings(&["b"]));

    let selection = sets.select(&rules(), &[], &[]).unwrap();

    assert_eq!(ids(&selection.rules), strings(&["a", "b", "c"]));
}

#[test]
fn default_ruleset_is_selected_when_nothing_is_given() {
    let sets = rulesets(&[DEFAULT, ATTACK]).unwrap();

    let selection = sets.select(&rules(), &[], &[]).unwrap();

    assert_eq!(ids(&selection.rules), strings(&["a", "c"]));
    assert_eq!(selection.rulesets.len(), 1);
    assert_eq!(selection.rulesets[0].id, "default");
    assert_eq!(
        selection.rulesets[0].digest,
        sets.get("default").unwrap().digest()
    );
    assert!(selection.refs.is_empty());
}

#[test]
fn selecting_rulesets_runs_the_union_of_their_rules() {
    let sets = rulesets(&[DEFAULT, ATTACK]).unwrap();

    let selection = sets
        .select(&rules(), &strings(&["attack", "default"]), &[])
        .unwrap();

    assert_eq!(ids(&selection.rules), strings(&["a", "ab", "b", "c"]));
    let selected: Vec<&str> = selection
        .rulesets
        .iter()
        .map(|set| set.id.as_str())
        .collect();
    assert_eq!(selected, ["attack", "default"]);
}

#[test]
fn selection_keeps_the_rule_sources() {
    let sets = rulesets(&[ATTACK]).unwrap();
    let rules = rules();

    let selection = sets.select(&rules, &strings(&["attack"]), &[]).unwrap();

    assert_eq!(selection.rules.rule_sources(), rules.rule_sources());
}

#[test]
fn selection_reports_refs_of_the_selected_rulesets() {
    let sets = rulesets(&[DEFAULT, ATTACK]).unwrap();

    let selection = sets
        .select(&rules(), &strings(&["default", "attack"]), &[])
        .unwrap();

    assert_eq!(
        selection.refs.get("b"),
        Some(&vec![RuleRefs {
            ruleset: "attack".to_owned(),
            ids: strings(&["T1580", "T1526"]),
        }])
    );
    assert_eq!(selection.refs.get("c"), None);
}

#[test]
fn selecting_an_unknown_ruleset_is_an_error() {
    let sets = rulesets(&[DEFAULT]).unwrap();

    let error = sets
        .select(&rules(), &strings(&["missing"]), &[])
        .unwrap_err();

    assert!(
        matches!(&error, RulesetError::UnknownRuleset { ruleset } if ruleset == "missing"),
        "{error:?}"
    );
}

#[test]
fn selecting_rules_narrows_the_selected_rulesets() {
    let sets = rulesets(&[DEFAULT, ATTACK]).unwrap();

    let selection = sets
        .select(&rules(), &strings(&["attack"]), &strings(&["b"]))
        .unwrap();

    assert_eq!(ids(&selection.rules), strings(&["b"]));
    assert_eq!(selection.refs.len(), 1);
}

#[test]
fn selecting_a_scenario_also_runs_its_signals() {
    let sets = rulesets(&[DEFAULT, ATTACK]).unwrap();

    let selection = sets
        .select(&rules(), &strings(&["attack"]), &strings(&["ab"]))
        .unwrap();

    assert_eq!(ids(&selection.rules), strings(&["a", "ab", "b"]));
}

#[test]
fn selecting_rules_without_rulesets_selects_from_default() {
    let sets = rulesets(&[DEFAULT, ATTACK]).unwrap();

    let selection = sets.select(&rules(), &[], &strings(&["c"])).unwrap();

    assert_eq!(ids(&selection.rules), strings(&["c"]));
    assert_eq!(selection.rulesets[0].id, "default");
}

#[test]
fn selecting_a_rule_outside_the_rulesets_is_an_error() {
    let sets = rulesets(&[DEFAULT, ATTACK]).unwrap();

    let error = sets
        .select(&rules(), &strings(&["attack"]), &strings(&["c"]))
        .unwrap_err();

    assert!(
        matches!(&error, RulesetError::RuleNotSelected { rule, rulesets } if rule == "c" && rulesets == &strings(&["attack"])),
        "{error:?}"
    );
}

#[test]
fn builtin_rulesets_are_consistent_with_builtin_rules() {
    let rules = Rules::new([RuleSource::builtin().unwrap()]).unwrap();
    let sets = Rulesets::builtin().unwrap();

    sets.check(&rules).unwrap();
    assert!(sets.get(DEFAULT_RULESET_ID).is_some());
    assert_eq!(sets.unlisted(&rules), Vec::<&str>::new());
}

#[test]
fn default_ruleset_has_no_framework() {
    let sets = Rulesets::builtin().unwrap();

    let default = sets.get(DEFAULT_RULESET_ID).unwrap();
    assert!(default.framework.is_none());
}
