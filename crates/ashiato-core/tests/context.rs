use std::collections::BTreeMap;

use ashiato_core::context::{Context, ContextError, Suppression};
use ashiato_core::query::batch_signal_sql;
use ashiato_core::rule::{Rule, RuleFile, RuleSource, Rules, SignalRule};
use ashiato_core::{AnalysisWindow, Duration, macros::Dialect};
use chrono::TimeDelta;
use serde_json::json;
use sha2::{Digest, Sha256};

const FULL_CONTEXT: &str = r#"
organization:
  internal_cidrs: [10.0.0.0/8, 203.0.113.0/24]
  trusted_accounts: ["111111111111", "222222222222"]

suppress:
  - origin: "workload:*"
    signals: [role_chain_hop]
  - principal_key: "arn:aws:iam::111111111111:role/deploy-pipeline"
  - signal: compute_resource_launch
    attrs: { stack_name: "prod-*" }

sensitive:
  s3_buckets: [customer-data-prod]

baseline:
  lookback: 30d

scoring:
  min_tactics: 3
  threshold: 10

params:
  s3_delete_burst.min_deletes: 5
"#;

const DELETE_BURST_RULE: &str = r#"
kind: signal
id: delete_burst
title: 削除の集中
severity: high
tactics: [TA0040]
params:
  min_deletes: 3
  bin: 5m
sql: |
  SELECT * FROM events
  WHERE {{ in_analysis_window(event_time) }}
  GROUP BY {{ time_bin(event_time, param('bin')) }}
  HAVING count(*) >= {{ param('min_deletes') }}
"#;

const LOGIN_RULE: &str = r#"
kind: signal
id: login
title: ログイン
severity: low
tactics: [TA0001]
sql: SELECT 'login'
"#;

const SCENARIO: &str = r#"
kind: scenario
id: repeated_login
title: 繰り返されるログイン
severity: medium
tactics: [TA0001]
window: 1h
steps:
  - signal: login
  - signal: login
"#;

fn parse(text: &str) -> Context {
    Context::parse(text).unwrap()
}

fn duration(text: &str) -> Duration {
    text.parse().unwrap()
}

fn rules() -> Rules {
    let files = [
        ("delete_burst", DELETE_BURST_RULE),
        ("login", LOGIN_RULE),
        ("repeated_login", SCENARIO),
    ]
    .iter()
    .map(|(id, content)| RuleFile {
        path: format!("{id}/rule.yaml"),
        content: (*content).to_owned(),
    })
    .collect();
    Rules::new([RuleSource::parse("test", files).unwrap()]).unwrap()
}

fn signal_rule<'a>(rules: &'a Rules, id: &str) -> &'a SignalRule {
    match rules.get(id) {
        Some(Rule::Signal(rule)) => rule,
        other => panic!("expected signal rule {id}, got {other:?}"),
    }
}

fn window() -> AnalysisWindow {
    AnalysisWindow::new(
        "2026-10-01T00:00:00Z".parse().unwrap(),
        "2026-10-02T00:00:00Z".parse().unwrap(),
        TimeDelta::days(30),
    )
    .unwrap()
}

#[test]
fn full_context_is_read_into_typed_settings() {
    let context = parse(FULL_CONTEXT);

    let cidrs: Vec<String> = context
        .organization
        .internal_cidrs
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(cidrs, ["10.0.0.0/8", "203.0.113.0/24"]);
    assert_eq!(
        context.organization.trusted_accounts,
        ["111111111111", "222222222222"]
    );
    assert_eq!(
        context.suppress,
        vec![
            Suppression {
                origin: Some("workload:*".to_owned()),
                signals: Some(vec!["role_chain_hop".to_owned()]),
                ..Suppression::default()
            },
            Suppression {
                principal_key: Some("arn:aws:iam::111111111111:role/deploy-pipeline".to_owned()),
                ..Suppression::default()
            },
            Suppression {
                signal: Some("compute_resource_launch".to_owned()),
                attrs: BTreeMap::from([("stack_name".to_owned(), "prod-*".to_owned())]),
                ..Suppression::default()
            },
        ]
    );
    assert_eq!(context.sensitive.s3_buckets, ["customer-data-prod"]);
    assert_eq!(context.baseline.lookback, duration("30d"));
    assert_eq!(context.scoring.min_tactics, 3);
    assert_eq!(context.scoring.threshold, 10);
    assert_eq!(
        context.params,
        BTreeMap::from([("s3_delete_burst.min_deletes".to_owned(), json!(5))])
    );
}

#[test]
fn context_without_a_file_has_the_defaults_of_design_section_8() {
    let context = Context::default();

    assert!(context.organization.internal_cidrs.is_empty());
    assert!(context.organization.trusted_accounts.is_empty());
    assert!(context.suppress.is_empty());
    assert!(context.sensitive.s3_buckets.is_empty());
    assert_eq!(context.baseline.lookback, duration("30d"));
    assert_eq!(context.scoring.min_tactics, 3);
    assert_eq!(context.scoring.threshold, 10);
    assert!(context.params.is_empty());
    assert_eq!(context.digest(), None);
}

#[test]
fn an_empty_file_gives_the_defaults() {
    for text in ["", "# no settings yet\n", "{}\n"] {
        let context = parse(text);

        assert_eq!(context.baseline, Context::default().baseline, "{text:?}");
        assert_eq!(context.scoring, Context::default().scoring, "{text:?}");
        assert!(context.suppress.is_empty(), "{text:?}");
        assert!(context.digest().is_some(), "{text:?}");
    }
}

#[test]
fn omitted_settings_take_their_defaults() {
    let context = parse("baseline:\n  lookback: 7d\nscoring:\n  threshold: 20\n");

    assert_eq!(context.baseline.lookback, duration("7d"));
    assert_eq!(context.scoring.threshold, 20);
    assert_eq!(context.scoring.min_tactics, 3);
    assert!(context.organization.internal_cidrs.is_empty());
}

#[test]
fn unknown_keys_are_rejected() {
    for text in [
        "unknown: 1\n",
        "organization:\n  internal_cidr: [10.0.0.0/8]\n",
        "suppress:\n  - origin: \"workload:*\"\n    rule: role_chain_hop\n",
        "sensitive:\n  buckets: [customer-data-prod]\n",
        "baseline:\n  window: 30d\n",
        "scoring:\n  min_score: 10\n",
    ] {
        let error = Context::parse(text).unwrap_err();
        assert!(matches!(error, ContextError::Parse(_)), "{text}: {error:?}");
    }
}

#[test]
fn invalid_values_are_rejected() {
    for text in [
        "organization:\n  internal_cidrs: [10.0.0.0/33]\n",
        "organization:\n  internal_cidrs: [intranet]\n",
        "baseline:\n  lookback: 30 days\n",
        "scoring:\n  min_tactics: -1\n",
        "suppress: workload\n",
    ] {
        let error = Context::parse(text).unwrap_err();
        assert!(matches!(error, ContextError::Parse(_)), "{text}: {error:?}");
    }
}

#[test]
fn a_suppress_entry_without_conditions_is_rejected() {
    let text = "suppress:\n  - principal_key: arn:aws:iam::111111111111:user/alice\n  - {}\n";

    let error = Context::parse(text).unwrap_err();

    assert!(
        matches!(error, ContextError::EmptySuppression { index: 1 }),
        "{error:?}"
    );
}

#[test]
fn a_param_without_the_rule_id_is_rejected() {
    let error = Context::parse("params:\n  min_deletes: 5\n").unwrap_err();

    assert!(
        matches!(&error, ContextError::MalformedParamKey { key } if key == "min_deletes"),
        "{error:?}"
    );
}

#[test]
fn digest_is_the_sha256_of_the_content() {
    let hex: String = Sha256::digest(FULL_CONTEXT.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();

    assert_eq!(
        parse(FULL_CONTEXT).digest(),
        Some(format!("sha256:{hex}").as_str())
    );
}

#[test]
fn digest_changes_with_the_content() {
    let changed = FULL_CONTEXT.replace("threshold: 10", "threshold: 12");

    assert_ne!(parse(&changed).digest(), parse(FULL_CONTEXT).digest());
    assert_eq!(parse(FULL_CONTEXT).digest(), parse(FULL_CONTEXT).digest());
}

#[test]
fn params_override_the_declared_params_of_signal_rules() {
    let rules = rules();
    let context = parse("params:\n  delete_burst.min_deletes: 5\n");

    let overridden = context.apply_params(&rules).unwrap();

    let rule = signal_rule(&overridden, "delete_burst");
    assert_eq!(rule.params["min_deletes"], json!(5));
    assert_eq!(rule.params["bin"], json!("5m"));
    assert_eq!(
        signal_rule(&rules, "delete_burst").params["min_deletes"],
        json!(3)
    );
    assert_eq!(overridden.rule_sources(), rules.rule_sources());
}

#[test]
fn overridden_params_reach_the_expanded_sql() {
    let context = parse("params:\n  delete_burst.min_deletes: 5\n  delete_burst.bin: 10m\n");
    let overridden = context.apply_params(&rules()).unwrap();
    let window = window();

    let sql = batch_signal_sql(
        signal_rule(&overridden, "delete_burst"),
        Dialect::DuckDb,
        &window,
        window.since(),
        window.since() + TimeDelta::hours(1),
    )
    .unwrap();

    assert!(sql.contains("count(*) >= 5"), "{sql}");
    assert!(sql.contains("INTERVAL '10 minutes'"), "{sql}");
}

#[test]
fn a_context_without_params_leaves_the_rules_as_they_are() {
    let rules = rules();

    let overridden = Context::default().apply_params(&rules).unwrap();

    assert_eq!(
        signal_rule(&overridden, "delete_burst"),
        signal_rule(&rules, "delete_burst")
    );
    assert_eq!(overridden.iter().count(), rules.iter().count());
}

#[test]
fn a_param_of_an_unknown_rule_is_rejected() {
    for (key, rule_id) in [
        ("delete_bursts.min_deletes", "delete_bursts"),
        ("repeated_login.window", "repeated_login"),
    ] {
        let context = parse(&format!("params:\n  {key}: 5\n"));

        let error = context.apply_params(&rules()).unwrap_err();

        assert!(
            matches!(&error, ContextError::UnknownRule { key: k, rule_id: r } if k == key && r == rule_id),
            "{key}: {error:?}"
        );
    }
}

#[test]
fn a_param_the_rule_does_not_declare_is_rejected() {
    for key in ["delete_burst.min_delete", "login.min_deletes"] {
        let context = parse(&format!("params:\n  {key}: 5\n"));

        let error = context.apply_params(&rules()).unwrap_err();

        assert!(
            matches!(&error, ContextError::UndeclaredParam { key: k, .. } if k == key),
            "{key}: {error:?}"
        );
    }
}
