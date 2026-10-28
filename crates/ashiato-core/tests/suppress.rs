use std::collections::BTreeMap;

use ashiato_core::analysis::{Analysis, analyze, correlate, correlate_available};
use ashiato_core::context::Context;
use ashiato_core::lineage::{Caller, LineageEdge};
use ashiato_core::macros::Dialect;
use ashiato_core::query::{BackendError, DataAvailability, QueryBackend};
use ashiato_core::rule::{RuleFile, RuleSource, Rules};
use ashiato_core::sequence::ObservedSignal;
use ashiato_core::{AnalysisWindow, SignalRow};
use async_trait::async_trait;
use chrono::TimeDelta;

const ACCOUNT: &str = "111111111111";
const ALICE_ARN: &str = "arn:aws:iam::111111111111:user/alice";
const BOB_ARN: &str = "arn:aws:iam::111111111111:user/bob";
const ADMIN_ROLE_ARN: &str = "arn:aws:iam::111111111111:role/admin";
const EC2_SERVICE: &str = "service:ec2.amazonaws.com";
const ALICE_KEY: &str = "AKIAALICE";
const BOB_KEY: &str = "AKIABOB";
const CHILD_KEY: &str = "ASIACHILD";

const LOGIN_RULE: &str = r#"
kind: signal
id: login
title: ログイン
severity: medium
tactics: [TA0001]
sql: SELECT 'login'
"#;

const ESCALATE_RULE: &str = r#"
kind: signal
id: escalate
title: 権限の変更
severity: high
tactics: [TA0004]
sql: SELECT 'escalate'
"#;

const LAUNCH_RULE: &str = r#"
kind: signal
id: launch
title: 計算資源の起動
severity: medium
tactics: [TA0040]
attrs: [stack_name]
sql: SELECT 'launch'
"#;

const EXTERNAL_LOGIN_RULE: &str = r#"
kind: signal
id: external_login
title: 外部からのログイン
severity: medium
tactics: [TA0001]
external_source_only: true
sql: SELECT 'external_login'
"#;

const SCENARIO: &str = r#"
kind: scenario
id: login_then_escalate
title: ログインの後の権限の変更
severity: critical
tactics: [TA0001, TA0004]
window: 1h
steps:
  - signal: login
  - signal: escalate
"#;

fn rules() -> Rules {
    let files = [
        ("login", LOGIN_RULE),
        ("escalate", ESCALATE_RULE),
        ("launch", LAUNCH_RULE),
        ("external_login", EXTERNAL_LOGIN_RULE),
        ("login_then_escalate", SCENARIO),
    ]
    .iter()
    .map(|(id, content)| RuleFile {
        path: format!("{id}/rule.yaml"),
        content: (*content).to_owned(),
    })
    .collect();
    Rules::new([RuleSource::parse("test", files).unwrap()]).unwrap()
}

fn context(text: &str) -> Context {
    Context::parse(text).unwrap()
}

fn row(minute: u32, access_key_id: Option<&str>, principal_key: &str, event_id: &str) -> SignalRow {
    let time = format!("2026-10-01T10:{minute:02}:00Z").parse().unwrap();
    SignalRow {
        start_time: time,
        end_time: time,
        event_ids: vec![event_id.to_owned()],
        access_key_id: access_key_id.map(str::to_owned),
        principal_key: principal_key.to_owned(),
        source_ip: "198.51.100.7".to_owned(),
        account_id: ACCOUNT.to_owned(),
        region: "us-east-1".to_owned(),
        attrs: BTreeMap::new(),
    }
}

fn observed(signal_id: &str, row: SignalRow) -> ObservedSignal {
    ObservedSignal {
        signal_id: signal_id.to_owned(),
        row,
    }
}

fn alice(signal_id: &str, minute: u32, event_id: &str) -> ObservedSignal {
    observed(signal_id, row(minute, Some(ALICE_KEY), ALICE_ARN, event_id))
}

fn alice_admin(signal_id: &str, minute: u32, event_id: &str) -> ObservedSignal {
    observed(
        signal_id,
        row(minute, Some(CHILD_KEY), ADMIN_ROLE_ARN, event_id),
    )
}

fn bob(signal_id: &str, minute: u32, event_id: &str) -> ObservedSignal {
    observed(signal_id, row(minute, Some(BOB_KEY), BOB_ARN, event_id))
}

fn workload(signal_id: &str, minute: u32, event_id: &str) -> ObservedSignal {
    observed(signal_id, row(minute, None, EC2_SERVICE, event_id))
}

fn with_attr(mut signal: ObservedSignal, name: &str, value: &str) -> ObservedSignal {
    signal.row.attrs.insert(name.to_owned(), value.to_owned());
    signal
}

fn from_ip(mut signal: ObservedSignal, source_ip: &str) -> ObservedSignal {
    signal.row.source_ip = source_ip.to_owned();
    signal
}

fn alice_edge() -> LineageEdge {
    LineageEdge {
        child_access_key_id: CHILD_KEY.to_owned(),
        parent: Caller {
            access_key_id: Some(ALICE_KEY.to_owned()),
            principal_key: ALICE_ARN.to_owned(),
            principal_arn: Some(ALICE_ARN.to_owned()),
            identity_type: "IAMUser".to_owned(),
            account_id: Some(ACCOUNT.to_owned()),
            invoked_by: None,
        },
        role_arn: Some(ADMIN_ROLE_ARN.to_owned()),
        issued_by_event_id: "evt-assume".to_owned(),
        cross_account: false,
        federation: None,
    }
}

fn kept_events(analysis: &Analysis) -> Vec<String> {
    let mut events: Vec<String> = analysis
        .signals
        .iter()
        .flat_map(|signal| signal.evidence.event_ids.clone())
        .collect();
    events.sort();
    events
}

fn correlate_in(context: &Context, observed: Vec<ObservedSignal>) -> Analysis {
    correlate(&rules(), observed, vec![alice_edge()], context)
}

#[test]
fn origin_and_signals_suppress_the_listed_signals_of_matching_origins() {
    let context = context("suppress:\n  - origin: \"iam_user:*\"\n    signals: [login]\n");

    let analysis = correlate_in(
        &context,
        vec![
            alice("login", 0, "evt-1"),
            alice_admin("escalate", 5, "evt-2"),
            workload("login", 10, "evt-3"),
            bob("login", 15, "evt-4"),
        ],
    );

    assert_eq!(kept_events(&analysis), ["evt-2", "evt-3"]);
}

#[test]
fn origin_alone_suppresses_every_signal_of_matching_origins() {
    let context = context("suppress:\n  - origin: \"workload:*\"\n");

    let analysis = correlate_in(
        &context,
        vec![
            workload("login", 0, "evt-1"),
            workload("escalate", 5, "evt-2"),
            alice("login", 10, "evt-3"),
        ],
    );

    assert_eq!(kept_events(&analysis), ["evt-3"]);
}

#[test]
fn principal_key_suppresses_the_signals_of_the_principal_whatever_their_origin() {
    let context = context(&format!(
        "suppress:\n  - principal_key: \"{ADMIN_ROLE_ARN}\"\n"
    ));

    let analysis = correlate_in(
        &context,
        vec![
            alice("login", 0, "evt-1"),
            alice_admin("escalate", 5, "evt-2"),
        ],
    );

    assert_eq!(kept_events(&analysis), ["evt-1"]);
}

#[test]
fn principal_key_is_compared_without_wildcards() {
    let context = context("suppress:\n  - principal_key: \"arn:aws:iam::111111111111:*\"\n");

    let analysis = correlate_in(
        &context,
        vec![
            alice("login", 0, "evt-1"),
            alice_admin("escalate", 5, "evt-2"),
        ],
    );

    assert_eq!(kept_events(&analysis), ["evt-1", "evt-2"]);
}

#[test]
fn signal_and_attrs_suppress_the_signals_whose_attrs_match() {
    let context = context("suppress:\n  - signal: launch\n    attrs: { stack_name: \"prod-*\" }\n");

    let analysis = correlate_in(
        &context,
        vec![
            with_attr(alice("launch", 0, "evt-1"), "stack_name", "prod-web"),
            with_attr(alice("launch", 5, "evt-2"), "stack_name", "dev-web"),
            alice("launch", 10, "evt-3"),
            with_attr(alice("login", 15, "evt-4"), "stack_name", "prod-web"),
        ],
    );

    assert_eq!(kept_events(&analysis), ["evt-2", "evt-3", "evt-4"]);
}

#[test]
fn a_signal_is_suppressed_only_when_every_condition_of_an_entry_holds() {
    let context = context(&format!(
        "suppress:\n  - signal: login\n    principal_key: \"{BOB_ARN}\"\n  - origin: \"workload:*\"\n"
    ));

    let analysis = correlate_in(
        &context,
        vec![
            alice("login", 0, "evt-1"),
            bob("login", 5, "evt-2"),
            bob("escalate", 10, "evt-3"),
            workload("escalate", 15, "evt-4"),
        ],
    );

    assert_eq!(kept_events(&analysis), ["evt-1", "evt-3"]);
}

#[test]
fn an_asterisk_in_origin_and_attrs_matches_any_text() {
    for (pattern, value, suppressed) in [
        ("prod-*", "prod-web", true),
        ("prod-*", "prod-", true),
        ("prod-*", "dev-prod-web", false),
        ("*-web", "prod-web", true),
        ("*-web", "prod-web-2", false),
        ("prod-*-web", "prod-eu-web", true),
        ("prod-*-web", "prod-web", false),
        ("p*d*b", "prod-web", true),
        ("*", "anything", true),
        ("*", "", true),
        ("prod-web", "prod-web", true),
        ("prod-web", "prod-web-2", false),
        ("prod-web", "prod-we", false),
    ] {
        let context = context(&format!(
            "suppress:\n  - attrs: {{ stack_name: \"{pattern}\" }}\n"
        ));

        let analysis = correlate_in(
            &context,
            vec![with_attr(alice("launch", 0, "evt-1"), "stack_name", value)],
        );

        assert_eq!(
            analysis.signals.is_empty(),
            suppressed,
            "{pattern} against {value}"
        );
    }

    for (pattern, suppressed) in [
        ("iam_user:*", true),
        ("iam_user:arn:aws:iam::111111111111:user/*", true),
        ("*:user/alice", true),
        ("iam_user:arn:aws:iam::111111111111:user/alice", true),
        ("iam_user:arn:aws:iam::111111111111:user/al", false),
        ("role:*", false),
    ] {
        let context = context(&format!("suppress:\n  - origin: \"{pattern}\"\n"));

        let analysis = correlate_in(&context, vec![alice_admin("escalate", 0, "evt-1")]);

        assert_eq!(analysis.signals.is_empty(), suppressed, "{pattern}");
    }
}

#[test]
fn suppressed_signals_are_not_used_to_match_scenarios() {
    let signals = || {
        vec![
            alice("login", 0, "evt-1"),
            alice_admin("escalate", 5, "evt-2"),
        ]
    };
    let context = context("suppress:\n  - origin: \"iam_user:*\"\n    signals: [login]\n");

    let without = correlate_in(&Context::default(), signals());
    let with = correlate_in(&context, signals());

    assert_eq!(without.matches.len(), 1, "{:#?}", without.matches);
    assert!(with.matches.is_empty(), "{:#?}", with.matches);
    assert_eq!(kept_events(&with), ["evt-2"]);
}

fn external_logins(ips: &[&str]) -> Vec<ObservedSignal> {
    ips.iter()
        .enumerate()
        .map(|(index, ip)| {
            from_ip(
                alice("external_login", u32::try_from(index).unwrap(), ip),
                ip,
            )
        })
        .collect()
}

#[test]
fn external_source_only_signals_from_the_internal_cidrs_are_dropped() {
    let context = context(
        "organization:\n  internal_cidrs: [10.0.0.0/8, 203.0.113.0/24, \"2001:db8::/32\"]\n",
    );
    let inside = [
        "10.0.0.0",
        "10.255.255.255",
        "203.0.113.0",
        "203.0.113.255",
        "2001:db8::1",
        "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff",
    ];
    let outside = [
        "9.255.255.255",
        "11.0.0.0",
        "203.0.112.255",
        "203.0.114.0",
        "2001:db9::",
        "198.51.100.7",
    ];

    let analysis = correlate_in(
        &context,
        external_logins(&[inside.as_slice(), outside.as_slice()].concat()),
    );

    let mut expected = outside.map(str::to_owned).to_vec();
    expected.sort();
    assert_eq!(kept_events(&analysis), expected);
}

#[test]
fn source_ips_that_are_not_ip_addresses_are_kept() {
    let context = context("organization:\n  internal_cidrs: [0.0.0.0/0, \"::/0\"]\n");
    let sources = ["ec2.amazonaws.com", "AWS Internal", ""];

    let analysis = correlate_in(&context, external_logins(&sources));

    let mut expected = sources.map(str::to_owned).to_vec();
    expected.sort();
    assert_eq!(kept_events(&analysis), expected);
}

#[test]
fn internal_cidrs_apply_only_to_external_source_only_rules() {
    let context = context("organization:\n  internal_cidrs: [10.0.0.0/8]\n");

    let analysis = correlate_in(
        &context,
        vec![
            from_ip(alice("login", 0, "evt-1"), "10.0.0.1"),
            from_ip(alice("external_login", 5, "evt-2"), "10.0.0.1"),
        ],
    );

    assert_eq!(kept_events(&analysis), ["evt-1"]);
}

#[test]
fn the_default_context_keeps_every_signal() {
    let analysis = correlate_in(
        &Context::default(),
        vec![
            from_ip(alice("external_login", 0, "evt-1"), "10.0.0.1"),
            workload("login", 5, "evt-2"),
            with_attr(alice("launch", 10, "evt-3"), "stack_name", "prod-web"),
        ],
    );

    assert_eq!(kept_events(&analysis), ["evt-1", "evt-2", "evt-3"]);
}

#[test]
fn correlate_available_applies_the_context() {
    let context = context("suppress:\n  - origin: \"workload:*\"\n");

    let analysis = correlate_available(
        &rules(),
        vec![workload("login", 0, "evt-1"), alice("login", 5, "evt-2")],
        Vec::new(),
        DataAvailability::default(),
        &context,
    );

    assert_eq!(kept_events(&analysis), ["evt-2"]);
}

struct FakeBackend {
    rows: Vec<SignalRow>,
}

#[async_trait]
impl QueryBackend for FakeBackend {
    fn dialect(&self) -> Dialect {
        Dialect::DuckDb
    }

    async fn query_signals(&self, sql: &str) -> Result<Vec<SignalRow>, BackendError> {
        Ok(if sql.trim_end().ends_with("SELECT 'login'") {
            self.rows.clone()
        } else {
            Vec::new()
        })
    }

    async fn data_availability(
        &self,
        _window: &AnalysisWindow,
    ) -> Result<DataAvailability, BackendError> {
        Ok(DataAvailability::default())
    }

    async fn query_lineage(
        &self,
        _window: &AnalysisWindow,
    ) -> Result<Vec<LineageEdge>, BackendError> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn analyze_applies_the_context() {
    let backend = FakeBackend {
        rows: vec![
            row(0, None, EC2_SERVICE, "evt-1"),
            row(5, Some(ALICE_KEY), ALICE_ARN, "evt-2"),
        ],
    };
    let window = AnalysisWindow::new(
        "2026-10-01T00:00:00Z".parse().unwrap(),
        "2026-10-02T00:00:00Z".parse().unwrap(),
        TimeDelta::days(30),
    )
    .unwrap();
    let context = context("suppress:\n  - origin: \"workload:*\"\n");

    let analysis = analyze(&backend, &rules(), &window, &context)
        .await
        .unwrap();

    assert_eq!(kept_events(&analysis), ["evt-2"]);
}
