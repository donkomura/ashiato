use std::collections::{BTreeMap, HashMap};

use ashiato_core::analysis::{AnalysisError, analyze, correlate};
use ashiato_core::lineage::{Caller, LineageEdge, LineageStep, LineageWarning, Origin, OriginKind};
use ashiato_core::macros::Dialect;
use ashiato_core::output::{RuleSkippedRecord, SignalEvidence, SkipReason, StepRecord};
use ashiato_core::query::{BackendError, DataAvailability, QueryBackend};
use ashiato_core::rule::{DataRequirement, RuleFile, RuleSource, Rules};
use ashiato_core::sequence::ObservedSignal;
use ashiato_core::{AnalysisWindow, Severity, SignalRow, Tactic};
use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};

const ACCOUNT: &str = "111111111111";
const ALICE_ARN: &str = "arn:aws:iam::111111111111:user/alice";
const BOB_ARN: &str = "arn:aws:iam::111111111111:user/bob";
const ADMIN_ROLE_ARN: &str = "arn:aws:iam::111111111111:role/admin";
const ALICE_KEY: &str = "AKIAALICE";
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

const BROKEN_RULE: &str = r#"
kind: signal
id: broken
title: 実行に失敗するルール
severity: low
tactics: [TA0007]
sql: SELECT 'broken'
"#;

const S3_RULE: &str = r#"
kind: signal
id: s3_read
title: S3 のオブジェクトの読み取り
severity: medium
tactics: [TA0010]
requires: [s3_data_events]
sql: SELECT 's3_read'
"#;

const S3_SCENARIO: &str = r#"
kind: scenario
id: login_then_s3_read
title: ログインの後の S3 のオブジェクトの読み取り
severity: high
tactics: [TA0001, TA0010]
window: 1h
steps:
  - signal: login
  - signal: s3_read
    optional: true
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

#[derive(Default)]
struct FakeBackend {
    signals: HashMap<String, Vec<SignalRow>>,
    edges: Vec<LineageEdge>,
    lineage_fails: bool,
    data: DataAvailability,
    data_availability_fails: bool,
}

impl FakeBackend {
    fn with_signals(mut self, sql: &str, rows: Vec<SignalRow>) -> Self {
        self.signals.insert(sql.to_owned(), rows);
        self
    }

    fn with_edge(mut self, edge: LineageEdge) -> Self {
        self.edges.push(edge);
        self
    }
}

#[async_trait]
impl QueryBackend for FakeBackend {
    fn dialect(&self) -> Dialect {
        Dialect::DuckDb
    }

    async fn query_signals(&self, sql: &str) -> Result<Vec<SignalRow>, BackendError> {
        self.signals
            .iter()
            .find(|(rule_sql, _)| sql.trim_end().ends_with(rule_sql.as_str()))
            .map(|(_, rows)| rows.clone())
            .ok_or_else(|| BackendError::MissingColumn {
                column: "start_time".to_owned(),
            })
    }

    async fn data_availability(
        &self,
        _window: &AnalysisWindow,
    ) -> Result<DataAvailability, BackendError> {
        if self.data_availability_fails {
            return Err(BackendError::MissingColumn {
                column: "events".to_owned(),
            });
        }
        Ok(self.data)
    }

    async fn query_lineage(
        &self,
        _window: &AnalysisWindow,
    ) -> Result<Vec<LineageEdge>, BackendError> {
        if self.lineage_fails {
            return Err(BackendError::MissingColumn {
                column: "child_access_key_id".to_owned(),
            });
        }
        Ok(self.edges.clone())
    }
}

fn rules(files: &[(&str, &str)]) -> Rules {
    let files = files
        .iter()
        .map(|(id, content)| RuleFile {
            path: format!("{id}/rule.yaml"),
            content: (*content).to_owned(),
        })
        .collect();
    Rules::new([RuleSource::parse("test", files).unwrap()]).unwrap()
}

fn scenario_rules() -> Rules {
    rules(&[
        ("login", LOGIN_RULE),
        ("escalate", ESCALATE_RULE),
        ("login_then_escalate", SCENARIO),
    ])
}

fn window() -> AnalysisWindow {
    AnalysisWindow::new(
        "2026-10-01T00:00:00Z".parse().unwrap(),
        "2026-10-02T00:00:00Z".parse().unwrap(),
        TimeDelta::days(30),
    )
    .unwrap()
}

fn at(time: &str) -> DateTime<Utc> {
    format!("2026-10-01T{time}:00Z").parse().unwrap()
}

fn row(time: &str, access_key_id: &str, principal_key: &str, event_id: &str) -> SignalRow {
    SignalRow {
        start_time: at(time),
        end_time: at(time),
        event_ids: vec![event_id.to_owned()],
        access_key_id: Some(access_key_id.to_owned()),
        principal_key: principal_key.to_owned(),
        source_ip: "198.51.100.7".to_owned(),
        account_id: ACCOUNT.to_owned(),
        region: "us-east-1".to_owned(),
        attrs: BTreeMap::new(),
    }
}

fn evidence(row: &SignalRow) -> SignalEvidence {
    SignalEvidence {
        start_time: row.start_time,
        end_time: row.end_time,
        event_ids: row.event_ids.clone(),
        access_key_id: row.access_key_id.clone(),
        source_ip: row.source_ip.clone(),
        attrs: row.attrs.clone(),
    }
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

fn iam_user(arn: &str) -> Origin {
    Origin {
        id: format!("iam_user:{arn}"),
        kind: OriginKind::IamUser,
    }
}

#[tokio::test]
async fn signals_of_a_role_chain_are_matched_under_their_origin() {
    let login = row("10:00", ALICE_KEY, ALICE_ARN, "evt-login");
    let escalate = row("10:10", CHILD_KEY, ADMIN_ROLE_ARN, "evt-escalate");
    let backend = FakeBackend::default()
        .with_signals("SELECT 'login'", vec![login.clone()])
        .with_signals("SELECT 'escalate'", vec![escalate.clone()])
        .with_edge(alice_edge());

    let analysis = analyze(&backend, &scenario_rules(), &window())
        .await
        .unwrap();

    assert_eq!(analysis.matches.len(), 1, "{:#?}", analysis.matches);
    let found = &analysis.matches[0];
    assert_eq!(found.scenario_id, "login_then_escalate");
    assert_eq!(found.severity, Severity::Critical);
    assert_eq!(found.origin, iam_user(ALICE_ARN));
    assert_eq!(found.start_time, at("10:00"));
    assert_eq!(found.end_time, at("10:10"));
    assert_eq!(
        found.steps,
        vec![
            StepRecord {
                signal_id: "login".to_owned(),
                signals: vec![evidence(&login)],
            },
            StepRecord {
                signal_id: "escalate".to_owned(),
                signals: vec![evidence(&escalate)],
            },
        ]
    );
    assert_eq!(
        found.lineage,
        vec![LineageStep {
            access_key_id: CHILD_KEY.to_owned(),
            role_arn: Some(ADMIN_ROLE_ARN.to_owned()),
            issued_by_event_id: "evt-assume".to_owned(),
        }]
    );
    assert!(analysis.warnings.is_empty());
}

#[tokio::test]
async fn signals_of_different_origins_are_not_matched_together() {
    let backend = FakeBackend::default()
        .with_signals(
            "SELECT 'login'",
            vec![row("10:00", ALICE_KEY, ALICE_ARN, "evt-login")],
        )
        .with_signals(
            "SELECT 'escalate'",
            vec![row("10:10", "AKIABOB", BOB_ARN, "evt-escalate")],
        );

    let analysis = analyze(&backend, &scenario_rules(), &window())
        .await
        .unwrap();

    assert!(analysis.matches.is_empty(), "{:#?}", analysis.matches);
}

#[tokio::test]
async fn signal_records_carry_the_rule_and_the_origin_in_origin_and_time_order() {
    let backend = FakeBackend::default()
        .with_signals(
            "SELECT 'login'",
            vec![
                row("10:30", ALICE_KEY, ALICE_ARN, "evt-alice-login"),
                row("10:00", "AKIABOB", BOB_ARN, "evt-bob-login"),
            ],
        )
        .with_signals(
            "SELECT 'escalate'",
            vec![row("10:10", CHILD_KEY, ADMIN_ROLE_ARN, "evt-escalate")],
        )
        .with_edge(alice_edge());

    let analysis = analyze(&backend, &scenario_rules(), &window())
        .await
        .unwrap();

    let signals: Vec<(&str, &str, &str)> = analysis
        .signals
        .iter()
        .map(|signal| {
            (
                signal.origin.id.as_str(),
                signal.signal_id.as_str(),
                signal.evidence.event_ids[0].as_str(),
            )
        })
        .collect();
    let alice = format!("iam_user:{ALICE_ARN}");
    let bob = format!("iam_user:{BOB_ARN}");
    assert_eq!(
        signals,
        vec![
            (alice.as_str(), "escalate", "evt-escalate"),
            (alice.as_str(), "login", "evt-alice-login"),
            (bob.as_str(), "login", "evt-bob-login"),
        ]
    );
    let escalate = &analysis.signals[0];
    assert_eq!(escalate.severity, Severity::High);
    assert_eq!(escalate.tactics, vec![Tactic::PrivilegeEscalation]);
    assert_eq!(escalate.principal_key, ADMIN_ROLE_ARN);
    assert_eq!(escalate.account_id, ACCOUNT);
    assert_eq!(escalate.region, "us-east-1");
}

#[tokio::test]
async fn a_failing_signal_rule_is_reported_with_its_id() {
    let rules = rules(&[("login", LOGIN_RULE), ("broken", BROKEN_RULE)]);
    let backend = FakeBackend::default().with_signals("SELECT 'login'", Vec::new());

    let error = analyze(&backend, &rules, &window()).await.unwrap_err();

    match error {
        AnalysisError::Signal(error) => assert_eq!(error.rule_id(), "broken"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn a_failing_lineage_query_is_an_error() {
    let backend = FakeBackend {
        lineage_fails: true,
        ..FakeBackend::default()
    }
    .with_signals("SELECT 'login'", Vec::new())
    .with_signals("SELECT 'escalate'", Vec::new());

    let error = analyze(&backend, &scenario_rules(), &window())
        .await
        .unwrap_err();

    assert!(
        matches!(error, AnalysisError::Lineage(_)),
        "unexpected error: {error:?}"
    );
}

fn s3_rules() -> Rules {
    rules(&[
        ("login", LOGIN_RULE),
        ("s3_read", S3_RULE),
        ("login_then_s3_read", S3_SCENARIO),
    ])
}

fn skipped(rule_id: &str) -> RuleSkippedRecord {
    RuleSkippedRecord {
        rule_id: rule_id.to_owned(),
        reason: SkipReason::MissingData,
        requires: vec![DataRequirement::S3DataEvents],
    }
}

#[tokio::test]
async fn rules_requiring_missing_data_and_their_scenarios_are_skipped() {
    let backend = FakeBackend {
        data: DataAvailability {
            events: 10,
            s3_data_events: false,
        },
        ..FakeBackend::default()
    }
    .with_signals(
        "SELECT 'login'",
        vec![row("10:00", ALICE_KEY, ALICE_ARN, "evt-login")],
    );

    let mut analysis = analyze(&backend, &s3_rules(), &window()).await.unwrap();

    analysis.skipped.sort_by(|a, b| a.rule_id.cmp(&b.rule_id));
    assert_eq!(
        analysis.skipped,
        vec![skipped("login_then_s3_read"), skipped("s3_read")]
    );
    assert!(analysis.matches.is_empty(), "{:#?}", analysis.matches);
    let signal_ids: Vec<&str> = analysis
        .signals
        .iter()
        .map(|signal| signal.signal_id.as_str())
        .collect();
    assert_eq!(signal_ids, ["login"]);
}

#[tokio::test]
async fn rules_requiring_available_data_are_run() {
    let data = DataAvailability {
        events: 10,
        s3_data_events: true,
    };
    let backend = FakeBackend {
        data,
        ..FakeBackend::default()
    }
    .with_signals(
        "SELECT 'login'",
        vec![row("10:00", ALICE_KEY, ALICE_ARN, "evt-login")],
    )
    .with_signals(
        "SELECT 's3_read'",
        vec![row("10:10", ALICE_KEY, ALICE_ARN, "evt-read")],
    );

    let analysis = analyze(&backend, &s3_rules(), &window()).await.unwrap();

    assert!(analysis.skipped.is_empty(), "{:#?}", analysis.skipped);
    assert_eq!(analysis.data, data);
    assert_eq!(analysis.matches.len(), 1, "{:#?}", analysis.matches);
    assert_eq!(analysis.matches[0].scenario_id, "login_then_s3_read");
}

#[tokio::test]
async fn the_analysis_reports_the_data_availability_of_the_backend() {
    let data = DataAvailability {
        events: 42,
        s3_data_events: false,
    };
    let backend = FakeBackend {
        data,
        ..FakeBackend::default()
    }
    .with_signals("SELECT 'login'", Vec::new())
    .with_signals("SELECT 'escalate'", Vec::new());

    let analysis = analyze(&backend, &scenario_rules(), &window())
        .await
        .unwrap();

    assert_eq!(analysis.data, data);
    assert!(analysis.skipped.is_empty(), "{:#?}", analysis.skipped);
}

#[tokio::test]
async fn a_failing_data_availability_query_is_an_error() {
    let backend = FakeBackend {
        data_availability_fails: true,
        ..FakeBackend::default()
    }
    .with_signals("SELECT 'login'", Vec::new())
    .with_signals("SELECT 'escalate'", Vec::new());

    let error = analyze(&backend, &scenario_rules(), &window())
        .await
        .unwrap_err();

    assert!(
        matches!(error, AnalysisError::DataAvailability(_)),
        "unexpected error: {error:?}"
    );
}

const GRANDCHILD_KEY: &str = "ASIAGRANDCHILD";
const AUDIT_ROLE_ARN: &str = "arn:aws:iam::111111111111:role/audit";

fn observed(signal_id: &str, row: SignalRow) -> ObservedSignal {
    ObservedSignal {
        signal_id: signal_id.to_owned(),
        row,
    }
}

fn session_edge(child: &str, parent_key: &str, role_arn: &str, event_id: &str) -> LineageEdge {
    LineageEdge {
        child_access_key_id: child.to_owned(),
        parent: Caller {
            access_key_id: Some(parent_key.to_owned()),
            principal_key: ADMIN_ROLE_ARN.to_owned(),
            principal_arn: Some(ADMIN_ROLE_ARN.to_owned()),
            identity_type: "AssumedRole".to_owned(),
            account_id: Some(ACCOUNT.to_owned()),
            invoked_by: None,
        },
        role_arn: Some(role_arn.to_owned()),
        issued_by_event_id: event_id.to_owned(),
        cross_account: false,
        federation: None,
    }
}

#[test]
fn correlate_groups_signals_of_issued_sessions_under_their_origin_and_matches_the_scenario() {
    let login = row("10:00", ALICE_KEY, ALICE_ARN, "evt-login");
    let escalate = row("10:10", GRANDCHILD_KEY, AUDIT_ROLE_ARN, "evt-escalate");

    let analysis = correlate(
        &scenario_rules(),
        vec![
            observed("login", login.clone()),
            observed("escalate", escalate.clone()),
        ],
        vec![
            alice_edge(),
            session_edge(
                GRANDCHILD_KEY,
                CHILD_KEY,
                AUDIT_ROLE_ARN,
                "evt-assume-audit",
            ),
        ],
    );

    assert_eq!(analysis.matches.len(), 1, "{:#?}", analysis.matches);
    let found = &analysis.matches[0];
    assert_eq!(found.scenario_id, "login_then_escalate");
    assert_eq!(found.origin, iam_user(ALICE_ARN));
    assert_eq!(
        found.steps,
        vec![
            StepRecord {
                signal_id: "login".to_owned(),
                signals: vec![evidence(&login)],
            },
            StepRecord {
                signal_id: "escalate".to_owned(),
                signals: vec![evidence(&escalate)],
            },
        ]
    );
    assert_eq!(
        found.lineage,
        vec![
            LineageStep {
                access_key_id: CHILD_KEY.to_owned(),
                role_arn: Some(ADMIN_ROLE_ARN.to_owned()),
                issued_by_event_id: "evt-assume".to_owned(),
            },
            LineageStep {
                access_key_id: GRANDCHILD_KEY.to_owned(),
                role_arn: Some(AUDIT_ROLE_ARN.to_owned()),
                issued_by_event_id: "evt-assume-audit".to_owned(),
            },
        ]
    );
    let origins: Vec<&Origin> = analysis
        .signals
        .iter()
        .map(|signal| &signal.origin)
        .collect();
    assert_eq!(origins, vec![&iam_user(ALICE_ARN), &iam_user(ALICE_ARN)]);
    assert!(analysis.warnings.is_empty());
}

#[test]
fn correlate_without_the_edge_keeps_the_signals_of_the_session_apart() {
    let analysis = correlate(
        &scenario_rules(),
        vec![
            observed("login", row("10:00", ALICE_KEY, ALICE_ARN, "evt-login")),
            observed(
                "escalate",
                row("10:10", CHILD_KEY, ADMIN_ROLE_ARN, "evt-escalate"),
            ),
        ],
        Vec::new(),
    );

    assert!(analysis.matches.is_empty(), "{:#?}", analysis.matches);
    let origins: Vec<(&str, OriginKind)> = analysis
        .signals
        .iter()
        .map(|signal| (signal.origin.id.as_str(), signal.origin.kind))
        .collect();
    let alice = format!("iam_user:{ALICE_ARN}");
    let role = format!("role:{ADMIN_ROLE_ARN}");
    assert_eq!(
        origins,
        vec![
            (alice.as_str(), OriginKind::IamUser),
            (role.as_str(), OriginKind::UnresolvedRole),
        ]
    );
}

#[test]
fn correlate_uses_one_of_the_duplicated_edges_of_a_session() {
    let analysis = correlate(
        &scenario_rules(),
        vec![
            observed("login", row("10:00", ALICE_KEY, ALICE_ARN, "evt-login")),
            observed(
                "escalate",
                row("10:10", CHILD_KEY, ADMIN_ROLE_ARN, "evt-escalate"),
            ),
        ],
        vec![alice_edge(), alice_edge()],
    );

    assert_eq!(analysis.matches.len(), 1, "{:#?}", analysis.matches);
    assert_eq!(analysis.matches[0].lineage.len(), 1);
}

#[test]
fn correlate_reports_each_lineage_cycle_once() {
    let analysis = correlate(
        &scenario_rules(),
        vec![
            observed("login", row("10:00", CHILD_KEY, ADMIN_ROLE_ARN, "evt-1")),
            observed("login", row("10:05", CHILD_KEY, ADMIN_ROLE_ARN, "evt-2")),
        ],
        vec![
            session_edge(CHILD_KEY, GRANDCHILD_KEY, ADMIN_ROLE_ARN, "evt-assume-1"),
            session_edge(GRANDCHILD_KEY, CHILD_KEY, AUDIT_ROLE_ARN, "evt-assume-2"),
        ],
    );

    assert_eq!(
        analysis.warnings,
        vec![LineageWarning::Cycle {
            access_key_id: CHILD_KEY.to_owned(),
        }]
    );
    assert_eq!(analysis.signals.len(), 2);
}

#[test]
fn correlate_does_not_depend_on_the_order_of_signals_and_edges_with_distinct_times() {
    let signals = vec![
        observed("login", row("10:00", ALICE_KEY, ALICE_ARN, "evt-login")),
        observed(
            "escalate",
            row("10:10", GRANDCHILD_KEY, AUDIT_ROLE_ARN, "evt-escalate"),
        ),
        observed("login", row("10:20", "AKIABOB", BOB_ARN, "evt-bob-login")),
        observed(
            "escalate",
            row("10:30", CHILD_KEY, ADMIN_ROLE_ARN, "evt-escalate-2"),
        ),
    ];
    let edges = vec![
        alice_edge(),
        session_edge(
            GRANDCHILD_KEY,
            CHILD_KEY,
            AUDIT_ROLE_ARN,
            "evt-assume-audit",
        ),
    ];

    let forward = correlate(&scenario_rules(), signals.clone(), edges.clone());
    let backward = correlate(
        &scenario_rules(),
        signals.into_iter().rev().collect(),
        edges.into_iter().rev().collect(),
    );

    assert_eq!(forward, backward);
    assert_eq!(forward.signals.len(), 4);
    assert_eq!(forward.matches.len(), 1, "{:#?}", forward.matches);
}

#[test]
fn correlate_without_signals_returns_an_empty_analysis() {
    let analysis = correlate(&scenario_rules(), Vec::new(), vec![alice_edge()]);

    assert!(analysis.signals.is_empty());
    assert!(analysis.matches.is_empty());
    assert!(analysis.warnings.is_empty());
}
