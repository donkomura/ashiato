use std::collections::BTreeMap;

use ashiato_core::analysis::correlate;
use ashiato_core::context::Context;
use ashiato_core::lineage::{Origin, OriginKind};
use ashiato_core::output::{OriginScoreRecord, SignalEvidence, SignalRecord};
use ashiato_core::rule::{RuleFile, RuleSource, Rules};
use ashiato_core::scoring::{ScoringSettings, score};
use ashiato_core::sequence::ObservedSignal;
use ashiato_core::{Severity, SignalRow, Tactic};

const ALICE_ARN: &str = "arn:aws:iam::111111111111:user/alice";
const BOB_ARN: &str = "arn:aws:iam::111111111111:user/bob";

fn origin(name: &str) -> Origin {
    Origin {
        id: format!("iam_user:arn:aws:iam::111111111111:user/{name}"),
        kind: OriginKind::IamUser,
    }
}

fn signal(id: &str, severity: Severity, tactics: &[Tactic], owner: &str) -> SignalRecord {
    SignalRecord {
        signal_id: id.to_owned(),
        severity,
        tactics: tactics.to_vec(),
        origin: origin(owner),
        principal_key: String::new(),
        account_id: "111111111111".to_owned(),
        region: "us-east-1".to_owned(),
        evidence: SignalEvidence {
            start_time: "2026-10-01T10:00:00Z".parse().unwrap(),
            end_time: "2026-10-01T10:00:00Z".parse().unwrap(),
            event_ids: vec![format!("evt-{id}")],
            access_key_id: None,
            source_ip: "198.51.100.7".to_owned(),
            attrs: BTreeMap::new(),
        },
        refs: Vec::new(),
    }
}

fn settings(min_tactics: usize, threshold: u32) -> ScoringSettings {
    ScoringSettings {
        min_tactics,
        threshold,
    }
}

fn only(scores: Vec<OriginScoreRecord>) -> OriginScoreRecord {
    assert_eq!(scores.len(), 1, "{scores:#?}");
    scores.into_iter().next().unwrap()
}

#[test]
fn default_settings_need_three_tactics_and_ten_points() {
    assert_eq!(ScoringSettings::default(), settings(3, 10));
}

#[test]
fn each_severity_scores_its_points() {
    for (severity, points) in [
        (Severity::Low, 1),
        (Severity::Medium, 3),
        (Severity::High, 5),
        (Severity::Critical, 8),
    ] {
        let found = only(score(
            &[signal("probe", severity, &[Tactic::Discovery], "alice")],
            ScoringSettings::default(),
        ));

        assert_eq!(found.score, points, "{severity:?}");
    }
}

#[test]
fn a_repeated_signal_scores_its_points_once() {
    let found = only(score(
        &[
            signal("probe", Severity::Medium, &[Tactic::Discovery], "alice"),
            signal("probe", Severity::Medium, &[Tactic::Discovery], "alice"),
            signal("escalate", Severity::High, &[Tactic::Persistence], "alice"),
            signal("probe", Severity::Medium, &[Tactic::Discovery], "alice"),
        ],
        ScoringSettings::default(),
    ));

    assert_eq!(found.score, 8);
    assert_eq!(found.signal_ids, ["escalate", "probe"]);
}

#[test]
fn tactics_are_the_sorted_union_of_the_observed_signals() {
    let found = only(score(
        &[
            signal(
                "zeta",
                Severity::Low,
                &[Tactic::PrivilegeEscalation, Tactic::InitialAccess],
                "alice",
            ),
            signal(
                "alpha",
                Severity::Low,
                &[Tactic::Persistence, Tactic::InitialAccess],
                "alice",
            ),
        ],
        ScoringSettings::default(),
    ));

    assert_eq!(
        found.tactics,
        [
            Tactic::InitialAccess,
            Tactic::Persistence,
            Tactic::PrivilegeEscalation,
        ]
    );
    assert_eq!(found.signal_ids, ["alpha", "zeta"]);
}

#[test]
fn above_threshold_needs_both_enough_tactics_and_enough_points() {
    let cases: [(&str, Vec<SignalRecord>, bool); 4] = [
        (
            "exactly both",
            vec![
                signal("a", Severity::Medium, &[Tactic::InitialAccess], "alice"),
                signal("b", Severity::Medium, &[Tactic::Persistence], "alice"),
            ],
            true,
        ),
        (
            "more than both",
            vec![
                signal("a", Severity::High, &[Tactic::InitialAccess], "alice"),
                signal(
                    "b",
                    Severity::Medium,
                    &[Tactic::Persistence, Tactic::Impact],
                    "alice",
                ),
            ],
            true,
        ),
        (
            "enough points with too few tactics",
            vec![
                signal("a", Severity::High, &[Tactic::InitialAccess], "alice"),
                signal("b", Severity::High, &[Tactic::InitialAccess], "alice"),
            ],
            false,
        ),
        (
            "enough tactics with too few points",
            vec![signal(
                "a",
                Severity::High,
                &[Tactic::InitialAccess, Tactic::Persistence],
                "alice",
            )],
            false,
        ),
    ];

    for (name, signals, expected) in cases {
        let found = only(score(&signals, settings(2, 6)));

        assert_eq!(found.above_threshold, expected, "{name}: {found:#?}");
    }
}

#[test]
fn default_settings_are_met_at_exactly_three_tactics_and_ten_points() {
    let found = only(score(
        &[
            signal(
                "a",
                Severity::High,
                &[Tactic::InitialAccess, Tactic::Persistence],
                "alice",
            ),
            signal("b", Severity::High, &[Tactic::Impact], "alice"),
        ],
        ScoringSettings::default(),
    ));

    assert_eq!(found.score, 10);
    assert!(found.above_threshold, "{found:#?}");
}

#[test]
fn origins_are_ordered_by_score_and_then_by_origin_id() {
    let signals = vec![
        signal("probe", Severity::Medium, &[Tactic::Discovery], "alice"),
        signal("escalate", Severity::High, &[Tactic::Persistence], "carol"),
        signal("probe", Severity::Low, &[Tactic::Discovery], "dave"),
        signal("escalate", Severity::High, &[Tactic::Persistence], "bob"),
    ];
    let mut reversed = signals.clone();
    reversed.reverse();

    let scores = score(&signals, ScoringSettings::default());

    let order: Vec<(&str, u32)> = scores
        .iter()
        .map(|found| (found.origin.id.as_str(), found.score))
        .collect();
    assert_eq!(
        order,
        [
            ("iam_user:arn:aws:iam::111111111111:user/bob", 5),
            ("iam_user:arn:aws:iam::111111111111:user/carol", 5),
            ("iam_user:arn:aws:iam::111111111111:user/alice", 3),
            ("iam_user:arn:aws:iam::111111111111:user/dave", 1),
        ]
    );
    assert_eq!(score(&reversed, ScoringSettings::default()), scores);
}

#[test]
fn no_signals_give_no_origin_scores() {
    assert!(score(&[], ScoringSettings::default()).is_empty());
}

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

fn scenario_rules() -> Rules {
    let files = [
        ("login", LOGIN_RULE),
        ("escalate", ESCALATE_RULE),
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

fn observed(signal_id: &str, time: &str, principal_key: &str) -> ObservedSignal {
    ObservedSignal {
        signal_id: signal_id.to_owned(),
        row: SignalRow {
            start_time: format!("2026-10-01T{time}:00Z").parse().unwrap(),
            end_time: format!("2026-10-01T{time}:00Z").parse().unwrap(),
            event_ids: vec![format!("evt-{signal_id}-{time}")],
            access_key_id: None,
            principal_key: principal_key.to_owned(),
            source_ip: "198.51.100.7".to_owned(),
            account_id: "111111111111".to_owned(),
            region: "us-east-1".to_owned(),
            attrs: BTreeMap::new(),
        },
    }
}

#[test]
fn origins_with_a_scenario_match_are_scored_from_their_signals_alone() {
    let analysis = correlate(
        &scenario_rules(),
        vec![
            observed("login", "10:00", ALICE_ARN),
            observed("escalate", "10:10", ALICE_ARN),
            observed("login", "10:20", BOB_ARN),
        ],
        Vec::new(),
        &Context::default(),
    );
    assert_eq!(analysis.matches.len(), 1, "{:#?}", analysis.matches);

    let scores = score(&analysis.signals, ScoringSettings::default());

    assert_eq!(
        scores,
        [
            OriginScoreRecord {
                origin: origin("alice"),
                score: 8,
                tactics: vec![Tactic::InitialAccess, Tactic::PrivilegeEscalation],
                signal_ids: vec!["escalate".to_owned(), "login".to_owned()],
                above_threshold: false,
            },
            OriginScoreRecord {
                origin: origin("bob"),
                score: 3,
                tactics: vec![Tactic::InitialAccess],
                signal_ids: vec!["login".to_owned()],
                above_threshold: false,
            },
        ]
    );
}
