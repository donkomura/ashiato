use std::collections::BTreeMap;

use ashiato_core::Severity;
use ashiato_core::analysis::Analysis;
use ashiato_core::lineage::{Origin, OriginKind};
use ashiato_core::output::{
    RecordKind, ScenarioMatchRecord, SignalEvidence, SignalRecord, StepRecord,
};

fn origin(name: &str) -> Origin {
    Origin {
        id: format!("iam_user:arn:aws:iam::111111111111:user/{name}"),
        kind: OriginKind::IamUser,
    }
}

fn evidence(start: &str, event_ids: &[&str], source_ip: &str) -> SignalEvidence {
    SignalEvidence {
        start_time: start.parse().unwrap(),
        end_time: start.parse().unwrap(),
        event_ids: event_ids.iter().map(|id| (*id).to_owned()).collect(),
        access_key_id: None,
        source_ip: source_ip.to_owned(),
        attrs: BTreeMap::new(),
    }
}

fn signal(id: &str, severity: Severity, owner: &str, evidence: SignalEvidence) -> SignalRecord {
    SignalRecord {
        signal_id: id.to_owned(),
        severity,
        tactics: Vec::new(),
        origin: origin(owner),
        principal_key: String::new(),
        account_id: "111111111111".to_owned(),
        region: "us-east-1".to_owned(),
        evidence,
    }
}

fn scenario_match(
    id: &str,
    severity: Severity,
    owner: &str,
    steps: Vec<SignalEvidence>,
) -> ScenarioMatchRecord {
    ScenarioMatchRecord {
        scenario_id: id.to_owned(),
        severity,
        origin: origin(owner),
        start_time: steps.iter().map(|step| step.start_time).min().unwrap(),
        end_time: steps.iter().map(|step| step.end_time).max().unwrap(),
        steps: steps
            .into_iter()
            .map(|step| StepRecord {
                signal_id: "step".to_owned(),
                signals: vec![step],
            })
            .collect(),
        lineage: Vec::new(),
    }
}

fn analysis() -> Analysis {
    Analysis {
        signals: vec![
            signal(
                "probe",
                Severity::Medium,
                "alice",
                evidence("2026-10-01T10:00:00Z", &["e1", "e2"], "203.0.113.1"),
            ),
            signal(
                "probe",
                Severity::Medium,
                "alice",
                evidence("2026-10-01T12:00:00Z", &["e3"], "203.0.113.2"),
            ),
            signal(
                "probe",
                Severity::Medium,
                "alice",
                evidence("2026-10-01T11:00:00Z", &["e4"], "203.0.113.1"),
            ),
            signal(
                "probe",
                Severity::Medium,
                "bob",
                evidence("2026-10-01T09:00:00Z", &["e5"], "203.0.113.9"),
            ),
            signal(
                "root_usage",
                Severity::Low,
                "alice",
                evidence("2026-10-01T08:00:00Z", &["e6"], "198.51.100.1"),
            ),
        ],
        matches: vec![
            scenario_match(
                "hijack",
                Severity::High,
                "alice",
                vec![
                    evidence("2026-10-01T10:00:00Z", &["e1"], "203.0.113.1"),
                    evidence("2026-10-01T10:30:00Z", &["e7", "e8"], "203.0.113.1"),
                ],
            ),
            scenario_match(
                "hijack",
                Severity::High,
                "alice",
                vec![
                    evidence("2026-10-01T11:00:00Z", &["e4"], "203.0.113.1"),
                    evidence("2026-10-01T10:30:00Z", &["e7", "e8"], "203.0.113.1"),
                ],
            ),
        ],
        warnings: Vec::new(),
    }
}

#[test]
fn summary_aggregates_records_by_kind_rule_and_origin() {
    let summaries = analysis().summarize();

    let keys: Vec<(RecordKind, &str, &str)> = summaries
        .iter()
        .map(|summary| {
            (
                summary.kind,
                summary.rule_id.as_str(),
                summary.origin.id.as_str(),
            )
        })
        .collect();
    assert_eq!(
        keys,
        [
            (
                RecordKind::ScenarioMatch,
                "hijack",
                "iam_user:arn:aws:iam::111111111111:user/alice"
            ),
            (
                RecordKind::Signal,
                "probe",
                "iam_user:arn:aws:iam::111111111111:user/alice"
            ),
            (
                RecordKind::Signal,
                "probe",
                "iam_user:arn:aws:iam::111111111111:user/bob"
            ),
            (
                RecordKind::Signal,
                "root_usage",
                "iam_user:arn:aws:iam::111111111111:user/alice"
            ),
        ]
    );
}

#[test]
fn summary_counts_records_events_and_source_ips_over_the_time_span() {
    let summaries = analysis().summarize();

    let probe = &summaries[1];
    assert_eq!(probe.severity, Severity::Medium);
    assert_eq!(probe.count, 3);
    assert_eq!(probe.events, 4);
    assert_eq!(probe.source_ips, 2);
    assert_eq!(
        probe.first_seen,
        "2026-10-01T10:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap()
    );
    assert_eq!(
        probe.last_seen,
        "2026-10-01T12:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap()
    );

    let hijack = &summaries[0];
    assert_eq!(hijack.count, 2);
    assert_eq!(hijack.events, 4);
    assert_eq!(hijack.source_ips, 1);
}

#[test]
fn summary_orders_by_severity_then_by_count() {
    let mut analysis = analysis();
    analysis.signals.push(signal(
        "burst",
        Severity::Medium,
        "carol",
        evidence("2026-10-01T10:00:00Z", &["e9"], "203.0.113.3"),
    ));

    let order: Vec<(String, usize)> = analysis
        .summarize()
        .into_iter()
        .map(|summary| (summary.rule_id, summary.count))
        .collect();
    assert_eq!(
        order,
        [
            ("hijack".to_owned(), 2),
            ("probe".to_owned(), 3),
            ("burst".to_owned(), 1),
            ("probe".to_owned(), 1),
            ("root_usage".to_owned(), 1),
        ]
    );
}

#[test]
fn retain_severities_keeps_only_the_listed_severities() {
    let mut analysis = analysis();

    analysis.retain_severities(&[Severity::Low, Severity::High]);

    assert!(
        analysis
            .signals
            .iter()
            .all(|signal| signal.signal_id == "root_usage")
    );
    assert_eq!(analysis.signals.len(), 1);
    assert_eq!(analysis.matches.len(), 2);
}
