use ashiato_core::{AnalysisWindow, Severity, Tactic, WindowError};
use chrono::{DateTime, TimeDelta, Utc};

fn at(s: &str) -> DateTime<Utc> {
    s.parse().unwrap()
}

#[test]
fn severity_is_written_in_lowercase() {
    for (text, severity) in [
        ("low", Severity::Low),
        ("medium", Severity::Medium),
        ("high", Severity::High),
        ("critical", Severity::Critical),
    ] {
        let json = format!("\"{text}\"");
        assert_eq!(serde_json::from_str::<Severity>(&json).unwrap(), severity);
        assert_eq!(serde_json::to_string(&severity).unwrap(), json);
    }
}

#[test]
fn unknown_severity_is_rejected() {
    assert!(serde_json::from_str::<Severity>("\"High\"").is_err());
    assert!(serde_json::from_str::<Severity>("\"info\"").is_err());
}

#[test]
fn tactic_is_written_as_mitre_attack_id() {
    for (id, tactic) in [
        ("TA0043", Tactic::Reconnaissance),
        ("TA0042", Tactic::ResourceDevelopment),
        ("TA0001", Tactic::InitialAccess),
        ("TA0002", Tactic::Execution),
        ("TA0003", Tactic::Persistence),
        ("TA0004", Tactic::PrivilegeEscalation),
        ("TA0005", Tactic::DefenseEvasion),
        ("TA0006", Tactic::CredentialAccess),
        ("TA0007", Tactic::Discovery),
        ("TA0008", Tactic::LateralMovement),
        ("TA0009", Tactic::Collection),
        ("TA0011", Tactic::CommandAndControl),
        ("TA0010", Tactic::Exfiltration),
        ("TA0040", Tactic::Impact),
    ] {
        let json = format!("\"{id}\"");
        assert_eq!(serde_json::from_str::<Tactic>(&json).unwrap(), tactic);
        assert_eq!(serde_json::to_string(&tactic).unwrap(), json);
    }
}

#[test]
fn unknown_tactic_is_rejected() {
    assert!(serde_json::from_str::<Tactic>("\"TA9999\"").is_err());
    assert!(serde_json::from_str::<Tactic>("\"Impact\"").is_err());
}

#[test]
fn baseline_period_ends_where_analysis_period_starts() {
    let window = AnalysisWindow::new(
        at("2026-09-24T00:00:00Z"),
        at("2026-10-25T00:00:00Z"),
        TimeDelta::days(30),
    )
    .unwrap();

    assert_eq!(window.since(), at("2026-09-24T00:00:00Z"));
    assert_eq!(window.until(), at("2026-10-25T00:00:00Z"));
    assert_eq!(window.baseline_since(), at("2026-08-25T00:00:00Z"));
    assert_eq!(window.baseline_until(), at("2026-09-24T00:00:00Z"));
}

#[test]
fn analysis_period_must_not_be_empty() {
    let since = at("2026-10-25T00:00:00Z");
    for until in [since, at("2026-10-24T00:00:00Z")] {
        assert_eq!(
            AnalysisWindow::new(since, until, TimeDelta::days(30)),
            Err(WindowError::Empty { since, until })
        );
    }
}

#[test]
fn zero_lookback_is_allowed() {
    let window = AnalysisWindow::new(
        at("2026-09-24T00:00:00Z"),
        at("2026-10-25T00:00:00Z"),
        TimeDelta::zero(),
    )
    .unwrap();

    assert_eq!(window.baseline_since(), window.baseline_until());
}

#[test]
fn negative_lookback_is_rejected() {
    assert_eq!(
        AnalysisWindow::new(
            at("2026-09-24T00:00:00Z"),
            at("2026-10-25T00:00:00Z"),
            TimeDelta::days(-1),
        ),
        Err(WindowError::NegativeLookback(TimeDelta::days(-1)))
    );
}

#[test]
fn severity_parses_from_its_lowercase_name() {
    assert_eq!("high".parse::<Severity>().unwrap(), Severity::High);
    let error = "urgent".parse::<Severity>().unwrap_err();
    assert!(error.to_string().contains("urgent"), "{error}");
}

#[test]
fn severities_are_ordered_from_low_to_critical() {
    assert!(Severity::Low < Severity::Medium);
    assert!(Severity::Medium < Severity::High);
    assert!(Severity::High < Severity::Critical);
}
