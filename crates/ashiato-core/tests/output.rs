use std::collections::BTreeMap;

use ashiato_core::lineage::{LineageStep, Origin, OriginKind};
use ashiato_core::output::{
    JsonLinesSink, OutputRecord, RuleSkippedRecord, RunRecord, ScenarioMatchRecord, SignalEvidence,
    SignalRecord, Sink, SkipReason, StepRecord,
};
use ashiato_core::query::DataAvailability;
use ashiato_core::rule::{DataRequirement, RuleSourceDigest};
use ashiato_core::ruleset::RulesetDigest;
use ashiato_core::{Severity, Tactic};
use serde_json::{Value, json};

const ALICE_ARN: &str = "arn:aws:iam::111111111111:user/alice";

fn alice() -> Origin {
    Origin {
        id: format!("iam_user:{ALICE_ARN}"),
        kind: OriginKind::IamUser,
    }
}

fn evidence() -> SignalEvidence {
    SignalEvidence {
        start_time: "2026-10-01T10:00:00Z".parse().unwrap(),
        end_time: "2026-10-01T10:05:00Z".parse().unwrap(),
        event_ids: vec!["evt-1".to_owned(), "evt-2".to_owned()],
        access_key_id: Some("ASIACHILD".to_owned()),
        source_ip: "198.51.100.7".to_owned(),
        attrs: BTreeMap::from([("bucket".to_owned(), "customer-data".to_owned())]),
    }
}

fn run_record() -> OutputRecord {
    OutputRecord::Run(RunRecord {
        tool_version: "0.1.0".to_owned(),
        since: "2026-09-24T00:00:00Z".parse().unwrap(),
        until: "2026-10-25T00:00:00Z".parse().unwrap(),
        rule_sources: vec![RuleSourceDigest {
            id: "builtin".to_owned(),
            digest: "sha256:abc".to_owned(),
        }],
        rulesets: vec![RulesetDigest {
            id: "default".to_owned(),
            digest: "sha256:def".to_owned(),
        }],
        data: DataAvailability {
            events: 12345678,
            s3_data_events: true,
        },
    })
}

fn write_lines(records: &[OutputRecord]) -> String {
    let mut sink = JsonLinesSink::new(Vec::new());
    for record in records {
        sink.write(record).unwrap();
    }
    String::from_utf8(sink.into_inner()).unwrap()
}

fn written(record: OutputRecord) -> Value {
    let output = write_lines(&[record]);
    serde_json::from_str(output.trim_end()).unwrap()
}

#[test]
fn run_record_has_the_shape_of_design_section_11() {
    assert_eq!(
        written(run_record()),
        json!({
            "type": "run",
            "tool_version": "0.1.0",
            "since": "2026-09-24T00:00:00Z",
            "until": "2026-10-25T00:00:00Z",
            "rule_sources": [{ "id": "builtin", "digest": "sha256:abc" }],
            "rulesets": [{ "id": "default", "digest": "sha256:def" }],
            "data": { "events": 12345678, "s3_data_events": true },
        })
    );
}

#[test]
fn rule_skipped_record_has_the_shape_of_design_section_11() {
    assert_eq!(
        written(OutputRecord::RuleSkipped(RuleSkippedRecord {
            rule_id: "cross_account_s3_destruction".to_owned(),
            reason: SkipReason::MissingData,
            requires: vec![DataRequirement::S3DataEvents],
        })),
        json!({
            "type": "rule_skipped",
            "rule_id": "cross_account_s3_destruction",
            "reason": "missing_data",
            "requires": ["s3_data_events"],
        })
    );
}

#[test]
fn signal_record_carries_the_origin_and_the_signal_columns() {
    let record = OutputRecord::Signal(SignalRecord {
        signal_id: "s3_delete_burst".to_owned(),
        severity: Severity::High,
        tactics: vec![Tactic::Impact],
        origin: alice(),
        principal_key: "arn:aws:iam::111111111111:role/admin".to_owned(),
        account_id: "111111111111".to_owned(),
        region: "us-east-1".to_owned(),
        evidence: evidence(),
        refs: Vec::new(),
    });

    assert_eq!(
        written(record),
        json!({
            "type": "signal",
            "signal_id": "s3_delete_burst",
            "severity": "high",
            "tactics": ["TA0040"],
            "origin": { "id": format!("iam_user:{ALICE_ARN}"), "kind": "iam_user" },
            "principal_key": "arn:aws:iam::111111111111:role/admin",
            "account_id": "111111111111",
            "region": "us-east-1",
            "start_time": "2026-10-01T10:00:00Z",
            "end_time": "2026-10-01T10:05:00Z",
            "event_ids": ["evt-1", "evt-2"],
            "access_key_id": "ASIACHILD",
            "source_ip": "198.51.100.7",
            "attrs": { "bucket": "customer-data" },
        })
    );
}

#[test]
fn scenario_match_record_has_the_shape_of_design_section_11() {
    let login = SignalEvidence {
        access_key_id: None,
        attrs: BTreeMap::new(),
        ..evidence()
    };
    let record = OutputRecord::ScenarioMatch(ScenarioMatchRecord {
        scenario_id: "console_takeover_mining".to_owned(),
        severity: Severity::Critical,
        origin: alice(),
        start_time: "2026-10-01T10:00:00Z".parse().unwrap(),
        end_time: "2026-10-01T10:05:00Z".parse().unwrap(),
        steps: vec![
            StepRecord {
                signal_id: "console_login_without_mfa".to_owned(),
                signals: vec![login],
            },
            StepRecord {
                signal_id: "cloudshell_activity".to_owned(),
                signals: Vec::new(),
            },
        ],
        lineage: vec![LineageStep {
            access_key_id: "ASIACHILD".to_owned(),
            role_arn: Some("arn:aws:iam::111111111111:role/admin".to_owned()),
            issued_by_event_id: "evt-0".to_owned(),
        }],
        refs: Vec::new(),
    });

    assert_eq!(
        written(record),
        json!({
            "type": "scenario_match",
            "scenario_id": "console_takeover_mining",
            "severity": "critical",
            "origin": { "id": format!("iam_user:{ALICE_ARN}"), "kind": "iam_user" },
            "start_time": "2026-10-01T10:00:00Z",
            "end_time": "2026-10-01T10:05:00Z",
            "steps": [
                {
                    "signal_id": "console_login_without_mfa",
                    "signals": [{
                        "start_time": "2026-10-01T10:00:00Z",
                        "end_time": "2026-10-01T10:05:00Z",
                        "event_ids": ["evt-1", "evt-2"],
                        "access_key_id": null,
                        "source_ip": "198.51.100.7",
                        "attrs": {},
                    }],
                },
                { "signal_id": "cloudshell_activity", "signals": [] },
            ],
            "lineage": [{
                "access_key_id": "ASIACHILD",
                "role_arn": "arn:aws:iam::111111111111:role/admin",
                "issued_by_event_id": "evt-0",
            }],
        })
    );
}

#[test]
fn json_lines_sink_writes_one_line_per_record() {
    let output = write_lines(&[run_record(), run_record()]);

    let lines: Vec<&str> = output.split_terminator('\n').collect();
    assert_eq!(lines.len(), 2, "{output:?}");
    assert!(output.ends_with('\n'), "{output:?}");
    for line in lines {
        let value: Value = serde_json::from_str(line).unwrap();
        assert_eq!(value["type"], "run");
    }
}
