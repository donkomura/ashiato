use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use ashiato_core::SignalRow;
use ashiato_core::rule::{Rule, RuleFile, RuleSet, ScenarioRule};
use ashiato_core::sequence::{ObservedSignal, ScenarioMatch, match_scenario};
use chrono::{DateTime, TimeDelta, Utc};

fn scenario(window: &str, steps: &str) -> ScenarioRule {
    let content = format!(
        "kind: scenario\n\
         id: test_scenario\n\
         title: test\n\
         severity: high\n\
         tactics: [TA0001]\n\
         window: {window}\n\
         steps:\n{steps}"
    );
    let set = RuleSet::parse(
        "test",
        vec![RuleFile {
            path: "test_scenario/rule.yaml".to_owned(),
            content,
        }],
    )
    .unwrap();
    match set.rules() {
        [Rule::Scenario(rule)] => rule.clone(),
        other => panic!("expected one scenario rule, got {other:?}"),
    }
}

fn at(minutes: i64) -> DateTime<Utc> {
    "2026-10-01T10:00:00Z".parse::<DateTime<Utc>>().unwrap() + TimeDelta::minutes(minutes)
}

fn signal(label: &str, signal_id: &str, minutes: i64, attrs: &[(&str, &str)]) -> ObservedSignal {
    spanning(label, signal_id, minutes, minutes, attrs)
}

fn spanning(
    label: &str,
    signal_id: &str,
    start_minutes: i64,
    end_minutes: i64,
    attrs: &[(&str, &str)],
) -> ObservedSignal {
    ObservedSignal {
        signal_id: signal_id.to_owned(),
        row: SignalRow {
            start_time: at(start_minutes),
            end_time: at(end_minutes),
            event_ids: vec![label.to_owned()],
            access_key_id: Some("ASIA1".to_owned()),
            principal_key: "arn:aws:iam::111111111111:role/admin".to_owned(),
            source_ip: "198.51.100.1".to_owned(),
            account_id: "111111111111".to_owned(),
            region: "us-east-1".to_owned(),
            attrs: attrs
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect::<BTreeMap<_, _>>(),
        },
    }
}

fn labels(matches: &[ScenarioMatch<'_>]) -> Vec<Vec<Vec<String>>> {
    matches
        .iter()
        .map(|found| {
            found
                .steps
                .iter()
                .map(|step| {
                    step.signals
                        .iter()
                        .map(|signal| signal.row.event_ids[0].clone())
                        .collect()
                })
                .collect()
        })
        .collect()
}

fn found(steps: &[&[&str]]) -> Vec<Vec<String>> {
    steps
        .iter()
        .map(|step| step.iter().map(|label| (*label).to_owned()).collect())
        .collect()
}

const A_THEN_B: &str = "  - signal: a\n  - signal: b\n";

#[test]
fn steps_satisfied_in_order_form_a_match() {
    let rule = scenario("1h", A_THEN_B);
    let signals = [signal("a1", "a", 0, &[]), signal("b1", "b", 10, &[])];

    let matches = match_scenario(&rule, &signals);

    assert_eq!(labels(&matches), [found(&[&["a1"], &["b1"]])]);
    assert_eq!(matches[0].steps[0].signal_id, "a");
    assert_eq!(matches[0].steps[1].signal_id, "b");
    assert_eq!(matches[0].start_time(), at(0));
    assert_eq!(matches[0].end_time(), at(10));
}

#[test]
fn steps_in_the_wrong_order_do_not_match() {
    let rule = scenario("1h", A_THEN_B);
    let signals = [signal("b1", "b", 0, &[]), signal("a1", "a", 10, &[])];

    assert_eq!(match_scenario(&rule, &signals).len(), 0);
}

#[test]
fn signals_are_ordered_by_start_time_regardless_of_input_order() {
    let rule = scenario("1h", A_THEN_B);
    let signals = [signal("b1", "b", 10, &[]), signal("a1", "a", 0, &[])];

    assert_eq!(
        labels(&match_scenario(&rule, &signals)),
        [found(&[&["a1"], &["b1"]])]
    );
}

#[test]
fn a_match_must_end_within_the_window() {
    let rule = scenario("1h", A_THEN_B);
    let inside = [signal("a1", "a", 0, &[]), signal("b1", "b", 60, &[])];
    let outside = [signal("a1", "a", 0, &[]), signal("b1", "b", 61, &[])];
    let ends_outside = [signal("a1", "a", 0, &[]), spanning("b1", "b", 50, 61, &[])];

    assert_eq!(match_scenario(&rule, &inside).len(), 1);
    assert_eq!(match_scenario(&rule, &outside).len(), 0);
    assert_eq!(match_scenario(&rule, &ends_outside).len(), 0);
}

#[test]
fn an_optional_step_is_included_when_present() {
    let rule = scenario(
        "1h",
        "  - signal: a\n  - signal: o\n    optional: true\n  - signal: b\n",
    );
    let signals = [
        signal("a1", "a", 0, &[]),
        signal("o1", "o", 5, &[]),
        signal("b1", "b", 10, &[]),
    ];

    assert_eq!(
        labels(&match_scenario(&rule, &signals)),
        [found(&[&["a1"], &["o1"], &["b1"]])]
    );
}

#[test]
fn an_optional_step_can_be_skipped_when_absent() {
    let rule = scenario(
        "1h",
        "  - signal: a\n  - signal: o\n    optional: true\n  - signal: b\n",
    );
    let signals = [signal("a1", "a", 0, &[]), signal("b1", "b", 10, &[])];

    assert_eq!(
        labels(&match_scenario(&rule, &signals)),
        [found(&[&["a1"], &[], &["b1"]])]
    );
}

#[test]
fn a_missing_required_step_prevents_a_match() {
    let rule = scenario("1h", "  - signal: a\n  - signal: b\n  - signal: c\n");
    let signals = [signal("a1", "a", 0, &[]), signal("c1", "c", 10, &[])];

    assert_eq!(match_scenario(&rule, &signals).len(), 0);
}

#[test]
fn min_count_requires_that_many_signals_in_the_step() {
    let rule = scenario("1h", "  - signal: a\n  - signal: b\n    min_count: 3\n");
    let two = [
        signal("a1", "a", 0, &[]),
        signal("b1", "b", 1, &[]),
        signal("b2", "b", 2, &[]),
    ];
    let three = [
        signal("a1", "a", 0, &[]),
        signal("b1", "b", 1, &[]),
        signal("b2", "b", 2, &[]),
        signal("b3", "b", 3, &[]),
    ];

    assert_eq!(match_scenario(&rule, &two).len(), 0);
    assert_eq!(
        labels(&match_scenario(&rule, &three)),
        [found(&[&["a1"], &["b1", "b2", "b3"]])]
    );
}

#[test]
fn min_count_with_distinct_by_counts_distinct_attribute_values() {
    let rule = scenario(
        "1h",
        "  - signal: a\n  - signal: b\n    min_count: 2\n    distinct_by: role_arn\n",
    );
    let same_role = [
        signal("a1", "a", 0, &[]),
        signal("b1", "b", 1, &[("role_arn", "role/x")]),
        signal("b2", "b", 2, &[("role_arn", "role/x")]),
    ];
    let different_roles = [
        signal("a1", "a", 0, &[]),
        signal("b1", "b", 1, &[("role_arn", "role/x")]),
        signal("b2", "b", 2, &[("role_arn", "role/x")]),
        signal("b3", "b", 3, &[("role_arn", "role/y")]),
    ];

    assert_eq!(match_scenario(&rule, &same_role).len(), 0);
    assert_eq!(
        labels(&match_scenario(&rule, &different_roles)),
        [found(&[&["a1"], &["b1", "b3"]])]
    );
}

#[test]
fn max_gap_limits_the_time_since_the_previous_step() {
    let rule = scenario("1h", "  - signal: a\n  - signal: b\n    max_gap: 5m\n");
    let close = [spanning("a1", "a", 0, 10, &[]), signal("b1", "b", 15, &[])];
    let far = [spanning("a1", "a", 0, 10, &[]), signal("b1", "b", 16, &[])];

    assert_eq!(match_scenario(&rule, &close).len(), 1);
    assert_eq!(match_scenario(&rule, &far).len(), 0);
}

#[test]
fn max_gap_after_a_skipped_optional_step_is_measured_from_the_last_matched_step() {
    let rule = scenario(
        "1h",
        "  - signal: a\n  - signal: o\n    optional: true\n  - signal: b\n    max_gap: 5m\n",
    );
    let signals = [signal("a1", "a", 0, &[]), signal("b1", "b", 6, &[])];

    assert_eq!(match_scenario(&rule, &signals).len(), 0);
}

const SAME_BUCKET: &str = "  - signal: a\n    same: [bucket]\n  - signal: b\n    same: [bucket]\n";

#[test]
fn same_requires_equal_attribute_values() {
    let rule = scenario("1h", SAME_BUCKET);
    let equal = [
        signal("a1", "a", 0, &[("bucket", "evidence")]),
        signal("b1", "b", 1, &[("bucket", "evidence")]),
    ];
    let different = [
        signal("a1", "a", 0, &[("bucket", "evidence")]),
        signal("b1", "b", 1, &[("bucket", "backups")]),
    ];
    let missing = [
        signal("a1", "a", 0, &[("bucket", "evidence")]),
        signal("b1", "b", 1, &[]),
    ];

    assert_eq!(match_scenario(&rule, &equal).len(), 1);
    assert_eq!(match_scenario(&rule, &different).len(), 0);
    assert_eq!(match_scenario(&rule, &missing).len(), 0);
}

#[test]
fn steps_without_same_are_not_bound_by_it() {
    let rule = scenario(
        "1h",
        "  - signal: a\n  - signal: b\n    same: [bucket]\n  - signal: c\n    same: [bucket]\n",
    );
    let signals = [
        signal("a1", "a", 0, &[("bucket", "other")]),
        signal("b1", "b", 1, &[("bucket", "evidence")]),
        signal("c1", "c", 2, &[("bucket", "evidence")]),
    ];

    assert_eq!(match_scenario(&rule, &signals).len(), 1);
}

#[test]
fn a_later_candidate_is_tried_when_the_earliest_one_breaks_a_later_same_binding() {
    let rule = scenario(
        "1h",
        "  - signal: a\n  - signal: b\n    same: [bucket]\n  - signal: c\n    same: [bucket]\n",
    );
    let signals = [
        signal("a1", "a", 0, &[]),
        signal("b1", "b", 1, &[("bucket", "backups")]),
        signal("b2", "b", 2, &[("bucket", "evidence")]),
        signal("c1", "c", 3, &[("bucket", "evidence")]),
    ];

    assert_eq!(
        labels(&match_scenario(&rule, &signals)),
        [found(&[&["a1"], &["b2"], &["c1"]])]
    );
}

#[test]
fn the_search_resumes_after_the_first_signal_of_the_previous_match() {
    let rule = scenario("1h", A_THEN_B);
    let signals = [
        signal("a1", "a", 0, &[]),
        signal("a2", "a", 1, &[]),
        signal("b1", "b", 2, &[]),
    ];

    assert_eq!(
        labels(&match_scenario(&rule, &signals)),
        [found(&[&["a1"], &["b1"]]), found(&[&["a2"], &["b1"]])]
    );
}

#[test]
fn the_same_set_of_signals_is_not_reported_twice() {
    let rule = scenario("1h", A_THEN_B);
    let signals = [
        signal("a1", "a", 0, &[]),
        signal("b1", "b", 1, &[]),
        signal("b2", "b", 2, &[]),
    ];

    assert_eq!(
        labels(&match_scenario(&rule, &signals)),
        [found(&[&["a1"], &["b1"]])]
    );
}

#[test]
fn skipping_a_leading_optional_step_does_not_report_a_subset_of_a_match_again() {
    let rule = scenario(
        "1h",
        "  - signal: o\n    optional: true\n  - signal: a\n  - signal: b\n",
    );
    let signals = [
        signal("o1", "o", 0, &[]),
        signal("a1", "a", 1, &[]),
        signal("b1", "b", 2, &[]),
    ];

    assert_eq!(
        labels(&match_scenario(&rule, &signals)),
        [found(&[&["o1"], &["a1"], &["b1"]])]
    );
}

#[test]
fn a_signal_is_not_used_by_two_steps_of_one_match() {
    let rule = scenario("1h", "  - signal: a\n  - signal: a\n");
    let one = [signal("a1", "a", 0, &[])];
    let two = [signal("a1", "a", 0, &[]), signal("a2", "a", 1, &[])];

    assert_eq!(match_scenario(&rule, &one).len(), 0);
    assert_eq!(
        labels(&match_scenario(&rule, &two)),
        [found(&[&["a1"], &["a2"]])]
    );
}

fn time_limit() -> Duration {
    Duration::from_secs(5)
}

#[test]
fn five_thousand_signals_without_a_match_are_searched_in_reasonable_time() {
    let rule = scenario(
        "1h",
        "  - signal: a\n  - signal: b\n    same: [bucket]\n  - signal: c\n    same: [bucket]\n  - signal: d\n    same: [bucket]\n",
    );
    let buckets = ["b0", "b1", "b2", "b3", "b4"];
    let signals: Vec<ObservedSignal> = (0..5_000)
        .map(|index| {
            let signal_id = ["a", "b", "c", "d"][index % 4];
            let bucket = if signal_id == "d" {
                format!("d{index}")
            } else {
                buckets[index % buckets.len()].to_owned()
            };
            let mut observed = signal(&format!("s{index}"), signal_id, 0, &[]);
            observed.row.start_time += TimeDelta::milliseconds(index as i64);
            observed.row.end_time = observed.row.start_time;
            observed.row.attrs.insert("bucket".to_owned(), bucket);
            observed
        })
        .collect();

    let started = Instant::now();
    let matches = match_scenario(&rule, &signals);

    assert_eq!(matches.len(), 0);
    assert!(
        started.elapsed() < time_limit(),
        "took {:?}",
        started.elapsed()
    );
}

#[test]
fn five_thousand_signals_with_many_matches_are_searched_in_reasonable_time() {
    let rule = scenario(
        "1h",
        "  - signal: a\n  - signal: b\n    min_count: 2\n    same: [bucket]\n  - signal: c\n    same: [bucket]\n",
    );
    let signals: Vec<ObservedSignal> = (0..5_000)
        .map(|index| {
            let signal_id = ["a", "b", "c"][index % 3];
            let bucket = format!("b{}", index % 7);
            let mut observed = signal(&format!("s{index}"), signal_id, 0, &[]);
            observed.row.start_time += TimeDelta::milliseconds(index as i64);
            observed.row.end_time = observed.row.start_time;
            observed.row.attrs.insert("bucket".to_owned(), bucket);
            observed
        })
        .collect();

    let started = Instant::now();
    let matches = match_scenario(&rule, &signals);

    assert!(!matches.is_empty());
    assert!(
        started.elapsed() < time_limit(),
        "took {:?}",
        started.elapsed()
    );
}
