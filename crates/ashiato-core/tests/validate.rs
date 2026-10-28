use std::collections::BTreeMap;

use ashiato_core::Duration;
use ashiato_core::macros::MacroError;
use ashiato_core::rule::{RuleFile, RuleSource, Rules};
use ashiato_core::validate::{
    StreamingCheck, StreamingViolation, ValidationError, function_calls, validate,
    validate_streaming,
};

fn signal(id: &str, sql: &str) -> RuleFile {
    let sql: String = sql.lines().map(|line| format!("  {line}\n")).collect();
    RuleFile {
        path: format!("{id}/rule.yaml"),
        content: format!(
            "kind: signal\nid: {id}\ntitle: 試験用の兆候\nseverity: low\ntactics: [TA0007]\nparams:\n  bin: 5m\nsql: |\n{sql}"
        ),
    }
}

fn rules(files: Vec<RuleFile>) -> Rules {
    Rules::new([RuleSource::parse("test", files).unwrap()]).unwrap()
}

#[test]
fn function_calls_lists_identifiers_followed_by_a_parenthesis() {
    let calls = function_calls("SELECT count(*), min(event_time), split_part(a, '.', 1) FROM t");

    assert_eq!(calls, ["count", "min", "split_part"]);
}

#[test]
fn function_calls_allows_whitespace_before_the_parenthesis() {
    assert_eq!(function_calls("SELECT max (event_time) FROM t"), ["max"]);
}

#[test]
fn function_calls_finds_nested_calls() {
    assert_eq!(
        function_calls("SELECT lower(trim(a)) FROM t"),
        ["lower", "trim"]
    );
}

#[test]
fn function_calls_ignores_keywords_followed_by_a_parenthesis() {
    let sql = "SELECT 1 FROM t AS e \
               WHERE a IN ('x', 'y') AND NOT EXISTS (SELECT 1 FROM u) \
               OR (b = 1) AND c IN(SELECT d FROM v)";

    assert!(function_calls(sql).is_empty(), "{:?}", function_calls(sql));
}

#[test]
fn function_calls_ignores_parentheses_in_string_literals() {
    let sql = "SELECT 1 FROM t WHERE a = 'regexp_matches(x)' AND b = 'it''s lower(x)'";

    assert!(function_calls(sql).is_empty(), "{:?}", function_calls(sql));
}

#[test]
fn function_calls_ignores_comments() {
    let sql = "SELECT 1 -- regexp_matches(a)\nFROM t /* upper(b)\n lower(c) */ WHERE d = 1";

    assert!(function_calls(sql).is_empty(), "{:?}", function_calls(sql));
}

#[test]
fn function_calls_ignores_quoted_identifiers() {
    assert!(function_calls(r#"SELECT "max(x)" FROM t"#).is_empty());
}

#[test]
fn function_calls_ignores_macros() {
    let sql = "SELECT {{ json(request_parameters, '$.bucketName') }} FROM events \
               WHERE {{ in_analysis_window(event_time) }} \
               GROUP BY {{ time_bin(event_time, param('bin')) }}";

    assert!(function_calls(sql).is_empty(), "{:?}", function_calls(sql));
}

#[test]
fn function_calls_reads_calls_after_a_string_literal() {
    assert_eq!(function_calls("SELECT ',' || lower(a) FROM t"), ["lower"]);
}

#[test]
fn validate_accepts_the_builtin_rules() {
    let builtin = Rules::new([RuleSource::builtin().unwrap()]).unwrap();

    assert_eq!(validate(&builtin), []);
}

#[test]
fn validate_accepts_allowed_functions_in_any_case() {
    let rules = rules(vec![signal(
        "probe",
        "SELECT COUNT(*), Array_Agg(event_id) FROM events\nWHERE {{ in_analysis_window(event_time) }}",
    )]);

    assert_eq!(validate(&rules), []);
}

#[test]
fn validate_rejects_functions_outside_the_allow_list() {
    let rules = rules(vec![signal(
        "probe",
        "SELECT event_id FROM events\nWHERE regexp_matches(event_name, '^List')",
    )]);

    let errors = validate(&rules);

    assert_eq!(
        errors,
        [ValidationError::DisallowedFunction {
            rule_id: "probe".to_owned(),
            function: "regexp_matches".to_owned(),
        }]
    );
    let message = errors[0].to_string();
    assert!(message.contains("probe"), "{message}");
    assert!(message.contains("regexp_matches"), "{message}");
}

#[test]
fn validate_reports_every_disallowed_function_of_every_rule() {
    let rules = rules(vec![
        signal(
            "first",
            "SELECT list_value(a), regexp_matches(b, 'x') FROM events",
        ),
        signal("second", "SELECT epoch_ms(event_time) FROM events"),
    ]);

    let functions: Vec<(String, String)> = validate(&rules)
        .into_iter()
        .map(|error| match error {
            ValidationError::DisallowedFunction { rule_id, function } => (rule_id, function),
            other => panic!("unexpected error: {other:?}"),
        })
        .collect();

    assert_eq!(
        functions,
        [
            ("first".to_owned(), "list_value".to_owned()),
            ("first".to_owned(), "regexp_matches".to_owned()),
            ("second".to_owned(), "epoch_ms".to_owned()),
        ]
    );
}

#[test]
fn validate_rejects_macros_that_cannot_be_expanded() {
    let rules = rules(vec![
        signal("unknown_macro", "SELECT {{ regexp(a) }} FROM events"),
        signal(
            "undefined_param",
            "SELECT {{ param('missing') }} FROM events",
        ),
    ]);

    assert_eq!(
        validate(&rules),
        [
            ValidationError::Macro {
                rule_id: "undefined_param".to_owned(),
                error: MacroError::UndefinedParam {
                    name: "missing".to_owned(),
                },
            },
            ValidationError::Macro {
                rule_id: "unknown_macro".to_owned(),
                error: MacroError::UnknownMacro {
                    name: "regexp".to_owned(),
                },
            },
        ]
    );
}

fn duration(text: &str) -> Duration {
    text.parse().unwrap()
}

fn streaming_checks(sql: &str, batch: &str) -> Vec<StreamingCheck> {
    validate_streaming(&rules(vec![signal("probe", sql)]), duration(batch))
        .iter()
        .map(StreamingViolation::check)
        .collect()
}

#[test]
fn streaming_checks_report_the_rules_that_the_batches_of_the_pipeline_cannot_evaluate() {
    use StreamingCheck::{AggregateWithoutTimeBin, BatchNotMultipleOfTimeBin, InBaselineWindow};

    let cases: &[(&str, &str, &str, &[StreamingCheck])] = &[
        (
            "a row per event",
            "SELECT event_id FROM events WHERE {{ in_analysis_window(event_time) }}",
            "1h",
            &[],
        ),
        (
            "grouped by time_bin",
            "SELECT count(*) FROM events\nGROUP BY principal_key,\n  {{ time_bin(event_time, param('bin')) }}\nHAVING count(*) >= 2",
            "1h",
            &[],
        ),
        (
            "grouped by time_bin in lower case",
            "select count(*) from events group by {{ time_bin(event_time, 5m) }}, principal_key",
            "1h",
            &[],
        ),
        (
            "grouped without time_bin",
            "SELECT count(*) FROM events GROUP BY principal_key",
            "1h",
            &[AggregateWithoutTimeBin],
        ),
        (
            "grouped without time_bin and without aggregate functions",
            "SELECT principal_key FROM events GROUP BY principal_key",
            "1h",
            &[AggregateWithoutTimeBin],
        ),
        (
            "aggregated without GROUP BY",
            "SELECT min(event_time), max(event_time) FROM events",
            "1h",
            &[AggregateWithoutTimeBin],
        ),
        (
            "aggregated inside another function call",
            "SELECT cast(count(*) AS VARCHAR) FROM events",
            "1h",
            &[AggregateWithoutTimeBin],
        ),
        (
            "time_bin outside the GROUP BY",
            "SELECT {{ time_bin(event_time, 5m) }}, count(*) FROM events GROUP BY principal_key",
            "1h",
            &[AggregateWithoutTimeBin],
        ),
        (
            "time_bin only after the GROUP BY",
            "SELECT count(*) FROM events GROUP BY principal_key\nORDER BY {{ time_bin(event_time, 5m) }}",
            "1h",
            &[AggregateWithoutTimeBin],
        ),
        (
            "GROUP BY and an aggregate in a string literal",
            "SELECT event_id FROM events WHERE event_name = 'count(*) GROUP BY principal_key'",
            "1h",
            &[],
        ),
        (
            "GROUP BY and an aggregate in comments",
            "SELECT event_id -- count(*) GROUP BY principal_key\nFROM events /* min(event_time)\nGROUP BY a */",
            "1h",
            &[],
        ),
        (
            "an aggregate name in a quoted identifier",
            r#"SELECT "count(*)" FROM events"#,
            "1h",
            &[],
        ),
        (
            "an identifier that only contains an aggregate name",
            "SELECT event_id, account_count FROM events WHERE min_time IS NULL",
            "1h",
            &[],
        ),
        (
            "grouped without time_bin in a subquery",
            "SELECT event_id FROM events\nWHERE principal_key IN (\n  SELECT principal_key FROM events GROUP BY principal_key HAVING count(*) > 1\n)",
            "1h",
            &[AggregateWithoutTimeBin],
        ),
        (
            "grouped without time_bin in a CTE",
            "WITH c AS (SELECT access_key_id, min(event_time) AS t FROM events GROUP BY access_key_id)\nSELECT access_key_id FROM c",
            "1h",
            &[AggregateWithoutTimeBin],
        ),
        (
            "grouped by time_bin in a subquery of a query without aggregates",
            "SELECT * FROM (\n  SELECT count(*) AS n FROM events GROUP BY {{ time_bin(event_time, 5m) }}\n) t WHERE n > 1",
            "1h",
            &[],
        ),
        (
            "grouped by time_bin outside a subquery grouped without it",
            "SELECT count(*) FROM (\n  SELECT principal_key, min(event_time) AS t FROM events GROUP BY principal_key\n) s\nGROUP BY {{ time_bin(t, 5m) }}",
            "1h",
            &[AggregateWithoutTimeBin],
        ),
        (
            "grouped by time_bin inside parentheses",
            "SELECT count(*) FROM events GROUP BY (principal_key, {{ time_bin(event_time, 5m) }})",
            "1h",
            &[],
        ),
        (
            "a union of a select grouped by time_bin and an aggregate without it",
            "SELECT count(*) FROM events GROUP BY {{ time_bin(event_time, 5m) }}\nUNION ALL\nSELECT count(*) FROM events",
            "1h",
            &[AggregateWithoutTimeBin],
        ),
        (
            "SELECT DISTINCT",
            "SELECT DISTINCT event_id FROM events",
            "1h",
            &[],
        ),
        (
            "a batch that is a multiple of the time_bin of the params",
            "SELECT count(*) FROM events GROUP BY {{ time_bin(event_time, param('bin')) }}",
            "15m",
            &[],
        ),
        (
            "a batch that is not a multiple of the time_bin of the params",
            "SELECT count(*) FROM events GROUP BY {{ time_bin(event_time, param('bin')) }}",
            "7m",
            &[BatchNotMultipleOfTimeBin],
        ),
        (
            "a batch shorter than the time_bin",
            "SELECT count(*) FROM events GROUP BY {{ time_bin(event_time, 2h) }}",
            "1h",
            &[BatchNotMultipleOfTimeBin],
        ),
        (
            "a batch in days and a time_bin in hours",
            "SELECT count(*) FROM events GROUP BY {{ time_bin(event_time, 8h) }}",
            "1d",
            &[],
        ),
        (
            "a time_bin outside the GROUP BY that does not divide the batch",
            "SELECT {{ time_bin(event_time, 7m) }} AS bin, event_id FROM events",
            "1h",
            &[BatchNotMultipleOfTimeBin],
        ),
        (
            "in_baseline_window",
            "SELECT event_id FROM events WHERE {{ in_baseline_window(event_time) }}",
            "1h",
            &[InBaselineWindow],
        ),
        (
            "in_baseline_window and a GROUP BY without time_bin",
            "SELECT count(*) FROM events\nWHERE {{ in_baseline_window(event_time) }} OR {{ in_analysis_window(event_time) }}\nGROUP BY principal_key",
            "1h",
            &[AggregateWithoutTimeBin, InBaselineWindow],
        ),
        (
            "the baseline table",
            "SELECT event_id FROM events e\nWHERE NOT EXISTS (SELECT 1 FROM baseline b WHERE b.principal_key = e.principal_key)",
            "1h",
            &[],
        ),
    ];

    for (name, sql, batch, expected) in cases {
        assert_eq!(&streaming_checks(sql, batch), expected, "{name}");
    }
}

#[test]
fn streaming_checks_report_each_time_bin_length_that_does_not_divide_the_batch() {
    let sql = "SELECT count(*) FROM events\nGROUP BY {{ time_bin(event_time, 7m) }}, {{ time_bin(event_time, param('bin')) }}, {{ time_bin(event_time, 7m) }}";

    let violations = validate_streaming(&rules(vec![signal("probe", sql)]), duration("8m"));

    assert_eq!(
        violations,
        [
            StreamingViolation::BatchNotMultipleOfTimeBin {
                rule_id: "probe".to_owned(),
                bin: duration("7m"),
                batch: duration("8m"),
            },
            StreamingViolation::BatchNotMultipleOfTimeBin {
                rule_id: "probe".to_owned(),
                bin: duration("5m"),
                batch: duration("8m"),
            },
        ]
    );
    let message = violations[0].to_string();
    for part in ["probe", "7m", "8m"] {
        assert!(message.contains(part), "{message}");
    }
}

#[test]
fn streaming_checks_leave_the_macros_that_cannot_be_expanded_to_validate() {
    let rules = rules(vec![
        signal(
            "first",
            "SELECT count(*) FROM events GROUP BY {{ time_bin(event_time, param('missing')) }}",
        ),
        signal("second", "SELECT {{ time_bin(event_time, 7m }} FROM events"),
    ]);

    assert_eq!(validate_streaming(&rules, duration("1h")), []);
}

#[test]
fn streaming_violations_name_the_rule_and_the_check() {
    let rules = rules(vec![signal(
        "probe",
        "SELECT count(*) FROM events WHERE {{ in_baseline_window(event_time) }} GROUP BY principal_key",
    )]);

    let violations = validate_streaming(&rules, duration("1h"));

    assert_eq!(
        violations,
        [
            StreamingViolation::AggregateWithoutTimeBin {
                rule_id: "probe".to_owned(),
            },
            StreamingViolation::InBaselineWindow {
                rule_id: "probe".to_owned(),
            },
        ]
    );
    for violation in &violations {
        assert_eq!(violation.rule_id(), "probe");
        let message = violation.to_string();
        assert!(message.contains("probe"), "{message}");
    }
    assert!(violations[0].to_string().contains("time_bin"));
    assert!(violations[1].to_string().contains("in_baseline_window"));
}

#[test]
fn the_builtin_rules_that_the_pipeline_cannot_evaluate_are_the_ones_that_read_across_batches() {
    use StreamingCheck::{AggregateWithoutTimeBin, InBaselineWindow};
    let builtin = Rules::new([RuleSource::builtin().unwrap()]).unwrap();

    let mut checks: BTreeMap<&str, Vec<StreamingCheck>> = BTreeMap::new();
    let violations = validate_streaming(&builtin, duration("1h"));
    for violation in &violations {
        checks
            .entry(violation.rule_id())
            .or_default()
            .push(violation.check());
    }

    assert_eq!(
        checks,
        BTreeMap::from([
            ("cloudtrail_logging_paused", vec![AggregateWithoutTimeBin]),
            (
                "first_seen_account_region_write",
                vec![AggregateWithoutTimeBin, InBaselineWindow]
            ),
            (
                "role_session_outlier",
                vec![AggregateWithoutTimeBin, InBaselineWindow]
            ),
            ("self_privilege_escalation", vec![AggregateWithoutTimeBin]),
            (
                "temporary_credential_ip_change",
                vec![AggregateWithoutTimeBin, InBaselineWindow]
            ),
        ])
    );
}
