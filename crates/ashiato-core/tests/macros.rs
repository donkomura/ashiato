use std::collections::BTreeMap;

use ashiato_core::macros::{Dialect, MacroContext, MacroError, WindowScope, expand};
use ashiato_core::{AnalysisWindow, DurationError};
use chrono::TimeDelta;
use serde_json::{Value, json};

fn window() -> AnalysisWindow {
    AnalysisWindow::new(
        "2026-09-24T00:00:00Z".parse().unwrap(),
        "2026-10-25T00:00:00Z".parse().unwrap(),
        TimeDelta::days(30),
    )
    .unwrap()
}

fn expand_with(sql: &str, params: &[(&str, Value)]) -> Result<String, MacroError> {
    expand_in(sql, WindowScope::Whole, params)
}

fn expand_in(
    sql: &str,
    scope: WindowScope,
    params: &[(&str, Value)],
) -> Result<String, MacroError> {
    let window = window();
    let params: BTreeMap<String, Value> = params
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect();
    expand(
        sql,
        &MacroContext {
            dialect: Dialect::DuckDb,
            window: &window,
            scope,
            params: &params,
        },
    )
}

fn expand_ok(sql: &str) -> String {
    expand_with(sql, &[]).unwrap()
}

fn batch(start: &str, end: &str) -> WindowScope {
    WindowScope::Batch {
        start: start.parse().unwrap(),
        end: end.parse().unwrap(),
    }
}

fn expand_in_batch(sql: &str, start: &str, end: &str) -> String {
    expand_in(sql, batch(start, end), &[]).unwrap()
}

#[test]
fn sql_without_macros_is_returned_unchanged() {
    let sql = "SELECT '{' AS a, '}}' AS b\nFROM events WHERE x = '{ y }'\n";

    assert_eq!(expand_ok(sql), sql);
}

#[test]
fn json_macro_expands_to_json_extract_string() {
    assert_eq!(
        expand_ok("SELECT {{ json(request_parameters, '$.bucketName') }} AS attr_bucket"),
        "SELECT json_extract_string(request_parameters, '$.bucketName') AS attr_bucket"
    );
}

#[test]
fn json_macro_accepts_qualified_column() {
    assert_eq!(
        expand_ok("{{ json(e.request_parameters, '$.roleArn') }}"),
        "json_extract_string(e.request_parameters, '$.roleArn')"
    );
}

#[test]
fn time_bin_macro_expands_to_time_bucket() {
    for (duration, interval) in [
        ("30s", "30 seconds"),
        ("1s", "1 second"),
        ("5m", "5 minutes"),
        ("1m", "1 minute"),
        ("1h", "1 hour"),
        ("2h", "2 hours"),
        ("1d", "1 day"),
        ("30d", "30 days"),
    ] {
        assert_eq!(
            expand_ok(&format!("{{{{ time_bin(event_time, {duration}) }}}}")),
            format!("time_bucket(INTERVAL '{interval}', event_time)"),
            "{duration}"
        );
    }
}

#[test]
fn time_bin_macro_accepts_duration_from_param() {
    assert_eq!(
        expand_with(
            "{{ time_bin(event_time, param('bin')) }}",
            &[("bin", json!("5m"))]
        )
        .unwrap(),
        "time_bucket(INTERVAL '5 minutes', event_time)"
    );
}

#[test]
fn time_bin_macro_rejects_invalid_duration() {
    assert_eq!(
        expand_with("{{ time_bin(event_time, 5x) }}", &[]),
        Err(MacroError::Duration(DurationError::Invalid(
            "5x".to_owned()
        )))
    );
    assert_eq!(
        expand_with(
            "{{ time_bin(event_time, param('bin')) }}",
            &[("bin", json!("five minutes"))]
        ),
        Err(MacroError::Duration(DurationError::Invalid(
            "five minutes".to_owned()
        )))
    );
}

#[test]
fn in_analysis_window_macro_expands_to_range_condition() {
    assert_eq!(
        expand_ok("WHERE {{ in_analysis_window(event_time) }}"),
        "WHERE (event_time >= TIMESTAMP '2026-09-24 00:00:00' \
         AND event_time < TIMESTAMP '2026-10-25 00:00:00')"
    );
}

#[test]
fn in_baseline_window_macro_expands_to_range_condition() {
    assert_eq!(
        expand_ok("WHERE {{ in_baseline_window(e.event_time) }}"),
        "WHERE (e.event_time >= TIMESTAMP '2026-08-25 00:00:00' \
         AND e.event_time < TIMESTAMP '2026-09-24 00:00:00')"
    );
}

#[test]
fn param_macro_expands_to_sql_literal() {
    let params = [
        ("min_deletes", json!(3)),
        ("ratio", json!(0.5)),
        ("pattern", json!("p3.%")),
        ("quoted", json!("o'brien")),
        ("enabled", json!(true)),
    ];
    for (sql, expected) in [
        ("{{ param('min_deletes') }}", "3"),
        ("{{ param('ratio') }}", "0.5"),
        ("{{ param('pattern') }}", "'p3.%'"),
        ("{{ param('quoted') }}", "'o''brien'"),
        ("{{ param('enabled') }}", "TRUE"),
    ] {
        assert_eq!(expand_with(sql, &params).unwrap(), expected, "{sql}");
    }
}

#[test]
fn param_macro_rejects_value_without_sql_literal() {
    for value in [json!(null), json!(["a", "b"]), json!({ "a": 1 })] {
        assert!(
            matches!(
                expand_with("{{ param('value') }}", &[("value", value.clone())]),
                Err(MacroError::InvalidArgument { ref name, .. }) if name == "param"
            ),
            "{value}"
        );
    }
}

#[test]
fn undefined_param_is_rejected() {
    assert_eq!(
        expand_with("{{ param('missing') }}", &[("other", json!(1))]),
        Err(MacroError::UndefinedParam {
            name: "missing".to_owned()
        })
    );
}

#[test]
fn whitespace_around_arguments_is_ignored() {
    assert_eq!(
        expand_with(
            "{{json(  request_parameters ,'$.bucketName'  )}} {{  time_bin( event_time,param( 'bin' ) )  }}",
            &[("bin", json!("5m"))]
        )
        .unwrap(),
        "json_extract_string(request_parameters, '$.bucketName') \
         time_bucket(INTERVAL '5 minutes', event_time)"
    );
}

#[test]
fn string_argument_may_contain_comma_parenthesis_and_quote() {
    assert_eq!(
        expand_ok("{{ json(resources, '$[0].ARN, (x) ''y''') }}"),
        "json_extract_string(resources, '$[0].ARN, (x) ''y''')"
    );
}

#[test]
fn unknown_macro_is_rejected() {
    assert_eq!(
        expand_with("{{ regexp(source_ip, '^10\\.') }}", &[]),
        Err(MacroError::UnknownMacro {
            name: "regexp".to_owned()
        })
    );
}

#[test]
fn wrong_number_of_arguments_is_rejected() {
    for (sql, macro_name) in [
        ("{{ json(request_parameters) }}", "json"),
        ("{{ time_bin(event_time) }}", "time_bin"),
        (
            "{{ in_analysis_window(event_time, 1) }}",
            "in_analysis_window",
        ),
        ("{{ param('a', 'b') }}", "param"),
    ] {
        assert!(
            matches!(
                expand_with(sql, &[("a", json!(1))]),
                Err(MacroError::InvalidArgument { ref name, .. }) if name == macro_name
            ),
            "{sql}"
        );
    }
}

#[test]
fn argument_of_wrong_kind_is_rejected() {
    for (sql, macro_name) in [
        ("{{ json(request_parameters, bucketName) }}", "json"),
        ("{{ json('request_parameters', '$.a') }}", "json"),
        ("{{ param(name) }}", "param"),
        ("{{ in_analysis_window(param('a')) }}", "in_analysis_window"),
    ] {
        assert!(
            matches!(
                expand_with(sql, &[("a", json!(1))]),
                Err(MacroError::InvalidArgument { ref name, .. }) if name == macro_name
            ),
            "{sql}"
        );
    }
}

#[test]
fn malformed_macro_expression_is_rejected() {
    for sql in [
        "{{ }}",
        "{{ json }}",
        "{{ json(request_parameters, '$.a' }}",
        "{{ json(request_parameters, '$.a) }}",
        "{{ json(request_parameters, '$.a') extra }}",
    ] {
        assert!(
            matches!(expand_with(sql, &[]), Err(MacroError::Syntax { .. })),
            "{sql}: {:?}",
            expand_with(sql, &[])
        );
    }
}

#[test]
fn unclosed_macro_is_rejected() {
    assert_eq!(
        expand_with("SELECT 1 WHERE {{ in_analysis_window(event_time)", &[]),
        Err(MacroError::Unclosed { offset: 15 })
    );
}

#[test]
fn signal_sql_from_design_document_expands_completely() {
    let sql = r#"
  SELECT
    min(event_time)          AS start_time,
    {{ json(request_parameters, '$.bucketName') }} AS attr_bucket
  FROM events
  WHERE {{ in_analysis_window(event_time) }}
    AND event_name IN ('DeleteObject', 'DeleteObjects')
  GROUP BY access_key_id,
           {{ json(request_parameters, '$.bucketName') }},
           {{ time_bin(event_time, param('bin')) }}
  HAVING count(*) >= {{ param('min_deletes') }}
"#;

    let expanded = expand_with(sql, &[("min_deletes", json!(3)), ("bin", json!("5m"))]).unwrap();

    assert_eq!(
        expanded,
        r#"
  SELECT
    min(event_time)          AS start_time,
    json_extract_string(request_parameters, '$.bucketName') AS attr_bucket
  FROM events
  WHERE (event_time >= TIMESTAMP '2026-09-24 00:00:00' AND event_time < TIMESTAMP '2026-10-25 00:00:00')
    AND event_name IN ('DeleteObject', 'DeleteObjects')
  GROUP BY access_key_id,
           json_extract_string(request_parameters, '$.bucketName'),
           time_bucket(INTERVAL '5 minutes', event_time)
  HAVING count(*) >= 3
"#
    );
}

#[test]
fn in_analysis_window_macro_expands_to_the_batch_inside_the_analysis_window() {
    assert_eq!(
        expand_in_batch(
            "WHERE {{ in_analysis_window(event_time) }}",
            "2026-10-01T03:00:00Z",
            "2026-10-01T04:00:00Z",
        ),
        "WHERE (event_time >= TIMESTAMP '2026-10-01 03:00:00' \
         AND event_time < TIMESTAMP '2026-10-01 04:00:00')"
    );
}

#[test]
fn in_analysis_window_macro_cuts_the_batch_at_the_start_of_the_analysis_window() {
    assert_eq!(
        expand_in_batch(
            "WHERE {{ in_analysis_window(e.event_time) }}",
            "2026-09-23T23:00:00Z",
            "2026-09-24T01:00:00Z",
        ),
        "WHERE (e.event_time >= TIMESTAMP '2026-09-24 00:00:00' \
         AND e.event_time < TIMESTAMP '2026-09-24 01:00:00')"
    );
}

#[test]
fn in_analysis_window_macro_cuts_the_batch_at_the_end_of_the_analysis_window() {
    assert_eq!(
        expand_in_batch(
            "WHERE {{ in_analysis_window(event_time) }}",
            "2026-10-24T23:00:00Z",
            "2026-10-25T01:00:00Z",
        ),
        "WHERE (event_time >= TIMESTAMP '2026-10-24 23:00:00' \
         AND event_time < TIMESTAMP '2026-10-25 00:00:00')"
    );
}

#[test]
fn in_analysis_window_macro_cuts_a_batch_longer_than_the_analysis_window_at_both_ends() {
    assert_eq!(
        expand_in_batch(
            "WHERE {{ in_analysis_window(event_time) }}",
            "2026-09-01T00:00:00Z",
            "2026-11-01T00:00:00Z",
        ),
        "WHERE (event_time >= TIMESTAMP '2026-09-24 00:00:00' \
         AND event_time < TIMESTAMP '2026-10-25 00:00:00')"
    );
}

#[test]
fn in_analysis_window_macro_expands_to_false_for_a_batch_outside_the_analysis_window() {
    for (start, end) in [
        ("2026-09-01T00:00:00Z", "2026-09-01T01:00:00Z"),
        ("2026-09-23T23:00:00Z", "2026-09-24T00:00:00Z"),
        ("2026-10-25T00:00:00Z", "2026-10-25T01:00:00Z"),
        ("2026-11-01T00:00:00Z", "2026-11-01T01:00:00Z"),
    ] {
        assert_eq!(
            expand_in_batch("WHERE {{ in_analysis_window(event_time) }}", start, end),
            "WHERE FALSE",
            "{start} - {end}"
        );
    }
}

#[test]
fn in_baseline_window_macro_expands_to_false_in_a_batch() {
    for (start, end) in [
        ("2026-09-01T00:00:00Z", "2026-09-01T01:00:00Z"),
        ("2026-10-01T00:00:00Z", "2026-10-01T01:00:00Z"),
    ] {
        assert_eq!(
            expand_in_batch("WHERE {{ in_baseline_window(event_time) }}", start, end),
            "WHERE FALSE",
            "{start} - {end}"
        );
    }
}

#[test]
fn baseline_or_analysis_window_condition_is_limited_to_the_batch() {
    assert_eq!(
        expand_in_batch(
            "WHERE ({{ in_baseline_window(event_time) }} OR {{ in_analysis_window(event_time) }})",
            "2026-10-01T00:00:00Z",
            "2026-10-01T01:00:00Z",
        ),
        "WHERE (FALSE OR (event_time >= TIMESTAMP '2026-10-01 00:00:00' \
         AND event_time < TIMESTAMP '2026-10-01 01:00:00'))"
    );
}

#[test]
fn other_macros_expand_in_a_batch_as_in_the_whole_window() {
    let sql = "SELECT {{ json(request_parameters, '$.bucketName') }}, \
               {{ time_bin(event_time, param('bin')) }}, {{ param('min_deletes') }}";
    let params = [("min_deletes", json!(3)), ("bin", json!("5m"))];

    assert_eq!(
        expand_in(
            sql,
            batch("2026-10-01T00:00:00Z", "2026-10-01T01:00:00Z"),
            &params
        )
        .unwrap(),
        expand_with(sql, &params).unwrap()
    );
    assert_eq!(
        expand_with(sql, &params).unwrap(),
        "SELECT json_extract_string(request_parameters, '$.bucketName'), \
         time_bucket(INTERVAL '5 minutes', event_time), 3"
    );
}

#[test]
fn errors_are_reported_in_a_batch_as_in_the_whole_window() {
    assert_eq!(
        expand_in(
            "{{ param('missing') }}",
            batch("2026-10-01T00:00:00Z", "2026-10-01T01:00:00Z"),
            &[]
        ),
        Err(MacroError::UndefinedParam {
            name: "missing".to_owned()
        })
    );
    assert!(matches!(
        expand_in(
            "{{ in_baseline_window(event_time, 1) }}",
            batch("2026-10-01T00:00:00Z", "2026-10-01T01:00:00Z"),
            &[]
        ),
        Err(MacroError::InvalidArgument { ref name, .. }) if name == "in_baseline_window"
    ));
}
