use std::collections::BTreeMap;

use ashiato_core::macros::{Dialect, MacroContext, MacroError, expand};
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
            params: &params,
        },
    )
}

fn expand_ok(sql: &str) -> String {
    expand_with(sql, &[]).unwrap()
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
