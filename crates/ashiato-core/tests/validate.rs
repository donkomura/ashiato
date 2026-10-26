use ashiato_core::macros::MacroError;
use ashiato_core::rule::{RuleFile, RuleSource, Rules};
use ashiato_core::validate::{ValidationError, function_calls, validate};

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
