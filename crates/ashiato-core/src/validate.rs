use serde::Serialize;

use crate::Duration;
use crate::fixture::fixture_window;
use crate::macros::{self, Dialect, MacroContext, MacroError, WindowScope};
use crate::rule::{Rule, Rules, SignalRule};

const ALLOWED_FUNCTIONS: [&str; 23] = [
    "array_agg",
    "avg",
    "bool_and",
    "bool_or",
    "cast",
    "coalesce",
    "concat",
    "count",
    "greatest",
    "least",
    "length",
    "lower",
    "max",
    "min",
    "nullif",
    "replace",
    "split_part",
    "starts_with",
    "strpos",
    "substr",
    "sum",
    "trim",
    "upper",
];

const KEYWORDS: [&str; 30] = [
    "all",
    "and",
    "any",
    "as",
    "between",
    "by",
    "case",
    "else",
    "except",
    "exists",
    "filter",
    "from",
    "having",
    "in",
    "intersect",
    "is",
    "join",
    "lateral",
    "like",
    "not",
    "on",
    "or",
    "over",
    "select",
    "some",
    "then",
    "union",
    "using",
    "values",
    "where",
];

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValidationError {
    #[error(
        "signal {rule_id} calls {function}, which is not in the functions shared by DuckDB and Athena"
    )]
    DisallowedFunction { rule_id: String, function: String },
    #[error("signal {rule_id} has a macro that cannot be expanded: {error}")]
    Macro { rule_id: String, error: MacroError },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamingCheck {
    AggregateWithoutTimeBin,
    BatchNotMultipleOfTimeBin,
    InBaselineWindow,
}

// Checks of streaming-pipeline.md 8.4. They are warnings rather than errors, since the rules
// still work for `analyze`, and `run` leaves out the rules that fail them.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamingViolation {
    #[error(
        "signal {rule_id} aggregates without grouping by time_bin, so the batches of the pipeline cannot evaluate it"
    )]
    AggregateWithoutTimeBin { rule_id: String },
    #[error(
        "the batch length {batch} is not a multiple of the time_bin length {bin} of signal {rule_id}"
    )]
    BatchNotMultipleOfTimeBin {
        rule_id: String,
        bin: Duration,
        batch: Duration,
    },
    #[error("signal {rule_id} uses in_baseline_window, which the pipeline expands to FALSE")]
    InBaselineWindow { rule_id: String },
}

impl StreamingViolation {
    pub fn rule_id(&self) -> &str {
        todo!()
    }

    pub fn check(&self) -> StreamingCheck {
        todo!()
    }
}

pub fn function_calls(sql: &str) -> Vec<&str> {
    let mut calls = Vec::new();
    let mut pos = 0;
    while pos < sql.len() {
        let rest = &sql[pos..];
        pos += if rest.starts_with("{{") {
            skip_past(rest, "{{", "}}")
        } else if rest.starts_with("--") {
            skip_past(rest, "--", "\n")
        } else if rest.starts_with("/*") {
            skip_past(rest, "/*", "*/")
        } else if rest.starts_with(['\'', '"']) {
            quoted_len(rest)
        } else if rest.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
            let len = rest
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(rest.len());
            let name = &rest[..len];
            if rest[len..].trim_start().starts_with('(') && !is_keyword(name) {
                calls.push(name);
            }
            len
        } else {
            rest.chars().next().map_or(1, char::len_utf8)
        };
    }
    calls
}

fn skip_past(rest: &str, open: &str, close: &str) -> usize {
    rest[open.len()..]
        .find(close)
        .map_or(rest.len(), |end| open.len() + end + close.len())
}

fn quoted_len(rest: &str) -> usize {
    let quote = rest.as_bytes()[0];
    let bytes = rest.as_bytes();
    let mut pos = 1;
    while pos < bytes.len() {
        if bytes[pos] == quote {
            if bytes.get(pos + 1) != Some(&quote) {
                return pos + 1;
            }
            pos += 1;
        }
        pos += 1;
    }
    rest.len()
}

fn is_keyword(name: &str) -> bool {
    KEYWORDS
        .iter()
        .any(|keyword| keyword.eq_ignore_ascii_case(name))
}

fn is_allowed_function(name: &str) -> bool {
    ALLOWED_FUNCTIONS
        .iter()
        .any(|function| function.eq_ignore_ascii_case(name))
}

pub fn validate(rules: &Rules) -> Vec<ValidationError> {
    let window = fixture_window();
    let mut errors = Vec::new();
    for rule in rules.iter() {
        let Rule::Signal(signal) = rule else {
            continue;
        };
        let context = MacroContext {
            dialect: Dialect::DuckDb,
            window: &window,
            scope: WindowScope::Whole,
            params: &signal.params,
        };
        if let Err(error) = macros::expand(&signal.sql, &context) {
            errors.push(ValidationError::Macro {
                rule_id: signal.id.clone(),
                error,
            });
        }
        for function in function_calls(&signal.sql) {
            if !is_allowed_function(function) {
                errors.push(ValidationError::DisallowedFunction {
                    rule_id: signal.id.clone(),
                    function: function.to_owned(),
                });
            }
        }
    }
    errors
}

pub fn validate_streaming(rules: &Rules, batch: Duration) -> Vec<StreamingViolation> {
    let _ = (rules, batch);
    todo!()
}

pub fn streaming_violations(rule: &SignalRule, batch: Duration) -> Vec<StreamingViolation> {
    let _ = (rule, batch);
    todo!()
}
