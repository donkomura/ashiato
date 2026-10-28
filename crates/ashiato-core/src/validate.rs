use serde::Serialize;

use crate::Duration;
use crate::fixture::fixture_window;
use crate::macros::{self, Dialect, MacroContext, MacroError, MacroKind, WindowScope};
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

const AGGREGATE_FUNCTIONS: [&str; 8] = [
    "array_agg",
    "avg",
    "bool_and",
    "bool_or",
    "count",
    "max",
    "min",
    "sum",
];

const CLAUSES_AFTER_GROUP_BY: [&str; 6] =
    ["having", "limit", "offset", "order", "qualify", "window"];

const SET_OPERATORS: [&str; 3] = ["except", "intersect", "union"];

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
        match self {
            Self::AggregateWithoutTimeBin { rule_id }
            | Self::BatchNotMultipleOfTimeBin { rule_id, .. }
            | Self::InBaselineWindow { rule_id } => rule_id,
        }
    }

    pub fn check(&self) -> StreamingCheck {
        match self {
            Self::AggregateWithoutTimeBin { .. } => StreamingCheck::AggregateWithoutTimeBin,
            Self::BatchNotMultipleOfTimeBin { .. } => StreamingCheck::BatchNotMultipleOfTimeBin,
            Self::InBaselineWindow { .. } => StreamingCheck::InBaselineWindow,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Token<'a> {
    Macro(&'a str),
    Word(&'a str),
    Open,
    Close,
    Other,
}

// Splits SQL into the tokens that the checks look at, skipping whitespace, comments and the
// contents of string literals and quoted identifiers, without parsing the SQL (段階 20).
struct Tokens<'a> {
    sql: &'a str,
    pos: usize,
}

impl<'a> Iterator for Tokens<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Token<'a>> {
        loop {
            let rest = &self.sql[self.pos..];
            let c = rest.chars().next()?;
            let (len, token) = if rest.starts_with("{{") {
                let len = skip_past(rest, "{{", "}}");
                let inner = &rest[2..len];
                (
                    len,
                    Some(Token::Macro(inner.strip_suffix("}}").unwrap_or(inner))),
                )
            } else if rest.starts_with("--") {
                (skip_past(rest, "--", "\n"), None)
            } else if rest.starts_with("/*") {
                (skip_past(rest, "/*", "*/"), None)
            } else if c == '\'' || c == '"' {
                (quoted_len(rest), Some(Token::Other))
            } else if c.is_ascii_alphabetic() || c == '_' {
                let len = rest
                    .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                    .unwrap_or(rest.len());
                (len, Some(Token::Word(&rest[..len])))
            } else {
                let token = match c {
                    '(' => Some(Token::Open),
                    ')' => Some(Token::Close),
                    c if c.is_whitespace() => None,
                    _ => Some(Token::Other),
                };
                (c.len_utf8(), token)
            };
            self.pos += len;
            if token.is_some() {
                return token;
            }
        }
    }
}

fn tokens(sql: &str) -> Tokens<'_> {
    Tokens { sql, pos: 0 }
}

pub fn function_calls(sql: &str) -> Vec<&str> {
    let mut tokens = tokens(sql).peekable();
    let mut calls = Vec::new();
    while let Some(token) = tokens.next() {
        if let Token::Word(name) = token
            && tokens.peek() == Some(&Token::Open)
            && !is_keyword(name)
        {
            calls.push(name);
        }
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
    is_one_of(&KEYWORDS, name)
}

fn is_one_of(words: &[&str], name: &str) -> bool {
    words.iter().any(|word| word.eq_ignore_ascii_case(name))
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
    rules
        .iter()
        .filter_map(|rule| match rule {
            Rule::Signal(signal) => Some(streaming_violations(signal, batch)),
            Rule::Scenario(_) => None,
        })
        .flatten()
        .collect()
}

pub fn streaming_violations(rule: &SignalRule, batch: Duration) -> Vec<StreamingViolation> {
    let window = fixture_window();
    let context = MacroContext {
        dialect: Dialect::DuckDb,
        window: &window,
        scope: WindowScope::Whole,
        params: &rule.params,
    };
    let scan = scan(&rule.sql, &context);
    let rule_id = || rule.id.clone();
    let mut violations = Vec::new();
    if scan.aggregates_without_time_bin {
        violations.push(StreamingViolation::AggregateWithoutTimeBin { rule_id: rule_id() });
    }
    let batch_seconds = batch.to_time_delta().num_seconds();
    let mut bins: Vec<Duration> = Vec::new();
    for bin in scan.bins {
        if batch_seconds % bin.to_time_delta().num_seconds() != 0 && !bins.contains(&bin) {
            bins.push(bin);
            violations.push(StreamingViolation::BatchNotMultipleOfTimeBin {
                rule_id: rule_id(),
                bin,
                batch,
            });
        }
    }
    if scan.baseline_window {
        violations.push(StreamingViolation::InBaselineWindow { rule_id: rule_id() });
    }
    violations
}

// A SELECT and its clauses, up to the closing parenthesis or a set operator.
#[derive(Debug, Default)]
struct Block {
    selects: bool,
    aggregates: bool,
    grouped: bool,
    in_group_by: bool,
    grouped_by_time_bin: bool,
}

impl Block {
    fn needs_time_bin(&self) -> bool {
        (self.aggregates || self.grouped) && !self.grouped_by_time_bin
    }
}

#[derive(Debug, Default)]
struct Scan {
    aggregates_without_time_bin: bool,
    bins: Vec<Duration>,
    baseline_window: bool,
}

// Each parenthesis opens a block, so that subqueries and CTEs are checked on their own, and
// an aggregate inside a function call counts for the innermost enclosing SELECT.
fn scan(sql: &str, context: &MacroContext<'_>) -> Scan {
    let mut result = Scan::default();
    let mut blocks = vec![Block::default()];
    let mut tokens = tokens(sql).peekable();
    let mut previous = None;
    while let Some(token) = tokens.next() {
        match token {
            Token::Open => blocks.push(Block::default()),
            Token::Close if blocks.len() > 1 => {
                let block = blocks.pop().expect("more than one block is open");
                result.aggregates_without_time_bin |= block.needs_time_bin();
            }
            Token::Macro(expression) => match macros::classify(expression, context) {
                Ok(MacroKind::TimeBin(bin)) => {
                    result.bins.extend(bin);
                    if let Some(block) = enclosing_select(&mut blocks)
                        && block.in_group_by
                    {
                        block.grouped_by_time_bin = true;
                    }
                }
                Ok(MacroKind::BaselineWindow) => result.baseline_window = true,
                Ok(MacroKind::Other) | Err(_) => {}
            },
            Token::Word(word) => {
                let block = blocks
                    .last_mut()
                    .expect("the outermost block is never closed");
                if word.eq_ignore_ascii_case("select") {
                    block.selects = true;
                } else if word.eq_ignore_ascii_case("by")
                    && previous.is_some_and(|word: &str| word.eq_ignore_ascii_case("group"))
                {
                    block.grouped = true;
                    block.in_group_by = true;
                } else if is_one_of(&CLAUSES_AFTER_GROUP_BY, word) {
                    block.in_group_by = false;
                } else if is_one_of(&SET_OPERATORS, word) {
                    result.aggregates_without_time_bin |= std::mem::take(block).needs_time_bin();
                } else if is_one_of(&AGGREGATE_FUNCTIONS, word)
                    && tokens.peek() == Some(&Token::Open)
                    && let Some(block) = enclosing_select(&mut blocks)
                {
                    block.aggregates = true;
                }
            }
            Token::Close | Token::Other => {}
        }
        previous = match token {
            Token::Word(word) => Some(word),
            _ => None,
        };
    }
    for block in blocks {
        result.aggregates_without_time_bin |= block.needs_time_bin();
    }
    result
}

fn enclosing_select(blocks: &mut [Block]) -> Option<&mut Block> {
    blocks.iter_mut().rev().find(|block| block.selects)
}
