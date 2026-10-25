use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::{AnalysisWindow, Duration, DurationError, DurationUnit};

const OPEN: &str = "{{";
const CLOSE: &str = "}}";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    DuckDb,
}

#[derive(Debug, Clone, Copy)]
pub struct MacroContext<'a> {
    pub dialect: Dialect,
    pub window: &'a AnalysisWindow,
    pub params: &'a BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MacroError {
    #[error("macro starting at byte {offset} is not closed with }}}}")]
    Unclosed { offset: usize },
    #[error("malformed macro {expression:?}: {message}")]
    Syntax { expression: String, message: String },
    #[error("unknown macro {name}")]
    UnknownMacro { name: String },
    #[error("param {name} is not defined")]
    UndefinedParam { name: String },
    #[error("invalid argument to macro {name}: {message}")]
    InvalidArgument { name: String, message: String },
    #[error(transparent)]
    Duration(#[from] DurationError),
}

pub fn expand(sql: &str, context: &MacroContext<'_>) -> Result<String, MacroError> {
    let mut expanded = String::with_capacity(sql.len());
    let mut rest = sql;
    while let Some(start) = rest.find(OPEN) {
        let offset = sql.len() - rest.len() + start;
        let after_open = &rest[start + OPEN.len()..];
        let end = after_open
            .find(CLOSE)
            .ok_or(MacroError::Unclosed { offset })?;
        let call = Parser::parse(&after_open[..end])?;
        expanded.push_str(&rest[..start]);
        expanded.push_str(&render(&call, context)?);
        rest = &after_open[end + CLOSE.len()..];
    }
    expanded.push_str(rest);
    Ok(expanded)
}

#[derive(Debug)]
struct Call {
    name: String,
    args: Vec<Arg>,
}

#[derive(Debug)]
enum Arg {
    Bare(String),
    Str(String),
    Call(Call),
}

struct Parser<'a> {
    expression: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn parse(expression: &'a str) -> Result<Call, MacroError> {
        let mut parser = Self { expression, pos: 0 };
        parser.skip_whitespace();
        let name = parser.word();
        let call = parser.call(name)?;
        parser.skip_whitespace();
        if parser.pos < expression.len() {
            return Err(parser.error("unexpected text after the macro call"));
        }
        Ok(call)
    }

    fn call(&mut self, name: &str) -> Result<Call, MacroError> {
        if !is_identifier(name) {
            return Err(self.error("expected a macro name"));
        }
        self.skip_whitespace();
        self.expect('(')?;
        let mut args = Vec::new();
        self.skip_whitespace();
        if self.eat(')') {
            return Ok(Call {
                name: name.to_owned(),
                args,
            });
        }
        loop {
            args.push(self.arg()?);
            self.skip_whitespace();
            if self.eat(')') {
                break;
            }
            self.expect(',')?;
        }
        Ok(Call {
            name: name.to_owned(),
            args,
        })
    }

    fn arg(&mut self) -> Result<Arg, MacroError> {
        self.skip_whitespace();
        if self.eat('\'') {
            return self.string().map(Arg::Str);
        }
        let word = self.word();
        if word.is_empty() {
            return Err(self.error("expected an argument"));
        }
        self.skip_whitespace();
        if self.rest().starts_with('(') {
            return self.call(word).map(Arg::Call);
        }
        Ok(Arg::Bare(word.to_owned()))
    }

    fn string(&mut self) -> Result<String, MacroError> {
        let mut value = String::new();
        loop {
            let Some(c) = self.rest().chars().next() else {
                return Err(self.error("string literal is not closed"));
            };
            self.pos += c.len_utf8();
            if c != '\'' {
                value.push(c);
            } else if self.eat('\'') {
                value.push('\'');
            } else {
                return Ok(value);
            }
        }
    }

    fn word(&mut self) -> &'a str {
        let rest = &self.expression[self.pos..];
        let len = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '.'))
            .unwrap_or(rest.len());
        self.pos += len;
        &rest[..len]
    }

    fn rest(&self) -> &'a str {
        &self.expression[self.pos..]
    }

    fn skip_whitespace(&mut self) {
        let rest = self.rest();
        self.pos += rest.len() - rest.trim_start().len();
    }

    fn eat(&mut self, c: char) -> bool {
        if self.rest().starts_with(c) {
            self.pos += c.len_utf8();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, c: char) -> Result<(), MacroError> {
        if self.eat(c) {
            Ok(())
        } else {
            Err(self.error(&format!("expected {c:?}")))
        }
    }

    fn error(&self, message: &str) -> MacroError {
        MacroError::Syntax {
            expression: self.expression.trim().to_owned(),
            message: format!("{message} at byte {}", self.pos),
        }
    }
}

fn is_identifier(word: &str) -> bool {
    word.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') && !word.contains('.')
}

fn render(call: &Call, context: &MacroContext<'_>) -> Result<String, MacroError> {
    match context.dialect {
        Dialect::DuckDb => render_duckdb(call, context),
    }
}

fn render_duckdb(call: &Call, context: &MacroContext<'_>) -> Result<String, MacroError> {
    match call.name.as_str() {
        "json" => {
            let [column, path] = args(call)?;
            Ok(format!(
                "json_extract_string({}, {})",
                column_arg(call, column)?,
                quote(&string_arg(call, path, context)?)
            ))
        }
        "time_bin" => {
            let [column, duration] = args(call)?;
            Ok(format!(
                "time_bucket(INTERVAL '{}', {})",
                interval(duration_arg(call, duration, context)?),
                column_arg(call, column)?
            ))
        }
        "in_analysis_window" => {
            let [column] = args(call)?;
            Ok(range(
                column_arg(call, column)?,
                context.window.since(),
                context.window.until(),
            ))
        }
        "in_baseline_window" => {
            let [column] = args(call)?;
            Ok(range(
                column_arg(call, column)?,
                context.window.baseline_since(),
                context.window.baseline_until(),
            ))
        }
        "param" => {
            let [name] = args(call)?;
            literal(call, param_value(call, name, context)?)
        }
        _ => Err(MacroError::UnknownMacro {
            name: call.name.clone(),
        }),
    }
}

fn args<const N: usize>(call: &Call) -> Result<[&Arg; N], MacroError> {
    let args: Vec<&Arg> = call.args.iter().collect();
    args.try_into().map_err(|args: Vec<&Arg>| {
        invalid_argument(
            call,
            format!("expected {N} arguments, but got {}", args.len()),
        )
    })
}

fn column_arg<'a>(call: &Call, arg: &'a Arg) -> Result<&'a str, MacroError> {
    match arg {
        Arg::Bare(column) => Ok(column),
        _ => Err(invalid_argument(call, "expected a column name".to_owned())),
    }
}

fn string_arg(call: &Call, arg: &Arg, context: &MacroContext<'_>) -> Result<String, MacroError> {
    match arg {
        Arg::Str(value) => Ok(value.clone()),
        Arg::Call(param) if param.name == "param" => {
            let [name] = args(param)?;
            match param_value(param, name, context)? {
                Value::String(value) => Ok(value.clone()),
                other => Err(invalid_argument(
                    call,
                    format!("expected a string param, but got {other}"),
                )),
            }
        }
        _ => Err(invalid_argument(
            call,
            "expected a string literal or param".to_owned(),
        )),
    }
}

fn duration_arg(
    call: &Call,
    arg: &Arg,
    context: &MacroContext<'_>,
) -> Result<Duration, MacroError> {
    let text = match arg {
        Arg::Bare(text) => text.clone(),
        _ => string_arg(call, arg, context)?,
    };
    Ok(text.parse()?)
}

fn param_value<'a>(
    call: &Call,
    name: &Arg,
    context: &MacroContext<'a>,
) -> Result<&'a Value, MacroError> {
    let Arg::Str(name) = name else {
        return Err(invalid_argument(
            call,
            "expected the param name as a string literal".to_owned(),
        ));
    };
    context
        .params
        .get(name)
        .ok_or_else(|| MacroError::UndefinedParam { name: name.clone() })
}

fn literal(call: &Call, value: &Value) -> Result<String, MacroError> {
    match value {
        Value::Number(number) => Ok(number.to_string()),
        Value::String(text) => Ok(quote(text)),
        Value::Bool(true) => Ok("TRUE".to_owned()),
        Value::Bool(false) => Ok("FALSE".to_owned()),
        other => Err(invalid_argument(
            call,
            format!("{other} cannot be written as an SQL literal"),
        )),
    }
}

fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

fn interval(duration: Duration) -> String {
    let unit = match duration.unit() {
        DurationUnit::Seconds => "second",
        DurationUnit::Minutes => "minute",
        DurationUnit::Hours => "hour",
        DurationUnit::Days => "day",
    };
    let plural = if duration.amount() == 1 { "" } else { "s" };
    format!("{} {unit}{plural}", duration.amount())
}

fn range(column: &str, since: DateTime<Utc>, until: DateTime<Utc>) -> String {
    format!(
        "({column} >= {} AND {column} < {})",
        timestamp(since),
        timestamp(until)
    )
}

fn timestamp(time: DateTime<Utc>) -> String {
    format!("TIMESTAMP '{}'", time.format("%Y-%m-%d %H:%M:%S%.f"))
}

fn invalid_argument(call: &Call, message: String) -> MacroError {
    MacroError::InvalidArgument {
        name: call.name.clone(),
        message,
    }
}
