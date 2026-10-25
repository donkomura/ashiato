use crate::macros::MacroError;
use crate::rule::Rules;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValidationError {
    #[error(
        "signal {rule_id} calls {function}, which is not in the functions shared by DuckDB and Athena"
    )]
    DisallowedFunction { rule_id: String, function: String },
    #[error("signal {rule_id} has a macro that cannot be expanded: {error}")]
    Macro { rule_id: String, error: MacroError },
}

pub fn function_calls(sql: &str) -> Vec<&str> {
    let _ = sql;
    todo!()
}

pub fn validate(rules: &Rules) -> Vec<ValidationError> {
    let _ = rules;
    todo!()
}
