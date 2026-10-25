use std::collections::BTreeMap;

use crate::{AnalysisWindow, DurationError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    DuckDb,
}

#[derive(Debug, Clone, Copy)]
pub struct MacroContext<'a> {
    pub dialect: Dialect,
    pub window: &'a AnalysisWindow,
    pub params: &'a BTreeMap<String, serde_json::Value>,
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

pub fn expand(_sql: &str, _context: &MacroContext<'_>) -> Result<String, MacroError> {
    todo!()
}
