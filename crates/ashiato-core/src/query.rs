use async_trait::async_trait;

use crate::macros::{Dialect, MacroError};
use crate::rule::SignalRule;
use crate::{AnalysisWindow, SignalRow};

#[async_trait]
pub trait QueryBackend: Send + Sync {
    fn dialect(&self) -> Dialect;
    async fn query_signals(&self, sql: &str) -> Result<Vec<SignalRow>, BackendError>;
}

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("the query result has no {column} column")]
    MissingColumn { column: String },
    #[error("column {column} has an unexpected value: {message}")]
    InvalidValue { column: String, message: String },
    #[error("the query failed")]
    Query(#[source] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, thiserror::Error)]
pub enum SignalQueryError {
    #[error("failed to expand the macros of signal rule {rule_id}")]
    Macro {
        rule_id: String,
        #[source]
        source: MacroError,
    },
    #[error("failed to run signal rule {rule_id}")]
    Backend {
        rule_id: String,
        #[source]
        source: BackendError,
    },
}

impl SignalQueryError {
    pub fn rule_id(&self) -> &str {
        match self {
            SignalQueryError::Macro { rule_id, .. } | SignalQueryError::Backend { rule_id, .. } => {
                rule_id
            }
        }
    }
}

pub async fn query_signal_rule<B: QueryBackend + ?Sized>(
    backend: &B,
    rule: &SignalRule,
    window: &AnalysisWindow,
) -> Result<Vec<SignalRow>, SignalQueryError> {
    todo!(
        "expand {} for {window:?} and run it on {:?}",
        rule.id,
        backend.dialect()
    )
}
