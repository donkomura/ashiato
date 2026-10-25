use async_trait::async_trait;

use crate::macros::{Dialect, MacroContext, MacroError, expand};
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
    let context = MacroContext {
        dialect: backend.dialect(),
        window,
        params: &rule.params,
    };
    let sql = expand(&rule.sql, &context).map_err(|source| SignalQueryError::Macro {
        rule_id: rule.id.clone(),
        source,
    })?;
    backend
        .query_signals(&sql)
        .await
        .map_err(|source| SignalQueryError::Backend {
            rule_id: rule.id.clone(),
            source,
        })
}
