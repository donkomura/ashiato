use async_trait::async_trait;
use serde::Serialize;

use crate::lineage::LineageEdge;
use crate::macros::{Dialect, MacroContext, MacroError, expand};
use crate::rule::{DataRequirement, SignalRule};
use crate::{AnalysisWindow, SignalRow};

const BASELINE_CTES: &str = "\
WITH baseline AS (
  SELECT principal_key, 'source_ip' AS dim, source_ip AS value FROM events
   WHERE {{ in_baseline_window(event_time) }} GROUP BY 1, 3
  UNION ALL
  SELECT principal_key, 'user_agent', user_agent FROM events
   WHERE {{ in_baseline_window(event_time) }} GROUP BY 1, 3
  UNION ALL
  SELECT principal_key, 'region', aws_region FROM events
   WHERE {{ in_baseline_window(event_time) }} GROUP BY 1, 3
  UNION ALL
  SELECT principal_key, 'event_name', event_name FROM events
   WHERE {{ in_baseline_window(event_time) }} GROUP BY 1, 3
),
baseline_principals AS (
  SELECT DISTINCT principal_key FROM events
   WHERE {{ in_baseline_window(event_time) }}
)";
const WITH_KEYWORD: &str = "with";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct DataAvailability {
    pub events: u64,
    pub s3_data_events: bool,
}

impl DataAvailability {
    pub fn satisfies(&self, requirement: DataRequirement) -> bool {
        match requirement {
            DataRequirement::S3DataEvents => self.s3_data_events,
        }
    }
}

#[async_trait]
pub trait QueryBackend: Send + Sync {
    fn dialect(&self) -> Dialect;
    async fn query_signals(&self, sql: &str) -> Result<Vec<SignalRow>, BackendError>;
    async fn data_availability(
        &self,
        window: &AnalysisWindow,
    ) -> Result<DataAvailability, BackendError>;
    async fn query_lineage(
        &self,
        window: &AnalysisWindow,
    ) -> Result<Vec<LineageEdge>, BackendError>;
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
    let sql =
        expand(&with_baseline(&rule.sql), &context).map_err(|source| SignalQueryError::Macro {
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

fn with_baseline(sql: &str) -> String {
    let trimmed = sql.trim_start();
    let rest = trimmed
        .get(..WITH_KEYWORD.len())
        .filter(|keyword| keyword.eq_ignore_ascii_case(WITH_KEYWORD))
        .map(|_| &trimmed[WITH_KEYWORD.len()..])
        .filter(|rest| rest.starts_with(char::is_whitespace));
    match rest {
        Some(ctes) => format!("{BASELINE_CTES},\n{}", ctes.trim_start()),
        None => format!("{BASELINE_CTES}\n{sql}"),
    }
}
