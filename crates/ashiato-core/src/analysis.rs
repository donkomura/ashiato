use crate::AnalysisWindow;
use crate::lineage::LineageWarning;
use crate::output::{ScenarioMatchRecord, SignalRecord};
use crate::query::{BackendError, QueryBackend, SignalQueryError};
use crate::rule::Rules;

#[derive(Debug, Clone, PartialEq)]
pub struct Analysis {
    pub signals: Vec<SignalRecord>,
    pub matches: Vec<ScenarioMatchRecord>,
    pub warnings: Vec<LineageWarning>,
}

#[derive(Debug, thiserror::Error)]
pub enum AnalysisError {
    #[error(transparent)]
    Signal(#[from] SignalQueryError),
    #[error("failed to query the session lineage")]
    Lineage(#[source] BackendError),
}

pub async fn analyze<B: QueryBackend + ?Sized>(
    _backend: &B,
    _rules: &Rules,
    _window: &AnalysisWindow,
) -> Result<Analysis, AnalysisError> {
    todo!()
}
