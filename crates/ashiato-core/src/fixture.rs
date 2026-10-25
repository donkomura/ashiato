use serde_json::Value;

use crate::AnalysisWindow;
use crate::record::RecordError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expectation {
    Positive,
    Negative,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixtureFile {
    pub path: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Fixture {
    pub rule_id: String,
    pub expectation: Expectation,
    pub path: String,
    pub records: Vec<Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum FixtureError {
    #[error("fixture {path} is not placed at <rule id>/fixtures/{{positive,negative}}/<name>.json")]
    Layout { path: String },
    #[error("failed to parse fixture {path}")]
    Json {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("record {index} of fixture {path} is invalid")]
    Record {
        path: String,
        index: usize,
        #[source]
        source: RecordError,
    },
}

impl Fixture {
    pub fn parse(file: &FixtureFile) -> Result<Self, FixtureError> {
        todo!("{file:?}")
    }
}

pub fn builtin_fixtures() -> Result<Vec<Fixture>, FixtureError> {
    todo!()
}

pub fn fixture_window() -> AnalysisWindow {
    todo!()
}
