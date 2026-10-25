use std::fmt;

use ashiato_core::fixture::{Expectation, Fixture};
use ashiato_core::query::SignalQueryError;
use ashiato_core::rule::Rules;

use crate::IngestError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixtureReport {
    pub rule_id: String,
    pub path: String,
    pub verdict: Verdict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Passed,
    Failed {
        expectation: Expectation,
        signals: usize,
    },
    Skipped,
}

#[derive(Debug, thiserror::Error)]
pub enum FixtureRunError {
    #[error("fixture {path} refers to rule {rule_id}, which is not defined")]
    UnknownRule { rule_id: String, path: String },
    #[error("failed to ingest fixture {path} of rule {rule_id}")]
    Ingest {
        rule_id: String,
        path: String,
        #[source]
        source: IngestError,
    },
    #[error("failed to run rule {rule_id} against fixture {path}")]
    Query {
        rule_id: String,
        path: String,
        #[source]
        source: Box<SignalQueryError>,
    },
}

impl fmt::Display for FixtureReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _ = f;
        todo!()
    }
}

pub async fn run_fixture(
    rules: &Rules,
    fixture: &Fixture,
) -> Result<FixtureReport, FixtureRunError> {
    todo!("{rules:?} {fixture:?}")
}
