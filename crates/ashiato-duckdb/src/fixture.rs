use std::fmt;

use ashiato_core::analysis::{AnalysisError, analyze};
use ashiato_core::fixture::{Expectation, Fixture, fixture_window};
use ashiato_core::query::query_signal_rule;
use ashiato_core::rule::{Rule, Rules};

use crate::{EventStore, IngestError};

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
        detections: Detections,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Detections {
    Signals(usize),
    ScenarioMatches(usize),
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
        source: Box<AnalysisError>,
    },
}

impl fmt::Display for FixtureReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            rule_id,
            path,
            verdict,
        } = self;
        match verdict {
            Verdict::Passed => write!(f, "PASS {rule_id} ({path})"),
            Verdict::Failed {
                expectation: Expectation::Positive,
                detections,
            } => write!(
                f,
                "FAIL {rule_id} ({path}): expected at least one {}, but got none",
                detections.noun()
            ),
            Verdict::Failed {
                expectation: Expectation::Negative,
                detections,
            } => write!(
                f,
                "FAIL {rule_id} ({path}): expected no {}, but got {}",
                detections.plural_noun(),
                detections.count()
            ),
        }
    }
}

impl Detections {
    fn noun(self) -> &'static str {
        match self {
            Detections::Signals(_) => "signal",
            Detections::ScenarioMatches(_) => "scenario match",
        }
    }

    fn plural_noun(self) -> &'static str {
        match self {
            Detections::Signals(_) => "signals",
            Detections::ScenarioMatches(_) => "scenario matches",
        }
    }

    fn count(self) -> usize {
        match self {
            Detections::Signals(count) | Detections::ScenarioMatches(count) => count,
        }
    }
}

pub async fn run_fixture(
    rules: &Rules,
    fixture: &Fixture,
) -> Result<FixtureReport, FixtureRunError> {
    let report = |verdict| FixtureReport {
        rule_id: fixture.rule_id.clone(),
        path: fixture.path.clone(),
        verdict,
    };
    let Some(rule) = rules.get(&fixture.rule_id) else {
        return Err(FixtureRunError::UnknownRule {
            rule_id: fixture.rule_id.clone(),
            path: fixture.path.clone(),
        });
    };

    let ingest_error = |source| FixtureRunError::Ingest {
        rule_id: fixture.rule_id.clone(),
        path: fixture.path.clone(),
        source,
    };
    let store = EventStore::open_in_memory().map_err(ingest_error)?;
    store
        .ingest_records(&fixture.records)
        .map_err(ingest_error)?;
    let backend = store.into_backend();

    let window = fixture_window();
    let detections = match rule {
        Rule::Signal(rule) => query_signal_rule(&backend, rule, &window)
            .await
            .map(|signals| Detections::Signals(signals.len()))
            .map_err(AnalysisError::from),
        Rule::Scenario(scenario) => analyze(&backend, rules, &window).await.map(|analysis| {
            let matches = analysis
                .matches
                .iter()
                .filter(|found| found.scenario_id == scenario.id)
                .count();
            Detections::ScenarioMatches(matches)
        }),
    }
    .map_err(|source| FixtureRunError::Query {
        rule_id: fixture.rule_id.clone(),
        path: fixture.path.clone(),
        source: Box::new(source),
    })?;
    let passed = match fixture.expectation {
        Expectation::Positive => detections.count() > 0,
        Expectation::Negative => detections.count() == 0,
    };
    Ok(report(if passed {
        Verdict::Passed
    } else {
        Verdict::Failed {
            expectation: fixture.expectation,
            detections,
        }
    }))
}
