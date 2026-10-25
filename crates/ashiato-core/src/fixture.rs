use std::path::{Component, Path};

use chrono::{TimeDelta, TimeZone, Utc};
use include_dir::Dir;
use serde::Deserialize;
use serde_json::Value;

use crate::AnalysisWindow;
use crate::record::{RecordError, complete_record};
use crate::rule::BUILTIN_RULES;

const FIXTURES_DIRECTORY: &str = "fixtures";
const FIXTURE_EXTENSION: &str = ".json";
const FIXTURE_BASELINE_DAYS: i64 = 30;

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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureContent {
    #[serde(rename = "Records")]
    records: Vec<Value>,
}

impl Fixture {
    pub fn parse(file: &FixtureFile) -> Result<Self, FixtureError> {
        let (rule_id, expectation) = locate(&file.path).ok_or_else(|| FixtureError::Layout {
            path: file.path.clone(),
        })?;
        let content: FixtureContent =
            serde_json::from_str(&file.content).map_err(|source| FixtureError::Json {
                path: file.path.clone(),
                source,
            })?;
        let records = content
            .records
            .into_iter()
            .enumerate()
            .map(|(index, record)| {
                complete_record(record).map_err(|source| FixtureError::Record {
                    path: file.path.clone(),
                    index,
                    source,
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            rule_id,
            expectation,
            path: file.path.clone(),
            records,
        })
    }
}

fn locate(path: &str) -> Option<(String, Expectation)> {
    let parts: Vec<&str> = Path::new(path)
        .components()
        .filter_map(|component| match component {
            Component::Normal(part) => part.to_str(),
            _ => None,
        })
        .collect();
    let [.., rule_id, FIXTURES_DIRECTORY, kind, name] = parts.as_slice() else {
        return None;
    };
    if !name.ends_with(FIXTURE_EXTENSION) {
        return None;
    }
    let expectation = match *kind {
        "positive" => Expectation::Positive,
        "negative" => Expectation::Negative,
        _ => return None,
    };
    Some(((*rule_id).to_owned(), expectation))
}

pub fn builtin_fixtures() -> Result<Vec<Fixture>, FixtureError> {
    let mut files = Vec::new();
    collect_builtin_fixture_files(&BUILTIN_RULES, &mut files);
    files.sort_by(|a, b| a.path.cmp(&b.path));
    files.iter().map(Fixture::parse).collect()
}

fn collect_builtin_fixture_files(dir: &Dir<'_>, files: &mut Vec<FixtureFile>) {
    for file in dir.files() {
        let path = file.path().to_string_lossy();
        if !path.ends_with(FIXTURE_EXTENSION) {
            continue;
        }
        if let Some(content) = file.contents_utf8() {
            files.push(FixtureFile {
                path: path.into_owned(),
                content: content.to_owned(),
            });
        }
    }
    for child in dir.dirs() {
        collect_builtin_fixture_files(child, files);
    }
}

// The analysis window covers the day of the default eventTime of RecordBuilder,
// so that records which do not set eventTime fall inside it. Records for the
// baseline period are written with an eventTime in the preceding 30 days.
pub fn fixture_window() -> AnalysisWindow {
    let since = Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap();
    AnalysisWindow::new(
        since,
        since + TimeDelta::days(1),
        TimeDelta::days(FIXTURE_BASELINE_DAYS),
    )
    .expect("the fixture window is a fixed, non-empty window")
}
