use std::path::{Component, Path};

use ashiato_core::rule::builtin_files;
use serde::Deserialize;
use serde_json::Value;

use crate::record::{RecordError, complete_record};

const FIXTURES_DIRECTORY: &str = "fixtures";
const FIXTURE_EXTENSION: &str = ".json";

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
    let mut files: Vec<FixtureFile> = builtin_files(FIXTURE_EXTENSION)
        .into_iter()
        .map(|file| FixtureFile {
            path: file.path,
            content: file.content,
        })
        .collect();
    files.sort_by(|a, b| a.path.cmp(&b.path));
    files.iter().map(Fixture::parse).collect()
}
