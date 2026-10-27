use std::ops::Range;
use std::path::{Path, PathBuf};

use ashiato_pipeline::{EventTime, Manifest, RunPlan};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListScope<'a> {
    pub accounts: Option<&'a [String]>,
    pub regions: Option<&'a [String]>,
    pub period: Range<EventTime>,
}

impl<'a> ListScope<'a> {
    pub fn from_plan(_plan: &'a RunPlan) -> Self {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalListing {
    pub manifest: Manifest,
    pub unrecognized: Vec<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
pub enum ListError {
    #[error("failed to read {path}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("log file path {0:?} is not valid UTF-8")]
    NonUtf8Path(PathBuf),
}

pub fn list_local(_root: &Path, _scope: &ListScope<'_>) -> Result<LocalListing, ListError> {
    todo!()
}
