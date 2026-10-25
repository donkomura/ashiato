use std::path::Path;

use async_trait::async_trait;
use chrono::NaiveDate;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Location {
    pub bucket: String,
    pub prefix: String,
}

impl S3Location {
    pub fn parse(uri: &str) -> anyhow::Result<Self> {
        todo!("{uri}")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyFilter {
    pub accounts: Vec<String>,
    pub regions: Vec<String>,
    pub from: Option<NaiveDate>,
    pub to: Option<NaiveDate>,
}

impl KeyFilter {
    pub fn is_unrestricted(&self) -> bool {
        todo!()
    }

    pub fn may_contain(&self, prefix: &str) -> bool {
        todo!("{prefix}")
    }

    pub fn matches(&self, key: &str) -> bool {
        todo!("{key}")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Listing {
    pub prefixes: Vec<String>,
    pub keys: Vec<String>,
}

#[async_trait]
pub trait ObjectStore: Sync {
    async fn list(&self, bucket: &str, prefix: &str) -> anyhow::Result<Listing>;
    async fn get(&self, bucket: &str, key: &str) -> anyhow::Result<Vec<u8>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DownloadSummary {
    pub downloaded: usize,
    pub skipped: usize,
}

pub async fn download(
    store: &dyn ObjectStore,
    location: &S3Location,
    filter: &KeyFilter,
    dest: &Path,
) -> anyhow::Result<DownloadSummary> {
    todo!("{store:p} {location:?} {filter:?} {dest:?}")
}
