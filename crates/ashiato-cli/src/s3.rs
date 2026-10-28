use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context;
use ashiato_ingest::S3Source;
pub use ashiato_ingest::{Listing, S3Location};
use async_trait::async_trait;
use chrono::{Datelike, NaiveDate};
use futures_util::{StreamExt, TryStreamExt, stream};

const SCHEME: &str = "s3://";
const LOG_ROOT: &str = "AWSLogs";
const ORGANIZATION_PREFIX: &str = "o-";
const SERVICE: &str = "CloudTrail";
const LOG_SUFFIX: &str = ".json.gz";
const PARTIAL_SUFFIX: &str = ".part";
const ACCOUNT_LEVEL: usize = 0;
const SERVICE_LEVEL: usize = 1;
const REGION_LEVEL: usize = 2;
const YEAR_LEVEL: usize = 3;
const DAY_LEVEL: usize = 5;
const CONCURRENT_REQUESTS: usize = 16;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyFilter {
    pub accounts: Vec<String>,
    pub regions: Vec<String>,
    pub from: Option<NaiveDate>,
    pub to: Option<NaiveDate>,
}

impl KeyFilter {
    pub fn is_unrestricted(&self) -> bool {
        self == &Self::default()
    }

    pub fn may_contain(&self, prefix: &str) -> bool {
        let Some((directories, _)) = prefix.rsplit_once('/') else {
            return true;
        };
        log_levels(directories).is_none_or(|levels| self.accepts(&levels))
    }

    pub fn matches(&self, key: &str) -> bool {
        let Some((directories, file)) = key.rsplit_once('/') else {
            return false;
        };
        let escapes = key
            .split('/')
            .any(|segment| matches!(segment, "" | "." | ".."));
        !escapes
            && file.ends_with(LOG_SUFFIX)
            && log_levels(directories)
                .is_some_and(|levels| levels.len() == DAY_LEVEL + 1 && self.accepts(&levels))
    }

    fn accepts(&self, levels: &[&str]) -> bool {
        levels
            .iter()
            .enumerate()
            .all(|(level, segment)| match level {
                ACCOUNT_LEVEL => {
                    self.accounts.is_empty() || self.accounts.iter().any(|a| a == segment)
                }
                SERVICE_LEVEL => *segment == SERVICE,
                REGION_LEVEL => {
                    self.regions.is_empty() || self.regions.iter().any(|r| r == segment)
                }
                YEAR_LEVEL..=DAY_LEVEL => self.accepts_date(&levels[YEAR_LEVEL..=level]),
                _ => false,
            })
    }

    fn accepts_date(&self, parts: &[&str]) -> bool {
        let Some(numbers) = parts
            .iter()
            .map(|part| {
                part.bytes()
                    .all(|byte| byte.is_ascii_digit())
                    .then(|| part.parse::<u32>().ok())
                    .flatten()
            })
            .collect::<Option<Vec<u32>>>()
        else {
            return false;
        };
        let valid = match numbers[..] {
            [year, month, day] => i32::try_from(year)
                .ok()
                .and_then(|year| NaiveDate::from_ymd_opt(year, month, day))
                .is_some(),
            [_, month] => (1..=12).contains(&month),
            _ => true,
        };
        let truncate = |date: NaiveDate| {
            let parts = [
                date.year().try_into().unwrap_or(0),
                date.month(),
                date.day(),
            ];
            parts[..numbers.len()].to_vec()
        };
        valid
            && self.from.is_none_or(|from| truncate(from) <= numbers)
            && self.to.is_none_or(|to| numbers <= truncate(to))
    }
}

fn log_levels(directories: &str) -> Option<Vec<&str>> {
    let segments: Vec<&str> = directories.split('/').collect();
    let root = segments.iter().position(|segment| *segment == LOG_ROOT)?;
    let mut levels = &segments[root + 1..];
    if levels
        .first()
        .is_some_and(|segment| segment.starts_with(ORGANIZATION_PREFIX))
    {
        levels = &levels[1..];
    }
    Some(levels.to_vec())
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
    fs::create_dir_all(dest).with_context(|| format!("failed to create {}", dest.display()))?;
    let (present, missing): (Vec<String>, Vec<String>) = list_logs(store, location, filter)
        .await?
        .into_iter()
        .partition(|key| dest.join(key).is_file());
    let summary = DownloadSummary {
        downloaded: missing.len(),
        skipped: present.len(),
    };
    stream::iter(missing)
        .map(|key| download_object(store, &location.bucket, key, dest))
        .buffer_unordered(CONCURRENT_REQUESTS)
        .try_collect::<Vec<()>>()
        .await?;
    Ok(summary)
}

async fn list_logs(
    store: &dyn ObjectStore,
    location: &S3Location,
    filter: &KeyFilter,
) -> anyhow::Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut prefixes = vec![location.prefix.clone()];
    while !prefixes.is_empty() {
        let listings: Vec<Listing> = stream::iter(&prefixes)
            .map(|prefix| async move {
                store
                    .list(&location.bucket, prefix)
                    .await
                    .with_context(|| format!("failed to list {SCHEME}{}/{prefix}", location.bucket))
            })
            .buffer_unordered(CONCURRENT_REQUESTS)
            .try_collect()
            .await?;
        prefixes.clear();
        for listing in listings {
            prefixes.extend(
                listing
                    .prefixes
                    .into_iter()
                    .filter(|prefix| filter.may_contain(prefix)),
            );
            keys.extend(listing.keys.into_iter().filter(|key| filter.matches(key)));
        }
    }
    Ok(keys)
}

async fn download_object(
    store: &dyn ObjectStore,
    bucket: &str,
    key: String,
    dest: &Path,
) -> anyhow::Result<()> {
    let body = store
        .get(bucket, &key)
        .await
        .with_context(|| format!("failed to download {SCHEME}{bucket}/{key}"))?;
    let path = dest.join(&key);
    // An interrupted download stays under the partial name, so a later run neither
    // skips it as already downloaded nor ingests it as a log.
    let mut partial = OsString::from(path.as_os_str());
    partial.push(PARTIAL_SUFFIX);
    let partial = PathBuf::from(partial);
    let write = || -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&partial, body)?;
        fs::rename(&partial, &path)
    };
    write().with_context(|| format!("failed to write {}", path.display()))
}

pub struct AwsObjectStore {
    source: S3Source,
}

impl AwsObjectStore {
    pub async fn connect(bucket: &str) -> anyhow::Result<Self> {
        Ok(Self {
            source: S3Source::connect(bucket, None).await,
        })
    }
}

#[async_trait]
impl ObjectStore for AwsObjectStore {
    async fn list(&self, bucket: &str, prefix: &str) -> anyhow::Result<Listing> {
        Ok(self.source.list_delimited(bucket, prefix).await?)
    }

    async fn get(&self, bucket: &str, key: &str) -> anyhow::Result<Vec<u8>> {
        Ok(self.source.get_latest(bucket, key).await?)
    }
}
