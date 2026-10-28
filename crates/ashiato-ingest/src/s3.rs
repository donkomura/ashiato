use std::ops::Range;
use std::time::Duration;

use ashiato_pipeline::{EventTime, Manifest, ManifestEntry, PartitionManifest};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use chrono::{Days, TimeDelta};

use crate::local::{ListScope, arrange, is_excluded_directory, is_log_file, parse_file_name};
use crate::worker::{FileSource, SourceError};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const SCHEME: &str = "s3://";
const LOG_ROOT: &str = "AWSLogs";
const ORGANIZATION_PREFIX: &str = "o-";
const DAY_PREFIX_FORMAT: &str = "%Y/%m/%d/";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Location {
    pub bucket: String,
    pub prefix: String,
}

impl S3Location {
    pub fn parse(uri: &str) -> Result<Self, S3Error> {
        let invalid = |reason| S3Error::InvalidUri {
            uri: uri.to_owned(),
            reason,
        };
        let path = uri
            .strip_prefix(SCHEME)
            .ok_or_else(|| invalid("expected the s3:// scheme"))?;
        let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
        if bucket.is_empty() {
            return Err(invalid("no bucket"));
        }
        Ok(Self {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
        })
    }
}

fn object_uri(bucket: &str, key: &str) -> String {
    format!("{SCHEME}{bucket}/{key}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrailRoot {
    AwsLogs(String),
    Organization(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct S3Object {
    pub key: String,
    pub version_id: Option<String>,
    pub etag: Option<String>,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Listing {
    pub manifest: Manifest,
    pub unrecognized: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Listing {
    pub prefixes: Vec<String>,
    pub keys: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        todo!()
    }
}

pub struct S3Source {
    client: Client,
    retry: RetryPolicy,
}

impl S3Source {
    pub async fn connect(_bucket: &str, _endpoint_url: Option<&str>) -> Self {
        todo!()
    }

    pub fn from_conf(_config: aws_sdk_s3::Config) -> Self {
        todo!()
    }

    pub fn with_retry(self, retry: RetryPolicy) -> Self {
        Self { retry, ..self }
    }

    pub async fn list(
        &self,
        _location: &S3Location,
        _scope: &ListScope<'_>,
    ) -> Result<S3Listing, S3Error> {
        let _ = (&self.client, self.retry);
        todo!()
    }

    pub async fn list_delimited(&self, _bucket: &str, _prefix: &str) -> Result<Listing, S3Error> {
        todo!()
    }
}

#[async_trait]
impl FileSource for S3Source {
    async fn read(&self, _entry: &ManifestEntry) -> Result<Vec<u8>, SourceError> {
        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    #[error("invalid S3 URI {uri}: {reason}")]
    InvalidUri { uri: String, reason: &'static str },
    #[error(
        "the S3 prefix {prefix} is below AWSLogs/ or AWSLogs/<org-id>/; \
         point source.uri at one of them and use accounts and regions instead"
    )]
    BelowTrailRoot { prefix: String },
    #[error("{operation} {target} failed")]
    Request {
        operation: &'static str,
        target: String,
        #[source]
        source: BoxError,
    },
    #[error("{operation} {target} failed {attempts} times")]
    RetriesExhausted {
        operation: &'static str,
        target: String,
        attempts: u32,
        #[source]
        source: BoxError,
    },
    #[error("{uri} was listed but no longer exists")]
    Missing { uri: String },
    #[error("{uri} changed after it was listed: the ETag was {expected} but is now {actual:?}")]
    EtagMismatch {
        uri: String,
        expected: String,
        actual: Option<String>,
    },
}

// CloudTrail delivers to <bucket>/[<key prefix>/]AWSLogs/[<org-id>/]<account>/CloudTrail/....
// https://docs.aws.amazon.com/awscloudtrail/latest/userguide/get-and-view-cloudtrail-log-files.html
pub fn trail_root(prefix: &str) -> Result<TrailRoot, S3Error> {
    let segments: Vec<&str> = prefix
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    let Some(root) = segments.iter().position(|segment| *segment == LOG_ROOT) else {
        let mut root: String = segments
            .iter()
            .map(|segment| format!("{segment}/"))
            .collect();
        root.push_str(LOG_ROOT);
        root.push('/');
        return Ok(TrailRoot::AwsLogs(root));
    };
    let joined = |count: usize| -> String {
        segments[..count]
            .iter()
            .map(|segment| format!("{segment}/"))
            .collect()
    };
    match &segments[root + 1..] {
        [] => Ok(TrailRoot::AwsLogs(joined(root + 1))),
        [organization] if organization.starts_with(ORGANIZATION_PREFIX) => {
            Ok(TrailRoot::Organization(joined(root + 2)))
        }
        _ => Err(S3Error::BelowTrailRoot {
            prefix: prefix.to_owned(),
        }),
    }
}

pub fn day_prefixes(period: &Range<EventTime>) -> Vec<String> {
    if period.is_empty() {
        return Vec::new();
    }
    let last = (period.end - TimeDelta::microseconds(1)).date_naive();
    let mut day = period.start.date_naive();
    let mut prefixes = Vec::new();
    while day <= last {
        prefixes.push(day.format(DAY_PREFIX_FORMAT).to_string());
        day = day + Days::new(1);
    }
    prefixes
}

pub fn arrange_objects(bucket: &str, objects: Vec<S3Object>, scope: &ListScope<'_>) -> S3Listing {
    let mut found = Vec::new();
    let mut unrecognized = Vec::new();
    for object in objects {
        let (directories, name) = object.key.rsplit_once('/').unwrap_or(("", &object.key));
        if !is_log_file(name) || directories.split('/').any(is_excluded_directory) {
            continue;
        }
        match parse_file_name(name) {
            Some(name) => found.push((name, object)),
            None => unrecognized.push(object_uri(bucket, &object.key)),
        }
    }
    unrecognized.sort();
    let partitions = arrange(scope, found)
        .into_iter()
        .map(|(partition, files)| PartitionManifest {
            partition,
            files: (0..)
                .zip(files)
                .map(|(file_seq, (file_time, object))| ManifestEntry {
                    file_seq,
                    uri: object_uri(bucket, &object.key),
                    version_id: object.version_id,
                    etag: object.etag,
                    size: object.size,
                    file_time,
                })
                .collect(),
        })
        .collect();
    S3Listing {
        manifest: Manifest { partitions },
        unrecognized,
    }
}

impl From<S3Error> for SourceError {
    fn from(error: S3Error) -> Self {
        Self::S3(Box::new(error))
    }
}
