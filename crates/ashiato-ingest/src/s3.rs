use std::ops::Range;

use ashiato_pipeline::{EventTime, Manifest, ManifestEntry, PartitionManifest};
use chrono::{Days, TimeDelta};

use crate::local::{ListScope, arrange, is_excluded_directory, is_log_file, parse_file_name};

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

#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    #[error("invalid S3 URI {uri}: {reason}")]
    InvalidUri { uri: String, reason: &'static str },
    #[error(
        "the S3 prefix {prefix} is below AWSLogs/ or AWSLogs/<org-id>/; \
         point source.uri at one of them and use accounts and regions instead"
    )]
    BelowTrailRoot { prefix: String },
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
