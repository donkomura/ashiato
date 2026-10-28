use std::ops::Range;

use ashiato_pipeline::{EventTime, Manifest};

use crate::local::ListScope;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Location {
    pub bucket: String,
    pub prefix: String,
}

impl S3Location {
    pub fn parse(_uri: &str) -> Result<Self, S3Error> {
        todo!()
    }
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

pub fn trail_root(_prefix: &str) -> Result<TrailRoot, S3Error> {
    todo!()
}

pub fn day_prefixes(_period: &Range<EventTime>) -> Vec<String> {
    todo!()
}

pub fn arrange_objects(
    _bucket: &str,
    _objects: Vec<S3Object>,
    _scope: &ListScope<'_>,
) -> S3Listing {
    todo!()
}
