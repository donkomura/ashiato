use std::ops::Range;
use std::time::Duration;

use ashiato_pipeline::{EventTime, Manifest, ManifestEntry, PartitionManifest};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};
use chrono::{Days, TimeDelta};
use futures_util::{StreamExt, TryStreamExt, stream};

use crate::local::{
    ListScope, arrange, is_account_id, is_excluded_directory, is_log_file, is_region,
    parse_file_name,
};
use crate::worker::{FileSource, SourceError};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const SCHEME: &str = "s3://";
const LOG_ROOT: &str = "AWSLogs";
const ORGANIZATION_PREFIX: &str = "o-";
const DAY_PREFIX_FORMAT: &str = "%Y/%m/%d/";
const SERVICE_DIRECTORY: &str = "CloudTrail/";
const DELIMITER: &str = "/";
const CONCURRENT_REQUESTS: usize = 16;
const DEFAULT_REGION: &str = "us-east-1";
const LEGACY_EU_LOCATION: &str = "EU";
const LEGACY_EU_REGION: &str = "eu-west-1";
// S3 reports the version of an object written while versioning was never enabled as "null".
const NULL_VERSION: &str = "null";
const NOT_FOUND: u16 = 404;
const PRECONDITION_FAILED: u16 = 412;
const TOO_MANY_REQUESTS: u16 = 429;
const RETRYABLE_CODES: [&str; 4] = [
    "SlowDown",
    "RequestTimeout",
    "Throttling",
    "ThrottlingException",
];

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
        Self {
            max_attempts: 10,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(20),
        }
    }
}

impl RetryPolicy {
    fn backoff(&self, retry: u32) -> Duration {
        self.initial_backoff
            .saturating_mul(2u32.saturating_pow(retry))
            .min(self.max_backoff)
    }
}

pub struct S3Source {
    client: Client,
    retry: RetryPolicy,
}

enum Failure {
    Retryable(BoxError),
    Final(S3Error),
}

impl S3Source {
    pub async fn connect(bucket: &str, endpoint_url: Option<&str>) -> Self {
        let region = RegionProviderChain::default_provider().or_else(DEFAULT_REGION);
        let mut loader = aws_config::defaults(BehaviorVersion::latest()).region(region);
        if let Some(url) = endpoint_url {
            loader = loader.endpoint_url(url);
        }
        let shared = loader.load().await;
        // S3 compatible storages such as MinIO address a bucket by the path, not the host name.
        let builder =
            aws_sdk_s3::config::Builder::from(&shared).force_path_style(endpoint_url.is_some());
        let source = Self::from_conf(builder.clone().build());
        match bucket_region(&source.client, bucket).await {
            Some(region) => Self::from_conf(builder.region(Region::new(region)).build()),
            None => source,
        }
    }

    // The SDK's own retries are turned off, so that the retries of RetryPolicy also cover a
    // failure while reading the body and the number of attempts is exactly max_attempts.
    pub fn from_conf(config: aws_sdk_s3::Config) -> Self {
        let config = config
            .to_builder()
            .retry_config(RetryConfig::disabled())
            .build();
        Self {
            client: Client::from_conf(config),
            retry: RetryPolicy::default(),
        }
    }

    pub fn with_retry(self, retry: RetryPolicy) -> Self {
        Self { retry, ..self }
    }

    // Lists the date prefixes of the period for each account and region under source.uri, and
    // finds the accounts and regions from the prefixes when they are not given (4.1 節, 6.1 節).
    pub async fn list(
        &self,
        location: &S3Location,
        scope: &ListScope<'_>,
    ) -> Result<S3Listing, S3Error> {
        let bucket = &location.bucket;
        let roots = match trail_root(&location.prefix)? {
            TrailRoot::Organization(root) => vec![root],
            TrailRoot::AwsLogs(root) => {
                let listing = self.list_delimited(bucket, &root).await?;
                let organizations: Vec<String> = subdirectories(&root, &listing.prefixes)
                    .filter(|name| name.starts_with(ORGANIZATION_PREFIX))
                    .map(|name| format!("{root}{name}/"))
                    .collect();
                std::iter::once(root).chain(organizations).collect()
            }
        };
        let accounts = self
            .children(bucket, roots, scope.accounts, is_account_id)
            .await?;
        let services = accounts
            .into_iter()
            .map(|account| format!("{account}{SERVICE_DIRECTORY}"))
            .collect();
        let regions = self
            .children(bucket, services, scope.regions, is_region)
            .await?;
        let days = day_prefixes(&scope.period);
        let prefixes: Vec<String> = regions
            .iter()
            .flat_map(|region| days.iter().map(move |day| format!("{region}{day}")))
            .collect();
        let objects: Vec<Vec<S3Object>> = stream::iter(&prefixes)
            .map(|prefix| self.list_objects(bucket, prefix))
            .buffer_unordered(CONCURRENT_REQUESTS)
            .try_collect()
            .await?;
        Ok(arrange_objects(bucket, objects.concat(), scope))
    }

    async fn children(
        &self,
        bucket: &str,
        parents: Vec<String>,
        given: Option<&[String]>,
        valid: fn(&str) -> bool,
    ) -> Result<Vec<String>, S3Error> {
        if let Some(given) = given {
            return Ok(parents
                .iter()
                .flat_map(|parent| given.iter().map(move |name| format!("{parent}{name}/")))
                .collect());
        }
        let listings: Vec<(String, Listing)> = stream::iter(parents)
            .map(|parent| async move {
                let listing = self.list_delimited(bucket, &parent).await?;
                Ok::<_, S3Error>((parent, listing))
            })
            .buffer_unordered(CONCURRENT_REQUESTS)
            .try_collect()
            .await?;
        Ok(listings
            .iter()
            .flat_map(|(parent, listing)| {
                subdirectories(parent, &listing.prefixes)
                    .filter(|name| valid(name))
                    .map(move |name| format!("{parent}{name}/"))
            })
            .collect())
    }

    pub async fn list_delimited(&self, bucket: &str, prefix: &str) -> Result<Listing, S3Error> {
        let target = object_uri(bucket, prefix);
        let mut listing = Listing::default();
        let mut token: Option<String> = None;
        loop {
            let page = self
                .retrying(&target, || async {
                    self.client
                        .list_objects_v2()
                        .bucket(bucket)
                        .prefix(prefix)
                        .delimiter(DELIMITER)
                        .set_continuation_token(token.clone())
                        .send()
                        .await
                        .map_err(|error| request_failure("ListObjectsV2", &target, &error))
                })
                .await?;
            listing.prefixes.extend(
                page.common_prefixes()
                    .iter()
                    .filter_map(|common| common.prefix().map(str::to_owned)),
            );
            listing.keys.extend(
                page.contents()
                    .iter()
                    .filter_map(|object| object.key().map(str::to_owned)),
            );
            token = page.next_continuation_token().map(str::to_owned);
            if page.is_truncated() != Some(true) || token.is_none() {
                return Ok(listing);
            }
        }
    }

    // ListObjectVersions returns the VersionId with the ETag and the size at the price of a
    // LIST request per 1000 objects, while ListObjectsV2 would need a HeadObject per file to
    // learn the VersionId (5.4 節).
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html
    async fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<S3Object>, S3Error> {
        let target = object_uri(bucket, prefix);
        let mut objects = Vec::new();
        let mut key_marker: Option<String> = None;
        let mut version_marker: Option<String> = None;
        loop {
            let page = self
                .retrying(&target, || async {
                    self.client
                        .list_object_versions()
                        .bucket(bucket)
                        .prefix(prefix)
                        .delimiter(DELIMITER)
                        .set_key_marker(key_marker.clone())
                        .set_version_id_marker(version_marker.clone())
                        .send()
                        .await
                        .map_err(|error| request_failure("ListObjectVersions", &target, &error))
                })
                .await?;
            // A key whose latest version is a delete marker has no latest version here, so a
            // deleted log is not listed.
            objects.extend(
                page.versions()
                    .iter()
                    .filter(|version| version.is_latest() == Some(true))
                    .filter_map(|version| {
                        Some(S3Object {
                            key: version.key()?.to_owned(),
                            version_id: version
                                .version_id()
                                .filter(|id| *id != NULL_VERSION)
                                .map(str::to_owned),
                            etag: version.e_tag().map(str::to_owned),
                            size: version
                                .size()
                                .and_then(|size| u64::try_from(size).ok())
                                .unwrap_or(0),
                        })
                    }),
            );
            key_marker = page.next_key_marker().map(str::to_owned);
            version_marker = page.next_version_id_marker().map(str::to_owned);
            if page.is_truncated() != Some(true) || key_marker.is_none() {
                return Ok(objects);
            }
        }
    }

    async fn get(&self, entry: &ManifestEntry) -> Result<Vec<u8>, S3Error> {
        let uri = &entry.uri;
        let location = S3Location::parse(uri)?;
        let changed = |actual: Option<&str>| {
            Failure::Final(S3Error::EtagMismatch {
                uri: uri.clone(),
                expected: entry.etag.clone().unwrap_or_default(),
                actual: actual.map(str::to_owned),
            })
        };
        self.retrying(uri, || async {
            // If-Match makes S3 refuse a changed object before sending its body. The ETag of the
            // response is still compared, in case a storage ignores the header.
            let output = self
                .client
                .get_object()
                .bucket(&location.bucket)
                .key(&location.prefix)
                .set_version_id(entry.version_id.clone())
                .set_if_match(entry.etag.clone())
                .send()
                .await
                .map_err(|error| match status(&error) {
                    Some(NOT_FOUND) => Failure::Final(S3Error::Missing { uri: uri.clone() }),
                    Some(PRECONDITION_FAILED) => changed(None),
                    _ => request_failure("GetObject", uri, &error),
                })?;
            if entry.etag.is_some() && output.e_tag() != entry.etag.as_deref() {
                return Err(changed(output.e_tag()));
            }
            let body = output
                .body
                .collect()
                .await
                .map_err(|error| Failure::Retryable(error.into()))?;
            Ok(body.into_bytes().to_vec())
        })
        .await
    }

    async fn retrying<T, F, Fut>(&self, target: &str, mut attempt: F) -> Result<T, S3Error>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, Failure>>,
    {
        let mut attempts = 0;
        loop {
            attempts += 1;
            match attempt().await {
                Ok(value) => return Ok(value),
                Err(Failure::Final(error)) => return Err(error),
                Err(Failure::Retryable(source)) if attempts >= self.retry.max_attempts => {
                    return Err(S3Error::RetriesExhausted {
                        target: target.to_owned(),
                        attempts,
                        source,
                    });
                }
                Err(Failure::Retryable(_)) => {
                    tokio::time::sleep(self.retry.backoff(attempts - 1)).await;
                }
            }
        }
    }
}

#[async_trait]
impl FileSource for S3Source {
    async fn read(&self, entry: &ManifestEntry) -> Result<Vec<u8>, SourceError> {
        Ok(self.get(entry).await?)
    }
}

// Trail buckets often live in a region other than the investigator's default, and S3
// rejects requests sent to the wrong region. Without s3:GetBucketLocation, the
// configured region is used as is.
async fn bucket_region(client: &Client, bucket: &str) -> Option<String> {
    let output = client
        .get_bucket_location()
        .bucket(bucket)
        .send()
        .await
        .ok()?;
    let region = match output
        .location_constraint()
        .map(|location| location.as_str())
    {
        None | Some("") => DEFAULT_REGION,
        Some(LEGACY_EU_LOCATION) => LEGACY_EU_REGION,
        Some(region) => region,
    };
    Some(region.to_owned())
}

fn subdirectories<'a>(parent: &'a str, prefixes: &'a [String]) -> impl Iterator<Item = &'a str> {
    prefixes.iter().filter_map(move |prefix| {
        prefix
            .strip_prefix(parent)?
            .strip_suffix('/')
            .filter(|name| !name.is_empty() && !name.contains('/'))
    })
}

fn status<E>(error: &SdkError<E, HttpResponse>) -> Option<u16> {
    match error {
        SdkError::ServiceError(service) => Some(service.raw().status().as_u16()),
        _ => None,
    }
}

// S3 asks clients to retry 503 SlowDown and other server errors with growing intervals.
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html#optimizing-performance-timeouts-retries
fn request_failure<E>(
    operation: &'static str,
    target: &str,
    error: &SdkError<E, HttpResponse>,
) -> Failure
where
    E: ProvideErrorMetadata + std::error::Error + 'static,
{
    let message: BoxError = DisplayErrorContext(error).to_string().into();
    let retryable = match error {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => {
            true
        }
        SdkError::ServiceError(service) => {
            let status = service.raw().status().as_u16();
            status >= 500
                || status == TOO_MANY_REQUESTS
                || service
                    .err()
                    .code()
                    .is_some_and(|code| RETRYABLE_CODES.contains(&code))
        }
        _ => false,
    };
    if retryable {
        Failure::Retryable(message)
    } else {
        Failure::Final(S3Error::Request {
            operation,
            target: target.to_owned(),
            source: message,
        })
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
    #[error("requests for {target} failed {attempts} times")]
    RetriesExhausted {
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
