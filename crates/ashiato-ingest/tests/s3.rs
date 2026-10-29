use std::fs;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use ashiato_ingest::{
    DEFAULT_PREFETCH_FILES, FileSource, ListScope, PartitionAssignment, RetryPolicy, S3Error,
    S3Listing, S3Location, S3Source, SourceError, WorkerError, WorkerSettings, run_worker,
};
use ashiato_pipeline::{
    EventTime, PartitionId, PartitionManifest, PartitionMessage, PartitionReader, Position,
    partition_channels,
};
use ashiato_testkit::log_file::{LogFile, write_log_file};
use ashiato_testkit::minio::{ACCESS_KEY, Minio, REGION, SECRET_KEY};
use ashiato_testkit::record::RecordBuilder;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use chrono::{TimeDelta, TimeZone, Utc};
use serde_json::Value;
use tempfile::TempDir;

const BUCKET: &str = "trail-bucket";
const ORG_ID: &str = "o-abcdef1234";

fn at(day: u32, hour: u32, minute: u32) -> EventTime {
    Utc.with_ymd_and_hms(2026, 10, day, hour, minute, 0)
        .unwrap()
}

fn source(minio: &Minio) -> S3Source {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(REGION))
        .endpoint_url(minio.endpoint())
        .force_path_style(true)
        .credentials_provider(Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test"))
        .build();
    S3Source::from_conf(config).with_retry(RetryPolicy {
        max_attempts: 2,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(10),
    })
}

fn location(prefix: &str) -> S3Location {
    S3Location::parse(&format!("s3://{BUCKET}/{prefix}")).unwrap()
}

fn first_days() -> ListScope<'static> {
    ListScope {
        accounts: None,
        regions: None,
        period: at(1, 0, 0)..at(3, 0, 0),
    }
}

fn records(ids: &[&str]) -> Vec<Value> {
    ids.iter()
        .map(|id| RecordBuilder::new().set("eventID", *id).build())
        .collect()
}

fn log(
    org_id: Option<&str>,
    account: &str,
    region: &str,
    time: EventTime,
    ids: &[&str],
) -> LogFile {
    LogFile {
        org_id: org_id.map(str::to_owned),
        unique: ids.join("-"),
        records: records(ids),
        ..LogFile::new(account, region, time)
    }
}

struct Trail {
    dir: TempDir,
}

impl Trail {
    fn new() -> Self {
        Self {
            dir: TempDir::new().unwrap(),
        }
    }

    fn root(&self) -> PathBuf {
        self.dir.path().join("bucket")
    }

    fn write(&self, log: &LogFile) -> String {
        write_log_file(&self.root(), log);
        key(log)
    }

    fn write_other(&self, key: &str) {
        let path = self.root().join(key);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, b"not a cloudtrail log").unwrap();
    }

    fn size(&self, key: &str) -> u64 {
        fs::metadata(self.root().join(key)).unwrap().len()
    }

    // Writes the log outside the uploaded tree, so that it can replace an object after listing.
    fn replacement(&self, log: &LogFile) -> PathBuf {
        let root = self.dir.path().join("replacement");
        write_log_file(&root, log)
    }
}

fn key(log: &LogFile) -> String {
    log.relative_path().to_str().unwrap().to_owned()
}

fn uploaded(minio: &Minio, trail: &Trail, versioned: bool) {
    minio.create_bucket(BUCKET, versioned);
    minio.upload_dir(&trail.root(), BUCKET);
}

fn listed_keys(listing: &S3Listing) -> Vec<(PartitionId, Vec<String>)> {
    listing
        .manifest
        .partitions
        .iter()
        .map(|partition| {
            (
                partition.partition.clone(),
                partition
                    .files
                    .iter()
                    .map(|file| {
                        file.uri
                            .strip_prefix(&format!("s3://{BUCKET}/"))
                            .unwrap()
                            .to_owned()
                    })
                    .collect(),
            )
        })
        .collect()
}

async fn ingest(
    source: S3Source,
    manifest: PartitionManifest,
) -> (Vec<PartitionMessage>, Result<(), WorkerError>) {
    let partition = manifest.partition.clone();
    let (writer, mut reader) = partition_channels([partition.clone()], 4);
    let worker = tokio::spawn(run_worker(
        vec![PartitionAssignment {
            index: 0,
            manifest,
            start: Position::START,
        }],
        Arc::new(source) as Arc<dyn FileSource>,
        Arc::new(writer),
        WorkerSettings {
            allowed_lateness: TimeDelta::minutes(30),
            max_message_size: 1 << 20,
            decode_threads: NonZeroUsize::MIN,
            prefetch_files: DEFAULT_PREFETCH_FILES,
        },
        Arc::default(),
    ));
    let mut messages = Vec::new();
    loop {
        let deliveries = reader.fetch(&partition, 16).await.unwrap();
        if deliveries.is_empty() {
            break;
        }
        messages.extend(deliveries.into_iter().map(|delivery| delivery.message));
        if matches!(
            messages.last(),
            Some(PartitionMessage::EndOfPartition { .. })
        ) {
            break;
        }
    }
    (messages, worker.await.unwrap())
}

fn event_ids(messages: &[PartitionMessage]) -> Vec<String> {
    messages
        .iter()
        .filter_map(|message| match message {
            PartitionMessage::FileChunk { events, .. } => Some(events),
            _ => None,
        })
        .flat_map(|events| {
            events
                .column_by_name("event_id")
                .unwrap()
                .as_string::<i32>()
                .iter()
                .map(|id| id.unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn single_partition(listing: S3Listing) -> PartitionManifest {
    let mut partitions = listing.manifest.partitions;
    assert_eq!(partitions.len(), 1, "{partitions:#?}");
    partitions.remove(0)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker for MinIO; see docs/s3-testing.md"]
async fn listing_finds_the_accounts_and_regions_of_the_organization_and_account_trails() {
    let minio = Minio::start();
    let trail = Trail::new();
    let org_first = trail.write(&log(
        Some(ORG_ID),
        "111111111111",
        "us-east-1",
        at(1, 10, 0),
        &["o1"],
    ));
    let account_next = trail.write(&log(
        None,
        "111111111111",
        "us-east-1",
        at(1, 10, 5),
        &["a1"],
    ));
    let other = trail.write(&log(
        Some(ORG_ID),
        "222222222222",
        "ap-northeast-1",
        at(2, 0, 0),
        &["o2"],
    ));
    trail.write(&log(
        None,
        "111111111111",
        "us-east-1",
        at(3, 0, 0),
        &["after"],
    ));
    trail.write_other(
        "AWSLogs/111111111111/CloudTrail-Digest/us-east-1/2026/10/01/\
         111111111111_CloudTrail_us-east-1_20261001T1001Z_digest.json.gz",
    );
    let notes = "AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01/notes.json.gz";
    trail.write_other(notes);
    uploaded(&minio, &trail, true);
    let source = source(&minio);

    let listing = source
        .list(&location("AWSLogs/"), &first_days())
        .await
        .unwrap();

    assert_eq!(
        listed_keys(&listing),
        [
            (
                PartitionId::new("111111111111", "us-east-1"),
                vec![org_first.clone(), account_next.clone()]
            ),
            (
                PartitionId::new("222222222222", "ap-northeast-1"),
                vec![other.clone()]
            ),
        ]
    );
    assert_eq!(listing.unrecognized, [format!("s3://{BUCKET}/{notes}")]);
    let first = &listing.manifest.partitions[0].files[0];
    assert_eq!(first.size, trail.size(&org_first));
    assert!(first.etag.as_deref().is_some_and(|etag| !etag.is_empty()));
    assert!(
        first
            .version_id
            .as_deref()
            .is_some_and(|version| !version.is_empty() && version != "null"),
        "{first:?}"
    );

    let organization = source
        .list(&location(&format!("AWSLogs/{ORG_ID}/")), &first_days())
        .await
        .unwrap();
    assert_eq!(
        listed_keys(&organization),
        [
            (
                PartitionId::new("111111111111", "us-east-1"),
                vec![org_first.clone()]
            ),
            (
                PartitionId::new("222222222222", "ap-northeast-1"),
                vec![other]
            ),
        ]
    );

    let accounts = ["111111111111".to_owned()];
    let regions = ["us-east-1".to_owned(), "eu-west-1".to_owned()];
    let given = source
        .list(
            &location(""),
            &ListScope {
                accounts: Some(&accounts),
                regions: Some(&regions),
                ..first_days()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        listed_keys(&given),
        [
            (PartitionId::new("111111111111", "eu-west-1"), vec![]),
            (
                PartitionId::new("111111111111", "us-east-1"),
                vec![org_first, account_next]
            ),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker for MinIO; see docs/s3-testing.md"]
async fn listing_a_bucket_without_versioning_records_no_version() {
    let minio = Minio::start();
    let trail = Trail::new();
    trail.write(&log(
        None,
        "111111111111",
        "us-east-1",
        at(1, 10, 0),
        &["a1"],
    ));
    uploaded(&minio, &trail, false);

    let listing = source(&minio)
        .list(&location("AWSLogs/"), &first_days())
        .await
        .unwrap();

    let file = &single_partition(listing).files[0];
    assert_eq!(file.version_id, None, "{file:?}");
    assert!(file.etag.is_some(), "{file:?}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker for MinIO; see docs/s3-testing.md"]
async fn a_versioned_object_is_read_at_the_listed_version() {
    let minio = Minio::start();
    let trail = Trail::new();
    let listed = log(
        None,
        "111111111111",
        "us-east-1",
        at(1, 10, 0),
        &["evt-listed"],
    );
    let key = trail.write(&listed);
    uploaded(&minio, &trail, true);
    let source = source(&minio);
    let manifest = single_partition(
        source
            .list(&location("AWSLogs/"), &first_days())
            .await
            .unwrap(),
    );
    let replacement = trail.replacement(&LogFile {
        records: records(&["evt-newer-1", "evt-newer-2"]),
        ..listed
    });
    minio.put_file(&replacement, BUCKET, &key);

    let (messages, result) = ingest(source, manifest.clone()).await;

    result.unwrap();
    assert_eq!(event_ids(&messages), ["evt-listed"]);
    let PartitionMessage::FileChunk { file, .. } = &messages[0] else {
        panic!("{messages:#?}");
    };
    assert_eq!(file.version_id, manifest.files[0].version_id);
    assert_eq!(file.etag, manifest.files[0].etag);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker for MinIO; see docs/s3-testing.md"]
async fn an_object_replaced_after_listing_stops_the_worker_without_versioning() {
    let minio = Minio::start();
    let trail = Trail::new();
    let listed = log(
        None,
        "111111111111",
        "us-east-1",
        at(1, 10, 0),
        &["evt-listed"],
    );
    let key = trail.write(&listed);
    uploaded(&minio, &trail, false);
    let source = source(&minio);
    let manifest = single_partition(
        source
            .list(&location("AWSLogs/"), &first_days())
            .await
            .unwrap(),
    );
    let replacement = trail.replacement(&LogFile {
        records: records(&["evt-newer"]),
        ..listed
    });
    minio.put_file(&replacement, BUCKET, &key);

    let (messages, result) = ingest(source, manifest).await;

    let error = result.unwrap_err();
    assert!(
        matches!(
            &error,
            WorkerError::Source {
                file_seq: 0,
                source: SourceError::S3(error),
                ..
            } if matches!(**error, S3Error::EtagMismatch { .. })
        ),
        "{error:?}"
    );
    assert!(messages.is_empty(), "{messages:#?}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker for MinIO; see docs/s3-testing.md"]
async fn an_object_deleted_after_listing_stops_the_worker() {
    let minio = Minio::start();
    let trail = Trail::new();
    let key = trail.write(&log(
        None,
        "111111111111",
        "us-east-1",
        at(1, 10, 0),
        &["evt-listed"],
    ));
    uploaded(&minio, &trail, false);
    let source = source(&minio);
    let manifest = single_partition(
        source
            .list(&location("AWSLogs/"), &first_days())
            .await
            .unwrap(),
    );
    minio.remove(BUCKET, &key);

    let (messages, result) = ingest(source, manifest).await;

    let error = result.unwrap_err();
    assert!(
        matches!(
            &error,
            WorkerError::Source {
                source: SourceError::S3(error),
                ..
            } if matches!(**error, S3Error::Missing { .. })
        ),
        "{error:?}"
    );
    assert!(messages.is_empty(), "{messages:#?}");
}
