use std::io::Write;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ashiato_ingest::{
    DEFAULT_PREFETCH_FILES, FileSource, PartitionAssignment, RetryPolicy, S3Error, S3Source,
    SourceError, WorkerError, WorkerSettings, run_worker,
};
use ashiato_pipeline::{
    ManifestEntry, PartitionId, PartitionManifest, PartitionMessage, PartitionReader, Position,
    partition_channels,
};
use ashiato_testkit::record::RecordBuilder;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use chrono::{TimeDelta, TimeZone, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const ETAG: &str = "\"0123456789abcdef0123456789abcdef\"";
const KEY: &str = "AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01/\
                   111111111111_CloudTrail_us-east-1_20261001T1005Z_a.json.gz";
const INITIAL_BACKOFF: Duration = Duration::from_millis(20);
const SLOW_DOWN: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
    <Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>";

// Answers the first `failures` requests with 503 SlowDown and the rest with the object, so
// that the retries of the S3 source can be observed without an S3 compatible server.
struct SlowDownServer {
    endpoint: String,
    requests: Arc<Mutex<Vec<Instant>>>,
}

impl SlowDownServer {
    async fn start(failures: usize, object: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut head = Vec::new();
                let mut buffer = [0; 4096];
                while !head.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = stream.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    head.extend_from_slice(&buffer[..read]);
                }
                let count = {
                    let mut requests = recorded.lock().unwrap();
                    requests.push(Instant::now());
                    requests.len()
                };
                let (status, headers, body) = if count <= failures {
                    (
                        "503 Slow Down",
                        "Content-Type: application/xml\r\n".to_owned(),
                        SLOW_DOWN.as_bytes().to_vec(),
                    )
                } else {
                    (
                        "200 OK",
                        format!("Content-Type: application/octet-stream\r\nETag: {ETAG}\r\n"),
                        object.clone(),
                    )
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });
        Self { endpoint, requests }
    }

    fn requests(&self) -> Vec<Instant> {
        self.requests.lock().unwrap().clone()
    }
}

fn source(endpoint: &str, max_attempts: u32) -> S3Source {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .credentials_provider(Credentials::new("test", "test-secret", None, None, "test"))
        .build();
    S3Source::from_conf(config).with_retry(RetryPolicy {
        max_attempts,
        initial_backoff: INITIAL_BACKOFF,
        max_backoff: Duration::from_secs(1),
    })
}

fn log_object() -> Vec<u8> {
    let record = RecordBuilder::new().set("eventID", "evt-1").build();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(json!({ "Records": [record] }).to_string().as_bytes())
        .unwrap();
    encoder.finish().unwrap()
}

fn assignment(size: u64) -> PartitionAssignment {
    PartitionAssignment {
        index: 0,
        manifest: PartitionManifest {
            partition: PartitionId::new("111111111111", "us-east-1"),
            files: vec![ManifestEntry {
                file_seq: 0,
                uri: format!("s3://trail-bucket/{KEY}"),
                version_id: None,
                etag: Some(ETAG.to_owned()),
                size,
                file_time: Utc.with_ymd_and_hms(2026, 10, 1, 10, 5, 0).unwrap(),
            }],
        },
        start: Position::START,
    }
}

async fn ingest(source: S3Source, size: u64) -> (Vec<PartitionMessage>, Result<(), WorkerError>) {
    let assignment = assignment(size);
    let partition = assignment.manifest.partition.clone();
    let (writer, mut reader) = partition_channels([partition.clone()], 4);
    let worker = tokio::spawn(run_worker(
        vec![assignment],
        Arc::new(source) as Arc<dyn FileSource>,
        Arc::new(writer),
        WorkerSettings {
            allowed_lateness: TimeDelta::minutes(30),
            max_message_size: 1 << 20,
            decode_threads: NonZeroUsize::MIN,
            prefetch_files: DEFAULT_PREFETCH_FILES,
        },
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

#[tokio::test(flavor = "multi_thread")]
async fn slow_down_is_retried_with_growing_intervals_until_the_object_is_read() {
    let object = log_object();
    let server = SlowDownServer::start(3, object.clone()).await;

    let (messages, result) = ingest(source(&server.endpoint, 4), object.len() as u64).await;

    result.unwrap();
    assert!(
        matches!(
            &messages[..],
            [
                PartitionMessage::FileChunk {
                    record_count: 1,
                    ..
                },
                PartitionMessage::EndOfPartition { .. }
            ]
        ),
        "{messages:#?}"
    );
    let requests = server.requests();
    assert_eq!(requests.len(), 4);
    let intervals: Vec<Duration> = requests.windows(2).map(|pair| pair[1] - pair[0]).collect();
    for (retry, interval) in (0..).zip(&intervals) {
        assert!(
            *interval >= INITIAL_BACKOFF * 2u32.pow(retry),
            "{intervals:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_down_beyond_the_retry_limit_stops_the_worker() {
    let object = log_object();
    let server = SlowDownServer::start(usize::MAX, object.clone()).await;

    let (messages, result) = ingest(source(&server.endpoint, 3), object.len() as u64).await;

    let error = result.unwrap_err();
    assert!(
        matches!(
            &error,
            WorkerError::Source {
                file_seq: 0,
                source: SourceError::S3(error),
                ..
            } if matches!(**error, S3Error::RetriesExhausted { attempts: 3, .. })
        ),
        "{error:?}"
    );
    assert_eq!(server.requests().len(), 3);
    assert!(
        !messages
            .iter()
            .any(|message| matches!(message, PartitionMessage::FileError { .. })),
        "a failed request is not a broken file: {messages:#?}"
    );
}
