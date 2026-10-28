use std::io::Write;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::RecordBatch;
use ashiato_duckdb::EventStore;
use ashiato_ingest::{
    DEFAULT_PREFETCH_FILES, FileSource, LocalSource, PartitionAssignment, WorkerSettings,
    run_worker,
};
use ashiato_pipeline::{
    EVENT_COLUMNS, ManifestEntry, PartitionId, PartitionManifest, PartitionMessage,
    PartitionReader, Position, partition_channels,
};
use ashiato_testkit::record::RecordBuilder;
use chrono::{TimeDelta, TimeZone, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use tempfile::TempDir;

const JSON_COLUMNS: [&str; 4] = [
    "request_parameters",
    "response_elements",
    "additional_event_data",
    "resources",
];

fn record(id: &str) -> RecordBuilder {
    RecordBuilder::new().set("eventID", id)
}

fn identity(id: &str, identity: Value) -> Value {
    let mut record = record(id).build();
    record["userIdentity"] = identity;
    record
}

fn varied_records() -> Vec<Value> {
    let mut records = vec![
        record("iam-user").build(),
        identity(
            "root",
            json!({ "type": "Root", "principalId": "111111111111", "arn": "arn:aws:iam::111111111111:root", "accountId": "111111111111" }),
        ),
        identity(
            "assumed-role",
            json!({
                "type": "AssumedRole",
                "principalId": "AROAEXAMPLE:session-1",
                "arn": "arn:aws:sts::111111111111:assumed-role/Admin/session-1",
                "accountId": "111111111111",
                "accessKeyId": "ASIAEXAMPLE",
                "sessionContext": {
                    "sessionIssuer": { "type": "Role", "arn": "arn:aws:iam::111111111111:role/Admin" },
                    "attributes": { "creationDate": "2026-10-01T00:00:00Z", "mfaAuthenticated": "true" }
                }
            }),
        ),
        identity(
            "assumed-role-short-arn",
            json!({ "type": "AssumedRole", "arn": "arn:aws:sts::111111111111:assumed-role", "sessionContext": { "attributes": { "mfaAuthenticated": "false" } } }),
        ),
        identity(
            "federated-user",
            json!({ "type": "FederatedUser", "arn": "arn:aws:sts::111111111111:federated-user/bob", "sessionContext": { "sessionIssuer": { "arn": "arn:aws:iam::111111111111:user/bob" } } }),
        ),
        identity(
            "aws-service",
            json!({ "type": "AWSService", "invokedBy": "lambda.amazonaws.com" }),
        ),
        identity(
            "aws-account",
            json!({ "type": "AWSAccount", "principalId": "AIDAEXAMPLE", "accountId": "222222222222" }),
        ),
        identity(
            "unknown-without-arn",
            json!({ "type": "SAMLUser", "principalId": "saml-principal" }),
        ),
        identity(
            "unknown-with-empty-arn",
            json!({ "type": "WebIdentityUser", "principalId": "web-principal", "arn": "" }),
        ),
        identity("unknown-without-anything", json!({ "type": "Unknown" })),
        identity("no-identity", Value::Null),
        record("fractional-time")
            .set("eventTime", "2026-10-01T00:00:00.123Z")
            .build(),
        record("objects")
            .set(
                "requestParameters",
                json!({ "b": 1, "a": { "z": 2, "y": [1, 2.5, "三"] }, "flag": true }),
            )
            .set("responseElements", Value::Null)
            .set("additionalEventData", json!({ "MFAUsed": "No" }))
            .set(
                "resources",
                json!([{ "ARN": "arn:aws:s3:::bucket", "type": "AWS::S3::Bucket" }]),
            )
            .set("errorCode", "AccessDenied")
            .set("userAgent", "aws-cli/2.0 \"quoted\" \\ backslash")
            .build(),
        record("number-as-string-field")
            .set("recipientAccountId", 111111111111_u64)
            .build(),
    ];
    for (index, value) in [
        json!(true),
        json!(false),
        json!("true"),
        json!("FALSE"),
        json!("t"),
        json!("1"),
        json!("0"),
        json!("yes"),
        json!("no"),
        json!("nope"),
        json!(1),
        json!(""),
    ]
    .into_iter()
    .enumerate()
    {
        records.push(
            record(&format!("boolean-{index}"))
                .set("readOnly", value.clone())
                .set("sessionCredentialFromConsole", value)
                .build(),
        );
    }
    records
}

fn write_gzip(dir: &TempDir, name: &str, records: &[Value]) -> std::path::PathBuf {
    let path = dir.path().join(name);
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(json!({ "Records": records }).to_string().as_bytes())
        .unwrap();
    std::fs::write(&path, encoder.finish().unwrap()).unwrap();
    path
}

async fn worker_batches(path: &std::path::Path) -> Vec<RecordBatch> {
    let partition = PartitionId::new("111111111111", "us-east-1");
    let assignment = PartitionAssignment {
        index: 0,
        manifest: PartitionManifest {
            partition: partition.clone(),
            files: vec![ManifestEntry {
                file_seq: 0,
                uri: path.to_string_lossy().into_owned(),
                version_id: None,
                etag: None,
                size: 0,
                file_time: Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap(),
            }],
        },
        start: Position::START,
    };
    let (writer, mut reader) = partition_channels([partition.clone()], 64);
    run_worker(
        vec![assignment],
        Arc::new(LocalSource) as Arc<dyn FileSource>,
        Arc::new(writer),
        WorkerSettings {
            allowed_lateness: TimeDelta::minutes(30),
            max_message_size: 1 << 20,
            decode_threads: NonZeroUsize::MIN,
            prefetch_files: DEFAULT_PREFETCH_FILES,
        },
    )
    .await
    .unwrap();
    let mut batches = Vec::new();
    for delivery in reader.fetch(&partition, 64).await.unwrap() {
        match delivery.message {
            PartitionMessage::FileChunk { events, .. } => batches.push(events),
            PartitionMessage::EndOfPartition { .. } => {}
            other => panic!("expected chunks, but got {other:?}"),
        }
    }
    batches
}

fn column_definitions() -> String {
    EVENT_COLUMNS
        .iter()
        .map(|column| {
            let column_type = match *column {
                "event_time" | "session_created_at" => "TIMESTAMP",
                "read_only" | "mfa_authenticated" | "credential_from_console" => "BOOLEAN",
                "file_id" | "record_index" => "BIGINT",
                _ => "VARCHAR",
            };
            format!("{column} {column_type}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn comparable_columns() -> String {
    EVENT_COLUMNS
        .iter()
        .filter(|column| !matches!(**column, "file_id" | "record_index"))
        .map(|column| {
            if JSON_COLUMNS.contains(column) {
                format!("CAST(CAST({column} AS JSON) AS VARCHAR) AS {column}")
            } else {
                (*column).to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[tokio::test(flavor = "multi_thread")]
async fn the_worker_normalizes_records_like_the_batch_ingest() {
    let dir = TempDir::new().unwrap();
    let logs = TempDir::new().unwrap();
    let records = varied_records();
    let path = write_gzip(&logs, "a.json.gz", &records);
    let database = dir.path().join("batch.duckdb");
    let ingested = EventStore::open(&database)
        .unwrap()
        .ingest_directory(logs.path())
        .unwrap();
    assert_eq!(ingested.events, records.len());

    let batches = worker_batches(&path).await;

    let connection = duckdb::Connection::open(&database).unwrap();
    connection
        .execute_batch(&format!(
            "CREATE TABLE streamed ({});",
            column_definitions()
        ))
        .unwrap();
    {
        let mut appender = connection.appender("streamed").unwrap();
        for batch in batches {
            appender.append_record_batch(batch).unwrap();
        }
    }
    let columns = comparable_columns();
    let differences = |left: &str, right: &str| -> Vec<String> {
        let mut statement = connection
            .prepare(&format!(
                "SELECT to_json(d)::VARCHAR FROM (SELECT {columns} FROM {left} EXCEPT ALL SELECT {columns} FROM {right}) d ORDER BY 1"
            ))
            .unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    let count: usize = connection
        .query_row("SELECT count(*) FROM streamed", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, records.len());
    assert_eq!(differences("events", "streamed"), Vec::<String>::new());
    assert_eq!(differences("streamed", "events"), Vec::<String>::new());
}
