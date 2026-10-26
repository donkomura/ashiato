use std::fs::{self, File};
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use ashiato_core::record::RecordBuilder;
use ashiato_duckdb::{EventStore, IngestSummary};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const LOG_DIR: &str = "AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01";

struct Ingested {
    _dir: TempDir,
    summary: IngestSummary,
    db: duckdb::Connection,
}

fn write_log(root: &Path, relative: &str, records: &[Value]) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut encoder = GzEncoder::new(File::create(path).unwrap(), Compression::default());
    encoder
        .write_all(json!({ "Records": records }).to_string().as_bytes())
        .unwrap();
    encoder.finish().unwrap();
}

fn ingest(write_logs: impl FnOnce(&Path)) -> Ingested {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("source");
    fs::create_dir(&source).unwrap();
    write_logs(&source);
    let db_path: PathBuf = dir.path().join("ashiato.duckdb");
    let summary = {
        let store = EventStore::open(&db_path).unwrap();
        store.ingest_directory(&source).unwrap()
    };
    let db = duckdb::Connection::open(&db_path).unwrap();
    Ingested {
        _dir: dir,
        summary,
        db,
    }
}

fn ingest_records(records: &[Value]) -> Ingested {
    ingest(|source| write_log(source, &format!("{LOG_DIR}/log.json.gz"), records))
}

fn column<T: duckdb::types::FromSql>(db: &duckdb::Connection, column: &str, event_id: &str) -> T {
    db.query_row(
        &format!("SELECT {column} FROM events WHERE event_id = ?"),
        [event_id],
        |row| row.get(0),
    )
    .unwrap_or_else(|error| panic!("failed to read {column} of {event_id}: {error}"))
}

fn count(db: &duckdb::Connection, table: &str) -> i64 {
    db.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
        row.get(0)
    })
    .unwrap()
}

fn assumed_role_record(event_id: &str) -> Value {
    RecordBuilder::new()
        .set("eventID", event_id)
        .set("userIdentity.type", "AssumedRole")
        .set("userIdentity.principalId", "AROAEXAMPLEROLE:alice-session")
        .set(
            "userIdentity.arn",
            "arn:aws:sts::111111111111:assumed-role/Deployer/alice-session",
        )
        .set("userIdentity.accessKeyId", "ASIAEXAMPLESESSION")
        .set("userIdentity.userName", Value::Null)
        .set(
            "userIdentity.sessionContext.sessionIssuer.arn",
            "arn:aws:iam::111111111111:role/Deployer",
        )
        .set(
            "userIdentity.sessionContext.attributes.creationDate",
            "2026-10-01T09:00:00Z",
        )
        .set(
            "userIdentity.sessionContext.attributes.mfaAuthenticated",
            "true",
        )
        .build()
}

#[test]
fn ingest_maps_cloudtrail_fields_to_event_columns() {
    let record = RecordBuilder::new()
        .set("eventID", "evt-columns")
        .set("eventTime", "2026-10-01T12:34:56Z")
        .set("eventSource", "s3.amazonaws.com")
        .set("eventName", "DeleteObject")
        .set("eventCategory", "Data")
        .set("readOnly", false)
        .set("awsRegion", "ap-northeast-1")
        .set("recipientAccountId", "222222222222")
        .set("sourceIPAddress", "203.0.113.7")
        .set("userAgent", "aws-sdk-go/1.0")
        .set("errorCode", "AccessDenied")
        .set("userIdentity.invokedBy", "cloudformation.amazonaws.com")
        .set("sessionCredentialFromConsole", "true")
        .set(
            "requestParameters",
            json!({ "bucketName": "evidence", "key": "a.txt" }),
        )
        .set("responseElements", json!({ "deleted": true }))
        .set("additionalEventData", json!({ "bytesTransferredIn": 0 }))
        .set(
            "resources",
            json!([{ "type": "AWS::S3::Object", "ARN": "arn:aws:s3:::evidence/a.txt" }]),
        )
        .build();
    let Ingested { db, .. } = ingest_records(&[record]);
    let id = "evt-columns";

    assert_eq!(
        column::<String>(&db, "CAST(event_time AS VARCHAR)", id),
        "2026-10-01 12:34:56"
    );
    assert_eq!(
        column::<String>(&db, "event_source", id),
        "s3.amazonaws.com"
    );
    assert_eq!(column::<String>(&db, "event_name", id), "DeleteObject");
    assert_eq!(column::<String>(&db, "event_category", id), "Data");
    assert_eq!(column::<Option<bool>>(&db, "read_only", id), Some(false));
    assert_eq!(column::<String>(&db, "aws_region", id), "ap-northeast-1");
    assert_eq!(
        column::<String>(&db, "recipient_account_id", id),
        "222222222222"
    );
    assert_eq!(column::<String>(&db, "source_ip", id), "203.0.113.7");
    assert_eq!(column::<String>(&db, "user_agent", id), "aws-sdk-go/1.0");
    assert_eq!(column::<String>(&db, "error_code", id), "AccessDenied");
    assert_eq!(column::<String>(&db, "identity_type", id), "IAMUser");
    assert_eq!(
        column::<String>(&db, "principal_arn", id),
        "arn:aws:iam::111111111111:user/alice"
    );
    assert_eq!(
        column::<String>(&db, "principal_id", id),
        "AIDAEXAMPLEPRINCIPAL"
    );
    assert_eq!(
        column::<String>(&db, "principal_account_id", id),
        "111111111111"
    );
    assert_eq!(
        column::<String>(&db, "access_key_id", id),
        "AKIAEXAMPLEACCESSKEY"
    );
    assert_eq!(
        column::<String>(&db, "invoked_by", id),
        "cloudformation.amazonaws.com"
    );
    assert_eq!(
        column::<Option<bool>>(&db, "credential_from_console", id),
        Some(true)
    );

    let json_column = |name: &str| -> Value {
        serde_json::from_str(&column::<String>(
            &db,
            &format!("CAST({name} AS VARCHAR)"),
            id,
        ))
        .unwrap()
    };
    assert_eq!(
        json_column("request_parameters"),
        json!({ "bucketName": "evidence", "key": "a.txt" })
    );
    assert_eq!(json_column("response_elements"), json!({ "deleted": true }));
    assert_eq!(
        json_column("additional_event_data"),
        json!({ "bytesTransferredIn": 0 })
    );
    assert_eq!(
        json_column("resources"),
        json!([{ "type": "AWS::S3::Object", "ARN": "arn:aws:s3:::evidence/a.txt" }])
    );
}

#[test]
fn ingest_maps_session_context_of_assumed_role() {
    let Ingested { db, .. } = ingest_records(&[assumed_role_record("evt-role")]);
    let id = "evt-role";

    assert_eq!(column::<String>(&db, "identity_type", id), "AssumedRole");
    assert_eq!(
        column::<String>(&db, "session_issuer_arn", id),
        "arn:aws:iam::111111111111:role/Deployer"
    );
    assert_eq!(column::<String>(&db, "session_name", id), "alice-session");
    assert_eq!(
        column::<String>(&db, "CAST(session_created_at AS VARCHAR)", id),
        "2026-10-01 09:00:00"
    );
    assert_eq!(
        column::<Option<bool>>(&db, "mfa_authenticated", id),
        Some(true)
    );
}

#[test]
fn session_name_is_null_unless_identity_is_assumed_role() {
    let iam_user = RecordBuilder::new().set("eventID", "evt-user").build();
    let federated_user = RecordBuilder::new()
        .set("eventID", "evt-federated")
        .set("userIdentity.type", "FederatedUser")
        .set(
            "userIdentity.arn",
            "arn:aws:sts::111111111111:federated-user/bob",
        )
        .set(
            "userIdentity.sessionContext.sessionIssuer.arn",
            "arn:aws:iam::111111111111:user/bob",
        )
        .build();
    let Ingested { db, .. } = ingest_records(&[iam_user, federated_user]);

    assert_eq!(
        column::<Option<String>>(&db, "session_name", "evt-user"),
        None
    );
    assert_eq!(
        column::<Option<String>>(&db, "session_name", "evt-federated"),
        None
    );
}

#[test]
fn principal_key_is_derived_from_identity_type() {
    let iam_user = RecordBuilder::new().set("eventID", "evt-iam-user").build();
    let root = RecordBuilder::new()
        .set("eventID", "evt-root")
        .set(
            "userIdentity",
            json!({
                "type": "Root",
                "principalId": "222222222222",
                "accountId": "222222222222",
            }),
        )
        .build();
    let assumed_role = assumed_role_record("evt-assumed-role");
    let federated_user = RecordBuilder::new()
        .set("eventID", "evt-federated-user")
        .set(
            "userIdentity",
            json!({
                "type": "FederatedUser",
                "principalId": "111111111111:bob",
                "arn": "arn:aws:sts::111111111111:federated-user/bob",
                "accountId": "111111111111",
                "sessionContext": {
                    "sessionIssuer": {
                        "type": "IAMUser",
                        "arn": "arn:aws:iam::111111111111:user/bob",
                    },
                },
            }),
        )
        .build();
    let aws_service = RecordBuilder::new()
        .set("eventID", "evt-aws-service")
        .set(
            "userIdentity",
            json!({ "type": "AWSService", "invokedBy": "ec2.amazonaws.com" }),
        )
        .build();
    let aws_account = RecordBuilder::new()
        .set("eventID", "evt-aws-account")
        .set(
            "userIdentity",
            json!({
                "type": "AWSAccount",
                "principalId": "AIDAEXTERNAL",
                "accountId": "333333333333",
            }),
        )
        .build();
    let other_with_arn = RecordBuilder::new()
        .set("eventID", "evt-other-with-arn")
        .set(
            "userIdentity",
            json!({
                "type": "IdentityCenterUser",
                "principalId": "identitystore-user",
                "arn": "arn:aws:identitystore::111111111111:user/carol",
                "accountId": "111111111111",
            }),
        )
        .build();
    let other_without_arn = RecordBuilder::new()
        .set("eventID", "evt-other-without-arn")
        .set(
            "userIdentity",
            json!({ "type": "Unknown", "principalId": "anonymous", "arn": "" }),
        )
        .build();
    let Ingested { db, .. } = ingest_records(&[
        iam_user,
        root,
        assumed_role,
        federated_user,
        aws_service,
        aws_account,
        other_with_arn,
        other_without_arn,
    ]);

    let expected = [
        ("evt-iam-user", "arn:aws:iam::111111111111:user/alice"),
        ("evt-root", "arn:aws:iam::222222222222:root"),
        (
            "evt-assumed-role",
            "arn:aws:iam::111111111111:role/Deployer",
        ),
        ("evt-federated-user", "arn:aws:iam::111111111111:user/bob"),
        ("evt-aws-service", "service:ec2.amazonaws.com"),
        ("evt-aws-account", "account:333333333333"),
        (
            "evt-other-with-arn",
            "arn:aws:identitystore::111111111111:user/carol",
        ),
        ("evt-other-without-arn", "unknown:anonymous"),
    ];
    for (event_id, principal_key) in expected {
        assert_eq!(
            column::<String>(&db, "principal_key", event_id),
            principal_key,
            "principal_key of {event_id}"
        );
    }
}

#[test]
fn ingest_accepts_records_of_different_shapes_in_one_file() {
    let records = [
        RecordBuilder::new()
            .set("eventID", "evt-shape-1")
            .set("requestParameters", json!({ "bucketName": "evidence" }))
            .build(),
        RecordBuilder::new()
            .set("eventID", "evt-shape-2")
            .set(
                "requestParameters",
                json!({ "instancesSet": { "items": [{ "imageId": "ami-1" }] }, "minCount": 1 }),
            )
            .build(),
        RecordBuilder::new()
            .set("eventID", "evt-shape-3")
            .set("requestParameters", Value::Null)
            .set(
                "userIdentity",
                json!({ "type": "AWSService", "invokedBy": "s3.amazonaws.com" }),
            )
            .build(),
        assumed_role_record("evt-shape-4"),
    ];
    let Ingested { db, summary, .. } = ingest_records(&records);

    assert_eq!(
        summary,
        IngestSummary {
            files: 1,
            skipped_files: 0,
            events: 4,
        }
    );
    assert_eq!(count(&db, "events"), 4);
    assert_eq!(count(&db, "events_raw"), 4);
    assert_eq!(
        column::<Option<String>>(&db, "CAST(request_parameters AS VARCHAR)", "evt-shape-3"),
        None
    );
    assert_eq!(
        column::<String>(
            &db,
            "json_extract_string(request_parameters, '$.instancesSet.items[0].imageId')",
            "evt-shape-2"
        ),
        "ami-1"
    );
}

#[test]
fn ingest_reads_a_log_file_larger_than_16_mib() {
    let padding = "x".repeat(1024 * 1024);
    let records: Vec<Value> = (0..20)
        .map(|i| {
            RecordBuilder::new()
                .set("eventID", format!("evt-large-{i}"))
                .set("requestParameters", json!({ "padding": padding }))
                .build()
        })
        .collect();

    let Ingested { db, summary, .. } = ingest_records(&records);

    assert_eq!(summary.events, 20);
    assert_eq!(count(&db, "events"), 20);
}

#[test]
fn events_raw_keeps_original_record() {
    let record = assumed_role_record("evt-raw");
    let Ingested { db, .. } = ingest_records(std::slice::from_ref(&record));

    let raw: String = db
        .query_row(
            "SELECT CAST(raw AS VARCHAR) FROM events_raw WHERE event_id = ?",
            ["evt-raw"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(serde_json::from_str::<Value>(&raw).unwrap(), record);
}

#[test]
fn ingest_reads_log_files_recursively_and_skips_digest_and_insight() {
    let Ingested { db, summary, .. } = ingest(|source| {
        let record = |id: &str| RecordBuilder::new().set("eventID", id).build();
        write_log(source, &format!("{LOG_DIR}/a.json.gz"), &[record("evt-a")]);
        write_log(
            source,
            "AWSLogs/222222222222/CloudTrail/ap-northeast-1/2026/10/02/b.json.gz",
            &[record("evt-b1"), record("evt-b2")],
        );
        write_log(
            source,
            "AWSLogs/111111111111/CloudTrail-Digest/us-east-1/2026/10/01/digest.json.gz",
            &[record("evt-digest")],
        );
        write_log(
            source,
            "AWSLogs/111111111111/CloudTrail-Insight/us-east-1/2026/10/01/insight.json.gz",
            &[record("evt-insight")],
        );
        fs::write(source.join(LOG_DIR).join("notes.txt"), "not a log").unwrap();
    });

    assert_eq!(
        summary,
        IngestSummary {
            files: 2,
            skipped_files: 0,
            events: 3,
        }
    );
    let mut ids: Vec<String> = db
        .prepare("SELECT event_id FROM events ORDER BY event_id")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    ids.sort();
    assert_eq!(ids, ["evt-a", "evt-b1", "evt-b2"]);
}

#[test]
fn ingest_of_directory_without_logs_creates_empty_tables() {
    let Ingested { db, summary, .. } = ingest(|_| {});

    assert_eq!(
        summary,
        IngestSummary {
            files: 0,
            skipped_files: 0,
            events: 0,
        }
    );
    assert_eq!(count(&db, "events"), 0);
    assert_eq!(count(&db, "events_raw"), 0);
}

fn event_ids(db: &duckdb::Connection, table: &str) -> Vec<String> {
    db.prepare(&format!("SELECT event_id FROM {table} ORDER BY event_id"))
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[test]
fn ingesting_same_directory_twice_does_not_change_row_counts() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("source");
    let record = |id: &str| RecordBuilder::new().set("eventID", id).build();
    write_log(
        &source,
        &format!("{LOG_DIR}/log.json.gz"),
        &[record("evt-1"), record("evt-2")],
    );
    let db_path = dir.path().join("ashiato.duckdb");

    let store = EventStore::open(&db_path).unwrap();
    let first = store.ingest_directory(&source).unwrap();
    let second = store.ingest_directory(&source).unwrap();
    drop(store);

    assert_eq!(
        first,
        IngestSummary {
            files: 1,
            skipped_files: 0,
            events: 2,
        }
    );
    assert_eq!(
        second,
        IngestSummary {
            files: 0,
            skipped_files: 1,
            events: 0,
        }
    );
    let db = duckdb::Connection::open(&db_path).unwrap();
    assert_eq!(count(&db, "events"), 2);
    assert_eq!(count(&db, "events_raw"), 2);
    assert_eq!(count(&db, "ingested_files"), 1);
}

#[test]
fn file_with_same_content_under_different_name_is_skipped() {
    let Ingested { db, summary, .. } = ingest(|source| {
        let record = RecordBuilder::new().set("eventID", "evt-copied").build();
        write_log(source, &format!("{LOG_DIR}/a.json.gz"), &[record]);
        fs::copy(
            source.join(LOG_DIR).join("a.json.gz"),
            source.join(LOG_DIR).join("b.json.gz"),
        )
        .unwrap();
    });

    assert_eq!(
        summary,
        IngestSummary {
            files: 1,
            skipped_files: 1,
            events: 1,
        }
    );
    assert_eq!(count(&db, "events"), 1);
    assert_eq!(count(&db, "ingested_files"), 1);
}

#[test]
fn same_event_id_in_different_files_is_stored_once() {
    let Ingested { db, summary, .. } = ingest(|source| {
        let record = |id: &str| RecordBuilder::new().set("eventID", id).build();
        write_log(
            source,
            "AWSLogs/o-example/111111111111/CloudTrail/us-east-1/2026/10/01/org.json.gz",
            &[record("evt-shared"), record("evt-org-only")],
        );
        write_log(
            source,
            &format!("{LOG_DIR}/account.json.gz"),
            &[record("evt-shared"), record("evt-account-only")],
        );
    });

    assert_eq!(
        summary,
        IngestSummary {
            files: 2,
            skipped_files: 0,
            events: 3,
        }
    );
    let expected = ["evt-account-only", "evt-org-only", "evt-shared"];
    assert_eq!(event_ids(&db, "events"), expected);
    assert_eq!(event_ids(&db, "events_raw"), expected);
}

#[test]
fn ingest_with_several_jobs_stores_each_file_and_event_once() {
    let dir = TempDir::new().unwrap();
    let source = dir.path().join("source");
    let record = |id: String| RecordBuilder::new().set("eventID", id).build();
    for file in 0..8 {
        write_log(
            &source,
            &format!("{LOG_DIR}/log-{file}.json.gz"),
            &[
                record(format!("evt-{file}")),
                record(format!("evt-shared-{}", file % 2)),
            ],
        );
    }
    for copy in 0..4 {
        fs::copy(
            source.join(LOG_DIR).join("log-0.json.gz"),
            source.join(LOG_DIR).join(format!("copy-{copy}.json.gz")),
        )
        .unwrap();
    }
    let db_path = dir.path().join("ashiato.duckdb");

    let summary = EventStore::open(&db_path)
        .unwrap()
        .ingest_directory_with_jobs(&source, NonZeroUsize::new(4).unwrap())
        .unwrap();

    assert_eq!(
        summary,
        IngestSummary {
            files: 8,
            skipped_files: 4,
            events: 10,
        }
    );
    let db = duckdb::Connection::open(&db_path).unwrap();
    assert_eq!(count(&db, "events"), 10);
    assert_eq!(count(&db, "events_raw"), 10);
    assert_eq!(count(&db, "ingested_files"), 8);
}

#[test]
fn same_event_id_within_one_file_is_stored_once() {
    let record = RecordBuilder::new().set("eventID", "evt-repeated").build();
    let Ingested { db, summary, .. } = ingest_records(&[record.clone(), record]);

    assert_eq!(summary.events, 1);
    assert_eq!(event_ids(&db, "events"), ["evt-repeated"]);
    assert_eq!(event_ids(&db, "events_raw"), ["evt-repeated"]);
}

#[test]
fn ingested_files_records_hash_uri_size_and_time() {
    let mut log_path = PathBuf::new();
    let Ingested { _dir, db, .. } = ingest(|source| {
        write_log(
            source,
            &format!("{LOG_DIR}/log.json.gz"),
            &[RecordBuilder::new().build()],
        );
        log_path = source.join(LOG_DIR).join("log.json.gz");
    });
    let bytes = fs::read(&log_path).unwrap();
    let expected_sha256: String = Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();

    let (sha256, uri, size, has_ingested_at): (String, String, i64, bool) = db
        .query_row(
            "SELECT sha256, uri, size, ingested_at IS NOT NULL FROM ingested_files",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(sha256, expected_sha256);
    assert_eq!(uri, log_path.to_string_lossy());
    assert_eq!(size, i64::try_from(bytes.len()).unwrap());
    assert!(has_ingested_at);
}
