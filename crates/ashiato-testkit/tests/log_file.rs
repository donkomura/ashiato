use std::fs::File;
use std::io::Read;
use std::path::Path;

use ashiato_testkit::log_file::{LogFile, write_log_file};
use ashiato_testkit::record::RecordBuilder;
use chrono::{TimeZone, Utc};
use flate2::read::GzDecoder;
use serde_json::{Value, json};

fn read_gzip_json(path: &Path) -> Value {
    let mut content = String::new();
    GzDecoder::new(File::open(path).unwrap())
        .read_to_string(&mut content)
        .unwrap();
    serde_json::from_str(&content).unwrap()
}

#[test]
fn account_trail_file_is_written_under_the_cloudtrail_key_layout() {
    let root = tempfile::tempdir().unwrap();
    let records = vec![RecordBuilder::new().build(), RecordBuilder::new().build()];
    let log = LogFile {
        unique: "u1".to_owned(),
        records: records.clone(),
        ..LogFile::new(
            "111111111111",
            "us-east-1",
            Utc.with_ymd_and_hms(2026, 10, 1, 9, 5, 0).unwrap(),
        )
    };

    let path = write_log_file(root.path(), &log);

    assert_eq!(
        path,
        root.path().join(
            "AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01/\
             111111111111_CloudTrail_us-east-1_20261001T0905Z_u1.json.gz"
        )
    );
    assert_eq!(read_gzip_json(&path), json!({ "Records": records }));
}

#[test]
fn organization_trail_file_is_written_under_the_organization_id() {
    let root = tempfile::tempdir().unwrap();
    let log = LogFile {
        org_id: Some("o-abcdef1234".to_owned()),
        unique: "u2".to_owned(),
        ..LogFile::new(
            "222222222222",
            "ap-northeast-1",
            Utc.with_ymd_and_hms(2026, 1, 2, 23, 55, 0).unwrap(),
        )
    };

    let path = write_log_file(root.path(), &log);

    assert_eq!(
        path,
        root.path().join(
            "AWSLogs/o-abcdef1234/222222222222/CloudTrail/ap-northeast-1/2026/01/02/\
             222222222222_CloudTrail_ap-northeast-1_20260102T2355Z_u2.json.gz"
        )
    );
    assert_eq!(read_gzip_json(&path), json!({ "Records": [] }));
}
