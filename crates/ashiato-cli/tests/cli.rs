use std::fs::{self, File};
use std::io::Write;
use std::process::Command;

use ashiato_core::record::RecordBuilder;
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::json;
use tempfile::TempDir;

fn ashiato() -> Command {
    Command::new(env!("CARGO_BIN_EXE_ashiato"))
}

#[test]
fn version_flag_prints_package_version() {
    let output = ashiato().arg("--version").output().unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("ashiato {}\n", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn ingest_creates_workspace_and_loads_local_logs() {
    let dir = TempDir::new().unwrap();
    let source = dir
        .path()
        .join("source/AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01");
    fs::create_dir_all(&source).unwrap();
    let mut encoder = GzEncoder::new(
        File::create(source.join("log.json.gz")).unwrap(),
        Compression::default(),
    );
    let records = [
        RecordBuilder::new().set("eventID", "evt-1").build(),
        RecordBuilder::new().set("eventID", "evt-2").build(),
    ];
    encoder
        .write_all(json!({ "Records": records }).to_string().as_bytes())
        .unwrap();
    encoder.finish().unwrap();
    let workspace = dir.path().join("cases/case-1");

    let output = ashiato()
        .arg("ingest")
        .arg("--workspace")
        .arg(&workspace)
        .arg("--source")
        .arg(dir.path().join("source"))
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let db = duckdb::Connection::open(workspace.join("ashiato.duckdb")).unwrap();
    let events: i64 = db
        .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(events, 2);
}

#[test]
fn ingest_rejects_s3_source_as_unsupported() {
    let dir = TempDir::new().unwrap();

    let output = ashiato()
        .arg("ingest")
        .arg("--workspace")
        .arg(dir.path().join("case"))
        .arg("--source")
        .arg("s3://trail-bucket/AWSLogs/")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("not supported"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
