use std::cell::Cell;
use std::error::Error;
use std::fs;
use std::path::Path;

use ashiato_duckdb::{PipelineStore, StoreError};
use ashiato_ingest::{ListScope, list_local};
use ashiato_pipeline::{
    EventTime, Manifest, ManifestEntry, PartitionId, PartitionManifest, RunPlan,
};
use ashiato_testkit::log_file::{LogFile, write_log_file};
use chrono::{TimeZone, Utc};

const DIGEST: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000001";

type TestResult<T> = Result<T, Box<dyn Error>>;

fn at(day: u32, hour: u32, minute: u32) -> EventTime {
    Utc.with_ymd_and_hms(2026, 10, day, hour, minute, 0)
        .unwrap()
}

fn sample_manifest() -> Manifest {
    Manifest {
        partitions: vec![
            PartitionManifest {
                partition: PartitionId::new("111111111111", "eu-west-1"),
                files: vec![],
            },
            PartitionManifest {
                partition: PartitionId::new("111111111111", "us-east-1"),
                files: vec![
                    ManifestEntry {
                        file_seq: 0,
                        uri: "s3://trail-bucket/AWSLogs/111111111111/CloudTrail/us-east-1/2026/10/01/a.json.gz"
                            .to_owned(),
                        version_id: Some("3HL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY".to_owned()),
                        etag: Some("\"d41d8cd98f00b204e9800998ecf8427e\"".to_owned()),
                        size: 12_345_678_901,
                        file_time: at(1, 10, 0),
                    },
                    ManifestEntry {
                        file_seq: 1,
                        uri: "/logs/b.json.gz".to_owned(),
                        version_id: None,
                        etag: None,
                        size: 0,
                        file_time: at(1, 10, 5),
                    },
                ],
            },
            PartitionManifest {
                partition: PartitionId::new("222222222222", "us-east-1"),
                files: vec![],
            },
        ],
    }
}

fn plan(until: &str) -> RunPlan {
    RunPlan::parse(
        &format!(
            r#"
run_id: case-1
source:
  kind: local
  uri: logs
analysis_window:
  since: 2026-10-01T00:00:00Z
  until: {until}
pipeline:
  allowed_lateness: 30m
placement:
  mode: single_process
"#
        ),
        Path::new("/cases/case-1"),
    )
    .unwrap()
}

fn log(time: EventTime, unique: &str) -> LogFile {
    LogFile {
        unique: unique.to_owned(),
        ..LogFile::new("111111111111", "us-east-1", time)
    }
}

#[test]
fn first_start_saves_the_listed_manifest_and_returns_it() {
    let mut store = PipelineStore::open_in_memory().unwrap();

    let manifest: TestResult<Manifest> = store.start_run(DIGEST, || Ok(sample_manifest()));

    assert_eq!(manifest.unwrap(), sample_manifest());
}

#[test]
fn later_start_returns_the_saved_manifest_without_listing_again() {
    let mut store = PipelineStore::open_in_memory().unwrap();
    store
        .start_run::<StoreError>(DIGEST, || Ok(sample_manifest()))
        .unwrap();
    let listed = Cell::new(false);

    let manifest = store
        .start_run::<StoreError>(DIGEST, || {
            listed.set(true);
            Ok(Manifest::default())
        })
        .unwrap();

    assert_eq!(manifest, sample_manifest());
    assert!(!listed.get(), "the manifest was listed again");
}

#[test]
fn empty_manifest_is_saved_as_well() {
    let mut store = PipelineStore::open_in_memory().unwrap();
    store
        .start_run::<StoreError>(DIGEST, || Ok(Manifest::default()))
        .unwrap();

    let manifest = store
        .start_run::<StoreError>(DIGEST, || Ok(sample_manifest()))
        .unwrap();

    assert_eq!(manifest, Manifest::default());
}

#[test]
fn saved_manifest_does_not_follow_changes_in_the_directory() {
    let workspace = tempfile::tempdir().unwrap();
    let logs = tempfile::tempdir().unwrap();
    let database = workspace.path().join("ashiato.duckdb");
    let plan = plan("2026-10-02T00:00:00Z");
    let scope = ListScope::from_plan(&plan);
    write_log_file(logs.path(), &log(at(1, 10, 0), "a"));
    let removed = write_log_file(logs.path(), &log(at(1, 10, 5), "b"));
    let first = {
        let mut store = PipelineStore::open(&database).unwrap();
        let manifest: TestResult<Manifest> = store.start_run(plan.digest(), || {
            Ok(list_local(logs.path(), &scope)?.manifest)
        });
        manifest.unwrap()
    };
    assert_eq!(first.partitions[0].files.len(), 2);

    fs::remove_file(&removed).unwrap();
    write_log_file(logs.path(), &log(at(1, 9, 55), "c"));
    write_log_file(logs.path(), &log(at(1, 10, 10), "d"));
    let relisted = list_local(logs.path(), &scope).unwrap().manifest;
    assert_ne!(relisted, first);

    let mut store = PipelineStore::open(&database).unwrap();
    let resumed: TestResult<Manifest> = store.start_run(plan.digest(), || {
        Ok(list_local(logs.path(), &scope)?.manifest)
    });

    assert_eq!(resumed.unwrap(), first);
}

#[test]
fn start_with_a_different_run_plan_is_an_error() {
    let workspace = tempfile::tempdir().unwrap();
    let database = workspace.path().join("ashiato.duckdb");
    let started = plan("2026-10-02T00:00:00Z");
    let changed = plan("2026-10-03T00:00:00Z");
    assert_ne!(started.digest(), changed.digest());
    PipelineStore::open(&database)
        .unwrap()
        .start_run::<StoreError>(started.digest(), || Ok(sample_manifest()))
        .unwrap();
    let listed = Cell::new(false);

    let error = PipelineStore::open(&database)
        .unwrap()
        .start_run::<StoreError>(changed.digest(), || {
            listed.set(true);
            Ok(sample_manifest())
        })
        .unwrap_err();

    match &error {
        StoreError::DigestMismatch { stored, given } => {
            assert_eq!(stored, started.digest());
            assert_eq!(given, changed.digest());
        }
        other => panic!("unexpected error: {other:?}"),
    }
    let message = error.to_string();
    assert!(message.contains(started.digest()), "{message}");
    assert!(message.contains(changed.digest()), "{message}");
    assert!(
        !listed.get(),
        "the manifest was listed for a mismatching run plan"
    );
}

#[test]
fn failed_listing_saves_nothing() {
    let mut store = PipelineStore::open_in_memory().unwrap();
    let failed: TestResult<Manifest> = store.start_run(DIGEST, || Err("listing failed".into()));
    assert_eq!(failed.unwrap_err().to_string(), "listing failed");

    let manifest = store
        .start_run::<StoreError>(DIGEST, || Ok(sample_manifest()))
        .unwrap();

    assert_eq!(manifest, sample_manifest());
}

#[test]
fn the_memory_limit_is_applied_to_the_database() {
    let store = PipelineStore::open_in_memory().unwrap();

    store.set_memory_limit("1GiB").unwrap();

    assert_eq!(store.memory_limit().unwrap(), "1.0 GiB");
}

#[test]
fn the_memory_limit_stays_at_the_default_of_duckdb_unless_it_is_set() {
    let default: String = duckdb::Connection::open_in_memory()
        .unwrap()
        .query_row("SELECT current_setting('memory_limit')", [], |row| {
            row.get(0)
        })
        .unwrap();

    let store = PipelineStore::open_in_memory().unwrap();

    assert_eq!(store.memory_limit().unwrap(), default);
}

#[test]
fn an_invalid_memory_limit_is_an_error_of_duckdb() {
    let store = PipelineStore::open_in_memory().unwrap();
    let default = store.memory_limit().unwrap();

    for limit in ["two gigabytes", "1GB'; SET memory_limit = '1MB"] {
        let error = store.set_memory_limit(limit).unwrap_err();

        assert!(matches!(error, StoreError::DuckDb(_)), "{error:?}");
        assert_eq!(store.memory_limit().unwrap(), default);
    }
}
