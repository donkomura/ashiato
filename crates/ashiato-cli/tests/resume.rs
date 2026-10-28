use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use ashiato_duckdb::{Analyzer, AnalyzerSettings, PipelineStore, StoreError};
use ashiato_ingest::{
    FileSource, ListScope, LocalSource, PartitionAssignment, WorkerSettings, list_local, run_worker,
};
use ashiato_pipeline::{RunPlan, SourceLocation, partition_channels};
use ashiato_testkit::fixture::{Expectation, builtin_fixtures};
use ashiato_testkit::log_file::{LogFile, write_log_file};
use chrono::{DateTime, TimeDelta, Utc};
use serde_json::{Value, json};
use tempfile::TempDir;

const PLAN: &str = r#"
run_id: resume-1
source:
  kind: local
  uri: {logs}
analysis_window:
  since: 2026-10-01T00:00:00Z
  until: 2026-10-02T00:00:00Z
rules: [builtin]
pipeline:
  batch: 1h
  allowed_lateness: 30m
placement:
  mode: single_process
"#;

const SCENARIO_RULE_IDS: [&str; 4] = [
    "console_takeover_mining",
    "role_chain_escalation",
    "leaked_key_recon_persistence",
    "cross_account_s3_destruction",
];

const TABLES: [&str; 9] = [
    "signals",
    "lineage_edges",
    "baseline",
    "baseline_principals",
    "ingested_files",
    "late_events",
    "batches",
    "data_counters",
    "partition_progress",
];

// Each chunk holds a single record, so that the first run stops inside a file.
const MAX_MESSAGE_SIZE: usize = 4097;

fn event_time(record: &Value) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(record["eventTime"].as_str().unwrap())
        .unwrap()
        .with_timezone(&Utc)
}

// Every file of the account trail is also delivered by the organization trail.
fn write_logs(root: &Path) {
    let five_minutes = TimeDelta::minutes(5).num_seconds();
    let mut files: BTreeMap<(String, String, i64), Vec<Value>> = BTreeMap::new();
    let records = builtin_fixtures()
        .unwrap()
        .into_iter()
        .filter(|fixture| {
            fixture.expectation == Expectation::Positive
                && SCENARIO_RULE_IDS.contains(&fixture.rule_id.as_str())
        })
        .enumerate()
        .flat_map(|(fixture, file)| {
            file.records
                .into_iter()
                .enumerate()
                .map(move |(index, mut record)| {
                    record["eventID"] = json!(format!("resume-{fixture:04}-{index:04}"));
                    record
                })
        });
    for record in records {
        let file_time = (event_time(&record).timestamp() / five_minutes + 2) * five_minutes;
        files
            .entry((
                record["recipientAccountId"].as_str().unwrap().to_owned(),
                record["awsRegion"].as_str().unwrap().to_owned(),
                file_time,
            ))
            .or_default()
            .push(record);
    }
    for ((account, region, file_time), records) in files {
        let account_trail = LogFile {
            records,
            ..LogFile::new(
                account,
                region,
                DateTime::from_timestamp(file_time, 0).unwrap(),
            )
        };
        write_log_file(root, &account_trail);
        write_log_file(
            root,
            &LogFile {
                org_id: Some("o-exampleorgid".to_owned()),
                unique: "OrganizationTrail".to_owned(),
                ..account_trail
            },
        );
    }
}

// Both workspaces read the same directory by its absolute path, so that the URIs saved in
// ingested_files are the same.
fn write_plan(dir: &Path, logs: &Path) -> PathBuf {
    fs::create_dir_all(dir).unwrap();
    let plan = dir.join("run.yaml");
    fs::write(&plan, PLAN.replace("{logs}", &logs.display().to_string())).unwrap();
    plan
}

fn run_command(plan: &Path) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_ashiato"))
        .arg("run")
        .arg("--plan")
        .arg(plan)
        .arg("--emit-signals")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

// The same wiring as the run command, stopped after the first commit that leaves a partition
// inside a file. Nothing is finalized, so the rules of the plan are not needed here.
async fn stop_inside_a_file(plan_path: &Path) {
    let plan = RunPlan::load(plan_path).unwrap();
    let window = plan.analysis_window();
    let SourceLocation::Local { root } = &plan.source.location else {
        panic!("the plan reads a local directory");
    };
    let mut store = PipelineStore::open(&plan_path.with_file_name("ashiato.duckdb")).unwrap();
    let manifest = store
        .start_run::<StoreError>(plan.digest(), || {
            Ok(list_local(root, &ListScope::from_plan(&plan))
                .unwrap()
                .manifest)
        })
        .unwrap();
    let allowed_lateness = plan.pipeline.allowed_lateness.to_time_delta();
    let mut analyzer = Analyzer::open(
        store,
        manifest.clone(),
        AnalyzerSettings {
            batch: plan.pipeline.batch.to_time_delta(),
            allowed_lateness,
            read_since: window.baseline_since(),
            fetch_max: 2,
            commit_every: 2,
        },
    )
    .unwrap();
    let assignments: Vec<PartitionAssignment> = manifest
        .partitions
        .iter()
        .zip(analyzer.resume_positions())
        .enumerate()
        .map(|(index, (partition, start))| PartitionAssignment {
            index: u32::try_from(index).unwrap(),
            manifest: partition.clone(),
            start,
        })
        .collect();
    let (writer, mut reader) = partition_channels(
        manifest
            .partitions
            .iter()
            .map(|partition| partition.partition.clone()),
        4,
    );
    let worker = tokio::spawn(run_worker(
        assignments,
        Arc::new(LocalSource) as Arc<dyn FileSource>,
        Arc::new(writer),
        WorkerSettings {
            allowed_lateness,
            max_message_size: MAX_MESSAGE_SIZE,
        },
    ));
    loop {
        assert!(!analyzer.all_partitions_ended(), "no file was left");
        analyzer.receive(&mut reader).await.unwrap();
        analyzer.commit().unwrap();
        if analyzer
            .resume_positions()
            .iter()
            .any(|position| position.record_offset > 0)
        {
            break;
        }
    }
    drop(analyzer);
    drop(reader);
    worker.abort();
    let _ = worker.await;
}

fn differing_rows(reference: &Path, resumed: &Path, table: &str) -> (u64, u64) {
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(&format!(
            "ATTACH '{}' AS reference (READ_ONLY); ATTACH '{}' AS resumed (READ_ONLY);",
            reference.display(),
            resumed.display()
        ))
        .unwrap();
    let count = |from: &str, to: &str| -> u64 {
        connection
            .query_row(
                &format!(
                    "SELECT count(*) FROM (SELECT * FROM {from}.{table} \
                     EXCEPT ALL SELECT * FROM {to}.{table})"
                ),
                [],
                |row| row.get(0),
            )
            .unwrap()
    };
    (count("reference", "resumed"), count("resumed", "reference"))
}

#[tokio::test(flavor = "multi_thread")]
async fn run_resumes_a_run_stopped_inside_a_file_and_gives_the_same_output() {
    let dir = TempDir::new().unwrap();
    let logs = dir.path().join("logs");
    write_logs(&logs);
    let reference_plan = write_plan(&dir.path().join("reference"), &logs);
    let resumed_plan = write_plan(&dir.path().join("resumed"), &logs);
    let expected = run_command(&reference_plan);
    assert!(expected.contains("\"scenario_match\""), "{expected}");

    stop_inside_a_file(&resumed_plan).await;
    let resumed = run_command(&resumed_plan);

    assert_eq!(resumed, expected);
    let reference_db = reference_plan.with_file_name("ashiato.duckdb");
    let resumed_db = resumed_plan.with_file_name("ashiato.duckdb");
    for table in TABLES {
        assert_eq!(
            differing_rows(&reference_db, &resumed_db, table),
            (0, 0),
            "{table}"
        );
    }
    let connection = duckdb::Connection::open(&resumed_db).unwrap();
    let pending: u64 = connection
        .query_row("SELECT count(*) FROM pending", [], |row| row.get(0))
        .unwrap();
    assert_eq!(pending, 0);
}
