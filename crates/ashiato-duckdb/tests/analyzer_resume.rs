mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ashiato_core::analysis::{Analysis, correlate_available};
use ashiato_core::context::Context;
use ashiato_core::fixture::fixture_window;
use ashiato_core::lineage::LineageEdge;
use ashiato_core::rule::{Rule, RuleSource, Rules, SignalRule};
use ashiato_core::sequence::ObservedSignal;
use ashiato_duckdb::{Analyzer, AnalyzerError, AnalyzerSettings, PipelineStore, StoreError};
use ashiato_ingest::{
    FileSource, LocalSource, PartitionAssignment, WorkerError, WorkerSettings, run_worker,
};
use ashiato_pipeline::{ChannelReader, Manifest, PartitionReader, Position, partition_channels};
use ashiato_testkit::fixture::{Expectation, builtin_fixtures};
use ashiato_testkit::log_file::LogFile;
use ashiato_testkit::record::RecordBuilder;
use chrono::{DateTime, TimeDelta};
use common::{LATENESS_MINUTES, Workspace, event_time, query_strings};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

const DIGEST: &str = "resume-test-plan";

// The rules of the four scenarios of design.md 12, including the rules that use the baseline.
const SCENARIO_RULE_IDS: [&str; 17] = [
    "console_takeover_mining",
    "console_login_without_mfa",
    "cloudshell_activity",
    "compute_resource_launch",
    "role_chain_escalation",
    "assume_role_by_assumed_role",
    "iam_privilege_modification",
    "leaked_key_recon_persistence",
    "first_seen_identity_probe",
    "access_denied_enumeration",
    "iam_persistence",
    "novel_api_burst",
    "cross_account_s3_destruction",
    "cross_account_assume_role",
    "s3_bucket_enumeration",
    "s3_object_read_or_copy",
    "s3_delete_burst",
];

// The tables of streaming-pipeline.md 14.1.3, and recent_event_ids to check the removal of
// duplicates across a restart.
const COMPARED_TABLES: [&str; 10] = [
    "signals",
    "lineage_edges",
    "baseline",
    "baseline_principals",
    "ingested_files",
    "late_events",
    "batches",
    "data_counters",
    "partition_progress",
    "recent_event_ids",
];

const ORGANIZATION_ID: &str = "o-exampleorgid";
const LATE_EVERY: usize = 9;
const LATE_DELIVERY_HOURS: i64 = 2;

// Each chunk holds a single record, so that most commits leave a partition inside a file.
const MAX_MESSAGE_SIZE: usize = 4097;

fn builtin_rules() -> Rules {
    Rules::new([RuleSource::builtin().unwrap()]).unwrap()
}

fn signal_rules(rules: &Rules) -> Vec<SignalRule> {
    rules
        .iter()
        .filter_map(|rule| match rule {
            Rule::Signal(rule) => Some(rule.clone()),
            Rule::Scenario(_) => None,
        })
        .collect()
}

fn fixture_records() -> Vec<Value> {
    builtin_fixtures()
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
        })
        .collect()
}

// Every file of the account trail is also delivered by the organization trail, and some records
// are delivered hours after they happened, so that the duplicates and the late events cross the
// point where the analyzer stops.
fn write_dataset(workspace: &Workspace) {
    let window = fixture_window();
    let five_minutes = TimeDelta::minutes(5).num_seconds();
    let mut files: BTreeMap<(String, String, i64), Vec<Value>> = BTreeMap::new();
    for (index, record) in fixture_records().into_iter().enumerate() {
        let time = event_time(&record);
        if time < window.baseline_since() || time >= window.until() - TimeDelta::hours(1) {
            continue;
        }
        let delay = if index % LATE_EVERY == 0 {
            TimeDelta::hours(LATE_DELIVERY_HOURS).num_seconds()
        } else {
            0
        };
        let file_time = (time.timestamp() / five_minutes + 2) * five_minutes + delay;
        files
            .entry((
                record["recipientAccountId"].as_str().unwrap().to_owned(),
                record["awsRegion"].as_str().unwrap().to_owned(),
                file_time,
            ))
            .or_default()
            .push(record);
    }
    assert!(files.len() > 4, "the dataset has {} files", files.len());
    for ((account, region, file_time), records) in files {
        let account_trail = LogFile {
            records,
            ..LogFile::new(
                account,
                region,
                DateTime::from_timestamp(file_time, 0).unwrap(),
            )
        };
        let organization_trail = LogFile {
            org_id: Some(ORGANIZATION_ID.to_owned()),
            unique: "OrganizationTrail".to_owned(),
            ..account_trail.clone()
        };
        workspace.write(&account_trail);
        workspace.write(&organization_trail);
    }
    workspace.write_bytes(
        &LogFile::new(
            "111111111111",
            "us-east-1",
            window.since() + TimeDelta::hours(3),
        ),
        b"not a gzip stream",
    );
}

fn settings() -> AnalyzerSettings {
    AnalyzerSettings {
        batch: TimeDelta::hours(1),
        allowed_lateness: TimeDelta::minutes(LATENESS_MINUTES),
        read_since: fixture_window().baseline_since(),
        fetch_max: 2,
        commit_every: 2,
    }
}

// The manifest is listed only when the run starts for the first time, as the run command does
// (streaming-pipeline.md 5.4).
fn open_run(database: &Path, workspace: &Workspace) -> (Analyzer, Manifest) {
    let mut store = PipelineStore::open(database).unwrap();
    let manifest = store
        .start_run::<StoreError>(DIGEST, || Ok(workspace.manifest()))
        .unwrap();
    let analyzer = Analyzer::open(store, manifest.clone(), settings())
        .unwrap()
        .with_queries(signal_rules(&builtin_rules()), fixture_window());
    (analyzer, manifest)
}

fn start_worker(
    manifest: &Manifest,
    starts: &[Position],
) -> (ChannelReader, JoinHandle<Result<(), WorkerError>>) {
    let assignments: Vec<PartitionAssignment> = manifest
        .partitions
        .iter()
        .zip(starts)
        .enumerate()
        .map(|(index, (partition, start))| PartitionAssignment {
            index: u32::try_from(index).unwrap(),
            manifest: partition.clone(),
            start: *start,
        })
        .collect();
    let (writer, reader) = partition_channels(
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
            allowed_lateness: TimeDelta::minutes(LATENESS_MINUTES),
            max_message_size: MAX_MESSAGE_SIZE,
        },
    ));
    (reader, worker)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    // After the n-th receive, before its commit.
    AfterReceive(usize),
    // After the first commit that leaves a partition inside a file, before the acknowledgement
    // and finalize_batches.
    AfterCommitInsideFile,
    // After the commit of the end of every partition, before finalize_batches.
    AfterLastCommit,
    // After the n-th call of finalize_batches that finalized a batch, while other batches are
    // still pending. A batch is finalized in one transaction, so stopping inside a batch is the
    // same as stopping before it.
    AfterBatches(usize),
    // After every partition is streamed and every batch is finalized, before the correlation.
    BeforeCorrelation,
}

struct Stopped {
    resume_positions: Vec<Position>,
    batches: u64,
}

async fn run_until(database: &Path, workspace: &Workspace, stop: Stop) -> Stopped {
    let (mut analyzer, manifest) = open_run(database, workspace);
    let (mut reader, worker) = start_worker(&manifest, &analyzer.resume_positions());
    let mut receives = 0;
    let mut finalizing = 0;
    let stopped = loop {
        if analyzer.all_partitions_ended() {
            analyzer.commit().unwrap();
            analyzer.finalize_batches().unwrap();
            break stop == Stop::BeforeCorrelation;
        }
        analyzer.receive(&mut reader).await.unwrap();
        receives += 1;
        if stop == Stop::AfterReceive(receives) {
            break true;
        }
        let handles = analyzer.commit().unwrap();
        let inside_file = analyzer
            .resume_positions()
            .iter()
            .any(|position| position.record_offset > 0);
        if (stop == Stop::AfterCommitInsideFile && inside_file)
            || (stop == Stop::AfterLastCommit && analyzer.all_partitions_ended())
        {
            break true;
        }
        reader.ack(&handles).await.unwrap();
        if !analyzer.finalize_batches().unwrap().is_empty() {
            finalizing += 1;
            if stop == Stop::AfterBatches(finalizing) {
                break true;
            }
        }
    };
    assert!(stopped, "the stream ended before {stop:?}");
    drop(analyzer);
    drop(reader);
    worker.abort();
    let _ = worker.await;
    let (analyzer, _) = open_run(database, workspace);
    let resume_positions = analyzer.resume_positions();
    drop(analyzer);
    Stopped {
        resume_positions,
        batches: row_count(database, "batches"),
    }
}

struct Resumed {
    analyzed: Result<(), AnalyzerError>,
    worked: Result<(), WorkerError>,
}

// The ingest worker is started again from the positions the analyzer committed (streaming-
// pipeline.md 6.4).
async fn resume(database: &Path, workspace: &Workspace) -> Resumed {
    let (mut analyzer, manifest) = open_run(database, workspace);
    let (mut reader, worker) = start_worker(&manifest, &analyzer.resume_positions());
    let analyzed = analyzer.run(&mut reader).await;
    drop(analyzer);
    // A partition that had ended before the restart is not read again, so its end of partition
    // stays in the channel. The reader is kept until the worker finishes to accept that send.
    if analyzed.is_err() {
        drop(reader);
        let worked = worker.await.unwrap();
        return Resumed { analyzed, worked };
    }
    let worked = worker.await.unwrap();
    drop(reader);
    Resumed { analyzed, worked }
}

async fn reference(workspace: &Workspace) -> PathBuf {
    let database = workspace.database("reference");
    let resumed = resume(&database, workspace).await;
    resumed.analyzed.unwrap();
    resumed.worked.unwrap();
    database
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

fn row_count(database: &Path, table: &str) -> u64 {
    query_strings(database, &format!("SELECT count(*)::VARCHAR FROM {table}"))[0]
        .parse()
        .unwrap()
}

struct Output {
    signals: Vec<ObservedSignal>,
    edges: Vec<LineageEdge>,
    analysis: Analysis,
}

fn output(database: &Path) -> Output {
    let store = PipelineStore::open(database).unwrap();
    let signals = store.observed_signals().unwrap();
    let edges = store.lineage_edges().unwrap();
    let analysis = correlate_available(
        &builtin_rules(),
        signals.clone(),
        edges.clone(),
        store.data_availability().unwrap(),
        &Context::default(),
    );
    Output {
        signals,
        edges,
        analysis,
    }
}

fn assert_meaningful(reference: &Path) {
    for table in [
        "signals",
        "lineage_edges",
        "baseline",
        "baseline_principals",
        "late_events",
        "batches",
    ] {
        assert!(row_count(reference, table) > 0, "{table} is empty");
    }
    // Every record is delivered by both trails, so each event is counted once only if the copies
    // were dropped.
    let counted: Vec<String> = query_strings(
        reference,
        "SELECT (SELECT events FROM data_counters)::VARCHAR \
         UNION ALL SELECT sum(record_count)::VARCHAR FROM ingested_files",
    );
    let events: u64 = counted[0].parse().unwrap();
    let records: u64 = counted[1].parse().unwrap();
    assert!(events > 0 && events * 2 <= records, "{events} of {records}");
    assert!(
        row_count(reference, "ingested_files WHERE error IS NOT NULL") > 0,
        "no file error"
    );
    assert!(
        !output(reference).analysis.matches.is_empty(),
        "no scenario matched"
    );
}

fn assert_same_as_reference(reference: &Path, resumed: &Path, label: &str) {
    assert_eq!(row_count(resumed, "pending"), 0, "pending after {label}");
    for table in COMPARED_TABLES {
        assert_eq!(
            differing_rows(reference, resumed, table),
            (0, 0),
            "{table} after {label}"
        );
    }
    let expected = output(reference);
    let actual = output(resumed);
    assert_eq!(actual.signals, expected.signals, "signals after {label}");
    assert_eq!(actual.edges, expected.edges, "lineage edges after {label}");
    assert_eq!(actual.analysis, expected.analysis, "analysis after {label}");
}

// Returns where each run stopped, and the number of batches of the run that was not stopped.
async fn assert_resumes_to_the_reference(stops: &[Stop]) -> (Vec<Stopped>, u64) {
    let workspace = Workspace::new();
    write_dataset(&workspace);
    let reference = reference(&workspace).await;
    assert_meaningful(&reference);
    let mut stopped = Vec::new();
    for (index, stop) in stops.iter().enumerate() {
        let database = workspace.database(&format!("resumed-{index}"));
        stopped.push(run_until(&database, &workspace, *stop).await);
        let resumed = resume(&database, &workspace).await;
        resumed.analyzed.unwrap();
        resumed.worked.unwrap();
        assert_same_as_reference(&reference, &database, &format!("{stop:?}"));
    }
    (stopped, row_count(&reference, "batches"))
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_after_receiving_and_resuming_gives_the_same_tables_and_output() {
    assert_resumes_to_the_reference(&[
        Stop::AfterReceive(1),
        Stop::AfterReceive(7),
        Stop::AfterReceive(40),
    ])
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_after_a_commit_inside_a_file_resumes_the_worker_from_that_record() {
    let (stopped, _) = assert_resumes_to_the_reference(&[Stop::AfterCommitInsideFile]).await;

    assert!(
        stopped[0]
            .resume_positions
            .iter()
            .any(|position| position.record_offset > 0),
        "{:?}",
        stopped[0].resume_positions
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_after_the_last_commit_before_finalizing_gives_the_same_tables_and_output() {
    assert_resumes_to_the_reference(&[Stop::AfterLastCommit]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_between_batches_gives_the_same_tables_and_output() {
    let (stopped, batches) =
        assert_resumes_to_the_reference(&[Stop::AfterBatches(1), Stop::AfterBatches(2)]).await;

    for stopped in stopped {
        assert!(
            0 < stopped.batches && stopped.batches < batches,
            "{} of {batches} batches were finalized before stopping",
            stopped.batches
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_before_the_correlation_gives_the_same_tables_and_output() {
    assert_resumes_to_the_reference(&[Stop::BeforeCorrelation]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_added_to_the_source_while_stopped_is_not_read() {
    let workspace = Workspace::new();
    write_dataset(&workspace);
    let reference = reference(&workspace).await;
    let database = workspace.database("resumed");
    run_until(&database, &workspace, Stop::AfterCommitInsideFile).await;

    let window = fixture_window();
    for account in ["111111111111", "999999999999"] {
        workspace.write(&LogFile {
            records: vec![
                RecordBuilder::new()
                    .set("eventID", format!("added-{account}"))
                    .set(
                        "eventTime",
                        (window.since() + TimeDelta::hours(2)).to_rfc3339(),
                    )
                    .set("recipientAccountId", account)
                    .build(),
            ],
            ..LogFile::new(
                account,
                "us-east-1",
                window.since() + TimeDelta::hours(2) + TimeDelta::minutes(5),
            )
        });
    }
    let resumed = resume(&database, &workspace).await;

    resumed.analyzed.unwrap();
    resumed.worked.unwrap();
    assert_same_as_reference(&reference, &database, "adding files");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_listed_file_removed_from_the_source_while_stopped_is_a_worker_error() {
    let workspace = Workspace::new();
    write_dataset(&workspace);
    let database = workspace.database("resumed");
    run_until(&database, &workspace, Stop::AfterReceive(1)).await;
    let (analyzer, manifest) = open_run(&database, &workspace);
    drop(analyzer);
    let removed = manifest.partitions[0].files.last().unwrap().uri.clone();
    std::fs::remove_file(&removed).unwrap();

    let resumed = resume(&database, &workspace).await;

    match resumed.worked {
        Err(WorkerError::Source { uri, .. }) => assert_eq!(uri, removed),
        other => panic!("expected the removed file to stop the worker: {other:?}"),
    }
    assert!(
        matches!(resumed.analyzed, Err(AnalyzerError::Closed(_))),
        "{:?}",
        resumed.analyzed.err()
    );
}
