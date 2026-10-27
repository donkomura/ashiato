mod common;

use std::path::Path;

use ashiato_pipeline::{EventTime, Manifest, PartitionReader};
use ashiato_testkit::log_file::LogFile;
use ashiato_testkit::record::RecordBuilder;
use chrono::TimeDelta;
use common::{Workspace, at, log, open, query_strings, rows, settings, start_worker};
use serde_json::Value;

const DEDUP_TABLES: [&str; 3] = ["pending", "late_events", "recent_event_ids"];

fn event(id: &str, time: EventTime) -> Value {
    RecordBuilder::new()
        .set("eventID", id)
        .set("eventTime", time.to_rfc3339())
        .build()
}

fn organization_log(account: &str, time: EventTime, records: Vec<Value>) -> LogFile {
    LogFile {
        org_id: Some("o-exampleorgid".to_owned()),
        unique: "OrganizationTrail".to_owned(),
        ..log(account, time, records)
    }
}

fn overlapping_trails() -> Workspace {
    let workspace = Workspace::new();
    workspace.write(&organization_log(
        "111111111111",
        at(1, 0, 5),
        vec![event("shared", at(1, 0, 1)), event("org-only", at(1, 0, 2))],
    ));
    workspace.write(&log(
        "111111111111",
        at(1, 0, 5),
        vec![
            event("shared", at(1, 0, 1)),
            event("account-only", at(1, 0, 3)),
            event("late", at(1, 0, 0) - TimeDelta::hours(1)),
        ],
    ));
    workspace.write(&log(
        "111111111111",
        at(1, 0, 20),
        vec![
            event("org-only", at(1, 0, 2)),
            event("shared", at(1, 0, 1)),
            event("next", at(1, 0, 30)),
        ],
    ));
    workspace.write(&log(
        "222222222222",
        at(1, 0, 5),
        vec![event("shared", at(1, 0, 1))],
    ));
    workspace
}

async fn receive_without_finalizing(database: &Path, manifest: &Manifest, commits: Option<usize>) {
    let mut analyzer = open(database, manifest, settings());
    let (mut reader, worker) = start_worker(manifest, &analyzer.resume_positions());
    let mut remaining = commits;
    while !analyzer.all_partitions_ended() {
        analyzer.receive(&mut reader).await.unwrap();
        if remaining == Some(0) {
            break;
        }
        let handles = analyzer.commit().unwrap();
        reader.ack(&handles).await.unwrap();
        remaining = remaining.map(|count| count - 1);
    }
    drop(analyzer);
    drop(reader);
    worker.abort();
}

fn pending_events(database: &Path) -> Vec<String> {
    query_strings(
        database,
        "SELECT event_id || ' ' || file_id || ' ' || record_index FROM pending \
         ORDER BY event_id, file_id",
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn an_event_in_both_the_organization_and_the_account_trail_is_received_once() {
    let workspace = overlapping_trails();
    let manifest = workspace.manifest();
    assert_eq!(manifest.partitions[0].files.len(), 3);
    let database = workspace.database("run");

    receive_without_finalizing(&database, &manifest, None).await;

    let ids = query_strings(
        &database,
        "SELECT event_id || '@' || (file_id >> 32) FROM pending ORDER BY event_id, file_id",
    );
    assert_eq!(
        ids,
        vec![
            "account-only@0",
            "next@0",
            "org-only@0",
            "shared@0",
            "shared@1",
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_first_copy_in_the_order_of_the_files_is_kept() {
    let workspace = overlapping_trails();
    let manifest = workspace.manifest();
    let database = workspace.database("run");

    receive_without_finalizing(&database, &manifest, None).await;

    let first_file = &manifest.partitions[0].files[0];
    let first_file_id = ashiato_pipeline::file_id(0, first_file.file_seq);
    let kept = query_strings(
        &database,
        "SELECT file_id::VARCHAR FROM pending WHERE event_id = 'shared' AND file_id >> 32 = 0",
    );
    assert_eq!(kept, vec![first_file_id.to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_before_the_commit_does_not_drop_the_events_received_again() {
    let workspace = overlapping_trails();
    let manifest = workspace.manifest();
    let reference = workspace.database("reference");
    receive_without_finalizing(&reference, &manifest, None).await;

    for commits in 0..=4 {
        let resumed = workspace.database(&format!("resumed-{commits}"));
        receive_without_finalizing(&resumed, &manifest, Some(commits)).await;
        receive_without_finalizing(&resumed, &manifest, None).await;

        assert_eq!(
            pending_events(&resumed),
            pending_events(&reference),
            "pending after stopping at commit {commits}"
        );
        for table in DEDUP_TABLES {
            assert_eq!(
                rows(&resumed, table),
                rows(&reference, table),
                "{table} after stopping at commit {commits}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn late_events_are_recorded_once_after_a_restart() {
    let workspace = overlapping_trails();
    let manifest = workspace.manifest();
    let database = workspace.database("resumed");
    receive_without_finalizing(&database, &manifest, Some(1)).await;
    receive_without_finalizing(&database, &manifest, None).await;

    assert_eq!(
        query_strings(&database, "SELECT event_id FROM late_events"),
        vec!["late"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn event_ids_older_than_twice_the_allowed_lateness_before_the_partition_watermark_expire() {
    let workspace = overlapping_trails();
    workspace.write(&log(
        "111111111111",
        at(1, 2, 0),
        vec![event("much-later", at(1, 1, 45))],
    ));
    let manifest = workspace.manifest();
    let database = workspace.database("run");

    receive_without_finalizing(&database, &manifest, None).await;

    // The last file of the first partition is at 02:00, so its watermark is 01:30 and the
    // event IDs before 00:30 expire. The second partition stays at the watermark 23:35.
    assert_eq!(
        query_strings(
            &database,
            "SELECT partition_index || ' ' || event_id FROM recent_event_ids \
             ORDER BY partition_index, event_id"
        ),
        vec!["0 much-later", "0 next", "1 shared"]
    );
}
