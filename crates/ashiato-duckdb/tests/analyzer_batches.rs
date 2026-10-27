mod common;

use ashiato_ingest::{ListScope, list_local};
use ashiato_pipeline::{EventTime, Manifest, PartitionReader};
use ashiato_testkit::record::RecordBuilder;
use common::{Workspace, at, log, open, query_strings, rows, run_to_end, settings, start_worker};
use serde_json::Value;

const BATCH_TABLES: [&str; 5] = [
    "pending",
    "late_events",
    "batches",
    "ingested_files",
    "partition_progress",
];

fn event(id: &str, time: EventTime) -> Value {
    RecordBuilder::new()
        .set("eventID", id)
        .set("eventTime", time.to_rfc3339())
        .build()
}

fn hourly_files(workspace: &Workspace, account: &str) {
    for hour in 1..=3 {
        workspace.write(&log(
            account,
            at(1, hour, 5),
            vec![event(&format!("{account}-{hour}"), at(1, hour - 1, 40))],
        ));
    }
}

fn batches(database: &std::path::Path) -> Vec<String> {
    query_strings(
        database,
        "SELECT strftime(batch_start, '%H:%M') || '-' || strftime(batch_end, '%H:%M') \
         FROM batches ORDER BY batch_start",
    )
}

fn pending(database: &std::path::Path) -> Vec<String> {
    query_strings(database, "SELECT event_id FROM pending ORDER BY event_id")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_is_finalized_once_the_global_watermark_reaches_its_end() {
    let workspace = Workspace::new();
    hourly_files(&workspace, "111111111111");
    let manifest = workspace.manifest();
    let database = workspace.database("run");

    let finalized: Vec<Vec<String>> = finalize_step_by_step(&database, &manifest)
        .await
        .into_iter()
        .map(|batches| {
            batches
                .into_iter()
                .map(|(start, end)| format!("{}-{}", start.format("%H:%M"), end.format("%H:%M")))
                .collect()
        })
        .collect();

    // The watermarks of the files at 01:05, 02:05 and 03:05 are 00:35, 01:35 and 02:35.
    assert_eq!(
        finalized,
        vec![
            vec![],
            vec!["00:00-01:00".to_owned()],
            vec!["01:00-02:00".to_owned()],
            vec!["02:00-03:00".to_owned()],
        ]
    );
    assert_eq!(
        batches(&database),
        ["00:00-01:00", "01:00-02:00", "02:00-03:00"]
    );
    assert!(pending(&database).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lagging_partition_holds_back_the_batches_of_the_others() {
    let workspace = Workspace::new();
    hourly_files(&workspace, "111111111111");
    hourly_files(&workspace, "222222222222");
    let manifest = workspace.manifest();
    let database = workspace.database("run");

    let finalized = finalize_step_by_step(&database, &manifest).await;

    // The analyzer reads the partition with the lowest watermark first, so the partitions
    // take turns. A batch waits until both partitions have passed its end.
    let hour = |start: u32| (at(1, start, 0), at(1, start + 1, 0));
    assert_eq!(
        finalized,
        vec![
            vec![],
            vec![],
            vec![],
            vec![hour(0)],
            vec![],
            vec![hour(1)],
            vec![],
            vec![hour(2)],
        ]
    );
    assert!(pending(&database).is_empty());
}

async fn finalize_step_by_step(
    database: &std::path::Path,
    manifest: &Manifest,
) -> Vec<Vec<(EventTime, EventTime)>> {
    let mut analyzer = open(database, manifest, settings());
    let (mut reader, worker) = start_worker(manifest, &analyzer.resume_positions());
    let mut finalized = Vec::new();
    while !analyzer.all_partitions_ended() {
        analyzer.receive(&mut reader).await.unwrap();
        let handles = analyzer.commit().unwrap();
        reader.ack(&handles).await.unwrap();
        finalized.push(analyzer.finalize_batches().unwrap());
    }
    drop(analyzer);
    worker.await.unwrap().unwrap();
    finalized
}

#[tokio::test(flavor = "multi_thread")]
async fn ending_every_partition_finalizes_all_pending_batches() {
    let workspace = Workspace::new();
    workspace.write(&log(
        "111111111111",
        at(1, 0, 5),
        vec![event("early", at(1, 0, 1)), event("late-hour", at(1, 5, 0))],
    ));
    let manifest = workspace.manifest();
    let database = workspace.database("run");

    run_to_end(&database, &manifest, settings()).await;

    assert_eq!(batches(&database), ["00:00-01:00", "05:00-06:00"]);
    assert!(pending(&database).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_partition_without_files_does_not_hold_back_the_global_watermark() {
    let workspace = Workspace::new();
    hourly_files(&workspace, "111111111111");
    let accounts = ["111111111111".to_owned(), "222222222222".to_owned()];
    let regions = ["us-east-1".to_owned()];
    let manifest: Manifest = list_local(
        workspace.logs(),
        &ListScope {
            accounts: Some(&accounts),
            regions: Some(&regions),
            period: at(1, 0, 0)..at(31, 0, 0),
        },
    )
    .unwrap()
    .manifest;
    assert!(manifest.partitions[1].files.is_empty());
    let database = workspace.database("run");
    let mut analyzer = open(&database, &manifest, settings());
    let (mut reader, worker) = start_worker(&manifest, &analyzer.resume_positions());

    let mut finalized_before_the_end = Vec::new();
    while !analyzer.all_partitions_ended() {
        analyzer.receive(&mut reader).await.unwrap();
        analyzer.commit().unwrap();
        let batches = analyzer.finalize_batches().unwrap();
        if !analyzer.all_partitions_ended() {
            finalized_before_the_end.extend(batches);
        }
    }
    drop(analyzer);
    worker.await.unwrap().unwrap();

    assert_eq!(
        finalized_before_the_end,
        vec![(at(1, 0, 0), at(1, 1, 0)), (at(1, 1, 0), at(1, 2, 0))]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn events_older_than_the_watermark_of_their_file_are_late() {
    let workspace = Workspace::new();
    workspace.write(&log(
        "111111111111",
        at(1, 3, 5),
        vec![
            event("on-time", at(1, 2, 40)),
            event("late", at(1, 2, 34)),
            event("boundary", at(1, 2, 35)),
        ],
    ));
    let manifest = workspace.manifest();
    let database = workspace.database("run");
    let mut analyzer = open(&database, &manifest, settings());
    let (mut reader, worker) = start_worker(&manifest, &analyzer.resume_positions());
    analyzer.receive(&mut reader).await.unwrap();
    analyzer.commit().unwrap();
    drop(analyzer);
    drop(reader);
    worker.abort();

    assert_eq!(pending(&database), ["boundary", "on-time"]);
    let file_id = manifest_file_id(&database);
    assert_eq!(
        query_strings(
            &database,
            "SELECT concat_ws(' ', event_id, file_id, record_index) FROM late_events"
        ),
        [format!("late {file_id} 1")]
    );
}

fn manifest_file_id(database: &std::path::Path) -> String {
    query_strings(
        database,
        "SELECT file_id::VARCHAR FROM ingested_files WHERE partition_index = 0 AND file_seq = 0",
    )
    .remove(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_and_resuming_gives_the_same_batches_and_late_events() {
    let workspace = Workspace::new();
    hourly_files(&workspace, "111111111111");
    hourly_files(&workspace, "222222222222");
    workspace.write(&log(
        "111111111111",
        at(1, 3, 30),
        vec![event("late", at(1, 0, 0)), event("on-time", at(1, 3, 20))],
    ));
    let manifest = workspace.manifest();
    let reference = workspace.database("reference");
    run_to_end(&reference, &manifest, settings()).await;

    for commits in 1..=8 {
        let resumed = workspace.database(&format!("resumed-{commits}"));
        let mut analyzer = open(&resumed, &manifest, settings());
        let (mut reader, worker) = start_worker(&manifest, &analyzer.resume_positions());
        for _ in 0..commits {
            analyzer.receive(&mut reader).await.unwrap();
            analyzer.commit().unwrap();
            if commits % 2 == 0 {
                analyzer.finalize_batches().unwrap();
            }
        }
        drop(analyzer);
        drop(reader);
        worker.abort();
        run_to_end(&resumed, &manifest, settings()).await;

        for table in BATCH_TABLES {
            assert_eq!(
                rows(&resumed, table),
                rows(&reference, table),
                "{table} after stopping at commit {commits}"
            );
        }
    }
}
