mod common;

use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use ashiato_duckdb::AnalyzerError;
use ashiato_pipeline::{
    EventTime, IngestedFile, Manifest, PartitionMessage, PartitionReader, PartitionWriter,
    Position, events_schema, partition_channels,
};
use ashiato_testkit::record::RecordBuilder;
use common::{Workspace, at, log, open, query_strings, rows, run_to_end, settings, start_worker};
use serde_json::Value;

const RECEIVE_TABLES: [&str; 3] = ["pending", "ingested_files", "partition_progress"];

fn event(id: &str, time: EventTime) -> Value {
    RecordBuilder::new()
        .set("eventID", id)
        .set("eventTime", time.to_rfc3339())
        .build()
}

fn two_partitions() -> Workspace {
    let workspace = Workspace::new();
    workspace.write(&log(
        "111111111111",
        at(1, 0, 5),
        vec![event("a-1", at(1, 0, 1)), event("a-2", at(1, 0, 2))],
    ));
    workspace.write(&log("111111111111", at(1, 0, 10), Vec::new()));
    workspace.write_bytes(
        &log("111111111111", at(1, 0, 15), Vec::new()),
        b"not a gzip stream",
    );
    workspace.write(&log(
        "111111111111",
        at(1, 0, 20),
        vec![
            event("a-3", at(1, 0, 16)),
            event("a-4", at(1, 0, 17)),
            event("a-5", at(1, 0, 18)),
        ],
    ));
    workspace.write(&log(
        "222222222222",
        at(1, 0, 5),
        vec![event("b-1", at(1, 0, 3))],
    ));
    workspace
}

#[tokio::test(flavor = "multi_thread")]
async fn received_messages_are_committed_with_their_files_and_positions() {
    let workspace = two_partitions();
    let manifest = workspace.manifest();
    let database = workspace.database("run");
    let mut analyzer = open(&database, &manifest, settings());
    assert_eq!(analyzer.resume_positions(), vec![Position::START; 2]);
    let (mut reader, worker) = start_worker(&manifest, &analyzer.resume_positions());

    while !analyzer.all_partitions_ended() {
        analyzer.receive(&mut reader).await.unwrap();
        analyzer.commit().unwrap();
    }
    assert_eq!(
        analyzer.resume_positions(),
        vec![Position::new(4, 0), Position::new(1, 0)]
    );
    drop(analyzer);
    worker.await.unwrap().unwrap();

    assert_eq!(
        query_strings(&database, "SELECT event_id FROM pending ORDER BY event_id"),
        vec!["a-1", "a-2", "a-3", "a-4", "a-5", "b-1"]
    );
    assert_eq!(
        query_strings(
            &database,
            "SELECT concat_ws(' ', partition_index, file_seq, record_count, error IS NOT NULL, \
             length(sha256)) FROM ingested_files ORDER BY partition_index, file_seq"
        ),
        vec![
            "0 0 2 false 64",
            "0 1 0 false 64",
            "0 2 0 true 64",
            "0 3 3 false 64",
            "1 0 1 false 64",
        ]
    );
    assert_eq!(
        query_strings(
            &database,
            "SELECT concat_ws(' ', partition_index, file_seq, record_offset, ended) \
             FROM partition_progress ORDER BY partition_index"
        ),
        vec!["0 4 0 true", "1 1 0 true"]
    );
}

async fn stop_after_commits(database: &Path, manifest: &Manifest, commits: usize, ack: bool) {
    let mut analyzer = open(database, manifest, settings());
    let (mut reader, worker) = start_worker(manifest, &analyzer.resume_positions());
    for _ in 0..commits {
        analyzer.receive(&mut reader).await.unwrap();
        let handles = analyzer.commit().unwrap();
        if ack {
            reader.ack(&handles).await.unwrap();
        }
    }
    analyzer.receive(&mut reader).await.unwrap();
    drop(analyzer);
    drop(reader);
    worker.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_between_commit_and_ack_and_resuming_gives_the_same_state() {
    let workspace = two_partitions();
    let manifest = workspace.manifest();
    let reference = workspace.database("reference");
    run_to_end(&reference, &manifest, settings()).await;

    for commits in [1, 3, 5] {
        let resumed = workspace.database(&format!("resumed-{commits}"));
        stop_after_commits(&resumed, &manifest, commits, false).await;
        run_to_end(&resumed, &manifest, settings()).await;

        for table in RECEIVE_TABLES {
            assert_eq!(
                rows(&resumed, table),
                rows(&reference, table),
                "{table} after stopping at commit {commits}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn messages_sent_again_from_the_start_are_not_counted_twice() {
    let workspace = two_partitions();
    let manifest = workspace.manifest();
    let reference = workspace.database("reference");
    run_to_end(&reference, &manifest, settings()).await;

    let resent = workspace.database("resent");
    stop_after_commits(&resent, &manifest, 4, true).await;
    let mut analyzer = open(&resent, &manifest, settings());
    let (mut reader, worker) = start_worker(&manifest, &[Position::START; 2]);
    analyzer.run(&mut reader).await.unwrap();
    drop(analyzer);
    drop(reader);
    worker.abort();

    for table in RECEIVE_TABLES {
        assert_eq!(rows(&resent, table), rows(&reference, table), "{table}");
    }
}

fn empty_chunk(manifest: &Manifest, file_seq: u64) -> PartitionMessage {
    let partition = &manifest.partitions[0];
    let entry = &partition.files[usize::try_from(file_seq).unwrap()];
    PartitionMessage::FileChunk {
        partition: partition.partition.clone(),
        position: Position::new(file_seq, 0),
        record_count: 0,
        file: IngestedFile {
            uri: entry.uri.clone(),
            version_id: None,
            etag: None,
            size: entry.size,
            sha256: "0".repeat(64),
            file_time: entry.file_time,
            record_count: 0,
        },
        events: RecordBatch::new_empty(Arc::clone(&events_schema())),
        watermark: entry.file_time,
    }
}

fn empty_files() -> (Workspace, Manifest) {
    let workspace = Workspace::new();
    workspace.write(&log("111111111111", at(1, 0, 5), Vec::new()));
    workspace.write(&log("111111111111", at(1, 0, 10), Vec::new()));
    let manifest = workspace.manifest();
    (workspace, manifest)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gap_in_the_positions_is_an_error_in_a_single_process() {
    let (workspace, manifest) = empty_files();
    let partition = manifest.partitions[0].partition.clone();
    let (writer, mut reader) = partition_channels([partition.clone()], 4);
    writer.put(empty_chunk(&manifest, 1)).await.unwrap();
    let mut analyzer = open(&workspace.database("gap"), &manifest, settings());

    let error = analyzer.run(&mut reader).await.unwrap_err();

    assert!(
        matches!(
            &error,
            AnalyzerError::Gap { partition: gap, expected, received }
                if *gap == partition
                    && *expected == Position::START
                    && *received == Position::new(1, 0)
        ),
        "{error:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_partition_that_stops_before_its_end_is_an_error() {
    let (workspace, manifest) = empty_files();
    let partition = manifest.partitions[0].partition.clone();
    let (writer, mut reader) = partition_channels([partition.clone()], 4);
    writer.put(empty_chunk(&manifest, 0)).await.unwrap();
    let last_message = empty_chunk(&manifest, 1);
    let sender = tokio::spawn(async move { writer.put(last_message).await });
    sender.await.unwrap().unwrap();
    let mut analyzer = open(&workspace.database("closed"), &manifest, settings());

    let error = analyzer.run(&mut reader).await.unwrap_err();

    assert!(
        matches!(&error, AnalyzerError::Closed(closed) if *closed == partition),
        "{error:?}"
    );
}
