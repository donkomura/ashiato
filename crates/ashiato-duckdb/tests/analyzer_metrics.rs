mod common;

use std::sync::Arc;
use std::time::Duration;

use ashiato_pipeline::{AnalyzerCounts, AnalyzerMetrics, EventTime, PartitionReader};
use ashiato_testkit::record::RecordBuilder;
use chrono::TimeDelta;
use common::{Workspace, at, log, open, run_to_end, settings, start_worker};
use serde_json::Value;

fn event(id: &str, time: EventTime) -> Value {
    RecordBuilder::new()
        .set("eventID", id)
        .set("eventTime", time.to_rfc3339())
        .build()
}

// The watermarks of the files at 01:05, 02:05 and 03:05 are 00:35, 01:35 and 02:35.
fn hourly_files(workspace: &Workspace, account: &str) {
    for hour in 1..=3 {
        workspace.write(&log(
            account,
            at(1, hour, 5),
            vec![event(&format!("{account}-{hour}"), at(1, hour - 1, 40))],
        ));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_analyzer_counts_the_files_and_the_rows_and_marks_when_every_end_is_committed() {
    let workspace = Workspace::new();
    hourly_files(&workspace, "111111111111");
    hourly_files(&workspace, "222222222222");
    let manifest = workspace.manifest();
    let database = workspace.database("run");
    let metrics = Arc::new(AnalyzerMetrics::default());
    let mut analyzer = open(&database, &manifest, settings()).with_metrics(Arc::clone(&metrics));
    assert_eq!(
        metrics.counts(),
        AnalyzerCounts {
            manifest_files: 6,
            ..AnalyzerCounts::default()
        }
    );
    let (mut reader, worker) = start_worker(&manifest, &analyzer.resume_positions());

    let mut ingested = Vec::new();
    while !analyzer.all_partitions_ended() {
        assert_eq!(metrics.last_end_committed.get(), None);
        analyzer.receive(&mut reader).await.unwrap();
        let handles = analyzer.commit().unwrap();
        reader.ack(&handles).await.unwrap();
        analyzer.finalize_batches().unwrap();
        ingested.push(metrics.counts().ingested_files);
    }
    drop(analyzer);
    worker.await.unwrap().unwrap();

    assert_eq!(ingested, [1, 2, 3, 4, 5, 6, 6, 6]);
    assert_eq!(
        metrics.counts(),
        AnalyzerCounts {
            manifest_files: 6,
            ingested_files: 6,
            global_watermark: Some(at(1, 2, 35)),
            pending_rows: 0,
            pending_max_rows: 4,
        }
    );
    assert!(metrics.last_end_committed.get().is_some());
    assert!(metrics.fetch_wait.get() > Duration::ZERO);
    assert!(metrics.commit.get() > Duration::ZERO);
    assert!(metrics.batch.get() > Duration::ZERO);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_global_watermark_follows_the_committed_watermarks_until_every_partition_ends() {
    let workspace = Workspace::new();
    hourly_files(&workspace, "111111111111");
    hourly_files(&workspace, "222222222222");
    let manifest = workspace.manifest();
    let database = workspace.database("run");
    let metrics = Arc::new(AnalyzerMetrics::default());
    let mut analyzer = open(&database, &manifest, settings()).with_metrics(Arc::clone(&metrics));
    let (mut reader, worker) = start_worker(&manifest, &analyzer.resume_positions());

    let mut watermarks = Vec::new();
    while !analyzer.all_partitions_ended() {
        analyzer.receive(&mut reader).await.unwrap();
        let handles = analyzer.commit().unwrap();
        reader.ack(&handles).await.unwrap();
        analyzer.finalize_batches().unwrap();
        watermarks.push(metrics.counts().global_watermark);
    }
    drop(analyzer);
    worker.await.unwrap().unwrap();

    // read_since is 00:00, so a partition that has not started holds the global watermark at
    // 23:30 of the day before.
    assert_eq!(
        watermarks,
        [
            Some(at(1, 0, 0) - TimeDelta::minutes(30)),
            Some(at(1, 0, 35)),
            Some(at(1, 0, 35)),
            Some(at(1, 1, 35)),
            Some(at(1, 1, 35)),
            Some(at(1, 2, 35)),
            Some(at(1, 2, 35)),
            Some(at(1, 2, 35)),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resumed_analyzer_counts_the_files_ingested_before() {
    let workspace = Workspace::new();
    hourly_files(&workspace, "111111111111");
    let manifest = workspace.manifest();
    let database = workspace.database("run");
    run_to_end(&database, &manifest, settings()).await;

    let metrics = Arc::new(AnalyzerMetrics::default());
    let _analyzer = open(&database, &manifest, settings()).with_metrics(Arc::clone(&metrics));

    let counts = metrics.counts();
    assert_eq!((counts.manifest_files, counts.ingested_files), (3, 3));
    assert_eq!(counts.pending_rows, 0);
}
