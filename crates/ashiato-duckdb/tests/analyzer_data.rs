mod common;

use std::path::Path;

use ashiato_core::fixture::fixture_window;
use ashiato_core::query::{DataAvailability, QueryBackend};
use ashiato_duckdb::{Analyzer, AnalyzerSettings, DuckDbBackend, EventStore, PipelineStore};
use ashiato_pipeline::Manifest;
use ashiato_testkit::record::RecordBuilder;
use chrono::TimeDelta;
use common::{LATENESS_MINUTES, Workspace, start_worker, write_records};
use serde_json::Value;

async fn run_pipeline(database: &Path, manifest: &Manifest) -> PipelineStore {
    let window = fixture_window();
    let settings = AnalyzerSettings {
        batch: TimeDelta::hours(1),
        allowed_lateness: TimeDelta::minutes(LATENESS_MINUTES),
        read_since: window.baseline_since(),
        fetch_max: 4,
        commit_every: 4,
    };
    let mut analyzer = Analyzer::open(
        PipelineStore::open(database).unwrap(),
        manifest.clone(),
        settings,
    )
    .unwrap()
    .with_queries(Vec::new(), window);
    let (mut reader, worker) = start_worker(manifest, &analyzer.resume_positions());
    analyzer.run(&mut reader).await.unwrap();
    worker.await.unwrap().unwrap();
    analyzer.into_store()
}

async fn batch_data(workspace: &Workspace) -> DataAvailability {
    let database = workspace.database("batch");
    EventStore::open(&database)
        .unwrap()
        .ingest_directory(workspace.logs())
        .unwrap();
    DuckDbBackend::open(&database)
        .unwrap()
        .data_availability(&fixture_window())
        .await
        .unwrap()
}

fn records(s3_data_events: bool) -> Vec<Value> {
    let window = fixture_window();
    let mut records = vec![
        RecordBuilder::new()
            .set(
                "eventTime",
                (window.baseline_since() + TimeDelta::days(1)).to_rfc3339(),
            )
            .build(),
        RecordBuilder::new()
            .set(
                "eventTime",
                (window.since() + TimeDelta::hours(1)).to_rfc3339(),
            )
            .build(),
        RecordBuilder::new()
            .set(
                "eventTime",
                (window.since() + TimeDelta::hours(3)).to_rfc3339(),
            )
            .set("eventSource", "s3.amazonaws.com")
            .set("eventName", "ListBuckets")
            .build(),
    ];
    if s3_data_events {
        records.push(
            RecordBuilder::new()
                .set(
                    "eventTime",
                    (window.since() + TimeDelta::hours(5)).to_rfc3339(),
                )
                .set("eventSource", "s3.amazonaws.com")
                .set("eventName", "GetObject")
                .set("eventCategory", "Data")
                .build(),
        );
    }
    records
}

#[tokio::test(flavor = "multi_thread")]
async fn the_data_counted_batch_by_batch_matches_the_data_of_all_events_at_once() {
    for s3_data_events in [false, true] {
        let workspace = Workspace::new();
        write_records(&workspace, records(s3_data_events), &fixture_window());
        let manifest = workspace.manifest();

        let store = run_pipeline(&workspace.database("pipeline"), &manifest).await;
        let expected = batch_data(&workspace).await;

        assert_eq!(
            expected,
            DataAvailability {
                events: if s3_data_events { 3 } else { 2 },
                s3_data_events,
            }
        );
        assert_eq!(store.data_availability().unwrap(), expected);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_workspace_without_batches_has_no_data() {
    let workspace = Workspace::new();
    let manifest = workspace.manifest();

    let store = run_pipeline(&workspace.database("pipeline"), &manifest).await;

    assert_eq!(
        store.data_availability().unwrap(),
        DataAvailability::default()
    );
}
