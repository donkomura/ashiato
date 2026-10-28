mod common;

use std::path::Path;

use ashiato_core::fixture::fixture_window;
use ashiato_duckdb::{Analyzer, AnalyzerSettings, EventStore, PipelineStore};
use ashiato_pipeline::Manifest;
use ashiato_testkit::fixture::builtin_fixtures;
use chrono::TimeDelta;
use common::{LATENESS_MINUTES, Workspace, query_strings, start_worker, write_records};
use serde_json::{Value, json};

fn fixture_records() -> Vec<Value> {
    builtin_fixtures()
        .unwrap()
        .into_iter()
        .enumerate()
        .flat_map(|(fixture, file)| {
            file.records
                .into_iter()
                .enumerate()
                .map(move |(index, mut record)| {
                    record["eventID"] = json!(format!("fixture-{fixture:04}-{index:04}"));
                    record
                })
        })
        .collect()
}

async fn run_pipeline(database: &Path, manifest: &Manifest, batch: TimeDelta) {
    let window = fixture_window();
    let settings = AnalyzerSettings {
        batch,
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
}

fn timestamp(time: chrono::DateTime<chrono::Utc>) -> String {
    time.format("TIMESTAMP '%Y-%m-%d %H:%M:%S'").to_string()
}

// The baseline CTE of the batch processing (design.md 6.4) over all events at once.
fn batch_baseline(database: &Path) -> (Vec<String>, Vec<String>) {
    let window = fixture_window();
    let range = format!(
        "event_time >= {} AND event_time < {}",
        timestamp(window.baseline_since()),
        timestamp(window.baseline_until())
    );
    let baseline = query_strings(
        database,
        &format!(
            "SELECT CAST(to_json(b) AS VARCHAR) AS row FROM (
               SELECT principal_key, 'source_ip' AS dim, source_ip AS value FROM events
                WHERE {range} GROUP BY 1, 3
               UNION ALL
               SELECT principal_key, 'user_agent', user_agent FROM events
                WHERE {range} GROUP BY 1, 3
               UNION ALL
               SELECT principal_key, 'region', aws_region FROM events
                WHERE {range} GROUP BY 1, 3
               UNION ALL
               SELECT principal_key, 'event_name', event_name FROM events
                WHERE {range} GROUP BY 1, 3
             ) b ORDER BY row"
        ),
    );
    let principals = query_strings(
        database,
        &format!(
            "SELECT CAST(to_json(p) AS VARCHAR) AS row FROM (
               SELECT DISTINCT principal_key FROM events WHERE {range}
             ) p ORDER BY row"
        ),
    );
    (baseline, principals)
}

fn pipeline_baseline(database: &Path) -> (Vec<String>, Vec<String>) {
    (
        query_strings(
            database,
            "SELECT CAST(to_json(b) AS VARCHAR) AS row FROM baseline b ORDER BY row",
        ),
        query_strings(
            database,
            "SELECT CAST(to_json(p) AS VARCHAR) AS row FROM baseline_principals p ORDER BY row",
        ),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn the_baseline_tables_match_the_baseline_of_all_events_at_once() {
    let workspace = Workspace::new();
    write_records(&workspace, fixture_records(), &fixture_window());
    let manifest = workspace.manifest();
    let pipeline = workspace.database("pipeline");
    run_pipeline(&pipeline, &manifest, TimeDelta::hours(1)).await;
    let batch = workspace.database("batch");
    EventStore::open(&batch)
        .unwrap()
        .ingest_directory(workspace.logs())
        .unwrap();

    let expected = batch_baseline(&batch);

    assert!(!expected.0.is_empty());
    assert!(!expected.1.is_empty());
    assert_eq!(pipeline_baseline(&pipeline), expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_baseline_tables_do_not_depend_on_the_batch_length() {
    let workspace = Workspace::new();
    write_records(&workspace, fixture_records(), &fixture_window());
    let manifest = workspace.manifest();
    let hourly = workspace.database("hourly");
    let daily = workspace.database("daily");

    run_pipeline(&hourly, &manifest, TimeDelta::hours(1)).await;
    run_pipeline(&daily, &manifest, TimeDelta::hours(24)).await;

    assert_eq!(pipeline_baseline(&daily), pipeline_baseline(&hourly));
}
