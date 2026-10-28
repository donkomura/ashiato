mod common;

use std::num::NonZeroU32;
use std::path::Path;

use ashiato_core::fixture::fixture_window;
use ashiato_core::rule::{Rule, RuleSource, Rules, SignalRule};
use ashiato_duckdb::{Analyzer, AnalyzerSettings, PipelineStore};
use ashiato_pipeline::{Manifest, PartitionId, assign_partitions};
use ashiato_testkit::fixture::builtin_fixtures;
use chrono::TimeDelta;
use common::{LATENESS_MINUTES, Workspace, rows, start_workers, write_records};
use serde_json::{Value, json};

const TABLES: [&str; 12] = [
    "pending",
    "ingested_files",
    "partition_progress",
    "late_events",
    "batches",
    "recent_event_ids",
    "signals",
    "lineage_edges",
    "baseline",
    "baseline_principals",
    "data_counters",
    "incoming",
];

fn signal_rules() -> Vec<SignalRule> {
    Rules::new([RuleSource::builtin().unwrap()])
        .unwrap()
        .iter()
        .filter_map(|rule| match rule {
            Rule::Signal(rule) => Some(rule.clone()),
            Rule::Scenario(_) => None,
        })
        .collect()
}

const REGIONS: [&str; 4] = ["us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1"];

// Moves each fixture to one of several regions, so that the records make enough partitions to
// deal to several workers. Only the outputs of different placements are compared, so the
// verdicts of the fixtures do not matter here.
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
                    record["awsRegion"] = json!(REGIONS[fixture % REGIONS.len()]);
                    record
                })
        })
        .collect()
}

async fn run_with_workers(database: &Path, manifest: &Manifest, groups: &[Vec<usize>]) {
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
    .with_queries(signal_rules(), window);
    let (mut reader, workers) = start_workers(manifest, &analyzer.resume_positions(), groups);
    analyzer.run(&mut reader).await.unwrap();
    workers.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_number_of_workers_and_the_assignment_of_partitions_do_not_change_the_output() {
    let workspace = Workspace::new();
    write_records(&workspace, fixture_records(), &fixture_window());
    let manifest = workspace.manifest();
    let partitions: Vec<PartitionId> = manifest
        .partitions
        .iter()
        .map(|partition| partition.partition.clone())
        .collect();
    let count = partitions.len();
    assert!(count >= 3, "{count} partitions");
    let reference = workspace.database("one-worker");
    run_with_workers(&reference, &manifest, &[(0..count).collect()]).await;
    let reference_store = PipelineStore::open(&reference).unwrap();
    assert!(!reference_store.observed_signals().unwrap().is_empty());
    assert!(!reference_store.lineage_edges().unwrap().is_empty());
    let reference_signals = reference_store.observed_signals().unwrap();
    let reference_edges = reference_store.lineage_edges().unwrap();
    drop(reference_store);

    let mut placements: Vec<(String, Vec<Vec<usize>>)> = [2, 3, count + 1]
        .into_iter()
        .map(|workers| {
            let workers = NonZeroU32::new(u32::try_from(workers).unwrap()).unwrap();
            (
                format!("{workers} workers"),
                assign_partitions(&partitions, workers),
            )
        })
        .collect();
    placements.push((
        "an uneven assignment".to_owned(),
        vec![(1..count).rev().collect(), vec![0]],
    ));
    for (name, groups) in placements {
        let database = workspace.database(&name.replace(' ', "-"));
        run_with_workers(&database, &manifest, &groups).await;

        let store = PipelineStore::open(&database).unwrap();
        assert_eq!(
            store.observed_signals().unwrap(),
            reference_signals,
            "{name}"
        );
        assert_eq!(store.lineage_edges().unwrap(), reference_edges, "{name}");
        drop(store);
        for table in TABLES {
            assert_eq!(
                rows(&database, table),
                rows(&reference, table),
                "{table} with {name}"
            );
        }
    }
}
