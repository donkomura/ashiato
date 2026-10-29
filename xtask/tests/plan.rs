use std::fs;
use std::path::Path;

use ashiato_pipeline::{PlacementMode, RunPlan, SourceLocation};
use chrono::{TimeZone, Utc};
use serde_yaml::Value;
use tempfile::TempDir;
use xtask::compose::Layout;
use xtask::plan::{PlanSpec, write_plan};

fn spec() -> PlanSpec {
    PlanSpec {
        run_id: "integration-a".to_owned(),
        bucket: "ashiato-integration".to_owned(),
        endpoint_url: "http://minio:9000".to_owned(),
        baseline_since: Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap(),
        since: Utc.with_ymd_and_hms(2026, 9, 8, 0, 0, 0).unwrap(),
        until: Utc.with_ymd_and_hms(2026, 9, 15, 0, 0, 0).unwrap(),
        batch: "1h".to_owned(),
        allowed_lateness: "30m".to_owned(),
        max_skew: "2h".to_owned(),
    }
}

fn load(dir: &Path, layout: Layout) -> (RunPlan, Value) {
    let workspace = dir.join(format!("{layout:?}"));
    fs::create_dir_all(&workspace).unwrap();
    let path = write_plan(&spec(), layout, &workspace).unwrap();
    assert_eq!(path, workspace.join("run.yaml"));
    let yaml = serde_yaml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    (RunPlan::load(&path).unwrap(), yaml)
}

#[test]
fn the_reference_and_the_distributed_runs_differ_only_in_placement_and_keep_the_digest() {
    let dir = TempDir::new().unwrap();
    let (reference, mut reference_yaml) = load(dir.path(), Layout::SingleProcess);
    for workers in 1..=4 {
        let (run, mut run_yaml) = load(dir.path(), Layout::Distributed { workers });

        assert_eq!(run.digest(), reference.digest(), "{workers} workers");
        assert_eq!(run.broker, reference.broker);
        for yaml in [&mut reference_yaml, &mut run_yaml] {
            yaml.as_mapping_mut().unwrap().remove("placement");
        }
        assert_eq!(run_yaml, reference_yaml, "{workers} workers");
    }
}

#[test]
fn each_layout_sets_its_placement() {
    let dir = TempDir::new().unwrap();
    let (reference, _) = load(dir.path(), Layout::SingleProcess);
    assert_eq!(reference.placement.mode, PlacementMode::SingleProcess);
    assert_eq!(reference.placement.nats_url, None);

    let (run, _) = load(dir.path(), Layout::Distributed { workers: 3 });
    assert_eq!(run.placement.mode, PlacementMode::Distributed);
    assert_eq!(run.placement.nats_url.as_deref(), Some("nats://nats:4222"));
    assert_eq!(run.placement.ingest_workers.get(), 3);
}

#[test]
fn the_plan_reads_the_dataset_from_minio_over_its_baseline_and_analysis_periods() {
    let dir = TempDir::new().unwrap();
    let (plan, _) = load(dir.path(), Layout::Distributed { workers: 4 });
    let spec = spec();

    assert_eq!(plan.run_id, "integration-a");
    assert_eq!(
        plan.source.location,
        SourceLocation::S3 {
            uri: "s3://ashiato-integration/AWSLogs/".to_owned(),
            endpoint_url: Some("http://minio:9000".to_owned()),
        }
    );
    let window = plan.analysis_window();
    assert_eq!(window.since(), spec.since);
    assert_eq!(window.until(), spec.until);
    assert_eq!(window.baseline_since(), spec.baseline_since);
    assert_eq!(plan.pipeline.batch, "1h".parse().unwrap());
    assert_eq!(plan.pipeline.allowed_lateness, "30m".parse().unwrap());
    assert_eq!(plan.pipeline.max_skew, Some("2h".parse().unwrap()));
}

#[test]
fn the_stream_fits_in_the_jetstream_store_of_the_test_machine() {
    let dir = TempDir::new().unwrap();
    let (plan, _) = load(dir.path(), Layout::Distributed { workers: 4 });

    assert_eq!(plan.broker.max_bytes, 4 << 30);
}
