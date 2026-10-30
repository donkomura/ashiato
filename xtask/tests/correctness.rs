use std::fs;
use std::process::Command;
use std::time::Duration;

use ashiato_nats::validate_run_id;
use ashiato_testkit::dataset::{DatasetKind, DatasetSpec, DatasetSummary};
use chrono::{TimeZone, Utc};
use tempfile::TempDir;
use xtask::cli::CorrectnessDataset;
use xtask::correctness::{
    DATASET_SEED, Options, correctness, count_lines, dataset_spec, integration_dir, plan_spec,
    run_id, run_timeout,
};
use xtask::docker::{BUCKET, project_name};
use xtask::plan::PlanSpec;
use xtask::schedule::correctness_runs;

fn summary() -> DatasetSummary {
    DatasetSummary {
        kind: DatasetKind::A,
        seed: DATASET_SEED,
        scale: 0.01,
        org_id: "o-ashiato0001".to_owned(),
        accounts: Vec::new(),
        regions: Vec::new(),
        files: 460,
        compressed_bytes: 1_234_567,
        events: 89_012,
        baseline_since: Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap(),
        since: Utc.with_ymd_and_hms(2026, 9, 8, 0, 0, 0).unwrap(),
        until: Utc.with_ymd_and_hms(2026, 9, 15, 0, 0, 0).unwrap(),
        lookback: "7d".to_owned(),
        batch: "1h".to_owned(),
        allowed_lateness: "30m".to_owned(),
        max_skew: "1h".to_owned(),
        partitions: Vec::new(),
        expected_scenarios: Vec::new(),
    }
}

#[test]
fn the_datasets_of_the_command_are_generated_from_a_fixed_seed() {
    assert_eq!(
        dataset_spec(CorrectnessDataset::A, 0.01),
        DatasetSpec {
            kind: DatasetKind::A,
            seed: DATASET_SEED,
            scale: 0.01,
        }
    );
    assert_eq!(
        dataset_spec(CorrectnessDataset::ANoDataEvents, 1.0),
        DatasetSpec {
            kind: DatasetKind::ANoDataEvents,
            seed: DATASET_SEED,
            scale: 1.0,
        }
    );
}

#[test]
fn every_run_of_a_dataset_has_the_same_valid_run_id() {
    assert_eq!(run_id(DatasetKind::A), "integration-a");
    assert_eq!(
        run_id(DatasetKind::ANoDataEvents),
        "integration-a-no-data-events"
    );
    for kind in [DatasetKind::A, DatasetKind::ANoDataEvents, DatasetKind::B] {
        validate_run_id(&run_id(kind)).unwrap();
    }
}

#[test]
fn the_plan_reads_the_bucket_through_the_minio_of_the_run_over_the_periods_of_the_dataset() {
    let summary = summary();

    assert_eq!(
        plan_spec(&summary, "integration-a"),
        PlanSpec {
            run_id: "integration-a".to_owned(),
            bucket: BUCKET.to_owned(),
            endpoint_url: "http://minio:9000".to_owned(),
            baseline_since: summary.baseline_since,
            since: summary.since,
            until: summary.until,
            batch: "1h".to_owned(),
            allowed_lateness: "30m".to_owned(),
            max_skew: "1h".to_owned(),
        }
    );
}

#[test]
fn a_run_may_take_longer_with_a_larger_dataset_and_has_room_for_the_late_worker_of_f4() {
    let small = run_timeout(0.001);
    assert!(small >= Duration::from_secs(300), "{small:?}");
    assert!(run_timeout(1.0) > run_timeout(0.1));
    assert!(run_timeout(1.0) >= Duration::from_secs(1800));
}

#[test]
fn the_lines_of_an_output_are_counted_with_or_without_the_last_newline() {
    assert_eq!(count_lines(b""), 0);
    assert_eq!(count_lines(b"{}\n"), 1);
    assert_eq!(count_lines(b"{}\n{}\n"), 2);
    assert_eq!(count_lines(b"{}\n{}"), 2);
}

#[test]
fn the_runs_are_kept_under_the_target_directory_of_the_repository() {
    let dir = integration_dir();

    assert!(dir.ends_with("target/integration"), "{}", dir.display());
    assert!(
        dir.parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("Cargo.lock")
            .is_file()
    );
}

fn containers_of(project: &str) -> String {
    let output = Command::new("docker")
        .args([
            "ps",
            "--all",
            "--quiet",
            "--filter",
            &format!("label=com.docker.compose.project={project}"),
        ])
        .output()
        .unwrap();
    String::from_utf8(output.stdout).unwrap()
}

// Builds ashiato in release and runs the nine runs of 14.1.3 節 on a small dataset A in Docker,
// which takes several minutes.
#[test]
#[ignore = "needs Docker and takes several minutes"]
fn every_run_of_a_small_dataset_a_matches_r0() {
    let dir = TempDir::new().unwrap();
    let options = Options {
        dataset: dataset_spec(CorrectnessDataset::A, 0.005),
        integration_dir: dir.path().to_path_buf(),
    };

    let report = correctness(&options).unwrap();

    let markdown = fs::read_to_string(dir.path().join("report.md")).unwrap();
    if !report.passed() {
        // The logs of the runs are the only clue to a failure that does not happen every time.
        let kept = dir.keep();
        panic!("the runs are kept in {}\n{markdown}", kept.display());
    }
    assert_eq!(markdown, report.markdown());
    for run in correctness_runs() {
        let run_dir = dir.path().join(run.name);
        assert!(run_dir.join("logs").join("analyzer.stdout").is_file());
        assert!(run_dir.join("workspace").join("ashiato.duckdb").is_file());
        assert_eq!(containers_of(&project_name("a", run.name)), "");
    }
    assert!(dir.path().join("datasets").join("a-0.005").is_dir());
}
