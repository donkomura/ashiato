mod common;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use ashiato_ingest::{ListScope, list_local};
use ashiato_pipeline::{PartitionId, PlacementMode, RunPlan, validate_distributed};
use ashiato_testkit::minio::Minio;
use ashiato_testkit::nats::Nats;
use common::{Process, ashiato, finish};
use serde_json::Value;
use tempfile::TempDir;

const BUCKET: &str = "ashiato-example";
const RUN_ID: &str = "example";
const WORKERS: u32 = 2;
const MINIO_ENDPOINT: &str = "http://127.0.0.1:9000";
const NATS_URL: &str = "nats://127.0.0.1:4222";
const ACCOUNT: &str = "111111111111";
const REGIONS: [&str; 2] = ["ap-northeast-1", "us-east-1"];
const EXPECTED_SIGNALS: [&str; 2] = ["cloudtrail_logging_disabled", "console_login_without_mfa"];

fn example_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/pipeline")
}

fn example_plan() -> PathBuf {
    example_dir().join("workspace/run.yaml")
}

fn make_logs(out: &Path) {
    let output = Command::new("sh")
        .arg(example_dir().join("make-logs.sh"))
        .arg(out)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn the_example_plan_is_a_distributed_plan_with_two_workers() {
    let plan = RunPlan::load(&example_plan()).unwrap();

    validate_distributed(&plan).unwrap();
    assert_eq!(plan.run_id, RUN_ID);
    assert_eq!(plan.placement.mode, PlacementMode::Distributed);
    assert_eq!(plan.placement.nats_url.as_deref(), Some(NATS_URL));
    assert_eq!(plan.placement.ingest_workers.get(), WORKERS);
}

#[test]
fn make_logs_writes_one_partition_per_region_in_the_analysis_window() {
    let plan = RunPlan::load(&example_plan()).unwrap();
    let dir = TempDir::new().unwrap();

    make_logs(dir.path());

    let listing = list_local(dir.path(), &ListScope::from_plan(&plan)).unwrap();
    assert!(
        listing.unrecognized.is_empty(),
        "{:?}",
        listing.unrecognized
    );
    let partitions: BTreeSet<PartitionId> = listing
        .manifest
        .partitions
        .iter()
        .inspect(|partition| assert!(!partition.files.is_empty(), "{partition:?}"))
        .map(|partition| partition.partition.clone())
        .collect();
    let expected: BTreeSet<PartitionId> = REGIONS
        .iter()
        .map(|region| PartitionId::new(ACCOUNT, *region))
        .collect();
    assert_eq!(partitions, expected);
}

// The servers of the example listen on fixed ports, but the test servers listen on free ones,
// so only the two addresses are rewritten and the rest of the example plan runs as written.
#[test]
#[ignore = "needs Docker for MinIO and NATS; see docs/nats-testing.md"]
fn the_example_reports_the_signals_written_in_its_readme() {
    let minio = Minio::start();
    let nats = Nats::start();
    let dir = TempDir::new().unwrap();
    let logs = dir.path().join("logs");
    make_logs(&logs);
    minio.create_bucket(BUCKET, true);
    minio.upload_dir(&logs, BUCKET);
    let example = fs::read_to_string(example_plan()).unwrap();
    assert!(example.contains(MINIO_ENDPOINT), "{example}");
    assert!(example.contains(NATS_URL), "{example}");
    let plan = dir.path().join("workspace/run.yaml");
    fs::create_dir_all(plan.parent().unwrap()).unwrap();
    fs::write(
        &plan,
        example
            .replace(MINIO_ENDPOINT, minio.endpoint())
            .replace(NATS_URL, &nats.url()),
    )
    .unwrap();

    let run = Process::spawn(
        ashiato(dir.path())
            .args(["run", "--plan"])
            .arg(&plan)
            .arg("--emit-signals")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped()),
    );
    let workers: Vec<Process> = (0..WORKERS)
        .map(|worker_id| {
            Process::spawn(
                ashiato(dir.path())
                    .args(["ingest-worker", "--nats"])
                    .arg(nats.url())
                    .args(["--run", RUN_ID, "--worker-id"])
                    .arg(worker_id.to_string())
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped()),
            )
        })
        .collect();
    for worker in workers {
        finish(worker);
    }
    let output = finish(run);

    let signals: BTreeSet<String> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|record| record["type"] == "signal")
        .map(|record| record["signal_id"].as_str().unwrap().to_owned())
        .collect();
    let expected: BTreeSet<String> = EXPECTED_SIGNALS.map(str::to_owned).into();
    assert_eq!(signals, expected);
}
