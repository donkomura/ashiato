use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use ashiato_testkit::dataset::{DatasetKind, DatasetSpec, DatasetSummary};

use crate::cli::CorrectnessDataset;
use crate::compose::MINIO_ENDPOINT;
use crate::compose::{RunDir, Service};
use crate::docker::{BUCKET, dataset_name};
use crate::plan::PlanSpec;
use crate::reference::{compare_scenarios, compare_signals, scenario_check, signal_check};
use crate::report::{ReferenceCheck, Report, RunResult};
use crate::runner::{
    CPU_DIR, Runner, build_ashiato, compare, environment, place_dataset, prepare_dataset,
};
use crate::schedule::{REFERENCE, correctness_runs};
use crate::topology::{MAX_WORKERS, assign_cpus, read_physical_cores};

pub const DATASET_SEED: u64 = 1;

pub const REPORT_FILE: &str = "report.md";
const REFERENCE_WORK_DIR: &str = "reference-check";
// The base covers starting the containers and the late worker of F4; the hour per scale is far
// more than a single process needs for dataset A, so that only a stuck run reaches it.
const BASE_TIMEOUT: Duration = Duration::from_secs(300);
const TIMEOUT_PER_SCALE: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone, PartialEq)]
pub struct Options {
    pub dataset: DatasetSpec,
    pub integration_dir: PathBuf,
}

pub(crate) fn repository() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask is a directory of the repository")
}

pub fn integration_dir() -> PathBuf {
    repository().join("target").join("integration")
}

pub fn dataset_spec(dataset: CorrectnessDataset, scale: f64) -> DatasetSpec {
    let kind = match dataset {
        CorrectnessDataset::A => DatasetKind::A,
        CorrectnessDataset::ANoDataEvents => DatasetKind::ANoDataEvents,
    };
    DatasetSpec {
        kind,
        seed: DATASET_SEED,
        scale,
    }
}

// Every run of a dataset uses the same run_id, so that the outputs can match byte for byte; each
// run has a JetStream store of its own, so the streams do not meet.
pub fn run_id(kind: DatasetKind) -> String {
    format!("integration-{}", dataset_name(kind))
}

pub fn plan_spec(summary: &DatasetSummary, run_id: &str) -> PlanSpec {
    PlanSpec {
        run_id: run_id.to_owned(),
        bucket: BUCKET.to_owned(),
        endpoint_url: MINIO_ENDPOINT.to_owned(),
        baseline_since: summary.baseline_since,
        since: summary.since,
        until: summary.until,
        batch: summary.batch.clone(),
        allowed_lateness: summary.allowed_lateness.clone(),
        max_skew: summary.max_skew.clone(),
    }
}

pub fn run_timeout(scale: f64) -> Duration {
    BASE_TIMEOUT + TIMEOUT_PER_SCALE.mul_f64(scale)
}

pub fn count_lines(output: &[u8]) -> usize {
    let newlines = output.iter().filter(|byte| **byte == b'\n').count();
    newlines + usize::from(output.last().is_some_and(|byte| *byte != b'\n'))
}

// Follows the steps of 14.1.6 節. The runs leave their logs, outputs and DuckDB files under
// integration_dir/<run> whether they pass or not.
pub fn correctness(options: &Options) -> anyhow::Result<Report> {
    let cores = read_physical_cores(Path::new(CPU_DIR))?;
    let cpus = assign_cpus(&cores, MAX_WORKERS)?;
    let environment = environment(cores, cpus.clone())?;
    let (dataset, summary) = prepare_dataset(options)?;
    let minio_volume = place_dataset(&options.dataset, &dataset)?;
    let ashiato = build_ashiato()?;
    let runner = Runner {
        integration_dir: options.integration_dir.clone(),
        dataset: dataset_name(options.dataset.kind),
        cpus,
        ashiato,
        minio_volume,
        plan: plan_spec(&summary, &run_id(options.dataset.kind)),
        timeout: run_timeout(options.dataset.scale),
    };
    let mut runs: Vec<RunResult> = correctness_runs()
        .into_iter()
        .map(|spec| runner.execute(spec))
        .collect();
    let reference = runner.run_dir(REFERENCE);
    for run in runs.iter_mut().filter(|run| run.spec.name != REFERENCE) {
        let compared = compare(
            &reference,
            &runner.run_dir(run.spec.name),
            &runner.plan.run_id,
        );
        run.comparison = Some(compared.map_err(|error| format!("{error:#}")));
    }
    let reference_checks = check_reference(&reference, &dataset, &summary);
    let report = Report {
        dataset: runner.dataset.to_owned(),
        environment,
        summary,
        runs,
        reference_checks: Some(reference_checks),
    };
    let path = options.integration_dir.join(REPORT_FILE);
    fs::write(&path, report.markdown())
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(report)
}

// R0 is checked on its own, since every run would match a wrong R0 (14.1.3 節).
fn check_reference(
    reference: &RunDir,
    dataset: &Path,
    summary: &DatasetSummary,
) -> Vec<ReferenceCheck> {
    let output = fs::read(reference.stdout(Service::Analyzer)).unwrap_or_default();
    let scenarios = compare_scenarios(&output, &summary.expected_scenarios)
        .map_err(|error| format!("{error:#}"));
    // The run directory of R0 is created anew for every run, so no earlier work dir is left.
    let work_dir = reference.root().join(REFERENCE_WORK_DIR);
    let signals = compare_signals(&reference.database(), dataset, &work_dir)
        .map_err(|error| format!("{error:#}"));
    vec![scenario_check(scenarios), signal_check(signals)]
}
