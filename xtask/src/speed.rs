use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Context;
use ashiato_testkit::dataset::{DatasetKind, DatasetSpec};
use chrono::Utc;
use serde::Deserialize;

use crate::cli::ScalingDataset;
use crate::compose::{RunDir, Service};
use crate::correctness::{self, DATASET_SEED, plan_spec, run_id, run_timeout};
use crate::docker::{dataset_name, project_name};
use crate::metrics::{AnalyzerMetrics, latest_analyzer, latest_worker};
use crate::report::{Report, ScalingReport, Unmeasured};
use crate::runner::{
    CPU_DIR, Runner, build_ashiato, compare, docker, environment, place_dataset, prepare_dataset,
    strings,
};
use crate::scaling::{Run, ScalingError, StatsSample, evaluate, parse_docker_stats};
use crate::schedule::{REFERENCE, scaling_runs};
use crate::topology::{assign_cpus, read_physical_cores};

pub const REPORT_FILE: &str = "scaling-report.md";
pub const RUNS_DIR: &str = "scaling";
pub const STATS_LOG: &str = "docker-stats.log";

const STATS_INTERVAL: Duration = Duration::from_secs(1);
const SAMPLE_HEADER: &str = "# ";

#[derive(Debug, Clone, PartialEq)]
pub struct Options {
    pub dataset: DatasetSpec,
    pub integration_dir: PathBuf,
    pub workers: Vec<u32>,
    pub repeat: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MeasureError {
    #[error("the analyzer wrote no final metrics")]
    NoAnalyzerMetrics,
    #[error("worker {0} wrote no final metrics")]
    NoWorkerMetrics(u32),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimedSample {
    pub unix_ms: i64,
    pub sample: StatsSample,
}

pub fn dataset_spec(dataset: ScalingDataset, scale: f64) -> DatasetSpec {
    let kind = match dataset {
        ScalingDataset::B => DatasetKind::B,
    };
    DatasetSpec {
        kind,
        seed: DATASET_SEED,
        scale,
    }
}

// The log keeps the output of docker stats as it is, including the containers of other runs, so
// that a contention for the CPUs can be seen in it afterwards.
pub fn stats_entry(unix_ms: i64, output: &str) -> String {
    let mut entry = format!("{SAMPLE_HEADER}{unix_ms}\n{output}");
    if !entry.ends_with('\n') {
        entry.push('\n');
    }
    entry
}

#[derive(Deserialize)]
struct Container {
    #[serde(rename = "Name")]
    name: String,
}

pub fn parse_stats_log(log: &str, project: &str) -> Result<Vec<TimedSample>, ScalingError> {
    let prefix = format!("{project}-");
    let mut entries: Vec<(i64, String)> = Vec::new();
    for line in log.lines().filter(|line| !line.trim().is_empty()) {
        let invalid = || ScalingError::InvalidStats(line.to_owned());
        if let Some(time) = line.strip_prefix(SAMPLE_HEADER) {
            entries.push((time.parse().map_err(|_| invalid())?, String::new()));
            continue;
        }
        let (_, lines) = entries.last_mut().ok_or_else(invalid)?;
        let container: Container = serde_json::from_str(line).map_err(|_| invalid())?;
        if container.name.starts_with(&prefix) {
            lines.push_str(line);
            lines.push('\n');
        }
    }
    entries
        .into_iter()
        .map(|(unix_ms, lines)| {
            Ok(TimedSample {
                unix_ms,
                sample: parse_docker_stats(&lines, project)?,
            })
        })
        .collect()
}

// Starting the containers and the correlation take as long whatever the number of workers, so
// only the samples within T(N) show what limited the ingestion (14.1.4 節).
fn during_ingestion(samples: &[TimedSample], analyzer: &AnalyzerMetrics) -> Vec<StatsSample> {
    let phases = analyzer.phases_unix_ms;
    let (Some(start), Some(end)) = (phases.first_plan_response, phases.last_end_committed) else {
        return Vec::new();
    };
    samples
        .iter()
        .filter(|sample| (start..=end).contains(&sample.unix_ms))
        .map(|sample| sample.sample.clone())
        .collect()
}

// The worker logs are in the order of the workers, numbered from 1 as in 14.1.3 節.
pub fn collect_run(
    analyzer_log: &str,
    worker_logs: &[String],
    samples: &[TimedSample],
) -> Result<Run, MeasureError> {
    let analyzer = latest_analyzer(analyzer_log)
        .filter(|metrics| metrics.is_final)
        .ok_or(MeasureError::NoAnalyzerMetrics)?;
    let worker_metrics = worker_logs
        .iter()
        .zip(1..)
        .map(|(log, number)| {
            latest_worker(log)
                .filter(|metrics| metrics.is_final)
                .ok_or(MeasureError::NoWorkerMetrics(number))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Run {
        workers: worker_metrics.len() as u32,
        stats: during_ingestion(samples, &analyzer),
        analyzer,
        worker_metrics,
    })
}

// Follows the steps of 14.1.6 節 for 14.1.4 節. The runs are kept under integration_dir/scaling,
// so that the R0 of dataset B does not replace the R0 of the correctness check.
pub fn scaling(options: &Options) -> anyhow::Result<ScalingReport> {
    let most_workers = options
        .workers
        .iter()
        .copied()
        .max()
        .context("no number of workers was given")?;
    let cores = read_physical_cores(Path::new(CPU_DIR))?;
    let cpus = assign_cpus(&cores, most_workers)?;
    let environment = environment(cores, cpus.clone())?;
    let (dataset, summary) = prepare_dataset(&correctness::Options {
        dataset: options.dataset,
        integration_dir: options.integration_dir.clone(),
    })?;
    let minio_volume = place_dataset(&options.dataset, &dataset)?;
    let ashiato = build_ashiato()?;
    let runner = Runner {
        integration_dir: options.integration_dir.join(RUNS_DIR),
        dataset: dataset_name(options.dataset.kind),
        cpus,
        ashiato,
        minio_volume,
        plan: plan_spec(&summary, &run_id(options.dataset.kind)),
        timeout: run_timeout(options.dataset.scale),
    };
    let mut runs = Vec::new();
    let mut measured = Vec::new();
    let mut unmeasured = Vec::new();
    for planned in scaling_runs(&options.workers, options.repeat) {
        let name = planned.spec.name;
        let (result, stats_log) = sample_stats(|| runner.execute(planned.spec));
        let run_dir = runner.run_dir(name);
        fs::create_dir_all(run_dir.logs())?;
        fs::write(run_dir.logs().join(STATS_LOG), &stats_log)?;
        if planned.measured {
            let project = project_name(runner.dataset, name);
            match measure(
                &run_dir,
                &project,
                planned.spec.layout.workers(),
                &stats_log,
            ) {
                Ok(run) => measured.push(run),
                Err(error) => unmeasured.push(Unmeasured {
                    run: name,
                    error: format!("{error:#}"),
                }),
            }
        }
        runs.push(result);
    }
    let reference = runner.run_dir(REFERENCE);
    for run in runs.iter_mut().filter(|run| run.spec.name != REFERENCE) {
        let compared = compare(
            &reference,
            &runner.run_dir(run.spec.name),
            &runner.plan.run_id,
        );
        run.comparison = Some(compared.map_err(|error| format!("{error:#}")));
    }
    let report = ScalingReport {
        scaling: evaluate(&measured, &summary, runner.cpus.servers.count())
            .map_err(|error| error.to_string()),
        runs: Report {
            dataset: runner.dataset.to_owned(),
            environment,
            summary,
            runs,
            reference_checks: None,
        },
        unmeasured,
    };
    let path = options.integration_dir.join(REPORT_FILE);
    fs::write(&path, report.markdown())
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(report)
}

fn measure(run_dir: &RunDir, project: &str, workers: u32, stats_log: &str) -> anyhow::Result<Run> {
    let read = |service: Service| {
        let path = run_dir.stderr(service);
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))
    };
    let analyzer_log = read(Service::Analyzer)?;
    let worker_logs = (1..=workers)
        .map(|number| read(Service::Worker(number)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let samples = parse_stats_log(stats_log, project)?;
    Ok(collect_run(&analyzer_log, &worker_logs, &samples)?)
}

// docker stats --no-stream waits for a second reading of the containers, so a sample can take
// longer than the interval, and the next one then starts at once. A failed call, as when a
// container goes away during it, only leaves a gap in the samples.
fn sample_stats<T>(run: impl FnOnce() -> T) -> (T, String) {
    let (stop, stopped) = mpsc::channel::<()>();
    thread::scope(|scope| {
        let sampler = scope.spawn(move || {
            let args = strings(&["stats", "--no-stream", "--format", "{{json .}}"]);
            let mut log = String::new();
            let mut next = Instant::now();
            loop {
                if let Ok(output) = docker(&args) {
                    log.push_str(&stats_entry(Utc::now().timestamp_millis(), &output));
                }
                next = (next + STATS_INTERVAL).max(Instant::now());
                let wait = next.saturating_duration_since(Instant::now());
                if stopped.recv_timeout(wait) != Err(RecvTimeoutError::Timeout) {
                    return log;
                }
            }
        });
        let value = run();
        drop(stop);
        let log = sampler.join().expect("the stats sampler does not panic");
        (value, log)
    })
}
