use std::path::PathBuf;

use ashiato_testkit::dataset::{DatasetKind, DatasetSpec};
use serde::Deserialize;

use crate::cli::ScalingDataset;
use crate::correctness::DATASET_SEED;
use crate::metrics::{AnalyzerMetrics, latest_analyzer, latest_worker};
use crate::scaling::{Run, ScalingError, StatsSample, parse_docker_stats};

pub const REPORT_FILE: &str = "scaling-report.md";
pub const RUNS_DIR: &str = "scaling";
pub const STATS_LOG: &str = "docker-stats.log";

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

const SAMPLE_HEADER: &str = "# ";

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
