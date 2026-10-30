use std::collections::BTreeMap;
use std::time::Duration;

use ashiato_testkit::dataset::DatasetSummary;
use serde::Deserialize;

use crate::compose::Service;
use crate::metrics::{AnalyzerMetrics, AnalyzerTimes, WorkerMetrics, WorkerTimes};

// The criterion of 14.1.4 節 is E(N) >= 0.8 for N = 2, 3 and 4. 17 節 marks 0.8 as a provisional
// value to be revised after the first measurement. It is a percentage so that a run exactly on
// the criterion is judged in integer arithmetic.
pub const MIN_EFFICIENCY_PERCENT: u64 = 80;

// The CPU usage of each service in one `docker stats --no-stream` call, in percent of one CPU.
pub type StatsSample = BTreeMap<String, f64>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScalingError {
    #[error("the analyzer metrics have no {0} time")]
    MissingPhase(&'static str),
    #[error("the ingestion ended at {end} ms, which is not after it started at {start} ms")]
    EmptyIngestion { start: i64, end: i64 },
    #[error("{found} of the {workers} workers wrote metrics")]
    MissingWorkers { workers: u32, found: usize },
    #[error("invalid docker stats line {0:?}")]
    InvalidStats(String),
    #[error("no runs were measured")]
    NoRuns,
}

// One measured run, without the warm-up run (14.1.4 節). The metrics are the final lines of the
// analyzer and of every worker, and the stats are taken once a second while the run lasts.
#[derive(Debug, Clone, PartialEq)]
pub struct Run {
    pub workers: u32,
    pub analyzer: AnalyzerMetrics,
    pub worker_metrics: Vec<WorkerMetrics>,
    pub stats: Vec<StatsSample>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Throughput {
    pub bytes_per_second: f64,
    pub events_per_second: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CpuUsage {
    pub mean: f64,
    pub max: f64,
    pub samples: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Level {
    pub workers: u32,
    pub durations: Vec<Duration>,
    pub median: Duration,
    pub throughput: Throughput,
    pub speedup: Option<f64>,
    pub efficiency: Option<f64>,
    pub meets_criterion: Option<bool>,
    pub worker_times: WorkerTimes,
    pub analyzer_times: AnalyzerTimes,
    pub cpu: BTreeMap<String, CpuUsage>,
    pub servers_cpu: Option<CpuUsage>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Scaling {
    pub compressed_bytes: u64,
    pub events: u64,
    pub server_cpu_limit: f64,
    pub levels: Vec<Level>,
}

impl Scaling {
    // Without one worker there is no speedup to judge, and one worker alone checks nothing, so
    // neither passes.
    pub fn passed(&self) -> bool {
        let mut checked = self
            .levels
            .iter()
            .filter(|level| level.workers > 1)
            .peekable();
        checked.peek().is_some() && checked.all(|level| level.meets_criterion == Some(true))
    }
}

// The correlation takes as long whatever the number of workers, so T(N) ends at the last
// EndOfPartition (14.1.4 節).
pub fn ingest_duration(metrics: &AnalyzerMetrics) -> Result<Duration, ScalingError> {
    let phases = metrics.phases_unix_ms;
    let start = phases
        .first_plan_response
        .ok_or(ScalingError::MissingPhase("first_plan_response"))?;
    let end = phases
        .last_end_committed
        .ok_or(ScalingError::MissingPhase("last_end_committed"))?;
    end.checked_sub(start)
        .and_then(|millis| u64::try_from(millis).ok())
        .filter(|millis| *millis > 0)
        .map(Duration::from_millis)
        .ok_or(ScalingError::EmptyIngestion { start, end })
}

pub fn median(durations: &[Duration]) -> Option<Duration> {
    let mut sorted = durations.to_vec();
    sorted.sort_unstable();
    let middle = sorted.len() / 2;
    match sorted.len() {
        0 => None,
        len if len % 2 == 1 => Some(sorted[middle]),
        _ => Some((sorted[middle - 1] + sorted[middle]) / 2),
    }
}

// The tasks of a worker wait at the same time, so the times add up to more than the run took.
// The share is of the whole breakdown rather than of T(N) (14.1.5 節).
pub fn rejected_share(times: WorkerTimes) -> Option<f64> {
    let total = times.fetch_wait + times.decode + times.publish_wait + times.rejected_wait;
    share(times.rejected_wait, total)
}

fn share(part: u64, total: u64) -> Option<f64> {
    (total > 0).then(|| part as f64 / total as f64)
}

#[derive(Deserialize)]
struct StatsLine {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "CPUPerc")]
    cpu: String,
}

// One line of `docker stats --no-stream --format '{{json .}}'` per container.
pub fn parse_docker_stats(output: &str, project: &str) -> Result<StatsSample, ScalingError> {
    let mut sample = StatsSample::new();
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        let invalid = || ScalingError::InvalidStats(line.to_owned());
        let stats: StatsLine = serde_json::from_str(line).map_err(|_| invalid())?;
        // docker stats prints "--" for a container that has no reading yet or has stopped.
        if stats.cpu == "--" {
            continue;
        }
        let percent = stats
            .cpu
            .strip_suffix('%')
            .and_then(|percent| percent.parse::<f64>().ok())
            .filter(|percent| percent.is_finite() && *percent >= 0.0)
            .ok_or_else(invalid)?;
        sample.insert(service_name(&stats.name, project).to_owned(), percent);
    }
    Ok(sample)
}

// Compose names a container <project>-<service>-<index>. The project changes with every run, so
// the usage is kept by service to be pooled over the runs of a number of workers.
fn service_name<'a>(container: &'a str, project: &str) -> &'a str {
    container
        .strip_prefix(project)
        .and_then(|rest| rest.strip_prefix('-'))
        .and_then(|rest| rest.rsplit_once('-'))
        .filter(|(_, index)| !index.is_empty() && index.bytes().all(|byte| byte.is_ascii_digit()))
        .map_or(container, |(service, _)| service)
}

// docker stats counts 100% for each logical CPU, so the limit of physical core 0 is 100% times
// the logical CPUs that MinIO and NATS are pinned to (14.1.1 節).
pub fn evaluate(
    runs: &[Run],
    dataset: &DatasetSummary,
    server_cpus: usize,
) -> Result<Scaling, ScalingError> {
    if runs.is_empty() {
        return Err(ScalingError::NoRuns);
    }
    let mut grouped: BTreeMap<u32, Vec<(&Run, Duration)>> = BTreeMap::new();
    for run in runs {
        let duration = ingest_duration(&run.analyzer)?;
        if run.worker_metrics.len() != run.workers as usize {
            return Err(ScalingError::MissingWorkers {
                workers: run.workers,
                found: run.worker_metrics.len(),
            });
        }
        grouped
            .entry(run.workers)
            .or_default()
            .push((run, duration));
    }
    let one = grouped.get(&1).map(|runs| median_of(runs));
    Ok(Scaling {
        compressed_bytes: dataset.compressed_bytes,
        events: dataset.events,
        server_cpu_limit: 100.0 * server_cpus as f64,
        levels: grouped
            .into_iter()
            .map(|(workers, runs)| level(workers, &runs, one, dataset))
            .collect(),
    })
}

fn median_of(runs: &[(&Run, Duration)]) -> Duration {
    let durations: Vec<Duration> = runs.iter().map(|(_, duration)| *duration).collect();
    median(&durations).expect("every number of workers has a run")
}

fn level(
    workers: u32,
    runs: &[(&Run, Duration)],
    one: Option<Duration>,
    dataset: &DatasetSummary,
) -> Level {
    let median = median_of(runs);
    let seconds = median.as_secs_f64();
    let speedup = one.map(|one| one.as_secs_f64() / seconds);
    let mut worker_times = WorkerTimes::default();
    let mut analyzer_times = AnalyzerTimes::default();
    for (run, _) in runs {
        for metrics in &run.worker_metrics {
            let times = metrics.time_ms;
            worker_times.fetch_wait += times.fetch_wait;
            worker_times.decode += times.decode;
            worker_times.publish_wait += times.publish_wait;
            worker_times.rejected_wait += times.rejected_wait;
        }
        analyzer_times.fetch_wait += run.analyzer.time_ms.fetch_wait;
        analyzer_times.commit += run.analyzer.time_ms.commit;
        analyzer_times.batch += run.analyzer.time_ms.batch;
    }
    let samples: Vec<&StatsSample> = runs.iter().flat_map(|(run, _)| &run.stats).collect();
    let (cpu, servers_cpu) = cpu_usage(&samples);
    Level {
        workers,
        durations: runs.iter().map(|(_, duration)| *duration).collect(),
        median,
        throughput: Throughput {
            bytes_per_second: dataset.compressed_bytes as f64 / seconds,
            events_per_second: dataset.events as f64 / seconds,
        },
        speedup,
        efficiency: speedup.map(|speedup| speedup / f64::from(workers)),
        meets_criterion: one
            .filter(|_| workers > 1)
            .map(|one| meets_criterion(one, median, workers)),
        worker_times,
        analyzer_times,
        cpu,
        servers_cpu,
    }
}

// E(N) = T(1) / (N T(N)) >= MIN_EFFICIENCY_PERCENT / 100, multiplied out.
fn meets_criterion(one: Duration, many: Duration, workers: u32) -> bool {
    one.as_nanos() * 100
        >= u128::from(MIN_EFFICIENCY_PERCENT) * u128::from(workers) * many.as_nanos()
}

// MinIO and NATS share physical core 0, so their usage is added up in each sample to be compared
// with the limit of the core.
fn cpu_usage(samples: &[&StatsSample]) -> (BTreeMap<String, CpuUsage>, Option<CpuUsage>) {
    let servers = [Service::Minio.name(), Service::Nats.name()];
    let mut by_service: BTreeMap<&str, Vec<f64>> = BTreeMap::new();
    let mut on_servers = Vec::new();
    for sample in samples {
        for (service, percent) in *sample {
            by_service.entry(service).or_default().push(*percent);
        }
        let usage: Vec<f64> = servers
            .iter()
            .filter_map(|service| sample.get(service).copied())
            .collect();
        if !usage.is_empty() {
            on_servers.push(usage.iter().sum());
        }
    }
    let cpu = by_service
        .into_iter()
        .filter_map(|(service, values)| Some((service.to_owned(), summarize(&values)?)))
        .collect();
    (cpu, summarize(&on_servers))
}

fn summarize(values: &[f64]) -> Option<CpuUsage> {
    let max = values.iter().copied().reduce(f64::max)?;
    Some(CpuUsage {
        mean: values.iter().sum::<f64>() / values.len() as f64,
        max,
        samples: values.len(),
    })
}

pub fn report_section(scaling: &Scaling) -> String {
    todo!("{scaling:?}")
}
