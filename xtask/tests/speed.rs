use std::fs;
use std::process::Command;

use ashiato_testkit::dataset::{DatasetKind, DatasetSpec};
use tempfile::TempDir;
use xtask::cli::ScalingDataset;
use xtask::correctness::DATASET_SEED;
use xtask::docker::project_name;
use xtask::metrics::{AnalyzerMetrics, AnalyzerPhases, AnalyzerTimes, WorkerMetrics, WorkerTimes};
use xtask::scaling::{Run, ScalingError, StatsSample};
use xtask::schedule::scaling_runs;
use xtask::speed::{
    MeasureError, Options, REPORT_FILE, RUNS_DIR, STATS_LOG, TimedSample, collect_run,
    dataset_spec, parse_stats_log, scaling, stats_entry,
};

const START: i64 = 1_730_000_000_000;
const PROJECT: &str = "ashiato-integration-b-n2-1";

fn stats_line(name: &str, cpu: &str) -> String {
    format!(
        "{{\"BlockIO\":\"0B / 0B\",\"CPUPerc\":\"{cpu}\",\"Container\":\"0123456789ab\",\
         \"ID\":\"0123456789ab\",\"MemPerc\":\"1.25%\",\"MemUsage\":\"12.5MiB / 1GiB\",\
         \"Name\":\"{name}\",\"NetIO\":\"1.2kB / 3.4kB\",\"PIDs\":\"12\"}}"
    )
}

fn container(service: &str) -> String {
    format!("{PROJECT}-{service}-1")
}

fn sample(usage: &[(&str, f64)]) -> StatsSample {
    usage
        .iter()
        .map(|(name, percent)| ((*name).to_owned(), *percent))
        .collect()
}

fn timed(unix_ms: i64, usage: &[(&str, f64)]) -> TimedSample {
    TimedSample {
        unix_ms,
        sample: sample(usage),
    }
}

#[test]
fn the_scaling_command_generates_dataset_b_from_the_fixed_seed() {
    assert_eq!(
        dataset_spec(ScalingDataset::B, 0.5),
        DatasetSpec {
            kind: DatasetKind::B,
            seed: DATASET_SEED,
            scale: 0.5,
        }
    );
}

#[test]
fn the_stats_log_keeps_the_time_of_each_sample_and_reads_only_the_containers_of_the_run() {
    let first = [
        stats_line(&container("minio"), "120.5%"),
        stats_line("ashiato-integration-a-d4-minio-1", "99.0%"),
        stats_line("ashiato-integration-b-n2-10-nats-1", "98.0%"),
        stats_line("buildkit", "3.5%"),
        stats_line(&container("worker-2"), "--"),
        stats_line(&container("worker-1"), "97.25%"),
    ];
    let last = stats_line(&container("nats"), "12%");
    let log = [
        stats_entry(START, &(first.join("\n") + "\n")),
        stats_entry(START + 1_000, ""),
        stats_entry(START + 2_000, &last),
    ]
    .concat();

    assert!(first.iter().all(|line| log.contains(line.as_str())));
    assert_eq!(
        parse_stats_log(&log, PROJECT),
        Ok(vec![
            timed(START, &[("minio", 120.5), ("worker-1", 97.25)]),
            timed(START + 1_000, &[]),
            timed(START + 2_000, &[("nats", 12.0)]),
        ])
    );
    assert_eq!(parse_stats_log("", PROJECT), Ok(Vec::new()));
}

#[test]
fn a_broken_stats_log_is_an_error() {
    let header = stats_entry(START, "");
    let good = stats_line(&container("minio"), "1.0%");
    let cases = [
        (format!("{good}\n"), good.clone()),
        (format!("{header}not json\n"), "not json".to_owned()),
        (
            format!("{header}{}\n", stats_line(&container("minio"), "abc%")),
            stats_line(&container("minio"), "abc%"),
        ),
        (format!("{header}# soon\n{good}\n"), "# soon".to_owned()),
    ];
    for (log, line) in cases {
        assert_eq!(
            parse_stats_log(&log, PROJECT),
            Err(ScalingError::InvalidStats(line)),
            "{log}"
        );
    }
}

fn analyzer_line(unix_ms: i64, last: bool, start: i64, end: Option<i64>) -> String {
    let end = end.map_or_else(|| "null".to_owned(), |end| end.to_string());
    format!(
        "{{\"metrics\":\"analyzer\",\"unix_ms\":{unix_ms},\"final\":{last},\
         \"manifest_files\":24,\"ingested_files\":24,\
         \"global_watermark\":null,\"pending_rows\":0,\"pending_max_rows\":9,\
         \"time_ms\":{{\"fetch_wait\":10,\"commit\":20,\"batch\":30}},\
         \"phases_unix_ms\":{{\"first_plan_response\":{start},\
         \"last_end_committed\":{end},\"correlated\":null}}}}"
    )
}

fn worker_line(worker_id: u32, last: bool, decode: u64) -> String {
    format!(
        "{{\"metrics\":\"worker\",\"worker_id\":{worker_id},\"unix_ms\":{},\
         \"final\":{last},\"partitions\":[],\"time_ms\":{{\"fetch_wait\":1,\"decode\":{decode},\
         \"publish_wait\":3,\"rejected_wait\":4}}}}",
        START + 9_000
    )
}

#[test]
fn a_measured_run_takes_the_final_metrics_and_the_samples_during_the_ingestion() {
    let end = START + 5_000;
    let analyzer_log = [
        "starting".to_owned(),
        analyzer_line(START + 1_000, false, START, None),
        analyzer_line(START + 8_000, true, START, Some(end)),
    ]
    .join("\n");
    let worker_logs = vec![
        [worker_line(0, false, 1), worker_line(0, true, 20)].join("\n") + "\n",
        worker_line(1, true, 30),
    ];
    let samples = [
        timed(START - 1, &[("minio", 1.0)]),
        timed(START, &[("minio", 2.0)]),
        timed(START + 2_500, &[("minio", 3.0)]),
        timed(end, &[("minio", 4.0)]),
        timed(end + 1, &[("minio", 5.0)]),
    ];

    let run = collect_run(&analyzer_log, &worker_logs, &samples).unwrap();

    let worker = |worker_id, decode| WorkerMetrics {
        worker_id,
        unix_ms: START + 9_000,
        is_final: true,
        time_ms: WorkerTimes {
            fetch_wait: 1,
            decode,
            publish_wait: 3,
            rejected_wait: 4,
        },
    };
    assert_eq!(
        run,
        Run {
            workers: 2,
            analyzer: AnalyzerMetrics {
                unix_ms: START + 8_000,
                is_final: true,
                manifest_files: 24,
                ingested_files: 24,
                global_watermark: None,
                pending_rows: 0,
                pending_max_rows: 9,
                time_ms: AnalyzerTimes {
                    fetch_wait: 10,
                    commit: 20,
                    batch: 30,
                },
                phases_unix_ms: AnalyzerPhases {
                    first_plan_response: Some(START),
                    last_end_committed: Some(end),
                    correlated: None,
                },
            },
            worker_metrics: vec![worker(0, 20), worker(1, 30)],
            stats: vec![
                sample(&[("minio", 2.0)]),
                sample(&[("minio", 3.0)]),
                sample(&[("minio", 4.0)]),
            ],
        }
    );
}

#[test]
fn without_the_phases_of_the_ingestion_no_sample_is_kept() {
    let analyzer_log = analyzer_line(START + 8_000, true, START, None);
    let samples = [timed(START + 1_000, &[("minio", 1.0)])];

    let run = collect_run(&analyzer_log, &[worker_line(0, true, 2)], &samples).unwrap();

    assert_eq!(run.stats, Vec::<StatsSample>::new());
}

#[test]
fn a_run_without_the_final_metrics_of_every_role_is_not_measured() {
    let analyzer = analyzer_line(START + 8_000, true, START, Some(START + 5_000));
    let worker = worker_line(0, true, 2);
    let cases = [
        (
            String::new(),
            vec![worker.clone()],
            MeasureError::NoAnalyzerMetrics,
        ),
        (
            analyzer_line(START + 1_000, false, START, None),
            vec![worker.clone()],
            MeasureError::NoAnalyzerMetrics,
        ),
        (
            analyzer.clone(),
            vec![worker.clone(), "Error: lost".to_owned()],
            MeasureError::NoWorkerMetrics(2),
        ),
        (
            analyzer.clone(),
            vec![worker_line(0, false, 2), worker.clone()],
            MeasureError::NoWorkerMetrics(1),
        ),
    ];
    for (analyzer_log, worker_logs, error) in cases {
        assert_eq!(
            collect_run(&analyzer_log, &worker_logs, &[]),
            Err(error.clone()),
            "{error}"
        );
    }
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

// Builds ashiato in release and runs R0 and one warm-up and one measured run for one and two
// workers on a small dataset B in Docker. A dataset this small says nothing about the efficiency,
// so only the procedure is checked.
#[test]
#[ignore = "needs Docker and takes several minutes"]
fn every_run_of_a_small_dataset_b_matches_r0_and_gives_the_speed_of_each_number_of_workers() {
    let dir = TempDir::new().unwrap();
    let options = Options {
        dataset: dataset_spec(ScalingDataset::B, 0.005),
        integration_dir: dir.path().to_path_buf(),
        workers: vec![1, 2],
        repeat: 1,
    };

    let report = scaling(&options).unwrap();

    let markdown = fs::read_to_string(dir.path().join(REPORT_FILE)).unwrap();
    if !report.runs.passed() || !report.unmeasured.is_empty() || report.scaling.is_err() {
        let kept = dir.keep();
        panic!("the runs are kept in {}\n{markdown}", kept.display());
    }
    assert_eq!(markdown, report.markdown());
    let levels: Vec<u32> = report
        .scaling
        .as_ref()
        .unwrap()
        .levels
        .iter()
        .map(|level| level.workers)
        .collect();
    assert_eq!(levels, [1, 2]);
    for run in scaling_runs(&options.workers, options.repeat) {
        let run_dir = dir.path().join(RUNS_DIR).join(run.spec.name);
        assert!(run_dir.join("logs").join("analyzer.stdout").is_file());
        assert!(run_dir.join("logs").join(STATS_LOG).is_file());
        assert_eq!(containers_of(&project_name("b", run.spec.name)), "");
    }
    assert!(dir.path().join("datasets").join("b-0.005").is_dir());
}
