use std::collections::BTreeMap;
use std::time::Duration;

use ashiato_testkit::dataset::{DatasetKind, DatasetSummary};
use chrono::{TimeZone, Utc};
use xtask::metrics::{AnalyzerMetrics, AnalyzerPhases, AnalyzerTimes, WorkerMetrics, WorkerTimes};
use xtask::scaling::{
    CpuUsage, MIN_EFFICIENCY_PERCENT, Run, ScalingError, StatsSample, Throughput, evaluate,
    ingest_duration, median, parse_docker_stats, rejected_share, report_section,
};

const START: i64 = 1_730_000_000_000;
const PROJECT: &str = "ashiato-integration-b-n2-r1";

fn analyzer(start: Option<i64>, end: Option<i64>) -> AnalyzerMetrics {
    AnalyzerMetrics {
        unix_ms: START + 600_000,
        is_final: true,
        manifest_files: 10,
        ingested_files: 10,
        global_watermark: None,
        pending_rows: 0,
        pending_max_rows: 0,
        time_ms: AnalyzerTimes::default(),
        phases_unix_ms: AnalyzerPhases {
            first_plan_response: start,
            last_end_committed: end,
            correlated: end.map(|end| end + 60_000),
        },
    }
}

fn worker(worker_id: u32, times: WorkerTimes) -> WorkerMetrics {
    WorkerMetrics {
        worker_id,
        unix_ms: START + 600_000,
        is_final: true,
        time_ms: times,
    }
}

fn times(fetch_wait: u64, decode: u64, publish_wait: u64, rejected_wait: u64) -> WorkerTimes {
    WorkerTimes {
        fetch_wait,
        decode,
        publish_wait,
        rejected_wait,
    }
}

fn run(workers: u32, millis: i64) -> Run {
    Run {
        workers,
        analyzer: analyzer(Some(START), Some(START + millis)),
        worker_metrics: (0..workers)
            .map(|id| worker(id, WorkerTimes::default()))
            .collect(),
        stats: Vec::new(),
    }
}

fn dataset(compressed_bytes: u64, events: u64) -> DatasetSummary {
    let day = |day| Utc.with_ymd_and_hms(2024, 10, day, 0, 0, 0).unwrap();
    DatasetSummary {
        kind: DatasetKind::B,
        seed: 1,
        scale: 1.0,
        org_id: "o-example".to_owned(),
        accounts: vec!["111111111111".to_owned()],
        regions: vec!["us-east-1".to_owned()],
        files: 10,
        compressed_bytes,
        events,
        baseline_since: day(1),
        since: day(8),
        until: day(9),
        lookback: "7d".to_owned(),
        batch: "1h".to_owned(),
        allowed_lateness: "15m".to_owned(),
        max_skew: "1h".to_owned(),
        partitions: Vec::new(),
        expected_scenarios: Vec::new(),
    }
}

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

fn sample(usage: &[(&str, f64)]) -> StatsSample {
    usage
        .iter()
        .map(|(name, percent)| ((*name).to_owned(), *percent))
        .collect()
}

fn stats_line(name: &str, cpu: &str) -> String {
    format!(
        "{{\"BlockIO\":\"0B / 0B\",\"CPUPerc\":\"{cpu}\",\"Container\":\"0123456789ab\",\
         \"ID\":\"0123456789ab\",\"MemPerc\":\"1.25%\",\"MemUsage\":\"12.5MiB / 1GiB\",\
         \"Name\":\"{name}\",\"NetIO\":\"1.2kB / 3.4kB\",\"PIDs\":\"12\"}}"
    )
}

#[test]
fn the_criterion_is_the_provisional_efficiency_of_17() {
    assert_eq!(MIN_EFFICIENCY_PERCENT, 80);
}

#[test]
fn the_ingestion_time_runs_from_the_first_plan_response_to_the_last_end_committed() {
    let cases = [
        (Some(START), Some(START + 12_345), Ok(ms(12_345))),
        (Some(START), Some(START + 1), Ok(ms(1))),
        (
            None,
            Some(START),
            Err(ScalingError::MissingPhase("first_plan_response")),
        ),
        (
            Some(START),
            None,
            Err(ScalingError::MissingPhase("last_end_committed")),
        ),
        (
            None,
            None,
            Err(ScalingError::MissingPhase("first_plan_response")),
        ),
        (
            Some(START),
            Some(START),
            Err(ScalingError::EmptyIngestion {
                start: START,
                end: START,
            }),
        ),
        (
            Some(START),
            Some(START - 1),
            Err(ScalingError::EmptyIngestion {
                start: START,
                end: START - 1,
            }),
        ),
    ];
    for (start, end, expected) in cases {
        assert_eq!(
            ingest_duration(&analyzer(start, end)),
            expected,
            "{start:?} to {end:?}"
        );
    }
}

#[test]
fn the_median_of_an_even_number_of_runs_is_the_mean_of_the_middle_two() {
    let cases: [(&[u64], Option<Duration>); 7] = [
        (&[], None),
        (&[7_000], Some(ms(7_000))),
        (&[3_000, 1_000, 2_000], Some(ms(2_000))),
        (&[1_000, 9_000, 9_000], Some(ms(9_000))),
        (&[4_000, 1_000, 3_000, 2_000], Some(ms(2_500))),
        (&[5_000, 5_001], Some(Duration::from_micros(5_000_500))),
        (&[5_000, 1_000, 4_000, 2_000, 3_000], Some(ms(3_000))),
    ];
    for (millis, expected) in cases {
        let durations: Vec<Duration> = millis.iter().copied().map(ms).collect();
        assert_eq!(median(&durations), expected, "{millis:?}");
    }
}

#[test]
fn the_runs_of_each_number_of_workers_are_reduced_to_their_median() {
    let runs = [
        run(2, 6_500),
        run(1, 11_000),
        run(1, 9_000),
        run(2, 6_000),
        run(1, 10_000),
    ];

    let scaling = evaluate(&runs, &dataset(2_000_000, 1_000), 2).unwrap();

    let summary: Vec<(u32, Vec<Duration>, Duration)> = scaling
        .levels
        .iter()
        .map(|level| (level.workers, level.durations.clone(), level.median))
        .collect();
    assert_eq!(
        summary,
        [
            (1, vec![ms(11_000), ms(9_000), ms(10_000)], ms(10_000)),
            (2, vec![ms(6_500), ms(6_000)], ms(6_250)),
        ]
    );
    assert_eq!(scaling.compressed_bytes, 2_000_000);
    assert_eq!(scaling.events, 1_000);
}

#[test]
fn the_speed_is_the_compressed_bytes_and_the_events_over_the_median_time() {
    let cases = [
        (2_000_000, 1_000, &[2_500][..], 800_000.0, 400.0),
        (
            2_000_000,
            1_000,
            &[1_000, 3_000, 9_000],
            2_000_000.0 / 3.0,
            1_000.0 / 3.0,
        ),
        (5_000, 0, &[500], 10_000.0, 0.0),
    ];
    for (bytes, events, millis, bytes_per_second, events_per_second) in cases {
        let runs: Vec<Run> = millis.iter().map(|millis| run(1, *millis)).collect();
        let scaling = evaluate(&runs, &dataset(bytes, events), 2).unwrap();
        assert_eq!(
            scaling.levels[0].throughput,
            Throughput {
                bytes_per_second,
                events_per_second,
            },
            "{bytes} bytes and {events} events in {millis:?} ms"
        );
    }
}

#[test]
fn the_speedup_and_the_efficiency_are_relative_to_one_worker() {
    let runs = [run(1, 10_000), run(2, 6_250), run(4, 2_000)];

    let scaling = evaluate(&runs, &dataset(1, 1), 2).unwrap();

    let values: Vec<_> = scaling
        .levels
        .iter()
        .map(|level| {
            (
                level.workers,
                level.speedup,
                level.efficiency,
                level.meets_criterion,
            )
        })
        .collect();
    assert_eq!(
        values,
        [
            (1, Some(1.0), Some(1.0), None),
            (2, Some(1.6), Some(0.8), Some(true)),
            (4, Some(5.0), Some(1.25), Some(true)),
        ]
    );
    assert!(scaling.passed());
}

#[test]
fn an_efficiency_of_exactly_the_criterion_meets_it() {
    let cases = [
        (10_000, 2, 6_250, true),
        (10_000, 2, 6_251, false),
        (12_000, 3, 5_000, true),
        (12_000, 3, 5_001, false),
        (16_000, 4, 5_000, true),
        (16_000, 4, 5_001, false),
        (10_000, 2, 10_000, false),
        (10_000, 2, 15_000, false),
    ];
    for (one, workers, many, expected) in cases {
        let scaling = evaluate(&[run(1, one), run(workers, many)], &dataset(1, 1), 2).unwrap();
        let level = &scaling.levels[1];
        assert_eq!(level.workers, workers);
        assert_eq!(
            level.meets_criterion,
            Some(expected),
            "T(1) = {one} ms, T({workers}) = {many} ms"
        );
        assert_eq!(scaling.passed(), expected, "T({workers}) = {many} ms");
    }
}

#[test]
fn every_measured_number_of_workers_must_meet_the_criterion() {
    let cases: [(&[(u32, i64)], bool); 7] = [
        (&[(1, 12_000), (2, 7_000), (3, 5_000), (4, 3_750)], true),
        (&[(1, 12_000), (2, 7_000), (3, 5_001), (4, 3_750)], false),
        (&[(1, 12_000), (2, 7_600), (3, 5_000), (4, 3_750)], false),
        (&[(1, 12_000), (2, 7_000), (3, 5_000), (4, 3_751)], false),
        (&[(1, 12_000), (3, 5_000)], true),
        (&[(1, 12_000)], false),
        (&[(2, 7_000), (3, 5_000), (4, 3_750)], false),
    ];
    for (measured, expected) in cases {
        let runs: Vec<Run> = measured
            .iter()
            .map(|(workers, millis)| run(*workers, *millis))
            .collect();
        let scaling = evaluate(&runs, &dataset(1, 1), 2).unwrap();
        assert_eq!(scaling.passed(), expected, "{measured:?}");
    }
}

#[test]
fn without_one_worker_there_is_no_speedup_to_judge() {
    let runs = [run(2, 6_000), run(3, 4_000)];

    let scaling = evaluate(&runs, &dataset(1, 1), 2).unwrap();

    for level in &scaling.levels {
        assert_eq!(level.speedup, None, "{}", level.workers);
        assert_eq!(level.efficiency, None, "{}", level.workers);
        assert_eq!(level.meets_criterion, None, "{}", level.workers);
    }
    assert_eq!(scaling.levels[0].median, ms(6_000));
    assert!(!scaling.passed());
}

#[test]
fn runs_that_cannot_be_measured_fail_the_evaluation() {
    let mut no_end = run(1, 1_000);
    no_end.analyzer.phases_unix_ms.last_end_committed = None;
    let mut missing_worker = run(3, 1_000);
    missing_worker.worker_metrics.pop();
    let cases = [
        (vec![], ScalingError::NoRuns),
        (
            vec![run(1, 1_000), no_end],
            ScalingError::MissingPhase("last_end_committed"),
        ),
        (
            vec![run(1, 1_000), run(2, 0)],
            ScalingError::EmptyIngestion {
                start: START,
                end: START,
            },
        ),
        (
            vec![run(1, 1_000), missing_worker],
            ScalingError::MissingWorkers {
                workers: 3,
                found: 2,
            },
        ),
    ];
    for (runs, expected) in cases {
        assert_eq!(evaluate(&runs, &dataset(1, 1), 2), Err(expected.clone()));
    }
}

#[test]
fn the_rejected_wait_is_a_share_of_the_whole_breakdown_of_the_workers() {
    let cases = [
        (times(10, 20, 30, 40), Some(0.4)),
        (times(0, 0, 0, 5), Some(1.0)),
        (times(1, 1, 2, 0), Some(0.0)),
        (times(0, 0, 0, 0), None),
    ];
    for (times, expected) in cases {
        assert_eq!(rejected_share(times), expected, "{times:?}");
    }
}

#[test]
fn the_time_breakdowns_are_added_up_over_the_runs_of_each_number_of_workers() {
    let mut first = run(2, 5_000);
    first.worker_metrics = vec![
        worker(0, times(1, 2, 3, 4)),
        worker(1, times(10, 20, 30, 40)),
    ];
    first.analyzer.time_ms = AnalyzerTimes {
        fetch_wait: 100,
        commit: 200,
        batch: 300,
    };
    let mut second = run(2, 5_000);
    second.worker_metrics = vec![
        worker(1, times(100, 200, 300, 400)),
        worker(0, times(0, 0, 0, 1_000)),
    ];
    second.analyzer.time_ms = AnalyzerTimes {
        fetch_wait: 1,
        commit: 2,
        batch: 3,
    };
    let mut alone = run(1, 9_000);
    alone.worker_metrics = vec![worker(0, times(5, 5, 5, 0))];

    let scaling = evaluate(&[first, alone, second], &dataset(1, 1), 2).unwrap();

    assert_eq!(scaling.levels[0].worker_times, times(5, 5, 5, 0));
    assert_eq!(scaling.levels[0].analyzer_times, AnalyzerTimes::default());
    assert_eq!(scaling.levels[1].worker_times, times(111, 222, 333, 1_444));
    assert_eq!(
        scaling.levels[1].analyzer_times,
        AnalyzerTimes {
            fetch_wait: 101,
            commit: 202,
            batch: 303,
        }
    );
}

#[test]
fn docker_stats_lines_give_the_cpu_usage_of_each_service() {
    let output = [
        stats_line(&format!("{PROJECT}-minio-1"), "123.45%"),
        String::new(),
        stats_line(&format!("{PROJECT}-worker-2-1"), "0.00%"),
        stats_line(&format!("{PROJECT}-analyzer-1"), "--"),
        stats_line("buildkit", "3.5%"),
        stats_line(&format!("{PROJECT}-nats-1"), "201%"),
    ]
    .join("\n");

    assert_eq!(
        parse_docker_stats(&output, PROJECT),
        Ok(sample(&[
            ("minio", 123.45),
            ("worker-2", 0.0),
            ("buildkit", 3.5),
            ("nats", 201.0),
        ]))
    );
    assert_eq!(parse_docker_stats("", PROJECT), Ok(StatsSample::new()));
}

#[test]
fn a_broken_docker_stats_line_is_an_error() {
    let name = format!("{PROJECT}-minio-1");
    let lines = [
        stats_line(&name, "12.5"),
        stats_line(&name, "%"),
        stats_line(&name, "abc%"),
        stats_line(&name, "-1.0%"),
        stats_line(&name, "NaN%"),
        format!("{{\"Name\":\"{name}\"}}"),
        "{\"CPUPerc\":\"1.0%\"}".to_owned(),
        stats_line(&name, "1.0%")[..40].to_owned(),
        "minio 1.0%".to_owned(),
    ];
    for line in lines {
        let output = format!("{}\n{line}\n", stats_line(&name, "1.0%"));
        assert_eq!(
            parse_docker_stats(&output, PROJECT),
            Err(ScalingError::InvalidStats(line.clone())),
            "{line}"
        );
    }
}

#[test]
fn the_cpu_usage_is_pooled_over_the_runs_of_each_number_of_workers() {
    let mut first = run(1, 1_000);
    first.stats = vec![
        sample(&[
            ("minio", 50.0),
            ("nats", 30.0),
            ("analyzer", 90.0),
            ("worker-1", 100.0),
        ]),
        sample(&[
            ("minio", 150.0),
            ("nats", 40.0),
            ("analyzer", 70.0),
            ("worker-1", 98.0),
        ]),
    ];
    let mut second = run(1, 1_000);
    second.stats = vec![
        sample(&[("minio", 100.0), ("nats", 20.0), ("worker-1", 99.0)]),
        sample(&[("analyzer", 20.0)]),
    ];
    let mut other = run(2, 1_000);
    other.stats = vec![sample(&[("nats", 5.0)])];

    let scaling = evaluate(&[first, other, second], &dataset(1, 1), 2).unwrap();

    let level = &scaling.levels[0];
    let usage = |mean, max, samples| CpuUsage { mean, max, samples };
    assert_eq!(
        level.cpu,
        BTreeMap::from([
            ("analyzer".to_owned(), usage(60.0, 90.0, 3)),
            ("minio".to_owned(), usage(100.0, 150.0, 3)),
            ("nats".to_owned(), usage(30.0, 40.0, 3)),
            ("worker-1".to_owned(), usage(99.0, 100.0, 3)),
        ])
    );
    assert_eq!(level.servers_cpu, Some(usage(130.0, 190.0, 3)));
    assert_eq!(scaling.levels[1].servers_cpu, Some(usage(5.0, 5.0, 1)));
    assert_eq!(scaling.server_cpu_limit, 200.0);
}

#[test]
fn runs_without_stats_have_no_cpu_usage() {
    let scaling = evaluate(&[run(1, 1_000)], &dataset(1, 1), 1).unwrap();

    assert_eq!(scaling.levels[0].cpu, BTreeMap::new());
    assert_eq!(scaling.levels[0].servers_cpu, None);
    assert_eq!(scaling.server_cpu_limit, 100.0);
}

fn analyzer_times(fetch_wait: u64, commit: u64, batch: u64) -> AnalyzerTimes {
    AnalyzerTimes {
        fetch_wait,
        commit,
        batch,
    }
}

fn measured_run(
    workers: u32,
    millis: i64,
    worker_times: WorkerTimes,
    analyzer_times: AnalyzerTimes,
    stats: Vec<StatsSample>,
) -> Run {
    let mut run = run(workers, millis);
    run.worker_metrics = (0..workers).map(|id| worker(id, worker_times)).collect();
    run.analyzer.time_ms = analyzer_times;
    run.stats = stats;
    run
}

const REPORT: &str = "\
## 速度の確認

データセットは、圧縮後 3145728 バイト、1200 イベントである。合格の基準は、計測した N = 2、3、4 のすべてで E(N) が 0.80 以上であることである（0.80 は 17 節の仮の値）。

結果: 不合格（E(N) が基準を下回った N: 3）

### 取り込み時間と速度

| N | T(N) の中央値 (s) | 各回の T(N) (s) | MiB/s | イベント/s | S(N) | E(N) | 判定 |
|---|---|---|---|---|---|---|---|
| 1 | 12.000 | 12.000, 11.000, 13.000 | 0.25 | 100 | 1.00 | 1.00 | — |
| 2 | 7.500 | 7.000, 8.000 | 0.40 | 160 | 1.60 | 0.80 | 合格 |
| 3 | 6.000 | 6.000 | 0.50 | 200 | 2.00 | 0.67 | 不合格 |

### ワーカーの時間の内訳

N ごとに、すべての回とワーカーの時間を合計した。割合は内訳の合計に対する値である。

| N | S3 からの取得待ち | 展開・パース・正規化 | PubAck 待ち | JetStream の拒否による送信待ち |
|---|---|---|---|---|
| 1 | 100 ms (20.0%) | 300 ms (60.0%) | 50 ms (10.0%) | 50 ms (10.0%) |
| 2 | 400 ms (25.0%) | 400 ms (25.0%) | 400 ms (25.0%) | 400 ms (25.0%) |
| 3 | 0 ms (—) | 0 ms (—) | 0 ms (—) | 0 ms (—) |

### 分析コンシューマーの時間の内訳

N ごとに、すべての回の時間を合計した。割合は内訳の合計に対する値である。

| N | メッセージの取り出し待ち | pending への追加と確定 | バッチの処理 |
|---|---|---|---|
| 1 | 10 ms (10.0%) | 20 ms (20.0%) | 70 ms (70.0%) |
| 2 | 0 ms (—) | 0 ms (—) | 0 ms (—) |
| 3 | 0 ms (—) | 0 ms (—) | 0 ms (—) |

### コンテナごとの CPU 使用率

`docker stats` で 1 秒ごとに取った値で、100% が論理 CPU 1 つに当たる。物理コア 0 の行は MinIO と NATS の合計で、上限は 200% である。

| N | コンテナ | 平均 | 最大 | 標本の数 |
|---|---|---|---|---|
| 1 | 物理コア 0（minio + nats） | 80.0% | 80.0% | 1 |
| 1 | analyzer | 90.0% | 90.0% | 1 |
| 1 | minio | 50.0% | 50.0% | 1 |
| 1 | nats | 30.0% | 30.0% | 1 |
| 1 | worker-1 | 100.0% | 100.0% | 1 |
";

#[test]
fn the_report_lists_the_times_the_speed_and_the_values_to_find_the_bottleneck() {
    let none = WorkerTimes::default;
    let runs = [
        measured_run(
            1,
            12_000,
            times(100, 300, 50, 50),
            analyzer_times(10, 20, 70),
            vec![sample(&[
                ("minio", 50.0),
                ("nats", 30.0),
                ("analyzer", 90.0),
                ("worker-1", 100.0),
            ])],
        ),
        measured_run(1, 11_000, none(), AnalyzerTimes::default(), Vec::new()),
        measured_run(1, 13_000, none(), AnalyzerTimes::default(), Vec::new()),
        measured_run(
            2,
            7_000,
            times(50, 50, 50, 50),
            AnalyzerTimes::default(),
            Vec::new(),
        ),
        measured_run(
            2,
            8_000,
            times(150, 150, 150, 150),
            AnalyzerTimes::default(),
            Vec::new(),
        ),
        measured_run(3, 6_000, none(), AnalyzerTimes::default(), Vec::new()),
    ];

    let scaling = evaluate(&runs, &dataset(3_145_728, 1_200), 2).unwrap();

    assert_eq!(report_section(&scaling), REPORT);
}

#[test]
fn the_report_gives_the_reason_of_a_failure() {
    let cases: [(&[(u32, i64)], &str); 6] = [
        (
            &[(1, 12_000), (2, 7_000), (3, 5_000), (4, 3_750)],
            "結果: 合格\n",
        ),
        (
            &[(1, 12_000), (2, 7_600), (3, 5_000), (4, 3_751)],
            "結果: 不合格（E(N) が基準を下回った N: 2、4）\n",
        ),
        (
            &[(1, 12_000), (3, 5_001)],
            "結果: 不合格（E(N) が基準を下回った N: 3）\n",
        ),
        (
            &[(1, 12_000)],
            "結果: 不合格（N = 2、3、4 のいずれも計測していない）\n",
        ),
        (
            &[(2, 7_000), (3, 5_000)],
            "結果: 不合格（N = 1 を計測していないので S(N) を求められない）\n",
        ),
        (
            &[(2, 7_000)],
            "結果: 不合格（N = 1 を計測していないので S(N) を求められない）\n",
        ),
    ];
    for (measured, expected) in cases {
        let runs: Vec<Run> = measured
            .iter()
            .map(|(workers, millis)| run(*workers, *millis))
            .collect();
        let report = report_section(&evaluate(&runs, &dataset(1, 1), 2).unwrap());
        assert!(report.contains(expected), "{measured:?}\n{report}");
        assert_eq!(report.matches("結果: ").count(), 1, "{report}");
    }
}

#[test]
fn without_one_worker_or_stats_the_report_leaves_the_values_out() {
    let runs = [run(2, 6_000), run(3, 4_500)];

    let report = report_section(&evaluate(&runs, &dataset(1, 1), 2).unwrap());

    assert!(
        report.contains("| 2 | 6.000 | 6.000 | 0.00 | 0 | — | — | — |\n"),
        "{report}"
    );
    assert!(
        report.contains("| 3 | 4.500 | 4.500 | 0.00 | 0 | — | — | — |\n"),
        "{report}"
    );
    assert!(
        report.ends_with("### コンテナごとの CPU 使用率\n\nCPU 使用率は記録されていない。\n"),
        "{report}"
    );
}
