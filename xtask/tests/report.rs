use std::time::Duration;

use ashiato_testkit::dataset::{DatasetKind, DatasetSummary};
use chrono::{TimeZone, Utc};
use xtask::compare::{Comparison, TABLES, TableDifference, WorkspaceComparison};
use xtask::metrics::{AnalyzerMetrics, AnalyzerPhases, AnalyzerTimes, WorkerMetrics, WorkerTimes};
use xtask::report::{
    Environment, ImageVersion, Injection, ReferenceCheck, Report, RunResult, ScalingReport,
    Unmeasured,
};
use xtask::scaling::{Run, Scaling, evaluate, report_section};
use xtask::schedule::{RunSpec, correctness_runs, scaling_runs};
use xtask::topology::{CpuAssignment, Cpuset};

fn environment() -> Environment {
    Environment {
        cores: vec![
            Cpuset::new([0, 6]),
            Cpuset::new([1, 7]),
            Cpuset::new([2, 8]),
            Cpuset::new([3, 9]),
            Cpuset::new([4, 10]),
            Cpuset::new([5, 11]),
        ],
        cpus: CpuAssignment {
            servers: Cpuset::new([0, 6]),
            analyzer: Cpuset::new([1, 7]),
            workers: vec![
                Cpuset::new([2, 8]),
                Cpuset::new([3, 9]),
                Cpuset::new([4, 10]),
                Cpuset::new([5, 11]),
            ],
        },
        docker: "28.1.1".to_owned(),
        compose: "2.35.1".to_owned(),
        nats: ImageVersion {
            image: "nats:2.10.29".to_owned(),
            version: "nats-server: v2.10.29".to_owned(),
        },
        minio: ImageVersion {
            image: "bitnamilegacy/minio:2025.7.23".to_owned(),
            version: "minio version DEVELOPMENT.2025-07-23T15-54-02Z".to_owned(),
        },
        runtime_image: "debian:bookworm-20251229-slim".to_owned(),
    }
}

fn summary() -> DatasetSummary {
    DatasetSummary {
        kind: DatasetKind::A,
        seed: 1,
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

fn spec(name: &str) -> RunSpec {
    correctness_runs()
        .into_iter()
        .find(|run| run.name == name)
        .unwrap()
}

fn matching() -> Comparison {
    Comparison {
        output_matches: true,
        workspace: WorkspaceComparison {
            tables: TABLES
                .iter()
                .map(|table| TableDifference {
                    table: (*table).to_owned(),
                    only_in_reference: 0,
                    only_in_run: 0,
                })
                .collect(),
            pending_rows: 0,
        },
        stream_removed: true,
    }
}

fn passing(name: &str) -> RunResult {
    let spec = spec(name);
    RunResult {
        spec,
        elapsed: Duration::from_millis(12_340),
        analyzer_exit: Some(0),
        output_lines: 42,
        injection: spec.fault.map(|_| Injection {
            since_start: Duration::from_millis(5_250),
            ingested_files: 150,
            manifest_files: 440,
        }),
        comparison: (name != "r0").then(|| Ok(matching())),
        error: None,
    }
}

fn report(runs: Vec<RunResult>) -> Report {
    Report {
        dataset: "a".to_owned(),
        environment: environment(),
        summary: summary(),
        runs,
        reference_checks: None,
    }
}

fn all_passing() -> Vec<RunResult> {
    correctness_runs()
        .iter()
        .map(|run| passing(run.name))
        .collect()
}

fn mismatching() -> Comparison {
    let mut comparison = matching();
    comparison.output_matches = false;
    comparison.stream_removed = false;
    comparison.workspace.pending_rows = 3;
    comparison.workspace.tables[0].only_in_reference = 2;
    comparison.workspace.tables[3].only_in_run = 5;
    comparison
}

#[test]
fn a_run_passes_only_when_the_analyzer_ends_well_and_everything_matches_r0() {
    for run in all_passing() {
        assert!(run.passed(), "{}", run.spec.name);
    }

    let mut failed_analyzer = passing("d1");
    failed_analyzer.analyzer_exit = Some(1);
    let mut timed_out = passing("d2");
    timed_out.analyzer_exit = None;
    timed_out.error = Some("the run did not end within 600 s".to_owned());
    let mut differs = passing("d3");
    differs.comparison = Some(Ok(mismatching()));
    let mut not_compared = passing("d4");
    not_compared.comparison = Some(Err("no such table: signals".to_owned()));
    let mut missing_comparison = passing("d4");
    missing_comparison.comparison = None;
    let mut not_injected = passing("f2");
    not_injected.injection = None;
    let mut empty_reference = passing("r0");
    empty_reference.output_lines = 0;
    for run in [
        failed_analyzer,
        timed_out,
        differs,
        not_compared,
        missing_comparison,
        not_injected,
        empty_reference,
    ] {
        assert!(!run.passed(), "{run:?}");
    }
}

#[test]
fn the_report_fails_when_a_run_or_a_check_of_r0_fails() {
    assert!(report(all_passing()).passed());

    let mut runs = all_passing();
    runs[5].comparison = Some(Ok(mismatching()));
    assert!(!report(runs).passed());

    let mut checked = report(all_passing());
    checked.reference_checks = Some(vec![ReferenceCheck {
        name: "scenario_match".to_owned(),
        passed: true,
        detail: String::new(),
    }]);
    assert!(checked.passed());
    checked.reference_checks.as_mut().unwrap()[0].passed = false;
    assert!(!checked.passed());
}

#[test]
fn the_report_records_the_environment_and_the_dataset() {
    let markdown = report(all_passing()).markdown();

    for line in [
        "| 物理コア | 0,6 / 1,7 / 2,8 / 3,9 / 4,10 / 5,11 |",
        "| MinIO と NATS の CPU | 0,6 |",
        "| 分析コンシューマーの CPU | 1,7 |",
        "| ワーカー 1 の CPU | 2,8 |",
        "| ワーカー 4 の CPU | 5,11 |",
        "| Docker | 28.1.1 |",
        "| Docker Compose | 2.35.1 |",
        "| NATS | nats:2.10.29（nats-server: v2.10.29） |",
        "| MinIO | bitnamilegacy/minio:2025.7.23（minio version DEVELOPMENT.2025-07-23T15-54-02Z） |",
        "| ashiato を動かすイメージ | debian:bookworm-20251229-slim |",
        "| データセット | a |",
        "| 種 | 1 |",
        "| scale | 0.01 |",
        "| ファイル | 460 |",
        "| 圧縮後のバイト数 | 1234567 |",
        "| イベント | 89012 |",
        "| ベースラインの期間 | 2026-09-01T00:00:00Z〜2026-09-08T00:00:00Z |",
        "| 調査期間 | 2026-09-08T00:00:00Z〜2026-09-15T00:00:00Z |",
    ] {
        assert!(
            markdown.lines().any(|row| row == line),
            "{line}\n{markdown}"
        );
    }
}

#[test]
fn the_report_has_a_row_for_each_run_with_its_fault_and_its_result() {
    let mut runs = all_passing();
    runs[1].analyzer_exit = None;
    runs[1].error = Some("the run did not end within 600 s".to_owned());
    runs[6].injection = None;
    let markdown = report(runs).markdown();

    assert!(
        markdown.contains("結果: 不合格（9 件の実行のうち 2 件が不合格）"),
        "{markdown}"
    );
    for line in [
        "| 実行 | 配置 | 障害の注入 | 所要時間 | 分析コンシューマーの終了コード | 出力の行数 | 結果 |",
        "| R0 | single_process | なし | 12.3 秒 | 0 | 42 | 合格 |",
        "| D1 | distributed、ワーカー 1 | なし | 12.3 秒 | - | 42 | 不合格 |",
        "| D4 | distributed、ワーカー 4 | なし | 12.3 秒 | 0 | 42 | 合格 |",
        "| F1 | distributed、ワーカー 4 | 30% でワーカー 2 を止め、10 秒後に再起動する（5.2 秒後、150/440 ファイルで注入した） | 12.3 秒 | 0 | 42 | 合格 |",
        "| F2 | distributed、ワーカー 4 | 50% で NATS を再起動する（注入しなかった） | 12.3 秒 | 0 | 42 | 不合格 |",
        "| F3 | distributed、ワーカー 4 | 70% で分析コンシューマーを再起動する（5.2 秒後、150/440 ファイルで注入した） | 12.3 秒 | 0 | 42 | 合格 |",
        "| F4 | distributed、ワーカー 4 | ワーカー 3 を 60 秒遅れて起動する（5.2 秒後、150/440 ファイルで注入した） | 12.3 秒 | 0 | 42 | 合格 |",
    ] {
        assert!(
            markdown.lines().any(|row| row == line),
            "{line}\n{markdown}"
        );
    }
}

#[test]
fn the_report_lists_what_did_not_match_with_the_differences_of_each_table() {
    let mut runs = all_passing();
    runs[3].comparison = Some(Ok(mismatching()));
    runs[4].comparison = Some(Err("no such table: signals".to_owned()));
    runs[7].analyzer_exit = Some(1);
    runs[7].injection = None;
    runs[7].error = Some("the run did not end within 600 s".to_owned());
    let markdown = report(runs).markdown();
    let section = |name: &str| {
        let start = markdown
            .find(&format!("### {name}\n"))
            .unwrap_or_else(|| panic!("no section {name}\n{markdown}"));
        let rest = &markdown[start + 1..];
        let end = rest
            .find("\n#")
            .map_or(markdown.len(), |end| start + 1 + end);
        markdown[start..end].to_owned()
    };

    assert!(markdown.contains("結果: 不合格（9 件の実行のうち 3 件が不合格）"));
    assert!(!markdown.contains("### D1\n"), "{markdown}");
    assert!(!markdown.contains("### R0\n"), "{markdown}");

    let d3 = section("D3");
    for line in [
        "- 標準出力が R0 と一致しない。",
        "- pending に 3 行が残っている。",
        "- JetStream のストリームが残っている。",
        "| テーブル | R0 にだけある行 | この実行にだけある行 |",
        "| signals | 2 | 0 |",
        "| lineage_edges | 0 | 0 |",
        "| ingested_files | 0 | 5 |",
        "| partition_progress | 0 | 0 |",
    ] {
        assert!(d3.lines().any(|row| row == line), "{line}\n{d3}");
    }

    let d4 = section("D4");
    assert!(
        d4.lines()
            .any(|row| row == "- R0 と比べられなかった: no such table: signals"),
        "{d4}"
    );

    let f3 = section("F3");
    for line in [
        "- 分析コンシューマーが終了コード 1 で終わった。",
        "- 障害を注入する前に実行が終わった。",
        "- エラー: the run did not end within 600 s",
    ] {
        assert!(f3.lines().any(|row| row == line), "{line}\n{f3}");
    }
}

#[test]
fn the_report_passes_with_a_place_for_the_checks_of_r0() {
    let mut report = report(all_passing());
    let markdown = report.markdown();

    assert!(
        markdown.contains("結果: 合格（9 件の実行のうち 0 件が不合格）"),
        "{markdown}"
    );
    assert!(!markdown.contains("## 一致しなかった項目"), "{markdown}");
    assert!(
        markdown.contains("R0 の確認（14.1.3 節）は、まだ行っていない。"),
        "{markdown}"
    );

    report.reference_checks = Some(vec![
        ReferenceCheck {
            name: "scenario_match が正解の一覧と一致する".to_owned(),
            passed: true,
            detail: "4 件".to_owned(),
        },
        ReferenceCheck {
            name: "signals がバッチ処理と一致する".to_owned(),
            passed: false,
            detail: "R0 にだけある行が 2 行".to_owned(),
        },
    ]);
    let markdown = report.markdown();
    assert!(markdown.contains("結果: 不合格"), "{markdown}");
    for line in [
        "| 確認 | 結果 | 詳細 |",
        "| scenario_match が正解の一覧と一致する | 合格 | 4 件 |",
        "| signals がバッチ処理と一致する | 不合格 | R0 にだけある行が 2 行 |",
    ] {
        assert!(
            markdown.lines().any(|row| row == line),
            "{line}\n{markdown}"
        );
    }
}

const START: i64 = 1_730_000_000_000;

fn measured(workers: u32, millis: i64) -> Run {
    Run {
        workers,
        analyzer: AnalyzerMetrics {
            unix_ms: START + 60_000,
            is_final: true,
            manifest_files: 24,
            ingested_files: 24,
            global_watermark: None,
            pending_rows: 0,
            pending_max_rows: 0,
            time_ms: AnalyzerTimes::default(),
            phases_unix_ms: AnalyzerPhases {
                first_plan_response: Some(START),
                last_end_committed: Some(START + millis),
                correlated: None,
            },
        },
        worker_metrics: (0..workers)
            .map(|worker_id| WorkerMetrics {
                worker_id,
                unix_ms: START + 60_000,
                is_final: true,
                time_ms: WorkerTimes::default(),
            })
            .collect(),
        stats: Vec::new(),
    }
}

fn scaling(two_workers_ms: i64) -> Scaling {
    let mut summary = summary();
    summary.kind = DatasetKind::B;
    evaluate(
        &[measured(1, 10_000), measured(2, two_workers_ms)],
        &summary,
        2,
    )
    .unwrap()
}

fn scaling_report(two_workers_ms: i64) -> ScalingReport {
    let runs = scaling_runs(&[1, 2], 1)
        .into_iter()
        .map(|run| RunResult {
            spec: run.spec,
            elapsed: Duration::from_millis(12_340),
            analyzer_exit: Some(0),
            output_lines: 42,
            injection: None,
            comparison: (run.spec.name != "r0").then(|| Ok(matching())),
            error: None,
        })
        .collect();
    let mut runs = report(runs);
    runs.dataset = "b".to_owned();
    ScalingReport {
        runs,
        unmeasured: Vec::new(),
        scaling: Ok(scaling(two_workers_ms)),
    }
}

#[test]
fn the_scaling_passes_only_when_every_run_matches_r0_and_every_efficiency_meets_the_criterion() {
    assert!(scaling_report(6_000).passed());

    let slow = scaling_report(7_000);
    assert!(!slow.passed());

    let mut differs = scaling_report(6_000);
    differs.runs.runs[1].comparison = Some(Ok(mismatching()));
    assert!(!differs.passed());

    let mut unmeasured = scaling_report(6_000);
    unmeasured.unmeasured.push(Unmeasured {
        run: "n2-1",
        error: "the analyzer wrote no final metrics".to_owned(),
    });
    assert!(!unmeasured.passed());

    let mut not_evaluated = scaling_report(6_000);
    not_evaluated.scaling = Err("no runs were measured".to_owned());
    assert!(!not_evaluated.passed());
}

#[test]
fn the_scaling_report_records_the_environment_the_runs_and_the_speed() {
    let report = scaling_report(6_000);
    let markdown = report.markdown();

    assert!(
        markdown.starts_with(
            "# 統合テストの速度の確認（データセット b）\n\n\
             結果: 合格（一致の確認: 5 件の実行のうち 0 件が不合格。速度の確認: 合格）\n\n"
        ),
        "{markdown}"
    );
    for line in [
        "| MinIO と NATS の CPU | 0,6 |",
        "| Docker | 28.1.1 |",
        "| データセット | b |",
        "| 圧縮後のバイト数 | 1234567 |",
        "| R0 | single_process | なし | 12.3 秒 | 0 | 42 | 合格 |",
        "| N1-WARMUP | distributed、ワーカー 1 | なし | 12.3 秒 | 0 | 42 | 合格 |",
        "| N1-1 | distributed、ワーカー 1 | なし | 12.3 秒 | 0 | 42 | 合格 |",
        "| N2-WARMUP | distributed、ワーカー 2 | なし | 12.3 秒 | 0 | 42 | 合格 |",
        "| N2-1 | distributed、ワーカー 2 | なし | 12.3 秒 | 0 | 42 | 合格 |",
    ] {
        assert!(
            markdown.lines().any(|row| row == line),
            "{line}\n{markdown}"
        );
    }
    let section = report_section(report.scaling.as_ref().unwrap());
    assert!(
        markdown.ends_with(&format!(
            "\n{section}\nCPU 使用率の標本は、各回の取り込み時間 T(N) の間に取ったものに限った。\n"
        )),
        "{markdown}"
    );
    assert!(!markdown.contains("## R0 の確認"), "{markdown}");
    assert!(!markdown.contains("## 一致しなかった項目"), "{markdown}");
    assert!(
        !markdown.contains("## 計測値を読めなかった実行"),
        "{markdown}"
    );
}

#[test]
fn the_scaling_report_gives_the_runs_that_did_not_match_or_could_not_be_measured() {
    let mut report = scaling_report(7_000);
    report.runs.runs[2].comparison = Some(Ok(mismatching()));
    report.unmeasured.push(Unmeasured {
        run: "n2-1",
        error: "the analyzer wrote no final metrics".to_owned(),
    });
    let markdown = report.markdown();

    assert!(
        markdown.contains(
            "結果: 不合格（一致の確認: 5 件の実行のうち 1 件が不合格。速度の確認: 不合格）"
        ),
        "{markdown}"
    );
    assert!(markdown.contains("### N1-1\n"), "{markdown}");
    assert!(
        markdown.contains(
            "## 計測値を読めなかった実行\n\n- N2-1: the analyzer wrote no final metrics\n"
        ),
        "{markdown}"
    );
    assert!(markdown.contains("結果: 不合格（E(N) が基準を下回った N: 2）"));

    report.scaling = Err("no runs were measured".to_owned());
    let markdown = report.markdown();
    assert!(
        markdown.contains("速度の確認: 計算できなかった）"),
        "{markdown}"
    );
    assert!(
        markdown.ends_with("## 速度の確認\n\n速度を計算できなかった: no runs were measured\n"),
        "{markdown}"
    );
}
