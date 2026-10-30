use std::fmt::Write;
use std::time::Duration;

use ashiato_testkit::dataset::DatasetSummary;
use chrono::{DateTime, SecondsFormat, Utc};

use crate::compare::Comparison;
use crate::compose::Layout;
use crate::scaling::Scaling;
use crate::schedule::{Fault, REFERENCE, RunSpec};
use crate::topology::{CpuAssignment, Cpuset};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageVersion {
    pub image: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Environment {
    pub cores: Vec<Cpuset>,
    pub cpus: CpuAssignment,
    pub docker: String,
    pub compose: String,
    pub nats: ImageVersion,
    pub minio: ImageVersion,
    pub runtime_image: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Injection {
    pub since_start: Duration,
    pub ingested_files: u64,
    pub manifest_files: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunResult {
    pub spec: RunSpec,
    pub elapsed: Duration,
    pub analyzer_exit: Option<i64>,
    pub output_lines: usize,
    pub injection: Option<Injection>,
    pub comparison: Option<Result<Comparison, String>>,
    pub error: Option<String>,
}

impl RunResult {
    // R0 is not compared with itself, but its output must not be empty (see outputs_match).
    pub fn passed(&self) -> bool {
        let ended = self.error.is_none() && self.analyzer_exit == Some(0);
        let injected = self.spec.fault.is_none() || self.injection.is_some();
        let matched = if self.spec.name == REFERENCE {
            self.output_lines > 0
        } else {
            matches!(&self.comparison, Some(Ok(comparison)) if comparison.passed())
        };
        ended && injected && matched
    }

    fn problems(&self) -> Vec<String> {
        let mut problems = Vec::new();
        if let Some(code) = self.analyzer_exit.filter(|code| *code != 0) {
            problems.push(format!(
                "分析コンシューマーが終了コード {code} で終わった。"
            ));
        }
        if self.spec.fault.is_some() && self.injection.is_none() {
            problems.push("障害を注入する前に実行が終わった。".to_owned());
        }
        if self.spec.name == REFERENCE && self.output_lines == 0 {
            problems.push("標準出力が空である。".to_owned());
        }
        match &self.comparison {
            Some(Ok(comparison)) => {
                if !comparison.output_matches {
                    problems.push("標準出力が R0 と一致しない。".to_owned());
                }
                if comparison.workspace.pending_rows > 0 {
                    problems.push(format!(
                        "pending に {} 行が残っている。",
                        comparison.workspace.pending_rows
                    ));
                }
                if !comparison.stream_removed {
                    problems.push("JetStream のストリームが残っている。".to_owned());
                }
            }
            Some(Err(error)) => problems.push(format!("R0 と比べられなかった: {error}")),
            None if self.spec.name != REFERENCE => {
                problems.push("R0 と比べていない。".to_owned());
            }
            None => {}
        }
        if let Some(error) = &self.error {
            problems.push(format!("エラー: {error}"));
        }
        problems
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Report {
    pub dataset: String,
    pub environment: Environment,
    pub summary: DatasetSummary,
    pub runs: Vec<RunResult>,
    pub reference_checks: Option<Vec<ReferenceCheck>>,
}

impl Report {
    pub fn passed(&self) -> bool {
        self.runs.iter().all(RunResult::passed)
            && self
                .reference_checks
                .iter()
                .flatten()
                .all(|check| check.passed)
    }

    pub fn markdown(&self) -> String {
        let mut out = format!(
            "# 統合テストの一致の確認（データセット {}）\n\n",
            self.dataset
        );
        let failed = self.runs.iter().filter(|run| !run.passed()).count();
        let _ = writeln!(
            out,
            "結果: {}（{} 件の実行のうち {failed} 件が不合格）\n",
            verdict(self.passed()),
            self.runs.len()
        );
        self.write_environment(&mut out);
        self.write_dataset(&mut out);
        self.write_runs(&mut out);
        self.write_reference_checks(&mut out);
        out
    }

    fn write_environment(&self, out: &mut String) {
        let env = &self.environment;
        let cores: Vec<String> = env.cores.iter().map(Cpuset::to_string).collect();
        let mut rows = vec![
            ("物理コア".to_owned(), cores.join(" / ")),
            (
                "MinIO と NATS の CPU".to_owned(),
                env.cpus.servers.to_string(),
            ),
            (
                "分析コンシューマーの CPU".to_owned(),
                env.cpus.analyzer.to_string(),
            ),
        ];
        rows.extend(
            env.cpus
                .workers
                .iter()
                .zip(1..)
                .map(|(cpus, number)| (format!("ワーカー {number} の CPU"), cpus.to_string())),
        );
        rows.extend([
            ("Docker".to_owned(), env.docker.clone()),
            ("Docker Compose".to_owned(), env.compose.clone()),
            ("NATS".to_owned(), image(&env.nats)),
            ("MinIO".to_owned(), image(&env.minio)),
            (
                "ashiato を動かすイメージ".to_owned(),
                env.runtime_image.clone(),
            ),
        ]);
        write_table(out, "環境", &rows);
    }

    fn write_dataset(&self, out: &mut String) {
        let summary = &self.summary;
        let rows = [
            ("データセット", self.dataset.clone()),
            ("種", summary.seed.to_string()),
            ("scale", summary.scale.to_string()),
            ("ファイル", summary.files.to_string()),
            ("圧縮後のバイト数", summary.compressed_bytes.to_string()),
            ("イベント", summary.events.to_string()),
            (
                "ベースラインの期間",
                period(summary.baseline_since, summary.since),
            ),
            ("調査期間", period(summary.since, summary.until)),
            (
                "埋め込んだシナリオ",
                summary.expected_scenarios.len().to_string(),
            ),
        ]
        .map(|(name, value)| (name.to_owned(), value));
        write_table(out, "データセット", &rows);
    }

    fn write_runs(&self, out: &mut String) {
        out.push_str(
            "## 実行\n\n\
             | 実行 | 配置 | 障害の注入 | 所要時間 | 分析コンシューマーの終了コード | 出力の行数 | 結果 |\n\
             |---|---|---|---|---|---|---|\n",
        );
        for run in &self.runs {
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} | {} | {} | {} |",
                run.spec.name.to_uppercase(),
                layout(run.spec.layout),
                injection(run),
                seconds(run.elapsed),
                run.analyzer_exit
                    .map_or_else(|| "-".to_owned(), |code| code.to_string()),
                run.output_lines,
                verdict(run.passed())
            );
        }
        out.push('\n');
        if self.runs.iter().all(RunResult::passed) {
            return;
        }
        out.push_str("## 一致しなかった項目\n\n");
        for run in self.runs.iter().filter(|run| !run.passed()) {
            let _ = writeln!(out, "### {}\n", run.spec.name.to_uppercase());
            for problem in run.problems() {
                let _ = writeln!(out, "- {problem}");
            }
            out.push('\n');
            if let Some(Ok(comparison)) = &run.comparison
                && !comparison.workspace.matches()
            {
                out.push_str(
                    "| テーブル | R0 にだけある行 | この実行にだけある行 |\n|---|---|---|\n",
                );
                for table in &comparison.workspace.tables {
                    let _ = writeln!(
                        out,
                        "| {} | {} | {} |",
                        table.table, table.only_in_reference, table.only_in_run
                    );
                }
                out.push('\n');
            }
        }
    }

    fn write_reference_checks(&self, out: &mut String) {
        out.push_str("## R0 の確認\n\n");
        let Some(checks) = &self.reference_checks else {
            out.push_str("R0 の確認（14.1.3 節）は、まだ行っていない。\n");
            return;
        };
        out.push_str("| 確認 | 結果 | 詳細 |\n|---|---|---|\n");
        for check in checks {
            let _ = writeln!(
                out,
                "| {} | {} | {} |",
                check.name,
                verdict(check.passed),
                check.detail
            );
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unmeasured {
    pub run: &'static str,
    pub error: String,
}

// The report of 14.1.4 節. Every run, the warm-up runs included, is compared with R0 as in
// 14.1.3 節, and the measured runs give the speed.
#[derive(Debug, Clone, PartialEq)]
pub struct ScalingReport {
    pub runs: Report,
    pub unmeasured: Vec<Unmeasured>,
    pub scaling: Result<Scaling, String>,
}

impl ScalingReport {
    pub fn passed(&self) -> bool {
        todo!()
    }

    pub fn markdown(&self) -> String {
        todo!()
    }
}

fn write_table(out: &mut String, title: &str, rows: &[(String, String)]) {
    let _ = write!(out, "## {title}\n\n| 項目 | 値 |\n|---|---|\n");
    for (name, value) in rows {
        let _ = writeln!(out, "| {name} | {value} |");
    }
    out.push('\n');
}

fn verdict(passed: bool) -> &'static str {
    if passed { "合格" } else { "不合格" }
}

fn image(image: &ImageVersion) -> String {
    format!("{}（{}）", image.image, image.version)
}

fn period(since: DateTime<Utc>, until: DateTime<Utc>) -> String {
    let time = |time: DateTime<Utc>| time.to_rfc3339_opts(SecondsFormat::Secs, true);
    format!("{}〜{}", time(since), time(until))
}

fn seconds(duration: Duration) -> String {
    format!("{:.1} 秒", duration.as_secs_f64())
}

fn layout(layout: Layout) -> String {
    match layout {
        Layout::SingleProcess => "single_process".to_owned(),
        Layout::Distributed { workers } => format!("distributed、ワーカー {workers}"),
    }
}

fn injection(run: &RunResult) -> String {
    let Some(fault) = run.spec.fault else {
        return "なし".to_owned();
    };
    let fault = match fault {
        Fault::RestartWorker {
            worker,
            percent,
            down_for,
        } => format!(
            "{percent}% でワーカー {worker} を止め、{} 秒後に再起動する",
            down_for.as_secs()
        ),
        Fault::RestartNats { percent } => format!("{percent}% で NATS を再起動する"),
        Fault::RestartAnalyzer { percent } => {
            format!("{percent}% で分析コンシューマーを再起動する")
        }
        Fault::DelayWorker { worker, delay } => {
            format!("ワーカー {worker} を {} 秒遅れて起動する", delay.as_secs())
        }
    };
    match run.injection {
        Some(injection) => format!(
            "{fault}（{}後、{}/{} ファイルで注入した）",
            seconds(injection.since_start),
            injection.ingested_files,
            injection.manifest_files
        ),
        None => format!("{fault}（注入しなかった）"),
    }
}
