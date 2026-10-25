use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use ashiato_core::AnalysisWindow;
use ashiato_core::lineage::LineageWarning;
use ashiato_core::output::{JsonLinesSink, OutputRecord, RunRecord, Sink};
use ashiato_core::rule::{RuleFile, RuleSet, Rules};
use ashiato_duckdb::{DuckDbBackend, EventStore};
use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta, Utc};
use clap::{Parser, Subcommand};

const DATABASE_FILE: &str = "ashiato.duckdb";
const RULE_EXTENSION: &str = "yaml";
const BASELINE_LOOKBACK_DAYS: i64 = 30;

#[derive(Parser)]
#[command(name = "ashiato", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Ingest {
        #[arg(long)]
        workspace: PathBuf,
        #[arg(long)]
        source: String,
    },
    Analyze(AnalyzeArgs),
}

#[derive(clap::Args)]
struct AnalyzeArgs {
    #[arg(long)]
    workspace: PathBuf,
    #[arg(long, value_parser = parse_time)]
    since: DateTime<Utc>,
    #[arg(long, value_parser = parse_time)]
    until: DateTime<Utc>,
    #[arg(long)]
    rules: Option<PathBuf>,
    #[arg(long)]
    emit_signals: bool,
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Ingest { workspace, source } => ingest(&workspace, &source),
        Command::Analyze(args) => analyze(&args),
    }
}

fn ingest(workspace: &Path, source: &str) -> anyhow::Result<()> {
    if source.starts_with("s3://") {
        bail!("S3 sources are not supported yet: {source}");
    }
    fs::create_dir_all(workspace)
        .with_context(|| format!("failed to create workspace {}", workspace.display()))?;
    let store = EventStore::open(&workspace.join(DATABASE_FILE))?;
    let summary = store.ingest_directory(Path::new(source))?;
    eprintln!(
        "ingested {} files ({} skipped as already ingested), {} events",
        summary.files, summary.skipped_files, summary.events
    );
    Ok(())
}

fn analyze(args: &AnalyzeArgs) -> anyhow::Result<()> {
    let database = args.workspace.join(DATABASE_FILE);
    if !database.is_file() {
        bail!(
            "{} does not exist; run `ashiato ingest` first",
            database.display()
        );
    }
    let window = AnalysisWindow::new(
        args.since,
        args.until,
        TimeDelta::days(BASELINE_LOOKBACK_DAYS),
    )?;
    let mut sets = vec![RuleSet::builtin()?];
    if let Some(dir) = &args.rules {
        sets.push(load_rule_set(dir)?);
    }
    let rules = Rules::new(sets)?;
    let backend = DuckDbBackend::open(&database)?;
    let analysis = tokio::runtime::Builder::new_current_thread()
        .build()?
        .block_on(ashiato_core::analysis::analyze(&backend, &rules, &window))?;
    for warning in &analysis.warnings {
        report_warning(warning);
    }

    let mut sink = JsonLinesSink::new(BufWriter::new(io::stdout().lock()));
    sink.write(&OutputRecord::Run(RunRecord {
        tool_version: env!("CARGO_PKG_VERSION").to_owned(),
        since: window.since(),
        until: window.until(),
        rulesets: rules.rulesets().to_vec(),
    }))?;
    if args.emit_signals {
        for signal in analysis.signals {
            sink.write(&OutputRecord::Signal(signal))?;
        }
    }
    for found in analysis.matches {
        sink.write(&OutputRecord::ScenarioMatch(found))?;
    }
    sink.into_inner().flush()?;
    Ok(())
}

fn parse_time(text: &str) -> Result<DateTime<Utc>, String> {
    if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        return Ok(date.and_time(NaiveTime::MIN).and_utc());
    }
    DateTime::parse_from_rfc3339(text)
        .map(|time| time.with_timezone(&Utc))
        .map_err(|_| format!("expected YYYY-MM-DD or an RFC 3339 timestamp, but got {text}"))
}

fn report_warning(warning: &LineageWarning) {
    match warning {
        LineageWarning::Cycle { access_key_id } => {
            eprintln!("warning: the session lineage has a cycle at {access_key_id}");
        }
        LineageWarning::DepthLimit {
            access_key_id,
            max_depth,
        } => eprintln!(
            "warning: stopped tracing the session lineage at {access_key_id} after {max_depth} sessions"
        ),
    }
}

fn load_rule_set(dir: &Path) -> anyhow::Result<RuleSet> {
    let mut files = Vec::new();
    collect_rule_files(dir, dir, &mut files)?;
    Ok(RuleSet::parse(dir.to_string_lossy(), files)?)
}

fn collect_rule_files(root: &Path, dir: &Path, files: &mut Vec<RuleFile>) -> anyhow::Result<()> {
    let entries =
        fs::read_dir(dir).with_context(|| format!("failed to read rules {}", dir.display()))?;
    for entry in entries {
        let path = entry?.path();
        if path.is_dir() {
            collect_rule_files(root, &path, files)?;
        } else if path.extension().is_some_and(|ext| ext == RULE_EXTENSION) {
            let content = fs::read_to_string(&path)
                .with_context(|| format!("failed to read rule {}", path.display()))?;
            let relative = path.strip_prefix(root).unwrap_or(&path);
            files.push(RuleFile {
                path: relative.to_string_lossy().into_owned(),
                content,
            });
        }
    }
    Ok(())
}
