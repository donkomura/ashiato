use std::collections::BTreeSet;
use std::fs;
use std::io::{self, BufWriter, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use ashiato_core::AnalysisWindow;
use ashiato_core::fixture::{Fixture, FixtureFile, builtin_fixtures};
use ashiato_core::lineage::LineageWarning;
use ashiato_core::output::{JsonLinesSink, OutputRecord, RunRecord, Sink};
use ashiato_core::rule::{RuleFile, RuleSet, Rules};
use ashiato_core::validate::validate;
use ashiato_duckdb::fixture::{Verdict, run_fixture};
use ashiato_duckdb::{DuckDbBackend, EventStore};
use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta, Utc};
use clap::{Parser, Subcommand};

const DATABASE_FILE: &str = "ashiato.duckdb";
const RULE_EXTENSION: &str = "yaml";
const FIXTURE_EXTENSION: &str = "json";
const BASELINE_LOOKBACK_DAYS: i64 = 30;
const DEFAULT_INGEST_JOBS: NonZeroUsize = NonZeroUsize::new(4).unwrap();

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
        #[arg(long, default_value_t = DEFAULT_INGEST_JOBS)]
        jobs: NonZeroUsize,
    },
    Analyze(AnalyzeArgs),
    Rules {
        #[command(subcommand)]
        command: RulesCommand,
    },
}

#[derive(Subcommand)]
enum RulesCommand {
    Validate(RulesArgs),
    Test(RulesArgs),
}

#[derive(clap::Args)]
struct RulesArgs {
    #[arg(long)]
    rules: Option<PathBuf>,
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
        Command::Ingest {
            workspace,
            source,
            jobs,
        } => ingest(&workspace, &source, jobs),
        Command::Analyze(args) => analyze(&args),
        Command::Rules {
            command: RulesCommand::Validate(args),
        } => validate_rules(&args),
        Command::Rules {
            command: RulesCommand::Test(args),
        } => test_rules(&args),
    }
}

fn ingest(workspace: &Path, source: &str, jobs: NonZeroUsize) -> anyhow::Result<()> {
    if source.starts_with("s3://") {
        bail!("S3 sources are not supported yet: {source}");
    }
    fs::create_dir_all(workspace)
        .with_context(|| format!("failed to create workspace {}", workspace.display()))?;
    let store = EventStore::open(&workspace.join(DATABASE_FILE))?;
    let summary = store.ingest_directory_with_jobs(Path::new(source), jobs)?;
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
    let rules = Rules::new(rule_sets(args.rules.as_deref())?)?;
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

fn validate_rules(args: &RulesArgs) -> anyhow::Result<()> {
    let rules = Rules::new(rule_sets(args.rules.as_deref())?)?;
    let errors = validate(&rules);
    for error in &errors {
        eprintln!("error: {error}");
    }
    if !errors.is_empty() {
        bail!("{} problems found in the rules", errors.len());
    }
    eprintln!("validated {} rules", rules.iter().count());
    Ok(())
}

fn test_rules(args: &RulesArgs) -> anyhow::Result<()> {
    let sets = rule_sets(args.rules.as_deref())?;
    let overridden: BTreeSet<String> = sets[1..]
        .iter()
        .flat_map(RuleSet::rules)
        .map(|rule| rule.id().to_owned())
        .collect();
    let rules = Rules::new(sets)?;
    let mut fixtures: Vec<Fixture> = builtin_fixtures()?
        .into_iter()
        .filter(|fixture| !overridden.contains(&fixture.rule_id))
        .collect();
    if let Some(dir) = &args.rules {
        for (path, content) in read_files(dir, FIXTURE_EXTENSION)? {
            fixtures.push(Fixture::parse(&FixtureFile { path, content })?);
        }
    }

    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let mut failures = 0;
    for fixture in &fixtures {
        let report = runtime.block_on(run_fixture(&rules, fixture))?;
        if matches!(report.verdict, Verdict::Failed { .. }) {
            failures += 1;
        }
        println!("{report}");
    }
    if failures > 0 {
        bail!("{failures} of {} fixtures failed", fixtures.len());
    }
    eprintln!("{} fixtures passed", fixtures.len());
    Ok(())
}

fn rule_sets(dir: Option<&Path>) -> anyhow::Result<Vec<RuleSet>> {
    let mut sets = vec![RuleSet::builtin()?];
    if let Some(dir) = dir {
        sets.push(load_rule_set(dir)?);
    }
    Ok(sets)
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
    let files = read_files(dir, RULE_EXTENSION)?
        .into_iter()
        .map(|(path, content)| RuleFile { path, content })
        .collect();
    Ok(RuleSet::parse(dir.to_string_lossy(), files)?)
}

fn read_files(root: &Path, extension: &str) -> anyhow::Result<Vec<(String, String)>> {
    let mut files = Vec::new();
    collect_files(root, root, extension, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_files(
    root: &Path,
    dir: &Path,
    extension: &str,
    files: &mut Vec<(String, String)>,
) -> anyhow::Result<()> {
    let entries =
        fs::read_dir(dir).with_context(|| format!("failed to read rules {}", dir.display()))?;
    for entry in entries {
        let path = entry?.path();
        if path.is_dir() {
            collect_files(root, &path, extension, files)?;
        } else if path.extension().is_some_and(|ext| ext == extension) {
            let content = fs::read_to_string(&path)
                .with_context(|| format!("failed to read {}", path.display()))?;
            let relative = path.strip_prefix(root).unwrap_or(&path);
            files.push((relative.to_string_lossy().into_owned(), content));
        }
    }
    Ok(())
}
