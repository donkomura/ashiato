use std::collections::BTreeSet;
use std::fs;
use std::io::{self, BufWriter, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use ashiato_cli::s3::{AwsObjectStore, KeyFilter, S3Location, download};
use ashiato_core::analysis::Analysis;
use ashiato_core::context::Context as ContextFile;
use ashiato_core::lineage::LineageWarning;
use ashiato_core::output::{JsonLinesSink, OriginScoreRecord, OutputRecord, RunRecord, Sink};
use ashiato_core::rule::{Rule, RuleFile, RuleSource, Rules};
use ashiato_core::ruleset::{RulesetError, Rulesets};
use ashiato_core::scoring::{ScoringSettings, score};
use ashiato_core::validate::validate;
use ashiato_core::{AnalysisWindow, Severity};
use ashiato_duckdb::fixture::{Verdict, run_fixture};
use ashiato_duckdb::{DuckDbBackend, EventStore};
use ashiato_testkit::fixture::{Fixture, FixtureFile, builtin_fixtures};
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use clap::{Parser, Subcommand};

mod run;

const DATABASE_FILE: &str = "ashiato.duckdb";
const EVIDENCE_DIR: &str = "evidence";
const S3_SCHEME: &str = "s3://";
const RULE_EXTENSION: &str = "yaml";
const FIXTURE_EXTENSION: &str = "json";
const DEFAULT_INGEST_JOBS: NonZeroUsize = NonZeroUsize::new(4).unwrap();

#[derive(Parser)]
#[command(name = "ashiato", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Run(run::RunArgs),
    Ingest(IngestArgs),
    Analyze(AnalyzeArgs),
    Rules {
        #[command(subcommand)]
        command: RulesCommand,
    },
    Rulesets {
        #[command(subcommand)]
        command: RulesetsCommand,
    },
}

#[derive(Subcommand)]
enum RulesCommand {
    Validate(RulesArgs),
    Test(RulesArgs),
}

#[derive(Subcommand)]
enum RulesetsCommand {
    List(CatalogArgs),
    Show(ShowRulesetArgs),
}

#[derive(clap::Args)]
struct CatalogArgs {
    #[arg(long)]
    rules: Option<PathBuf>,
    #[arg(long)]
    rulesets: Option<PathBuf>,
}

#[derive(clap::Args)]
struct RulesArgs {
    #[command(flatten)]
    catalog: CatalogArgs,
    #[arg(long, value_delimiter = ',')]
    ruleset: Vec<String>,
}

#[derive(clap::Args)]
struct ShowRulesetArgs {
    id: String,
    #[command(flatten)]
    catalog: CatalogArgs,
}

#[derive(clap::Args)]
struct IngestArgs {
    #[arg(long)]
    workspace: PathBuf,
    #[arg(long)]
    source: String,
    #[arg(long, value_delimiter = ',')]
    accounts: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    regions: Vec<String>,
    #[arg(long, value_parser = parse_date)]
    from: Option<NaiveDate>,
    #[arg(long, value_parser = parse_date)]
    to: Option<NaiveDate>,
    #[arg(long, default_value_t = DEFAULT_INGEST_JOBS)]
    jobs: NonZeroUsize,
}

#[derive(clap::Args)]
struct AnalyzeArgs {
    #[arg(long)]
    workspace: PathBuf,
    #[arg(long, value_parser = parse_time)]
    since: DateTime<Utc>,
    #[arg(long, value_parser = parse_time)]
    until: DateTime<Utc>,
    #[command(flatten)]
    catalog: CatalogArgs,
    #[arg(long, value_delimiter = ',')]
    ruleset: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    rule: Vec<String>,
    #[arg(long)]
    context: Option<PathBuf>,
    #[arg(long)]
    emit_signals: bool,
    #[arg(long, value_delimiter = ',')]
    severity: Vec<Severity>,
    #[arg(long)]
    summary: bool,
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Run(args) => run::run(&args),
        Command::Ingest(args) => ingest(&args),
        Command::Analyze(args) => analyze(&args),
        Command::Rules {
            command: RulesCommand::Validate(args),
        } => validate_rules(&args),
        Command::Rules {
            command: RulesCommand::Test(args),
        } => test_rules(&args),
        Command::Rulesets {
            command: RulesetsCommand::List(args),
        } => list_rulesets(&args),
        Command::Rulesets {
            command: RulesetsCommand::Show(args),
        } => show_ruleset(&args),
    }
}

fn ingest(args: &IngestArgs) -> anyhow::Result<()> {
    let filter = KeyFilter {
        accounts: args.accounts.clone(),
        regions: args.regions.clone(),
        from: args.from,
        to: args.to,
    };
    if let (Some(from), Some(to)) = (filter.from, filter.to)
        && from > to
    {
        bail!("--from must not be after --to, but got {from} and {to}");
    }
    let s3_location = if args.source.starts_with(S3_SCHEME) {
        Some(S3Location::parse(&args.source)?)
    } else if filter.is_unrestricted() {
        None
    } else {
        bail!("--accounts, --regions, --from and --to apply only to S3 sources");
    };
    fs::create_dir_all(&args.workspace)
        .with_context(|| format!("failed to create workspace {}", args.workspace.display()))?;
    let source = match s3_location {
        Some(location) => {
            let evidence = args.workspace.join(EVIDENCE_DIR);
            download_evidence(&location, &filter, &evidence)?;
            evidence
        }
        None => PathBuf::from(&args.source),
    };
    let store = EventStore::open(&args.workspace.join(DATABASE_FILE))?;
    let summary = store.ingest_directory_with_jobs(&source, args.jobs)?;
    eprintln!(
        "ingested {} files ({} skipped as already ingested), {} events",
        summary.files, summary.skipped_files, summary.events
    );
    Ok(())
}

fn download_evidence(
    location: &S3Location,
    filter: &KeyFilter,
    evidence: &Path,
) -> anyhow::Result<()> {
    let summary = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let store = AwsObjectStore::connect(&location.bucket).await?;
            download(&store, location, filter, evidence).await
        })?;
    eprintln!(
        "downloaded {} files ({} skipped as already downloaded)",
        summary.downloaded, summary.skipped
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
    let context = match &args.context {
        Some(path) => load_context(path)?,
        None => ContextFile::default(),
    };
    let window = AnalysisWindow::new(
        args.since,
        args.until,
        context.baseline.lookback.to_time_delta(),
    )?;
    let catalog = load_catalog(&args.catalog)?;
    let rules = context.apply_params(&catalog.rules)?;
    let selection = catalog.rulesets.select(&rules, &args.ruleset, &args.rule)?;
    let backend = DuckDbBackend::open(&database)?;
    let mut analysis = tokio::runtime::Builder::new_current_thread()
        .build()?
        .block_on(ashiato_core::analysis::analyze(
            &backend,
            &selection.rules,
            &window,
            &context,
        ))?;
    analysis.attach_refs(&selection.refs);
    for warning in &analysis.warnings {
        report_warning(warning);
    }
    let scores = score(&analysis.signals, ScoringSettings::from(&context.scoring));
    if !args.severity.is_empty() {
        analysis.retain_severities(&args.severity);
    }

    let run = OutputRecord::Run(RunRecord {
        tool_version: env!("CARGO_PKG_VERSION").to_owned(),
        since: window.since(),
        until: window.until(),
        rule_sources: selection.rules.rule_sources().to_vec(),
        rulesets: selection.rulesets,
        context_digest: context.digest().map(str::to_owned),
        data: analysis.data,
    });
    write_output(&run, analysis, scores, args.emit_signals, args.summary)
}

fn load_context(path: &Path) -> anyhow::Result<ContextFile> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read context file {}", path.display()))?;
    ContextFile::parse(&text).with_context(|| format!("invalid context file {}", path.display()))
}

fn write_output(
    run: &OutputRecord,
    analysis: Analysis,
    scores: Vec<OriginScoreRecord>,
    emit_signals: bool,
    summary: bool,
) -> anyhow::Result<()> {
    let mut sink = JsonLinesSink::new(BufWriter::new(io::stdout().lock()));
    sink.write(run)?;
    for skipped in &analysis.skipped {
        sink.write(&OutputRecord::RuleSkipped(skipped.clone()))?;
    }
    if summary {
        for summary in analysis.summarize() {
            sink.write(&OutputRecord::Summary(summary))?;
        }
    } else {
        if emit_signals {
            for signal in analysis.signals {
                sink.write(&OutputRecord::Signal(signal))?;
            }
        }
        for found in analysis.matches {
            sink.write(&OutputRecord::ScenarioMatch(found))?;
        }
        for found in scores {
            sink.write(&OutputRecord::OriginScore(found))?;
        }
    }
    sink.into_inner().flush()?;
    Ok(())
}

fn validate_rules(args: &RulesArgs) -> anyhow::Result<()> {
    let Catalog { rules, .. } = load_catalog(&args.catalog)?;
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
    let Catalog {
        rules,
        rulesets,
        custom_rule_ids,
    } = load_catalog(&args.catalog)?;
    let mut fixtures: Vec<Fixture> = builtin_fixtures()?
        .into_iter()
        .filter(|fixture| !custom_rule_ids.contains(&fixture.rule_id))
        .collect();
    if let Some(dir) = &args.catalog.rules {
        for (path, content) in read_files(dir, FIXTURE_EXTENSION)? {
            fixtures.push(Fixture::parse(&FixtureFile { path, content })?);
        }
    }
    if !args.ruleset.is_empty() {
        let selection = rulesets.select(&rules, &args.ruleset, &[])?;
        fixtures.retain(|fixture| selection.rules.get(&fixture.rule_id).is_some());
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

fn list_rulesets(args: &CatalogArgs) -> anyhow::Result<()> {
    let Catalog { rulesets, .. } = load_catalog(args)?;
    let mut out = BufWriter::new(io::stdout().lock());
    for set in rulesets.iter() {
        let mut line = serde_json::json!({
            "id": set.id,
            "title": set.title,
            "rules": set.rules.len(),
        });
        if let Some(framework) = &set.framework {
            line["framework"] = serde_json::to_value(framework)?;
        }
        writeln!(out, "{line}")?;
    }
    out.flush()?;
    Ok(())
}

fn show_ruleset(args: &ShowRulesetArgs) -> anyhow::Result<()> {
    let Catalog {
        rules, rulesets, ..
    } = load_catalog(&args.catalog)?;
    let set = rulesets
        .get(&args.id)
        .ok_or_else(|| RulesetError::UnknownRuleset {
            ruleset: args.id.clone(),
        })?;
    let mut out = BufWriter::new(io::stdout().lock());
    for entry in &set.rules {
        let rule = rules
            .get(&entry.id)
            .expect("load_catalog checks that every listed rule exists");
        let (kind, severity, tactics) = match rule {
            Rule::Signal(rule) => ("signal", rule.severity, &rule.tactics),
            Rule::Scenario(rule) => ("scenario", rule.severity, &rule.tactics),
        };
        let line = serde_json::json!({
            "id": entry.id,
            "kind": kind,
            "severity": severity,
            "tactics": tactics,
            "refs": entry.refs,
        });
        writeln!(out, "{line}")?;
    }
    out.flush()?;
    Ok(())
}

struct Catalog {
    rules: Rules,
    rulesets: Rulesets,
    custom_rule_ids: BTreeSet<String>,
}

fn load_catalog(args: &CatalogArgs) -> anyhow::Result<Catalog> {
    load_catalog_from(args.rules.as_slice(), args.rulesets.as_slice())
}

fn load_catalog_from(rule_dirs: &[PathBuf], ruleset_dirs: &[PathBuf]) -> anyhow::Result<Catalog> {
    let sources = rule_sources(rule_dirs)?;
    let custom_rule_ids: BTreeSet<String> = sources[1..]
        .iter()
        .flat_map(RuleSource::rules)
        .map(|rule| rule.id().to_owned())
        .collect();
    let rules = Rules::new(sources)?;
    let mut rulesets = Rulesets::builtin()?;
    for dir in ruleset_dirs {
        rulesets.extend(load_rulesets(dir)?);
    }
    let unlisted: Vec<String> = rulesets
        .unlisted(&rules)
        .into_iter()
        .map(str::to_owned)
        .collect();
    rulesets.add_to_default(unlisted);
    rulesets.check(&rules)?;
    Ok(Catalog {
        rules,
        rulesets,
        custom_rule_ids,
    })
}

fn load_rulesets(dir: &Path) -> anyhow::Result<Rulesets> {
    let files = read_files(dir, RULE_EXTENSION)?
        .into_iter()
        .map(|(path, content)| RuleFile { path, content })
        .collect();
    Ok(Rulesets::parse(files)?)
}

fn rule_sources(dirs: &[PathBuf]) -> anyhow::Result<Vec<RuleSource>> {
    let mut sets = vec![RuleSource::builtin()?];
    for dir in dirs {
        sets.push(load_rule_source(dir)?);
    }
    Ok(sets)
}

fn parse_date(text: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .map_err(|_| format!("expected YYYY-MM-DD, but got {text}"))
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

fn load_rule_source(dir: &Path) -> anyhow::Result<RuleSource> {
    let files = read_files(dir, RULE_EXTENSION)?
        .into_iter()
        .map(|(path, content)| RuleFile { path, content })
        .collect();
    Ok(RuleSource::parse(dir.to_string_lossy(), files)?)
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
