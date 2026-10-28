use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, bail};
use ashiato_core::Severity;
use ashiato_core::analysis::correlate_available;
use ashiato_core::output::{OutputRecord, PipelineRunRecord, RunRecord};
use ashiato_core::rule::{Rule, SignalRule};
use ashiato_core::scoring::{ScoringSettings, score};
use ashiato_duckdb::{Analyzer, AnalyzerError, AnalyzerSettings, PipelineStore};
use ashiato_ingest::{
    FileSource, ListScope, LocalSource, PartitionAssignment, WorkerSettings, list_local, run_worker,
};
use ashiato_pipeline::{
    Manifest, PlacementMode, RuleOrigin, RunPlan, SourceLocation, partition_channels,
};

use crate::{DATABASE_FILE, load_catalog_from, report_warning, write_output};

const FETCH_MAX: usize = 16;
const COMMIT_EVERY: usize = 64;

#[derive(clap::Args)]
pub(crate) struct RunArgs {
    #[arg(long)]
    plan: PathBuf,
    #[arg(long)]
    emit_signals: bool,
    #[arg(long, value_delimiter = ',')]
    severity: Vec<Severity>,
    #[arg(long)]
    summary: bool,
}

pub(crate) fn run(args: &RunArgs) -> anyhow::Result<()> {
    let plan = RunPlan::load(&args.plan)?;
    let root = local_root(&plan)?;
    let rule_dirs = rule_dirs(&plan)?;
    let catalog = load_catalog_from(&rule_dirs, &plan.ruleset_dirs)?;
    let rules = plan.context.apply_params(&catalog.rules)?;
    let selection = catalog
        .rulesets
        .select(&rules, &plan.rulesets, &plan.rule)?;
    let workspace = args
        .plan
        .parent()
        .context("the run plan must be a file in the workspace directory")?;

    let mut store = PipelineStore::open(&workspace.join(DATABASE_FILE))?;
    let manifest = store.start_run(plan.digest(), || list(&root, &plan))?;
    let signal_rules: Vec<SignalRule> = selection
        .rules
        .iter()
        .filter_map(|rule| match rule {
            Rule::Signal(rule) => Some(rule.clone()),
            Rule::Scenario(_) => None,
        })
        .collect();
    let store = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(stream(&plan, store, manifest, signal_rules))?;

    let counts = store.ingest_counts()?;
    let mut analysis = correlate_available(
        &selection.rules,
        store.observed_signals()?,
        store.lineage_edges()?,
        store.data_availability()?,
        &plan.context,
    );
    analysis.attach_refs(&selection.refs);
    for warning in &analysis.warnings {
        report_warning(warning);
    }
    let scores = score(
        &analysis.signals,
        ScoringSettings::from(&plan.context.scoring),
    );
    if !args.severity.is_empty() {
        analysis.retain_severities(&args.severity);
    }
    let window = plan.analysis_window();
    let run = OutputRecord::PipelineRun(PipelineRunRecord {
        run: RunRecord {
            tool_version: env!("CARGO_PKG_VERSION").to_owned(),
            since: window.since(),
            until: window.until(),
            rule_sources: selection.rules.rule_sources().to_vec(),
            rulesets: selection.rulesets,
            context_digest: plan.context.digest().map(str::to_owned),
            data: analysis.data,
        },
        plan_digest: plan.digest().to_owned(),
        ingested_files: counts.ingested_files,
        file_errors: counts.file_errors,
        late_events: counts.late_events,
    });
    write_output(&run, analysis, scores, args.emit_signals, args.summary)
}

// Settings for later stages of the implementation are rejected rather than ignored, so that a
// run never silently differs from its plan.
fn local_root(plan: &RunPlan) -> anyhow::Result<PathBuf> {
    if plan.placement.mode == PlacementMode::Distributed {
        bail!("placement.mode: distributed is not supported yet; use single_process");
    }
    if plan.placement.ingest_workers.get() != 1 {
        bail!("placement.ingest_workers other than 1 is not supported yet");
    }
    match &plan.source.location {
        SourceLocation::Local { root } => Ok(root.clone()),
        SourceLocation::S3 { .. } => bail!("source.kind: s3 is not supported yet; use local"),
    }
}

fn rule_dirs(plan: &RunPlan) -> anyhow::Result<Vec<PathBuf>> {
    if !plan.rules.contains(&RuleOrigin::Builtin) {
        bail!("rules must include builtin, which the builtin rulesets refer to");
    }
    Ok(plan
        .rules
        .iter()
        .filter_map(|origin| match origin {
            RuleOrigin::Builtin => None,
            RuleOrigin::Directory(dir) => Some(dir.clone()),
        })
        .collect())
}

fn list(root: &Path, plan: &RunPlan) -> anyhow::Result<Manifest> {
    let listing = list_local(root, &ListScope::from_plan(plan))?;
    for path in &listing.unrecognized {
        eprintln!(
            "warning: skipped {}, whose name does not follow the CloudTrail log file layout",
            path.display()
        );
    }
    Ok(listing.manifest)
}

async fn stream(
    plan: &RunPlan,
    store: PipelineStore,
    manifest: Manifest,
    signal_rules: Vec<SignalRule>,
) -> anyhow::Result<PipelineStore> {
    let window = plan.analysis_window();
    let allowed_lateness = plan.pipeline.allowed_lateness.to_time_delta();
    let settings = AnalyzerSettings {
        batch: plan.pipeline.batch.to_time_delta(),
        allowed_lateness,
        read_since: window.baseline_since(),
        fetch_max: FETCH_MAX,
        commit_every: COMMIT_EVERY,
    };
    let mut analyzer =
        Analyzer::open(store, manifest.clone(), settings)?.with_queries(signal_rules, window);
    let assignments: Vec<PartitionAssignment> = manifest
        .partitions
        .into_iter()
        .zip(analyzer.resume_positions())
        .enumerate()
        .map(|(index, (partition, start))| PartitionAssignment {
            index: u32::try_from(index).expect("the manifest holds fewer than 2^32 partitions"),
            manifest: partition,
            start,
        })
        .collect();
    let (writer, mut reader) = partition_channels(
        assignments
            .iter()
            .map(|assignment| assignment.manifest.partition.clone()),
        usize::try_from(plan.broker.max_messages_per_partition)?,
    );
    let worker = tokio::spawn(run_worker(
        assignments,
        Arc::new(LocalSource) as Arc<dyn FileSource>,
        Arc::new(writer),
        WorkerSettings {
            allowed_lateness,
            max_message_size: usize::try_from(plan.broker.max_message_size)?,
        },
    ));
    let analyzed = analyzer.run(&mut reader).await;
    drop(reader);
    let worked = worker.await?;
    // A worker that stops makes its partitions end early, so its error explains the failure
    // better than the analyzer's.
    match (analyzed, worked) {
        (Err(AnalyzerError::Closed(_)), Err(error)) | (Ok(()), Err(error)) => Err(error.into()),
        (Err(error), _) => Err(error.into()),
        (Ok(()), Ok(())) => Ok(analyzer.into_store()),
    }
}
