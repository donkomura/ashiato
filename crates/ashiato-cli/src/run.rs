use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, bail};
use ashiato_core::Severity;
use ashiato_core::analysis::{correlate_streamed, not_streamable_rules};
use ashiato_core::output::{OutputRecord, PipelineRunRecord, RunRecord};
use ashiato_core::rule::{Rule, SignalRule};
use ashiato_core::scoring::{ScoringSettings, score};
use ashiato_core::validate::validate_streaming;
use ashiato_duckdb::{Analyzer, AnalyzerError, AnalyzerSettings, PipelineStore};
use ashiato_ingest::{
    DEFAULT_PREFETCH_FILES, FileSource, ListScope, LocalSource, PartitionAssignment, S3Location,
    S3Source, WorkerSettings, list_local, run_workers, trail_root,
};
use ashiato_nats::{ControlService, EventStream, max_ack_pending, serve_control, validate_run_id};
use ashiato_pipeline::{
    AnalyzerMetrics, CommittedPositions, Manifest, PartitionId, PlacementMode, RuleOrigin, RunPlan,
    SourceLocation, WorkerMetrics, assign_partitions, partition_channels, unix_ms,
    validate_distributed,
};
use serde_json::Value;

use crate::metrics::Reporter;
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
    validate_placement(&plan)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let metrics = Arc::new(AnalyzerMetrics::default());
    let reporter = {
        let _runtime = runtime.enter();
        let metrics = Arc::clone(&metrics);
        Reporter::every_second(move |unix_ms| vec![metrics.line(unix_ms, false)])
    };
    let source = runtime.block_on(PlanSource::open(&plan))?;
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
    let manifest = store.start_run(plan.digest(), || runtime.block_on(source.list(&plan)))?;
    for violation in validate_streaming(&selection.rules, plan.pipeline.batch) {
        eprintln!("warning: {violation}; the rule is skipped");
    }
    let not_streamable = not_streamable_rules(&selection.rules, plan.pipeline.batch);
    let signal_rules: Vec<SignalRule> = selection
        .rules
        .iter()
        .filter_map(|rule| match rule {
            Rule::Signal(rule) => Some(rule.clone()),
            Rule::Scenario(_) => None,
        })
        .filter(|rule| {
            not_streamable
                .iter()
                .all(|skipped| skipped.rule_id != rule.id)
        })
        .collect();
    let analyzer = open_analyzer(&plan, store, manifest.clone(), signal_rules)?
        .with_metrics(Arc::clone(&metrics));
    let store = match plan.placement.mode {
        PlacementMode::SingleProcess => {
            runtime.block_on(stream(&plan, analyzer, manifest, source.file_source()))?
        }
        PlacementMode::Distributed => {
            runtime.block_on(analyze_distributed(&plan, analyzer, manifest, &metrics))?
        }
    };

    let counts = store.ingest_counts()?;
    let mut analysis = correlate_streamed(
        &selection.rules,
        store.observed_signals()?,
        store.lineage_edges()?,
        store.data_availability()?,
        not_streamable,
        &plan.context,
    );
    metrics.correlated.mark();
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
    write_output(&run, analysis, scores, args.emit_signals, args.summary)?;
    runtime.block_on(reporter.stop());
    crate::metrics::write([metrics.line(unix_ms(), true)]);
    Ok(())
}

// The run id names the subjects and the stream of NATS, so it is checked before the workspace
// is made.
fn validate_placement(plan: &RunPlan) -> anyhow::Result<()> {
    validate_distributed(plan)?;
    if plan.placement.mode == PlacementMode::Distributed {
        validate_run_id(&plan.run_id)?;
    }
    Ok(())
}

enum PlanSource {
    Local(PathBuf),
    S3(S3Location, Arc<S3Source>),
}

impl PlanSource {
    // The credentials of S3 are read by the default chain of the AWS SDK.
    async fn open(plan: &RunPlan) -> anyhow::Result<Self> {
        match &plan.source.location {
            SourceLocation::Local { root } => Ok(Self::Local(root.clone())),
            SourceLocation::S3 { uri, endpoint_url } => {
                let location = S3Location::parse(uri)?;
                trail_root(&location.prefix)?;
                let source = S3Source::connect(&location.bucket, endpoint_url.as_deref()).await;
                Ok(Self::S3(location, Arc::new(source)))
            }
        }
    }

    async fn list(&self, plan: &RunPlan) -> anyhow::Result<Manifest> {
        let scope = ListScope::from_plan(plan);
        let (manifest, unrecognized) = match self {
            Self::Local(root) => {
                let listing = list_local(root, &scope)?;
                let paths = listing.unrecognized.iter();
                let unrecognized = paths.map(|path| path.display().to_string()).collect();
                (listing.manifest, unrecognized)
            }
            Self::S3(location, source) => {
                let listing = source.list(location, &scope).await?;
                (listing.manifest, listing.unrecognized)
            }
        };
        for file in unrecognized {
            eprintln!(
                "warning: skipped {file}, whose name does not follow the CloudTrail log file layout"
            );
        }
        Ok(manifest)
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        match self {
            Self::Local(_) => Arc::new(LocalSource),
            Self::S3(_, source) => Arc::clone(source) as Arc<dyn FileSource>,
        }
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

async fn stream(
    plan: &RunPlan,
    mut analyzer: Analyzer,
    manifest: Manifest,
    source: Arc<dyn FileSource>,
) -> anyhow::Result<PipelineStore> {
    let allowed_lateness = plan.pipeline.allowed_lateness.to_time_delta();
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
    let partitions: Vec<PartitionId> = assignments
        .iter()
        .map(|assignment| assignment.manifest.partition.clone())
        .collect();
    let metrics: Vec<Arc<WorkerMetrics>> = (0..plan.placement.ingest_workers.get())
        .map(|worker_id| Arc::new(WorkerMetrics::new(worker_id)))
        .collect();
    let workers = assign_partitions(&partitions, plan.placement.ingest_workers)
        .into_iter()
        .map(|indexes| {
            indexes
                .into_iter()
                .map(|index| assignments[index].clone())
                .collect()
        })
        .zip(metrics.iter().cloned())
        .collect();
    let reporter = Reporter::every_second({
        let metrics = metrics.clone();
        move |unix_ms| worker_lines(&metrics, unix_ms, false)
    });
    let worker = tokio::spawn(run_workers(
        workers,
        source,
        Arc::new(writer),
        WorkerSettings {
            allowed_lateness,
            max_message_size: usize::try_from(plan.broker.max_message_size)?,
            decode_threads: decode_threads(plan.placement.ingest_workers),
            prefetch_files: DEFAULT_PREFETCH_FILES,
        },
    ));
    let analyzed = analyzer.run(&mut reader).await;
    // On a resumed run the worker sends the end of the partitions that had already ended again
    // (6.4 節), and the analyzer can finish before reading it. Closing the reader only after the
    // worker keeps that send from failing; a failed analyzer closes it first to stop the worker.
    let worked = if analyzed.is_ok() {
        let worked = worker.await?;
        drop(reader);
        worked
    } else {
        drop(reader);
        worker.await?
    };
    // A worker that stops makes its partitions end early, so its error explains the failure
    // better than the analyzer's.
    match (analyzed, worked) {
        (Err(AnalyzerError::Closed(_)), Err(error)) | (Ok(()), Err(error)) => Err(error.into()),
        (Err(error), _) => Err(error.into()),
        (Ok(()), Ok(())) => {
            reporter.stop().await;
            crate::metrics::write(worker_lines(&metrics, unix_ms(), true));
            Ok(analyzer.into_store())
        }
    }
}

fn worker_lines(metrics: &[Arc<WorkerMetrics>], unix_ms: u64, is_final: bool) -> Vec<Value> {
    metrics
        .iter()
        .map(|worker| worker.line(unix_ms, is_final))
        .collect()
}

fn open_analyzer(
    plan: &RunPlan,
    store: PipelineStore,
    manifest: Manifest,
    signal_rules: Vec<SignalRule>,
) -> Result<Analyzer, AnalyzerError> {
    let window = plan.analysis_window();
    let settings = AnalyzerSettings {
        batch: plan.pipeline.batch.to_time_delta(),
        allowed_lateness: plan.pipeline.allowed_lateness.to_time_delta(),
        read_since: window.baseline_since(),
        fetch_max: FETCH_MAX,
        commit_every: COMMIT_EVERY,
    };
    Ok(Analyzer::open(store, manifest, settings)?
        .with_queries(signal_rules, window)
        .with_max_skew(
            plan.pipeline
                .max_skew
                .map(|max_skew| max_skew.to_time_delta()),
        ))
}

// The ingest workers run as `ashiato ingest-worker` in other processes and take their part of
// the run from the control requests that this process answers (7.4 節).
async fn analyze_distributed(
    plan: &RunPlan,
    analyzer: Analyzer,
    manifest: Manifest,
    metrics: &Arc<AnalyzerMetrics>,
) -> anyhow::Result<PipelineStore> {
    let url = plan
        .placement
        .nats_url
        .as_deref()
        .context("placement.mode: distributed needs placement.nats_url")?;
    let client = async_nats::connect(url)
        .await
        .with_context(|| format!("failed to connect to {url}"))?;
    let partitions: Vec<PartitionId> = manifest
        .partitions
        .iter()
        .map(|partition| partition.partition.clone())
        .collect();
    let events =
        EventStream::create(client.clone(), &plan.run_id, partitions.len(), &plan.broker).await?;
    let committed = CommittedPositions::default();
    let mut analyzer = analyzer.with_committed_positions(committed.clone());
    let service = ControlService::new(plan, manifest, committed)?.with_metrics(Arc::clone(metrics));
    let server = serve_control(client.clone(), &plan.run_id, service).await?;
    let mut reader = events
        .reader(
            partitions,
            &plan.broker,
            max_ack_pending(COMMIT_EVERY, FETCH_MAX),
        )
        .await?;
    // A failed run keeps the stream, so that the analyzer restarted with the same plan receives
    // the messages that it has not committed (10 節).
    analyzer.run(&mut reader).await?;
    drop(server);
    events.delete().await?;
    Ok(analyzer.into_store())
}

// Each worker has its own decode threads, so the CPUs are divided among the workers.
fn decode_threads(workers: NonZeroU32) -> NonZeroUsize {
    let cpus = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
    let workers = usize::try_from(workers.get()).unwrap_or(usize::MAX);
    NonZeroUsize::new(cpus / workers).unwrap_or(NonZeroUsize::MIN)
}
