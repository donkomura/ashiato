use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::{Context, bail};
use ashiato_ingest::{
    DEFAULT_PREFETCH_FILES, FileSource, S3Location, S3Source, WorkerSettings, resume_assignments,
    run_worker,
};
use ashiato_nats::{NatsControl, NatsWriter};
use ashiato_pipeline::{PartitionWriter, WorkerMetrics, unix_ms};

use crate::metrics::Reporter;

#[derive(clap::Args)]
pub(crate) struct IngestWorkerArgs {
    #[arg(long)]
    nats: String,
    #[arg(long)]
    run: String,
    #[arg(long)]
    worker_id: u32,
}

// The worker takes its partitions and their files from the analyzer and lists nothing itself,
// so that every role reads the manifest saved at the start of the run (5.4 節).
pub(crate) fn ingest_worker(args: &IngestWorkerArgs) -> anyhow::Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(work(args))
}

async fn work(args: &IngestWorkerArgs) -> anyhow::Result<()> {
    let metrics = Arc::new(WorkerMetrics::new(args.worker_id));
    let reporter = Reporter::every_second({
        let metrics = Arc::clone(&metrics);
        move |unix_ms| vec![metrics.line(unix_ms, false)]
    });
    let client = async_nats::connect(&args.nats)
        .await
        .with_context(|| format!("failed to connect to {}", args.nats))?;
    let control = NatsControl::new(client.clone(), &args.run)?;
    let (plan, assignments) = resume_assignments(&control, args.worker_id).await?;
    if plan.run_id != args.run {
        bail!(
            "the analyzer answered for run {} rather than {}",
            plan.run_id,
            args.run
        );
    }
    let location = S3Location::parse(&plan.source.uri)?;
    let source = S3Source::connect(&location.bucket, plan.source.endpoint_url.as_deref()).await;
    let max_message_size = usize::try_from(plan.max_message_size)?;
    let writer = NatsWriter::new(client.clone(), &plan.run_id, max_message_size)?;
    run_worker(
        assignments,
        Arc::new(source) as Arc<dyn FileSource>,
        Arc::new(writer) as Arc<dyn PartitionWriter>,
        WorkerSettings {
            allowed_lateness: plan.allowed_lateness.to_time_delta(),
            max_message_size,
            decode_threads: std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN),
            prefetch_files: DEFAULT_PREFETCH_FILES,
        },
        Arc::clone(&metrics),
    )
    .await?;
    client.flush().await?;
    reporter.stop().await;
    crate::metrics::write([metrics.line(unix_ms(), true)]);
    Ok(())
}
