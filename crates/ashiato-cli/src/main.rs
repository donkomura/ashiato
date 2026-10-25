use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use ashiato_duckdb::EventStore;
use clap::{Parser, Subcommand};

const DATABASE_FILE: &str = "ashiato.duckdb";

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
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Ingest { workspace, source } => ingest(&workspace, &source),
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
