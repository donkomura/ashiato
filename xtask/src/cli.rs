use std::path::PathBuf;

use anyhow::ensure;
use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::correctness::{Options, REPORT_FILE, dataset_spec, integration_dir};
use crate::dist;
use crate::speed;
use crate::topology::MAX_WORKERS;

#[derive(Debug, PartialEq, Parser)]
#[command(name = "xtask", about = "Tasks of the ashiato repository")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, PartialEq, Subcommand)]
pub enum Command {
    #[command(subcommand)]
    Integration(Integration),
    Dist(DistArgs),
}

#[derive(Debug, PartialEq, Args)]
pub struct DistArgs {
    #[arg(long, value_enum)]
    pub target: DistTarget,
    #[arg(long, default_value = "target/dist")]
    pub output: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum DistTarget {
    #[value(name = "x86_64-unknown-linux-gnu")]
    X86_64,
    #[value(name = "aarch64-unknown-linux-gnu")]
    Aarch64,
}

impl DistTarget {
    pub fn triple(self) -> &'static str {
        match self {
            Self::X86_64 => "x86_64-unknown-linux-gnu",
            Self::Aarch64 => "aarch64-unknown-linux-gnu",
        }
    }
}

#[derive(Debug, PartialEq, Subcommand)]
pub enum Integration {
    Correctness(CorrectnessArgs),
    Scaling(ScalingArgs),
}

#[derive(Debug, PartialEq, Args)]
pub struct CorrectnessArgs {
    #[arg(long, value_enum)]
    pub dataset: CorrectnessDataset,
    #[arg(long, default_value = "1.0", value_parser = parse_scale)]
    pub scale: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CorrectnessDataset {
    A,
    ANoDataEvents,
}

#[derive(Debug, PartialEq, Args)]
pub struct ScalingArgs {
    #[arg(long, value_enum)]
    pub dataset: ScalingDataset,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "1,2,3,4",
        value_parser = clap::value_parser!(u32).range(1..=i64::from(MAX_WORKERS))
    )]
    pub workers: Vec<u32>,
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(1..))]
    pub repeat: u32,
    #[arg(long, default_value = "1.0", value_parser = parse_scale)]
    pub scale: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ScalingDataset {
    B,
}

// The scale shrinks the datasets of 14.1.2 節 for a quick run; 1.0 is the size in the design.
pub fn parse_scale(text: &str) -> Result<f64, String> {
    match text.parse::<f64>() {
        Ok(scale) if scale.is_finite() && scale > 0.0 => Ok(scale),
        _ => Err(format!("expected a positive number, got {text:?}")),
    }
}

pub fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Command::Integration(Integration::Correctness(args)) => correctness(&args),
        Command::Integration(Integration::Scaling(args)) => scaling(&args),
        Command::Dist(args) => {
            let archive = dist::dist(&args)?;
            eprintln!("wrote {}", archive.display());
            Ok(())
        }
    }
}

fn correctness(args: &CorrectnessArgs) -> anyhow::Result<()> {
    let options = Options {
        dataset: dataset_spec(args.dataset, args.scale),
        integration_dir: integration_dir(),
    };
    let report = crate::correctness::correctness(&options)?;
    let path = options.integration_dir.join(REPORT_FILE);
    ensure!(
        report.passed(),
        "integration correctness failed; see {}",
        path.display()
    );
    eprintln!("integration correctness passed; see {}", path.display());
    Ok(())
}

fn scaling(args: &ScalingArgs) -> anyhow::Result<()> {
    let options = speed::Options {
        dataset: speed::dataset_spec(args.dataset, args.scale),
        integration_dir: integration_dir(),
        workers: args.workers.clone(),
        repeat: args.repeat,
    };
    let report = speed::scaling(&options)?;
    let path = options.integration_dir.join(speed::REPORT_FILE);
    ensure!(
        report.passed(),
        "integration scaling failed; see {}",
        path.display()
    );
    eprintln!("integration scaling passed; see {}", path.display());
    Ok(())
}
