use clap::{Args, Parser, Subcommand, ValueEnum};

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

pub fn parse_scale(text: &str) -> Result<f64, String> {
    let _ = text;
    todo!()
}

pub fn run(cli: Cli) -> anyhow::Result<()> {
    let _ = cli;
    todo!()
}
