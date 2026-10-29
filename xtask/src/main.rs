use clap::Parser;
use xtask::cli::{Cli, run};

fn main() -> anyhow::Result<()> {
    run(Cli::parse())
}
