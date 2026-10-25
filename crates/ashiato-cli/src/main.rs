use clap::Parser;

#[derive(Parser)]
#[command(name = "ashiato", version)]
struct Cli {}

fn main() {
    Cli::parse();
}
