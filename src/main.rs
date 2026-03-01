use clap::Parser;

use wikiparse_rs::commands::Cli;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    cli.run()?;
    Ok(())
}
