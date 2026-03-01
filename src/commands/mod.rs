use std::io;

use clap::{Parser, Subcommand};

pub mod export_csv;
pub mod export_json;

#[derive(Debug, Parser)]
#[command(name = "wikiparse-rs")]
#[command(about = "Parse MediaWiki SQL dumps")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    ExportCsv(export_csv::ExportCsvArgs),
    ExportJson(export_json::ExportJsonArgs),
}

impl Cli {
    pub fn run(self) -> io::Result<()> {
        self.command.run()
    }
}

impl Command {
    pub fn run(self) -> io::Result<()> {
        match self {
            Self::ExportCsv(args) => export_csv::run_export_csv(args),
            Self::ExportJson(args) => export_json::run_export_json(args),
        }
    }
}
