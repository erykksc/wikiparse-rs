use std::fs::File;
use std::io::{self, BufReader, BufWriter, ErrorKind, Write};

use clap::{Parser, ValueEnum};

use wikiparse_rs::outputs::csv::{write_csv_header, write_generic_row};
use wikiparse_rs::outputs::json::write_json_row_object;
use wikiparse_rs::parsers::generic::iter_table_rows;
use wikiparse_rs::parsers::schema::{ALL_TABLES, WikipediaTable};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    Csv,
    Json,
}

#[derive(Debug, Parser)]
#[command(name = "wikiparse-rs")]
#[command(about = "Parse MediaWiki SQL dumps")]
pub struct Cli {
    #[arg(long)]
    table: String,
    #[arg(long)]
    format: OutputFormat,
    #[arg(long)]
    input: Option<String>,
    #[arg(long)]
    limit: Option<usize>,
}

fn parse_table(table_name: &str) -> io::Result<WikipediaTable> {
    if let Some(table) = WikipediaTable::from_table_name(table_name) {
        return Ok(table);
    }

    let supported_tables = ALL_TABLES
        .iter()
        .map(|table| table.table_name())
        .collect::<Vec<_>>()
        .join(", ");
    Err(io::Error::new(
        ErrorKind::InvalidInput,
        format!(
            "unsupported table '{}'; supported tables: {}",
            table_name, supported_tables
        ),
    ))
}

fn run_export_csv<R: io::BufRead>(
    out: &mut impl Write,
    reader: R,
    table: WikipediaTable,
    limit: usize,
) -> io::Result<()> {
    write_csv_header(out, table.column_names())?;
    for row in iter_table_rows(reader, table).take(limit) {
        write_generic_row(out, &row?)?;
    }
    Ok(())
}

fn run_export_json<R: io::BufRead>(
    out: &mut impl Write,
    reader: R,
    table: WikipediaTable,
    limit: usize,
) -> io::Result<()> {
    out.write_all(b"[\n")?;

    let mut first_row = true;
    for row in iter_table_rows(reader, table).take(limit) {
        if !first_row {
            out.write_all(b",\n")?;
        }
        write_json_row_object(out, table.column_names(), &row?)?;
        first_row = false;
    }

    if first_row {
        out.write_all(b"]\n")
    } else {
        out.write_all(b"\n]\n")
    }
}

fn run(args: Cli) -> io::Result<()> {
    let table = parse_table(&args.table)?;
    let input_path = args
        .input
        .unwrap_or_else(|| format!("{}.sql", table.table_name()));
    let file = File::open(input_path)?;
    let reader = BufReader::new(file);
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    let limit = args.limit.unwrap_or(usize::MAX);

    match args.format {
        OutputFormat::Csv => run_export_csv(&mut out, reader, table, limit)?,
        OutputFormat::Json => run_export_json(&mut out, reader, table, limit)?,
    }

    out.flush()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    run(cli)?;
    Ok(())
}
