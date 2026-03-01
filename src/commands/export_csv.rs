use std::fs::File;
use std::io::{self, BufReader, BufWriter, ErrorKind, Write};

use clap::Args;

use crate::outputs::csv::{write_csv_header, write_generic_row};
use crate::parsers::generic::iter_table_rows;
use crate::parsers::schema::{ALL_TABLES, WikipediaTable};

#[derive(Debug, Args)]
pub struct ExportCsvArgs {
    #[arg(long)]
    table: String,
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

fn run_export_table(
    table: WikipediaTable,
    input_path: &str,
    limit: Option<usize>,
) -> io::Result<()> {
    let file = File::open(input_path)?;
    let reader = BufReader::new(file);
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    write_csv_header(&mut out, table.column_names())?;
    let row_limit = limit.unwrap_or(usize::MAX);
    for row in iter_table_rows(reader, table).take(row_limit) {
        write_generic_row(&mut out, &row?)?;
    }

    out.flush()
}

pub fn run_export_csv(args: ExportCsvArgs) -> io::Result<()> {
    let table = parse_table(&args.table)?;
    let input_path = args
        .input
        .unwrap_or_else(|| format!("{}.sql", table.table_name()));
    run_export_table(table, &input_path, args.limit)
}
