use std::fs::File;
use std::io::{self, BufReader, BufWriter, ErrorKind, Write};

use clap::Args;

use crate::outputs::json::write_json_row_object;
use crate::parsers::generic::iter_table_rows;
use crate::parsers::schema::{ALL_TABLES, WikipediaTable};

#[derive(Debug, Args)]
pub struct ExportJsonArgs {
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

    out.write_all(b"[\n")?;
    let row_limit = limit.unwrap_or(usize::MAX);
    let mut first_row = true;
    for row in iter_table_rows(reader, table).take(row_limit) {
        if !first_row {
            out.write_all(b",\n")?;
        }
        write_json_row_object(&mut out, table.column_names(), &row?)?;
        first_row = false;
    }
    out.write_all(b"\n]\n")?;

    out.flush()
}

pub fn run_export_json(args: ExportJsonArgs) -> io::Result<()> {
    let table = parse_table(&args.table)?;
    let input_path = args
        .input
        .unwrap_or_else(|| format!("{}.sql", table.table_name()));
    run_export_table(table, &input_path, args.limit)
}
