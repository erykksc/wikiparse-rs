use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, ErrorKind, Write};

use clap::{ArgGroup, Parser, ValueEnum};

use wikiparse_rs::outputs::csv::{write_csv_header, write_generic_row};
use wikiparse_rs::outputs::json::write_json_row_object;
use wikiparse_rs::parsers::generic::iter_table_rows;
use wikiparse_rs::parsers::schema::{ALL_TABLES, WikipediaTable};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    Csv,
    Json,
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OutputFormat::Csv => write!(f, "csv"),
            OutputFormat::Json => write!(f, "json"),
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "wikiparse-rs")]
#[command(about = "Parse MediaWiki SQL dumps")]
#[command(group(
    ArgGroup::new("input_group")
        .required(true)
        .args(["input_pos", "input_flag"]),
))]
pub struct Cli {
    #[arg(short, long)]
    table: String,
    #[arg(short, long, default_value_t = OutputFormat::Csv)]
    format: OutputFormat,
    #[arg(short, long)]
    limit: Option<usize>,

    /// The input SQL dump file provided as a positional argument
    #[arg(value_name = "INPUT")]
    input_pos: Option<String>,

    /// The input SQL dump file provided via a flag
    #[arg(short, long = "input", value_name = "INPUT_FLAG")]
    input_flag: Option<String>,
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

fn run_export<R: io::BufRead>(
    out: &mut impl Write,
    reader: R,
    table: WikipediaTable,
    limit: usize,
    format: OutputFormat,
) -> io::Result<()> {
    match format {
        OutputFormat::Csv => run_export_csv(out, reader, table, limit),
        OutputFormat::Json => run_export_json(out, reader, table, limit),
    }
}

fn run(args: Cli) -> io::Result<()> {
    let table = parse_table(&args.table)?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    let limit = args.limit.unwrap_or(usize::MAX);

    if let Some(input_path) = args.input_pos.or(args.input_flag).as_deref()
        && input_path != "-"
    {
        let file = File::open(input_path)?;
        let reader = BufReader::new(file);
        run_export(&mut out, reader, table, limit, args.format)?;
        return out.flush();
    }

    let stdin = io::stdin();
    let reader = stdin.lock();
    run_export(&mut out, reader, table, limit, args.format)?;

    out.flush()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    run(cli)?;
    Ok(())
}
