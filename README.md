# wikiparse-rs

`wikiparse-rs` is a blazingly fast CLI and library written in Rust for streaming parsed uncompressed MediaWiki/Wikipedia SQL dumps.
It reads `INSERT` rows from supported Wikipedia tables and exports them as CSV or JSON.

## Install

From this repository root:

```bash
cargo install --path .
```

## Quick usage

Export a table dump to CSV:

```bash
wikiparse-rs --table page --format csv --input /path/to/page.sql > page.csv
```

Export from stdin (default when `--input` is omitted):

```bash
cat /path/to/page.sql | wikiparse-rs --table page --format csv > page.csv
```

Export a table dump to JSON:

```bash
wikiparse-rs --table linktarget --format json --input /path/to/linktarget.sql > linktarget.json
```

## CLI command

The `wikiparse-rs` binary is designed for scriptable dump export.

- `--table`: which supported MediaWiki table to parse (for example `page`, `pagelinks`, `linktarget`)
- `--format`: output format, `csv` or `json`
- `--input`: path to the SQL dump file, or `-` for stdin (defaults to stdin when omitted)
- `--limit`: optional row limit for quick sampling

This makes the command useful as a standalone binary to transform large SQL dumps into CSV/JSON for downstream tools.

Compressed dumps can be streamed without extracting first:

```bash
zcat /path/to/page.sql.gz | wikiparse-rs --table page --format csv > page.csv
```

Show progress while streaming a compressed dump with [pv](https://codeberg.org/ivarch/pv):

```bash
pv /path/to/page.sql.gz | zcat | cargo run -- --table page --format csv > page.csv
```

## Column selection with xsv

After exporting CSV, you can select only the columns you need with [xsv](https://github.com/BurntSushi/xsv):

```bash
wikiparse-rs --table page --format csv --input /path/to/page.sql \
  | xsv select page_id,page_title,page_namespace
```
