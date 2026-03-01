# AGENTS.md - wikiparse-rs

Guide for agentic coding tools operating in this repository.

## 1. Project Snapshot

- Language: Rust
- Edition: 2024
- Crate: `wikiparse-rs`
- Type: single Cargo package with one CLI binary (`src/main.rs`) and library modules (`src/lib.rs`)
- Purpose: parse Wikipedia SQL dumps as streaming iterators and export rows to CSV/JSON

Current CLI command:

- direct flags on the binary (`--table`, `--format`, `--input`, `--limit`)

Key files:

- `Cargo.toml` - package metadata/dependencies
- `src/main.rs` - CLI entrypoint, argument wiring, and export execution
- `src/lib.rs` - module exports (`outputs`, `parsers`, `sql_parsing`)
- `src/outputs/csv.rs` - generic CSV formatting/writers
- `src/outputs/json.rs` - generic JSON formatting/writers
- `src/parsers/generic.rs` - shared streaming SQL `INSERT` parser and generic row/value types
- `src/parsers/schema.rs` - supported table registry, names, and ordered column metadata
- `src/parsers/page.rs` - typed parser wrapper for `page`
- `src/parsers/pagelinks.rs` - typed parser wrapper for `pagelinks`
- `src/parsers/linktarget.rs` - typed parser wrapper for `linktarget`
- `src/parsers/mod.rs` - parser module exports and generic per-table iterator wrappers
- `src/sql_parsing.rs` - shared byte-level parsing helpers

## 2. Build/Lint/Test/Run Commands

Run from repository root.

Build:

```bash
cargo build
cargo build --release
cargo build --bin wikiparse-rs
```

Run:

```bash
# CLI entrypoint
cargo run -- --table page --format csv --input /path/to/page.sql
cargo run -- --table revision --format csv --input /path/to/revision.sql --limit 1000
cargo run -- --table linktarget --format json --input /path/to/linktarget.sql

# Release build run
cargo run --release -- --table pagelinks --format csv --input ~/wikipedia/pagelinks.sql --limit 500000
```

Library usage example:

```rust
use std::fs::File;
use std::io::{self, BufReader};

use wikiparse_rs::{iter_table_rows, WikipediaTable};

fn main() -> io::Result<()> {
    let file = File::open("revision.sql")?;
    let reader = BufReader::new(file);

    for row in iter_table_rows(reader, WikipediaTable::Revision).take(10) {
        let row = row?;
        println!("{} -> {} fields", row.table.table_name(), row.values.len());
    }

    Ok(())
}
```

Format and lint:

```bash
cargo fmt --all
cargo fmt --all -- --check
cargo clippy --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

Tests (full and scoped):

```bash
# all tests
cargo test

# library tests only
cargo test --lib

# binary entrypoint tests (if present)
cargo test --bin wikiparse-rs

# parser-focused module tests
cargo test pagelinks::tests
cargo test linktarget::tests
cargo test generic::tests
cargo test schema::tests
cargo test sql_parsing::tests
```

Single-test workflows (preferred inner loop):

```bash
# name filter across all targets
cargo test iter_table_rows

# name filter inside parser modules
cargo test parsers::pagelinks::tests
cargo test parsers::linktarget::tests

# exact single test
cargo test --lib parsers::schema::tests::roundtrip_table_name_for_all_tables -- --exact
cargo test --lib parse_sql_quoted_bytes_handles_escapes -- --exact

# show test output
cargo test -- --nocapture
```

## 3. Code Style Guidelines

Imports:

- Order imports: `std`, third-party crates, local crate modules.
- Keep imports explicit and minimal.
- Remove unused imports; do not silence warnings.

Formatting:

- Rustfmt is required; do not hand-format around it.
- Keep files ASCII unless a file already requires Unicode.
- Add comments only for non-obvious logic or invariants.

Types and parsing:

- Use schema-aligned integer widths (`u32`, `u64`, `i32`).
- Prefer checked arithmetic for untrusted numeric parsing.
- Keep low-level parsing byte-oriented (`&[u8]`) in hot paths.
- Use `Option` for primitive parse helpers where failure is expected.
- Convert to `io::Result` at row/iterator boundaries with clear errors.

Naming conventions:

- Files/modules/functions/locals: `snake_case`
- Types/enums/traits: `PascalCase`
- Constants: `UPPER_SNAKE_CASE`
- Parser helper names should be specific and verb-based.

Error handling:

- Treat dump input as untrusted/malformed.
- Avoid panics in parser paths.
- Use `io::ErrorKind::InvalidData` for format/validation failures.
- Include field/token context in error messages.

I/O and performance:

- Use streaming reads (`BufRead`, `read_until`) for large dumps.
- Use `BufWriter` for output.
- Minimize allocations and UTF-8 conversions in tight loops.
- Keep iterator output deterministic and stable.

CLI/output compatibility:

- Preserve defaults unless explicitly requested to change.
- Keep CSV headers and column order stable for each table's schema order.
- Keep JSON output as a valid top-level array (`[` first line, `]` last line) of per-row objects.
- Keep output script-friendly and deterministic.

## 4. Testing Guidance for Parser Changes

Prefer focused unit tests for:

- missing separators/parentheses and tuple arity mismatches
- signed/unsigned range boundaries for typed parser wrappers
- SQL quoted string escapes (`\\`, `\'`, doubled `'`)
- semicolon/end-of-line tuple termination
- iterator behavior across multiple `INSERT` lines
- per-table column metadata consistency (`column_names().len() == expected_columns()`)

Recommended validation sequence before handoff:

1. `cargo fmt --all -- --check`
2. `cargo test` (or clearly state scoped tests run)
3. `cargo clippy --all-targets --all-features -- -D warnings`

## 5. Cursor and Copilot Rules

Checked locations:

- `.cursor/rules/`
- `.cursorrules`
- `.github/copilot-instructions.md`

Status for this repository at generation time:

- No Cursor rule files found.
- No Copilot instruction file found.

If these files are added later, treat them as higher-priority local instructions.

## 6. Git and Workspace Hygiene

- Never edit generated files in `target/`.
- Do not commit large generated dump outputs unless requested.
- Keep changes tightly scoped to requested behavior.
- Avoid unrelated refactors in parser-critical files.
- In dirty worktrees, do not revert unrelated user changes.

## 7. Library-First Parsing API

Core exports from `src/lib.rs`:

- `WikipediaTable` - enum of all supported MediaWiki tables
- `ALL_TABLES` - list of all supported table variants
- `SqlValue` - generic SQL value representation (`Null`, `I64`, `U64`, `F64`, `Bytes`)
- `GenericRow` - parsed row container with table id and ordered values
- `iter_table_rows(reader, table)` - streaming iterator over parsed rows for a table
- `iter_table_rows_by_name(reader, "table_name")` - same, table resolved from name

Output format expectations:

- CSV export prints table schema column names from `WikipediaTable::column_names()`.
- `SqlValue::Bytes` exports as UTF-8 when valid, otherwise as `0x...` lowercase hex.
- `SqlValue::Null` exports as an empty CSV field.
- JSON export prints each row as an object keyed by `WikipediaTable::column_names()`.
- JSON export renders `SqlValue::Null` as `null` and numeric values as JSON numbers.

Parser module structure:

- Typed wrappers remain for `page`, `pagelinks`, and `linktarget` in `src/parsers/`.
- Other tables are available through generic modules exposed by `src/parsers/mod.rs`.
- Prefer generic iterator paths for new table integrations unless a typed wrapper is required.
