use std::io::{self, Write};

use crate::parsers::generic::{GenericRow, SqlValue};

pub fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn bytes_to_csv_text(bytes: &[u8]) -> String {
    if let Ok(text) = std::str::from_utf8(bytes) {
        return text.to_string();
    }

    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn value_to_csv_text(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => String::new(),
        SqlValue::I64(n) => n.to_string(),
        SqlValue::U64(n) => n.to_string(),
        SqlValue::F64(n) => n.to_string(),
        SqlValue::Bytes(bytes) => bytes_to_csv_text(bytes),
    }
}

pub fn write_csv_header<W: Write>(out: &mut W, columns: &[&str]) -> io::Result<()> {
    for (i, column) in columns.iter().enumerate() {
        if i > 0 {
            out.write_all(b",")?;
        }
        out.write_all(csv_escape(column).as_bytes())?;
    }
    out.write_all(b"\n")
}

pub fn write_generic_row<W: Write>(out: &mut W, row: &GenericRow) -> io::Result<()> {
    for (i, value) in row.values.iter().enumerate() {
        if i > 0 {
            out.write_all(b",")?;
        }
        let value_text = value_to_csv_text(value);
        out.write_all(csv_escape(&value_text).as_bytes())?;
    }
    out.write_all(b"\n")
}
