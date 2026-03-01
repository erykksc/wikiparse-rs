use std::io::{self, Write};

use crate::parsers::generic::{GenericRow, SqlValue};

fn bytes_to_json_string_text(bytes: &[u8]) -> String {
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

fn write_json_escaped_string<W: Write>(out: &mut W, text: &str) -> io::Result<()> {
    out.write_all(b"\"")?;
    for ch in text.chars() {
        match ch {
            '"' => out.write_all(b"\\\"")?,
            '\\' => out.write_all(b"\\\\")?,
            '\u{08}' => out.write_all(b"\\b")?,
            '\u{0c}' => out.write_all(b"\\f")?,
            '\n' => out.write_all(b"\\n")?,
            '\r' => out.write_all(b"\\r")?,
            '\t' => out.write_all(b"\\t")?,
            c if c.is_control() => {
                write!(out, "\\u{:04x}", c as u32)?;
            }
            c => {
                let mut buf = [0; 4];
                out.write_all(c.encode_utf8(&mut buf).as_bytes())?;
            }
        }
    }
    out.write_all(b"\"")
}

fn write_json_value<W: Write>(out: &mut W, value: &SqlValue) -> io::Result<()> {
    match value {
        SqlValue::Null => out.write_all(b"null"),
        SqlValue::I64(n) => write!(out, "{}", n),
        SqlValue::U64(n) => write!(out, "{}", n),
        SqlValue::F64(n) => {
            if !n.is_finite() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "non-finite floating-point value is not valid JSON",
                ));
            }
            write!(out, "{}", n)
        }
        SqlValue::Bytes(bytes) => {
            let text = bytes_to_json_string_text(bytes);
            write_json_escaped_string(out, &text)
        }
    }
}

pub fn write_json_row_object<W: Write>(
    out: &mut W,
    column_names: &[&str],
    row: &GenericRow,
) -> io::Result<()> {
    out.write_all(b"{")?;

    for (i, (column_name, value)) in column_names.iter().zip(&row.values).enumerate() {
        if i > 0 {
            out.write_all(b",")?;
        }
        write_json_escaped_string(out, column_name)?;
        out.write_all(b":")?;
        write_json_value(out, value)?;
    }

    out.write_all(b"}")
}
