use std::io::{self, BufRead};

use crate::sql_parsing::{parse_sql_quoted_bytes, skip_spaces};

use super::schema::WikipediaTable;

#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    I64(i64),
    U64(u64),
    F64(f64),
    Bytes(Vec<u8>),
}

impl SqlValue {
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::U64(n) => Some(*n),
            Self::I64(n) => u64::try_from(*n).ok(),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::I64(n) => Some(*n),
            Self::U64(n) => i64::try_from(*n).ok(),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GenericRow {
    pub table: WikipediaTable,
    pub values: Vec<SqlValue>,
}

fn parse_numeric_value(token: &[u8]) -> io::Result<SqlValue> {
    let has_float_marker = token.iter().any(|b| matches!(*b, b'.' | b'e' | b'E'));
    if has_float_marker {
        let text = std::str::from_utf8(token).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "numeric token is not valid UTF-8",
            )
        })?;
        let parsed = text.parse::<f64>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "failed to parse floating-point token",
            )
        })?;
        return Ok(SqlValue::F64(parsed));
    }

    if token.starts_with(b"-") {
        let text = std::str::from_utf8(token).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "numeric token is not valid UTF-8",
            )
        })?;
        let parsed = text.parse::<i64>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "failed to parse signed integer token",
            )
        })?;
        return Ok(SqlValue::I64(parsed));
    }

    let text = std::str::from_utf8(token).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "numeric token is not valid UTF-8",
        )
    })?;
    let parsed = text.parse::<u64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "failed to parse unsigned integer token",
        )
    })?;
    Ok(SqlValue::U64(parsed))
}

fn parse_value(tuple: &[u8], mut i: usize) -> io::Result<(SqlValue, usize)> {
    i = skip_spaces(tuple, i);
    if i >= tuple.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected end of tuple while parsing value",
        ));
    }

    if tuple[i] == b'\'' {
        let (bytes, next) = parse_sql_quoted_bytes(tuple, i).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "failed to parse SQL quoted value",
            )
        })?;
        return Ok((SqlValue::Bytes(bytes), next));
    }

    if tuple[i..].starts_with(b"NULL") {
        return Ok((SqlValue::Null, i + 4));
    }

    let start = i;
    while i < tuple.len() && tuple[i] != b',' && tuple[i] != b')' {
        i += 1;
    }

    let end = i;
    let token = std::str::from_utf8(&tuple[start..end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF-8 unquoted token"))?
        .trim();
    if token.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty unquoted token",
        ));
    }

    let value = parse_numeric_value(token.as_bytes())?;
    Ok((value, end))
}

fn parse_tuple_values(tuple: &[u8]) -> io::Result<Vec<SqlValue>> {
    let mut i = skip_spaces(tuple, 0);
    if i >= tuple.len() || tuple[i] != b'(' {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected '(' at tuple start",
        ));
    }
    i += 1;

    let mut values = Vec::new();
    loop {
        i = skip_spaces(tuple, i);
        if i >= tuple.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected end of tuple",
            ));
        }

        if tuple[i] == b')' {
            i += 1;
            i = skip_spaces(tuple, i);
            if i != tuple.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected characters after tuple close",
                ));
            }
            return Ok(values);
        }

        let (value, next) = parse_value(tuple, i)?;
        values.push(value);
        i = skip_spaces(tuple, next);

        if i >= tuple.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected end of tuple after value",
            ));
        }

        match tuple[i] {
            b',' => i += 1,
            b')' => {
                i += 1;
                i = skip_spaces(tuple, i);
                if i != tuple.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected characters after tuple close",
                    ));
                }
                return Ok(values);
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected comma or ')' after tuple value",
                ));
            }
        }
    }
}

pub struct TableRowsIter<R: BufRead> {
    reader: R,
    prefix: Vec<u8>,
    table: WikipediaTable,
    buf: [u8; 8192],
    buf_len: usize,
    buf_pos: usize,
    prefix_match: usize,
    in_values: bool,
    tuple_buf: Vec<u8>,
    in_quote: bool,
    quote_pending: bool,
    after_backslash: bool,
    finished: bool,
}

impl<R: BufRead> TableRowsIter<R> {
    pub fn new(reader: R, table: WikipediaTable) -> Self {
        let prefix = format!("INSERT INTO `{}` VALUES ", table.table_name()).into_bytes();
        Self {
            reader,
            prefix,
            table,
            buf: [0; 8192],
            buf_len: 0,
            buf_pos: 0,
            prefix_match: 0,
            in_values: false,
            tuple_buf: Vec::new(),
            in_quote: false,
            quote_pending: false,
            after_backslash: false,
            finished: false,
        }
    }

    fn parsed_row(&self, tuple: Vec<u8>) -> io::Result<GenericRow> {
        let values = parse_tuple_values(&tuple)?;
        let expected_columns = self.table.expected_columns();
        if values.len() != expected_columns {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unexpected column count for {}: expected {}, got {}",
                    self.table.table_name(),
                    expected_columns,
                    values.len()
                ),
            ));
        }
        Ok(GenericRow {
            table: self.table,
            values,
        })
    }

    fn process_byte(&mut self, b: u8) -> Option<io::Result<GenericRow>> {
        if !self.in_values {
            if b == self.prefix[self.prefix_match] {
                self.prefix_match += 1;
                if self.prefix_match == self.prefix.len() {
                    self.in_values = true;
                    self.prefix_match = 0;
                }
            } else {
                self.prefix_match = usize::from(b == self.prefix[0]);
            }
            return None;
        }

        if self.tuple_buf.is_empty() {
            if b == b'(' {
                self.tuple_buf.push(b);
                self.in_quote = false;
                self.quote_pending = false;
                self.after_backslash = false;
            } else if b == b';' {
                self.in_values = false;
            }
            return None;
        }

        self.tuple_buf.push(b);

        if self.in_quote {
            if self.quote_pending {
                if b == b'\'' {
                    self.quote_pending = false;
                    return None;
                }
                self.in_quote = false;
                self.quote_pending = false;
            } else if self.after_backslash {
                self.after_backslash = false;
                return None;
            } else {
                match b {
                    b'\\' => {
                        self.after_backslash = true;
                        return None;
                    }
                    b'\'' => {
                        self.quote_pending = true;
                        return None;
                    }
                    _ => return None,
                }
            }
        }

        if !self.in_quote {
            if b == b'\'' {
                self.in_quote = true;
                self.quote_pending = false;
                self.after_backslash = false;
                return None;
            }

            if b == b')' {
                let tuple = std::mem::take(&mut self.tuple_buf);
                return Some(self.parsed_row(tuple));
            }
        }

        None
    }
}

impl<R: BufRead> Iterator for TableRowsIter<R> {
    type Item = io::Result<GenericRow>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            if self.buf_pos >= self.buf_len {
                match self.reader.read(&mut self.buf) {
                    Ok(0) => {
                        self.finished = true;
                        if self.tuple_buf.is_empty() {
                            return None;
                        }
                        return Some(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "unexpected EOF while parsing {} tuple",
                                self.table.table_name()
                            ),
                        )));
                    }
                    Ok(n) => {
                        self.buf_len = n;
                        self.buf_pos = 0;
                    }
                    Err(err) => {
                        self.finished = true;
                        return Some(Err(err));
                    }
                }
            }

            while self.buf_pos < self.buf_len {
                let b = self.buf[self.buf_pos];
                self.buf_pos += 1;
                if let Some(row) = self.process_byte(b) {
                    return Some(row);
                }
            }
        }
    }
}

pub fn iter_table_rows<R: BufRead>(reader: R, table: WikipediaTable) -> TableRowsIter<R> {
    TableRowsIter::new(reader, table)
}

pub fn iter_table_rows_by_name<R: BufRead>(
    reader: R,
    table_name: &str,
) -> io::Result<TableRowsIter<R>> {
    let table = WikipediaTable::from_table_name(table_name).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported table: {}", table_name),
        )
    })?;
    Ok(iter_table_rows(reader, table))
}

pub fn parse_i32_field(value: &SqlValue, field_name: &str) -> io::Result<i32> {
    match value {
        SqlValue::I64(n) => i32::try_from(*n).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} out of i32 range", field_name),
            )
        }),
        SqlValue::U64(n) => i32::try_from(*n).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} out of i32 range", field_name),
            )
        }),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid {}", field_name),
        )),
    }
}

pub fn parse_u32_field(value: &SqlValue, field_name: &str) -> io::Result<u32> {
    match value {
        SqlValue::U64(n) => u32::try_from(*n).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} out of u32 range", field_name),
            )
        }),
        SqlValue::I64(n) => u32::try_from(*n).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} out of u32 range", field_name),
            )
        }),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid {}", field_name),
        )),
    }
}

pub fn parse_u64_field(value: &SqlValue, field_name: &str) -> io::Result<u64> {
    match value {
        SqlValue::U64(n) => Ok(*n),
        SqlValue::I64(n) => u64::try_from(*n).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} out of u64 range", field_name),
            )
        }),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid {}", field_name),
        )),
    }
}

pub fn parse_bytes_field(value: &SqlValue, field_name: &str) -> io::Result<Vec<u8>> {
    match value {
        SqlValue::Bytes(bytes) => Ok(bytes.clone()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid {}", field_name),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor};

    use super::{SqlValue, iter_table_rows};
    use crate::parsers::schema::WikipediaTable;

    #[test]
    fn iter_table_rows_parses_linktarget_rows() {
        let sql = b"INSERT INTO `linktarget` VALUES (1,0,'A'),(2,-1,'B');";
        let rows = iter_table_rows(Cursor::new(&sql[..]), WikipediaTable::LinkTarget)
            .collect::<io::Result<Vec<_>>>()
            .expect("must parse");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[0], SqlValue::U64(1));
        assert_eq!(rows[0].values[1], SqlValue::U64(0));
        assert_eq!(rows[0].values[2], SqlValue::Bytes(b"A".to_vec()));
        assert_eq!(rows[1].values[1], SqlValue::I64(-1));
    }

    #[test]
    fn iter_table_rows_rejects_wrong_arity() {
        let sql = b"INSERT INTO `pagelinks` VALUES (10,0);";
        let mut iter = iter_table_rows(Cursor::new(&sql[..]), WikipediaTable::PageLinks);
        let err = iter
            .next()
            .expect("must yield error")
            .expect_err("must fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
