use std::io::{self, BufRead};

use super::generic::{
    TableRowsIter, iter_table_rows, parse_bytes_field, parse_i32_field, parse_u64_field,
};
use super::schema::WikipediaTable;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkTargetRow {
    pub id: u64,
    pub namespace: i32,
    pub title: Vec<u8>,
}

pub struct LinkTargetIter<R: BufRead> {
    inner: TableRowsIter<R>,
}

impl<R: BufRead> LinkTargetIter<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: iter_table_rows(reader, WikipediaTable::LinkTarget),
        }
    }
}

impl<R: BufRead> Iterator for LinkTargetIter<R> {
    type Item = io::Result<LinkTargetRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|row| {
            let row = row?;
            Ok(LinkTargetRow {
                id: parse_u64_field(&row.values[0], "lt_id")?,
                namespace: parse_i32_field(&row.values[1], "lt_namespace")?,
                title: parse_bytes_field(&row.values[2], "lt_title")?,
            })
        })
    }
}

pub fn iter_rows<R: BufRead>(reader: R) -> LinkTargetIter<R> {
    LinkTargetIter::new(reader)
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor};

    use super::iter_rows;

    #[test]
    fn iter_rows_respects_tuple_boundaries_on_single_line_insert() {
        let sql = b"INSERT INTO `linktarget` VALUES (1,0,'A'),(2,-1,'B'),(3,2,'C');";
        let rows = iter_rows(Cursor::new(&sql[..]))
            .take(2)
            .collect::<io::Result<Vec<_>>>()
            .expect("must parse first two rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[1].id, 2);
    }
}
