use std::io;

use std::io::{BufRead, Cursor};

use super::generic::{
    TableRowsIter, iter_table_rows, parse_bytes_field, parse_i32_field, parse_u32_field,
};
use super::schema::WikipediaTable;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRow {
    pub id: u32,
    pub namespace: i32,
    pub title: Vec<u8>,
}

pub struct PageIter<R: BufRead> {
    inner: TableRowsIter<R>,
}

impl<R: BufRead> PageIter<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: iter_table_rows(reader, WikipediaTable::Page),
        }
    }
}

impl<R: BufRead> Iterator for PageIter<R> {
    type Item = io::Result<PageRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|row| {
            let row = row?;
            Ok(PageRow {
                id: parse_u32_field(&row.values[0], "page_id")?,
                namespace: parse_i32_field(&row.values[1], "page_namespace")?,
                title: parse_bytes_field(&row.values[2], "page_title")?,
            })
        })
    }
}

pub fn iter_rows<R: BufRead>(reader: R) -> PageIter<R> {
    PageIter::new(reader)
}

pub fn for_each_row<F>(sql: &[u8], mut f: F) -> io::Result<()>
where
    F: FnMut(PageRow) -> io::Result<bool>,
{
    for row in iter_rows(Cursor::new(sql)) {
        let should_continue = f(row?)?;
        if !should_continue {
            break;
        }
    }

    Ok(())
}
