use std::io::{self, BufRead};

use super::generic::{
    TableRowsIter, iter_table_rows, parse_i32_field, parse_u32_field, parse_u64_field,
};
use super::schema::WikipediaTable;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageLinkRow {
    pub from_id: u32,
    pub target_id: u64,
    pub from_namespace: i32,
}

pub struct PageLinksIter<R: BufRead> {
    inner: TableRowsIter<R>,
}

impl<R: BufRead> PageLinksIter<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: iter_table_rows(reader, WikipediaTable::PageLinks),
        }
    }
}

impl<R: BufRead> Iterator for PageLinksIter<R> {
    type Item = io::Result<PageLinkRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|row| {
            let row = row?;
            Ok(PageLinkRow {
                from_id: parse_u32_field(&row.values[0], "pl_from")?,
                from_namespace: parse_i32_field(&row.values[1], "pl_from_namespace")?,
                target_id: parse_u64_field(&row.values[2], "pl_target_id")?,
            })
        })
    }
}

pub fn iter_rows<R: BufRead>(reader: R) -> PageLinksIter<R> {
    PageLinksIter::new(reader)
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor};

    use super::iter_rows;

    #[test]
    fn iter_rows_respects_tuple_boundaries_on_single_line_insert() {
        let sql = b"INSERT INTO `pagelinks` VALUES (10,0,11),(12,1,13),(14,-2,15);";
        let rows = iter_rows(Cursor::new(&sql[..]))
            .take(2)
            .collect::<io::Result<Vec<_>>>()
            .expect("must parse first two rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].from_id, 10);
        assert_eq!(rows[0].from_namespace, 0);
        assert_eq!(rows[0].target_id, 11);
        assert_eq!(rows[1].from_id, 12);
        assert_eq!(rows[1].from_namespace, 1);
        assert_eq!(rows[1].target_id, 13);
    }
}
