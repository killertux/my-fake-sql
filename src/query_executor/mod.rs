use msql_srv::{Column, ColumnFlags, ColumnType};
use std::io::{BufRead, BufReader, Cursor, Read, Result};

pub use query_accumulator::QueryAccumulator;
pub use query_filter::QueryFilter;
pub use query_sanitizer::QuerySanitizer;
pub use runops::Runops;

mod query_accumulator;
mod query_filter;
mod query_sanitizer;
mod runops;

type Rows = Vec<String>;
type Columns = Vec<Column>;

pub trait QueryExecutor {
    fn query(&mut self, query: &str) -> Result<QueryResult>;
}

pub struct QueryResult {
    reader: Box<dyn BufRead>,
}

impl QueryResult {
    fn new(reader: impl Read + 'static) -> Self {
        Self {
            reader: Box::new(BufReader::new(reader)),
        }
    }

    fn empty() -> Self {
        Self::new(Cursor::new("\n"))
    }

    pub fn get_data(mut self) -> (Result<Columns>, impl Iterator<Item = Result<Rows>>) {
        (self.get_columns(), self.get_rows())
    }

    fn get_columns(&mut self) -> Result<Vec<Column>> {
        let mut header = String::new();
        self.reader.read_line(&mut header)?;
        Ok(header
            .split('\t')
            .map(|column_name| column_name.trim())
            .filter(|column_name| !column_name.is_empty())
            .map(|column| Column {
                table: "none".to_string(),
                column: column.to_string(),
                coltype: ColumnType::MYSQL_TYPE_STRING,
                colflags: ColumnFlags::empty(),
            })
            .collect())
    }

    fn get_rows(self) -> impl Iterator<Item = Result<Vec<String>>> {
        self.reader
            .lines()
            .filter(|result_row| match result_row {
                Ok(row) => !row.is_empty(),
                Err(_) => true,
            })
            .map(|result_row| {
                result_row.map(|row| row.split('\t').map(|value| value.to_string()).collect())
            })
    }
}

impl TryFrom<QueryResult> for String {
    type Error = std::io::Error;
    fn try_from(result: QueryResult) -> std::result::Result<String, Self::Error> {
        let (columns, rows) = result.get_data();
        let mut output = columns?
            .into_iter()
            .map(|column| column.column)
            .collect::<Vec<String>>()
            .join("\t")
            + "\n";
        for row in rows {
            output += &(row?.join("\t") + "\n");
        }
        Ok(output)
    }
}
