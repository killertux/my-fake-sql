use msql_srv::{Column, ColumnFlags, ColumnType};
use std::io::{BufRead, BufReader, Read, Result};

pub use query_accumulator::QueryAccumulator;
pub use query_data_type::QueryDataType;
pub use query_filter::QueryFilter;
pub use query_sanitizer::QuerySanitizer;
pub use runops::Runops;

mod query_accumulator;
mod query_data_type;
mod query_filter;
mod query_sanitizer;
mod runops;

type Rows = Vec<String>;
type Columns = Vec<Column>;

pub trait QueryExecutor {
    type QueryResult;
    fn query(&mut self, query: &str) -> Result<Option<Self::QueryResult>>;
}

pub trait QueryResult {
    fn get_data(self) -> (Result<Columns>, Box<dyn Iterator<Item = Result<Rows>>>);
}

pub struct ReaderQueryResult {
    reader: Box<dyn BufRead>,
}

impl QueryResult for ReaderQueryResult {
    fn get_data(mut self) -> (Result<Columns>, Box<dyn Iterator<Item = Result<Rows>>>) {
        (self.get_columns(), self.get_rows())
    }
}

impl ReaderQueryResult {
    fn new(reader: impl Read + 'static) -> Self {
        Self {
            reader: Box::new(BufReader::new(reader)),
        }
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

    fn get_rows(self) -> Box<dyn Iterator<Item = Result<Vec<String>>>> {
        Box::new(
            self.reader
                .lines()
                .filter(|result_row| match result_row {
                    Ok(row) => !row.is_empty(),
                    Err(_) => true,
                })
                .map(|result_row| {
                    result_row.map(|row| row.split('\t').map(|value| value.to_string()).collect())
                }),
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Result;
    use std::rc::Rc;

    pub struct FakeQueryExecutor {
        result_list: Vec<Result<Option<ReaderQueryResult>>>,
        query_list: Rc<Vec<String>>,
    }

    impl FakeQueryExecutor {
        pub fn new(result_list: Vec<Result<Option<ReaderQueryResult>>>) -> Self {
            Self {
                result_list: result_list,
                query_list: Rc::new(Vec::new()),
            }
        }

        pub fn get_query_list(&self) -> Rc<Vec<String>> {
            self.query_list.clone()
        }
    }

    impl QueryExecutor for FakeQueryExecutor {
        type QueryResult = ReaderQueryResult;
        fn query(&mut self, query: &str) -> Result<Option<ReaderQueryResult>> {
            Rc::get_mut(&mut self.query_list)
                .unwrap()
                .push(query.to_string());
            self.result_list.pop().unwrap()
        }
    }
}
