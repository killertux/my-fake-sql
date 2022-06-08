use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime};
use std::io::{BufRead, BufReader, Read};

pub use query_accumulator::QueryAccumulator;
pub use query_cache::{InMemoryQueryStorage, QueryCache};
pub use query_data_type::{DataTypeInfo, QueryDataType};
pub use query_filter::QueryFilter;
pub use query_sanitizer::QuerySanitizer;
pub use runops::{RunopsApi, SqlError};

mod query_accumulator;
mod query_cache;
mod query_data_type;
mod query_filter;
mod query_sanitizer;
mod runops;

pub type Row = Vec<ColumnValue>;
type Columns = Vec<Column>;

#[derive(Clone)]
pub enum ColumnValue {
    Null,
    String(String),
    I64(i64),
    I32(i32),
    I16(i16),
    I8(i8),
    Double(f64),
    Float(f32),
    DateTime(NaiveDateTime),
    Date(NaiveDate),
}

#[derive(Clone)]
pub struct Column {
    pub name: String,
    pub ty: Option<String>,
}

pub trait QueryExecutor {
    type QueryResult;
    fn query(&mut self, query: &str) -> Result<Option<Self::QueryResult>>;
}

pub trait QueryResult {
    fn get_data(self) -> (Result<Columns>, Box<dyn Iterator<Item = Result<Row>>>);
}

pub struct ReaderQueryResult {
    reader: Box<dyn BufRead>,
}

impl QueryResult for ReaderQueryResult {
    fn get_data(mut self) -> (Result<Columns>, Box<dyn Iterator<Item = Result<Row>>>) {
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
                name: column.to_string(),
                ty: None,
            })
            .collect())
    }

    fn get_rows(self) -> Box<dyn Iterator<Item = Result<Row>>> {
        Box::new(
            self.reader
                .lines()
                .filter(|result_row| match result_row {
                    Ok(row) => !row.is_empty(),
                    Err(_) => true,
                })
                .map(|result_row| {
                    result_row
                        .map(|row| {
                            row.split('\t')
                                .map(|value| match value {
                                    "NULL" => ColumnValue::Null,
                                    value => ColumnValue::String(value.to_string()),
                                })
                                .collect()
                        })
                        .map_err(|io_error| io_error.into())
                }),
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
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
