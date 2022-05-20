use chrono::{NaiveDate, NaiveDateTime};
use msql_srv::{Column, ColumnFlags, ColumnType, ToMysqlValue};
use std::io::Write;
use std::io::{BufRead, BufReader, Read, Result};
use std::time::Duration;

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

pub type Rows = Vec<ColumnValue>;
type Columns = Vec<Column>;

pub enum ColumnValue {
    RawValue(String),
    Null,
}

impl ToString for ColumnValue {
    fn to_string(&self) -> String {
        match self {
            ColumnValue::Null => "NULL".to_string(),
            ColumnValue::RawValue(value) => value.clone(),
        }
    }
}

impl ToMysqlValue for ColumnValue {
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        match self {
            ColumnValue::RawValue(string) => write!(w, "{}", &string),
            ColumnValue::Null => write!(w, "NULL"),
        }
    }
    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> std::io::Result<()> {
        match self {
            ColumnValue::Null => {
                unreachable!("Should be handled by is_nul. Copied from the Option implementation")
            }
            ColumnValue::RawValue(value) => match c.coltype {
                ColumnType::MYSQL_TYPE_LONGLONG => value.parse::<i64>().unwrap().to_mysql_bin(w, c),
                ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
                    value.parse::<i32>().unwrap().to_mysql_bin(w, c)
                }
                ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                    value.parse::<i16>().unwrap().to_mysql_bin(w, c)
                }
                ColumnType::MYSQL_TYPE_DOUBLE => value.parse::<f64>().unwrap().to_mysql_bin(w, c),
                ColumnType::MYSQL_TYPE_FLOAT => value.parse::<f32>().unwrap().to_mysql_bin(w, c),
                ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_DATETIME2 => {
                    NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S")
                        .unwrap()
                        .to_mysql_bin(w, c)
                }
                ColumnType::MYSQL_TYPE_DATE => NaiveDate::parse_from_str(&value, "%Y-%m-%d")
                    .unwrap()
                    .to_mysql_bin(w, c),
                ColumnType::MYSQL_TYPE_TINY => value.parse::<i8>().unwrap().to_mysql_bin(w, c),
                ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                    value.as_bytes().to_mysql_bin(w, c)
                }
                ColumnType::MYSQL_TYPE_TIME => {
                    Duration::from_secs(value.parse::<u64>().unwrap()).to_mysql_bin(w, c)
                    // Not sure if we are parsing correctly here
                }
                _ => value.to_mysql_bin(w, c),
            },
        }
    }
    fn is_null(&self) -> bool {
        match self {
            ColumnValue::Null => true,
            _ => false,
        }
    }
}

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

    fn get_rows(self) -> Box<dyn Iterator<Item = Result<Rows>>> {
        Box::new(
            self.reader
                .lines()
                .filter(|result_row| match result_row {
                    Ok(row) => !row.is_empty(),
                    Err(_) => true,
                })
                .map(|result_row| {
                    result_row.map(|row| {
                        row.split('\t')
                            .map(|value| match value {
                                "NULL" => ColumnValue::Null,
                                value => ColumnValue::RawValue(value.to_string()),
                            })
                            .collect()
                    })
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
