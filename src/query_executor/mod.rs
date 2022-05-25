use chrono::{NaiveDate, NaiveDateTime};
use msql_srv::{Column, ColumnFlags, ColumnType, ToMysqlValue};
use std::io::{BufRead, BufReader, Read, Result, Write};

pub use query_accumulator::QueryAccumulator;
pub use query_data_type::QueryDataType;
pub use query_filter::QueryFilter;
pub use query_sanitizer::QuerySanitizer;
pub use runops::RunopsApi;

mod query_accumulator;
mod query_data_type;
mod query_filter;
mod query_sanitizer;
mod runops;

pub type Rows = Vec<ColumnValue>;
type Columns = Vec<Column>;

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

impl ToMysqlValue for ColumnValue {
    fn is_null(&self) -> bool {
        match self {
            ColumnValue::Null => true,
            _ => false,
        }
    }

    fn to_mysql_text<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        match self {
            ColumnValue::Null => w.write_all(&[0xFB]),
            ColumnValue::String(string) => string.to_mysql_text(w),
            ColumnValue::I64(number) => number.to_mysql_text(w),
            ColumnValue::I32(number) => number.to_mysql_text(w),
            ColumnValue::I16(number) => number.to_mysql_text(w),
            ColumnValue::I8(number) => number.to_mysql_text(w),
            ColumnValue::Double(number) => number.to_mysql_text(w),
            ColumnValue::Float(number) => number.to_mysql_text(w),
            ColumnValue::DateTime(date_time) => date_time.to_mysql_text(w),
            ColumnValue::Date(date) => date.to_mysql_text(w),
        }
    }

    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> std::io::Result<()> {
        match self {
            ColumnValue::Null => unreachable!(),
            ColumnValue::String(string) => string.to_mysql_bin(w, c),
            ColumnValue::I64(number) => number.to_mysql_bin(w, c),
            ColumnValue::I32(number) => number.to_mysql_bin(w, c),
            ColumnValue::I16(number) => number.to_mysql_bin(w, c),
            ColumnValue::I8(number) => number.to_mysql_bin(w, c),
            ColumnValue::Double(number) => number.to_mysql_bin(w, c),
            ColumnValue::Float(number) => number.to_mysql_bin(w, c),
            ColumnValue::DateTime(date_time) => date_time.to_mysql_bin(w, c),
            ColumnValue::Date(date) => date.to_mysql_bin(w, c),
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
                                value => ColumnValue::String(value.to_string()),
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
