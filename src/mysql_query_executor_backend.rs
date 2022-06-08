use super::query_executor::{Column, ColumnValue, QueryExecutor, QueryResult, SqlError};
use anyhow::{bail, Result};
use chrono::{NaiveDate, NaiveDateTime};
use msql_srv::*;
use msql_srv::{Column as MySqlColumn, ColumnFlags};
use std::io::{Error, Read, Write};

pub struct Backend<T> {
    executor: T,
    prepared_statements: Vec<String>,
}

impl From<Column> for MySqlColumn {
    fn from(column: Column) -> Self {
        MySqlColumn {
            table: String::new(),
            column: column.name,
            colflags: ColumnFlags::empty(),
            coltype: match column.ty.as_ref().map(|ty| ty.as_str()) {
                Some("bigint") => ColumnType::MYSQL_TYPE_LONGLONG,
                Some("varchar") => ColumnType::MYSQL_TYPE_VAR_STRING,
                Some("tinyint") => ColumnType::MYSQL_TYPE_TINY,
                Some("datetime") => ColumnType::MYSQL_TYPE_DATETIME,
                Some("int") => ColumnType::MYSQL_TYPE_LONG,
                Some("mediumint") => ColumnType::MYSQL_TYPE_LONG,
                Some("text") => ColumnType::MYSQL_TYPE_STRING,
                Some("enum") => ColumnType::MYSQL_TYPE_ENUM,
                Some("decimal") => ColumnType::MYSQL_TYPE_NEWDECIMAL,
                Some("date") => ColumnType::MYSQL_TYPE_DATE,
                Some("binary") => ColumnType::MYSQL_TYPE_BLOB,
                Some("double") => ColumnType::MYSQL_TYPE_DOUBLE,
                Some("char") => ColumnType::MYSQL_TYPE_STRING,
                Some("tinytext") => ColumnType::MYSQL_TYPE_STRING,
                Some("time") => ColumnType::MYSQL_TYPE_TIME,
                Some("timestamp") => ColumnType::MYSQL_TYPE_TIMESTAMP,
                Some("smallint") => ColumnType::MYSQL_TYPE_INT24,
                Some("blob") => ColumnType::MYSQL_TYPE_BLOB,
                Some("float") => ColumnType::MYSQL_TYPE_FLOAT,
                Some("mediumblob") => ColumnType::MYSQL_TYPE_MEDIUM_BLOB,
                Some("longtext") => ColumnType::MYSQL_TYPE_STRING,
                Some("mediumtext") => ColumnType::MYSQL_TYPE_STRING,
                Some("varbinary") => ColumnType::MYSQL_TYPE_BLOB,
                Some("year") => ColumnType::MYSQL_TYPE_YEAR,
                Some("bit") => ColumnType::MYSQL_TYPE_BIT,
                None => ColumnType::MYSQL_TYPE_STRING,
                Some(any) => {
                    println!("Type not mapped {}", any);
                    ColumnType::MYSQL_TYPE_STRING
                }
            },
        }
    }
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

    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &MySqlColumn) -> std::io::Result<()> {
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

impl<T> Backend<T> {
    pub fn new(executor: T) -> Self {
        Self {
            executor,
            prepared_statements: Vec::new(),
        }
    }

    fn do_query<W: Write + Read, R>(
        &mut self,
        query: &str,
        results: QueryResultWriter<W>,
    ) -> Result<()>
    where
        T: QueryExecutor<QueryResult = R>,
        R: QueryResult,
    {
        println!("Query {}", query);
        let result = self.executor.query(query);
        match result {
            Ok(Some(query_result)) => {
                let (columns, rows) = query_result.get_data();
                let columns = columns?
                    .into_iter()
                    .map(|column| column.into())
                    .collect::<Vec<MySqlColumn>>();
                if columns.is_empty() {
                    return Ok(results.completed(0, 0)?);
                }
                let mut rw = results.start(&columns)?;
                let mut i = 0;
                for row in rows {
                    i += 1;
                    rw.write_row(row?)?;
                }
                println!("Number of rows: {}", i);
                Ok(rw.finish()?)
            }
            Ok(None) => Ok(results.start(&[])?.finish()?),
            Err(error) => match error.downcast_ref::<SqlError>() {
                Some(sql_error) => {
                    println!("Sql Error: {sql_error}");
                    Ok(results.error(
                        ErrorKind::ER_ERROR_ON_READ, // Using this as a generic error. Doing a mapping here would be too difficult
                        sql_error.to_string().as_bytes(),
                    )?)
                }
                None => bail!("{error}"),
            },
        }
    }
}

impl<W: Write + Read, T, R> MysqlShim<W> for Backend<T>
where
    T: QueryExecutor<QueryResult = R>,
    R: QueryResult,
{
    type Error = Error;

    fn on_prepare(&mut self, query: &str, info: StatementMetaWriter<W>) -> std::io::Result<()> {
        self.prepared_statements.push(query.to_string());
        let params: Vec<MySqlColumn> = query
            .chars()
            .filter(|character| *character == '?')
            .map(|_| MySqlColumn {
                table: "none".to_string(),
                column: "?".to_string(),
                coltype: ColumnType::MYSQL_TYPE_STRING,
                colflags: ColumnFlags::empty(),
            })
            .collect();
        info.reply((self.prepared_statements.len() - 1) as u32, &params, &[])
    }
    fn on_execute(
        &mut self,
        statement_id: u32,
        pp: ParamParser,
        results: QueryResultWriter<W>,
    ) -> std::io::Result<()> {
        let query = self
            .prepared_statements
            .get(statement_id as usize)
            .map(|query| query.clone());
        match query {
            Some(mut query) => {
                for param in pp.into_iter() {
                    // This is a mess. msql-srv has some very bad ways of handling this types and conversions
                    let value_str = match param.value.into_inner() {
                        ValueInner::NULL => "NULL".to_string(),
                        ValueInner::Bytes(bytes) => escaped(&String::from_utf8_lossy(bytes)),
                        ValueInner::Double(value) => format!("{}", value),
                        ValueInner::Int(value) => format!("{}", value),
                        ValueInner::UInt(value) => format!("{}", value),
                        ValueInner::Date(data) => {
                            let value = Value::from_inner(ValueInner::Date(data));
                            let date = NaiveDate::from(value);
                            escaped(&date.to_string())
                        }
                        ValueInner::Datetime(data) => {
                            let value = Value::from_inner(ValueInner::Datetime(data));
                            let date_time = NaiveDateTime::from(value);
                            escaped(&date_time.to_string())
                        }
                        ValueInner::Time(_) => panic!("Not sure how to parse this yet"),
                    };
                    query = query.replacen("?", &value_str, 1);
                }
                self.on_query(&query, results)
            }
            None => results.error(
                ErrorKind::ER_STMT_HAS_NO_OPEN_CURSOR,
                &"Statement not found".as_bytes(),
            ),
        }
    }
    fn on_close(&mut self, statement_id: u32) {
        self.prepared_statements.remove(statement_id as usize);
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> std::io::Result<()> {
        Ok(self.do_query(query, results).unwrap())
    }
}

fn escaped(value: &str) -> String {
    format!("'{}'", value)
}
