use super::query_executor::{QueryExecutor, QueryResult, SqlError};
use anyhow::{bail, Result};
use chrono::{NaiveDate, NaiveDateTime};
use msql_srv::Column;
use msql_srv::*;
use std::io::{Error, Read, Write};

pub struct Backend<T> {
    executor: T,
    prepared_statements: Vec<String>,
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
                let columns = columns?;
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
        let params: Vec<Column> = query
            .chars()
            .filter(|character| *character == '?')
            .map(|_| Column {
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
