use super::query_executor::{QueryExecutor, QueryResult};
use msql_srv::*;
use std::io::{Error, Read, Result, Write};

pub struct Backend<T> {
    executor: T,
}

impl<T> Backend<T> {
    pub fn new(executor: T) -> Self {
        Self { executor }
    }
}

impl<W: Write + Read, T, R> MysqlShim<W> for Backend<T>
where
    T: QueryExecutor<QueryResult = R>,
    R: QueryResult,
{
    type Error = Error;

    fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<W>) -> Result<()> {
        println!("--- prepare ---");
        info.reply(42, &[], &[])
    }
    fn on_execute(&mut self, _: u32, _: ParamParser, results: QueryResultWriter<W>) -> Result<()> {
        println!("--- Execute ---");
        results.completed(0, 0)
    }
    fn on_close(&mut self, _: u32) {
        println!("--- Close ---");
    }

    fn on_query(&mut self, query: &str, results: QueryResultWriter<W>) -> Result<()> {
        println!("Query {}", query);
        let result = self.executor.query(query)?;
        match result {
            Some(query_result) => {
                let (columns, rows) = query_result.get_data();
                let columns = columns?;
                if columns.is_empty() {
                    return results.completed(0, 0);
                }
                let mut rw = results.start(&columns)?;
                let mut i = 0;
                for row in rows {
                    i += 1;
                    rw.write_row(
                        row?.into_iter()
                            .map(|value| if value == "NULL" { None } else { Some(value) }), // We should probably move this out to another place
                    )?;
                }
                println!("Number of rows: {}", i);
                rw.finish()
            }
            None => results.completed(0, 0),
        }
    }
}
