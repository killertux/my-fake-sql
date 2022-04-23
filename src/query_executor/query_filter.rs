use super::{QueryExecutor, QueryResult};
use std::io::Result;

pub struct QueryFilter<T>(T);

impl<T> QueryFilter<T> {
    pub fn new(executor: T) -> Self {
        Self(executor)
    }
}

impl<T> QueryExecutor for QueryFilter<T>
where
    T: QueryExecutor,
{
    fn query(&mut self, query: &str) -> Result<QueryResult> {
        if query == "SHOW WARNINGS" {
            return Ok(QueryResult::empty());
        }

        self.0.query(&query)
    }
}
