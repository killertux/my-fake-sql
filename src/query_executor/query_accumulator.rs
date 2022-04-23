use super::{QueryExecutor, QueryResult};
use itertools::Itertools;
use ritelinked::LinkedHashSet;
use std::io::Result;

pub struct QueryAccumulator<T> {
    executor: T,
    acc: LinkedHashSet<String>,
}

impl<T> QueryAccumulator<T> {
    pub fn new(executor: T) -> Self {
        Self {
            executor,
            acc: LinkedHashSet::new(),
        }
    }
}

impl<T> QueryExecutor for QueryAccumulator<T>
where
    T: QueryExecutor,
{
    fn query(&mut self, query: &str) -> Result<QueryResult> {
        if query.starts_with("SET") {
            self.acc.insert(query.to_string());
            return Ok(QueryResult::empty());
        }
        if self.acc.is_empty() {
            return self.executor.query(query);
        }

        let result = self
            .executor
            .query(&(self.acc.iter().join(";\n") + ";\n" + query));
        return result;
    }
}
