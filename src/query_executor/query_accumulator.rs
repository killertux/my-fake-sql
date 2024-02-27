use super::QueryExecutor;
use anyhow::Result;
use itertools::Itertools;
use ritelinked::LinkedHashSet;

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
    type QueryResult = T::QueryResult;
    fn query(&mut self, query: &str) -> Result<Option<Self::QueryResult>> {
        let lower_case_query = query.to_lowercase();
        if lower_case_query.starts_with("set") {
            self.acc.insert(query.to_string());
            return Ok(None);
        }
        if self.acc.is_empty() {
            return self.executor.query(query);
        }

        self.executor
            .query(&(self.acc.iter().join(";\n") + ";\n" + query))
    }
}
