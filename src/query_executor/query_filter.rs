use super::QueryExecutor;
use anyhow::Result;

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
    type QueryResult = T::QueryResult;
    fn query(&mut self, query: &str) -> Result<Option<Self::QueryResult>> {
        if query == "SHOW WARNINGS" {
            return Ok(None);
        }

        self.0.query(&query)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::query_executor::test::FakeQueryExecutor;
    #[test]
    fn test_filter() {
        let query_filter_data_provider = [
            ("SHOW WARNINGS", true),
            ("SET SQL_SELECT_LIMIT 501", false),
            ("SELECT * FROM payment", false),
        ];

        for (query, should_filter) in query_filter_data_provider {
            let fake_executor = FakeQueryExecutor::new(vec![Ok(None)]);
            let executed_queries = fake_executor.get_query_list();
            let mut filter = QueryFilter::new(fake_executor);
            filter.query(query).unwrap();
            if should_filter {
                assert_eq!(true, executed_queries.is_empty());
            } else {
                // let executed_queries = dbg!(executed_queries);
                assert_eq!(1, executed_queries.len());
                assert_eq!(query, executed_queries[0]);
            }
        }
    }
}
