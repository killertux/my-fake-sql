use super::{Columns, QueryExecutor, QueryResult, Row};
use anyhow::Result;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;

pub struct QueryCache<T, S> {
    executor: T,
    storage: S,
    queries_to_cache: HashSet<String>,
}

pub trait QueryStorage {
    fn store(&mut self, query: &str, columns: Columns, rows: Vec<Row>);
    fn get(&self, query: &str) -> Option<CachedResult>;
}

#[derive(Clone)]
pub struct CachedResult {
    columns: Columns,
    rows: Vec<Row>,
}

pub enum CachedQueryResult<T: QueryResult> {
    CachedResult(CachedResult),
    Result(T),
}

impl<T: QueryResult> QueryResult for CachedQueryResult<T> {
    fn get_data(self) -> (Result<Columns>, Box<dyn Iterator<Item = Result<Row>>>) {
        match self {
            CachedQueryResult::Result(result) => result.get_data(),
            CachedQueryResult::CachedResult(cached_result) => (
                Ok(cached_result.columns),
                Box::new(cached_result.rows.into_iter().map(Ok)),
            ),
        }
    }
}

impl<T, S> QueryCache<T, S> {
    pub fn new(executor: T, storage: S, queries_to_cache: HashSet<String>) -> Self {
        Self {
            executor,
            storage,
            queries_to_cache,
        }
    }
}

impl<T, S, R> QueryExecutor for QueryCache<T, S>
where
    T: QueryExecutor<QueryResult = R>,
    S: QueryStorage,
    R: QueryResult,
{
    type QueryResult = CachedQueryResult<T::QueryResult>;
    fn query(&mut self, query: &str) -> Result<Option<Self::QueryResult>> {
        match self.storage.get(query) {
            None => match self.executor.query(query) {
                Ok(Some(result)) => {
                    if self.queries_to_cache.get(query).is_none() {
                        return Ok(Some(CachedQueryResult::Result(result)));
                    }
                    let (columns, rows) = result.get_data();
                    let columns = columns?;
                    let rows = rows.collect::<Result<Vec<Row>>>()?;
                    self.storage.store(query, columns.clone(), rows.clone());
                    Ok(Some(CachedQueryResult::CachedResult(CachedResult {
                        columns,
                        rows,
                    })))
                }
                Ok(None) => Ok(None),
                Err(err) => Err(err),
            },
            Some(result) => Ok(Some(CachedQueryResult::CachedResult(result))),
        }
    }
}

#[derive(Clone)]
pub struct InMemoryQueryStorage {
    dashmap: Arc<DashMap<String, CachedResult>>,
}

impl QueryStorage for InMemoryQueryStorage {
    fn store(&mut self, query: &str, columns: Columns, rows: Vec<Row>) {
        self.dashmap
            .insert(query.to_string(), CachedResult { columns, rows });
    }
    fn get(&self, query: &str) -> Option<CachedResult> {
        self.dashmap.get(query).map(|result| result.clone())
    }
}

impl InMemoryQueryStorage {
    pub fn new() -> Self {
        Self {
            dashmap: Arc::new(DashMap::new()),
        }
    }
}
