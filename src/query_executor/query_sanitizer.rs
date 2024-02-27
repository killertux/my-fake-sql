use super::QueryExecutor;
use anyhow::Result;
use itertools::Itertools;

pub struct QuerySanitizer<T>(T);

impl<T> QuerySanitizer<T> {
    pub fn new(executor: T) -> Self {
        Self(executor)
    }
}

impl<T> QueryExecutor for QuerySanitizer<T>
where
    T: QueryExecutor,
{
    type QueryResult = T::QueryResult;
    fn query(&mut self, query: &str) -> Result<Option<Self::QueryResult>> {
        let mut query: String = String::from(query.trim());
        if query.starts_with('/') {
            query = remove_comments_from_the_start(query);
        }
        query = query
            .trim()
            .lines()
            .map(|line| line.trim())
            .filter(|line| !line.trim().starts_with("--"))
            .filter(|line| !line.trim().starts_with('#'))
            .collect::<Vec<&str>>()
            .join("\n");
        self.0.query(&query.replace("@@language", "'english'"))
    }
}

fn remove_comments_from_the_start(query: String) -> String {
    // There are some very crazy allocations happening here. We can probably clean it up later
    query
        .chars()
        .tuple_windows()
        .skip_while(|(prev, next)| !(*prev == '*' && *next == '/'))
        .skip(1)
        .map(|(_, next)| next)
        .collect()
}
