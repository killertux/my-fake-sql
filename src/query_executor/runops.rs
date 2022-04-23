use super::{QueryExecutor, QueryResult};
use std::io::{Cursor, Result};
use std::process::Command;

pub struct Runops {
    target: String,
}

impl Runops {
    pub fn new(target: String) -> Self {
        Self { target }
    }
}

impl QueryExecutor for Runops {
    fn query(&mut self, query: &str) -> Result<QueryResult> {
        let output = Command::new("runops")
            .arg("tasks")
            .arg("create")
            .arg("-t")
            .arg(&self.target)
            .arg("-m")
            .arg("Fake SQL")
            .arg("-s")
            .arg(query)
            .output()?;

        if String::from_utf8_lossy(&output.stdout[..8]) == "https://" {
            let url = String::from_utf8_lossy(&output.stdout);
            let body = reqwest::blocking::get(url.as_ref()).expect("Error getting data from URL");
            return Ok(QueryResult::new(body));
        }
        Ok(QueryResult::new(Cursor::new(output.stdout)))
    }
}
