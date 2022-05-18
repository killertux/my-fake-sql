use super::{QueryExecutor, ReaderQueryResult};
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
    type QueryResult = ReaderQueryResult;

    fn query(&mut self, query: &str) -> Result<Option<Self::QueryResult>> {
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
        if output.stdout.len() > 9 && String::from_utf8_lossy(&output.stdout[..8]) == "https://" {
            let url = String::from_utf8_lossy(&output.stdout);
            let body = reqwest::blocking::get(url.as_ref()).expect("Error getting data from URL");
            return Ok(Some(ReaderQueryResult::new(body)));
        }
        if output.stdout.len() > 25
            && String::from_utf8_lossy(&output.stdout[..24]) == "Task returned empty logs"
        {
            return Ok(None);
        }

        Ok(Some(ReaderQueryResult::new(Cursor::new(output.stdout))))
    }
}
