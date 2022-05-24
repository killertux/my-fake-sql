use super::{QueryExecutor, ReaderQueryResult};
use serde::Deserialize;
use std::io::{Cursor, Result};
use std::process::Command;
use std::thread::sleep;
use std::time::Duration;

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
        let output_len = output.stdout.len();
        if output_len > 8 && String::from_utf8_lossy(&output.stdout[..8]) == "https://" {
            let url = String::from_utf8_lossy(&output.stdout);
            let body = reqwest::blocking::get(url.as_ref()).expect("Error getting data from URL");
            return Ok(Some(ReaderQueryResult::new(body)));
        }
        if output_len > 24
            && String::from_utf8_lossy(&output.stdout[..24]) == "Task returned empty logs"
        {
            return Ok(None);
        }
        if output_len > 21
            && String::from_utf8_lossy(&output.stdout[..21]) == "Your task is running."
        {
            let complete_report = String::from_utf8_lossy(&output.stdout);
            let id = complete_report.trim().split('/').last().unwrap();
            println!("Task {id} is taking too long. We will need to pool for the result");
            loop {
                sleep(Duration::from_secs(5));
                let output = Command::new("runops")
                    .arg("tasks")
                    .arg("logs")
                    .arg("-i")
                    .arg(id)
                    .output()?;
                let output_as_string = String::from_utf8_lossy(&output.stdout);
                if output_as_string.trim()
                    == "The logs for this task are not available yet. Please try again in a minute."
                {
                    continue;
                }
                let logs_result: LogsResult = deser_hjson::from_str(&output_as_string).unwrap();
                let body = reqwest::blocking::get(&logs_result.logs_url)
                    .expect("Error getting data from URL");
                return Ok(Some(ReaderQueryResult::new(body)));
            }
        }

        Ok(Some(ReaderQueryResult::new(Cursor::new(output.stdout))))
    }
}

#[derive(Deserialize)]
struct LogsResult {
    logs_url: String,
}
