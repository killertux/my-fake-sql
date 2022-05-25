use super::{QueryExecutor, ReaderQueryResult};
use reqwest::{
    blocking::{get, Client, ClientBuilder},
    header::HeaderMap,
    StatusCode,
};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Result};
use std::thread::sleep;
use std::time::Duration;

#[derive(Deserialize)]
struct LogsResult {
    logs_url: String,
}

pub struct RunopsApi {
    target: String,
    client: Client,
}

impl RunopsApi {
    pub fn new(target: String) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            std::fs::read_to_string(format!(
                "{}/.runops/config",
                dirs::home_dir().unwrap().to_string_lossy()
            ))?
            .parse()
            .unwrap(),
        );
        headers.insert("Accept", "application/json".parse().unwrap());
        Ok(Self {
            target,
            client: ClientBuilder::new()
                .default_headers(headers)
                .build()
                .expect("Error creating client"),
        })
    }
}

#[derive(Deserialize)]
struct RunopsTaskResponse {
    task_logs: String,
    id: u64,
}

#[derive(Serialize)]
struct RunopsTaskRequest<'a> {
    target: &'a str,
    script: &'a str,
}

impl<'a> RunopsTaskRequest<'a> {
    fn new(target: &'a str, script: &'a str) -> Self {
        Self { target, script }
    }
}

impl QueryExecutor for RunopsApi {
    type QueryResult = ReaderQueryResult;

    fn query(&mut self, query: &str) -> Result<Option<Self::QueryResult>> {
        let result: RunopsTaskResponse = self
            .client
            .post("https://api.runops.io/v1/tasks")
            .json(&RunopsTaskRequest::new(&self.target, query))
            .send()
            .expect("Error getting data from URL")
            .json()
            .expect("Error desserializing object");
        if result.task_logs.starts_with("https://") {
            let body = get(result.task_logs).expect("Error getting data from URL");
            return Ok(Some(ReaderQueryResult::new(body)));
        }
        if result.task_logs == "Task returned empty logs" {
            return Ok(None);
        }
        if result.task_logs.starts_with("Your task is running.") {
            println!(
                "Task {} is taking too long. We will need to pool for the result",
                result.id
            );
            loop {
                sleep(Duration::from_secs(5));
                let response = self
                    .client
                    .get(format!("https://api.runops.io/v1/tasks/{}/logs", result.id))
                    .send()
                    .expect("Error getting data from URL");
                match response.status() {
                    StatusCode::BAD_REQUEST => continue,
                    StatusCode::OK => {
                        let result: LogsResult =
                            response.json().expect("Error desserializing object");
                        let body = get(result.logs_url).expect("Error getting data from URL");
                        return Ok(Some(ReaderQueryResult::new(body)));
                    }
                    another_status => panic!("Unexpected status {another_status}"),
                }
            }
        }
        Ok(Some(ReaderQueryResult::new(Cursor::new(result.task_logs))))
    }
}
