use msql_srv::*;
use query_executor::{QueryAccumulator, QueryDataType, QueryFilter, QuerySanitizer, Runops};
use query_executor_backend::Backend;
use serde::Deserialize;
use sqlparser::dialect::MySqlDialect;
use std::fs::File;
use std::net::TcpListener;
use std::thread;

mod query_executor;
mod query_executor_backend;

#[derive(Deserialize)]
struct YamlTargetConfig {
    port: u16,
    target: String,
    with_type_discovery: Option<bool>,
}

fn main() -> std::io::Result<()> {
    let file = File::open("config.yml")?;
    let configs: Vec<YamlTargetConfig> = serde_yaml::from_reader(file).expect("Error parsing yaml");
    for config in configs {
        thread::spawn(move || tcp_listener(config));
    }
    loop {
        thread::park();
    }
}

fn tcp_listener(config: YamlTargetConfig) -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port))
        .expect(&format!("Error binding to port {}", config.port));

    while let Ok((s, _)) = listener.accept() {
        spawn_sql_processor(&config, s)
    }
    Ok(())
}

fn spawn_sql_processor(config: &YamlTargetConfig, s: std::net::TcpStream) {
    let target = config.target.clone();
    let with_type_discovery = config.with_type_discovery.clone();
    thread::spawn(move || {
        if let Some(true) = with_type_discovery {
            MysqlIntermediary::run_on_tcp(
                Backend::new(QuerySanitizer::new(QueryFilter::new(QueryDataType::new(
                    QueryAccumulator::new(Runops::new(target)),
                    MySqlDialect {},
                )))),
                s,
            )
            .unwrap();
        } else {
            MysqlIntermediary::run_on_tcp(
                Backend::new(QuerySanitizer::new(QueryFilter::new(
                    QueryAccumulator::new(Runops::new(target)),
                ))),
                s,
            )
            .unwrap();
        }
    });
}
