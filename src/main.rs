use msql_srv::*;
use mysql_query_executor_backend::Backend;
use query_executor::{
    DataTypeInfo, InMemoryQueryStorage, QueryAccumulator, QueryCache, QueryDataType, QueryExecutor,
    QueryFilter, QueryResult, QuerySanitizer, RunopsApi,
};
use serde::Deserialize;
use sqlparser::dialect::MySqlDialect;
use std::collections::HashSet;
use std::fs::File;
use std::net::{TcpListener, TcpStream};
use std::thread;

mod mysql_query_executor_backend;
mod query_executor;

#[derive(Deserialize)]
struct YamlTargetConfig {
    port: u16,
    target: String,
    with_type_discovery: Option<bool>,
    query_cache: Option<Vec<String>>,
    target_type: Option<TargetType>,
}

#[derive(Deserialize, Clone)]
enum TargetType {
    MySql,
    Postgres,
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
    let queries_connection_cache: HashSet<String> = match &config.query_cache {
        None => HashSet::new(),
        Some(paths) => paths
            .iter()
            .flat_map(|path| {
                std::fs::read_to_string(path)
                    .expect("")
                    .split("|\n")
                    .map(|query| query.to_string())
                    .collect::<Vec<String>>()
            })
            .collect(),
    };

    let query_storage = InMemoryQueryStorage::new();
    let mut data_type_info = None;
    while let Ok((s, _)) = listener.accept() {
        spawn_sql_processor(
            &config,
            s,
            queries_connection_cache.clone(),
            query_storage.clone(),
            &mut data_type_info,
        )
    }
    Ok(())
}

fn spawn_sql_processor(
    config: &YamlTargetConfig,
    s: TcpStream,
    queries_connection_cache: HashSet<String>,
    storage: InMemoryQueryStorage,
    data_type_info: &mut Option<DataTypeInfo>,
) {
    let target = config.target.clone();
    let with_type_discovery = config.with_type_discovery.clone();
    let target_type = config.target_type.clone().unwrap_or(TargetType::MySql);

    if let Some(true) = with_type_discovery {
        let query_executor = construct_query_executor_with_data_type(
            target,
            queries_connection_cache,
            storage,
            data_type_info,
        );
        spawn_intermediary(s, query_executor, target_type)
    } else {
        let query_executor = construct_query_executor(target, queries_connection_cache, storage);
        spawn_intermediary(s, query_executor, target_type)
    }
}

fn construct_query_executor_with_data_type(
    target: String,
    queries_connection_cache: HashSet<String>,
    storage: InMemoryQueryStorage,
    data_type_info: &mut Option<DataTypeInfo>,
) -> impl QueryExecutor<QueryResult = impl QueryResult> {
    let mut runops_api = RunopsApi::new(target).expect("Error creating runops client");
    *data_type_info = data_type_info
        .take()
        .or_else(|| Some(DataTypeInfo::load(&mut runops_api).expect("Error loading datatype")));
    let data_type_info_clone = data_type_info.clone().unwrap();
    QueryCache::new(
        QuerySanitizer::new(QueryFilter::new(QueryDataType::new(
            QueryAccumulator::new(runops_api),
            MySqlDialect {},
            data_type_info_clone,
        ))),
        storage,
        queries_connection_cache,
    )
}

fn construct_query_executor(
    target: String,
    queries_connection_cache: HashSet<String>,
    storage: InMemoryQueryStorage,
) -> impl QueryExecutor<QueryResult = impl QueryResult> {
    QueryCache::new(
        QuerySanitizer::new(QueryFilter::new(QueryAccumulator::new(
            RunopsApi::new(target).expect("Error creating runops client"),
        ))),
        storage,
        queries_connection_cache,
    )
}

fn spawn_intermediary(
    s: TcpStream,
    query_executor: impl QueryExecutor<QueryResult = impl QueryResult> + Send + 'static,
    target_type: TargetType,
) {
    thread::spawn(move || match target_type {
        TargetType::MySql => {
            MysqlIntermediary::run_on_tcp(Backend::new(query_executor), s).unwrap();
        }
        TargetType::Postgres => unimplemented!("Postgress bindings not implemented yet"),
    });
}
