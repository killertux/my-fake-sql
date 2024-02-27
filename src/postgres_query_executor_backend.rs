use crate::query_executor::{ColumnValue, QueryExecutor, QueryResult};
use crate::DataTypeInfo;
use anyhow::Result;
use bytes::BytesMut;
use postgres_shim::{
    Column as PostgresColumn, DefaultServerParameters, FromSql, ParameterValue, PostgresShim,
    ResultWriter, ToSqlValue, Type,
};
// use postgres_types::{Type, FromSql};
use std::collections::HashMap;
use std::io::{Result as IoResult, Write};

pub struct PostgresBackend<T> {
    executor: T,
    queries: HashMap<String, String>,
    data_type_info: DataTypeInfo,
}

impl<T> PostgresBackend<T> {
    pub fn new(executor: T, data_type_info: DataTypeInfo) -> Self {
        Self {
            executor,
            queries: HashMap::new(),
            data_type_info,
        }
    }

    fn do_execute<S, R>(
        &mut self,
        query: &str,
        result_writer: ResultWriter<'_, S>,
        describe_columns: Option<Vec<PostgresColumn>>,
    ) -> Result<()>
    where
        S: Write,
        T: QueryExecutor<QueryResult = R>,
        R: QueryResult,
    {
        match self.executor.query(query)? {
            None => {
                result_writer.empty_result()?;
            }
            Some(result) => {
                let (columns, rows) = result.get_data();
                let columns: Vec<PostgresColumn> = describe_columns.unwrap_or(
                    columns?
                        .into_iter()
                        .map(|column| PostgresColumn {
                            name: column.name,
                            column_type: Type::TEXT,
                        })
                        .collect(),
                );
                let mut row_writer = result_writer.start_writing(&columns)?;
                let mut n_rows = 0;
                for row in rows {
                    let row = row?;
                    if row.len() == 1 {
                        if let ColumnValue::String(string) = &row[0] {
                            if string.ends_with("row)") || string.ends_with("rows)") {
                                continue;
                            }
                        }
                    }
                    row_writer.write_row(row)?;
                    n_rows += 1;
                }
                println!("Number of rows: {}", n_rows);
                row_writer.finish()?;
            }
        }
        Ok(())
    }

    fn describe_by_parsing_query(&mut self, portal: &str) -> Result<Option<Vec<PostgresColumn>>> {
        let query = portal
            .replace("at time zone 'UTC'", "")
            .replace("distinct", "")
            .replace("from pg_user", "from pg_catalog.pg_user")
            .replace("order by inhseqno)", ")")
            .replace("order by inhrelid)", ")");
        let ast = sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::PostgreSqlDialect {},
            &query,
        )?;
        let columns_types = self
            .data_type_info
            .get_columns_types_from_ast("public", ast)?;
        println!("Expected column types : {:?}", columns_types);
        match columns_types.len() {
            0 => Ok(None),
            _ => {
                let columns: Vec<PostgresColumn> = columns_types
                    .into_iter()
                    .map(|(column_name, column_type)| PostgresColumn {
                        name: column_name,
                        column_type: match column_type.map(|ty| ty.to_lowercase()).as_deref() {
                            Some("text") | Some("name") | None => Type::TEXT,
                            Some("bigint") => Type::INT8,
                            Some("uuid") => Type::UUID,
                            Some("oid") => Type::OID,
                            Some("character varying") => Type::VARCHAR,
                            Some("bytea") => Type::BYTEA,
                            Some("timestamp with time zone") => Type::TIMESTAMPTZ,
                            Some("boolean") => Type::BOOL,
                            Some("integer") => Type::INT4,
                            Some("array") => Type::ANYARRAY,
                            Some("date") => Type::DATE,
                            Some(any) => {
                                println!("Type not mapped {any}");
                                Type::TEXT
                            }
                        },
                    })
                    .collect();
                Ok(Some(columns))
            }
        }
    }
}

type Portal = String;

impl<T, R> PostgresShim<Portal> for PostgresBackend<T>
where
    T: QueryExecutor<QueryResult = R>,
    R: QueryResult,
{
    fn prepare(&mut self, query_name: String, query: String, _: Vec<Type>) -> IoResult<()> {
        self.queries.insert(query_name, query);
        Ok(())
    }
    fn bind(&mut self, query_name: String, parameters: Vec<ParameterValue>) -> IoResult<Portal> {
        let mut query = self.queries.get(&query_name).unwrap().clone();
        for (index, value) in parameters.into_iter().enumerate() {
            match value {
                ParameterValue::Text(text) => {
                    // query = query.replacen("?", &format!("'{}'", text), 1);
                    query = query.replacen(&format!("${}", index + 1), &format!("'{}'", text), 1);
                }
                ParameterValue::Binary(value) => {
                    // This is very wrong :)
                    // The only way of doing this right, is by parsing the query and checking what is
                    // expected for each parameter.
                    let parameter = match value.len() {
                        4 => format!("{}", i32::from_sql(&Type::INT4, &value).unwrap()),
                        8 => format!("{}", i64::from_sql(&Type::INT8, &value).unwrap()),
                        _ => {
                            println!("Assuming text type");
                            format!("'{}'", String::from_sql(&Type::TEXT, &value).unwrap())
                        }
                    };
                    query = query.replacen(&format!("${}", index + 1), &parameter, 1);
                    // unimplemented!("We dont know how to handle binary types yet {:?}", value)
                }
            }
        }
        println!("Query {query}");
        Ok(query)
    }

    fn describe(&mut self, portal: &Portal) -> IoResult<Option<Vec<PostgresColumn>>> {
        match self.describe_by_parsing_query(portal) {
            Ok(result) => Ok(result),
            Err(err) => {
                println!("Error during describing {err}");
                match self
                    .executor
                    .query(portal)
                    .expect("Error getting version from target")
                {
                    Some(result) => {
                        let (columns, _) = result.get_data();
                        let columns: Vec<PostgresColumn> = columns
                            .unwrap()
                            .into_iter()
                            .map(|column| PostgresColumn {
                                name: column.name,
                                column_type: Type::TEXT,
                            })
                            .collect();
                        Ok(Some(columns))
                    }
                    None => Ok(None),
                }
            }
        }
    }

    fn execute<S>(
        &mut self,
        portal: Portal,
        _: u32,
        columns: Option<Vec<PostgresColumn>>,
        result_writer: ResultWriter<'_, S>,
    ) -> IoResult<()>
    where
        S: Write,
    {
        self.do_execute(&portal, result_writer, columns).unwrap();
        Ok(())
    }

    fn default_parameters(&mut self) -> DefaultServerParameters {
        let query = r#"
show server_version;
show server_encoding;
show client_encoding;
show application_name;
show default_transaction_read_only;
show is_superuser;
show session_authorization;
show DateStyle;
show IntervalStyle;
show TimeZone;
show integer_datetimes;
show standard_conforming_strings;
        "#;
        let result = self
            .executor
            .query(query)
            .expect("Error getting version from target")
            .unwrap();
        let (_, rows) = result.get_data();
        let parameters: Vec<String> = rows
            .map(|value| value.unwrap()[0].clone())
            .enumerate()
            .filter(|(i, _)| i % 3 == 0)
            .map(|(_, value)| match value {
                ColumnValue::String(value) => value.clone(),
                _ => unreachable!("We should always have a string here"),
            })
            .collect();
        DefaultServerParameters {
            server_version: parameters[0].clone(),
            server_encoding: parameters[1].clone(),
            client_encoding: parameters[2].clone(),
            application_name: parameters[3].clone(),
            default_transaction_read_only: parameters[4].clone(),
            in_hot_standby: "off".to_string(),
            is_superuser: parameters[5].clone(),
            session_authorization: parameters[6].clone(),
            date_style: parameters[7].clone(),
            interval_style: parameters[8].clone(),
            time_zone: parameters[9].clone(),
            integer_datetimes: parameters[10].clone(),
            standard_conforming_strings: parameters[11].clone(),
        }
    }
}

impl ToSqlValue for ColumnValue {
    fn as_bin_value(&self, _: &Type) -> Option<BytesMut> {
        todo!("Not implemented bin serialization")
    }
    fn as_str_value(&self, _: &Type) -> Option<BytesMut> {
        let mut buffer = BytesMut::new();
        match self {
            ColumnValue::String(value) => {
                buffer.extend_from_slice(value.as_bytes());
                Some(buffer)
            }
            _ => todo!("Handle more type representations"),
        }
    }
}
