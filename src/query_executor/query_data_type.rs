use super::{QueryExecutor, QueryResult};
use msql_srv::{Column, ColumnType};
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement, TableFactor};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use std::io::Result;
use std::sync::Mutex;

type TableName = String;
type TableAlias = String;
type ColumnName = String;

pub struct QueryDataType<T, D> {
    executor: T,
    dialect: D,
    data_type_info: Mutex<Option<Vec<(TableName, ColumnName, ColumnType)>>>,
}

impl<T, D> QueryDataType<T, D> {
    pub fn new(executor: T, dialect: D) -> Self {
        Self {
            executor,
            dialect,
            data_type_info: Mutex::new(None),
        }
    }

    fn load_data_type_hash<R>(&mut self) -> Result<()>
    where
        T: QueryExecutor<QueryResult = R>,
        R: QueryResult,
    {
        if self.data_type_info.lock().expect("Error locking").is_some() {
            return Ok(());
        }
        println!("Loading database structure");
        let mut type_map = Vec::new();
        let (_, rows) = self.executor.query("
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'mysql', 'pg_catalog')
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
        ")?.unwrap().get_data();
        for row in rows {
            let row = row?;
            type_map.push((
                row[1].clone(), // Unecessary clones here
                row[2].clone(),
                match row[3].as_str() {
                    "bigint" => ColumnType::MYSQL_TYPE_LONGLONG,
                    "varchar" => ColumnType::MYSQL_TYPE_VARCHAR,
                    "tinyint" => ColumnType::MYSQL_TYPE_TINY,
                    "datetime" => ColumnType::MYSQL_TYPE_DATETIME,
                    "int" => ColumnType::MYSQL_TYPE_LONG,
                    "mediumint" => ColumnType::MYSQL_TYPE_LONG,
                    "text" => ColumnType::MYSQL_TYPE_STRING,
                    "enum" => ColumnType::MYSQL_TYPE_ENUM,
                    "decimal" => ColumnType::MYSQL_TYPE_DECIMAL,
                    "date" => ColumnType::MYSQL_TYPE_DATE,
                    "binary" => ColumnType::MYSQL_TYPE_BLOB,
                    "double" => ColumnType::MYSQL_TYPE_DOUBLE,
                    "char" => ColumnType::MYSQL_TYPE_VARCHAR,
                    "tinytext" => ColumnType::MYSQL_TYPE_STRING,
                    "time" => ColumnType::MYSQL_TYPE_TIME,
                    "timestamp" => ColumnType::MYSQL_TYPE_TIMESTAMP,
                    "smallint" => ColumnType::MYSQL_TYPE_INT24,
                    "blob" => ColumnType::MYSQL_TYPE_BLOB,
                    "float" => ColumnType::MYSQL_TYPE_FLOAT,
                    "mediumblob" => ColumnType::MYSQL_TYPE_MEDIUM_BLOB,
                    "longtext" => ColumnType::MYSQL_TYPE_STRING,
                    "mediumtext" => ColumnType::MYSQL_TYPE_STRING,
                    "varbinary" => ColumnType::MYSQL_TYPE_BLOB,
                    "year" => ColumnType::MYSQL_TYPE_YEAR,
                    "bit" => ColumnType::MYSQL_TYPE_BIT,
                    any => {
                        println!("Type not mapped {}", any);
                        ColumnType::MYSQL_TYPE_STRING
                    }
                },
            ));
        }
        let mut lock = self.data_type_info.lock().expect("Error locking");
        *lock = Some(type_map);
        Ok(())
    }

    fn get_columns_types_from_ast<R>(&mut self, ast: Vec<Statement>) -> Result<Vec<ColumnType>>
    where
        T: QueryExecutor<QueryResult = R>,
        R: QueryResult,
    {
        self.load_data_type_hash()?;
        let lock = self.data_type_info.lock().expect("Error locking");
        let data_type_info = lock.as_ref().unwrap();
        let table_with_aliases = get_tables_with_aliases(&ast);
        let alias_to_column_and_type =
            get_alias_with_clomuns_and_column_type(table_with_aliases, data_type_info);
        Ok(get_columns_types(&ast, alias_to_column_and_type))
    }
}

impl<T, R, D> QueryExecutor for QueryDataType<T, D>
where
    T: QueryExecutor<QueryResult = R>,
    R: QueryResult,
    D: Dialect,
{
    type QueryResult = ResultWithCustomColumnTypes<R>;

    fn query(&mut self, query: &str) -> Result<Option<Self::QueryResult>> {
        if !query.to_lowercase().starts_with("select") {
            return match self.executor.query(query) {
                Ok(Some(result)) => Ok(Some(ResultWithCustomColumnTypes::new(result, vec![]))),
                Ok(None) => Ok(None),
                Err(error) => Err(error),
            };
        }
        let ast = Parser::parse_sql(&self.dialect, query);
        if ast.is_err() {
            println!("Failed to parse SQL. Result will not have types");
            return match self.executor.query(query) {
                Ok(Some(result)) => Ok(Some(ResultWithCustomColumnTypes::new(result, vec![]))),
                Ok(None) => Ok(None),
                Err(error) => Err(error),
            };
        }
        let columns_types = self.get_columns_types_from_ast(ast.unwrap())?;
        println!("Expected column types : {:?}", columns_types);
        let result = self.executor.query(query)?;
        match result {
            Some(result) => Ok(Some(ResultWithCustomColumnTypes::new(
                result,
                columns_types,
            ))),
            None => Ok(None),
        }
    }
}

fn get_tables_with_aliases(ast: &Vec<Statement>) -> Vec<(TableName, TableAlias)> {
    if ast.len() != 1 {
        todo!("We need to be able to handle multiple statements");
    }
    let mut result = Vec::new();
    match &ast[0] {
        Statement::Query(query) => match &query.body {
            SetExpr::Select(select) => {
                for table_with_join in &select.from {
                    result.push(process_table_factor(&table_with_join.relation));
                    for join in &table_with_join.joins {
                        result.push(process_table_factor(&join.relation));
                    }
                }
            }
            any => todo!("We can only parse selects - {:?}", any),
        },
        any => todo!("We can only parse querys - {:?}", any),
    }
    result
}

fn get_alias_with_clomuns_and_column_type(
    tables_with_aliases: Vec<(TableName, TableAlias)>,
    data_type_info: &Vec<(TableName, ColumnName, ColumnType)>,
) -> Vec<(TableAlias, ColumnName, ColumnType)> {
    tables_with_aliases
        .into_iter()
        .flat_map(|(table_name, alias_name)| {
            data_type_info
                .iter()
                .filter(move |(introspected_table_name, _, _)| {
                    *introspected_table_name == table_name
                })
                .map(move |(_, column_name, column_type)| {
                    (alias_name.clone(), column_name.clone(), column_type.clone())
                })
        })
        .collect()
}

fn process_table_factor(table_factor: &TableFactor) -> (String, String) {
    match table_factor {
        TableFactor::Table {
            name,
            alias,
            args: _,
            with_hints: _,
        } => {
            let table_name = match name.0.len() {
                1 => name.0[0].value.clone(),
                2 => format!("{}.{}", name.0[0].value, name.0[1].value),
                _ => panic!("Could not parse table, with or without namespace")
            };

            let alias = match alias {
                Some(table_alias) => table_alias.name.value.clone(),
                None => table_name.clone(),
            };
            (table_name, alias)
        }
        any => todo!("We can only parse simple tables - {:?}", any),
    }
}

fn get_columns_types(
    ast: &Vec<Statement>,
    alias_to_column_and_type: Vec<(String, String, ColumnType)>,
) -> Vec<ColumnType> {
    if ast.len() != 1 {
        todo!("We need to be able to handle multiple statements");
    }
    let mut result = Vec::new();
    match &ast[0] {
        Statement::Query(query) => {
            match &query.body {
                SetExpr::Select(select) => {
                    for projection in &select.projection {
                        match &projection {
                            SelectItem::UnnamedExpr(expr)
                            | SelectItem::ExprWithAlias { expr, alias: _ } => match &expr {
                                Expr::Identifier(ident) => result.push(find_type(
                                    &alias_to_column_and_type,
                                    &ident.value,
                                    None,
                                )),
                                Expr::CompoundIdentifier(idents) => {
                                    if idents.len() > 2 {
                                        todo!("We can only parse idents with table and column names - {:?}", idents);
                                    }
                                    result.push(find_type(
                                        &alias_to_column_and_type,
                                        &idents[1].value,
                                        Some(&idents[0].value),
                                    ))
                                }
                                _ => result.push(ColumnType::MYSQL_TYPE_STRING), // We should probably warn this cases
                            },
                            SelectItem::QualifiedWildcard(obj_name) => {
                                if obj_name.0.len() > 2 {
                                    todo!("We can only parse idents with table and column names - {:?}", obj_name.0);
                                }
                                alias_to_column_and_type
                                    .iter()
                                    .filter(|(table_alias, _, _)| {
                                        *table_alias == obj_name.0[0].value
                                    })
                                    .for_each(|(_, _, columnt_type)| {
                                        result.push(columnt_type.clone())
                                    })
                            }
                            SelectItem::Wildcard => alias_to_column_and_type
                                .iter()
                                .for_each(|(_, _, columnt_type)| result.push(columnt_type.clone())),
                        }
                    }
                }
                any => todo!("We can only parse selects - {:?}", any),
            }
        }
        any => todo!("We can only parse querys - {:?}", any),
    }
    result
}

fn find_type(
    alias_to_column_and_type: &Vec<(String, String, ColumnType)>,
    column_name: &str,
    table_name: Option<&str>,
) -> ColumnType {
    match table_name {
        Some(table_name) => {
            alias_to_column_and_type
                .iter()
                .find(|(s_table_name, s_column_name, _)| {
                    s_table_name == table_name && s_column_name == column_name
                })
        }
        None => alias_to_column_and_type
            .iter()
            .find(|(_, s_column_name, _)| s_column_name == column_name),
    }
    .map(|(_, _, column_type)| column_type.clone())
    .unwrap_or(ColumnType::MYSQL_TYPE_STRING) // We should probably inform when this happens
}

pub struct ResultWithCustomColumnTypes<T> {
    result: T,
    column_types: Vec<ColumnType>,
}

impl<T> ResultWithCustomColumnTypes<T> {
    fn new(result: T, column_types: Vec<ColumnType>) -> Self {
        Self {
            result,
            column_types,
        }
    }
}

impl<T> QueryResult for ResultWithCustomColumnTypes<T>
where
    T: QueryResult,
{
    fn get_data(
        self,
    ) -> (
        Result<Vec<Column>>,
        Box<dyn Iterator<Item = Result<Vec<String>>>>,
    ) {
        let (columns, rows) = self.result.get_data();
        if self.column_types.is_empty() {
            return (columns, rows);
        }
        (
            match columns {
                Ok(columns) => {
                    if columns.len() != self.column_types.len() {
                        panic!(
                            "Wrong number of columns in result. Expected {}, found {}",
                            self.column_types.len(),
                            columns.len()
                        )
                    }
                    Ok(columns
                        .into_iter()
                        .zip(self.column_types)
                        .map(|(mut column, column_type)| {
                            column.coltype = column_type;
                            column
                        })
                        .collect())
                }
                error => error,
            },
            rows,
        )
    }
}
