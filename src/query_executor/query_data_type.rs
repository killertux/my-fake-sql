use super::{ColumnValue, QueryExecutor, QueryResult, Row};
use anyhow::{bail, Result};
use chrono::{NaiveDate, NaiveDateTime};
use msql_srv::{Column, ColumnFlags, ColumnType};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, SelectItem, SetExpr, SetOperator, Statement, TableFactor,
};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

type Schema = String;
type TableName = String;
type TableAlias = String;
type ColumnName = String;

pub struct QueryDataType<T, D> {
    executor: T,
    dialect: D,
    data_type_info: Vec<(Schema, TableName, ColumnName, ColumnType)>,
    default_schema: Schema,
}

impl<T, D> QueryDataType<T, D> {
    pub fn new(executor: T, dialect: D) -> Self {
        Self {
            executor,
            dialect,
            data_type_info: Vec::new(),
            default_schema: String::new(),
        }
    }

    fn load_internals<R>(&mut self) -> Result<()>
    where
        T: QueryExecutor<QueryResult = R>,
        R: QueryResult,
    {
        if !self.data_type_info.is_empty() {
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
                to_string(&row[0]).into(),
                to_string(&row[1]).into(),
                to_string(&row[2]).into(),
                match to_string(&row[3]).as_ref() {
                    "bigint" => ColumnType::MYSQL_TYPE_LONGLONG,
                    "varchar" => ColumnType::MYSQL_TYPE_VAR_STRING,
                    "tinyint" => ColumnType::MYSQL_TYPE_TINY,
                    "datetime" => ColumnType::MYSQL_TYPE_DATETIME,
                    "int" => ColumnType::MYSQL_TYPE_LONG,
                    "mediumint" => ColumnType::MYSQL_TYPE_LONG,
                    "text" => ColumnType::MYSQL_TYPE_STRING,
                    "enum" => ColumnType::MYSQL_TYPE_ENUM,
                    "decimal" => ColumnType::MYSQL_TYPE_NEWDECIMAL,
                    "date" => ColumnType::MYSQL_TYPE_DATE,
                    "binary" => ColumnType::MYSQL_TYPE_BLOB,
                    "double" => ColumnType::MYSQL_TYPE_DOUBLE,
                    "char" => ColumnType::MYSQL_TYPE_STRING,
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
        self.data_type_info = type_map;
        if self.default_schema.is_empty() {
            println!("Loading current schema");
            let (_, mut rows) = self
                .executor
                .query("select database();")?
                .unwrap()
                .get_data();
            self.default_schema = to_string(&rows.next().unwrap()?[0]).clone();
        }

        Ok(())
    }

    fn get_columns_types_from_ast<R>(
        &mut self,
        ast: Vec<Statement>,
    ) -> Result<Vec<(ColumnName, ColumnType)>>
    where
        T: QueryExecutor<QueryResult = R>,
        R: QueryResult,
    {
        self.load_internals()?;
        let mut data_type_info = self.data_type_info.clone();
        let table_with_aliases =
            get_tables_with_aliases(&ast, &mut data_type_info, &self.default_schema)?;
        let alias_to_column_and_type =
            get_alias_with_clomuns_and_column_type(table_with_aliases, &mut data_type_info);
        get_columns_types(get_expr(&ast)?.unwrap(), alias_to_column_and_type)
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
        if query.to_lowercase().starts_with("use") {
            return match self.executor.query(query) {
                Ok(option) => {
                    self.default_schema = query.split_ascii_whitespace().skip(1).take(1).collect();
                    match option {
                        Some(result) => {
                            Ok(Some(ResultWithCustomColumnTypes::new(Some(result), vec![])))
                        }
                        None => Ok(None),
                    }
                }
                Err(error) => Err(error),
            };
        }
        if !query.to_lowercase().starts_with("select") {
            return match self.executor.query(query) {
                Ok(Some(result)) => {
                    Ok(Some(ResultWithCustomColumnTypes::new(Some(result), vec![])))
                }
                Ok(None) => Ok(None),
                Err(error) => Err(error),
            };
        }
        let ast = Parser::parse_sql(
            &self.dialect,
            &query.to_lowercase().replace("straight_join", "join"), // Our parser does not recognise straight_join
        );
        if ast.is_err() {
            println!("Failed to parse SQL. Result will not have types. {:?}", ast);
            return match self.executor.query(query) {
                Ok(Some(result)) => {
                    Ok(Some(ResultWithCustomColumnTypes::new(Some(result), vec![])))
                }
                Ok(None) => Ok(None),
                Err(error) => Err(error),
            };
        }
        let columns_types = self.get_columns_types_from_ast(ast.unwrap());
        if columns_types.is_err() {
            println!(
                "Failed to find proper types. Result will not have types. {:?}",
                columns_types
            );
            return match self.executor.query(query) {
                Ok(Some(result)) => {
                    Ok(Some(ResultWithCustomColumnTypes::new(Some(result), vec![])))
                }
                Ok(None) => Ok(None),
                Err(error) => Err(error),
            };
        }
        let columns_types = columns_types?;
        println!("Expected column types : {:?}", columns_types);
        let result = self.executor.query(query)?;
        Ok(Some(ResultWithCustomColumnTypes::new(
            result,
            columns_types,
        )))
    }
}

fn get_tables_with_aliases(
    ast: &Vec<Statement>,
    data_type_info: &mut Vec<(Schema, TableName, ColumnName, ColumnType)>,
    default_schema: &str,
) -> Result<Vec<(Schema, TableName, TableAlias)>> {
    if ast.len() != 1 {
        bail!("We need to be able to handle multiple statements");
    }
    match &ast[0] {
        Statement::Query(query) => {
            get_tables_with_aliases_from_set_expr(&query.body, data_type_info, default_schema)
        }
        any => bail!("We can only parse querys - {:?}", any),
    }
}

fn get_tables_with_aliases_from_set_expr(
    set_expr: &SetExpr,
    data_type_info: &mut Vec<(Schema, TableName, ColumnName, ColumnType)>,
    default_schema: &str,
) -> Result<Vec<(Schema, TableName, TableAlias)>> {
    let mut result = Vec::new();
    match set_expr {
        SetExpr::Select(select) => {
            for table_with_join in &select.from {
                result.push(process_table_factor(
                    &table_with_join.relation,
                    data_type_info,
                    default_schema,
                )?);
                for join in &table_with_join.joins {
                    result.push(process_table_factor(
                        &join.relation,
                        data_type_info,
                        default_schema,
                    )?);
                }
            }
        }
        SetExpr::SetOperation {
            op: SetOperator::Union,
            all: _,
            left,
            right: _,
        } => result.append(&mut get_tables_with_aliases_from_set_expr(
            &left,
            data_type_info,
            default_schema,
        )?),
        any => bail!("We can only parse selects - {:?}", any),
    }
    Ok(result)
}

fn get_alias_with_clomuns_and_column_type(
    tables_with_aliases: Vec<(Schema, TableName, TableAlias)>,
    data_type_info: &Vec<(Schema, TableName, ColumnName, ColumnType)>,
) -> Vec<(TableAlias, ColumnName, ColumnType)> {
    tables_with_aliases
        .into_iter()
        .flat_map(|(schema, table_name, alias_name)| {
            data_type_info
                .iter()
                .filter(
                    move |(introspected_schema, introspected_table_name, _, _)| {
                        *introspected_schema == schema && *introspected_table_name == table_name
                    },
                )
                .map(move |(_, _, column_name, column_type)| {
                    (alias_name.clone(), column_name.clone(), column_type.clone())
                })
        })
        .collect()
}

fn process_table_factor(
    table_factor: &TableFactor,
    data_type_info: &mut Vec<(Schema, TableName, ColumnName, ColumnType)>,
    default_schema: &str,
) -> Result<(Schema, TableName, TableAlias)> {
    match table_factor {
        TableFactor::Table {
            name,
            alias,
            args: _,
            with_hints: _,
        } => {
            let (schema, table_name) = match name.0.len() {
                1 => (default_schema.to_string(), name.0[0].value.clone()),
                2 => (name.0[0].value.clone(), name.0[1].value.clone()),
                _ => bail!("To many namespaces in the table name {:?}", name),
            };
            let alias = match alias {
                Some(table_alias) => table_alias.name.value.clone(),
                None => table_name.clone(),
            };
            Ok((schema, table_name, alias))
        }
        TableFactor::Derived {
            lateral: _,
            subquery,
            alias,
        } => {
            let mut result = Vec::new();
            let mut temp_data_type_info = data_type_info.clone();
            match &subquery.body {
                SetExpr::Select(select) => {
                    for table_with_join in &select.from {
                        result.push(process_table_factor(
                            &table_with_join.relation,
                            &mut temp_data_type_info,
                            default_schema,
                        )?);
                        for join in &table_with_join.joins {
                            result.push(process_table_factor(
                                &join.relation,
                                &mut temp_data_type_info,
                                default_schema,
                            )?);
                        }
                    }
                }
                any => bail!("We can only parse selects - {:?}", any),
            }
            let alias_to_column_and_type =
                get_alias_with_clomuns_and_column_type(result, &mut temp_data_type_info);

            let alias = alias.as_ref().unwrap().name.value.clone();
            for (column_name, column_type) in
                get_columns_types(&subquery.body, alias_to_column_and_type)?
            {
                data_type_info.push((alias.clone(), alias.clone(), column_name, column_type))
            }
            Ok((alias.clone(), alias.clone(), alias)) // We should probably create unique names here.
        }
        any => bail!("We can only parse simple tables - {:?}", any),
    }
}

fn get_expr(ast: &Vec<Statement>) -> Result<Option<&SetExpr>> {
    if ast.len() != 1 {
        bail!("We need to be able to handle multiple statements");
    }
    Ok(match &ast[0] {
        Statement::Query(query) => Some(&query.body),
        _ => None,
    })
}

fn get_columns_types(
    set_expr: &SetExpr,
    alias_to_column_and_type: Vec<(String, String, ColumnType)>,
) -> Result<Vec<(ColumnName, ColumnType)>> {
    let mut result = Vec::new();
    match set_expr {
        SetExpr::Select(select) => {
            for projection in &select.projection {
                match &projection {
                    SelectItem::UnnamedExpr(expr) => {
                        result.push(process_expr(expr, &alias_to_column_and_type)?);
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        result.push((
                            alias.value.to_string(),
                            process_expr(expr, &alias_to_column_and_type)?.1,
                        ));
                    }
                    SelectItem::QualifiedWildcard(obj_name) => {
                        if obj_name.0.len() > 2 {
                            bail!(
                                "We can only parse idents with table and column names - {:?}",
                                obj_name.0
                            );
                        }
                        alias_to_column_and_type
                            .iter()
                            .filter(|(table_alias, _, _)| *table_alias == obj_name.0[0].value)
                            .for_each(|(_, column_name, columnt_type)| {
                                result.push((column_name.clone(), columnt_type.clone()))
                            })
                    }
                    SelectItem::Wildcard => alias_to_column_and_type.iter().for_each(
                        |(_, column_name, columnt_type)| {
                            result.push((column_name.clone(), columnt_type.clone()))
                        },
                    ),
                }
            }
        }
        SetExpr::SetOperation {
            op: SetOperator::Union,
            all: _,
            left,
            right: _,
        } => result.append(&mut get_columns_types(&left, alias_to_column_and_type)?),
        any => bail!("We can only parse selects - {:?}", any),
    }
    Ok(result)
}

fn find_type(
    alias_to_column_and_type: &Vec<(String, String, ColumnType)>,
    column_name: &str,
    table_name: Option<&str>,
) -> (ColumnName, ColumnType) {
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
    .map(|(_, column_name, column_type)| (column_name.clone(), column_type.clone()))
    .unwrap_or((column_name.to_string(), ColumnType::MYSQL_TYPE_STRING)) // We should probably inform when this happens
}

pub struct ResultWithCustomColumnTypes<T> {
    result: Option<T>,
    column_types: Vec<(ColumnName, ColumnType)>,
}

impl<T> ResultWithCustomColumnTypes<T> {
    fn new(result: Option<T>, column_types: Vec<(ColumnName, ColumnType)>) -> Self {
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
    fn get_data(self) -> (Result<Vec<Column>>, Box<dyn Iterator<Item = Result<Row>>>) {
        match self.result {
            Some(result) => {
                let (columns, rows) = result.get_data();
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
                                .zip(&self.column_types)
                                .map(|(mut column, column_type)| {
                                    column.coltype = column_type.1;
                                    column
                                })
                                .collect())
                        }
                        error => error,
                    },
                    Box::new(rows.map(move |row| {
                        match row {
                            Err(error) => Err(error),
                            Ok(row) => Ok(row
                                .into_iter()
                                .zip(&self.column_types)
                                .map(|(column_value, column_type)| match column_value {
                                    ColumnValue::Null => ColumnValue::Null,
                                    ColumnValue::String(value) => match column_type.1 {
                                        ColumnType::MYSQL_TYPE_LONGLONG => {
                                            ColumnValue::I64(value.parse::<i64>().unwrap())
                                        }
                                        ColumnType::MYSQL_TYPE_LONG
                                        | ColumnType::MYSQL_TYPE_INT24 => {
                                            ColumnValue::I32(value.parse::<i32>().unwrap())
                                        }
                                        ColumnType::MYSQL_TYPE_SHORT
                                        | ColumnType::MYSQL_TYPE_YEAR => {
                                            ColumnValue::I16(value.parse::<i16>().unwrap())
                                        }
                                        ColumnType::MYSQL_TYPE_TINY => {
                                            ColumnValue::I8(value.parse::<i8>().unwrap())
                                        }
                                        ColumnType::MYSQL_TYPE_DOUBLE => {
                                            ColumnValue::Double(value.parse::<f64>().unwrap())
                                        }
                                        ColumnType::MYSQL_TYPE_FLOAT => {
                                            ColumnValue::Float(value.parse::<f32>().unwrap())
                                        }
                                        ColumnType::MYSQL_TYPE_TIMESTAMP
                                        | ColumnType::MYSQL_TYPE_DATETIME
                                        | ColumnType::MYSQL_TYPE_DATETIME2 => {
                                            ColumnValue::DateTime(
                                                NaiveDateTime::parse_from_str(
                                                    &value,
                                                    "%Y-%m-%d %H:%M:%S",
                                                )
                                                .unwrap(),
                                            )
                                        }
                                        ColumnType::MYSQL_TYPE_DATE => ColumnValue::Date(
                                            NaiveDate::parse_from_str(&value, "%Y-%m-%d").unwrap(),
                                        ),
                                        ColumnType::MYSQL_TYPE_DECIMAL
                                        | ColumnType::MYSQL_TYPE_NEWDECIMAL
                                        | ColumnType::MYSQL_TYPE_STRING
                                        | ColumnType::MYSQL_TYPE_VAR_STRING
                                        | _ => ColumnValue::String(value),
                                    },
                                    _ => panic!("We should only have string format here"),
                                })
                                .collect()),
                        }
                    })),
                )
            }
            None => (
                Ok(self
                    .column_types
                    .into_iter()
                    .map(|(column_name, column_type)| Column {
                        table: "none".to_string(),
                        column: column_name,
                        coltype: column_type,
                        colflags: ColumnFlags::empty(),
                    })
                    .collect()),
                Box::new(std::iter::empty()),
            ),
        }
    }
}

fn process_expr(
    expr: &Expr,
    alias_to_column_and_type: &Vec<(String, String, ColumnType)>,
) -> Result<(ColumnName, ColumnType)> {
    match &expr {
        Expr::Identifier(ident) => Ok(find_type(&alias_to_column_and_type, &ident.value, None)),
        Expr::CompoundIdentifier(idents) => {
            if idents.len() > 2 {
                bail!(
                    "We can only parse idents with table and column names - {:?}",
                    idents
                );
            }
            Ok(find_type(
                &alias_to_column_and_type,
                &idents[1].value,
                Some(&idents[0].value),
            ))
        }
        Expr::Function(function) => {
            let name = function.name.0[0].value.clone();
            match name.as_str() {
                "if" => {
                    let first = match &function.args[1] {
                        FunctionArg::Unnamed(arg) => match arg {
                            FunctionArgExpr::Expr(expr) => {
                                process_expr(expr, alias_to_column_and_type)?
                            }
                            _ => bail!("Cant handle wildcards here"),
                        },
                        _ => bail!("Cant handle names function arg"),
                    };
                    let second = match &function.args[1] {
                        FunctionArg::Unnamed(arg) => match arg {
                            FunctionArgExpr::Expr(expr) => {
                                process_expr(expr, alias_to_column_and_type)?
                            }
                            _ => bail!("Cant handle wildcards here"),
                        },
                        _ => bail!("Cant handle names function arg"),
                    };
                    if first.1 == ColumnType::MYSQL_TYPE_STRING {
                        Ok(second)
                    } else {
                        Ok(first)
                    }
                }
                _ => Ok((name, ColumnType::MYSQL_TYPE_STRING)), // We should probably warn this cases
            }
        }
        _ => Ok(("unknown".to_string(), ColumnType::MYSQL_TYPE_STRING)), // We should probably warn this cases
    }
}

fn to_string(value: &ColumnValue) -> &String {
    match value {
        ColumnValue::String(string) => string,
        _ => panic!("We are expecting bytes here"),
    }
}
