//! What the database holds, from `INFORMATION_SCHEMA`, with every name bound as a parameter.
//!
//! Spanner's default schema has no name (`''`): the explorer shows it as `default`, and the
//! core writes its tables' names unqualified. A table's or a view's definition is its
//! statement in the database's DDL. Primary keys are reported; foreign keys are not yet.

use serde_json::Value as Json;
use spelaion_driver_api::{
    ColumnInfo, DriverError, ObjectInfo, ObjectKind, Result, SchemaColumn, SchemaInfo, TableInfo,
};

use crate::SpannerConnection;

/// The explorer's name of the default schema.
pub const DEFAULT_SCHEMA: &str = "default";

fn text(row: &[Json], i: usize) -> String {
    match row.get(i) {
        Some(Json::String(s)) => s.clone(),
        Some(Json::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

/// The schema's name as `INFORMATION_SCHEMA` writes it.
fn stored(schema: &str) -> &str {
    if schema == DEFAULT_SCHEMA { "" } else { schema }
}

fn shown(schema: String) -> String {
    if schema.is_empty() {
        DEFAULT_SCHEMA.to_string()
    } else {
        schema
    }
}

fn kind(table_type: &str) -> ObjectKind {
    if table_type == "VIEW" {
        ObjectKind::View
    } else {
        ObjectKind::Table
    }
}

pub async fn list_schemas(conn: &SpannerConnection) -> Result<Vec<SchemaInfo>> {
    let rows = conn
        .rows(
            "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
            &[],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| {
            let name = text(row, 0);
            SchemaInfo {
                system: matches!(
                    name.as_str(),
                    "INFORMATION_SCHEMA" | "SPANNER_SYS" | "SPANNER_STATS"
                ),
                name: shown(name),
            }
        })
        .collect())
}

pub async fn list_objects(conn: &SpannerConnection, schema: &str) -> Result<Vec<ObjectInfo>> {
    let rows = conn
        .rows(
            "SELECT table_name, table_type FROM information_schema.tables \
             WHERE table_schema = @schema ORDER BY table_name",
            &[("schema", stored(schema))],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| ObjectInfo {
            schema: schema.to_string(),
            name: text(row, 0),
            kind: kind(&text(row, 1)),
            estimated_rows: None,
            comment: None,
        })
        .collect())
}

pub async fn describe_table(
    conn: &SpannerConnection,
    schema: &str,
    name: &str,
) -> Result<TableInfo> {
    let bound = [("schema", stored(schema)), ("name", name)];
    let objects = conn
        .rows(
            "SELECT table_type FROM information_schema.tables \
             WHERE table_schema = @schema AND table_name = @name",
            &bound,
        )
        .await?;
    let Some(object) = objects.first() else {
        return Err(DriverError::Other(format!(
            "there is no table {schema}.{name}"
        )));
    };
    let columns = conn
        .rows(
            "SELECT column_name, spanner_type, is_nullable, column_default \
             FROM information_schema.columns \
             WHERE table_schema = @schema AND table_name = @name ORDER BY ordinal_position",
            &bound,
        )
        .await?;
    let key = conn
        .rows(
            "SELECT column_name FROM information_schema.index_columns \
             WHERE table_schema = @schema AND table_name = @name AND index_name = 'PRIMARY_KEY' \
             ORDER BY ordinal_position",
            &bound,
        )
        .await?;
    Ok(TableInfo {
        object: ObjectInfo {
            schema: schema.to_string(),
            name: name.to_string(),
            kind: kind(&text(object, 0)),
            estimated_rows: None,
            comment: None,
        },
        columns: columns
            .iter()
            .map(|row| ColumnInfo {
                name: text(row, 0),
                native_type: text(row, 1),
                nullable: text(row, 2) == "YES",
                default: Some(text(row, 3)).filter(|s| !s.is_empty()),
                comment: None,
            })
            .collect(),
        primary_key: key.iter().map(|row| text(row, 0)).collect(),
        indexes: Vec::new(),
        foreign_keys: Vec::new(),
        constraints: Vec::new(),
    })
}

pub async fn list_columns(conn: &SpannerConnection, schema: &str) -> Result<Vec<SchemaColumn>> {
    let rows = conn
        .rows(
            "SELECT table_name, column_name, spanner_type FROM information_schema.columns \
             WHERE table_schema = @schema ORDER BY table_name, ordinal_position",
            &[("schema", stored(schema))],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| SchemaColumn {
            table: text(row, 0),
            name: text(row, 1),
            native_type: text(row, 2),
        })
        .collect())
}

/// The `CREATE TABLE` or `CREATE VIEW` statement of the database's DDL that names `name`.
pub async fn relation_definition(
    conn: &SpannerConnection,
    schema: &str,
    name: &str,
) -> Result<String> {
    let qualified = match stored(schema) {
        "" => name.to_string(),
        s => format!("{s}.{name}"),
    };
    let names_it = |statement: &str| {
        let mut words = statement.split_whitespace();
        let create = words
            .next()
            .is_some_and(|w| w.eq_ignore_ascii_case("CREATE"));
        let rest: Vec<&str> = words.collect();
        let object = rest
            .iter()
            .find(|w| {
                !matches!(
                    w.to_ascii_uppercase().as_str(),
                    "OR" | "REPLACE"
                        | "TABLE"
                        | "VIEW"
                        | "SQL"
                        | "SECURITY"
                        | "INVOKER"
                        | "DEFINER"
                )
            })
            .map(|w| {
                w.trim_matches('`')
                    .split('(')
                    .next()
                    .unwrap_or("")
                    .trim_matches('`')
            });
        create && object == Some(qualified.as_str())
    };
    conn.ddl_statements()
        .await?
        .into_iter()
        .find(|s| names_it(s))
        .map(|sql| spelaion_driver_api::terminated(&sql))
        .ok_or_else(|| DriverError::Other(format!("no definition for {qualified}")))
}
