//! Spanner's types as SpelaionDB's logical types, and a result's rows built from the values the
//! REST API sends (JSON, typed by the result's `rowType`).
//!
//! - `BOOL` is `Bool`; `INT64` is `Int` (sent as a string); `FLOAT64` and `FLOAT32` are `Float`
//!   (a number, or `"NaN"`, `"Infinity"`, `"-Infinity"`);
//! - `NUMERIC` is `Decimal`, kept as the server's text; `STRING` is `Text`; `BYTES` is `Bytes`
//!   (sent in base64);
//! - `DATE` is `Date`; `TIMESTAMP` is `TimestampTz`, to the microsecond (the server sends up to
//!   nanoseconds, in UTC);
//! - `JSON` is `Json`; `ARRAY<…>` is `Array`, written as JSON with its elements typed (an
//!   `INT64` past 2^53 stays a string, so no client rounds it);
//! - everything else (`STRUCT<…>`, `PROTO`, `ENUM`, `INTERVAL`, `UUID`…) is `Native`: a struct
//!   as a JSON object of its fields, the rest as the server's text.
//!
//! The streaming API splits a long value across messages (`chunkedValue`): [`Rows`] joins the
//! pieces as Spanner's API describes before a row is read.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder,
    RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
};
use serde_json::{Map, Value as Json};
use spelaion_driver_api::{ColumnMeta, DriverError, LogicalType, Result, ResultSchema};

/// One column's type, as the API describes it.
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    Scalar(String),
    Array(Box<Type>),
    Struct(Vec<(String, Type)>),
}

impl Type {
    pub fn from_json(json: &Json) -> Type {
        let code = json.get("code").and_then(Json::as_str).unwrap_or("");
        match code {
            "ARRAY" => Type::Array(Box::new(
                json.get("arrayElementType")
                    .map(Type::from_json)
                    .unwrap_or(Type::Scalar(String::new())),
            )),
            "STRUCT" => Type::Struct(
                json.get("structType")
                    .and_then(|s| s.get("fields"))
                    .and_then(Json::as_array)
                    .map(|fields| {
                        fields
                            .iter()
                            .map(|f| {
                                (
                                    f.get("name")
                                        .and_then(Json::as_str)
                                        .unwrap_or_default()
                                        .to_string(),
                                    f.get("type")
                                        .map(Type::from_json)
                                        .unwrap_or(Type::Scalar(String::new())),
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            ),
            other => Type::Scalar(other.to_string()),
        }
    }

    /// The type as GoogleSQL writes it: `INT64`, `ARRAY<STRING>`, `STRUCT<a INT64, b STRING>`.
    pub fn native(&self) -> String {
        match self {
            Type::Scalar(code) => code.clone(),
            Type::Array(element) => format!("ARRAY<{}>", element.native()),
            Type::Struct(fields) => format!(
                "STRUCT<{}>",
                fields
                    .iter()
                    .map(|(name, t)| if name.is_empty() {
                        t.native()
                    } else {
                        format!("{name} {}", t.native())
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }

    pub fn logical(&self) -> LogicalType {
        match self {
            Type::Array(_) => LogicalType::Array,
            Type::Struct(_) => LogicalType::Native,
            Type::Scalar(code) => match code.as_str() {
                "BOOL" => LogicalType::Bool,
                "INT64" => LogicalType::Int,
                "FLOAT64" | "FLOAT32" => LogicalType::Float,
                "NUMERIC" => LogicalType::Decimal,
                "STRING" => LogicalType::Text,
                "BYTES" => LogicalType::Bytes,
                "DATE" => LogicalType::Date,
                "TIMESTAMP" => LogicalType::TimestampTz,
                "JSON" => LogicalType::Json,
                _ => LogicalType::Native,
            },
        }
    }
}

pub fn column(name: String, t: &Type) -> ColumnMeta {
    ColumnMeta {
        name,
        logical: t.logical(),
        native_type: t.native(),
        native_id: None,
        origin: None,
    }
}

/// A value as plain JSON for an array's or a struct's cell: `INT64` as a number while it is
/// exact in a double, bytes in base64, structs as objects.
fn plain(t: &Type, value: &Json) -> Json {
    match (t, value) {
        (_, Json::Null) => Json::Null,
        (Type::Array(element), Json::Array(items)) => {
            Json::Array(items.iter().map(|v| plain(element, v)).collect())
        }
        (Type::Struct(fields), Json::Array(items)) => {
            let mut object = Map::new();
            for (i, ((name, ft), v)) in fields.iter().zip(items).enumerate() {
                let key = if name.is_empty() {
                    format!("_{i}")
                } else {
                    name.clone()
                };
                object.insert(key, plain(ft, v));
            }
            Json::Object(object)
        }
        (Type::Scalar(code), Json::String(s)) if code == "INT64" => match s.parse::<i64>() {
            Ok(n) if n.unsigned_abs() <= 1 << 53 => Json::from(n),
            _ => value.clone(),
        },
        (Type::Scalar(code), Json::String(s)) if code == "JSON" => {
            serde_json::from_str(s).unwrap_or_else(|_| value.clone())
        }
        _ => value.clone(),
    }
}

enum Builder {
    Bool(BooleanBuilder),
    Int(Int64Builder),
    Float(Float64Builder),
    Date(Date32Builder),
    Timestamp(TimestampMicrosecondBuilder),
    Bytes(BinaryBuilder),
    Text(StringBuilder),
}

/// The rows of one result, a batch at a time.
pub struct Batches {
    schema: Arc<ResultSchema>,
    types: Vec<Type>,
    builders: Vec<Builder>,
    rows: usize,
}

impl Batches {
    pub fn new(types: Vec<Type>, schema: Arc<ResultSchema>) -> Self {
        let builders = fresh(&schema);
        Self {
            schema,
            types,
            builders,
            rows: 0,
        }
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    /// One row as the API sends it.
    pub fn push(&mut self, row: &[Json]) -> Result<()> {
        if row.len() != self.builders.len() {
            return Err(DriverError::Other(format!(
                "a row of {} values in a result of {} columns",
                row.len(),
                self.builders.len()
            )));
        }
        for (((builder, value), column), t) in self
            .builders
            .iter_mut()
            .zip(row)
            .zip(&self.schema.columns)
            .zip(&self.types)
        {
            let bad = || DriverError::Decode {
                type_name: column.native_type.clone(),
                message: format!("{value} is not a value of this type"),
            };
            if value.is_null() {
                match builder {
                    Builder::Bool(b) => b.append_null(),
                    Builder::Int(b) => b.append_null(),
                    Builder::Float(b) => b.append_null(),
                    Builder::Date(b) => b.append_null(),
                    Builder::Timestamp(b) => b.append_null(),
                    Builder::Bytes(b) => b.append_null(),
                    Builder::Text(b) => b.append_null(),
                }
                continue;
            }
            match builder {
                Builder::Bool(b) => b.append_value(value.as_bool().ok_or_else(bad)?),
                Builder::Int(b) => b.append_value(
                    value
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(bad)?,
                ),
                Builder::Float(b) => b.append_value(match value {
                    Json::Number(n) => n.as_f64().ok_or_else(bad)?,
                    Json::String(s) => match s.as_str() {
                        "NaN" => f64::NAN,
                        "Infinity" => f64::INFINITY,
                        "-Infinity" => f64::NEG_INFINITY,
                        _ => return Err(bad()),
                    },
                    _ => return Err(bad()),
                }),
                Builder::Date(b) => b.append_value(value.as_str().and_then(days).ok_or_else(bad)?),
                Builder::Timestamp(b) => {
                    b.append_value(value.as_str().and_then(micros).ok_or_else(bad)?)
                }
                Builder::Bytes(b) => {
                    b.append_value(value.as_str().and_then(unbase64).ok_or_else(bad)?)
                }
                Builder::Text(b) => match (t, value) {
                    (Type::Array(_) | Type::Struct(_), _) => {
                        b.append_value(plain(t, value).to_string())
                    }
                    (_, Json::String(s)) => b.append_value(s),
                    (_, other) => b.append_value(other.to_string()),
                },
            }
        }
        self.rows += 1;
        Ok(())
    }

    /// The rows pushed since the last batch, as one batch; `None` when there are none.
    pub fn take(&mut self) -> Result<Option<RecordBatch>> {
        if self.rows == 0 {
            return Ok(None);
        }
        let builders = std::mem::replace(&mut self.builders, fresh(&self.schema));
        self.rows = 0;
        let arrays: Vec<ArrayRef> = builders
            .into_iter()
            .map(|builder| -> ArrayRef {
                match builder {
                    Builder::Bool(mut b) => Arc::new(b.finish()),
                    Builder::Int(mut b) => Arc::new(b.finish()),
                    Builder::Float(mut b) => Arc::new(b.finish()),
                    Builder::Date(mut b) => Arc::new(b.finish()),
                    Builder::Timestamp(mut b) => Arc::new(b.finish().with_timezone("UTC")),
                    Builder::Bytes(mut b) => Arc::new(b.finish()),
                    Builder::Text(mut b) => Arc::new(b.finish()),
                }
            })
            .collect();
        RecordBatch::try_new(self.schema.arrow.clone(), arrays)
            .map(Some)
            .map_err(|e| DriverError::Other(format!("building a batch: {e}")))
    }
}

fn fresh(schema: &ResultSchema) -> Vec<Builder> {
    schema
        .columns
        .iter()
        .map(|column| match column.logical {
            LogicalType::Bool => Builder::Bool(BooleanBuilder::new()),
            LogicalType::Int => Builder::Int(Int64Builder::new()),
            LogicalType::Float => Builder::Float(Float64Builder::new()),
            LogicalType::Date => Builder::Date(Date32Builder::new()),
            LogicalType::TimestampTz => Builder::Timestamp(TimestampMicrosecondBuilder::new()),
            LogicalType::Bytes => Builder::Bytes(BinaryBuilder::new()),
            LogicalType::Decimal
            | LogicalType::Text
            | LogicalType::Time
            | LogicalType::Timestamp
            | LogicalType::Uuid
            | LogicalType::Json
            | LogicalType::Interval
            | LogicalType::Array
            | LogicalType::Native => Builder::Text(StringBuilder::new()),
        })
        .collect()
}

/// The values of a streamed result, joined across messages and cut into rows.
pub struct Rows {
    columns: usize,
    pending: Vec<Json>,
    /// The last value of the previous message was cut: the next message's first value
    /// continues it.
    chunked: bool,
}

impl Rows {
    pub fn new(columns: usize) -> Self {
        Self {
            columns: columns.max(1),
            pending: Vec::new(),
            chunked: false,
        }
    }

    /// One message's `values` and `chunkedValue`; the rows it completes.
    pub fn feed(&mut self, values: Vec<Json>, chunked_value: bool) -> Result<Vec<Vec<Json>>> {
        let mut values = values.into_iter();
        if self.chunked
            && let Some(first) = values.next()
        {
            let last = self.pending.pop().ok_or_else(|| {
                DriverError::Other("a value continues one that never started".into())
            })?;
            self.pending.push(merge(last, first)?);
        }
        self.pending.extend(values);
        self.chunked = chunked_value;
        // A cut value is not a whole one yet: it stays pending.
        let whole = self.pending.len() - usize::from(self.chunked);
        let complete = whole / self.columns * self.columns;
        let rest = self.pending.split_off(complete);
        let done = std::mem::replace(&mut self.pending, rest);
        Ok(done.chunks(self.columns).map(<[Json]>::to_vec).collect())
    }

    /// Whether every value received is in a row.
    pub fn finished(&self) -> bool {
        self.pending.is_empty() && !self.chunked
    }
}

/// Joins two pieces of one value: strings are concatenated; for lists, the last element of
/// the first and the first of the second are joined when they are strings or lists, and the
/// rest follows.
fn merge(a: Json, b: Json) -> Result<Json> {
    match (a, b) {
        (Json::String(mut a), Json::String(b)) => {
            a.push_str(&b);
            Ok(Json::String(a))
        }
        (Json::Array(mut a), Json::Array(b)) => {
            let mut b = b.into_iter();
            match (a.pop(), b.next()) {
                (Some(last @ (Json::String(_) | Json::Array(_))), Some(first))
                    if matches!(first, Json::String(_) | Json::Array(_)) =>
                {
                    a.push(merge(last, first)?);
                }
                (last, first) => {
                    a.extend(last);
                    a.extend(first);
                }
            }
            a.extend(b);
            Ok(Json::Array(a))
        }
        (a, b) => Err(DriverError::Other(format!(
            "pieces of a value that cannot be joined: {a} and {b}"
        ))),
    }
}

/// Days since 1970-01-01 of `YYYY-MM-DD` (a year may have more digits or a sign).
fn days(text: &str) -> Option<i32> {
    let (sign, rest) = match text.strip_prefix('-') {
        Some(rest) => (-1, rest),
        None => (1, text),
    };
    let mut parts = rest.splitn(3, '-');
    let y: i64 = sign * parts.next()?.parse::<i64>().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let d: i64 = parts.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    i32::try_from(era * 146_097 + doe - 719_468).ok()
}

/// Microseconds since 1970-01-01 00:00 UTC of RFC 3339 as Spanner writes it
/// (`2024-02-29T10:20:30.123456789Z`); digits past the sixth are dropped.
fn micros(text: &str) -> Option<i64> {
    let text = text.strip_suffix('Z')?;
    let (date, time) = text.split_once('T')?;
    let (clock, fraction) = match time.split_once('.') {
        Some((clock, fraction)) => (clock, fraction),
        None => (time, ""),
    };
    let mut parts = clock.splitn(3, ':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let s: i64 = parts.next()?.parse().ok()?;
    if h > 23 || m > 59 || s > 60 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let digits: String = fraction
        .chars()
        .chain(std::iter::repeat('0'))
        .take(6)
        .collect();
    let fraction: i64 = digits.parse().ok()?;
    let seconds = i64::from(days(date)?) * 86_400 + h * 3_600 + m * 60 + s;
    seconds.checked_mul(1_000_000)?.checked_add(fraction)
}

const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 back to bytes; `None` for anything that is not.
fn unbase64(text: &str) -> Option<Vec<u8>> {
    let text = text.trim_end_matches('=');
    let mut out = Vec::with_capacity(text.len() * 3 / 4);
    let mut acc = 0u32;
    let mut bits = 0;
    for c in text.bytes() {
        let v = BASE64.iter().position(|&b| b == c)? as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(json: &str) -> Type {
        Type::from_json(&serde_json::from_str(json).unwrap())
    }

    #[test]
    fn types_map_to_logical_types_and_native_names() {
        for (json, logical, native) in [
            (r#"{"code":"BOOL"}"#, LogicalType::Bool, "BOOL"),
            (r#"{"code":"INT64"}"#, LogicalType::Int, "INT64"),
            (r#"{"code":"FLOAT32"}"#, LogicalType::Float, "FLOAT32"),
            (r#"{"code":"NUMERIC"}"#, LogicalType::Decimal, "NUMERIC"),
            (r#"{"code":"BYTES"}"#, LogicalType::Bytes, "BYTES"),
            (
                r#"{"code":"TIMESTAMP"}"#,
                LogicalType::TimestampTz,
                "TIMESTAMP",
            ),
            (r#"{"code":"JSON"}"#, LogicalType::Json, "JSON"),
            (
                r#"{"code":"ARRAY","arrayElementType":{"code":"STRING"}}"#,
                LogicalType::Array,
                "ARRAY<STRING>",
            ),
            (
                r#"{"code":"STRUCT","structType":{"fields":[{"name":"a","type":{"code":"INT64"}},{"name":"","type":{"code":"STRING"}}]}}"#,
                LogicalType::Native,
                "STRUCT<a INT64, STRING>",
            ),
            (r#"{"code":"INTERVAL"}"#, LogicalType::Native, "INTERVAL"),
        ] {
            let ty = t(json);
            assert_eq!(
                (ty.logical(), ty.native().as_str()),
                (logical, native),
                "{json}"
            );
        }
    }

    #[test]
    fn timestamps_and_bytes_read_as_spanner_writes_them() {
        assert_eq!(micros("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            micros("2024-02-29T10:20:30.123456789Z"),
            Some(1_709_202_030_123_456)
        );
        assert_eq!(micros("2024-02-29T10:20:30"), None);
        assert_eq!(unbase64("YWI=").as_deref(), Some(&b"ab"[..]));
        assert_eq!(unbase64("not base64!"), None);
    }

    #[test]
    fn chunked_values_are_joined_before_rows_are_read() {
        let mut rows = Rows::new(2);
        let v = |s: &str| -> Vec<Json> { serde_json::from_str(s).unwrap() };
        // One row and a half; its second value cut.
        assert_eq!(
            rows.feed(v(r#"["1", "ab", "2", "hel"]"#), true).unwrap(),
            vec![v(r#"["1", "ab"]"#)]
        );
        assert_eq!(
            rows.feed(v(r#"["lo", "3", ["x", "y"]]"#), false).unwrap(),
            vec![v(r#"["2", "hello"]"#), v(r#"["3", ["x", "y"]]"#)]
        );
        assert!(rows.finished());
        // A row that is not whole yet stays pending.
        assert!(rows.feed(v(r#"["4"]"#), false).unwrap().is_empty());
        assert!(!rows.finished());
        // A list cut inside its last string, and one cut between elements.
        assert_eq!(
            merge(serde_json::json!(["a", "b"]), serde_json::json!(["c", "d"])).unwrap(),
            serde_json::json!(["a", "bc", "d"])
        );
        assert_eq!(
            merge(serde_json::json!([1.5]), serde_json::json!([2.5])).unwrap(),
            serde_json::json!([1.5, 2.5])
        );
        assert!(merge(serde_json::json!(1), serde_json::json!(2)).is_err());
    }

    #[test]
    fn arrays_and_structs_are_cells_of_plain_json() {
        let schema_types = vec![
            t(r#"{"code":"ARRAY","arrayElementType":{"code":"INT64"}}"#),
            t(
                r#"{"code":"STRUCT","structType":{"fields":[{"name":"a","type":{"code":"INT64"}},{"name":"b","type":{"code":"STRING"}}]}}"#,
            ),
        ];
        let schema = Arc::new(ResultSchema::new(
            schema_types
                .iter()
                .enumerate()
                .map(|(i, ty)| column(format!("c{i}"), ty))
                .collect(),
        ));
        let mut batches = Batches::new(schema_types, schema);
        let row: Vec<Json> =
            serde_json::from_str(r#"[["1", "9223372036854775807"], ["2", "x"]]"#).unwrap();
        batches.push(&row).unwrap();
        let batch = batches.take().unwrap().unwrap();
        let text = |c: usize| {
            batch
                .column(c)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };
        assert_eq!(text(0), r#"[1,"9223372036854775807"]"#);
        assert_eq!(text(1), r#"{"a":2,"b":"x"}"#);
    }
}
