//! Tests against Google's Spanner emulator (1.5.28, its REST API).
//!
//! They need `SPELAION_TEST_SPANNER=host:port` and, for TLS, `SPELAION_TEST_SPANNER_TLS` (HTTPS
//! with a certificate for `spanner.spelaion.test` signed by `testing/keys/ca.crt`), and the
//! database `projects/spelaion/instances/test/databases/demo`, which `ci/services.sh` creates.
//! Locally: `ci/services.sh` and
//! `SPELAION_TEST_SPANNER=127.0.0.1:19020 SPELAION_TEST_SPANNER_TLS=127.0.0.1:19443 cargo test`.
//! Without the variables they are skipped with a notice, which does not count as tested.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions, LogicalType,
    ObjectKind, ResultSchema, Secret, TlsMode, TransactionState, Value,
};
use spelaion_driver_spanner::SpannerDriver;

const DATABASE: &str = "projects/spelaion/instances/test/databases/demo";

/// Spanner runs one schema change at a time: the tests that change the schema take turns.
static SCHEMA: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn addr(variable: &str) -> Option<(String, u16)> {
    let Ok(addr) = std::env::var(variable) else {
        eprintln!("{variable} not set: test skipped");
        return None;
    };
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    Some((host.to_string(), port.parse().expect("a port")))
}

fn params(host: &str, port: u16) -> ConnectParams {
    ConnectParams {
        host: host.into(),
        port,
        database: DATABASE.into(),
        tls: TlsMode::Disable,
        ..Default::default()
    }
}

async fn connect() -> Option<Box<dyn Connection>> {
    let (host, port) = addr("SPELAION_TEST_SPANNER")?;
    Some(
        SpannerDriver
            .connect(&params(&host, port))
            .await
            .expect("connects"),
    )
}

/// A name no other test run uses.
fn unique(prefix: &str) -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    format!(
        "{prefix}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    )
}

async fn run(conn: &dyn Connection, sql: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

async fn exec(conn: &dyn Connection, sql: &str) -> Option<u64> {
    let events = run(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}\n→ {e}"));
    events.iter().find_map(|e| match e {
        ExecEvent::Done { rows_affected } => *rows_affected,
        _ => None,
    })
}

async fn query(conn: &dyn Connection, sql: &str) -> (ResultSchema, Vec<Vec<Value>>) {
    let events = run(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}\n→ {e}"));
    let mut schema: Option<ResultSchema> = None;
    let mut rows = Vec::new();
    for event in events {
        match event {
            ExecEvent::Columns(s) => schema = Some((*s).clone()),
            ExecEvent::Batch(batch) => {
                let columns = &schema.as_ref().expect("columns come first").columns;
                for r in 0..batch.num_rows() {
                    rows.push(
                        (0..batch.num_columns())
                            .map(|c| {
                                Value::from_array(batch.column(c), r, columns[c].logical)
                                    .expect("a cell")
                            })
                            .collect(),
                    );
                }
            }
            ExecEvent::Notice(_) | ExecEvent::Done { .. } => {}
        }
    }
    (schema.unwrap_or_else(|| panic!("{sql}: no result")), rows)
}

/// Every kind of value: its logical type, its native type and the cell as the core shows it.
#[tokio::test]
async fn every_type_reads_as_the_server_writes_it() {
    let Some(conn) = connect().await else {
        return;
    };
    let (schema, rows) = query(
        conn.as_ref(),
        "SELECT TRUE AS b, 9223372036854775807 AS i, CAST(1.5 AS FLOAT32) AS f32, 0.1 AS f64, \
         CAST('NaN' AS FLOAT64) AS nan, CAST('inf' AS FLOAT64) AS inf, NUMERIC '12345.67' AS n, \
         'texto ñ' AS s, b'ab' AS by_, DATE '2024-02-29' AS d, \
         TIMESTAMP '2024-02-29 10:20:30.123456789+00' AS ts, JSON '{\"a\":[1,2]}' AS j, \
         [1, 2, 9007199254740993] AS arr, ARRAY(SELECT AS STRUCT 1 AS a, 'x' AS b) AS st, \
         CAST(NULL AS STRING) AS z",
    )
    .await;
    let cells: Vec<(String, LogicalType, String, String)> = schema
        .columns
        .iter()
        .zip(&rows[0])
        .map(|(c, v)| {
            (
                c.name.clone(),
                c.logical,
                c.native_type.clone(),
                v.to_string(),
            )
        })
        .collect();
    let expected: Vec<(&str, LogicalType, &str, &str)> = vec![
        ("b", LogicalType::Bool, "BOOL", "true"),
        ("i", LogicalType::Int, "INT64", "9223372036854775807"),
        ("f32", LogicalType::Float, "FLOAT32", "1.5"),
        ("f64", LogicalType::Float, "FLOAT64", "0.1"),
        ("nan", LogicalType::Float, "FLOAT64", "NaN"),
        ("inf", LogicalType::Float, "FLOAT64", "Infinity"),
        ("n", LogicalType::Decimal, "NUMERIC", "12345.67"),
        ("s", LogicalType::Text, "STRING", "texto ñ"),
        ("by_", LogicalType::Bytes, "BYTES", "\\x6162"),
        ("d", LogicalType::Date, "DATE", "2024-02-29"),
        (
            "ts",
            LogicalType::TimestampTz,
            "TIMESTAMP",
            "2024-02-29 10:20:30.123456+00",
        ),
        ("j", LogicalType::Json, "JSON", "{\"a\":[1,2]}"),
        (
            "arr",
            LogicalType::Array,
            "ARRAY<INT64>",
            "[1,2,\"9007199254740993\"]",
        ),
        (
            "st",
            LogicalType::Array,
            "ARRAY<STRUCT<a INT64, b STRING>>",
            "[{\"a\":1,\"b\":\"x\"}]",
        ),
        ("z", LogicalType::Text, "STRING", "NULL"),
    ];
    for (got, want) in cells.iter().zip(&expected) {
        assert_eq!(
            (got.0.as_str(), got.1, got.2.as_str(), got.3.as_str()),
            *want
        );
    }
    assert_eq!(cells.len(), expected.len());
}

/// DDL through the admin API, a data change committed on its own, and a transaction across
/// statements committed or rolled back.
#[tokio::test]
async fn schema_changes_data_changes_and_transactions() {
    let Some(conn) = connect().await else {
        return;
    };
    let _turn = SCHEMA.lock().await;
    let conn = conn.as_ref();
    let table = unique("Sdb352");
    // Written under comments, as an editor's script keeps them: Spanner's DDL takes none.
    exec(
        conn,
        &format!(
            "# the test's table\n-- one more\n/* and a block */\n\
             CREATE TABLE {table} (id INT64 NOT NULL, s STRING(MAX)) PRIMARY KEY (id)"
        ),
    )
    .await;
    assert_eq!(
        exec(
            conn,
            &format!("INSERT INTO {table} (id, s) VALUES (1, 'a'), (2, 'b')")
        )
        .await,
        Some(2)
    );
    assert_eq!(
        exec(conn, &format!("UPDATE {table} SET s = 'c' WHERE id = 2")).await,
        Some(1)
    );
    // A data change that fails leaves no transaction behind: a schema change still runs.
    assert!(
        run(
            conn,
            &format!("INSERT INTO {table} (id, s) VALUES (1, 'again')")
        )
        .await
        .is_err()
    );
    exec(conn, &format!("ALTER TABLE {table} ADD COLUMN extra INT64")).await;
    assert_eq!(
        conn.transaction_state().await.unwrap(),
        TransactionState::Idle
    );

    exec(conn, "BEGIN").await;
    assert_eq!(
        conn.transaction_state().await.unwrap(),
        TransactionState::Open
    );
    exec(
        conn,
        &format!("INSERT INTO {table} (id, s) VALUES (3, 'tx')"),
    )
    .await;
    // The transaction sees its own change.
    let (_, rows) = query(conn, &format!("SELECT count(*) FROM {table}")).await;
    assert_eq!(rows[0][0].to_string(), "3");
    // DDL waits for the transaction to end.
    assert!(run(conn, &format!("DROP TABLE {table}")).await.is_err());
    exec(conn, "ROLLBACK").await;
    assert_eq!(
        conn.transaction_state().await.unwrap(),
        TransactionState::Idle
    );
    let (_, rows) = query(conn, &format!("SELECT count(*) FROM {table}")).await;
    assert_eq!(rows[0][0].to_string(), "2");

    exec(conn, "BEGIN").await;
    exec(
        conn,
        &format!("INSERT INTO {table} (id, s) VALUES (4, 'kept')"),
    )
    .await;
    exec(conn, "COMMIT").await;
    let (_, rows) = query(conn, &format!("SELECT count(*) FROM {table}")).await;
    assert_eq!(rows[0][0].to_string(), "3");
    assert!(run(conn, "COMMIT").await.is_err());

    exec(conn, &format!("DROP TABLE {table}")).await;
}

#[tokio::test]
async fn a_large_result_comes_in_batches_of_the_size_asked() {
    let Some(conn) = connect().await else {
        return;
    };
    let events: Vec<ExecEvent> = conn
        .execute(
            // GENERATE_ARRAY stops at 16,000 elements: 25 × 1,000 rows.
            "SELECT a * 1000 + b FROM UNNEST(GENERATE_ARRAY(0, 24)) AS a, \
             UNNEST(GENERATE_ARRAY(1, 1000)) AS b",
            &ExecOptions {
                batch_size: 4000,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let sizes: Vec<usize> = events
        .iter()
        .filter_map(|e| match e {
            ExecEvent::Batch(b) => Some(b.num_rows()),
            _ => None,
        })
        .collect();
    assert_eq!(sizes.iter().sum::<usize>(), 25_000);
    assert!(sizes.iter().all(|&n| n <= 4000), "{sizes:?}");
}

/// A long value is sent in pieces by the streaming API: it arrives whole.
#[tokio::test]
async fn long_values_arrive_whole() {
    let Some(conn) = connect().await else {
        return;
    };
    let _turn = SCHEMA.lock().await;
    let conn = conn.as_ref();
    // A computed value stops at 1 MB: six stored values of 900 KB, read together, each with
    // an array of 9 values of 100 KB (an array, too, stops at 1 MB).
    let table = unique("Long");
    exec(
        conn,
        &format!("CREATE TABLE {table} (id INT64 NOT NULL, s STRING(MAX)) PRIMARY KEY (id)"),
    )
    .await;
    exec(
        conn,
        &format!(
            "INSERT INTO {table} (id, s) SELECT x, REPEAT('ab', 450000) \
             FROM UNNEST(GENERATE_ARRAY(1, 6)) AS x"
        ),
    )
    .await;
    let (_, rows) = query(
        conn,
        &format!(
            "SELECT s, ARRAY(SELECT REPEAT('x', 100000) FROM UNNEST(GENERATE_ARRAY(1, 9))) AS a \
             FROM {table} ORDER BY id"
        ),
    )
    .await;
    assert_eq!(rows.len(), 6);
    for row in &rows {
        match &row[0] {
            Value::Text(s) => {
                assert_eq!(s.len(), 900_000);
                assert!(
                    s.bytes()
                        .enumerate()
                        .all(|(i, b)| b == if i % 2 == 0 { b'a' } else { b'b' })
                );
            }
            other => panic!("{other:?}"),
        }
        let array: serde_json::Value = serde_json::from_str(&row[1].to_string()).unwrap();
        let items = array.as_array().unwrap();
        assert_eq!(items.len(), 9);
        assert!(items.iter().all(|s| s.as_str().unwrap().len() == 100_000));
    }
    exec(conn, &format!("DROP TABLE {table}")).await;
}

#[tokio::test]
async fn refusals_carry_the_servers_status_and_the_session_goes_on() {
    let Some(conn) = connect().await else {
        return;
    };
    match run(conn.as_ref(), "SELECT * FROM a_table_that_is_not_there").await {
        Err(DriverError::Database { code, message, .. }) => {
            assert_eq!(code.as_deref(), Some("INVALID_ARGUMENT"), "{message}");
            assert!(message.contains("a_table_that_is_not_there"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    let (_, rows) = query(conn.as_ref(), "SELECT 'still here'").await;
    assert_eq!(rows[0][0].to_string(), "still here");
}

#[tokio::test]
async fn the_explorer_reads_schemas_tables_views_and_keys() {
    let Some(conn) = connect().await else {
        return;
    };
    let _turn = SCHEMA.lock().await;
    let conn = conn.as_ref();
    let table = unique("Parent");
    let view = unique("V");
    exec(
        conn,
        &format!("CREATE TABLE {table} (id INT64 NOT NULL, name STRING(20)) PRIMARY KEY (id)"),
    )
    .await;
    exec(
        conn,
        &format!("CREATE VIEW {view} SQL SECURITY INVOKER AS SELECT t.id FROM {table} AS t"),
    )
    .await;

    let schemas = conn.list_schemas().await.unwrap();
    assert!(
        schemas.iter().any(|s| s.name == "default" && !s.system),
        "{schemas:?}"
    );
    assert!(
        schemas
            .iter()
            .any(|s| s.name == "INFORMATION_SCHEMA" && s.system)
    );
    let objects = conn.list_objects("default").await.unwrap();
    assert!(
        objects
            .iter()
            .any(|o| o.name == table && o.kind == ObjectKind::Table),
        "{objects:?}"
    );
    assert!(
        objects
            .iter()
            .any(|o| o.name == view && o.kind == ObjectKind::View)
    );
    let described = conn.describe_table("default", &table).await.unwrap();
    let columns: Vec<(&str, &str, bool)> = described
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.native_type.as_str(), c.nullable))
        .collect();
    assert_eq!(
        columns,
        [("id", "INT64", false), ("name", "STRING(20)", true)]
    );
    assert_eq!(described.primary_key, ["id"]);
    let all = conn.list_columns("default").await.unwrap();
    assert!(all.iter().any(|c| c.table == table && c.name == "name"));
    let created = conn.relation_definition("default", &table).await.unwrap();
    assert!(created.starts_with("CREATE TABLE"), "{created}");
    let defined = conn.relation_definition("default", &view).await.unwrap();
    assert!(defined.starts_with("CREATE VIEW"), "{defined}");
    assert_eq!(
        conn.current_database().await.unwrap().as_deref(),
        Some("demo")
    );
    assert!(conn.select_database("other").await.is_err());
    // A name bound as a parameter is a name, never SQL.
    assert!(conn.list_objects("x' OR '1'='1").await.unwrap().is_empty());

    exec(conn, &format!("DROP VIEW {view}")).await;
    exec(conn, &format!("DROP TABLE {table}")).await;
}

#[tokio::test]
async fn a_database_name_is_checked_before_it_is_sent() {
    let Some((host, port)) = addr("SPELAION_TEST_SPANNER") else {
        return;
    };
    for bad in [
        "demo",
        "spelaion/test/demo/../x",
        "projects/spelaion/instances/test/databases/demo?x",
    ] {
        let mut p = params(&host, port);
        p.database = bad.into();
        match SpannerDriver.connect(&p).await {
            Err(DriverError::Connection(message)) => {
                assert!(message.contains("projects/<project>"), "{bad}: {message}")
            }
            Err(other) => panic!("{bad}: {other:?}"),
            Ok(_) => panic!("{bad}: connected"),
        }
    }
    // The short form reaches the same database.
    let mut p = params(&host, port);
    p.database = "spelaion/test/demo".into();
    let conn = SpannerDriver.connect(&p).await.unwrap();
    let (_, rows) = query(conn.as_ref(), "SELECT 1").await;
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn a_database_that_is_not_there_is_a_connection_error() {
    let Some((host, port)) = addr("SPELAION_TEST_SPANNER") else {
        return;
    };
    let mut p = params(&host, port);
    p.database = "spelaion/test/nowhere".into();
    match SpannerDriver.connect(&p).await {
        Err(DriverError::Connection(_)) => {}
        Err(other) => panic!("{other:?}"),
        Ok(_) => panic!("connected to a database that is not there"),
    }
}

#[tokio::test]
async fn connecting_to_the_address_given_keeps_the_host_name() {
    let Some((_, port)) = addr("SPELAION_TEST_SPANNER") else {
        return;
    };
    let mut p = params("spanner.invalid", 1);
    p.connect_addr = Some(format!("127.0.0.1:{port}").parse().unwrap());
    let conn = SpannerDriver.connect(&p).await.unwrap();
    let (_, rows) = query(conn.as_ref(), "SELECT 1").await;
    assert_eq!(rows.len(), 1);
}

fn keys() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testing/keys")
}

/// TLS to 127.0.0.1, the address a tunnel gives, with the certificate checked against the
/// connection's host name; the token travels only in the header and never shows in an error.
#[tokio::test]
async fn tls_is_verified_against_the_host_name_and_the_token_is_never_shown() {
    let Some((host, port)) = addr("SPELAION_TEST_SPANNER_TLS") else {
        return;
    };
    let tls = |name: &str| ConnectParams {
        connect_addr: Some(format!("{host}:{port}").parse().unwrap()),
        tls: TlsMode::VerifyFull,
        ca_certificate: Some(keys().join("ca.crt")),
        // The emulator takes any token; Google checks it.
        password: Some(Secret::new("ya29.not-a-real-token-42")),
        ..params(name, port)
    };
    let conn = SpannerDriver
        .connect(&tls("spanner.spelaion.test"))
        .await
        .expect("verify-full against the name");
    let (_, rows) = query(conn.as_ref(), "SELECT 'over tls'").await;
    assert_eq!(rows[0][0].to_string(), "over tls");
    match run(conn.as_ref(), "SELECT * FROM not_there").await {
        Err(e) => assert!(!e.to_string().contains("ya29.not-a-real-token-42")),
        Ok(_) => panic!("a missing table was read"),
    }
    let refused = SpannerDriver
        .connect(&tls("another.spelaion.test"))
        .await
        .err()
        .expect("another name is refused");
    let text = refused.to_string();
    assert!(text.contains("TLS"), "{text}");
    assert!(!text.contains("ya29.not-a-real-token-42"), "{text}");
}
