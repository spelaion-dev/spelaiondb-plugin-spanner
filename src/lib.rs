//! Google Cloud Spanner for SpelaionDB, served as a plugin (SpelaionDB's ADR-041 and ADR-042).
//!
//! Spanner's REST API, GoogleSQL dialect. A connection is one Spanner session, created in
//! `connect`; its statements go one at a time through an actor:
//!
//! - a query runs as a single-use, strongly consistent read; a data change outside a
//!   transaction runs in a read-write transaction of its own, committed right after (rolled
//!   back if it fails);
//! - `BEGIN` (or `START TRANSACTION`), `COMMIT` and `ROLLBACK` are the client's in Spanner: they
//!   begin, commit and roll back a read-write transaction, and every statement in between runs
//!   in it, in order;
//! - DDL (`CREATE`, `ALTER`, `DROP`, `RENAME`, `GRANT`, `REVOKE`, `ANALYZE`) goes to the
//!   database's schema through the admin API, and its operation is followed until it is done;
//! - results are read from `executeStreamingSql`, whose cut values are joined (`types::Rows`).
//!
//! A session the server no longer has (it expires after an hour without use) loses the
//! connection (ADR-028): nothing is sent on it again and nothing is re-run.
//!
//! What it does not do yet is declared, and SpelaionDB hides it: editing rows, importing rows,
//! binding values, plans, cancelling a statement. Read-only mode and agents are not offered:
//! Spanner's read-only transactions are enforced by the server, but making them SpelaionDB's
//! barrier is a decision of its own (ADR-020 there).

pub mod http;
mod introspect;
pub mod tls;
pub mod types;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use hyper::{Method, StatusCode};
use serde_json::{Value as Json, json};
use spelaion_driver_api::{
    CancelHandle, Capabilities, ConnectParams, Connection, Driver, DriverError, EventStream,
    ExecEvent, ExecOptions, KeepAlive, ObjectInfo, ReferencingKey, Result, ResultSchema,
    SchemaColumn, SchemaInfo, TableInfo, TransactionState,
};
use tokio::sync::{mpsc, oneshot};

use crate::http::{Client, Endpoint};
use crate::tls::ClientIdentity;
use crate::types::{Batches, Rows, Type};

/// How long a schema change is followed before it is given up on (it goes on in the server).
const DDL_WAIT: Duration = Duration::from_secs(600);

/// The engine.
#[derive(Debug, Clone, Copy, Default)]
pub struct SpannerDriver;

/// `projects/P/instances/I/databases/D`, from that or from `P/I/D`; `None` for anything else.
/// Each id is Spanner's: lower-case letters, digits, `-` and `_` (and `.`, `:` in a project id
/// with a domain), so nothing else ever reaches a request's path.
pub fn database_path(text: &str) -> Option<String> {
    let parts: Vec<&str> = text.trim().trim_matches('/').split('/').collect();
    let (project, instance, database) = match parts.as_slice() {
        ["projects", p, "instances", i, "databases", d] => (*p, *i, *d),
        [p, i, d] => (*p, *i, *d),
        _ => return None,
    };
    let id = |s: &str, extra: &[u8]| {
        !s.is_empty()
            && s.bytes().all(|b| {
                b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || extra.contains(&b)
            })
    };
    (id(project, b".:") && id(instance, b"") && id(database, b"_"))
        .then(|| format!("projects/{project}/instances/{instance}/databases/{database}"))
}

/// A name the server gave (a session, an operation) is followed only under the database and
/// only when it holds nothing a path could be bent with.
fn checked_name(name: &str, under: &str) -> Option<String> {
    (name.starts_with(under)
        && name[under.len()..]
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'-' | b'_' | b'.'))
        && !name.contains(".."))
    .then(|| name.to_string())
}

#[async_trait]
impl Driver for SpannerDriver {
    fn engine(&self) -> &str {
        "spanner"
    }

    async fn connect(&self, params: &ConnectParams) -> Result<Box<dyn Connection>> {
        let identity = match (&params.client_certificate, &params.client_key) {
            (Some(cert), Some(key)) => Some(ClientIdentity { cert, key }),
            (None, None) => None,
            _ => {
                return Err(DriverError::Connection(
                    "a client certificate needs its key, and a key its certificate".into(),
                ));
            }
        };
        let database = database_path(&params.database).ok_or_else(|| {
            DriverError::Connection(
                "the database is projects/<project>/instances/<instance>/databases/<database> \
                 (or <project>/<instance>/<database>)"
                    .into(),
            )
        })?;
        let tls = tls::configure(params.tls, params.ca_certificate.as_deref(), identity)?;
        let endpoint = Arc::new(Endpoint::new(
            params.host.clone(),
            params.port,
            params.connect_addr,
            tls,
            params.connect_timeout(),
            params.password.clone(),
        ));
        let mut client = Client::new(endpoint);
        let created = tokio::time::timeout(
            params.connect_timeout(),
            call(
                &mut client,
                Method::POST,
                &format!("/v1/{database}/sessions"),
                Some(json!({}).to_string()),
            ),
        )
        .await
        .map_err(|_| {
            DriverError::Connection(format!(
                "the server did not answer within {} s",
                params.connect_timeout().as_secs()
            ))
        })?;
        let created = match created {
            Ok(json) => json,
            Err(DriverError::Database { message, .. }) => {
                return Err(DriverError::Connection(message));
            }
            Err(e) => return Err(e),
        };
        let session = created
            .get("name")
            .and_then(Json::as_str)
            .and_then(|name| checked_name(name, &format!("{database}/sessions/")))
            .ok_or_else(|| DriverError::Connection("the server gave no session".into()))?;
        let closed = Arc::new(AtomicBool::new(false));
        let (requests, receiver) = mpsc::channel(16);
        tokio::spawn(actor(
            Session {
                client,
                database,
                session,
                transaction: None,
            },
            receiver,
            closed.clone(),
        ));
        Ok(Box::new(SpannerConnection {
            capabilities: Capabilities {
                transactions: true,
                cancel: false,
                explain: false,
                editable_results: false,
                schemas: true,
                insert_rows: false,
                bound_params: false,
            },
            requests,
            closed,
        }))
    }
}

/// `name` as a quoted identifier (GoogleSQL backticks, with `\` and `` ` `` escaped).
pub fn quote(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

/// One request and its JSON answer; the server's error as a driver error.
async fn call(
    client: &mut Client,
    method: Method,
    path: &str,
    body: Option<String>,
) -> Result<Json> {
    let response = client.send(method, path, &[], body).await?;
    // One JSON value; a stream is a JSON array (Google) or one value after another, each
    // `{"result": …}` (the emulator's gateway): both are read as an array.
    let values: std::result::Result<Vec<Json>, _> =
        serde_json::Deserializer::from_slice(&response.body)
            .into_iter::<Json>()
            .collect();
    let json: Option<Json> = match values {
        Ok(mut values) if values.len() == 1 => values.pop(),
        Ok(values) if !values.is_empty() => Some(Json::Array(values)),
        _ => None,
    };
    if response.status.is_success() {
        return json.ok_or_else(|| DriverError::Other("the server's answer is not JSON".into()));
    }
    let scrub = |text: &str| client.endpoint().scrub(text);
    Err(error_of(
        response.status,
        json.as_ref(),
        &response.body,
        &scrub,
    ))
}

/// What a failed answer says: Google's `{"error": {…}}` or the emulator's `{"code", "message"}`.
fn error_of(
    status: StatusCode,
    json: Option<&Json>,
    body: &[u8],
    scrub: &dyn Fn(&str) -> String,
) -> DriverError {
    let error = json.map(|j| j.get("error").unwrap_or(j));
    let message = error
        .and_then(|e| e.get("message"))
        .and_then(Json::as_str)
        .map(|m| scrub(m.trim()))
        .unwrap_or_else(|| scrub(String::from_utf8_lossy(body).trim()));
    let code = error.and_then(|e| {
        e.get("status")
            .and_then(Json::as_str)
            .map(str::to_string)
            .or_else(|| e.get("code").map(|c| grpc_name(c.as_i64().unwrap_or(-1))))
    });
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => DriverError::Connection(format!(
            "the server refused the access token ({status}): {message}"
        )),
        _ if message.contains("Session not found") || message.contains("session not found") => {
            DriverError::Connection(format!("the session is gone: {message}"))
        }
        _ if status.is_client_error() || error.is_some() => DriverError::Database {
            code,
            message,
            detail: None,
            hint: None,
            position: None,
        },
        _ => DriverError::Connection(format!("the server answered {status}: {message}")),
    }
}

/// gRPC's status codes by name, as Google's API writes them in `status`.
fn grpc_name(code: i64) -> String {
    match code {
        1 => "CANCELLED",
        3 => "INVALID_ARGUMENT",
        4 => "DEADLINE_EXCEEDED",
        5 => "NOT_FOUND",
        6 => "ALREADY_EXISTS",
        7 => "PERMISSION_DENIED",
        9 => "FAILED_PRECONDITION",
        10 => "ABORTED",
        11 => "OUT_OF_RANGE",
        12 => "UNIMPLEMENTED",
        13 => "INTERNAL",
        14 => "UNAVAILABLE",
        16 => "UNAUTHENTICATED",
        _ => "UNKNOWN",
    }
    .to_string()
}

/// What a statement is, by its first word: where it is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Begin,
    Commit,
    Rollback,
    Ddl,
    Query,
    Change,
}

fn kind(sql: &str) -> Kind {
    let mut rest = sql.trim_start();
    // Comments and hints before the first word.
    loop {
        if let Some(after) = rest.strip_prefix("--").or_else(|| rest.strip_prefix('#')) {
            rest = after.split_once('\n').map_or("", |(_, r)| r).trim_start();
        } else if let Some(after) = rest.strip_prefix("/*") {
            rest = after.split_once("*/").map_or("", |(_, r)| r).trim_start();
        } else {
            break;
        }
    }
    if rest.starts_with('(') || rest.starts_with('@') {
        return Kind::Query;
    }
    let word: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect::<String>()
        .to_ascii_lowercase();
    match word.as_str() {
        "begin" | "start" => Kind::Begin,
        "commit" => Kind::Commit,
        "rollback" => Kind::Rollback,
        "create" | "alter" | "drop" | "rename" | "grant" | "revoke" | "analyze" => Kind::Ddl,
        "insert" | "update" | "delete" => Kind::Change,
        _ => Kind::Query,
    }
}

/// The open transaction: its id, and the next statement's sequence number.
struct Transaction {
    id: String,
    seqno: u64,
    aborted: bool,
}

struct Session {
    client: Client,
    database: String,
    session: String,
    transaction: Option<Transaction>,
}

/// A whole streamed result: its column types, its rows and, for a data change, its count.
struct Answer {
    columns: Vec<(String, Type)>,
    rows: Vec<Vec<Json>>,
    affected: Option<u64>,
}

impl Session {
    async fn call(&mut self, method: Method, path: &str, body: Option<Json>) -> Result<Json> {
        call(&mut self.client, method, path, body.map(|b| b.to_string())).await
    }

    /// Runs one statement through `executeStreamingSql` and joins its pieces.
    async fn stream(
        &mut self,
        sql: &str,
        transaction: Option<Json>,
        params: Option<Json>,
    ) -> Result<Answer> {
        let mut body = json!({ "sql": sql });
        if let Some(t) = transaction {
            body["transaction"] = t;
        }
        if let Some(tx) = self.transaction.as_mut() {
            body["seqno"] = json!(tx.seqno.to_string());
            tx.seqno += 1;
        }
        if let Some(params) = params {
            body["params"] = params["params"].clone();
            body["paramTypes"] = params["paramTypes"].clone();
        }
        let path = format!("/v1/{}:executeStreamingSql", self.session);
        let parts = self.call(Method::POST, &path, Some(body)).await?;
        let parts = match parts {
            Json::Array(parts) => parts,
            one @ Json::Object(_) => vec![one],
            _ => {
                return Err(DriverError::Other(
                    "the server's answer is not a result".into(),
                ));
            }
        };
        let mut columns: Vec<(String, Type)> = Vec::new();
        let mut rows = Vec::new();
        let mut reader: Option<Rows> = None;
        let mut affected = None;
        for part in parts {
            // The emulator's gateway wraps each message: `{"result": …}` or `{"error": …}`.
            let part = match part {
                Json::Object(mut object) if object.contains_key("result") => {
                    object.remove("result").unwrap_or(Json::Null)
                }
                other => other,
            };
            if let Some(error) = part.get("error") {
                let scrub = |t: &str| self.client.endpoint().scrub(t);
                return Err(error_of(
                    StatusCode::BAD_REQUEST,
                    Some(&json!({"error": error})),
                    b"",
                    &scrub,
                ));
            }
            if let Some(fields) = part
                .get("metadata")
                .and_then(|m| m.get("rowType"))
                .and_then(|r| r.get("fields"))
                .and_then(Json::as_array)
            {
                {
                    columns = fields
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
                        .collect();
                }
            }
            let values = match part.get("values") {
                Some(Json::Array(values)) => values.clone(),
                _ => Vec::new(),
            };
            let chunked = part
                .get("chunkedValue")
                .and_then(Json::as_bool)
                .unwrap_or(false);
            if !values.is_empty() || chunked {
                let r = reader.get_or_insert_with(|| Rows::new(columns.len()));
                rows.extend(r.feed(values, chunked)?);
            }
            if let Some(count) = part
                .get("stats")
                .and_then(|s| s.get("rowCountExact"))
                .and_then(|c| {
                    c.as_str()
                        .and_then(|c| c.parse().ok())
                        .or_else(|| c.as_u64())
                })
            {
                affected = Some(count);
            }
        }
        if reader.as_ref().is_some_and(|r| !r.finished()) {
            return Err(DriverError::Other(
                "the result ended in the middle of a row".into(),
            ));
        }
        Ok(Answer {
            columns,
            rows,
            affected,
        })
    }

    /// One statement, sent where its kind says.
    async fn run(&mut self, sql: &str, params: Option<Json>) -> Result<Answer> {
        let none = || Answer {
            columns: Vec::new(),
            rows: Vec::new(),
            affected: None,
        };
        match kind(sql) {
            Kind::Begin => {
                if self.transaction.is_some() {
                    return Err(DriverError::Other("a transaction is already open".into()));
                }
                let path = format!("/v1/{}:beginTransaction", self.session);
                let answer = self
                    .call(
                        Method::POST,
                        &path,
                        Some(json!({"options": {"readWrite": {}}})),
                    )
                    .await?;
                let id = answer
                    .get("id")
                    .and_then(Json::as_str)
                    .ok_or_else(|| DriverError::Other("the server began no transaction".into()))?;
                self.transaction = Some(Transaction {
                    id: id.to_string(),
                    seqno: 1,
                    aborted: false,
                });
                Ok(none())
            }
            Kind::Commit | Kind::Rollback => {
                let Some(tx) = self.transaction.take() else {
                    return Err(DriverError::Other("no transaction is open".into()));
                };
                let (verb, body) = if kind(sql) == Kind::Commit {
                    ("commit", json!({ "transactionId": tx.id }))
                } else {
                    ("rollback", json!({ "transactionId": tx.id }))
                };
                let path = format!("/v1/{}:{verb}", self.session);
                self.call(Method::POST, &path, Some(body)).await?;
                Ok(none())
            }
            Kind::Ddl => {
                if self.transaction.is_some() {
                    return Err(DriverError::Other(
                        "Spanner changes a schema outside transactions: commit or roll back first"
                            .into(),
                    ));
                }
                self.ddl(sql).await?;
                Ok(none())
            }
            Kind::Query | Kind::Change => {
                if let Some(tx) = &self.transaction {
                    let id = tx.id.clone();
                    let result = self.stream(sql, Some(json!({ "id": id })), params).await;
                    if let Err(DriverError::Database {
                        code: Some(code), ..
                    }) = &result
                        && code == "ABORTED"
                        && let Some(tx) = self.transaction.as_mut()
                    {
                        tx.aborted = true;
                    }
                    return result;
                }
                if kind(sql) == Kind::Query {
                    return self.stream(sql, None, params).await;
                }
                // A data change on its own: in a read-write transaction of its own, committed
                // when it succeeds and rolled back when it fails. It is begun first, not with
                // the statement: a failed statement's answer carries no transaction to end, and
                // one left open would hold every schema change back.
                let path = format!("/v1/{}:beginTransaction", self.session);
                let begun = self
                    .call(
                        Method::POST,
                        &path,
                        Some(json!({"options": {"readWrite": {}}})),
                    )
                    .await?;
                let id = begun
                    .get("id")
                    .and_then(Json::as_str)
                    .ok_or_else(|| DriverError::Other("the server began no transaction".into()))?
                    .to_string();
                self.transaction = Some(Transaction {
                    id: id.clone(),
                    seqno: 1,
                    aborted: false,
                });
                let result = self.stream(sql, Some(json!({ "id": id })), params).await;
                self.transaction = None;
                let end = |verb: &str| format!("/v1/{}:{verb}", self.session);
                match result {
                    Ok(answer) => {
                        self.call(
                            Method::POST,
                            &end("commit"),
                            Some(json!({ "transactionId": id })),
                        )
                        .await?;
                        Ok(answer)
                    }
                    Err(e) => {
                        let _ = self
                            .call(
                                Method::POST,
                                &end("rollback"),
                                Some(json!({ "transactionId": id })),
                            )
                            .await;
                        Err(e)
                    }
                }
            }
        }
    }

    /// A schema change through the admin API, followed until its operation is done.
    async fn ddl(&mut self, sql: &str) -> Result<()> {
        let path = format!("/v1/{}/ddl", self.database);
        let mut operation = self
            .call(Method::PATCH, &path, Some(json!({ "statements": [sql] })))
            .await?;
        let name = operation
            .get("name")
            .and_then(Json::as_str)
            .and_then(|n| checked_name(n, &format!("{}/operations/", self.database)));
        let started = tokio::time::Instant::now();
        loop {
            if operation.get("done").and_then(Json::as_bool) == Some(true) {
                if let Some(error) = operation.get("error") {
                    let scrub = |t: &str| self.client.endpoint().scrub(t);
                    return Err(error_of(
                        StatusCode::BAD_REQUEST,
                        Some(&json!({ "error": error })),
                        b"",
                        &scrub,
                    ));
                }
                return Ok(());
            }
            let Some(name) = &name else {
                return Err(DriverError::Other(
                    "the server gave no operation to follow".into(),
                ));
            };
            if started.elapsed() > DDL_WAIT {
                return Err(DriverError::Other(format!(
                    "the schema change goes on in the server after {} s: {name}",
                    DDL_WAIT.as_secs()
                )));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
            operation = self.call(Method::GET, &format!("/v1/{name}"), None).await?;
        }
    }

    /// The database's schema, as `CREATE` statements.
    async fn ddl_statements(&mut self) -> Result<Vec<String>> {
        let path = format!("/v1/{}/ddl", self.database);
        let answer = self.call(Method::GET, &path, None).await?;
        Ok(answer
            .get("statements")
            .and_then(Json::as_array)
            .map(|s| {
                s.iter()
                    .filter_map(Json::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default())
    }

    fn transaction_state(&self) -> TransactionState {
        match &self.transaction {
            None => TransactionState::Idle,
            Some(tx) if tx.aborted => TransactionState::Failed,
            Some(_) => TransactionState::Open,
        }
    }

    fn database_id(&self) -> String {
        self.database
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_string()
    }
}

enum Request {
    Execute {
        sql: String,
        batch_size: usize,
        events: mpsc::Sender<Result<ExecEvent>>,
        started: oneshot::Sender<Result<()>>,
    },
    Rows {
        sql: String,
        params: Option<Json>,
        reply: oneshot::Sender<Result<Vec<Vec<Json>>>>,
    },
    Ddl {
        reply: oneshot::Sender<Result<Vec<String>>>,
    },
    State {
        reply: oneshot::Sender<(TransactionState, String)>,
    },
}

async fn actor(
    mut session: Session,
    mut requests: mpsc::Receiver<Request>,
    closed: Arc<AtomicBool>,
) {
    let lost = |e: &DriverError| {
        if let DriverError::Connection(_) = e {
            closed.store(true, Ordering::SeqCst);
        }
    };
    while let Some(request) = requests.recv().await {
        match request {
            Request::Execute {
                sql,
                batch_size,
                events,
                started,
            } => match session.run(&sql, None).await {
                Err(e) => {
                    lost(&e);
                    let _ = started.send(Err(e));
                }
                Ok(answer) => {
                    let _ = started.send(Ok(()));
                    send_answer(answer, batch_size, &events).await;
                }
            },
            Request::Rows { sql, params, reply } => {
                let result = session.run(&sql, params).await.map(|a| a.rows);
                if let Err(e) = &result {
                    lost(e);
                }
                let _ = reply.send(result);
            }
            Request::Ddl { reply } => {
                let result = session.ddl_statements().await;
                if let Err(e) = &result {
                    lost(e);
                }
                let _ = reply.send(result);
            }
            Request::State { reply } => {
                let _ = reply.send((session.transaction_state(), session.database_id()));
            }
        }
    }
    // The connection is gone: the session is let go (a transaction left open is rolled back).
    if !closed.load(Ordering::SeqCst) {
        let path = format!("/v1/{}", session.session);
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            session.call(Method::DELETE, &path, None),
        )
        .await;
    }
}

async fn send_answer(answer: Answer, batch_size: usize, events: &mpsc::Sender<Result<ExecEvent>>) {
    // A data change, a schema change or transaction control: no result.
    if answer.columns.is_empty() || (answer.affected.is_some() && answer.rows.is_empty()) {
        let _ = events
            .send(Ok(ExecEvent::Done {
                rows_affected: answer.affected,
            }))
            .await;
        return;
    }
    let types: Vec<Type> = answer.columns.iter().map(|(_, t)| t.clone()).collect();
    let schema = Arc::new(ResultSchema::new(
        answer
            .columns
            .iter()
            .map(|(name, t)| types::column(name.clone(), t))
            .collect(),
    ));
    if events
        .send(Ok(ExecEvent::Columns(schema.clone())))
        .await
        .is_err()
    {
        return;
    }
    let mut batches = Batches::new(types, schema);
    let batch_size = batch_size.max(1);
    for row in &answer.rows {
        let full = match batches.push(row) {
            Ok(()) if batches.rows() >= batch_size => batches.take(),
            Ok(()) => Ok(None),
            Err(e) => Err(e),
        };
        match full {
            Ok(Some(batch)) => {
                if events.send(Ok(ExecEvent::Batch(batch))).await.is_err() {
                    return;
                }
            }
            Ok(None) => {}
            Err(e) => {
                let _ = events.send(Err(e)).await;
                return;
            }
        }
    }
    match batches.take() {
        Ok(Some(batch)) => {
            if events.send(Ok(ExecEvent::Batch(batch))).await.is_err() {
                return;
            }
        }
        Ok(None) => {}
        Err(e) => {
            let _ = events.send(Err(e)).await;
            return;
        }
    }
    let _ = events
        .send(Ok(ExecEvent::Done {
            rows_affected: answer.affected,
        }))
        .await;
}

/// The REST API has no way to stop a statement once it is sent.
struct NoCancel;

#[async_trait]
impl CancelHandle for NoCancel {
    async fn cancel(&self) -> Result<()> {
        Err(DriverError::Other(
            "a Spanner statement cannot be cancelled once it is sent".into(),
        ))
    }
}

struct SpannerConnection {
    capabilities: Capabilities,
    requests: mpsc::Sender<Request>,
    closed: Arc<AtomicBool>,
}

impl SpannerConnection {
    fn gone() -> DriverError {
        DriverError::Connection("the connection is closed".into())
    }

    /// One of the driver's own queries, with its values bound (`@name`), as JSON rows.
    pub(crate) async fn rows(&self, sql: &str, params: &[(&str, &str)]) -> Result<Vec<Vec<Json>>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Self::gone());
        }
        let params = (!params.is_empty()).then(|| {
            let mut values = serde_json::Map::new();
            let mut types = serde_json::Map::new();
            for (name, value) in params {
                values.insert((*name).to_string(), json!(value));
                types.insert((*name).to_string(), json!({"code": "STRING"}));
            }
            json!({ "params": values, "paramTypes": types })
        });
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::Rows {
                sql: sql.to_string(),
                params,
                reply,
            })
            .await
            .map_err(|_| Self::gone())?;
        answer.await.map_err(|_| Self::gone())?
    }

    pub(crate) async fn ddl_statements(&self) -> Result<Vec<String>> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::Ddl { reply })
            .await
            .map_err(|_| Self::gone())?;
        answer.await.map_err(|_| Self::gone())?
    }

    async fn state(&self) -> Result<(TransactionState, String)> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::State { reply })
            .await
            .map_err(|_| Self::gone())?;
        answer.await.map_err(|_| Self::gone())
    }
}

#[async_trait]
impl Connection for SpannerConnection {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn execute(&self, sql: &str, opts: &ExecOptions) -> Result<EventStream> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Self::gone());
        }
        let (events, receiver) = mpsc::channel(4);
        let (started, answer) = oneshot::channel();
        self.requests
            .send(Request::Execute {
                sql: sql.to_string(),
                batch_size: opts.batch_size,
                events,
                started,
            })
            .await
            .map_err(|_| Self::gone())?;
        answer.await.map_err(|_| Self::gone())??;
        let mut receiver = receiver;
        Ok(futures_util::stream::poll_fn(move |cx| receiver.poll_recv(cx)).boxed())
    }

    /// The REST API has no way to stop a statement that is running.
    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(NoCancel)
    }

    async fn server_version(&self) -> Result<String> {
        Ok("Cloud Spanner (GoogleSQL)".into())
    }

    async fn keep_alive(&self) -> Result<KeepAlive> {
        if self.transaction_state().await? != TransactionState::Idle {
            return Ok(KeepAlive::InTransaction);
        }
        self.rows("SELECT 1", &[]).await?;
        Ok(KeepAlive::Alive)
    }

    /// Known without asking: the transaction is the client's.
    async fn transaction_state(&self) -> Result<TransactionState> {
        Ok(self.state().await?.0)
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    async fn current_database(&self) -> Result<Option<String>> {
        Ok(Some(self.state().await?.1))
    }

    async fn list_databases(&self) -> Result<Vec<String>> {
        Ok(vec![self.state().await?.1])
    }

    async fn select_database(&self, database: &str) -> Result<()> {
        if database == self.state().await?.1 {
            Ok(())
        } else {
            Err(DriverError::Other(
                "a Spanner connection is to one database: open another connection for another"
                    .into(),
            ))
        }
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        introspect::list_schemas(self).await
    }

    async fn list_objects(&self, schema: &str) -> Result<Vec<ObjectInfo>> {
        introspect::list_objects(self, schema).await
    }

    async fn describe_table(&self, schema: &str, name: &str) -> Result<TableInfo> {
        introspect::describe_table(self, schema, name).await
    }

    async fn list_columns(&self, schema: &str) -> Result<Vec<SchemaColumn>> {
        introspect::list_columns(self, schema).await
    }

    async fn list_referencing_keys(
        &self,
        _schema: &str,
        _name: &str,
    ) -> Result<Vec<ReferencingKey>> {
        Ok(Vec::new())
    }

    async fn relation_definition(&self, schema: &str, name: &str) -> Result<String> {
        introspect::relation_definition(self, schema, name).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_paths_are_checked_before_they_reach_a_path() {
        assert_eq!(
            database_path("projects/p-1/instances/i/databases/d_1").as_deref(),
            Some("projects/p-1/instances/i/databases/d_1")
        );
        assert_eq!(
            database_path("p-1/i/d").as_deref(),
            Some("projects/p-1/instances/i/databases/d")
        );
        assert_eq!(
            database_path("example.com:p/i/d").as_deref(),
            Some("projects/example.com:p/instances/i/databases/d")
        );
        for bad in [
            "", "p/i", "p/i/d/x", "P/i/d", "p/i/../d", "p/i/d?x", "p/i/d#", "p/../i/d",
        ] {
            assert_eq!(database_path(bad), None, "{bad}");
        }
    }

    #[test]
    fn names_the_server_gives_stay_under_the_database() {
        let db = "projects/p/instances/i/databases/d";
        let under = format!("{db}/sessions/");
        assert!(checked_name(&format!("{db}/sessions/abc-1_2"), &under).is_some());
        for bad in [
            format!("{db}/sessions/../../x"),
            format!("{db}/sessions/a?b"),
            "projects/other/instances/i/databases/d/sessions/1".to_string(),
        ] {
            assert!(checked_name(&bad, &under).is_none(), "{bad}");
        }
    }

    #[test]
    fn statements_go_where_their_first_word_says() {
        for (sql, k) in [
            ("BEGIN", Kind::Begin),
            ("start transaction", Kind::Begin),
            ("COMMIT", Kind::Commit),
            ("rollback", Kind::Rollback),
            ("CREATE TABLE t (id INT64) PRIMARY KEY (id)", Kind::Ddl),
            ("  -- a comment\n DROP TABLE t", Kind::Ddl),
            ("/* x */ ALTER TABLE t ADD COLUMN c STRING(MAX)", Kind::Ddl),
            ("INSERT INTO t (id) VALUES (1)", Kind::Change),
            ("update t set c = 1 where true", Kind::Change),
            ("DELETE FROM t WHERE true", Kind::Change),
            ("SELECT 1", Kind::Query),
            ("WITH a AS (SELECT 1) SELECT * FROM a", Kind::Query),
            ("(SELECT 1)", Kind::Query),
            ("@{USE_ADDITIONAL_PARALLELISM=TRUE} SELECT 1", Kind::Query),
        ] {
            assert_eq!(kind(sql), k, "{sql}");
        }
    }

    #[test]
    fn errors_read_from_both_shapes() {
        let none = |t: &str| t.to_string();
        let emulator: Json =
            serde_json::from_str(r#"{"code":3,"message":"Syntax error: x"}"#).unwrap();
        match error_of(StatusCode::BAD_REQUEST, Some(&emulator), b"", &none) {
            DriverError::Database { code, message, .. } => {
                assert_eq!(code.as_deref(), Some("INVALID_ARGUMENT"));
                assert_eq!(message, "Syntax error: x");
            }
            other => panic!("{other:?}"),
        }
        let google: Json = serde_json::from_str(
            r#"{"error":{"code":404,"message":"Table not found: t","status":"NOT_FOUND"}}"#,
        )
        .unwrap();
        match error_of(StatusCode::NOT_FOUND, Some(&google), b"", &none) {
            DriverError::Database { code, .. } => assert_eq!(code.as_deref(), Some("NOT_FOUND")),
            other => panic!("{other:?}"),
        }
        let gone: Json =
            serde_json::from_str(r#"{"code":5,"message":"Session not found: x"}"#).unwrap();
        assert!(matches!(
            error_of(StatusCode::NOT_FOUND, Some(&gone), b"", &none),
            DriverError::Connection(_)
        ));
        let token = |t: &str| t.replace("ya29.secret", "***");
        let refused: Json =
            serde_json::from_str(r#"{"error":{"message":"bad token ya29.secret"}}"#).unwrap();
        let text = error_of(StatusCode::UNAUTHORIZED, Some(&refused), b"", &token).to_string();
        assert!(!text.contains("ya29.secret"), "{text}");
    }
}
