//! End-to-end `json` / `jsonb` tests through the Postgres wire protocol.
//!
//! A JSON column is UTF-8 text carrying the Arrow canonical `arrow.json`
//! extension and the `pg.json` marker. These tests pin the wire contract:
//! RowDescription reports OID 114/3802, text results are the JSON document,
//! binary `jsonb` results carry the one-byte version header, and binary
//! parameters are decoded (and `jsonb` normalized) before they reach
//! DataFusion.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_pg_catalog::setup_pg_catalog;
use datafusion_postgres::auth::AuthManager;
use datafusion_postgres::{ServerOptions, serve};
use postgres::NoTls;
use serde_json::json;
use tokio::sync::oneshot;

#[derive(Debug)]
struct RawJson<'a>(&'a str);

impl postgres_types::ToSql for RawJson<'_> {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if *ty == postgres_types::Type::JSONB {
            bytes::BufMut::put_u8(out, 1);
        }
        bytes::BufMut::put_slice(out, self.0.as_bytes());
        Ok(postgres_types::IsNull::No)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        matches!(
            *ty,
            postgres_types::Type::JSON | postgres_types::Type::JSONB
        )
    }

    postgres_types::to_sql_checked!();
}

fn json_field(name: &str, kind: &str) -> Field {
    Field::new(name, DataType::Utf8, true).with_metadata(HashMap::from([
        ("ARROW:extension:name".to_string(), "arrow.json".to_string()),
        ("pg.json".to_string(), kind.to_string()),
    ]))
}

fn register_docs(ctx: &SessionContext, table: &str, kind: &str) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        json_field("payload", kind),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        ],
    )
    .unwrap();
    ctx.register_batch(table, batch).unwrap();
}

fn service() -> SessionContext {
    let session_context = SessionContext::new();
    setup_pg_catalog(
        &session_context,
        "datafusion",
        Arc::new(AuthManager::default()),
    )
    .expect("failed to setup pg_catalog");
    register_docs(&session_context, "docs", "jsonb");
    register_docs(&session_context, "docs_json", "json");
    session_context
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn spawn_server(session_context: SessionContext) -> (u16, oneshot::Sender<()>) {
    let port = free_port();
    let (stop_tx, stop_rx) = oneshot::channel();

    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build server runtime");
        let ctx = Arc::new(session_context);
        let options = ServerOptions::new()
            .with_host("127.0.0.1".to_string())
            .with_port(port);
        runtime.block_on(async move {
            tokio::select! {
                _ = serve(ctx, &options) => {}
                _ = stop_rx => {}
            }
        });
    });

    (port, stop_tx)
}

fn connect(port: u16) -> postgres::Client {
    loop {
        let config = format!("host=127.0.0.1 port={port} user=postgres dbname=datafusion");
        match postgres::Client::connect(&config, NoTls) {
            Ok(client) => return client,
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
}

#[test]
fn jsonb_select_reports_oid_and_binary_version() {
    let (port, stop_tx) = spawn_server(service());
    let mut client = connect(port);

    let insert = client
        .prepare("INSERT INTO docs (id, payload) VALUES ($1, $2)")
        .expect("prepare insert should succeed");
    client
        .execute(&insert, &[&1i64, &json!({"b": 2, "a": 1})])
        .expect("jsonb parameter insert should succeed");

    let statement = client
        .prepare("SELECT payload FROM docs WHERE id = 1")
        .expect("prepare should succeed");
    assert_eq!(statement.columns()[0].type_().oid(), 3802);
    assert_eq!(statement.columns()[0].type_().name(), "jsonb");

    // The binary result must start with the jsonb format version byte; the
    // postgres crate's Value decoder proves it.
    let rows = client
        .query(&statement, &[])
        .expect("select should succeed");
    let value: serde_json::Value = rows[0].get(0);
    assert_eq!(value, json!({"a": 1, "b": 2}));

    // The text protocol returns the canonical JSON document.
    let messages = client
        .simple_query("SELECT payload FROM docs WHERE id = 1")
        .expect("simple query should succeed");
    let mut cells = Vec::new();
    for message in messages {
        if let postgres::SimpleQueryMessage::Row(row) = message {
            cells.push(row.get(0).map(str::to_owned));
        }
    }
    assert_eq!(cells, vec![Some(r#"{"a":1,"b":2}"#.to_string())]);

    let _ = stop_tx.send(());
}

#[test]
fn json_select_reports_json_oid_and_preserves_text() {
    let (port, stop_tx) = spawn_server(service());
    let mut client = connect(port);

    client
        .execute(
            "INSERT INTO docs_json (id, payload) VALUES (1, '{ \"b\": 2, \"a\": 1 }')",
            &[],
        )
        .expect("literal insert should succeed");

    let statement = client
        .prepare("SELECT payload FROM docs_json WHERE id = 1")
        .expect("prepare should succeed");
    assert_eq!(statement.columns()[0].type_().oid(), 114);
    assert_eq!(statement.columns()[0].type_().name(), "json");

    let rows = client
        .query(&statement, &[])
        .expect("select should succeed");
    let value: serde_json::Value = rows[0].get(0);
    assert_eq!(value, json!({"a": 1, "b": 2}));

    let _ = stop_tx.send(());
}

#[test]
fn prepared_insert_accepts_jsonb_parameter() {
    let (port, stop_tx) = spawn_server(service());
    let mut client = connect(port);

    let statement = client
        .prepare("INSERT INTO docs (id, payload) VALUES ($1, $2)")
        .expect("prepare insert should succeed");
    assert_eq!(statement.params().len(), 2);
    assert_eq!(statement.params()[1].oid(), 3802);
    assert_eq!(statement.params()[1].name(), "jsonb");

    client
        .execute(&statement, &[&7i64, &json!({"b": 2, "a": 1})])
        .expect("jsonb parameter insert should succeed");

    let rows = client
        .query("SELECT payload FROM docs WHERE id = 7", &[])
        .expect("select should succeed");
    let value: serde_json::Value = rows[0].get(0);
    assert_eq!(value, json!({"a": 1, "b": 2}));

    let _ = stop_tx.send(());
}

#[test]
fn jsonb_cast_reports_jsonb_but_text_cast_does_not() {
    let (port, stop_tx) = spawn_server(service());
    let mut client = connect(port);

    let statement = client
        .prepare("SELECT '{\"a\": 1}'::jsonb AS payload")
        .expect("jsonb cast should prepare");
    assert_eq!(statement.columns()[0].type_().oid(), 3802);

    let rows = client.query(&statement, &[]).expect("cast should run");
    let value: serde_json::Value = rows[0].get(0);
    assert_eq!(value, json!({"a": 1}));

    let statement = client
        .prepare("SELECT '{\"a\": 1}'::jsonb::text AS payload")
        .expect("jsonb::text should prepare");
    assert_eq!(
        statement.columns()[0].type_().oid(),
        25,
        "a type-only text cast must not keep the jsonb marker"
    );

    let _ = stop_tx.send(());
}

#[test]
fn invalid_jsonb_parameter_is_22p02() {
    let (port, stop_tx) = spawn_server(service());
    let mut client = connect(port);

    let statement = client
        .prepare_typed(
            "INSERT INTO docs (id, payload) VALUES ($1, $2)",
            &[postgres::types::Type::INT8, postgres::types::Type::JSONB],
        )
        .expect("prepare insert should succeed");

    let error = client
        .execute(&statement, &[&1i64, &RawJson("{not json")])
        .expect_err("invalid jsonb must fail");
    assert_eq!(
        error.code(),
        Some(&postgres::error::SqlState::INVALID_TEXT_REPRESENTATION)
    );

    let _ = stop_tx.send(());
}
