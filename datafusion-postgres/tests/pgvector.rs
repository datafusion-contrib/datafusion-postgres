//! End-to-end pgvector tests through the wire protocol.
//!
//! The in-process handler tests drive SQL through the server's query handlers
//! with a mock client; the network tests connect a real PostgreSQL client
//! ([`postgres`], the synchronous rust-postgres crate) to a live server and
//! exercise DDL, INSERT and queries using the official [`pgvector`] client
//! types -- proving binary/text compatibility against the real pgvector wire
//! format without re-implementing any pgvector encoding here.
#![cfg(feature = "pgvector")]

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{FixedSizeListArray, Float32Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_pg_catalog::setup_pg_catalog;
use pgwire::api::query::SimpleQueryHandler;
use postgres::NoTls;
use tokio::sync::oneshot;

use datafusion_postgres::DfSessionService;
use datafusion_postgres::auth::AuthManager;
use datafusion_postgres::testing::MockClient;
use datafusion_postgres::{ServerOptions, serve};

/// pgvector `vector` type OID, matching `arrow_pg::datatypes::PG_VECTOR_TYPE_OID`.
const VECTOR_OID: u32 = 16385;

/// Register `items(id bigint, embedding vector(3))` as an empty table whose
/// `embedding` field carries the `pg.vector` metadata.
fn register_items(ctx: &SessionContext) {
    let embedding = Field::new(
        "embedding",
        DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Float32, true)), 3),
        false,
    )
    .with_metadata(
        [("pg.vector".to_string(), "vector".to_string())]
            .into_iter()
            .collect(),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        embedding,
    ]));

    let values = Float32Array::from(Vec::<f32>::new());
    let embedding_array = FixedSizeListArray::try_new(
        Arc::new(Field::new_list_field(DataType::Float32, true)),
        3,
        Arc::new(values),
        None,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(embedding_array),
        ],
    )
    .unwrap();
    ctx.register_batch("items", batch).unwrap();
}

async fn service() -> (SessionContext, DfSessionService) {
    let session_context = SessionContext::new();
    setup_pg_catalog(
        &session_context,
        "datafusion",
        Arc::new(AuthManager::default()),
    )
    .expect("failed to setup pg_catalog");
    register_items(&session_context);

    let service = DfSessionService::new(Arc::new(session_context.clone()));
    (session_context, service)
}

#[tokio::test]
async fn insert_vector_literal_over_wire_protocol() {
    let (ctx, service) = service().await;
    let mut client = MockClient::new();

    // A plain INSERT of a pgvector bracket literal, exactly as psql would send
    // it. The handler must rewrite the value so DataFusion can store it.
    let responses = SimpleQueryHandler::do_query(
        &service,
        &mut client,
        "INSERT INTO items (id, embedding) VALUES (1, '[1,2,3]'), (2, '[4,5,6]')",
    )
    .await
    .expect("INSERT of vector literals should succeed");

    assert_eq!(responses.len(), 1);
    assert!(
        matches!(responses[0], pgwire::api::results::Response::Execution(_)),
        "INSERT must return an execution response"
    );

    // Rows actually landed in the table.
    let batches = ctx
        .sql("SELECT count(*) FROM items")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2, "two rows must have been inserted");

    // Distance operators keep working against the stored vectors.
    let responses = SimpleQueryHandler::do_query(
        &service,
        &mut client,
        "SELECT id FROM items ORDER BY embedding <-> '[1,2,3]' LIMIT 1",
    )
    .await
    .expect("distance query should succeed");
    assert_eq!(responses.len(), 1);
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Run a server over `session_context` on its own thread. The server is shut
/// down by sending on the returned oneshot channel.
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

/// Connect a synchronous rust-postgres client, retrying while the server's
/// listener comes up.
fn connect(port: u16) -> postgres::Client {
    loop {
        let config = format!("host=127.0.0.1 port={port} user=postgres dbname=datafusion");
        match postgres::Client::connect(&config, NoTls) {
            Ok(client) => return client,
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
}

/// A real PostgreSQL client driving the full pgwire protocol -- startup over
/// TCP, INSERT of pgvector literals, prepared vector parameters and vector
/// distance queries.
#[test]
fn real_pgwire_client_inserts_and_queries_vectors() {
    let session_context = SessionContext::new();
    setup_pg_catalog(
        &session_context,
        "datafusion",
        Arc::new(AuthManager::default()),
    )
    .expect("failed to setup pg_catalog");
    register_items(&session_context);

    let (port, stop_tx) = spawn_server(session_context);
    let mut client = connect(port);

    // INSERT of pgvector bracket literals (extended protocol).
    let inserted = client
        .execute(
            "INSERT INTO items (id, embedding) VALUES (1, '[1,2,3]'), (2, '[4,5,6]')",
            &[],
        )
        .expect("client INSERT of vector literals should succeed");
    assert_eq!(inserted, 2, "both rows must be inserted");

    // Nearest-neighbour query with the pgvector operator.
    let rows = client
        .query(
            "SELECT id FROM items ORDER BY embedding <-> '[1,2,3]' LIMIT 1",
            &[],
        )
        .expect("client distance query should succeed");
    assert_eq!(rows.len(), 1);
    let nearest: i64 = rows[0].get(0);
    assert_eq!(nearest, 1, "row 1 embeds [1,2,3] and must be the closest");

    // A psql-style simple-protocol read of the stored vector column returns the
    // pgvector text form.
    let messages = client
        .simple_query("SELECT embedding FROM items ORDER BY id LIMIT 1")
        .expect("simple query of the vector column should succeed");
    let mut cells = Vec::new();
    for message in messages {
        if let postgres::SimpleQueryMessage::Row(row) = message {
            cells.push(row.get(0).map(str::to_owned));
        }
    }
    assert_eq!(
        cells,
        vec![Some("[1,2,3]".to_string())],
        "stored vector must round-trip as pgvector text"
    );

    // Typed drivers introspect unknown-type result columns by querying
    // pg_catalog.pg_type for the column's OID during `prepare`. The official
    // pgvector::Vector decoder then reads the binary result.
    let statement = client
        .prepare("SELECT embedding FROM items LIMIT 1")
        .expect("prepare of a vector result column should succeed");
    assert_eq!(statement.columns().len(), 1);
    let vector_type = &statement.columns()[0].type_();
    assert_eq!(vector_type.name(), "vector");
    assert_eq!(vector_type.oid(), VECTOR_OID);

    let rows = client
        .query(&statement, &[])
        .expect("executing the prepared vector select should succeed");
    assert_eq!(rows.len(), 1);
    let decoded: pgvector::Vector = rows[0].get(0);
    assert_eq!(decoded, pgvector::Vector::from(vec![1.0, 2.0, 3.0]));

    let _ = stop_tx.send(());
}

/// A prepared INSERT binding a vector parameter over the extended protocol:
/// the server must report the parameter's type as pgvector `vector` (OID
/// 16385), and an official `pgvector::Vector` binary-encoded value must be
/// accepted and stored.
#[test]
fn prepared_insert_binds_vector_parameter() {
    let session_context = SessionContext::new();
    setup_pg_catalog(
        &session_context,
        "datafusion",
        Arc::new(AuthManager::default()),
    )
    .expect("failed to setup pg_catalog");
    register_items(&session_context);

    let (port, stop_tx) = spawn_server(session_context);
    let mut client = connect(port);

    // Prepare an INSERT with a bound vector. The server's ParameterDescription
    // must advertise the vector parameter as the pgvector type (OID 16385) so a
    // typed client knows how to binary-encode it.
    let statement = client
        .prepare("INSERT INTO items (id, embedding) VALUES ($1, $2)")
        .expect("prepare INSERT with a vector parameter should succeed");

    assert_eq!(statement.params().len(), 2, "two parameters expected");
    let vector_param = &statement.params()[1];
    assert_eq!(
        vector_param.oid(),
        VECTOR_OID,
        "vector parameter must be reported with the pgvector type OID"
    );
    assert_eq!(vector_param.name(), "vector");

    // Bind an id and an official pgvector::Vector, then execute.
    let id: i64 = 42;
    let vector = pgvector::Vector::from(vec![1.0, 2.0, 3.0]);
    let affected = client
        .execute(&statement, &[&id, &vector])
        .expect("executing the prepared INSERT should succeed");
    assert_eq!(affected, 1, "one row must be inserted");

    // The stored vector round-trips: the closest row to [1,2,3] is the one we
    // just inserted, and its text form is correct.
    let rows = client
        .query(
            "SELECT id FROM items ORDER BY embedding <-> '[1,2,3]' LIMIT 1",
            &[],
        )
        .unwrap();
    let nearest: i64 = rows[0].get(0);
    assert_eq!(nearest, 42, "inserted row must be the nearest match");

    let messages = client
        .simple_query("SELECT embedding FROM items WHERE id = 42")
        .expect("simple query of the vector column should succeed");
    let mut cells = Vec::new();
    for message in messages {
        if let postgres::SimpleQueryMessage::Row(row) = message {
            cells.push(row.get(0).map(str::to_owned));
        }
    }
    assert_eq!(
        cells,
        vec![Some("[1,2,3]".to_string())],
        "binary-encoded vector parameter must be stored correctly"
    );

    let _ = stop_tx.send(());
}

/// The canonical pgvector DDL -- `CREATE TABLE items (id int PRIMARY KEY,
/// embedding vector(3))` -- must work over the wire, and the created table must
/// accept both literal and bound-parameter vector INSERTs.
#[test]
fn create_table_ddl_with_vector_column() {
    let session_context = SessionContext::new();
    setup_pg_catalog(
        &session_context,
        "datafusion",
        Arc::new(AuthManager::default()),
    )
    .expect("failed to setup pg_catalog");

    let (port, stop_tx) = spawn_server(session_context);
    let mut client = connect(port);

    client
        .batch_execute("CREATE TABLE items (id int PRIMARY KEY, embedding vector(3))")
        .expect("CREATE TABLE with a vector(3) column should succeed");

    // Insert a pgvector literal ...
    client
        .execute(
            "INSERT INTO items (id, embedding) VALUES (1, '[1,2,3]'), (2, '[4,5,6]')",
            &[],
        )
        .expect("literal INSERT into the DDL-created table should succeed");

    // ... and a bound vector parameter via a prepared statement.
    let statement = client
        .prepare("INSERT INTO items (id, embedding) VALUES ($1, $2)")
        .expect("prepare INSERT should succeed");
    assert_eq!(statement.params()[1].oid(), VECTOR_OID);
    let id: i32 = 3;
    let vector = pgvector::Vector::from(vec![7.0, 8.0, 9.0]);
    client
        .execute(&statement, &[&id, &vector])
        .expect("prepared vector INSERT into the DDL-created table should succeed");

    // Rows landed and nearest-neighbour search works over the DDL-created table.
    let rows = client
        .query(
            "SELECT id FROM items ORDER BY embedding <-> '[7,8,9]' LIMIT 1",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    let nearest: i32 = rows[0].get(0);
    assert_eq!(nearest, 3, "nearest row must be the prepared vector insert");

    let _ = stop_tx.send(());
}

/// Full pgvector-rust (`pgvector::Vector`) client round-trip: DDL, INSERT of a
/// bound `Vector` parameter, and SELECT decoding the binary result back into
/// `Vector`. Because the official client both encodes and decodes the real
/// pgvector wire format, this pins binary/text compatibility.
#[test]
fn pgvector_rust_official_client_roundtrip() {
    let session_context = SessionContext::new();
    setup_pg_catalog(
        &session_context,
        "datafusion",
        Arc::new(AuthManager::default()),
    )
    .expect("failed to setup pg_catalog");

    let (port, stop_tx) = spawn_server(session_context);
    let mut client = connect(port);

    // DDL (inline PRIMARY KEY + vector(3)).
    client
        .execute(
            "CREATE TABLE items (id int PRIMARY KEY, embedding vector(3))",
            &[],
        )
        .expect("CREATE TABLE with a vector(3) column should succeed");

    // Insert rows with pgvector string literals ...
    let inserted = client
        .execute(
            "INSERT INTO items (id, embedding) VALUES (1, '[1,2,3]'), (2, '[4,5,6]')",
            &[],
        )
        .expect("literal vector INSERT should succeed");
    assert_eq!(inserted, 2);

    // ... and with an official pgvector::Vector bound parameter (binary).
    let official_vec = pgvector::Vector::from(vec![7.0, 8.0, 9.0]);
    let inserted = client
        .execute(
            "INSERT INTO items (id, embedding) VALUES ($1, $2)",
            &[&3i32, &official_vec],
        )
        .expect("official pgvector Vector parameter INSERT should succeed");
    assert_eq!(inserted, 1);

    // Read the stored vector back with the official client's binary decoder.
    let row = client
        .query_one("SELECT embedding FROM items WHERE id = 3", &[])
        .expect("SELECT of the vector column should succeed");
    let decoded: pgvector::Vector = row.get(0);
    assert_eq!(decoded, official_vec, "official Vector must round-trip");

    // Nearest-neighbour search via the pgvector operator over the same data.
    let row = client
        .query_one(
            "SELECT id FROM items ORDER BY embedding <-> '[7,8,9]' LIMIT 1",
            &[],
        )
        .expect("distance query should succeed");
    let nearest: i32 = row.get(0);
    assert_eq!(nearest, 3, "nearest row must be the [7,8,9] vector");

    let _ = stop_tx.send(());
}
