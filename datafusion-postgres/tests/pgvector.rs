//! End-to-end pgvector tests through the wire-protocol handler: INSERT of a
//! `'[...]'` string literal into a `vector(n)` column and vector distance
//! queries, driven exactly like a real PostgreSQL client would send them.
#![cfg(feature = "pgvector")]

use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use datafusion::arrow::array::{FixedSizeListArray, Float32Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_pg_catalog::setup_pg_catalog;
use pgwire::api::query::SimpleQueryHandler;
use postgres_types::{IsNull, ToSql, Type};
use tokio_postgres::NoTls;

use datafusion_postgres::DfSessionService;
use datafusion_postgres::auth::AuthManager;
use datafusion_postgres::testing::MockClient;
use datafusion_postgres::{ServerOptions, serve};

/// pgvector `vector` type OID, matching `arrow_pg::datatypes::PG_VECTOR_TYPE_OID`.
const VECTOR_OID: u32 = 16385;

/// A client-side pgvector `vector` value that binary-encodes like real pgvector
/// does: big-endian `int16` dimension followed by big-endian IEEE float32s.
#[derive(Debug)]
struct PgVector(Vec<f32>);

impl ToSql for PgVector {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if !PgVector::accepts(ty) {
            return Err("vector value bound to a non-vector parameter".into());
        }
        out.put_i16(self.0.len() as i16);
        for v in &self.0 {
            out.put_slice(&v.to_be_bytes());
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.oid() == VECTOR_OID
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.to_sql(ty, out)
    }
}

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

/// A real PostgreSQL client (`tokio-postgres`) driving the full pgwire
/// protocol -- startup/authentication over TCP, then INSERT of pgvector
/// literals and a vector distance query over the *extended* query protocol.
#[tokio::test]
async fn real_pgwire_client_inserts_and_queries_vectors() {
    let session_context = SessionContext::new();
    setup_pg_catalog(
        &session_context,
        "datafusion",
        Arc::new(AuthManager::default()),
    )
    .expect("failed to setup pg_catalog");
    register_items(&session_context);

    let port = free_port();
    let server = tokio::spawn(async move {
        let ctx = Arc::new(session_context);
        let options = ServerOptions::new()
            .with_host("127.0.0.1".to_string())
            .with_port(port);
        let _ = serve(ctx, &options).await;
    });

    // Connect over a real TCP socket, retrying briefly while the listener
    // comes up.
    let (client, connection) = loop {
        let mut config = tokio_postgres::Config::new();
        config.host("127.0.0.1");
        config.port(port);
        config.user("postgres");
        config.dbname("datafusion");
        if let Ok(connected) = config.connect(NoTls).await {
            break connected;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // INSERT of pgvector bracket literals (extended protocol).
    let inserted = client
        .execute(
            "INSERT INTO items (id, embedding) VALUES (1, '[1,2,3]'), (2, '[4,5,6]')",
            &[],
        )
        .await
        .expect("client INSERT of vector literals should succeed");
    assert_eq!(inserted, 2, "both rows must be inserted");

    // Nearest-neighbour query with the pgvector operator.
    let rows = client
        .query(
            "SELECT id FROM items ORDER BY embedding <-> '[1,2,3]' LIMIT 1",
            &[],
        )
        .await
        .expect("client distance query should succeed");
    assert_eq!(rows.len(), 1);
    let nearest: i64 = rows[0].get(0);
    assert_eq!(nearest, 1, "row 1 embeds [1,2,3] and must be the closest");

    // A psql-style simple-protocol read of the stored vector column returns the
    // pgvector text form. (psql does not introspect pg_type for a result's
    // unknown-type columns, so this exercises the same path psql uses.)
    let messages = client
        .simple_query("SELECT embedding FROM items ORDER BY id LIMIT 1")
        .await
        .expect("simple query of the vector column should succeed");
    use tokio_postgres::SimpleQueryMessage;
    let mut cells = Vec::new();
    for message in messages {
        if let SimpleQueryMessage::Row(row) = message {
            cells.push(row.get(0).map(str::to_owned));
        }
    }
    assert_eq!(
        cells,
        vec![Some("[1,2,3]".to_string())],
        "stored vector must round-trip as pgvector text"
    );

    // Typed drivers introspect unknown-type result columns by querying
    // pg_catalog.pg_type for the column's OID during `prepare`. With the
    // vector row injected and oid/"char" columns wired correctly this must
    // succeed and report the pgvector type.
    let prepared = client
        .prepare("SELECT embedding FROM items LIMIT 1")
        .await
        .expect("prepare of a vector result column should succeed");
    assert_eq!(prepared.columns().len(), 1);
    let vector_type = prepared.columns()[0].type_();
    assert_eq!(vector_type.name(), "vector");
    assert_eq!(vector_type.oid(), 16385); // matches arrow-pg PG_VECTOR_TYPE_OID

    let rows = client
        .query(&prepared, &[])
        .await
        .expect("executing the prepared vector select should succeed");
    assert_eq!(rows.len(), 1);

    server.abort();
}

/// A prepared INSERT binding a vector parameter over the extended protocol:
/// the server must report the parameter's type as pgvector `vector` (OID
/// 16385), and a client binary-encoded vector must be accepted and stored.
#[tokio::test]
async fn prepared_insert_binds_vector_parameter() {
    let session_context = SessionContext::new();
    setup_pg_catalog(
        &session_context,
        "datafusion",
        Arc::new(AuthManager::default()),
    )
    .expect("failed to setup pg_catalog");
    register_items(&session_context);

    let port = free_port();
    let server = tokio::spawn(async move {
        let ctx = Arc::new(session_context);
        let options = ServerOptions::new()
            .with_host("127.0.0.1".to_string())
            .with_port(port);
        let _ = serve(ctx, &options).await;
    });

    let (client, connection) = loop {
        let mut config = tokio_postgres::Config::new();
        config.host("127.0.0.1");
        config.port(port);
        config.user("postgres");
        config.dbname("datafusion");
        if let Ok(connected) = config.connect(NoTls).await {
            break connected;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Prepare an INSERT with a bound vector. The server's ParameterDescription
    // must advertise the vector parameter as the pgvector type (OID 16385) so a
    // typed client knows how to binary-encode it.
    let statement = client
        .prepare("INSERT INTO items (id, embedding) VALUES ($1, $2)")
        .await
        .expect("prepare INSERT with a vector parameter should succeed");

    assert_eq!(statement.params().len(), 2, "two parameters expected");
    let vector_param = &statement.params()[1];
    assert_eq!(
        vector_param.oid(),
        VECTOR_OID,
        "vector parameter must be reported with the pgvector type OID"
    );
    assert_eq!(vector_param.name(), "vector");

    // Bind an id and a binary-encoded vector and execute.
    let id: i64 = 42;
    let vector = PgVector(vec![1.0, 2.0, 3.0]);
    let affected = client
        .execute(&statement, &[&id, &vector])
        .await
        .expect("executing the prepared INSERT should succeed");
    assert_eq!(affected, 1, "one row must be inserted");

    // The stored vector round-trips: the closest row to [1,2,3] is the one we
    // just inserted, and its text form is correct.
    let rows = client
        .query(
            "SELECT id FROM items ORDER BY embedding <-> '[1,2,3]' LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    let nearest: i64 = rows[0].get(0);
    assert_eq!(nearest, 42, "inserted row must be the nearest match");

    let messages = client
        .simple_query("SELECT embedding FROM items WHERE id = 42")
        .await
        .expect("simple query of the vector column should succeed");
    use tokio_postgres::SimpleQueryMessage;
    let mut cells = Vec::new();
    for message in messages {
        if let SimpleQueryMessage::Row(row) = message {
            cells.push(row.get(0).map(str::to_owned));
        }
    }
    assert_eq!(
        cells,
        vec![Some("[1,2,3]".to_string())],
        "binary-encoded vector parameter must be stored correctly"
    );

    server.abort();
}

/// Start a server over `pg_catalog` (so the pgvector type planner and the
/// injected `vector` pg_type row are active) on a free port, returning the port
/// and server task.
fn spawn_pgvector_server() -> (u16, tokio::task::JoinHandle<()>) {
    let session_context = SessionContext::new();
    setup_pg_catalog(
        &session_context,
        "datafusion",
        Arc::new(AuthManager::default()),
    )
    .expect("failed to setup pg_catalog");

    let port = free_port();
    let server = tokio::spawn(async move {
        let ctx = Arc::new(session_context);
        let options = ServerOptions::new()
            .with_host("127.0.0.1".to_string())
            .with_port(port);
        let _ = serve(ctx, &options).await;
    });
    (port, server)
}

/// Connect a tokio-postgres client to `port`, retrying while the listener starts.
async fn connect_pgwire_client(port: u16) -> tokio_postgres::Client {
    let (client, connection) = loop {
        let mut config = tokio_postgres::Config::new();
        config.host("127.0.0.1");
        config.port(port);
        config.user("postgres");
        config.dbname("datafusion");
        if let Ok(connected) = config.connect(NoTls).await {
            break connected;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// The canonical pgvector DDL -- `CREATE TABLE items (id int PRIMARY KEY,
/// embedding vector(3))` -- must work over the wire, and the created table must
/// accept both literal and bound-parameter vector INSERTs.
#[tokio::test]
async fn create_table_ddl_with_vector_column() {
    let (port, server) = spawn_pgvector_server();
    let client = connect_pgwire_client(port).await;

    client
        .batch_execute("CREATE TABLE items (id int PRIMARY KEY, embedding vector(3))")
        .await
        .expect("CREATE TABLE with a vector(3) column should succeed");

    // Insert a pgvector literal ...
    client
        .execute(
            "INSERT INTO items (id, embedding) VALUES (1, '[1,2,3]'), (2, '[4,5,6]')",
            &[],
        )
        .await
        .expect("literal INSERT into the DDL-created table should succeed");

    // ... and a bound vector parameter via a prepared statement.
    let statement = client
        .prepare("INSERT INTO items (id, embedding) VALUES ($1, $2)")
        .await
        .expect("prepare INSERT should succeed");
    assert_eq!(statement.params()[1].oid(), VECTOR_OID);
    let id: i32 = 3;
    let vector = PgVector(vec![7.0, 8.0, 9.0]);
    client
        .execute(&statement, &[&id, &vector])
        .await
        .expect("prepared vector INSERT into the DDL-created table should succeed");

    // Rows landed and nearest-neighbour search works over the DDL-created table.
    let rows = client
        .query(
            "SELECT id FROM items ORDER BY embedding <-> '[7,8,9]' LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let nearest: i32 = rows[0].get(0);
    assert_eq!(nearest, 3, "nearest row must be the prepared vector insert");

    server.abort();
}
