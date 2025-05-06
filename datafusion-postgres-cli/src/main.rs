use std::sync::Arc;

use datafusion::execution::options::{
    ArrowReadOptions, AvroReadOptions, CsvReadOptions, NdJsonReadOptions, ParquetReadOptions,
};
use datafusion::prelude::SessionContext;
use datafusion_postgres::{DfSessionService, HandlerFactory}; // Assuming the crate name is `datafusion_postgres`
use pgwire::tokio::process_socket;
use structopt::StructOpt;
use tokio::net::TcpListener;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "datafusion-postgres",
    about = "A postgres interface for datafusion. Serve any CSV/JSON/Arrow files as tables."
)]
struct Opt {
    /// CSV files to register as table, using syntax `table_name:file_path`
    #[structopt(long("csv"))]
    csv_tables: Vec<String>,
    /// JSON files to register as table, using syntax `table_name:file_path`
    #[structopt(long("json"))]
    json_tables: Vec<String>,
    /// Arrow files to register as table, using syntax `table_name:file_path`
    #[structopt(long("arrow"))]
    arrow_tables: Vec<String>,
    /// Parquet files to register as table, using syntax `table_name:file_path`
    #[structopt(long("parquet"))]
    parquet_tables: Vec<String>,
    /// Avro files to register as table, using syntax `table_name:file_path`
    #[structopt(long("avro"))]
    avro_tables: Vec<String>,
    /// Port the server listens to, default to 5432
    #[structopt(short, default_value = "5432")]
    port: u16,
    /// Host address the server listens to, default to 127.0.0.1
    #[structopt(long("host"), default_value = "127.0.0.1")]
    host: String,
}

fn parse_table_def(table_def: &str) -> (&str, &str) {
    table_def
        .split_once(':')
        .expect("Use this pattern to register table: table_name:file_path")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = Opt::from_args();

    let session_context = SessionContext::new();

    // Register CSV tables
    for (table_name, table_path) in opts.csv_tables.iter().map(|s| parse_table_def(s.as_ref())) {
        session_context
            .register_csv(table_name, table_path, CsvReadOptions::default())
            .await
            .map_err(|e| format!("Failed to register CSV table '{}': {}", table_name, e))?;
        println!("Loaded {} as table {}", table_path, table_name);
    }

    // Register JSON tables
    for (table_name, table_path) in opts.json_tables.iter().map(|s| parse_table_def(s.as_ref())) {
        session_context
            .register_json(table_name, table_path, NdJsonReadOptions::default())
            .await
            .map_err(|e| format!("Failed to register JSON table '{}': {}", table_name, e))?;
        println!("Loaded {} as table {}", table_path, table_name);
    }

    // Register Arrow tables
    for (table_name, table_path) in opts
        .arrow_tables
        .iter()
        .map(|s| parse_table_def(s.as_ref()))
    {
        session_context
            .register_arrow(table_name, table_path, ArrowReadOptions::default())
            .await
            .map_err(|e| format!("Failed to register Arrow table '{}': {}", table_name, e))?;
        println!("Loaded {} as table {}", table_path, table_name);
    }

    // Register Parquet tables
    for (table_name, table_path) in opts
        .parquet_tables
        .iter()
        .map(|s| parse_table_def(s.as_ref()))
    {
        session_context
            .register_parquet(table_name, table_path, ParquetReadOptions::default())
            .await
            .map_err(|e| format!("Failed to register Parquet table '{}': {}", table_name, e))?;
        println!("Loaded {} as table {}", table_path, table_name);
    }

    // Register Avro tables
    for (table_name, table_path) in opts.avro_tables.iter().map(|s| parse_table_def(s.as_ref())) {
        session_context
            .register_avro(table_name, table_path, AvroReadOptions::default())
            .await
            .map_err(|e| format!("Failed to register Avro table '{}': {}", table_name, e))?;
        println!("Loaded {} as table {}", table_path, table_name);
    }

    // Get the first catalog name from the session context
    let catalog_name = session_context
        .catalog_names() // Fixed: Removed .catalog_list()
        .first()
        .cloned();

    // Create the handler factory with the session context and catalog name
    let factory = Arc::new(HandlerFactory(Arc::new(DfSessionService::new(
        session_context,
        catalog_name,
    ))));

    // Bind to the specified host and port
    let server_addr = format!("{}:{}", opts.host, opts.port);
    let listener = TcpListener::bind(&server_addr).await?;
    println!("Listening on {}", server_addr);

    // Accept incoming connections
    loop {
        let (socket, addr) = listener.accept().await?;
        let factory_ref = factory.clone();
        println!("Accepted connection from {}", addr);

        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, None, factory_ref).await {
                eprintln!("Error processing socket: {}", e);
            }
        });
    }
}
