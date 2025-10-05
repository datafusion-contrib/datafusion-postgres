# datafusion-postgres

[![Crates.io Version][crates-badge]][crates-url]
[![Docs.rs Version][docs-badge]][docs-url]

[crates-badge]: https://img.shields.io/crates/v/datafusion-postgres?label=datafusion-postgres
[crates-url]: https://crates.io/crates/datafusion-postgres
[docs-badge]: https://img.shields.io/docsrs/datafusion-postgres
[docs-url]: https://docs.rs/datafusion-postgres/latest/datafusion_postgres

A PostgreSQL-compatible server for [Apache DataFusion](https://datafusion.apache.org), supporting authentication, role-based access control, and SSL/TLS encryption. Available as both a library and CLI tool.

Built on [pgwire](https://github.com/sunng87/pgwire) to provide PostgreSQL wire protocol compatibility for analytical workloads.
It was originally an example of the [pgwire](https://github.com/sunng87/pgwire)
project.

## Scope of the Project

- `datafusion-postgres`: Postgres frontend for datafusion, as a library.
  - Serving Datafusion `SessionContext` with pgwire library
  - Customizible authentication
  - Permission control
  - Built-in `pg_catalog` tables
  - Built-in postgres functions for common meta queries
- `datafusion-postgres-cli`: A cli tool starts a postgres compatible server for
  datafusion supported file formats, just like python's `SimpleHTTPServer`.
- `arrow-pg`: A data type mapping, encoding/decoding library for arrow and
  postgres(pgwire) data types.

See `auth.rs` for complete implementation examples using `DfAuthSource`.

## Supported Database Clients

- Database Clients
  - [x] psql
  - [x] DBeaver
  - [x] pgcli
  - [x] VSCode SQLTools
- BI
  - [x] Metabase

## Quick Start

### The Library `datafusion-postgres`

The high-level entrypoint of `datafusion-postgres` library is the `serve`
function which takes a datafusion `SessionContext` and some server configuration
options.

```rust
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_postgres::{serve, ServerOptions};
use datafusion_postgres::pg_catalog::setup_pg_catalog;

// Create datafusion SessionContext
let session_context = Arc::new(SessionContext::new());
// Configure your `session_context`
// ...

// Optional: setup pg_catalog schema
setup_pg_catalog(session_context, "datafusion")?;

// Start the Postgres compatible server with SSL/TLS
let server_options = ServerOptions::new()
    .with_host("127.0.0.1".to_string())
    .with_port(5432)
    // Optional: setup tls
    .with_tls_cert_path(Some("server.crt".to_string()))
    .with_tls_key_path(Some("server.key".to_string()));

serve(session_context, &server_options).await
```

### Security Features

The server automatically includes:

- User authentication (default postgres superuser)
- Role-based access control with predefined roles:
  - readonly: SELECT permissions
  - readwrite: SELECT, INSERT, UPDATE, DELETE permissions
  - dbadmin: Full administrative permissions
- SSL/TLS encryption when certificates are provided
- Query-level permission checking

### The CLI `datafusion-postgres-cli`

Command-line tool to serve JSON/CSV/Arrow/Parquet/Avro files as PostgreSQL-compatible tables.

```
datafusion-postgres-cli 0.6.1
A PostgreSQL interface for DataFusion. Serve CSV/JSON/Arrow/Parquet files as tables.

USAGE:
    datafusion-postgres-cli [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --arrow <arrow-tables>...        Arrow files to register as table, using syntax `table_name:file_path`
        --avro <avro-tables>...          Avro files to register as table, using syntax `table_name:file_path`
        --csv <csv-tables>...            CSV files to register as table, using syntax `table_name:file_path`
    -d, --dir <directory>                Directory to serve, all supported files will be registered as tables
        --host <host>                    Host address the server listens to [default: 127.0.0.1]
        --json <json-tables>...          JSON files to register as table, using syntax `table_name:file_path`
        --parquet <parquet-tables>...    Parquet files to register as table, using syntax `table_name:file_path`
    -p <port>                            Port the server listens to [default: 5432]
        --tls-cert <tls-cert>            Path to TLS certificate file for SSL/TLS encryption
        --tls-key <tls-key>              Path to TLS private key file for SSL/TLS encryption
```

#### 🔒 Security Options

```bash
# Run with SSL/TLS encryption
datafusion-postgres-cli \
  --csv data:sample.csv \
  --tls-cert server.crt \
  --tls-key server.key

# Run without encryption (development only)
datafusion-postgres-cli --csv data:sample.csv
```

## 📋 Example Usage

### Basic Example

Host a CSV dataset as a PostgreSQL-compatible table:

```bash
datafusion-postgres-cli --csv climate:delhiclimate.csv
```

```
Loaded delhiclimate.csv as table climate
TLS not configured. Running without encryption.
Listening on 127.0.0.1:5432 (unencrypted)
```

### Connect with psql

> **🔐 Authentication**: The default setup allows connections without authentication for development. For secure deployments, use `DfAuthSource` with standard pgwire authentication handlers (cleartext, MD5, or SCRAM). See `auth.rs` for implementation examples.

```bash
psql -h 127.0.0.1 -p 5432 -U postgres
```

```sql
postgres=> SELECT COUNT(*) FROM climate;
 count
-------
  1462
(1 row)

postgres=> SELECT date, meantemp FROM climate WHERE meantemp > 35 LIMIT 5;
    date    | meantemp
------------+----------
 2017-05-15 |     36.9
 2017-05-16 |     37.9
 2017-05-17 |     38.6
 2017-05-18 |     37.4
 2017-05-19 |     35.4
(5 rows)

postgres=> BEGIN;
BEGIN
postgres=> SELECT AVG(meantemp) FROM climate;
       avg
------------------
 25.4955206557617
(1 row)
postgres=> COMMIT;
COMMIT
```

### 🔐 Production Setup with SSL/TLS

```bash
# Generate SSL certificates
openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt \
  -days 365 -nodes -subj "/C=US/ST=CA/L=SF/O=MyOrg/CN=localhost"

# Start secure server
datafusion-postgres-cli \
  --csv climate:delhiclimate.csv \
  --tls-cert server.crt \
  --tls-key server.key
```

```
Loaded delhiclimate.csv as table climate
TLS enabled using cert: server.crt and key: server.key
Listening on 127.0.0.1:5432 with TLS encryption
```

## Community

### Developer Mailing List

If you like the idea of pgwire, datafusion-postgres and want to join the
development of the library, or its ecosystem integrations, extensions, you are
welcomed to join our developer mailing list: https://groups.io/g/pgwire-dev/

## License

This library is released under Apache license.
