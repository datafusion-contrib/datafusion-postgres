# datafusion-postgres

![Crates.io Version](https://img.shields.io/crates/v/datafusion-postgres?label=datafusion-postgres)

Serving any [datafusion](https://datafusion.apache.org) `SessionContext` with full PostgreSQL compatibility, including authentication, role-based access control, and SSL/TLS encryption. Available as a library and a CLI tool.

This project adds a comprehensive [PostgreSQL compatible access layer](https://github.com/sunng87/pgwire) to the [Apache DataFusion](https://github.com/apache/arrow-datafusion) query engine, making it a drop-in replacement for PostgreSQL in analytics workloads.

It was originally an example of the [pgwire](https://github.com/sunng87/pgwire)
project.

## ✨ Key Features

- 🔌 **Full PostgreSQL Wire Protocol** - Compatible with all PostgreSQL clients and drivers
- 🛡️ **Enterprise Security** - Authentication, RBAC, and SSL/TLS encryption
- 🏗️ **Complete System Catalogs** - Real `pg_catalog` tables with accurate metadata  
- 📊 **Advanced Data Types** - Comprehensive Arrow ↔ PostgreSQL type mapping
- 🔄 **Transaction Support** - Full ACID transaction lifecycle (BEGIN/COMMIT/ROLLBACK)
- ⚡ **High Performance** - Apache DataFusion's columnar query execution

## 🎯 Roadmap & Status

- [x] **Core Features**
  - [x] datafusion-postgres as a CLI tool
  - [x] datafusion-postgres as a library
  - [x] datafusion information schema
  - [x] Complete `pg_catalog` system tables (pg_type, pg_attribute, pg_proc, pg_class, etc.)
  - [x] Comprehensive Arrow ↔ PostgreSQL data type mapping
  - [x] Essential PostgreSQL functions (version(), current_schema(), has_table_privilege(), etc.)

- [x] **Security & Authentication** 🆕
  - [x] User authentication and management
  - [x] Role-based access control (RBAC)
  - [x] Granular permissions (SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, etc.)
  - [x] Role inheritance and grant management
  - [x] SSL/TLS connection encryption
  - [x] Query-level permission checking

- [x] **Transaction Support** 🆕
  - [x] Full ACID transaction lifecycle
  - [x] BEGIN/COMMIT/ROLLBACK with all variants
  - [x] Failed transaction handling and recovery
  - [x] Transaction state management

- [ ] **Future Enhancements**
  - [ ] Connection pooling and performance optimizations
  - [ ] Advanced authentication methods (LDAP, certificates)
  - [ ] More PostgreSQL functions and operators
  - [ ] COPY protocol for bulk data loading

## 🔐 Authentication & Security

datafusion-postgres supports enterprise-grade authentication through pgwire's standard mechanisms:

### Production Authentication Setup

Proper pgwire authentication:

```rust
use pgwire::api::auth::cleartext::CleartextStartupHandler;
use datafusion_postgres::auth::{DfAuthSource, AuthManager};

// Setup authentication
let auth_manager = Arc::new(AuthManager::new());
let auth_source = Arc::new(DfAuthSource::new(auth_manager));

// Choose authentication method:
// 1. Cleartext (simple)
let authenticator = CleartextStartupHandler::new(
    auth_source,
    Arc::new(DefaultServerParameterProvider::default())
);

// 2. MD5 (recommended)
// let authenticator = MD5StartupHandler::new(auth_source, params);

// 3. SCRAM (enterprise - requires "server-api-scram" feature)
// let authenticator = SASLScramAuthStartupHandler::new(auth_source, params);
```

### User Management

```rust
// Add users to the RBAC system
auth_manager.add_user("admin", "secure_password", vec!["dbadmin".to_string()]).await;
auth_manager.add_user("analyst", "password123", vec!["readonly".to_string()]).await;
```

## 🚀 Quick Start

### The Library `datafusion-postgres`

The high-level entrypoint of `datafusion-postgres` library is the `serve`
function which takes a datafusion `SessionContext` and some server configuration
options.

```rust
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_postgres::{serve, ServerOptions};

// Create datafusion SessionContext
let session_context = Arc::new(SessionContext::new());
// Configure your `session_context`
// ...

// Start the Postgres compatible server with SSL/TLS
let server_options = ServerOptions::new()
    .with_host("127.0.0.1".to_string())
    .with_port(5432)
    .with_tls_cert_path(Some("server.crt".to_string()))
    .with_tls_key_path(Some("server.key".to_string()));

serve(session_context, &server_options).await
```

### Security Features

```rust
// The server automatically includes:
// - User authentication (default postgres superuser)
// - Role-based access control with predefined roles:
//   - readonly: SELECT permissions
//   - readwrite: SELECT, INSERT, UPDATE, DELETE permissions  
//   - dbadmin: Full administrative permissions
// - SSL/TLS encryption when certificates are provided
// - Query-level permission checking
```

### The CLI `datafusion-postgres-cli`

As a command-line application, this tool serves any JSON/CSV/Arrow/Parquet/Avro
files as tables, and exposes them via PostgreSQL compatible protocol with full security features.

```
datafusion-postgres-cli 0.6.1
A secure postgres interface for datafusion. Serve any CSV/JSON/Arrow/Parquet files as tables.

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

> **🔐 PRODUCTION AUTHENTICATION**: For production deployments, implement proper authentication by using `DfAuthSource` with pgwire's standard authentication handlers:
> - **Cleartext**: `CleartextStartupHandler` for simple password auth
> - **MD5**: `MD5StartupHandler` for MD5-hashed passwords  
> - **SCRAM**: `SASLScramAuthStartupHandler` for enterprise-grade security
> 
> See `auth.rs` for complete implementation examples. The default setup is for development only.

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

## License

This library is released under Apache license.
