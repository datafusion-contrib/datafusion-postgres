# PostgreSQL Wire Protocol, pgwire, and DataFusion: An Overview

## The Three Layers

### Layer 1: pgwire (Protocol Framework)

**What it is:** A Rust library that implements the PostgreSQL wire protocol—the TCP/TLS-based format that `psql`, JDBC drivers, and other PostgreSQL clients speak.

**What it does:**
- TCP/TLS listener and connection management
- Message codec (encode/decode wire format)
- Connection state machine (SSL → Startup → Auth → ReadyForQuery)
- Message dispatching (routes client messages to handlers)
- Auto-sends protocol responses (ReadyForQuery, ParseComplete, BindComplete, etc.)

**What it does NOT do:** Execute SQL queries. It has zero query semantics.

**Analogy:** Like `hyper` or `actix-web` for HTTP—a framework you build on top of.

### Layer 2: Apache DataFusion (Query Engine)

**What it is:** A columnar query engine that parses SQL, optimizes logical plans, and executes them on Arrow-format data.

**What it does:**
- SQL parsing → LogicalPlan
- Plan optimization
- Execution → Arrow RecordBatches

**What it does NOT do:** Manage connections, handle protocol, or understand PostgreSQL-specific concepts (sessions, transactions, parameters).

**Analogy:** The database engine.

### Layer 3: datafusion-postgres (Glue Layer)

**What it is:** A Rust library that wires DataFusion to pgwire, translating between them and handling PostgreSQL-specific protocol concerns.

**Key components:**
- `HandlerFactory` + `DfSessionService`: Implements pgwire's handler traits
- **Hook system**: Intercepts protocol-level commands before they reach DataFusion
- Type converters: Arrow → PostgreSQL data types (via `arrow-pg` crate)

## Why the Hook System?

DataFusion is a query engine. PostgreSQL is a database server. They speak different languages.

Commands like `SET datestyle='ISO'`, `BEGIN`, `COMMIT`, `ANALYZE` are **not queries**—they're **protocol semantics**:

- **SET/SHOW**: Need to update session state and send a `ParameterStatus` message back to the client
- **BEGIN/COMMIT**: Need to manage transaction boundaries and track connection state across multiple client messages
- **ANALYZE**: Custom protocol commands

DataFusion doesn't (and shouldn't) implement these. The hook system intercepts them:

```
Client sends: "SET datestyle='ISO'; SELECT 1"
     ↓
Parser splits: [SET stmt, SELECT stmt]
     ↓
Statement 1 (SET) → SetShowHook.handle() → Returns Ok(SET response)
     ↓
Statement 2 (SELECT) → Hook returns None → Passes to DataFusion
     ↓
DataFusion executes SELECT → Arrow RecordBatch
     ↓
Convert Arrow → PostgreSQL format
     ↓
pgwire encodes and sends to client
```

**Without hooks:** You'd patch DataFusion to understand PostgreSQL protocol every time you need a new feature.

**With hooks:** You handle protocol concerns in the right layer, leaving DataFusion as a pure query engine.

## Architecture

```
PostgreSQL Client (psql, DBeaver, etc.)
           ↓ (wire protocol)
        pgwire (transport)
           ↓ (calls handlers)
   datafusion-postgres (glue)
           ↓ (some routes to)
      DataFusion (query engine)
```

## Query Flow Example

1. Client sends: `SELECT COUNT(*) FROM table`
2. pgwire receives, parses, calls handler
3. datafusion-postgres hook system checks: "Is this SET/SHOW/BEGIN?" → No
4. Hook returns `None`
5. Query passed to DataFusion
6. DataFusion: parse → optimize → execute → Arrow RecordBatch
7. datafusion-postgres converts Arrow rows to PostgreSQL format
8. pgwire encodes as `RowDescription` + `DataRow` messages
9. Client receives result

## Key Insight

- **pgwire** exists so any Rust project can pretend to be a PostgreSQL server
- **datafusion-postgres** exists to wire DataFusion (an in-process query engine with no network layer) to pgwire
- **hooks** exist to separate protocol concerns from query execution

This separation means:
- pgwire stays generic (used by other databases: GreptimeDB, RisingLight, SpacetimeDB)
- DataFusion stays focused (just executes queries on Arrow data)
- datafusion-postgres is lightweight (just a translation layer + protocol hooks)
