# DataFusion PostgreSQL Integration Tests

This directory contains comprehensive integration tests for the DataFusion PostgreSQL wire protocol implementation. The tests verify compatibility with PostgreSQL clients and validate the enhanced features including authentication, role-based access control, and SSL/TLS encryption.

## Test Structure

### 1. Enhanced CSV Data Loading (`test_csv.py`)
- **Basic data access** - SELECT queries, row counting, filtering
- **Enhanced pg_catalog support** - System tables with real PostgreSQL metadata  
- **PostgreSQL functions** - version(), current_schema(), current_schemas(), has_table_privilege()
- **Table type detection** - Proper relkind values in pg_class
- **Transaction integration** - BEGIN/COMMIT within CSV tests

### 2. Transaction Support (`test_transactions.py`)
- **Complete transaction lifecycle** - BEGIN, COMMIT, ROLLBACK
- **Transaction variants** - Multiple syntax forms (BEGIN WORK, START TRANSACTION, etc.)
- **Failed transaction handling** - Error recovery and transaction state management
- **Transaction state persistence** - Multiple queries within transactions
- **Edge cases** - Error handling for invalid transaction operations

### 3. Enhanced Parquet Data Loading (`test_parquet.py`)
- **Advanced data types** - Complex Arrow types with PostgreSQL compatibility
- **Column metadata** - Comprehensive pg_attribute integration
- **Transaction support** - Full transaction lifecycle with Parquet data
- **Complex queries** - Aggregations, ordering, system table JOINs

### 4. Role-Based Access Control (`test_rbac.py`)
- **Authentication system** - User and role management
- **Permission checking** - Query-level access control
- **Superuser privileges** - Full access for postgres user
- **System catalog integration** - RBAC with PostgreSQL compatibility
- **Transaction security** - Access control within transactions

### 5. SSL/TLS Security (`test_ssl.py`)
- **Encryption support** - TLS configuration and setup
- **Certificate validation** - SSL certificate infrastructure
- **Connection security** - Encrypted and unencrypted connections
- **Feature availability** - Command-line TLS options

## Key Features Tested

### PostgreSQL Compatibility
- ✅ **Wire Protocol** - Full PostgreSQL client compatibility
- ✅ **System Catalogs** - Real pg_catalog tables with accurate metadata
- ✅ **Data Types** - Comprehensive type mapping (16 PostgreSQL types)
- ✅ **Functions** - Essential PostgreSQL functions implemented
- ✅ **Error Codes** - Proper PostgreSQL error code responses

### Security & Authentication
- ✅ **User Authentication** - Comprehensive user and role management
- ✅ **Role-Based Access Control** - Granular permission system
- ✅ **Permission Inheritance** - Hierarchical role relationships
- ✅ **Query-Level Security** - Per-operation access control
- ✅ **SSL/TLS Encryption** - Full transport layer security

### Transaction Management  
- ✅ **ACID Properties** - Complete transaction support
- ✅ **Error Recovery** - Failed transaction handling with proper error codes
- ✅ **State Management** - Transaction state tracking and persistence
- ✅ **Multiple Syntaxes** - Support for all PostgreSQL transaction variants

### Advanced Features
- ✅ **Complex Data Types** - Arrays, nested types, advanced Arrow types
- ✅ **Metadata Accuracy** - Precise column and table information
- ✅ **Query Optimization** - Efficient execution of complex queries
- ✅ **System Integration** - Seamless integration with PostgreSQL tooling

## Running the Tests

### Prerequisites
- Python 3.7+ with `psycopg` library
- DataFusion PostgreSQL server built (`cargo build`)
- Test data files (included in this directory)
- SSL certificates for TLS testing (`ssl/server.crt`, `ssl/server.key`)

### Execute All Tests
```bash
./test.sh
```

### Execute Individual Tests
```bash
# Enhanced CSV tests
python3 test_csv.py

# Transaction tests  
python3 test_transactions.py

# Enhanced Parquet tests
python3 test_parquet.py

# Role-based access control tests
python3 test_rbac.py

# SSL/TLS encryption tests
python3 test_ssl.py
```

### SSL/TLS Testing
```bash
# Generate test certificates
openssl req -x509 -newkey rsa:4096 -keyout ssl/server.key -out ssl/server.crt \
  -days 365 -nodes -subj "/C=US/ST=CA/L=SF/O=Test/OU=Test/CN=localhost"

# Run server with TLS
../target/debug/datafusion-postgres-cli -p 5433 \
  --csv delhi:delhiclimate.csv \
  --tls-cert ssl/server.crt \
  --tls-key ssl/server.key
```

## Security Features

### Authentication System
- **User Management** - Create, modify, and delete users
- **Role Management** - Hierarchical role system with inheritance
- **Password Authentication** - Secure user authentication (extensible)
- **Superuser Support** - Full administrative privileges

### Permission System
- **Granular Permissions** - SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER
- **Resource-Level Control** - Table, schema, database, function permissions
- **Grant Options** - WITH GRANT OPTION support for delegation
- **Inheritance** - Role-based permission inheritance

### Network Security
- **SSL/TLS Encryption** - Full transport layer security
- **Certificate Validation** - X.509 certificate support
- **Flexible Configuration** - Optional TLS with graceful fallback

## Test Data

### CSV Data (`delhiclimate.csv`)
- **1,462 rows** of Delhi climate data
- **Date, temperature, humidity** columns
- Perfect for testing data loading and filtering

### Parquet Data (`all_types.parquet`)  
- **14 different Arrow data types**
- **3 sample rows** with comprehensive type coverage
- Generated via `create_arrow_testfile.py`

### SSL Certificates (`ssl/`)
- **Test certificate** - Self-signed for testing
- **Private key** - RSA 4096-bit key
- **Production ready** - Real certificate infrastructure

## Expected Results

When all tests pass, you should see:

```
🎉 All enhanced integration tests passed!
==========================================

📈 Test Summary:
  ✅ Enhanced CSV data loading with PostgreSQL compatibility
  ✅ Complete transaction support (BEGIN/COMMIT/ROLLBACK)  
  ✅ Enhanced Parquet data loading with advanced data types
  ✅ Array types and complex data type support
  ✅ Improved pg_catalog system tables
  ✅ PostgreSQL function compatibility
  ✅ Role-based access control system
  ✅ SSL/TLS encryption support

🚀 Ready for production PostgreSQL workloads!
```

## Production Readiness

These tests validate that the DataFusion PostgreSQL server is ready for:
- **Secure production PostgreSQL workloads** with authentication and encryption
- **Complex analytical queries** with transaction safety and access control
- **Integration with existing PostgreSQL tools** and applications
- **Advanced data types** and modern analytics workflows
- **Enterprise security requirements** with RBAC and SSL/TLS

The test suite serves as both validation and documentation of the server's comprehensive PostgreSQL compatibility and security features.
