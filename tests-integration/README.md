# DataFusion PostgreSQL Integration Tests

This directory contains integration tests for PostgreSQL compatibility features in datafusion-postgres.

## Test Files

### Core Tests
- **`test_csv.py`** - Enhanced CSV data loading with PostgreSQL compatibility, transaction support, and pg_catalog testing
- **`test_parquet.py`** - Enhanced Parquet data loading with advanced data types, array support, and complex queries  
- **`test_transactions.py`** - Comprehensive transaction support testing (BEGIN, COMMIT, ROLLBACK, failed transactions)

### Test Runner
- **`test.sh`** - Main test runner script that executes all enhanced tests

## Features Tested

### 🔐 Transaction Support
- ✅ **Complete transaction lifecycle** - BEGIN, COMMIT, ROLLBACK, ABORT
- ✅ **Transaction variants** - BEGIN WORK, COMMIT TRANSACTION, START TRANSACTION, END, etc.
- ✅ **Failed transaction handling** - Proper error codes (25P01), query blocking until rollback
- ✅ **Transaction state persistence** - Multiple queries within single transaction
- ✅ **Edge cases** - COMMIT outside transaction, nested BEGIN, COMMIT in failed transaction

### 🗂️ Enhanced pg_catalog System Tables  
- ✅ **pg_type** - PostgreSQL data types (16+ core types: bool, int2, int4, int8, float4, float8, text, char, bytea, date, timestamp, time, interval, uuid, json, jsonb)
- ✅ **pg_class** - Table and relation metadata with **proper table type detection** (relkind: 'r' for regular, 'v' for views)
- ✅ **pg_attribute** - Column information with proper attnum, atttypid, etc.
- ✅ **pg_proc** - Function metadata for PostgreSQL compatibility functions
- ✅ **pg_namespace** - Schema information (public, pg_catalog, information_schema)
- ✅ **pg_database** - Database metadata

### 🔧 PostgreSQL Functions
- ✅ **version()** - Returns DataFusion PostgreSQL version string
- ✅ **current_schema()** - Returns current schema name
- ✅ **current_schemas(boolean)** - Returns schema search path
- ✅ **pg_get_userbyid(oid)** - Returns user name for given OID
- ✅ **has_table_privilege(user, table, privilege)** - Checks table privileges

### 🏗️ Advanced Data Type Support
- ✅ **Array types** - BOOL_ARRAY, INT2_ARRAY, INT4_ARRAY, INT8_ARRAY, FLOAT4_ARRAY, FLOAT8_ARRAY, TEXT_ARRAY
- ✅ **Advanced types** - MONEY, INET, MACADDR for financial/network data
- ✅ **Complex data types** - Nested lists, maps, union types, dictionary types
- ✅ **Enhanced parameter types** - TIME, UUID, JSON, JSONB, INTERVAL in prepared statements
- ✅ **Proper Arrow to PostgreSQL type mapping**
- ✅ **Fixed encoder issues** - Time32/Time64, LargeUtf8, Decimal256, Duration array types

### 🛡️ Error Handling & Compatibility
- ✅ **PostgreSQL-compatible error codes** - "22003" for numeric_value_out_of_range, "25P01" for aborted transactions
- ✅ **Proper error message formatting** - Standard PostgreSQL error structure
- ✅ **Graceful handling** - Invalid queries, data type conversions, transaction failures
- ✅ **Failed transaction recovery** - Proper blocking and rollback mechanisms

### 📋 information_schema Compatibility
- ✅ **information_schema.tables** - Table metadata with proper table types
- ✅ **information_schema.columns** - Column metadata with data type information
- ✅ **PostgreSQL tool compatibility** - Works with tools expecting standard system catalogs
- ✅ **JOIN support** - System table joins with user data

## Running Tests

### Simple Test Execution

Run all tests with a single command:
```bash
./tests-integration/test.sh
```

This script will:
1. Build the datafusion-postgres project
2. Set up Python virtual environment and dependencies
3. Run CSV data loading tests
4. Run Parquet data loading tests
5. Verify PostgreSQL compatibility features

### Manual Test Execution

If you prefer to run tests manually:

1. **Build the project:**
   ```bash
   cargo build
   ```

2. **Set up Python environment:**
   ```bash
   cd tests-integration
   python3 -m venv test_env
   source test_env/bin/activate
   pip install psycopg
   ```

3. **Run individual tests:**
   ```bash
   # Test CSV data with PostgreSQL compatibility
   ../target/debug/datafusion-postgres-cli -p 5433 --csv delhi:delhiclimate.csv &
   python3 test_csv.py
   
   # Test Parquet data
   ../target/debug/datafusion-postgres-cli -p 5433 --parquet all_types:all_types.parquet &
   python3 test_parquet.py
   ```

## Test Results

When running `./test.sh`, you should see output like:

```
🚀 Running DataFusion PostgreSQL Integration Tests
==================================================
Building datafusion-postgres...
Setting up Python dependencies...

📊 Test 1: Enhanced CSV Data Loading & PostgreSQL Compatibility
----------------------------------------------------------------
🔍 Testing CSV data loading and PostgreSQL compatibility...

📊 Basic Data Access Tests:
  ✓ Delhi dataset count: 1462 rows
  ✓ Limited query: 10 rows  
  ✓ Parameterized query: 527 rows where meantemp > 30

🗂️ Enhanced pg_catalog Tests:
  ✓ pg_catalog.pg_type: 16 data types
  ✓ Core PostgreSQL types present: bool, int4, json, text, uuid
  ✓ Table type detection: delhi = 'r' (regular table)
  ✓ pg_attribute: 162 columns tracked

🔧 PostgreSQL Functions Tests:
  ✓ version(): DataFusion PostgreSQL 48.0.0 on x86_64-pc-linux...
  ✓ current_schema(): public
  ✓ current_schemas(): ['public']
  ✓ has_table_privilege(): True

✅ Enhanced CSV test passed

🔐 Test 2: Transaction Support
------------------------------
🔐 Testing PostgreSQL Transaction Support

📝 Test 1: Basic Transaction Lifecycle
  ✓ BEGIN executed
  ✓ Query in transaction: 1462 rows
  ✓ COMMIT executed
  ✓ Query after commit works

📝 Test 2: Transaction Variants
  ✓ BEGIN -> COMMIT
  ✓ BEGIN TRANSACTION -> COMMIT TRANSACTION
  ✓ ROLLBACK
  ✓ ABORT

📝 Test 3: Failed Transaction Handling  
  ✓ Transaction started
  ✓ Invalid query failed as expected
  ✓ Subsequent query blocked (error code 25P01)
  ✓ ROLLBACK from failed transaction successful
  ✓ Query execution restored after rollback

✅ Transaction test passed

📦 Test 3: Enhanced Parquet Data Loading & Advanced Data Types
--------------------------------------------------------------
🔍 Testing Parquet data loading and advanced data types...

📦 Basic Parquet Data Tests:
  ✓ all_types dataset count: 3 rows
  ✓ Basic data retrieval: 1 rows
  ✓ Full data access: 3 rows

🏗️ Data Type Compatibility Tests:
  ✓ pg_catalog.pg_type: 16 data types
  ✓ Enhanced types available: json, jsonb, uuid, interval, bytea
  ✓ Column data types detected: 14 types

✅ Enhanced Parquet test passed

🎉 All enhanced integration tests passed!
==========================================

📈 Test Summary:
  ✅ Enhanced CSV data loading with PostgreSQL compatibility
  ✅ Complete transaction support (BEGIN/COMMIT/ROLLBACK)  
  ✅ Enhanced Parquet data loading with advanced data types
  ✅ Array types and complex data type support
  ✅ Improved pg_catalog system tables
  ✅ PostgreSQL function compatibility

🚀 Ready for production PostgreSQL workloads!
```

## Tool Compatibility

These tests verify that datafusion-postgres works with PostgreSQL tools that expect:

- Standard system catalog tables (pg_catalog.*)
- PostgreSQL built-in functions
- information_schema views
- Proper error codes and messages
- Compatible data type handling

This allows tools like pgAdmin, DBeaver, and other PostgreSQL clients to work with datafusion-postgres.

## Test Data

- **`delhiclimate.csv`** - Sample CSV data with weather information (1462 rows)
- **`all_types.parquet`** - Parquet file with various Arrow data types for testing

## Notes

- Tests connect to port 5433 to avoid conflicts with existing PostgreSQL installations
- All tests use `autocommit = True` since datafusion-postgres doesn't support transactions yet
- Error handling tests verify proper PostgreSQL error codes are returned
- Type handling is flexible to accommodate differences between Arrow and PostgreSQL types
