# DataFusion PostgreSQL Integration Tests

This directory contains integration tests for PostgreSQL compatibility features in datafusion-postgres.

## Test Files

### Core Tests
- **`test_csv.py`** - CSV data loading and PostgreSQL compatibility test
- **`test_parquet.py`** - Parquet data loading and data types test

### Test Runner
- **`test.sh`** - Main test runner script that executes all tests

## Features Tested

### pg_catalog System Tables
- ✅ **pg_type** - PostgreSQL data types (16 core types: bool, int2, int4, int8, float4, float8, text, char, bytea, date, timestamp, time, interval, uuid, json, jsonb)
- ✅ **pg_class** - Table and relation metadata
- ✅ **pg_attribute** - Column information with proper attnum, atttypid, etc.
- ✅ **pg_proc** - Function metadata for PostgreSQL compatibility functions
- ✅ **pg_namespace** - Schema information (public, pg_catalog, information_schema)
- ✅ **pg_database** - Database metadata

### PostgreSQL Functions
- ✅ **version()** - Returns DataFusion PostgreSQL version string
- ✅ **current_schema()** - Returns current schema name
- ✅ **current_schemas(boolean)** - Returns schema search path
- ✅ **pg_get_userbyid(oid)** - Returns user name for given OID
- ✅ **has_table_privilege(user, table, privilege)** - Checks table privileges

### Data Type Support
- ✅ Enhanced parameter types in prepared statements (TIME, UUID, JSON, INTERVAL)
- ✅ Proper Arrow to PostgreSQL type mapping
- ✅ Fixed Time32/Time64 encoder issues
- ✅ Support for LargeUtf8, Decimal256, Duration array types

### Error Handling
- ✅ PostgreSQL-compatible error codes (e.g., "22003" for numeric_value_out_of_range)
- ✅ Proper error message formatting
- ✅ Graceful handling of invalid queries and data type conversions

### information_schema Compatibility
- ✅ **information_schema.tables** - Table metadata
- ✅ **information_schema.columns** - Column metadata
- ✅ Compatible with PostgreSQL tools expecting standard system catalogs

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

📊 Test 1: CSV Data Loading & PostgreSQL Compatibility
------------------------------------------------------
🔍 Testing CSV data loading and basic queries...
✓ Delhi dataset count: 1462 rows
✓ Limited query: 10 rows
✓ Parameterized query: 527 rows where meantemp > 30
✓ pg_catalog.pg_type: 16 data types
✓ version(): DataFusion PostgreSQL 48.0.0 on x86_64-pc-linux-gnu, compiled by Rust
✅ All CSV tests passed!
✅ CSV test passed

📦 Test 2: Parquet Data Loading & Data Types
--------------------------------------------
🔍 Testing Parquet data loading and data types...
✓ all_types dataset count: 3 rows
✓ Basic data retrieval: 1 rows
✓ pg_catalog.pg_type: 16 data types
✓ all_types columns: 14 columns
✅ Parquet tests passed!
✅ Parquet test passed

🎉 All integration tests passed!
=================================
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
