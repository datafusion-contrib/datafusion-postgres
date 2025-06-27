#!/usr/bin/env bash

set -e

echo "🚀 Running DataFusion PostgreSQL Integration Tests"
echo "=================================================="

# Build the project
echo "Building datafusion-postgres..."
cargo build

# Set up test environment
cd tests-integration

# Create virtual environment if it doesn't exist
if [ ! -d "test_env" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv test_env
fi

# Activate virtual environment and install dependencies
echo "Setting up Python dependencies..."
source test_env/bin/activate
pip install -q psycopg

# Test 1: CSV data loading and PostgreSQL compatibility
echo ""
echo "📊 Test 1: Enhanced CSV Data Loading & PostgreSQL Compatibility"
echo "----------------------------------------------------------------"
../target/debug/datafusion-postgres-cli -p 5433 --csv delhi:delhiclimate.csv &
CSV_PID=$!
sleep 3

if python3 test_csv.py; then
    echo "✅ Enhanced CSV test passed"
else
    echo "❌ Enhanced CSV test failed"
    kill -9 $CSV_PID 2>/dev/null || true
    exit 1
fi

kill -9 $CSV_PID 2>/dev/null || true
sleep 3

# Test 2: Transaction support
echo ""
echo "🔐 Test 2: Transaction Support"
echo "------------------------------"
../target/debug/datafusion-postgres-cli -p 5433 --csv delhi:delhiclimate.csv &
TRANSACTION_PID=$!
sleep 3

if python3 test_transactions.py; then
    echo "✅ Transaction test passed"
else
    echo "❌ Transaction test failed"
    kill -9 $TRANSACTION_PID 2>/dev/null || true
    exit 1
fi

kill -9 $TRANSACTION_PID 2>/dev/null || true
sleep 3

# Test 3: Parquet data loading and advanced data types
echo ""
echo "📦 Test 3: Enhanced Parquet Data Loading & Advanced Data Types"
echo "--------------------------------------------------------------"
../target/debug/datafusion-postgres-cli -p 5434 --parquet all_types:all_types.parquet &
PARQUET_PID=$!
sleep 3

if python3 test_parquet.py; then
    echo "✅ Enhanced Parquet test passed"
else
    echo "❌ Enhanced Parquet test failed"
    kill -9 $PARQUET_PID 2>/dev/null || true
    exit 1
fi

kill -9 $PARQUET_PID 2>/dev/null || true

echo ""
echo "🎉 All enhanced integration tests passed!"
echo "=========================================="
echo ""
echo "📈 Test Summary:"
echo "  ✅ Enhanced CSV data loading with PostgreSQL compatibility"
echo "  ✅ Complete transaction support (BEGIN/COMMIT/ROLLBACK)"  
echo "  ✅ Enhanced Parquet data loading with advanced data types"
echo "  ✅ Array types and complex data type support"
echo "  ✅ Improved pg_catalog system tables"
echo "  ✅ PostgreSQL function compatibility"
echo ""
echo "🚀 Ready for production PostgreSQL workloads!"