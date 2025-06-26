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
echo "📊 Test 1: CSV Data Loading & PostgreSQL Compatibility"
echo "------------------------------------------------------"
../target/debug/datafusion-postgres-cli -p 5433 --csv delhi:delhiclimate.csv &
CSV_PID=$!
sleep 3

if python3 test_csv.py; then
    echo "✅ CSV test passed"
else
    echo "❌ CSV test failed"
    kill -9 $CSV_PID 2>/dev/null || true
    exit 1
fi

kill -9 $CSV_PID 2>/dev/null || true
sleep 3

# Test 2: Parquet data loading and data types
echo ""
echo "📦 Test 2: Parquet Data Loading & Data Types"
echo "--------------------------------------------"
../target/debug/datafusion-postgres-cli -p 5434 --parquet all_types:all_types.parquet &
PARQUET_PID=$!
sleep 3

if python3 test_parquet.py; then
    echo "✅ Parquet test passed"
else
    echo "❌ Parquet test failed"
    kill -9 $PARQUET_PID 2>/dev/null || true
    exit 1
fi

kill -9 $PARQUET_PID 2>/dev/null || true

echo ""
echo "🎉 All integration tests passed!"
echo "================================="