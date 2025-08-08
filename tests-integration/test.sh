#!/usr/bin/env bash

set -e

# Function to cleanup processes
cleanup() {
    echo "🧹 Cleaning up processes..."
    for pid in $CSV_PID $TRANSACTION_PID $PARQUET_PID $RBAC_PID $SSL_PID; do
        if [ ! -z "$pid" ]; then
            kill -9 $pid 2>/dev/null || true
        fi
    done
    
    # Clean up any remaining custom auth test processes
    pkill -f "custom_auth_server" 2>/dev/null || true
    pkill -f "custom-auth-test-server" 2>/dev/null || true
}

# Trap to cleanup on exit
trap cleanup EXIT

# Function to wait for port to be available
wait_for_port() {
    local port=$1
    local timeout=30
    local count=0
    
    # Use netstat as fallback if lsof is not available
    while (lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1) || (netstat -ln 2>/dev/null | grep ":$port " >/dev/null 2>&1); do
        if [ $count -ge $timeout ]; then
            echo "❌ Port $port still in use after ${timeout}s timeout"
            exit 1
        fi
        sleep 1
        count=$((count + 1))
    done
}

echo "🚀 Running DataFusion PostgreSQL Integration Tests"
echo "=================================================="

# Build the project
echo "Building datafusion-postgres..."
cd ..
cargo build
cd tests-integration

# Set up test environment

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
wait_for_port 5433
../target/debug/datafusion-postgres-cli -p 5433 --csv delhi:delhiclimate.csv &
CSV_PID=$!
sleep 5

# Check if server is actually running
if ! ps -p $CSV_PID > /dev/null 2>&1; then
    echo "❌ Server failed to start"
    exit 1
fi

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
wait_for_port 5433
../target/debug/datafusion-postgres-cli -p 5433 --csv delhi:delhiclimate.csv &
TRANSACTION_PID=$!
sleep 5

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
wait_for_port 5434
../target/debug/datafusion-postgres-cli -p 5434 --parquet all_types:all_types.parquet &
PARQUET_PID=$!
sleep 5

if python3 test_parquet.py; then
    echo "✅ Enhanced Parquet test passed"
else
    echo "❌ Enhanced Parquet test failed"
    kill -9 $PARQUET_PID 2>/dev/null || true
    exit 1
fi

kill -9 $PARQUET_PID 2>/dev/null || true
sleep 3

# Test 4: Role-Based Access Control
echo ""
echo "🔐 Test 4: Role-Based Access Control (RBAC)"
echo "--------------------------------------------"
wait_for_port 5435
../target/debug/datafusion-postgres-cli -p 5435 --csv delhi:delhiclimate.csv &
RBAC_PID=$!
sleep 5

# Check if server is actually running
if ! ps -p $RBAC_PID > /dev/null 2>&1; then
    echo "❌ RBAC server failed to start"
    exit 1
fi

if python3 test_rbac.py; then
    echo "✅ RBAC test passed"
else
    echo "❌ RBAC test failed"
    kill -9 $RBAC_PID 2>/dev/null || true
    exit 1
fi

kill -9 $RBAC_PID 2>/dev/null || true
sleep 3

# Test 5: SSL/TLS Security
echo ""
echo "🔒 Test 5: SSL/TLS Security Features"
echo "------------------------------------"
wait_for_port 5436
../target/debug/datafusion-postgres-cli -p 5436 --csv delhi:delhiclimate.csv &
SSL_PID=$!
sleep 5

# Check if server is actually running
if ! ps -p $SSL_PID > /dev/null 2>&1; then
    echo "❌ SSL server failed to start"
    exit 1
fi

if python3 test_ssl.py; then
    echo "✅ SSL/TLS test passed"
else
    echo "❌ SSL/TLS test failed"
    kill -9 $SSL_PID 2>/dev/null || true
    exit 1
fi

kill -9 $SSL_PID 2>/dev/null || true
sleep 3

# Test 6: Custom Authentication Manager
echo ""
echo "🔑 Test 6: Custom Authentication Manager"
echo "---------------------------------------"

if python3 test_custom_auth_simple.py; then
    echo "✅ Custom auth manager test passed"
else
    echo "❌ Custom auth manager test failed"
    exit 1
fi

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
echo "  ✅ Role-based access control (RBAC)"
echo "  ✅ SSL/TLS encryption support"
echo "  ✅ Custom authentication manager integration"
echo ""
echo "🚀 Ready for secure production PostgreSQL workloads with custom auth!"