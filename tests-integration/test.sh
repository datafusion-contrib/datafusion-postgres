#!/usr/bin/env bash

set -e

cargo build

# Activate virtual environment if it exists
if [ -d "test_env" ]; then
    source test_env/bin/activate
fi

./target/debug/datafusion-postgres-cli -p 5433 --csv delhi:tests-integration/delhiclimate.csv &
PID=$!
sleep 5
python3 tests-integration/test_fixed.py
kill -9 $PID 2>/dev/null

./target/debug/datafusion-postgres-cli -p 5433 --parquet all_types:tests-integration/all_types.parquet &
PID=$!
sleep 5
python3 tests-integration/test_all_types_fixed.py
kill -9 $PID 2>/dev/null