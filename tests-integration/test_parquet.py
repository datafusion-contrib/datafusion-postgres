#!/usr/bin/env python3
"""Test for Parquet data loading with various data types."""

import psycopg

def main():
    print("🔍 Testing Parquet data loading and data types...")
    
    conn = psycopg.connect("host=127.0.0.1 port=5434 user=postgres dbname=public")
    conn.autocommit = True

    # Test basic count queries
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM all_types")
        results = cur.fetchone()
        assert results[0] == 3
        print(f"✓ all_types dataset count: {results[0]} rows")

        # Test basic data retrieval
        cur.execute("SELECT * FROM all_types LIMIT 1")
        results = cur.fetchall()
        print(f"✓ Basic data retrieval: {len(results)} rows")

        # Test PostgreSQL compatibility with parquet data
        cur.execute("SELECT count(*) FROM pg_catalog.pg_type")
        pg_type_count = cur.fetchone()[0]
        print(f"✓ pg_catalog.pg_type: {pg_type_count} data types")

        # Test column metadata
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'all_types'")
        columns = cur.fetchall()
        print(f"✓ all_types columns: {len(columns)} columns")

    conn.close()
    print("✅ Parquet tests passed!")

if __name__ == "__main__":
    main()
