#!/usr/bin/env python3
"""Basic test for CSV data loading and PostgreSQL compatibility."""

import psycopg

def main():
    print("🔍 Testing CSV data loading and basic queries...")
    
    conn = psycopg.connect("host=127.0.0.1 port=5433 user=postgres dbname=public")
    conn.autocommit = True

    with conn.cursor() as cur:
        # Test basic count
        cur.execute("SELECT count(*) FROM delhi")
        results = cur.fetchone()
        assert results[0] == 1462
        print(f"✓ Delhi dataset count: {results[0]} rows")

        # Test basic query with limit
        cur.execute("SELECT * FROM delhi ORDER BY date LIMIT 10")
        results = cur.fetchall()
        assert len(results) == 10
        print(f"✓ Limited query: {len(results)} rows")

        # Test parameterized query
        cur.execute("SELECT date FROM delhi WHERE meantemp > %s ORDER BY date", [30])
        results = cur.fetchall()
        assert len(results) == 527
        assert len(results[0]) == 1
        print(f"✓ Parameterized query: {len(results)} rows where meantemp > 30")

        # Test pg_catalog functionality
        cur.execute("SELECT count(*) FROM pg_catalog.pg_type")
        pg_type_count = cur.fetchone()[0]
        print(f"✓ pg_catalog.pg_type: {pg_type_count} data types")

        # Test PostgreSQL function
        cur.execute("SELECT version()")
        version = cur.fetchone()[0]
        assert "DataFusion" in version
        print(f"✓ version(): {version}")

    conn.close()
    print("✅ All CSV tests passed!")

if __name__ == "__main__":
    main()
