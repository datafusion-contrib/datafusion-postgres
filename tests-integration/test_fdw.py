#!/usr/bin/env python3
"""
Test postgres_fdw foreign data wrapper support.

Requires a running PostgreSQL instance accessible via PGHOST/PGPORT/PGUSER/PGDATABASE env vars,
or defaults to localhost:5432 with user postgres and database fdw_test.

The datafusion-postgres server should be running on port specified by DF_PORT (default 5433).
"""

import os
import sys
import psycopg


DF_PORT = os.environ.get("DF_PORT", "5433")
PG_HOST = os.environ.get("PGHOST", "127.0.0.1")
PG_PORT = os.environ.get("PGPORT", "5432")
PG_USER = os.environ.get("PGUSER", "postgres")
PG_DB = os.environ.get("PGDATABASE", "fdw_test")


def main():
    print("🌍 Testing Foreign Data Wrapper (postgres_fdw)")
    print("=" * 50)

    try:
        conn = psycopg.connect(
            f"host={PG_HOST} port={PG_PORT} user={PG_USER} dbname={PG_DB}",
            autocommit=True,
        )
        pg_version = conn.info.server_version
        print(f"  Connected to PostgreSQL {pg_version // 10000}.{pg_version % 10000 // 100}")

        setup_fdw(conn)
        test_basic_query(conn)
        test_aggregate_query(conn)
        test_multiple_rows(conn)
        test_order_by(conn)
        test_cursor_lifecycle(conn)
        cleanup_fdw(conn)

        conn.close()
        print("\n✅ All FDW tests passed!")
        return 0

    except Exception as e:
        print(f"\n❌ FDW tests failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


def setup_fdw(conn):
    """Set up the foreign data wrapper connecting to datafusion-postgres."""
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        print("  ✓ postgres_fdw extension installed")

        cur.execute("DROP SERVER IF EXISTS df_server CASCADE")
        cur.execute(f"""
            CREATE SERVER df_server
                FOREIGN DATA WRAPPER postgres_fdw
                OPTIONS (host 'host.containers.internal', port '{DF_PORT}', dbname 'postgres')
        """)
        print("  ✓ Foreign server df_server created")

        cur.execute(f"""
            CREATE USER MAPPING FOR current_user
                SERVER df_server
                OPTIONS (user 'postgres', password '')
        """)
        print("  ✓ User mapping created")

        cur.execute("""
            IMPORT FOREIGN SCHEMA public
                LIMIT TO (delhi)
                FROM SERVER df_server
                INTO public
        """)
        print("  ✓ Foreign table delhi imported")


def test_basic_query(conn):
    """Test basic SELECT through FDW."""
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM delhi")
        result = cur.fetchone()[0]
        assert result > 0, f"Expected rows in delhi, got {result}"
        print(f"  ✓ Basic query: {result} rows in delhi")


def test_aggregate_query(conn):
    """Test aggregate functions through FDW."""
    with conn.cursor() as cur:
        cur.execute("SELECT avg(meantemp), max(humidity) FROM delhi")
        row = cur.fetchone()
        assert row[0] is not None, "Expected non-null avg(meantemp)"
        assert row[1] is not None, "Expected non-null max(humidity)"
        print(f"  ✓ Aggregate query: avg(meantemp)={row[0]:.2f}, max(humidity)={row[1]}")


def test_multiple_rows(conn):
    """Test fetching multiple rows through FDW."""
    with conn.cursor() as cur:
        cur.execute("SELECT date, meantemp FROM delhi ORDER BY date LIMIT 5")
        rows = cur.fetchall()
        assert len(rows) == 5, f"Expected 5 rows, got {len(rows)}"
        print(f"  ✓ Multiple rows: fetched {len(rows)} rows")


def test_order_by(conn):
    """Test ORDER BY through FDW."""
    with conn.cursor() as cur:
        cur.execute("SELECT date, meantemp FROM delhi ORDER BY meantemp DESC LIMIT 3")
        rows = cur.fetchall()
        temps = [row[1] for row in rows]
        assert temps == sorted(temps, reverse=True), "Expected descending order"
        print(f"  ✓ ORDER BY: top 3 temps = {temps}")


def test_cursor_lifecycle(conn):
    """Test DECLARE/FETCH/CLOSE cursor through FDW."""
    with conn.cursor() as cur:
        cur.execute("BEGIN")
        cur.execute("DECLARE fdw_cur CURSOR FOR SELECT date, meantemp FROM delhi ORDER BY date")
        print("  ✓ DECLARE CURSOR")

        cur.execute("FETCH FORWARD 3 FROM fdw_cur")
        rows = cur.fetchall()
        assert len(rows) == 3, f"Expected 3 rows from FETCH, got {len(rows)}"
        print(f"  ✓ FETCH FORWARD 3: got {len(rows)} rows")

        cur.execute("FETCH NEXT FROM fdw_cur")
        row = cur.fetchone()
        assert row is not None, "Expected a row from FETCH NEXT"
        print("  ✓ FETCH NEXT: got 1 row")

        cur.execute("CLOSE fdw_cur")
        cur.execute("COMMIT")
        print("  ✓ CLOSE + COMMIT")


def cleanup_fdw(conn):
    """Clean up FDW objects."""
    with conn.cursor() as cur:
        cur.execute("DROP FOREIGN TABLE IF EXISTS delhi CASCADE")
        cur.execute("DROP USER MAPPING IF EXISTS FOR current_user SERVER df_server")
        cur.execute("DROP SERVER IF EXISTS df_server CASCADE")
        print("  ✓ FDW objects cleaned up")


if __name__ == "__main__":
    sys.exit(main())
