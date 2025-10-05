#!/usr/bin/env python3
"""Enhanced test for CSV data loading, PostgreSQL compatibility, and new features."""

import psycopg

def main():
    print("🔍 Testing CSV data loading and PostgreSQL compatibility...")

    conn = psycopg.connect("host=127.0.0.1 port=5433 user=postgres dbname=public")
    conn.autocommit = True

    with conn.cursor() as cur:
        print("\n📊 Basic Data Access Tests:")
        test_basic_data_access(cur)

        print("\n🗂️ Enhanced pg_catalog Tests:")
        test_enhanced_pg_catalog(cur)

        print("\n🔧 PostgreSQL Functions Tests:")
        test_postgresql_functions(cur)

        print("\n📋 Table Type Detection Tests:")
        test_table_type_detection(cur)

        print("\n🔐 Transaction Integration Tests:")
        test_transaction_integration(cur)

    conn.close()
    print("\n✅ All enhanced CSV tests passed!")

def test_basic_data_access(cur):
    """Test basic data access and queries."""
    # Test basic count
    cur.execute("SELECT count(*) FROM delhi")
    results = cur.fetchone()
    assert results[0] == 1462
    print(f"  ✓ Delhi dataset count: {results[0]} rows")

    # Test basic query with limit
    cur.execute("SELECT * FROM delhi ORDER BY date LIMIT 10")
    results = cur.fetchall()
    assert len(results) == 10
    print(f"  ✓ Limited query: {len(results)} rows")

    # Test parameterized query
    cur.execute("SELECT date FROM delhi WHERE meantemp > %s ORDER BY date", [30])
    results = cur.fetchall()
    assert len(results) == 527
    assert len(results[0]) == 1
    print(f"  ✓ Parameterized query: {len(results)} rows where meantemp > 30")

def test_enhanced_pg_catalog(cur):
    """Test enhanced pg_catalog system tables."""
    # Test pg_type with comprehensive types
    cur.execute("SELECT count(*) FROM pg_catalog.pg_type")
    pg_type_count = cur.fetchone()[0]
    assert pg_type_count >= 16
    print(f"  ✓ pg_catalog.pg_type: {pg_type_count} data types")

    # Test specific data types exist
    cur.execute("SELECT typname FROM pg_catalog.pg_type WHERE typname IN ('bool', 'int4', 'text', 'float8', 'date') ORDER BY typname")
    types = [row[0] for row in cur.fetchall()]
    expected_types = ['bool', 'date', 'float8', 'int4', 'text']
    assert all(t in types for t in expected_types)
    print(f"  ✓ Core PostgreSQL types present: {', '.join(expected_types)}")

    # Test pg_class with proper table types
    cur.execute("SELECT relname, relkind FROM pg_catalog.pg_class WHERE relname = 'delhi'")
    result = cur.fetchone()
    assert result is not None
    assert result[1] == 'r'  # Should be regular table
    print(f"  ✓ Table type detection: {result[0]} = '{result[1]}' (regular table)")

    # Test pg_attribute has column information
    cur.execute("SELECT count(*) FROM pg_catalog.pg_attribute WHERE attnum > 0")
    attr_count = cur.fetchone()[0]
    assert attr_count > 0
    print(f"  ✓ pg_attribute: {attr_count} columns tracked")

def test_postgresql_functions(cur):
    """Test PostgreSQL compatibility functions."""
    # Test version function
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    assert "DataFusion" in version
    print(f"  ✓ version(): {version[:50]}...")

    # Test current_schema function
    cur.execute("SELECT current_schema()")
    schema = cur.fetchone()[0]
    assert schema == "public"
    print(f"  ✓ current_schema(): {schema}")

    # Test current_schemas function
    cur.execute("SELECT current_schemas(false)")
    schemas = cur.fetchone()[0]
    assert "public" in schemas
    print(f"  ✓ current_schemas(): {schemas}")

    # Test has_table_privilege function (2-parameter version)
    cur.execute("SELECT has_table_privilege('delhi', 'SELECT')")
    result = cur.fetchone()[0]
    assert isinstance(result, bool)
    print(f"  ✓ has_table_privilege(): {result}")

def test_table_type_detection(cur):
    """Test table type detection in pg_class."""
    cur.execute("""
        SELECT relname, relkind,
               CASE relkind
                   WHEN 'r' THEN 'regular table'
                   WHEN 'v' THEN 'view'
                   WHEN 'i' THEN 'index'
                   ELSE 'other'
               END as description
        FROM pg_catalog.pg_class
        ORDER BY relname
    """)
    results = cur.fetchall()

    # Should have multiple tables with proper types
    table_types = {}
    for name, kind, desc in results:
        table_types[name] = (kind, desc)

    # Delhi should be a regular table
    assert 'delhi' in table_types
    assert table_types['delhi'][0] == 'r'
    print(f"  ✓ Delhi table type: {table_types['delhi'][1]}")

    # System tables should also be regular tables
    system_tables = [name for name, (kind, _) in table_types.items() if name.startswith('pg_')]
    regular_system_tables = [name for name, (kind, _) in table_types.items() if name.startswith('pg_') and kind == 'r']
    print(f"  ✓ System tables: {len(system_tables)} total, {len(regular_system_tables)} regular tables")

def test_transaction_integration(cur):
    """Test transaction support with CSV data."""
    # Test transaction with data queries
    cur.execute("BEGIN")
    print("  ✓ Transaction started")

    # Execute multiple queries in transaction
    cur.execute("SELECT count(*) FROM delhi")
    count1 = cur.fetchone()[0]

    cur.execute("SELECT max(meantemp) FROM delhi")
    max_temp = cur.fetchone()[0]

    cur.execute("SELECT min(meantemp) FROM delhi")
    min_temp = cur.fetchone()[0]

    print(f"  ✓ Queries in transaction: {count1} rows, temp range {min_temp}-{max_temp}")

    # Commit transaction
    cur.execute("COMMIT")
    print("  ✓ Transaction committed successfully")

if __name__ == "__main__":
    main()
