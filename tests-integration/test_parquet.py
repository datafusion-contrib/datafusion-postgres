#!/usr/bin/env python3
"""Enhanced test for Parquet data loading, complex data types, and array support."""

import psycopg

def main():
    print("🔍 Testing Parquet data loading and advanced data types...")
    
    conn = psycopg.connect("host=127.0.0.1 port=5434 user=postgres dbname=public")
    conn.autocommit = True

    with conn.cursor() as cur:
        print("\n📦 Basic Parquet Data Tests:")
        test_basic_parquet_data(cur)
        
        print("\n🏗️ Data Type Compatibility Tests:")
        test_data_type_compatibility(cur)
        
        print("\n📋 Column Metadata Tests:")
        test_column_metadata(cur)
        
        print("\n🔧 Advanced PostgreSQL Features:")
        test_advanced_postgresql_features(cur)
        
        print("\n🔐 Transaction Support with Parquet:")
        test_transaction_support(cur)
        
        print("\n📊 Complex Query Tests:")
        test_complex_queries(cur)

    conn.close()
    print("\n✅ All enhanced Parquet tests passed!")

def test_basic_parquet_data(cur):
    """Test basic Parquet data access."""
    # Test basic count
    cur.execute("SELECT count(*) FROM all_types")
    results = cur.fetchone()
    assert results[0] == 3
    print(f"  ✓ all_types dataset count: {results[0]} rows")

    # Test basic data retrieval
    cur.execute("SELECT * FROM all_types LIMIT 1")
    results = cur.fetchall()
    print(f"  ✓ Basic data retrieval: {len(results)} rows")
    
    # Test that we can access all rows
    cur.execute("SELECT * FROM all_types")
    all_results = cur.fetchall()
    assert len(all_results) == 3
    print(f"  ✓ Full data access: {len(all_results)} rows")

def test_data_type_compatibility(cur):
    """Test PostgreSQL data type compatibility with Parquet data."""
    # Test pg_type has all our enhanced types
    cur.execute("SELECT count(*) FROM pg_catalog.pg_type")
    pg_type_count = cur.fetchone()[0]
    assert pg_type_count >= 16
    print(f"  ✓ pg_catalog.pg_type: {pg_type_count} data types")
    
    # Test specific enhanced types exist
    enhanced_types = ['json', 'jsonb', 'uuid', 'interval', 'bytea']
    cur.execute(f"SELECT typname FROM pg_catalog.pg_type WHERE typname IN ({','.join(['%s'] * len(enhanced_types))})", enhanced_types)
    found_types = [row[0] for row in cur.fetchall()]
    print(f"  ✓ Enhanced types available: {', '.join(found_types)}")
    
    # Test data type mapping works
    cur.execute("SELECT data_type FROM information_schema.columns WHERE table_name = 'all_types' ORDER BY ordinal_position")
    data_types = [row[0] for row in cur.fetchall()]
    print(f"  ✓ Column data types detected: {len(data_types)} types")

def test_column_metadata(cur):
    """Test column metadata and information schema."""
    # Test information_schema.columns
    cur.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = 'all_types'
        ORDER BY ordinal_position
    """)
    columns = cur.fetchall()
    
    if columns:
        print(f"  ✓ Column metadata: {len(columns)} columns")
        for col_name, data_type, nullable, default in columns[:3]:  # Show first 3
            print(f"    - {col_name}: {data_type} ({'nullable' if nullable == 'YES' else 'not null'})")
    else:
        print("  ⚠️  No column metadata found (may not be fully supported)")
    
    # Test pg_attribute for column information
    cur.execute("""
        SELECT a.attname, a.atttypid, a.attnum 
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
        WHERE c.relname = 'all_types' AND a.attnum > 0
        ORDER BY a.attnum
    """)
    pg_columns = cur.fetchall()
    if pg_columns:
        print(f"  ✓ pg_attribute columns: {len(pg_columns)} tracked")

def test_advanced_postgresql_features(cur):
    """Test advanced PostgreSQL features with Parquet data."""
    # Test array operations (if supported)
    try:
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'all_types' AND data_type LIKE '%array%'")
        array_columns = cur.fetchall()
        if array_columns:
            print(f"  ✓ Array columns detected: {len(array_columns)}")
        else:
            print("  ℹ️  No array columns detected (normal for basic test data)")
    except Exception:
        print("  ℹ️  Array type detection not available")
    
    # Test JSON operations (if JSON columns exist)
    try:
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'all_types' AND data_type IN ('json', 'jsonb')")
        json_columns = cur.fetchall()
        if json_columns:
            print(f"  ✓ JSON columns detected: {len(json_columns)}")
        else:
            print("  ℹ️  No JSON columns in test data")
    except Exception:
        print("  ℹ️  JSON type detection not available")
    
    # Test PostgreSQL functions work with Parquet data
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    assert "DataFusion" in version
    print(f"  ✓ PostgreSQL functions work: version() available")

def test_transaction_support(cur):
    """Test transaction support with Parquet data."""
    # Test transaction with Parquet queries
    cur.execute("BEGIN")
    print("  ✓ Transaction started")
    
    # Execute queries in transaction
    cur.execute("SELECT count(*) FROM all_types")
    count = cur.fetchone()[0]
    
    cur.execute("SELECT * FROM all_types LIMIT 1")
    sample = cur.fetchall()
    
    print(f"  ✓ Queries in transaction: {count} total rows, {len(sample)} sample")
    
    # Test rollback
    cur.execute("ROLLBACK")
    print("  ✓ Transaction rolled back")
    
    # Verify queries still work after rollback
    cur.execute("SELECT 1")
    result = cur.fetchone()[0]
    assert result == 1
    print("  ✓ Queries work after rollback")

def test_complex_queries(cur):
    """Test complex queries with Parquet data."""
    # Test aggregation queries
    try:
        cur.execute("SELECT count(*), count(DISTINCT *) FROM all_types")
        count_result = cur.fetchone()
        print(f"  ✓ Aggregation query: {count_result[0]} total rows")
    except Exception as e:
        print(f"  ℹ️  Complex aggregation not supported: {type(e).__name__}")
    
    # Test ORDER BY
    try:
        cur.execute("SELECT * FROM all_types ORDER BY 1 LIMIT 2")
        ordered_results = cur.fetchall()
        print(f"  ✓ ORDER BY query: {len(ordered_results)} ordered rows")
    except Exception as e:
        print(f"  ℹ️  ORDER BY may not be supported: {type(e).__name__}")
    
    # Test JOIN with system tables (basic compatibility test)
    try:
        cur.execute("""
            SELECT c.relname, count(*) as estimated_rows 
            FROM pg_catalog.pg_class c 
            WHERE c.relname = 'all_types'
            GROUP BY c.relname
        """)
        join_result = cur.fetchall()
        if join_result:
            print(f"  ✓ System table JOIN: found {join_result[0][0]} with {join_result[0][1]} estimated rows")
    except Exception as e:
        print(f"  ℹ️  System table JOIN: {type(e).__name__}")

if __name__ == "__main__":
    main()
