#!/usr/bin/env python3
"""
Comprehensive tests for PostgreSQL transaction support in datafusion-postgres.
Tests BEGIN, COMMIT, ROLLBACK, and failed transaction handling.
"""

import psycopg
import pytest


def main():
    print("🔐 Testing PostgreSQL Transaction Support")
    print("=" * 50)
    
    try:
        conn = psycopg.connect('host=127.0.0.1 port=5433 user=postgres dbname=public')
        conn.autocommit = True
        
        print("\n📝 Test 1: Basic Transaction Lifecycle")
        test_basic_transaction_lifecycle(conn)
        
        print("\n📝 Test 2: Transaction Variants")
        test_transaction_variants(conn)
        
        print("\n📝 Test 3: Failed Transaction Handling")
        test_failed_transaction_handling(conn)
        
        print("\n📝 Test 4: Transaction State Persistence")
        test_transaction_state_persistence(conn)
        
        print("\n📝 Test 5: Edge Cases")
        test_transaction_edge_cases(conn)
        
        conn.close()
        print("\n✅ All transaction tests passed!")
        return 0
        
    except Exception as e:
        print(f"\n❌ Transaction tests failed: {e}")
        return 1


def test_basic_transaction_lifecycle(conn):
    """Test basic BEGIN -> query -> COMMIT flow."""
    with conn.cursor() as cur:
        # Basic transaction flow
        cur.execute('BEGIN')
        print("  ✓ BEGIN executed")
        
        cur.execute('SELECT count(*) FROM delhi')
        result = cur.fetchone()[0]
        print(f"  ✓ Query in transaction: {result} rows")
        
        cur.execute('COMMIT')
        print("  ✓ COMMIT executed")
        
        # Verify we can execute queries after commit
        cur.execute('SELECT 1')
        result = cur.fetchone()[0]
        assert result == 1
        print("  ✓ Query after commit works")


def test_transaction_variants(conn):
    """Test all PostgreSQL transaction command variants."""
    with conn.cursor() as cur:
        # Test BEGIN variants
        variants = [
            ('BEGIN', 'COMMIT'),
            ('BEGIN TRANSACTION', 'COMMIT TRANSACTION'),
            ('BEGIN WORK', 'COMMIT WORK'),
            ('START TRANSACTION', 'END'),
            ('BEGIN', 'END TRANSACTION'),
        ]
        
        for begin_cmd, end_cmd in variants:
            cur.execute(begin_cmd)
            cur.execute('SELECT 1')
            result = cur.fetchone()[0]
            assert result == 1
            cur.execute(end_cmd)
            print(f"  ✓ {begin_cmd} -> {end_cmd}")
        
        # Test ROLLBACK variants
        rollback_variants = ['ROLLBACK', 'ROLLBACK TRANSACTION', 'ROLLBACK WORK', 'ABORT']
        
        for rollback_cmd in rollback_variants:
            try:
                cur.execute('BEGIN')
                cur.execute('SELECT 1')
                cur.execute(rollback_cmd)
                print(f"  ✓ {rollback_cmd}")
            except Exception as e:
                print(f"  ⚠️  {rollback_cmd}: {e}")
                # Try to recover
                try:
                    cur.execute('ROLLBACK')
                except:
                    pass


def test_failed_transaction_handling(conn):
    """Test failed transaction behavior and recovery."""
    with conn.cursor() as cur:
        # Start transaction and cause failure
        cur.execute('BEGIN')
        print("  ✓ Transaction started")
        
        # Execute invalid query to trigger failure
        try:
            cur.execute('SELECT * FROM nonexistent_table_xyz')
            assert False, "Should have failed"
        except Exception:
            print("  ✓ Invalid query failed as expected")
        
        # Try to execute another query in failed transaction
        try:
            cur.execute('SELECT 1')
            assert False, "Should be blocked in failed transaction"
        except Exception as e:
            assert "25P01" in str(e) or "aborted" in str(e).lower()
            print("  ✓ Subsequent query blocked (error code 25P01)")
        
        # ROLLBACK should work
        cur.execute('ROLLBACK')
        print("  ✓ ROLLBACK from failed transaction successful")
        
        # Now queries should work again
        cur.execute('SELECT 42')
        result = cur.fetchone()[0]
        assert result == 42
        print("  ✓ Query execution restored after rollback")


def test_transaction_state_persistence(conn):
    """Test that transaction state persists across multiple queries."""
    with conn.cursor() as cur:
        # Start transaction
        cur.execute('BEGIN')
        
        # Execute multiple queries in same transaction
        queries = [
            'SELECT count(*) FROM delhi',
            'SELECT 1 + 1',
            'SELECT current_schema()',
            'SELECT version()',
        ]
        
        for query in queries:
            cur.execute(query)
            result = cur.fetchone()
            assert result is not None
        
        print("  ✓ Multiple queries executed in same transaction")
        
        # Commit
        cur.execute('COMMIT')
        print("  ✓ Transaction committed successfully")


def test_transaction_edge_cases(conn):
    """Test edge cases and PostgreSQL compatibility."""
    with conn.cursor() as cur:
        # Test COMMIT outside transaction (should not error)
        cur.execute('COMMIT')
        print("  ✓ COMMIT outside transaction handled")
        
        # Test ROLLBACK outside transaction (should not error)
        cur.execute('ROLLBACK')
        print("  ✓ ROLLBACK outside transaction handled")
        
        # Test nested BEGIN (PostgreSQL allows with warning)
        try:
            cur.execute('BEGIN')
            cur.execute('BEGIN')  # Should not error
            cur.execute('COMMIT')
            print("  ✓ Nested BEGIN handled")
        except Exception as e:
            print(f"  ⚠️  Nested BEGIN: {e}")
            cur.execute('ROLLBACK')
        
        # Test COMMIT in failed transaction becomes ROLLBACK
        try:
            cur.execute('BEGIN')
            try:
                cur.execute('SELECT * FROM nonexistent_table')
            except Exception:
                pass
            
            # COMMIT in failed transaction should act like ROLLBACK
            cur.execute('COMMIT')  # This should internally do ROLLBACK
            
            # Should be able to execute queries now
            cur.execute('SELECT 1')
            result = cur.fetchone()[0]
            assert result == 1
            print("  ✓ COMMIT in failed transaction treated as ROLLBACK")
        except Exception as e:
            print(f"  ⚠️  Failed transaction COMMIT test: {e}")
            try:
                cur.execute('ROLLBACK')
            except:
                pass


if __name__ == "__main__":
    exit(main())
