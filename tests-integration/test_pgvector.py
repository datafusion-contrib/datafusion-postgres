#!/usr/bin/env python3
"""
Integration tests for pgvector functionality.
Tests typical pgvector workflows against the datafusion-postgres server:
DDL with a `vector(n)` column, INSERT of pgvector literals, the distance
operators (`<->` L2, `<#>` negative inner product, `<=>` cosine distance),
and nearest-neighbour queries.
"""

import psycopg


def main():
    print("🧪 Testing pgvector Queries")
    print("=" * 50)

    conn = psycopg.connect("host=127.0.0.1 port=5438 user=postgres dbname=public")
    conn.autocommit = True

    with conn.cursor() as cur:
        print("\n📋 Test 1: Create table with vector(3) column")
        test_create_table(cur)

        print("\n📋 Test 2: INSERT vector literals")
        test_insert_literals(cur)

        print("\n📋 Test 3: Nearest-neighbour (ORDER BY <->)")
        test_nearest_neighbour(cur)

        print("\n📋 Test 4: Distance operators")
        test_distance_operators(cur)

    conn.close()
    print("\n✅ All pgvector tests passed!")


def test_create_table(cur):
    """The canonical pgvector DDL must succeed and the vector type be known."""
    cur.execute("DROP TABLE IF EXISTS items")
    cur.execute(
        "CREATE TABLE items (id int PRIMARY KEY, embedding vector(3))"
    )
    # The pgvector `vector` type must be registered in pg_catalog.pg_type so
    # drivers can introspect it.
    cur.execute(
        "SELECT count(*) FROM pg_catalog.pg_type WHERE typname = 'vector'"
    )
    count = cur.fetchone()[0]
    assert count >= 1, "expected a vector row in pg_catalog.pg_type"
    print("  ✓ CREATE TABLE with vector(3); vector type registered in pg_type")


def test_insert_literals(cur):
    """INSERT ... VALUES with pgvector bracket literals."""
    cur.execute(
        "INSERT INTO items (id, embedding) VALUES "
        "(1, '[1,2,3]'), (2, '[4,5,6]')"
    )
    cur.execute("SELECT count(*) FROM items")
    count = cur.fetchone()[0]
    assert count == 2, f"expected 2 rows, got {count}"
    print(f"  ✓ Inserted 2 vector rows (count = {count})")


def test_nearest_neighbour(cur):
    """ORDER BY embedding <-> '[..]' LIMIT n returns the closest rows."""
    cur.execute(
        "SELECT id FROM items ORDER BY embedding <-> '[1,2,3]' LIMIT 1"
    )
    nearest = cur.fetchone()[0]
    assert nearest == 1, f"expected id 1 to be nearest, got {nearest}"
    print(f"  ✓ Nearest neighbour to [1,2,3] is id {nearest}")


def test_distance_operators(cur):
    """The L2, negative inner product and cosine operators return numbers."""
    # <-> : L2 distance; [1,2,3] vs itself is 0
    cur.execute("SELECT embedding <-> '[1,2,3]' FROM items WHERE id = 1")
    l2 = cur.fetchone()[0]
    assert abs(l2 - 0.0) < 1e-6, f"expected L2 distance 0, got {l2}"
    print(f"  ✓ <-> L2 distance: {l2}")

    # <#>: negative inner product; [4,5,6] . [4,5,6] = 77 -> -77
    cur.execute("SELECT embedding <#> '[4,5,6]' FROM items WHERE id = 2")
    neg_ip = cur.fetchone()[0]
    assert abs(neg_ip - (-77.0)) < 1e-4, (
        f"expected negative inner product -77, got {neg_ip}"
    )
    print(f"  ✓ <#> negative inner product: {neg_ip}")

    # <=>: cosine distance; identical vectors -> 0
    cur.execute("SELECT embedding <=> '[1,2,3]' FROM items WHERE id = 1")
    cosine = cur.fetchone()[0]
    assert abs(cosine - 0.0) < 1e-6, f"expected cosine distance 0, got {cosine}"
    print(f"  ✓ <=> cosine distance: {cosine}")


if __name__ == "__main__":
    main()
