#!/usr/bin/env python3
"""
Per-connection sessions, checked with two live connections.

Like in Postgres, session state (`SET`, prepared statements) is private to the
connection that created it, while catalog changes (`CREATE TABLE`, `INSERT`)
made through one connection are visible to the other.
"""

import psycopg

TIMESTAMPTZ_OID = 1184
TIMESTAMP_OID = 1114


def connect():
    conn = psycopg.connect("host=127.0.0.1 port=5433 user=postgres dbname=public")
    conn.autocommit = True
    return conn


def column_type(cur):
    """The type OID of the first column of the last result."""
    return cur.description[0].type_code


def scalar(cur, sql):
    cur.execute(sql)
    return cur.fetchone()[0]


def test_session_settings_stay_with_their_connection(alice, bob):
    assert scalar(bob, "SHOW time zone") == "UTC"

    alice.execute("SET TIME ZONE 'America/New_York'")
    assert scalar(alice, "SHOW time zone") == "America/New_York"
    assert scalar(bob, "SHOW time zone") == "UTC"

    # `now()` is `timestamptz` for the connection that picked a time zone and
    # stays `timestamp` for the other one.
    alice.execute("SELECT now()")
    bob.execute("SELECT now()")
    assert column_type(alice) == TIMESTAMPTZ_OID, column_type(alice)
    assert column_type(bob) == TIMESTAMP_OID, column_type(bob)
    assert scalar(alice, "SHOW time zone") == "America/New_York"
    assert scalar(bob, "SHOW time zone") == "UTC"
    print("  ✓ SET TIME ZONE only takes effect on its own connection")

    # Like in Postgres, an unknown zone is rejected and changes nothing ...
    try:
        scalar(alice, "SET TIME ZONE 'Mars/Olympus_Mons'")
    except psycopg.Error as error:
        assert "invalid value for parameter" in str(error), error
    else:
        raise AssertionError("an unknown time zone must be rejected")
    assert scalar(alice, "SHOW time zone") == "America/New_York"

    # ... and so does a reset, only on the connection that runs it.
    alice.execute("RESET timezone")
    assert scalar(alice, "SHOW time zone") == "UTC"
    alice.execute("SELECT now()")
    assert column_type(alice) == TIMESTAMP_OID, column_type(alice)
    assert scalar(bob, "SHOW time zone") == "UTC"
    print("  ✓ an unknown zone is rejected; RESET TIME ZONE only reaches its own connection")


def test_prepared_statements_stay_with_their_connection(alice, bob):
    alice.execute("PREPARE visit AS SELECT 1")
    assert scalar(alice, "EXECUTE visit") == 1

    try:
        scalar(bob, "EXECUTE visit")
    except psycopg.Error:
        pass
    else:
        raise AssertionError(
            "EXECUTE must not see another connection's prepared statement"
        )
    print("  ✓ PREPARE on one connection is not EXECUTE-able from another")


def test_catalog_changes_are_visible_to_every_connection(alice, bob):
    alice.execute("DROP TABLE IF EXISTS session_visits")
    alice.execute("CREATE TABLE session_visits (id INT)")
    alice.execute("INSERT INTO session_visits VALUES (1), (2)")
    bob.execute("INSERT INTO session_visits VALUES (3)")
    assert scalar(bob, "SELECT count(*) FROM session_visits") == 3
    alice.execute("DROP TABLE session_visits")
    print("  ✓ CREATE TABLE / INSERT through one connection are visible to the other")


def main():
    print("🔐 Testing per-connection sessions")
    print("=" * 50)

    alice, bob = connect(), connect()
    try:
        with alice.cursor() as a, bob.cursor() as b:
            print("\n📝 Test 1: session settings")
            test_session_settings_stay_with_their_connection(a, b)

            print("\n📝 Test 2: prepared statements")
            test_prepared_statements_stay_with_their_connection(a, b)

            print("\n📝 Test 3: catalog")
            test_catalog_changes_are_visible_to_every_connection(a, b)
    finally:
        alice.close()
        bob.close()

    print("\n✅ All per-connection session tests passed!")


if __name__ == "__main__":
    main()
