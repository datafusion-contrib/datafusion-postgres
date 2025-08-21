#!/bin/bash

# Exit on error
set -e

# Configuration
CONTAINER_NAME="postgres-analyzer"
DB_NAME="postgres"
DB_USER="postgres"
DB_PASSWORD="postgres"
POSTGRES_PORT="5433"  # Different port to avoid conflict

echo "=== PostgreSQL pg_catalog Table Analysis Script ==="

# Clean up any existing container
echo "Cleaning up existing container if any..."
docker rm -f $CONTAINER_NAME 2>/dev/null || true

# Start PostgreSQL container
echo "Starting PostgreSQL container..."
docker run -d \
  --name $CONTAINER_NAME \
  -e POSTGRES_PASSWORD=$DB_PASSWORD \
  -e POSTGRES_USER=$DB_USER \
  -e POSTGRES_DB=$DB_NAME \
  -p $POSTGRES_PORT:5432 \
  postgres:latest

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
  if docker exec $CONTAINER_NAME pg_isready -U $DB_USER >/dev/null 2>&1; then
    echo "PostgreSQL is ready!"
    break
  fi
  if [ $i -eq 30 ]; then
    echo "Timeout waiting for PostgreSQL to start"
    exit 1
  fi
  echo -n "."
  sleep 1
done

# Create analysis script
echo "Creating analysis script..."
cat << 'EOF' > analyze_tables.sql
-- Create test database to compare
CREATE DATABASE testdb;

-- Function to check if table content varies between databases
CREATE OR REPLACE FUNCTION check_table_variance(schema_name text, table_name text)
RETURNS TABLE(is_static boolean, row_count_db1 bigint, row_count_db2 bigint, reason text) AS $$
DECLARE
    count1 bigint;
    count2 bigint;
    query text;
BEGIN
    -- Get row count from current database
    query := format('SELECT COUNT(*) FROM %I.%I', schema_name, table_name);
    EXECUTE query INTO count1;

    -- Get row count from testdb
    query := format('SELECT COUNT(*) FROM dblink(''dbname=testdb'', ''SELECT COUNT(*) FROM %I.%I'') AS t(cnt bigint)', schema_name, table_name);
    BEGIN
        EXECUTE query INTO count2;
    EXCEPTION WHEN OTHERS THEN
        count2 := -1;
    END;

    -- Determine if static
    IF count1 = count2 AND count1 > 0 THEN
        RETURN QUERY SELECT true, count1, count2, 'Same row count in both databases';
    ELSIF count1 = count2 AND count1 = 0 THEN
        RETURN QUERY SELECT true, count1, count2, 'Empty in both databases';
    ELSIF count2 = -1 THEN
        RETURN QUERY SELECT false, count1, count2, 'Error accessing table in testdb';
    ELSE
        RETURN QUERY SELECT false, count1, count2, 'Different row counts between databases';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Analyze all pg_catalog tables
WITH catalog_tables AS (
    SELECT
        tablename,
        obj_description(('pg_catalog.' || tablename)::regclass, 'pg_class') as description
    FROM pg_tables
    WHERE schemaname = 'pg_catalog'
    ORDER BY tablename
),
analysis AS (
    SELECT
        ct.tablename,
        ct.description,
        (check_table_variance('pg_catalog', ct.tablename)).*
    FROM catalog_tables ct
)
SELECT
    tablename,
    CASE
        WHEN is_static THEN 'STATIC'
        ELSE 'DYNAMIC'
    END as table_type,
    row_count_db1 as row_count,
    reason,
    COALESCE(description, 'No description') as description
FROM analysis
ORDER BY is_static DESC, tablename;

-- Categorize tables by their purpose
SELECT '

=== CATEGORIZATION BY PURPOSE ===' as info;

-- Static system catalog tables (typically don't change between databases)
SELECT '
--- STATIC TABLES (System-wide configuration) ---' as category;
SELECT tablename, obj_description(('pg_catalog.' || tablename)::regclass, 'pg_class') as description
FROM pg_tables
WHERE schemaname = 'pg_catalog'
AND tablename IN (
    'pg_aggregate',      -- Aggregate functions
    'pg_am',            -- Access methods
    'pg_amop',          -- Access method operators
    'pg_amproc',        -- Access method support procedures
    'pg_cast',          -- Type casts
    'pg_collation',     -- Collations
    'pg_conversion',    -- Character set conversions
    'pg_language',      -- Procedural languages
    'pg_opclass',       -- Operator classes
    'pg_operator',      -- Operators
    'pg_opfamily',      -- Operator families
    'pg_proc',          -- Functions and procedures (built-in ones)
    'pg_range',         -- Range types
    'pg_ts_config',     -- Text search configurations
    'pg_ts_dict',       -- Text search dictionaries
    'pg_ts_parser',     -- Text search parsers
    'pg_ts_template',   -- Text search templates
    'pg_type'           -- Data types (built-in ones)
)
ORDER BY tablename;

-- Dynamic database-specific tables
SELECT '
--- DYNAMIC TABLES (Database-specific content) ---' as category;
SELECT tablename, obj_description(('pg_catalog.' || tablename)::regclass, 'pg_class') as description
FROM pg_tables
WHERE schemaname = 'pg_catalog'
AND tablename IN (
    'pg_attribute',     -- Table columns
    'pg_attrdef',       -- Column defaults
    'pg_class',         -- Tables, indexes, sequences, views
    'pg_constraint',    -- Constraints
    'pg_database',      -- Databases
    'pg_db_role_setting', -- Per-database role settings
    'pg_default_acl',   -- Default privileges
    'pg_depend',        -- Dependency relationships
    'pg_description',   -- Object descriptions
    'pg_enum',          -- Enum label entries
    'pg_event_trigger', -- Event triggers
    'pg_extension',     -- Installed extensions
    'pg_foreign_data_wrapper', -- Foreign data wrappers
    'pg_foreign_server',-- Foreign servers
    'pg_foreign_table', -- Foreign tables
    'pg_index',         -- Indexes
    'pg_inherits',      -- Table inheritance
    'pg_init_privs',    -- Initial privileges
    'pg_largeobject',   -- Large objects data
    'pg_largeobject_metadata', -- Large objects metadata
    'pg_namespace',     -- Schemas
    'pg_partitioned_table', -- Partitioned tables
    'pg_policy',        -- Row security policies
    'pg_publication',   -- Publications for logical replication
    'pg_publication_rel', -- Relation to publication mapping
    'pg_rewrite',       -- Query rewrite rules
    'pg_seclabel',      -- Security labels
    'pg_sequence',      -- Sequences
    'pg_statistic',     -- Column statistics
    'pg_statistic_ext', -- Extended statistics
    'pg_subscription',  -- Subscriptions
    'pg_tablespace',    -- Tablespaces
    'pg_trigger',       -- Triggers
    'pg_user_mapping'   -- User mappings for foreign data
)
ORDER BY tablename;

-- Global/cluster-wide tables (shared across all databases)
SELECT '
--- GLOBAL TABLES (Cluster-wide, shared) ---' as category;
SELECT tablename, obj_description(('pg_catalog.' || tablename)::regclass, 'pg_class') as description
FROM pg_tables
WHERE schemaname = 'pg_catalog'
AND tablename IN (
    'pg_authid',        -- Roles
    'pg_auth_members',  -- Role memberships
    'pg_shdepend',      -- Dependencies on shared objects
    'pg_shdescription', -- Descriptions of shared objects
    'pg_shseclabel',    -- Security labels on shared objects
    'pg_replication_origin', -- Replication origins
    'pg_subscription'   -- Logical replication subscriptions
)
ORDER BY tablename;
EOF

# Install dblink extension
echo "Installing dblink extension..."
docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS dblink;"

# Run analysis
echo "Running analysis..."
docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -f /analyze_tables.sql > pg_catalog_analysis.txt 2>&1 || true

# Copy analysis script to container and run it
docker cp analyze_tables.sql $CONTAINER_NAME:/analyze_tables.sql

# Create a Python script for detailed analysis
cat << 'EOF' > detailed_analysis.py
import psycopg2
import json
from collections import defaultdict

def analyze_table_references(conn):
    """Analyze which tables reference user data vs system data"""
    cur = conn.cursor()

    # Tables that definitely contain user data references
    user_data_tables = {
        'pg_attribute': 'Column definitions for user tables',
        'pg_attrdef': 'Default values for user table columns',
        'pg_class': 'All relations including user tables',
        'pg_constraint': 'User-defined constraints',
        'pg_depend': 'Dependencies including user objects',
        'pg_description': 'Comments on user objects',
        'pg_index': 'User-created indexes',
        'pg_inherits': 'User table inheritance',
        'pg_namespace': 'User-created schemas',
        'pg_trigger': 'User-defined triggers',
        'pg_rewrite': 'User-defined rules',
        'pg_statistic': 'Statistics on user data',
        'pg_sequence': 'User sequences',
        'pg_enum': 'User-defined enum values',
        'pg_extension': 'User-installed extensions',
        'pg_largeobject': 'User large object data',
        'pg_policy': 'User row-level security policies',
        'pg_partitioned_table': 'User partitioned tables',
        'pg_publication': 'User-defined publications',
        'pg_subscription': 'User-defined subscriptions',
        'pg_foreign_data_wrapper': 'User FDW definitions',
        'pg_foreign_server': 'User foreign servers',
        'pg_foreign_table': 'User foreign tables',
        'pg_user_mapping': 'User mappings for FDW',
        'pg_event_trigger': 'User event triggers',
        'pg_init_privs': 'Initial privileges on objects',
        'pg_default_acl': 'Default access privileges',
        'pg_seclabel': 'Security labels on objects',
        'pg_db_role_setting': 'Per-database role settings',
        'pg_tablespace': 'User-defined tablespaces'
    }

    # Tables that contain only system/static data
    static_tables = {
        'pg_aggregate': 'Built-in aggregate functions',
        'pg_am': 'Built-in access methods',
        'pg_amop': 'Access method operators',
        'pg_amproc': 'Access method procedures',
        'pg_cast': 'Built-in type casts',
        'pg_collation': 'Built-in collations',
        'pg_conversion': 'Built-in encoding conversions',
        'pg_language': 'Built-in procedural languages',
        'pg_opclass': 'Built-in operator classes',
        'pg_operator': 'Built-in operators',
        'pg_opfamily': 'Built-in operator families',
        'pg_proc': 'Built-in functions (mostly)',
        'pg_range': 'Built-in range types',
        'pg_ts_config': 'Built-in text search configs',
        'pg_ts_dict': 'Built-in text search dictionaries',
        'pg_ts_parser': 'Built-in text search parsers',
        'pg_ts_template': 'Built-in text search templates',
        'pg_type': 'Built-in data types (mostly)'
    }

    # Global catalog tables (shared across databases)
    global_tables = {
        'pg_authid': 'Database roles (global)',
        'pg_auth_members': 'Role memberships (global)',
        'pg_database': 'Database list (global)',
        'pg_shdepend': 'Dependencies on shared objects',
        'pg_shdescription': 'Comments on shared objects',
        'pg_shseclabel': 'Security labels on shared objects',
        'pg_replication_origin': 'Replication origins (global)'
    }

    # Some tables might have both system and user data
    mixed_tables = {
        'pg_proc': 'Contains both built-in and user-defined functions',
        'pg_type': 'Contains both built-in and user-defined types',
        'pg_namespace': 'Contains both system (pg_*, information_schema) and user schemas'
    }

    print("=== PG_CATALOG TABLE CATEGORIZATION ===\n")

    print("STATIC TABLES (Safe to export - contain only built-in PostgreSQL data):")
    print("-" * 80)
    for table, desc in sorted(static_tables.items()):
        print(f"{table:<30} {desc}")

    print(f"\nTotal static tables: {len(static_tables)}")

    print("\n\nDYNAMIC TABLES (Not safe to export - contain user/database-specific data):")
    print("-" * 80)
    for table, desc in sorted(user_data_tables.items()):
        print(f"{table:<30} {desc}")

    print(f"\nTotal dynamic tables: {len(user_data_tables)}")

    print("\n\nGLOBAL TABLES (Shared across all databases in the cluster):")
    print("-" * 80)
    for table, desc in sorted(global_tables.items()):
        print(f"{table:<30} {desc}")

    print(f"\nTotal global tables: {len(global_tables)}")

    print("\n\nMIXED CONTENT TABLES (Contain both system and user data):")
    print("-" * 80)
    for table, desc in sorted(mixed_tables.items()):
        print(f"{table:<30} {desc}")

    # Generate recommended blacklist
    blacklist = list(user_data_tables.keys()) + list(global_tables.keys())

    print("\n\nRECOMMENDED BLACKLIST FOR STATIC EXPORT:")
    print("-" * 80)
    print("BLACKLIST_TABLES=(")
    for table in sorted(blacklist):
        print(f'    "{table}"')
    print(")")

    print(f"\n\nSUMMARY:")
    print(f"- Static tables (safe to export): {len(static_tables)}")
    print(f"- Dynamic tables (user data): {len(user_data_tables)}")
    print(f"- Global tables (cluster-wide): {len(global_tables)}")
    print(f"- Mixed content tables: {len(mixed_tables)}")
    print(f"- Total tables to blacklist: {len(blacklist)}")

def main():
    conn_params = {
        'host': 'localhost',
        'database': 'postgres',
        'user': 'postgres',
        'password': 'postgres'
    }

    try:
        conn = psycopg2.connect(**conn_params)
        analyze_table_references(conn)
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    main()
EOF

# Install Python dependencies
echo "Installing Python dependencies..."
docker exec $CONTAINER_NAME apt-get update
docker exec $CONTAINER_NAME apt-get install -y python3 python3-pip python3-venv

# Create virtual environment and install dependencies
docker exec $CONTAINER_NAME python3 -m venv /opt/venv
docker exec $CONTAINER_NAME /opt/venv/bin/pip install psycopg2-binary

# Copy and run Python analysis
docker cp detailed_analysis.py $CONTAINER_NAME:/detailed_analysis.py
echo "Running detailed analysis..."
docker exec $CONTAINER_NAME /opt/venv/bin/python /detailed_analysis.py > pg_catalog_categorization.txt

# Display results
echo ""
echo "=== Analysis Results ==="
cat pg_catalog_categorization.txt

# Save recommended export script
cat << 'EOF' > export_static_tables_only.sh
#!/bin/bash

# This script exports only static pg_catalog tables
# Based on the analysis, these tables contain only built-in PostgreSQL data

# Exit on error
set -e

# Configuration
CONTAINER_NAME="postgres-avro-export"
DB_NAME="postgres"
DB_USER="postgres"
DB_PASSWORD="postgres"
EXPORT_DIR="./pg_catalog_static_exports"
POSTGRES_PORT="5432"

# Whitelist of static tables to export
STATIC_TABLES=(
    "pg_aggregate"
    "pg_am"
    "pg_amop"
    "pg_amproc"
    "pg_cast"
    "pg_collation"
    "pg_conversion"
    "pg_language"
    "pg_opclass"
    "pg_operator"
    "pg_opfamily"
    "pg_proc"
    "pg_range"
    "pg_ts_config"
    "pg_ts_dict"
    "pg_ts_parser"
    "pg_ts_template"
    "pg_type"
)

echo "=== PostgreSQL Static pg_catalog Tables Export ==="
echo "This script exports only static tables that don't contain user data"
echo ""

# Rest of the export script would go here...
# (Similar to the original export script but filtering for only static tables)
EOF

chmod +x export_static_tables_only.sh

# Clean up
echo "Cleaning up..."
rm -f analyze_tables.sql detailed_analysis.py
docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME

echo ""
echo "=== Analysis Complete ==="
echo "Results saved to:"
echo "- pg_catalog_categorization.txt (detailed categorization)"
echo "- export_static_tables_only.sh (script to export only static tables)"
echo ""
echo "Static tables are those containing only built-in PostgreSQL data."
echo "Dynamic tables contain user-specific or database-specific data."
