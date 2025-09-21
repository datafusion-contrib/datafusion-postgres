#!/bin/bash

# Exit on error
set -e

# Configuration
CONTAINER_NAME="postgres-arrow-export"
DB_NAME="postgres"
DB_USER="postgres"
DB_PASSWORD="postgres"
EXPORT_DIR="./pg_catalog_arrow_exports"
POSTGRES_PORT="5432"

# Export mode: "static" or "all"
EXPORT_MODE="${1:-static}"

# Static tables whitelist - these contain only built-in PostgreSQL data
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

# Dynamic tables blacklist - these contain user/database-specific data
DYNAMIC_TABLES=(
    "pg_attribute"
    "pg_attrdef"
    "pg_auth_members"
    "pg_authid"
    "pg_class"
    "pg_constraint"
    "pg_database"
    "pg_db_role_setting"
    "pg_default_acl"
    "pg_depend"
    "pg_description"
    "pg_enum"
    "pg_event_trigger"
    "pg_extension"
    "pg_foreign_data_wrapper"
    "pg_foreign_server"
    "pg_foreign_table"
    "pg_index"
    "pg_inherits"
    "pg_init_privs"
    "pg_largeobject"
    "pg_largeobject_metadata"
    "pg_namespace"
    "pg_partitioned_table"
    "pg_policy"
    "pg_publication"
    "pg_publication_namespace"
    "pg_publication_rel"
    "pg_replication_origin"
    "pg_rewrite"
    "pg_seclabel"
    "pg_sequence"
    "pg_shdepend"
    "pg_shdescription"
    "pg_shseclabel"
    "pg_statistic"
    "pg_statistic_ext"
    "pg_statistic_ext_data"
    "pg_subscription"
    "pg_subscription_rel"
    "pg_tablespace"
    "pg_trigger"
    "pg_user_mapping"
)

echo "=== PostgreSQL pg_catalog to Arrow IPC Export Script ==="
echo "Export mode: $EXPORT_MODE"
if [ "$EXPORT_MODE" = "static" ]; then
    echo "Will export only static tables (built-in PostgreSQL data)"
else
    echo "Will export all tables (including user-specific data)"
fi
echo ""

# Clean up any existing container
echo "Cleaning up existing container if any..."
docker rm -f $CONTAINER_NAME 2>/dev/null || true

# Create export directory
echo "Creating export directory..."
mkdir -p "$EXPORT_DIR"

# Start PostgreSQL container
echo "Starting PostgreSQL container..."
docker run -d \
  --name $CONTAINER_NAME \
  -e POSTGRES_PASSWORD=$DB_PASSWORD \
  -e POSTGRES_USER=$DB_USER \
  -e POSTGRES_DB=$DB_NAME \
  -p $POSTGRES_PORT:5432 \
  postgres:17.6

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

# Install required tools in container
echo "Installing required tools in container..."
docker exec $CONTAINER_NAME apt-get update
docker exec $CONTAINER_NAME apt-get install -y python3 python3-pip python3-venv

# Create Python virtual environment and install dependencies
echo "Setting up Python environment..."
docker exec $CONTAINER_NAME python3 -m venv /opt/venv
docker exec $CONTAINER_NAME /opt/venv/bin/pip install psycopg2-binary pyarrow pandas numpy

# Pass table lists to Python script
if [ "$EXPORT_MODE" = "static" ]; then
    WHITELIST_STR=$(printf '"%s",' "${STATIC_TABLES[@]}")
    WHITELIST_STR="[${WHITELIST_STR%,}]"
    TABLE_FILTER="whitelist"
else
    WHITELIST_STR="[]"
    TABLE_FILTER="all"
fi

# Create Python script for exporting to Arrow IPC
echo "Creating export script..."
cat << 'EOF' > export_to_arrow.py
import psycopg2
import pyarrow as pa
import pyarrow.feather as feather
import pandas as pd
import os
import sys
from datetime import datetime, date, time
from decimal import Decimal
import json
import numpy as np

def pg_type_to_arrow_type(pg_type, is_nullable=True):
    """Map PostgreSQL types to Arrow types"""
    type_mapping = {
        'bigint': pa.int64(),
        'integer': pa.int32(),
        'smallint': pa.int16(),
        'numeric': pa.decimal128(38, 10),  # Max precision for decimal128
        'real': pa.float32(),
        'double precision': pa.float64(),
        'boolean': pa.bool_(),
        'text': pa.string(),
        'character varying': pa.string(),
        'character': pa.string(),
        'name': pa.string(),
        'oid': pa.int32(),  # OIDs are signed
        'regproc': pa.string(),
        'regtype': pa.string(),
        'regclass': pa.string(),
        'timestamp': pa.timestamp('us'),
        'timestamp without time zone': pa.timestamp('us'),
        'timestamp with time zone': pa.timestamp('us', tz='UTC'),
        'date': pa.date32(),
        'time': pa.time64('us'),
        'time without time zone': pa.time64('us'),
        'interval': pa.string(),  # Store as string for now
        'bytea': pa.binary(),
        'json': pa.string(),
        'jsonb': pa.string(),
        'uuid': pa.string(),
        'inet': pa.string(),
        'cidr': pa.string(),
        'macaddr': pa.string(),
        'money': pa.decimal128(19, 2),
        'char': pa.string(),
        'bpchar': pa.string(),
        'aclitem': pa.string(),
        'pg_node_tree': pa.string(),
        'pg_ndistinct': pa.string(),
        'pg_dependencies': pa.string(),
        'pg_mcv_list': pa.string(),
        'anyarray': pa.string(),  # Store as string representation
        'oidvector': pa.string(),  # Store as string
        'int2vector': pa.string(),  # Store as string
    }

    arrow_type = type_mapping.get(pg_type, pa.string())
    return arrow_type

def convert_value(value, arrow_type):
    """Convert PostgreSQL value to Arrow-compatible value"""
    if value is None:
        return None

    # Handle array types - convert to string representation
    if isinstance(value, (list, tuple)):
        return str(value)

    # Handle decimal types
    if isinstance(arrow_type, pa.lib.Decimal128Type) and isinstance(value, Decimal):
        return value

    # Handle datetime types
    if isinstance(value, datetime):
        return value
    elif isinstance(value, date):
        return value
    elif isinstance(value, time):
        # Convert time to microseconds since midnight
        return (value.hour * 3600 + value.minute * 60 + value.second) * 1000000 + value.microsecond

    # Handle bytea
    if isinstance(value, (bytes, memoryview)):
        return bytes(value)

    # Default: return as-is
    return value

def get_table_info(conn, schema_name, table_name):
    """Get detailed table information"""
    cur = conn.cursor()

    # Get table comment/description
    cur.execute("""
        SELECT obj_description(c.oid, 'pg_class') as table_comment
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
    """, (schema_name, table_name))

    result = cur.fetchone()
    table_comment = result[0] if result and result[0] else "No description available"

    # Get row count
    cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
    row_count = cur.fetchone()[0]

    # Get table size
    cur.execute("""
        SELECT pg_size_pretty(pg_total_relation_size(c.oid)) as size
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
    """, (schema_name, table_name))

    result = cur.fetchone()
    table_size = result[0] if result else "Unknown"

    cur.close()
    return table_comment, row_count, table_size

def export_table_to_arrow(conn, schema_name, table_name, output_dir):
    """Export a single table to Arrow IPC format"""
    try:
        cur = conn.cursor()

        # Get table information for display
        table_comment, row_count, table_size = get_table_info(conn, schema_name, table_name)

        # Determine if table is static or dynamic
        static_tables = {
            'pg_aggregate', 'pg_am', 'pg_amop', 'pg_amproc', 'pg_cast',
            'pg_collation', 'pg_conversion', 'pg_language', 'pg_opclass',
            'pg_operator', 'pg_opfamily', 'pg_proc', 'pg_range',
            'pg_ts_config', 'pg_ts_dict', 'pg_ts_parser', 'pg_ts_template', 'pg_type'
        }

        table_type = "STATIC (Built-in data)" if table_name in static_tables else "DYNAMIC (User/DB-specific data)"

        print(f"\n{'='*80}")
        print(f"Table: {schema_name}.{table_name}")
        print(f"Type: {table_type}")
        print(f"Description: {table_comment}")
        print(f"Row Count: {row_count:,}")
        print(f"Size: {table_size}")
        print(f"{'='*80}")

        # Get column information
        cur.execute("""
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                pgd.description as column_comment
            FROM information_schema.columns c
            LEFT JOIN pg_catalog.pg_description pgd ON
                pgd.objoid = (
                    SELECT oid FROM pg_class
                    WHERE relname = c.table_name AND relnamespace = (
                        SELECT oid FROM pg_namespace WHERE nspname = c.table_schema
                    )
                ) AND pgd.objsubid = c.ordinal_position
            WHERE c.table_schema = %s AND c.table_name = %s
            ORDER BY c.ordinal_position
        """, (schema_name, table_name))

        columns = cur.fetchall()
        if not columns:
            print(f"No columns found for {schema_name}.{table_name}")
            return False

        # Display column information
        print("\nColumns:")
        print(f"{'Column Name':<30} {'Type':<20} {'Nullable':<10} {'Description':<40}")
        print("-" * 100)

        # Build Arrow schema
        arrow_fields = []
        for col_name, data_type, is_nullable, default, comment in columns:
            nullable = "YES" if is_nullable == 'YES' else "NO"
            desc = (comment[:37] + '...') if comment and len(comment) > 40 else (comment or '')
            print(f"{col_name:<30} {data_type:<20} {nullable:<10} {desc:<40}")

            # Create Arrow field
            arrow_type = pg_type_to_arrow_type(data_type, is_nullable == 'YES')
            field = pa.field(col_name, arrow_type, nullable=(is_nullable == 'YES'))
            arrow_fields.append(field)

        arrow_schema = pa.schema(arrow_fields)

        # Fetch all data
        cur.execute(f"SELECT * FROM {schema_name}.{table_name}")

        # Process data in batches to handle large tables
        batch_size = 10000
        all_batches = []

        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break

            # Convert rows to columnar format
            columns_data = {}
            for i, field in enumerate(arrow_fields):
                col_values = []
                for row in rows:
                    value = convert_value(row[i], field.type)
                    col_values.append(value)

                # Create Arrow array
                try:
                    if isinstance(field.type, pa.lib.Decimal128Type):
                        # Special handling for decimal types
                        arr = pa.array(col_values, type=field.type)
                    else:
                        arr = pa.array(col_values, type=field.type)
                    columns_data[field.name] = arr
                except Exception as e:
                    print(f"Warning: Error converting column {field.name}: {e}")
                    # Fallback to string
                    str_values = [str(v) if v is not None else None for v in col_values]
                    columns_data[field.name] = pa.array(str_values, type=pa.string())

            # Create record batch
            batch = pa.RecordBatch.from_pydict(columns_data, schema=arrow_schema)
            all_batches.append(batch)

        if not all_batches:
            # Create empty table with schema
            table = pa.Table.from_batches([], schema=arrow_schema)
        else:
            # Combine all batches into a table
            table = pa.Table.from_batches(all_batches, schema=arrow_schema)

        # Also save as Feather format for compatibility
        feather_file = os.path.join(output_dir, f'{table_name}.feather')
        feather.write_feather(table, feather_file)
        print(f"Saved as Feather format: {feather_file}")

        cur.close()
        return True

    except Exception as e:
        print(f"\nError exporting {schema_name}.{table_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    # Database connection parameters
    conn_params = {
        'host': 'localhost',
        'database': os.environ.get('DB_NAME', 'postgres'),
        'user': os.environ.get('DB_USER', 'postgres'),
        'password': os.environ.get('DB_PASSWORD', 'postgres')
    }

    # Get whitelist from environment variable
    whitelist_str = os.environ.get('WHITELIST_TABLES', '[]')
    whitelist_tables = json.loads(whitelist_str)
    table_filter = os.environ.get('TABLE_FILTER', 'all')

    output_dir = '/exports'
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Connect to database
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        # Get all tables in pg_catalog schema
        cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'pg_catalog'
            ORDER BY tablename
        """)

        all_tables = [row[0] for row in cur.fetchall()]
        print(f"\nFound {len(all_tables)} tables in pg_catalog schema")

        # Filter tables based on mode
        if table_filter == 'whitelist' and whitelist_tables:
            tables = [t for t in all_tables if t in whitelist_tables]
            print(f"Filtering to {len(tables)} static tables only")
            print(f"Static tables: {', '.join(sorted(tables))}")
        else:
            tables = all_tables
            print(f"Exporting all {len(tables)} tables")

        print(f"\nWill export {len(tables)} tables")
        print("="*80)

        # Export each table
        success_count = 0
        failed_tables = []

        for i, table in enumerate(sorted(tables), 1):
            print(f"\n[{i}/{len(tables)}] Processing {table}...")

            if export_table_to_arrow(conn, 'pg_catalog', table, output_dir):
                success_count += 1
            else:
                failed_tables.append(table)

        # Summary
        print("\n" + "="*80)
        print("EXPORT SUMMARY")
        print("="*80)
        print(f"Export mode: {table_filter}")
        print(f"Total tables found: {len(all_tables)}")
        print(f"Tables selected for export: {len(tables)}")
        print(f"Successfully exported: {success_count}")
        print(f"Failed to export: {len(failed_tables)}")

        if failed_tables:
            print(f"\nFailed tables: {', '.join(failed_tables)}")

        print("\n" + "="*80)

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Database connection error: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()
EOF

# Copy the Python script to container
docker cp export_to_arrow.py $CONTAINER_NAME:/export_to_arrow.py

# Run the export script with appropriate filter
echo "Running export script..."
docker exec -e DB_NAME=$DB_NAME -e DB_USER=$DB_USER -e DB_PASSWORD=$DB_PASSWORD \
  -e WHITELIST_TABLES="$WHITELIST_STR" -e TABLE_FILTER="$TABLE_FILTER" \
  $CONTAINER_NAME /opt/venv/bin/python /export_to_arrow.py

# Copy exported files from container to host
echo "Copying exported files to host..."
docker cp $CONTAINER_NAME:/exports/. "$EXPORT_DIR/"

# Clean up
echo "Cleaning up..."
rm -f export_to_arrow.py
docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME

echo "=== Export completed! ==="
echo "Arrow IPC files are available in: $EXPORT_DIR"
echo ""
echo "Files exported: $(ls -1 "$EXPORT_DIR"/*.arrow 2>/dev/null | wc -l)"
echo "Total size: $(du -sh "$EXPORT_DIR" 2>/dev/null | cut -f1)"
echo ""
echo "To view the exported files:"
echo "ls -la $EXPORT_DIR/"
echo ""
echo "Usage:"
echo "  ./export_pg_catalog_arrow.sh         # Export only static tables (default)"
echo "  ./export_pg_catalog_arrow.sh all     # Export all tables"
