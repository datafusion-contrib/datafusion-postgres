#!/usr/bin/env python3
"""
Export PostgreSQL query results to Arrow IPC Feather format.
Minimal dependencies: psycopg2, pyarrow
"""

import argparse
import psycopg2
import pyarrow as pa
import pyarrow.feather as feather
from psycopg2.extras import RealDictCursor
from typing import Dict, Any, List, Optional
import sys

def map_postgresql_to_arrow_type(type_oid: int) -> pa.DataType:
    """Map PostgreSQL data types to Arrow data types."""
    # Map OIDs to Arrow types
    type_mapping = {
        # Integer types (OIDs from PostgreSQL documentation)
        20: pa.int64(),    # int8 (bigint)
        21: pa.int16(),    # int2 (smallint)
        23: pa.int32(),    # int4 (integer)
        26: pa.int32(),    # oid

        # Floating point types
        700: pa.float32(),  # float4 (real)
        701: pa.float64(),  # float8 (double precision)
        1700: pa.float64(), # numeric (decimal)

        # Boolean
        16: pa.bool_(),     # bool

        # String types
        25: pa.string(),    # text
        1043: pa.string(),  # varchar
        18: pa.string(),    # char
        19: pa.string(),    # name

        # Date/time types
        1082: pa.date32(),           # date
        1114: pa.timestamp('us'),    # timestamp without time zone
        1184: pa.timestamp('us', tz='UTC'),  # timestamp with time zone
        1083: pa.time64('us'),       # time without time zone
        1266: pa.time64('us'),       # time with time zone

        # Binary data
        17: pa.binary(),    # bytea

        # JSON types
        114: pa.string(),   # json
        3802: pa.string(),  # jsonb

        # UUID
        2950: pa.string(),  # uuid (Arrow doesn't have native UUID type)

        # Network types
        869: pa.string(),   # inet
        650: pa.string(),   # cidr
        829: pa.string(),   # macaddr
    }

    return type_mapping.get(type_oid, pa.string())  # Fallback to string

def export_query_to_feather(
    connection_string: str,
    query: str,
    output_file: str,
    batch_size: int = 10000
) -> None:
    """Execute PostgreSQL query and export results to Arrow Feather format."""

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Execute query
        cursor.execute(query)

        # Get column information
        columns = []
        arrow_types = []
        column_names = []

        for desc in cursor.description:
            col_name = desc.name
            col_oid = desc.type_code

            arrow_type = map_postgresql_to_arrow_type(col_oid)

            columns.append(col_name)
            arrow_types.append(arrow_type)
            column_names.append(col_name)

        # Process data in batches
        all_data = {col: [] for col in columns}
        rows_processed = 0

        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break

            for row in batch:
                for col in columns:
                    all_data[col].append(row[col])

            rows_processed += len(batch)
            print(f"Processed {rows_processed} rows...", end='\r')

        print(f"\nTotal rows processed: {rows_processed}")

        if rows_processed > 0:
            # Convert to Arrow Table
            arrays = []
            for col, arrow_type in zip(columns, arrow_types):
                try:
                    array = pa.array(all_data[col], type=arrow_type)
                except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
                    print(f"Warning: Could not convert column '{col}' to {arrow_type}: {e}")
                    print("Falling back to string type")
                    array = pa.array([str(x) if x is not None else None for x in all_data[col]], type=pa.string())
                arrays.append(array)

            # Create table and write to feather
            table = pa.Table.from_arrays(arrays, names=column_names)
            feather.write_feather(table, output_file)

            print(f"Successfully exported {rows_processed} rows to {output_file}")
            print(f"Schema: {table.schema}")
        else:
            print("No data found for the query.")

    except psycopg2.Error as e:
        print(f"PostgreSQL error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def main():
    parser = argparse.ArgumentParser(description='Export PostgreSQL query to Arrow Feather format')

    # Connection options
    parser.add_argument('--host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--database', default='postgres', help='Database name')
    parser.add_argument('--user', default='postgres', help='Database user')
    parser.add_argument('--password', default='', help='Database password')

    # Alternative: connection string
    parser.add_argument('--connection-string', help='PostgreSQL connection string (overrides individual connection params)')

    parser.add_argument('--query', required=True, help='SQL query to execute')
    parser.add_argument('--output', required=True, help='Output feather file path')
    parser.add_argument('--batch-size', type=int, default=10000, help='Batch size for processing')

    args = parser.parse_args()

    # Build connection string
    if args.connection_string:
        connection_string = args.connection_string
    else:
        connection_string = f"host={args.host} port={args.port} dbname={args.database} user={args.user} password={args.password}"

    export_query_to_feather(
        connection_string=connection_string,
        query=args.query,
        output_file=args.output,
        batch_size=args.batch_size
    )

if __name__ == "__main__":
    main()
