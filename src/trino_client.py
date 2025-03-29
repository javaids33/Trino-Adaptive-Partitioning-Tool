# trino_client.py

import logging
from trino.dbapi import connect
from .config import TRINO_HOST, TRINO_PORT, TRINO_USER, TRINO_CATALOG_DEFAULT, TRINO_SCHEMA_DEFAULT

def get_connection():
    try:
        conn = connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG_DEFAULT,
            schema=TRINO_SCHEMA_DEFAULT,
        )
        logging.info("Connected to Trino.")
        return conn
    except Exception as e:
        logging.error("Error connecting to Trino: %s", e)
        raise

def get_all_materialized_views(cursor):
    """
    Retrieves all materialized views from the instance.
    Here we query the information_schema.views and filter for materialized views.
    (Adjust the WHERE clause if your Trino deployment flags them differently.)
    """
    query = f"""
    SELECT table_catalog, table_schema, table_name
    FROM "{TRINO_CATALOG_DEFAULT}"."information_schema"."views"
    WHERE table_type = 'MATERIALIZED VIEW'
    """
    cursor.execute(query)
    result = cursor.fetchall()
    views = [{"catalog": row[0], "schema": row[1], "table": row[2]} for row in result]
    return views

def get_query_logs(cursor, logs_table, time_filter=None):
    """
    Retrieves query logs with resource metrics from the specified logs_table.
    """
    query = f"""
    SELECT 
        query_id, 
        query, 
        create_time,
        execution_time_ms,
        cpu_time_ms,
        scheduled_time_ms,
        input_bytes,
        peak_memory_bytes,
        peak_total_memory_bytes
    FROM {logs_table}
    """
    if time_filter:
        query += f" WHERE {time_filter}"
    
    # Sorting by resource intensity to prioritize resource-heavy queries
    query += " ORDER BY execution_time_ms DESC"
    
    cursor.execute(query)
    result = cursor.fetchall()
    return result

def get_iceberg_tables(cursor, catalog, schema=None):
    """
    Get all Iceberg tables in the specified catalog and schema.
    """
    query = f"""
    SELECT table_schema, table_name 
    FROM {catalog}.information_schema.tables
    WHERE table_type = 'BASE TABLE'
    """
    
    if schema:
        query += f" AND table_schema = '{schema}'"
    
    cursor.execute(query)
    tables = cursor.fetchall()
    
    # Filter for Iceberg tables
    iceberg_tables = []
    for schema, table in tables:
        try:
            # Check if table has Iceberg metadata
            cursor.execute(f"SELECT * FROM {catalog}.{schema}.{table}.$metadata LIMIT 1")
            # If no error, it's an Iceberg table
            iceberg_tables.append((schema, table))
        except Exception:
            # Not an Iceberg table or metadata not accessible
            pass
    
    return iceberg_tables

def get_iceberg_partition_spec(cursor, catalog, schema, table):
    """
    Get current partition spec for an Iceberg table.
    """
    try:
        cursor.execute(f"SELECT partition_spec FROM {catalog}.{schema}.{table}.$metadata")
        result = cursor.fetchone()
        if result:
            return result[0]
        return None
    except Exception as e:
        logging.warning(f"Failed to get partition spec for {catalog}.{schema}.{table}: {e}")
        return None
