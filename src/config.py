# config.py

# Trino connection settings
TRINO_HOST = 'your.trino.host'
TRINO_PORT = 8080
TRINO_USER = 'your_user'
TRINO_CATALOG_DEFAULT = 'your_catalog'
TRINO_SCHEMA_DEFAULT = 'your_schema'

# Table containing native query logs.
# For native logs, you might use "system.runtime.queries" or a custom table.
QUERY_LOGS_TABLE = 'system.runtime.queries'

# Partitioning configuration
EXECUTE_PARTITIONING = False  # dry run by default
TOP_N = 3  # number of heavy usage columns to consider for partitioning
