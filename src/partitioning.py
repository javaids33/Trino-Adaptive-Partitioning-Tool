# partitioning.py

import logging
import pandas as pd
from collections import Counter
import sqlglot
from sqlglot import exp

def extract_select_statement(ddl):
    """
    Given a DDL (e.g. CREATE MATERIALIZED VIEW ... AS SELECT ...),
    extract the SELECT statement.
    """
    marker = " AS "
    idx = ddl.upper().find(marker)
    if idx != -1:
        return ddl[idx + len(marker):].strip(" \n;")
    else:
        logging.warning("Could not locate SELECT statement in DDL.")
        return None

def parse_underlying_query(ddl):
    """
    Parses the underlying SELECT query from the DDL using sqlglot.
    Returns a dictionary containing:
      - tables: list of underlying table names (and aliases)
      - join_columns: Counter of columns used in JOIN conditions
      - where_columns: Counter of columns used in the WHERE clause
    """
    query_text = extract_select_statement(ddl)
    if not query_text:
        return None
    try:
        parsed = sqlglot.parse_one(query_text)
    except Exception as e:
        logging.error("Error parsing SQL query: %s", e)
        return None

    tables = []
    join_columns = Counter()
    where_columns = Counter()

    for node in parsed.find_all(exp.Table):
        table_name = node.name
        alias = node.args.get("alias")
        if alias:
            tables.append(f"{table_name} AS {alias.name}")
        else:
            tables.append(table_name)

    for join in parsed.find_all(exp.Join):
        on = join.args.get("on")
        if on:
            for col in on.find_all(exp.Column):
                join_columns[col.name] += 1

    where = parsed.args.get("where")
    if where:
        for col in where.find_all(exp.Column):
            where_columns[col.name] += 1

    return {
        "tables": tables,
        "join_columns": join_columns,
        "where_columns": where_columns
    }

def aggregate_column_usage(view_data, query_log_data=None):
    """
    Aggregates weighted column usage across:
      - view definitions (information_schema columns weighted by query_count)
      - join columns parsed from the underlying query DDLs
      - table names referenced in query logs
    view_data: list of tuples (fully_qualified_view, columns, query_count, ddl)
    query_log_data: list of rows (query_id, query, create_time)
    Returns a pandas DataFrame of columns with their weighted frequency.
    """
    weighted_columns = []
    # Include columns from each view (weighted by query_count)
    for fq_view, columns, query_count, ddl in view_data:
        weighted_columns.extend(columns * query_count)
        if ddl:
            stats = parse_underlying_query(ddl)
            if stats:
                for col, cnt in stats.get("join_columns", {}).items():
                    weighted_columns.extend([col] * cnt)
    # Process query logs: parse each query and add referenced table names
    if query_log_data:
        for row in query_log_data:
            query_text = row[1]
            try:
                parsed = sqlglot.parse_one(query_text)
                for node in parsed.find_all(exp.Table):
                    table_name = node.name
                    weighted_columns.append(table_name)
            except Exception as e:
                logging.warning("Failed to parse query log query: %s", e)
    col_stats = Counter(weighted_columns)
    df_columns = pd.DataFrame(col_stats.items(), columns=['Column', 'WeightedFrequency'])
    return df_columns.sort_values(by='WeightedFrequency', ascending=False)

def analyze_column_cardinality(cursor, view_data):
    """
    Analyze cardinality of columns to improve partitioning decisions.
    High cardinality columns might lead to too many small partitions.
    """
    cardinality_stats = {}
    for fq_view, columns, _, _ in view_data:
        view_stats = {}
        for column in columns:
            try:
                # Sample-based cardinality estimation
                query = f"SELECT approx_distinct({column}) as cardinality FROM {fq_view}"
                cursor.execute(query)
                result = cursor.fetchone()
                if result:
                    view_stats[column] = result[0]
            except Exception as e:
                logging.warning(f"Failed to get cardinality for {fq_view}.{column}: {e}")
        cardinality_stats[fq_view] = view_stats
    return cardinality_stats

def analyze_query_resource_metrics(query_log_data):
    """
    Analyze resource usage patterns from query logs to identify resource-intensive queries
    that would benefit most from partitioning.
    
    Returns a dictionary of normalized resource metrics per query.
    """
    if not query_log_data or len(query_log_data) == 0:
        return {}
    
    # Extract metrics from all queries
    metrics = {
        'execution_time_ms': [row[3] for row in query_log_data if len(row) > 3 and row[3] is not None],
        'cpu_time_ms': [row[4] for row in query_log_data if len(row) > 4 and row[4] is not None],
        'input_bytes': [row[6] for row in query_log_data if len(row) > 6 and row[6] is not None],
        'peak_memory_bytes': [row[7] for row in query_log_data if len(row) > 7 and row[7] is not None]
    }
    
    # Calculate max values for normalization
    max_values = {
        'execution_time_ms': max(metrics['execution_time_ms']) if metrics['execution_time_ms'] else 1,
        'cpu_time_ms': max(metrics['cpu_time_ms']) if metrics['cpu_time_ms'] else 1,
        'input_bytes': max(metrics['input_bytes']) if metrics['input_bytes'] else 1,
        'peak_memory_bytes': max(metrics['peak_memory_bytes']) if metrics['peak_memory_bytes'] else 1
    }
    
    # Calculate resource intensity scores per query
    query_resource_scores = {}
    for row in query_log_data:
        query_id = row[0]
        
        # Skip if we don't have the metrics
        if len(row) < 8:
            continue
            
        # Calculate composite resource score (0-100)
        exec_time_norm = (row[3] / max_values['execution_time_ms']) * 40 if row[3] else 0
        cpu_time_norm = (row[4] / max_values['cpu_time_ms']) * 30 if row[4] else 0
        input_bytes_norm = (row[6] / max_values['input_bytes']) * 15 if row[6] else 0
        memory_norm = (row[7] / max_values['peak_memory_bytes']) * 15 if row[7] else 0
        
        resource_score = exec_time_norm + cpu_time_norm + input_bytes_norm + memory_norm
        query_resource_scores[query_id] = resource_score
    
    logging.info(f"Analyzed resource metrics for {len(query_resource_scores)} queries")
    return query_resource_scores

def analyze_query_performance(cursor, view_data, query_log_data):
    """
    Analyze query performance metrics from logs to correlate with column usage.
    """
    performance_metrics = {}
    if not query_log_data:
        return performance_metrics
        
    for row in query_log_data:
        query_id, query_text = row[0], row[1]
        try:
            # Extract query execution time from system tables
            cursor.execute(f"SELECT execution_time_ms FROM system.runtime.queries WHERE query_id = '{query_id}'")
            result = cursor.fetchone()
            if result:
                # Correlate performance with tables/columns used
                parsed = sqlglot.parse_one(query_text)
                tables = [node.name for node in parsed.find_all(exp.Table)]
                columns = [col.name for col in parsed.find_all(exp.Column)]
                
                for table in tables:
                    if table not in performance_metrics:
                        performance_metrics[table] = {"execution_time": 0, "query_count": 0, "columns": {}}
                    performance_metrics[table]["execution_time"] += result[0]
                    performance_metrics[table]["query_count"] += 1
                    
                    for col in columns:
                        if col not in performance_metrics[table]["columns"]:
                            performance_metrics[table]["columns"][col] = 0
                        performance_metrics[table]["columns"][col] += result[0]  # Weight by execution time
        except Exception as e:
            logging.warning(f"Failed to analyze performance for query {query_id}: {e}")
    
    return performance_metrics

def analyze_query_types(query_log_data):
    """
    Analyze query types (interactive vs batch) to better weight partition importance.
    Interactive queries benefit more from good partitioning.
    """
    query_type_stats = {}
    
    if not query_log_data:
        return query_type_stats
    
    for row in query_log_data:
        query_id, query_text = row[0], row[1]
        
        # Heuristic to determine if query is likely interactive or batch
        # Interactive queries often have LIMIT clauses or shorter execution times
        is_interactive = False
        
        # Check for LIMIT clause (common in interactive queries)
        if "LIMIT" in query_text.upper():
            is_interactive = True
            
        # Check execution time (if available)
        if len(row) > 3 and row[3] is not None:
            exec_time = row[3]  # execution_time_ms
            if exec_time < 10000:  # Less than 10 seconds is likely interactive
                is_interactive = True
        
        query_type_stats[query_id] = {
            "is_interactive": is_interactive,
            # Interactive queries get higher weight for partitioning optimization
            "partition_priority": 2.0 if is_interactive else 1.0
        }
    
    return query_type_stats

def analyze_data_distribution(cursor, view_data):
    """
    Analyze data distribution to detect skew in potential partition columns.
    Heavily skewed columns make poor partition keys as they create imbalanced partitions.
    """
    distribution_stats = {}
    
    for fq_view, columns, _, _ in view_data:
        view_stats = {}
        # Sample a subset of high-potential columns to avoid too many queries
        sample_columns = columns[:min(5, len(columns))]
        
        for column in sample_columns:
            try:
                # Get distribution metrics using approximate percentiles
                query = f"""
                SELECT
                    approx_percentile({column}, ARRAY[0.1, 0.5, 0.9]) as percentiles,
                    count(distinct {column}) as distinct_count,
                    count(*) as total_count
                FROM {fq_view}
                """
                cursor.execute(query)
                result = cursor.fetchone()
                
                if result and result[0]:
                    percentiles = result[0]
                    distinct_count = result[1]
                    total_count = result[2]
                    
                    # Calculate skew ratio: the ratio between 90th and 10th percentiles
                    # High skew indicates potential partition imbalance
                    if percentiles[0] != percentiles[2] and percentiles[0] != 0:
                        skew_ratio = percentiles[2] / percentiles[0]
                    else:
                        skew_ratio = 1.0
                        
                    # Calculate density: distinct values / total rows
                    # Higher density (closer to 1) means more unique values
                    density = distinct_count / total_count if total_count > 0 else 0
                    
                    view_stats[column] = {
                        "percentiles": percentiles,
                        "skew_ratio": skew_ratio,
                        "density": density,
                        "distinct_count": distinct_count
                    }
            except Exception as e:
                logging.warning(f"Failed to analyze distribution for {fq_view}.{column}: {e}")
                
        distribution_stats[fq_view] = view_stats
    
    return distribution_stats

def calculate_partition_score(column, view, cardinality_stats, performance_metrics, global_stats):
    """
    Calculate a composite score for each potential partition column.
    Higher score = better partition candidate.
    """
    score = 0
    
    # Base weighting from global usage statistics
    base_weight = global_stats.loc[global_stats['Column'] == column, 'WeightedFrequency'].values[0] if column in global_stats['Column'].values else 0
    score += base_weight * 1.0  # Base weight multiplier
    
    # Cardinality factor (penalize very high cardinality)
    if view in cardinality_stats and column in cardinality_stats[view]:
        cardinality = cardinality_stats[view][column]
        # Ideal cardinality depends on data size but typically between 100-10000 for partitioning
        if cardinality < 10:
            score += 5  # Too few values is great for partitioning
        elif cardinality < 100:
            score += 10  # Good cardinality range
        elif cardinality < 1000:
            score += 8  # Still reasonable
        elif cardinality < 10000:
            score += 5  # Getting to be too many partitions
        else:
            score += 1  # Too many potential partitions
    
    # Performance impact weighting
    if view in performance_metrics and "columns" in performance_metrics[view] and column in performance_metrics[view]["columns"]:
        # Higher execution time on this column = higher potential gain from partitioning
        score += min(performance_metrics[view]["columns"][column] / 1000, 50)  # Cap at 50 points
    
    return score

def produce_iceberg_partition_scripts(view_data, global_stats, cursor=None, top_n=3, query_log_data=None):
    """
    Generate Iceberg-specific partition scripts using appropriate transformations.
    """
    partition_scripts = {}
    
    # Get advanced statistics
    cardinality_stats = analyze_column_cardinality(cursor, view_data) if cursor else {}
    performance_metrics = analyze_query_performance(cursor, view_data, query_log_data) if cursor and query_log_data else {}
    
    # Get column types for each table
    column_types = {}
    column_stats = {}
    
    # First, get column scores
    for fq_view, columns, _, _ in view_data:
        view_scores = {}
        for column in columns:
            score = calculate_partition_score(column, fq_view, cardinality_stats, performance_metrics, global_stats)
            view_scores[column] = score
            
            # Get column type and stats if cursor is available
            if cursor:
                try:
                    # Get column type
                    schema, table = fq_view.split('.')
                    cursor.execute(f"DESCRIBE {fq_view} {column}")
                    result = cursor.fetchone()
                    if result:
                        if fq_view not in column_types:
                            column_types[fq_view] = {}
                        column_types[fq_view][column] = result[1]  # Type is usually in second column
                        
                        # Get additional stats for transformation selection
                        if fq_view not in column_stats:
                            column_stats[fq_view] = {}
                        column_stats[fq_view][column] = {
                            'cardinality': cardinality_stats.get(fq_view, {}).get(column, 0)
                        }
                        
                        # Get range for numeric columns
                        if result[1] in ('integer', 'bigint', 'double'):
                            try:
                                cursor.execute(f"SELECT MIN({column}), MAX({column}) FROM {fq_view}")
                                range_result = cursor.fetchone()
                                if range_result and range_result[0] is not None and range_result[1] is not None:
                                    column_stats[fq_view][column]['value_range'] = range_result[1] - range_result[0]
                            except Exception as e:
                                logging.warning(f"Failed to get value range for {fq_view}.{column}: {e}")
                        
                        # Get date granularity for date columns
                        if result[1] in ('date', 'timestamp'):
                            try:
                                # Sample approach to detect if dates are mostly daily, monthly, or yearly
                                cursor.execute(f"""
                                    WITH date_counts AS (
                                        SELECT 
                                            day({column}) as day_val,
                                            month({column}) as month_val,
                                            year({column}) as year_val,
                                            COUNT(*) as cnt
                                        FROM {fq_view}
                                        GROUP BY 1, 2, 3
                                    )
                                    SELECT
                                        COUNT(DISTINCT day_val) as day_count,
                                        COUNT(DISTINCT month_val) as month_count,
                                        COUNT(DISTINCT year_val) as year_count
                                    FROM date_counts
                                """)
                                date_result = cursor.fetchone()
                                if date_result:
                                    day_count, month_count, year_count = date_result
                                    # Determine appropriate granularity based on cardinality ratios
                                    if day_count > month_count * 20:  # Many days per month
                                        column_stats[fq_view][column]['date_granularity'] = 'day'
                                    elif month_count > year_count * 8:  # Many months per year
                                        column_stats[fq_view][column]['date_granularity'] = 'month'
                                    else:
                                        column_stats[fq_view][column]['date_granularity'] = 'year'
                            except Exception as e:
                                logging.warning(f"Failed to get date granularity for {fq_view}.{column}: {e}")
                                
                except Exception as e:
                    logging.warning(f"Failed to get column type for {fq_view}.{column}: {e}")
        
        # Sort by score
        sorted_columns = sorted(view_scores.items(), key=lambda x: x[1], reverse=True)
        top_columns = [col for col, score in sorted_columns[:top_n] if score > 0]
        
        if top_columns:
            # Generate Iceberg partition spec with appropriate transformations
            partition_specs = []
            for col in top_columns:
                col_type = column_types.get(fq_view, {}).get(col, 'unknown')
                col_stat = column_stats.get(fq_view, {}).get(col, {})
                
                # Get appropriate transformation
                from iceberg_utils import generate_iceberg_partition_spec
                partition_spec = generate_iceberg_partition_spec(col, col_type, col_stat)
                partition_specs.append(partition_spec)
            
            script = (
                f"-- Iceberg Partitioning script for {fq_view}\n"
                f"-- Column scores: {sorted_columns[:top_n]}\n"
                f"ALTER TABLE {fq_view} REPLACE PARTITION SPEC (\n    " + 
                ",\n    ".join(partition_specs) + 
                "\n);\n"
            )
            partition_scripts[fq_view] = script
        else:
            partition_scripts[fq_view] = f"-- {fq_view} does not contain suitable columns for partitioning.\n"
    
    return partition_scripts
