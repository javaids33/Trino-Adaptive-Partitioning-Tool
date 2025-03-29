# cli.py

import logging
import sys
import argparse
from .trino_client import get_connection, get_all_materialized_views, get_query_logs, analyze_query_resource_metrics
from .partitioning import aggregate_column_usage, produce_partition_scripts, analyze_column_cardinality, analyze_query_performance
from .config import QUERY_LOGS_TABLE, TOP_N
import pandas as pd

def main():
    parser = argparse.ArgumentParser(description="Adaptive Partitioning Tool for Trino")
    parser.add_argument("--execute", action="store_true", help="Execute partition scripts in Trino")
    parser.add_argument("--time_filter", type=str, default=None,
                        help="SQL condition to filter query logs (e.g., \"create_time >= TIMESTAMP '2023-01-01'\")")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Retrieve all materialized views
        views = get_all_materialized_views(cursor)
        logging.info("Found %d materialized views.", len(views))
        
        # For each view, retrieve DDL and columns.
        # We'll store tuples: (fully_qualified_view, [columns], query_count, ddl)
        # For simplicity, we assume a default query_count of 1 per view.
        view_data = []
        for view in views:
            fq_view = f"{view.get('schema')}.{view['table']}"
            ddl_query = f"SHOW CREATE MATERIALIZED VIEW {fq_view}"
            try:
                cursor.execute(ddl_query)
                ddl_result = cursor.fetchall()
                ddl = ddl_result[0][0] if ddl_result else None
            except Exception as e:
                logging.error("Error retrieving DDL for %s: %s", fq_view, e)
                ddl = None

            col_query = f"""
                SELECT column_name
                FROM "{view.get('catalog')}"."information_schema"."columns"
                WHERE table_schema = '{view.get('schema')}'
                  AND table_name = '{view.get('table')}'
                ORDER BY ordinal_position
            """
            try:
                cursor.execute(col_query)
                col_result = cursor.fetchall()
                columns = [row[0] for row in col_result] if col_result else []
            except Exception as e:
                logging.error("Error retrieving columns for %s: %s", fq_view, e)
                columns = []

            view_data.append((fq_view, columns, 1, ddl))

        # Enhanced query log retrieval with resource metrics
        query_log_data = None
        if args.time_filter:
            query_log_data = get_query_logs(cursor, QUERY_LOGS_TABLE, args.time_filter)
            logging.info("Retrieved %d query logs with resource metrics using filter: %s", 
                        len(query_log_data), args.time_filter)
                        
            # Log statistics about resource usage in the retrieved queries
            if query_log_data and len(query_log_data) > 0:
                resource_stats = analyze_query_resource_metrics(query_log_data)
                logging.info("Resource intensity scores calculated for %d queries", 
                            len(resource_stats))
                
                # Log the top 5 most resource-intensive queries
                top_queries = sorted(resource_stats.items(), key=lambda x: x[1], reverse=True)[:5]
                logging.info("Top 5 resource-intensive queries: %s", top_queries)

        # Aggregate column usage statistics with enhanced resource metrics
        global_stats = aggregate_column_usage(view_data, query_log_data)
        logging.info("Global Column Usage Stats:\n%s", global_stats.to_string(index=False))

        # Generate partition scripts
        column_scores = {}  # Store column scores for UI
        cardinality_stats = analyze_column_cardinality(cursor, view_data) if cursor else {}
        
        query_resource_scores = None
        if query_log_data:
            query_resource_scores = analyze_query_resource_metrics(query_log_data)
            
        performance_metrics = analyze_query_performance(cursor, view_data, query_log_data) if cursor and query_log_data else {}
        
        # Capture column scores during partition script generation
        for fq_view, columns, _, _ in view_data:
            column_scores[fq_view] = {}
            for column in columns:
                score = calculate_partition_score(column, fq_view, cardinality_stats, performance_metrics, global_stats)
                column_scores[fq_view][column] = score
        
        partition_scripts = produce_partition_scripts(
            view_data, 
            global_stats, 
            cursor=cursor, 
            top_n=TOP_N, 
            query_log_data=query_log_data
        )
        
        # Save results for UI visualization
        from ui.generate_ui_data import save_analysis_results
        save_analysis_results(
            global_stats,
            view_data,
            partition_scripts,
            column_scores,
            cardinality_stats,
            performance_metrics,
            query_resource_scores
        )

        # Add information about the UI
        print("\nUI Visualization:")
        print("To view the analysis results in the UI dashboard, run:")
        print("  streamlit run src/ui/app.py")

        # Optionally execute partition scripts.
        if args.execute:
            for fq_view, script in partition_scripts.items():
                if not script.strip().startswith("--"):
                    try:
                        logging.info("Executing partition script for %s", fq_view)
                        cursor.execute(script)
                    except Exception as e:
                        logging.error("Error executing partition script for %s: %s", fq_view, e)
        else:
            logging.info("Dry run mode enabled. Partition scripts were not executed.")

    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
