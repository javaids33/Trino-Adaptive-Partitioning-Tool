import os
import json
import pandas as pd
import logging
from pathlib import Path

def save_analysis_results(
    global_stats,
    view_data,
    partition_scripts,
    column_scores=None,
    cardinality_stats=None,
    performance_metrics=None,
    query_resource_scores=None,
    output_dir="results"
):
    """
    Save analysis results to files for UI consumption.
    
    Args:
        global_stats: DataFrame with global column statistics
        view_data: List of view data from analysis
        partition_scripts: Dictionary of partition scripts by table
        column_scores: Dictionary of column scores by table
        cardinality_stats: Dictionary of column cardinality by table
        performance_metrics: Dictionary of performance metrics by table
        query_resource_scores: Dictionary of resource scores by query
        output_dir: Directory to save results
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Save global stats
    if global_stats is not None:
        global_stats.to_csv(os.path.join(output_dir, "global_stats.csv"), index=False)
    
    # Save view data
    if view_data:
        # Convert view_data to a more JSON-friendly format
        json_view_data = []
        for fq_view, columns, definition, query_count in view_data:
            json_view_data.append({
                "view": fq_view,
                "columns": columns,
                "definition": definition,
                "query_count": query_count
            })
        
        with open(os.path.join(output_dir, "view_data.json"), "w") as f:
            json.dump(json_view_data, f, indent=2)
    
    # Create recommendations data
    recommendations = {}
    for table, script in partition_scripts.items():
        # Extract partition keys from script
        partition_keys = []
        if "SET PARTITIONING" in script:
            partition_section = script.split("SET PARTITIONING")[1]
            partition_section = partition_section.split("(")[1].split(")")[0]
            partition_keys = [k.strip() for k in partition_section.split(",")]
        
        recommendations[table] = {
            "script": script,
            "partition_keys": partition_keys
        }
    
    with open(os.path.join(output_dir, "partition_recommendations.json"), "w") as f:
        json.dump(recommendations, f, indent=2)
    
    # Save column scores
    if column_scores:
        with open(os.path.join(output_dir, "column_scores.json"), "w") as f:
            json.dump(column_scores, f, indent=2)
    
    # Save cardinality stats
    if cardinality_stats:
        with open(os.path.join(output_dir, "cardinality_stats.json"), "w") as f:
            json.dump(cardinality_stats, f, indent=2)
    
    # Save performance metrics
    if performance_metrics:
        with open(os.path.join(output_dir, "resource_metrics.json"), "w") as f:
            json.dump(performance_metrics, f, indent=2)
    
    # Save query resource scores
    if query_resource_scores:
        # Create a more detailed query metrics structure
        query_metrics = {}
        for query_id, score in query_resource_scores.items():
            query_metrics[query_id] = {"resource_score": score}
        
        with open(os.path.join(output_dir, "query_metrics.json"), "w") as f:
            json.dump(query_metrics, f, indent=2)
    
    logging.info(f"Analysis results saved to {output_dir} directory") 