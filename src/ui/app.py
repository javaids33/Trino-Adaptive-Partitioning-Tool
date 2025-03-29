import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import json
import os
import sys
from pathlib import Path

# Add parent directory to path to allow imports
sys.path.append(str(Path(__file__).parent.parent))

from partitioning import (
    analyze_column_cardinality,
    analyze_query_performance,
    analyze_query_resource_metrics,
    analyze_data_distribution
)

# Set page configuration
st.set_page_config(
    page_title="Trino Adaptive Partitioning Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

def load_data(results_dir="results"):
    """Load analysis results from the results directory"""
    data = {}
    
    # Define expected files
    expected_files = {
        "global_stats.csv": "global_stats",
        "partition_recommendations.json": "recommendations",
        "view_data.json": "view_data",
        "query_metrics.json": "query_metrics",
        "column_scores.json": "column_scores",
        "resource_metrics.json": "resource_metrics"
    }
    
    # Check if results directory exists
    if not os.path.exists(results_dir):
        st.error(f"Results directory '{results_dir}' not found. Run the analysis first!")
        return None
    
    # Load each file if it exists
    for filename, key in expected_files.items():
        filepath = os.path.join(results_dir, filename)
        if os.path.exists(filepath):
            if filename.endswith('.csv'):
                data[key] = pd.read_csv(filepath)
            elif filename.endswith('.json'):
                with open(filepath, 'r') as f:
                    data[key] = json.load(f)
        else:
            st.warning(f"File '{filename}' not found in results directory.")
    
    return data

def main():
    # Add a sidebar for navigation
    st.sidebar.title("Trino Partitioning Dashboard")
    page = st.sidebar.radio(
        "Navigate to", 
        ["Overview", "Column Statistics", "Resource Usage", "Partition Recommendations", "Query Analysis"]
    )
    
    # Load data
    data = load_data()
    if not data:
        st.stop()
    
    if page == "Overview":
        show_overview(data)
    elif page == "Column Statistics":
        show_column_statistics(data)
    elif page == "Resource Usage":
        show_resource_usage(data)
    elif page == "Partition Recommendations":
        show_recommendations(data)
    elif page == "Query Analysis":
        show_query_analysis(data)

def show_overview(data):
    st.title("Trino Adaptive Partitioning Overview")
    
    # Display a summary of the analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Analysis Summary")
        if "view_data" in data:
            view_count = len(data["view_data"])
            st.metric("Total Views Analyzed", view_count)
        
        if "global_stats" in data:
            column_count = len(data["global_stats"])
            st.metric("Total Columns Analyzed", column_count)
        
        if "recommendations" in data:
            rec_count = len(data["recommendations"])
            st.metric("Partition Recommendations", rec_count)
    
    with col2:
        st.subheader("Top Columns by Usage")
        if "global_stats" in data:
            # Show top 5 columns by weighted frequency
            top_columns = data["global_stats"].sort_values(by="WeightedFrequency", ascending=False).head(5)
            fig = px.bar(
                top_columns,
                x="Column",
                y="WeightedFrequency",
                title="Top 5 Columns by Usage Frequency",
                color="WeightedFrequency",
                color_continuous_scale="Viridis",
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Show a quick summary of partitioning benefits
    st.subheader("Partitioning Benefits Summary")
    if "recommendations" in data and "column_scores" in data:
        # Create a dataframe from the recommendations
        tables = []
        partition_keys = []
        scores = []
        
        for table, rec_data in data["recommendations"].items():
            if "partition_keys" in rec_data:
                tables.append(table)
                partition_keys.append(", ".join(rec_data["partition_keys"]))
                
                # Get scores for these keys from column_scores if available
                table_scores = data["column_scores"].get(table, {})
                avg_score = 0
                if table_scores:
                    scores_list = [table_scores.get(key, 0) for key in rec_data["partition_keys"]]
                    avg_score = sum(scores_list) / len(scores_list) if scores_list else 0
                scores.append(avg_score)
        
        if tables:
            summary_df = pd.DataFrame({
                "Table": tables,
                "Partition Keys": partition_keys,
                "Average Score": scores
            })
            summary_df = summary_df.sort_values(by="Average Score", ascending=False)
            st.dataframe(summary_df, use_container_width=True)
        else:
            st.info("No partition recommendations available.")
    else:
        st.info("No recommendation data available.")

def show_column_statistics(data):
    st.title("Column Usage Statistics")
    
    if "global_stats" not in data:
        st.warning("Column statistics data not available.")
        return
    
    # Allow filtering by table
    if "view_data" in data:
        tables = [item[0] for item in data["view_data"]]
        selected_table = st.selectbox("Filter by Table", ["All Tables"] + tables)
    else:
        selected_table = "All Tables"
    
    # Filter data if a specific table is selected
    filtered_stats = data["global_stats"]
    if selected_table != "All Tables":
        # We need to filter the global stats to only show columns from the selected table
        if "view_data" in data:
            table_columns = []
            for item in data["view_data"]:
                if item[0] == selected_table:
                    table_columns = item[1]
                    break
            
            filtered_stats = filtered_stats[filtered_stats["Column"].isin(table_columns)]
    
    # Sort by weighted frequency
    filtered_stats = filtered_stats.sort_values(by="WeightedFrequency", ascending=False)
    
    # Create visualizations
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Column Usage Frequency")
        
        # Take top 20 columns for better visualization
        top_n = min(20, len(filtered_stats))
        plot_data = filtered_stats.head(top_n)
        
        fig = px.bar(
            plot_data,
            x="Column",
            y="WeightedFrequency",
            title=f"Top {top_n} Columns by Usage Frequency",
            color="WeightedFrequency",
            color_continuous_scale="Viridis",
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Column Details")
        st.dataframe(filtered_stats, use_container_width=True)
    
    # Show column cardinality if available
    if "cardinality_stats" in data and selected_table != "All Tables":
        st.subheader("Column Cardinality Analysis")
        if selected_table in data["cardinality_stats"]:
            cardinality_data = pd.DataFrame(
                {"Column": k, "Cardinality": v} 
                for k, v in data["cardinality_stats"][selected_table].items()
            )
            
            # Calculate ideal cardinality score (1-10)
            def cardinality_score(card):
                if card < 10:
                    return 8  # Very few values
                elif card < 100:
                    return 10  # Ideal range
                elif card < 1000:
                    return 7  # Good
                elif card < 10000:
                    return 5  # Acceptable
                else:
                    return 2  # Too many values
            
            cardinality_data["Score"] = cardinality_data["Cardinality"].apply(cardinality_score)
            cardinality_data = cardinality_data.sort_values(by="Score", ascending=False)
            
            fig = px.scatter(
                cardinality_data,
                x="Column",
                y="Cardinality",
                size="Score",
                color="Score",
                title="Column Cardinality (lower is better for partitioning)",
                color_continuous_scale="RdYlGn_r",  # Reversed so red=high cardinality (bad)
                size_max=20,
                log_y=True,  # Log scale for cardinality
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
            
            st.dataframe(cardinality_data, use_container_width=True)
        else:
            st.info(f"No cardinality data available for {selected_table}.")

def show_resource_usage(data):
    st.title("Resource Usage Analysis")
    
    if "resource_metrics" not in data:
        st.warning("Resource metrics data not available.")
        return
    
    # Get list of tables
    tables = list(data["resource_metrics"].keys())
    if not tables:
        st.info("No resource metrics available for any tables.")
        return
    
    selected_table = st.selectbox("Select Table", tables)
    
    if selected_table in data["resource_metrics"]:
        table_metrics = data["resource_metrics"][selected_table]
        
        # Display resource score and query count
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Resource Intensity Score", f"{table_metrics.get('resource_score', 0):.2f}")
        with col2:
            st.metric("Query Count", table_metrics.get("query_count", 0))
        
        # Display column resource usage
        st.subheader("Column Resource Usage")
        
        if "columns" in table_metrics and table_metrics["columns"]:
            # Convert to dataframe
            column_data = pd.DataFrame({
                "Column": list(table_metrics["columns"].keys()),
                "Resource Score": list(table_metrics["columns"].values())
            })
            
            # Add predicate score if available
            if "predicate_columns" in table_metrics and table_metrics["predicate_columns"]:
                predicate_dict = table_metrics["predicate_columns"]
                column_data["Predicate Score"] = column_data["Column"].map(lambda x: predicate_dict.get(x, 0))
                column_data["Total Score"] = column_data["Resource Score"] + column_data["Predicate Score"]
                column_data = column_data.sort_values(by="Total Score", ascending=False)
            else:
                column_data = column_data.sort_values(by="Resource Score", ascending=False)
            
            # Create visualization
            fig = px.bar(
                column_data.head(15),  # Top 15 columns
                x="Column",
                y=["Resource Score", "Predicate Score"] if "Predicate Score" in column_data.columns else "Resource Score",
                title="Column Resource Usage",
                barmode="stack",
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show the data
            st.dataframe(column_data, use_container_width=True)
        else:
            st.info("No column resource metrics available for this table.")
    else:
        st.info(f"No resource metrics available for {selected_table}.")

def show_recommendations(data):
    st.title("Partition Recommendations")
    
    if "recommendations" not in data:
        st.warning("Recommendation data not available.")
        return
    
    # Get list of tables with recommendations
    tables = list(data["recommendations"].keys())
    if not tables:
        st.info("No partition recommendations available.")
        return
    
    selected_table = st.selectbox("Select Table", tables)
    
    if selected_table in data["recommendations"]:
        rec_data = data["recommendations"][selected_table]
        
        # Display the recommendation
        st.subheader("Recommended Partitioning")
        
        if "partition_keys" in rec_data and rec_data["partition_keys"]:
            # Create columns
            col1, col2 = st.columns([1, 2])
            
            with col1:
                # Show the partition keys
                st.markdown("#### Partition Keys")
                for key in rec_data["partition_keys"]:
                    st.success(key)
                
                # Show the partition script
                if "script" in rec_data:
                    st.markdown("#### Partition Script")
                    st.code(rec_data["script"], language="sql")
            
            with col2:
                # Show justification with column scores
                st.markdown("#### Justification")
                
                if "column_scores" in data and selected_table in data["column_scores"]:
                    table_scores = data["column_scores"][selected_table]
                    
                    # Create a dataframe with all scores
                    all_cols = list(table_scores.keys())
                    all_scores = list(table_scores.values())
                    
                    score_df = pd.DataFrame({
                        "Column": all_cols,
                        "Score": all_scores,
                        "Selected": [col in rec_data["partition_keys"] for col in all_cols]
                    })
                    score_df = score_df.sort_values(by="Score", ascending=False)
                    
                    # Create a bar chart highlighting selected columns
                    fig = px.bar(
                        score_df.head(15),  # Top 15 for visibility
                        x="Column",
                        y="Score",
                        color="Selected",
                        title="Column Partition Scores",
                        color_discrete_map={True: "green", False: "gray"},
                    )
                    fig.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Show a table with detailed breakdown
                    st.dataframe(score_df, use_container_width=True)
                else:
                    st.info("No detailed scoring data available for this table.")
        else:
            st.warning("No partition keys recommended for this table.")
    else:
        st.info(f"No recommendations available for {selected_table}.")

def show_query_analysis(data):
    st.title("Query Analysis")
    
    if "query_metrics" not in data:
        st.warning("Query analysis data not available.")
        return
    
    # Show overall query statistics
    st.subheader("Query Resource Distribution")
    
    if data["query_metrics"]:
        # Convert to dataframe
        queries = []
        for query_id, metrics in data["query_metrics"].items():
            query_data = {"query_id": query_id}
            query_data.update(metrics)
            queries.append(query_data)
        
        if queries:
            query_df = pd.DataFrame(queries)
            
            # Create visualizations for resource distribution
            col1, col2 = st.columns(2)
            
            with col1:
                # Show distribution of resource scores
                fig = px.histogram(
                    query_df,
                    x="resource_score",
                    nbins=20,
                    title="Distribution of Query Resource Scores",
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Show distribution of interactive vs batch queries
                if "is_interactive" in query_df.columns:
                    interactive_counts = query_df["is_interactive"].value_counts()
                    fig = px.pie(
                        names=["Interactive", "Batch"],
                        values=[
                            interactive_counts.get(True, 0),
                            interactive_counts.get(False, 0)
                        ],
                        title="Query Types"
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            # Show top resource-intensive queries
            st.subheader("Top Resource-Intensive Queries")
            top_queries = query_df.sort_values(by="resource_score", ascending=False).head(10)
            
            # Select and format columns
            display_cols = ["query_id", "resource_score"]
            if "execution_time_ms" in top_queries.columns:
                display_cols.append("execution_time_ms")
            if "is_interactive" in top_queries.columns:
                display_cols.append("is_interactive")
            
            st.dataframe(top_queries[display_cols], use_container_width=True)
            
            # Show individual query details
            st.subheader("Query Details")
            selected_query = st.selectbox(
                "Select Query to View Details",
                options=top_queries["query_id"].tolist()
            )
            
            if selected_query:
                query_details = next((q for q in queries if q["query_id"] == selected_query), None)
                if query_details:
                    # Display all query metrics
                    st.json(query_details)
                    
                    # If query text is available, show it
                    if "query_text" in query_details:
                        st.subheader("Query Text")
                        st.code(query_details["query_text"], language="sql")
        else:
            st.info("No query data available.")
    else:
        st.info("No query metrics available.")

if __name__ == "__main__":
    main() 