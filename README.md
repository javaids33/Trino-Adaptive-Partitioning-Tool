# Trino-Auto-Partitioning-Tool (T.A.P.T)

## Overview

The Adaptive Partitioning Tool for Trino is a data-driven solution for optimizing query performance in Trino databases through intelligent partitioning strategy recommendations. By analyzing query patterns, column usage statistics, and data characteristics, this tool helps database administrators make informed decisions about table partitioning.

## The Problem

Partitioning is a critical performance optimization technique in distributed SQL engines like Trino (formerly PrestoSQL), but determining the optimal partitioning strategy presents several challenges:

1. **Manual Configuration Burden**: Database administrators often rely on intuition or general rules of thumb.
2. **Workload Blindness**: Generic partitioning strategies don't account for specific query patterns.
3. **Resource Optimization Complexity**: Different columns benefit different query types.
4. **Data Distribution Challenges**: Columns with skewed distributions can create imbalanced partitions.
5. **Evolving Workloads**: Query patterns change over time, requiring refinement of partitioning strategies.

## Our Solution

This tool employs a data-driven approach to partition recommendation:

### Data Collection
- Analyzes materialized views from Trino catalogs
- Extracts query patterns from Trino query history
- Identifies frequently used filtering predicates in JOIN and WHERE clauses
- Collects resource metrics from query logs (execution time, CPU usage, memory consumption)

### Statistical Analysis
- **Frequency Analysis**: Weights columns by their usage frequency in queries
- **Cardinality Examination**: Assesses column cardinality for partition suitability
- **Resource Impact Analysis**: Identifies columns used in resource-intensive queries
- **Data Distribution Analysis**: Detects data skew to prevent imbalanced partitions
- **Query Type Classification**: Distinguishes between interactive and batch queries

### Scoring System
- Combines multiple factors into a partition suitability score
- Ranks columns based on their potential performance impact
- Provides clear recommendations with supporting evidence

### Visualization Dashboard
- Interactive Streamlit dashboard showing partition recommendations and their justification
- Visualizations of column usage patterns, resource metrics, and data distribution
- Detailed breakdown of scoring factors for transparency

## Features

- **Iceberg-Specific Optimizations**: Generates appropriate partition transformations for Iceberg tables (day, month, year for dates; bucketing for high-cardinality columns)
- **Resource Metrics Analysis**: Incorporates execution time, CPU usage, and memory consumption into recommendations
- **Query Pattern Recognition**: Identifies and prioritizes columns used in JOIN and WHERE clauses
- **Interactive vs. Batch Query Analysis**: Gives higher weight to columns that benefit interactive workloads
- **Dry-Run Mode**: Generates partition scripts without executing them for review
- **Comprehensive Dashboard**: Visualizes all analysis results for informed decision-making

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/trino-adaptive-partitioning.git
cd trino-adaptive-partitioning

# Install dependencies
pip install -r src/requirements.txt

# Configure your Trino connection
# Edit src/config.py with your Trino server details:
# - TRINO_HOST
# - TRINO_PORT
# - TRINO_USER
# - TRINO_CATALOG_DEFAULT
# - TRINO_SCHEMA_DEFAULT
```

## Usage

### Command-Line Interface

```bash
# Run analysis in dry-run mode (default)
python -m src.cli

# Run analysis with query logs from the last 7 days
python -m src.cli --time_filter "create_time >= current_timestamp - interval '7' day"

# Run analysis and execute the partitioning commands
python -m src.cli --execute
```

### Dashboard Visualization

After running the analysis, you can view the results in the interactive dashboard:

```bash
# Launch the visualization dashboard
python -m src.ui_dashboard

# Or directly with streamlit
streamlit run src/ui/app.py
```

The dashboard provides:
- Overview of column usage statistics
- Detailed column cardinality analysis
- Resource usage metrics by table and column
- Partition recommendations with justification
- Query analysis and resource distribution

## How It Works

### 1. Data Collection
The tool connects to your Trino server and:
- Retrieves all materialized views from the catalog
- Collects column information for each view
- Extracts the underlying query from each view's DDL
- Retrieves query logs with resource metrics if a time filter is provided

### 2. Analysis
The tool performs several types of analysis:
- Parses SQL queries to identify columns used in JOIN and WHERE clauses
- Calculates column cardinality to assess partition suitability
- Analyzes resource metrics to identify resource-intensive queries
- Examines data distribution to detect skew in potential partition columns
- Classifies queries as interactive or batch based on execution patterns

### 3. Scoring
Each potential partition column receives a composite score based on:
- Base weight from global usage statistics
- Cardinality factor (penalizing very high or very low cardinality)
- Resource impact multiplier based on query intensity
- Predicate usage bonus for columns frequently used in filters
- Data distribution factor to penalize highly skewed columns

### 4. Recommendation
The tool generates partition scripts for each table, recommending:
- The top N columns for partitioning (configurable, default is 3)
- Appropriate transformations for each column based on its data type and statistics
- Iceberg-specific partition specs with optimized transformations

## Configuration Options

Edit `src/config.py` to customize:
- `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`: Trino connection details
- `TRINO_CATALOG_DEFAULT`, `TRINO_SCHEMA_DEFAULT`: Default catalog and schema
- `QUERY_LOGS_TABLE`: Table containing query logs (default: system.runtime.queries)
- `TOP_N`: Number of columns to consider for partitioning (default: 3)

## Use Cases

### Data Warehouse Optimization
Optimize partitioning for analytical workloads to improve query performance and reduce resource consumption.

### Performance Tuning
Identify and address performance bottlenecks caused by suboptimal partitioning without manual trial and error.

### Iceberg Table Optimization
Generate optimized partition specs for Iceberg tables with appropriate transformations based on data characteristics.

### Workload Analysis
Understand query patterns and resource usage to make informed optimization decisions.

## Benefits

- **Data-Driven Decisions**: Replace guesswork with empirical analysis of actual query patterns
- **Resource Efficiency**: Optimize cluster resource utilization by improving partition pruning
- **Performance Gains**: Accelerate query execution through better partition strategies
- **Operational Insight**: Gain visibility into column usage patterns and resource consumption
- **Time Savings**: Eliminate manual analysis and experimentation for partition selection

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
```