def generate_iceberg_partition_spec(column, column_type, column_stats):
    """
    Generate appropriate Iceberg partition transformation for a column
    based on its data type and statistics.
    """
    # Date/timestamp columns often benefit from truncation
    if column_type in ('date', 'timestamp'):
        # Check granularity based on data distribution
        if 'date_granularity' in column_stats:
            granularity = column_stats['date_granularity']
            if granularity == 'day':
                return f"day({column})"
            elif granularity == 'month':
                return f"month({column})"
            elif granularity == 'year':
                return f"year({column})"
        # Default to month for dates
        return f"month({column})"
    
    # String columns with high cardinality benefit from bucketing
    elif column_type in ('varchar', 'string', 'char'):
        cardinality = column_stats.get('cardinality', 0)
        if cardinality > 10000:
            # High cardinality - use bucketing
            return f"bucket(16, {column})"
        else:
            # Lower cardinality - use the column directly
            return column
    
    # Integer columns might benefit from bucketing if high cardinality
    elif column_type in ('integer', 'bigint'):
        cardinality = column_stats.get('cardinality', 0)
        if cardinality > 1000:
            bucket_count = min(max(int(cardinality / 500), 4), 32)
            return f"bucket({bucket_count}, {column})"
        else:
            # Check range to see if truncation makes sense
            value_range = column_stats.get('value_range', 0)
            if value_range > 10000:
                # Large range - truncate to hundreds
                return f"truncate({column}, 100)"
            else:
                return column
    
    # Default - no transformation
    return column 