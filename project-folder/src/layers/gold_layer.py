"""
Gold Layer - Business-Ready Aggregations

This module provides the Gold layer for the Medallion Architecture, creating
business-ready aggregations from Silver layer data.

Architecture:
- GoldLayer: GENERIC base class providing reusable aggregation utilities
  - _aggregate_by_dimensions(): Generic groupBy + metrics calculation
  - _write_aggregation(): Generic Delta Lake writer with daily partitioning
  - read_gold_aggregation(): Generic reader for any aggregation

Gold Layer File Naming Convention:
- Format: {aggregation_name}_gold_{execution_date}_{timestamp}
- Example: breweries_by_location_gold_2026-01-26_20260127_054115
- execution_date: Airflow logical date (typically yesterday for daily runs)
- timestamp: When the aggregation was created (YYYYMMDD_HHMMSS)

Why this design:
- Open/Closed Principle: Generic methods are OPEN for extension (add new dimensions)
  but CLOSED for modification (core logic unchanged)
- Single Responsibility: This class handles ONLY generic Gold layer I/O
- Dependency Inversion: Depends on injected config/spark, not concrete implementations

Domain-specific aggregations (brewery, orders, etc.) should be implemented
in their respective pipeline modules using these generic utilities.
"""

import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, avg, min as _min, max as _max,
    sum as _sum, round as _round, collect_set, lit, current_timestamp
)

from src.utils.logger import get_logger
from src.utils.config import get_config
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)


class GoldLayer:
    """
    Gold Layer - Business aggregations from Silver data.
    
    This class provides GENERIC aggregation utilities that can be reused
    across different data domains (breweries, orders, etc.).
    
    SOLID Design:
    - Single Responsibility: Handles Gold layer I/O and generic aggregations
    - Open/Closed: Extend with domain-specific methods, don't modify core
    - Dependency Inversion: Config and Spark injected for testability
    
    Example usage for new data domain:
        gold = GoldLayer()
        df = gold._aggregate_by_dimensions(
            source_df,
            dimensions=['category', 'region'],
            metrics={'record_count': 'count', 'total_value': ('value', 'sum')}
        )
        gold._write_aggregation(df, 'orders_by_category_region')
    """
    
    def __init__(self, config: Optional[Dict] = None, spark: Optional[SparkSession] = None):
        """
        Initialize Gold layer with dependency injection.
        
        Args:
            config: Configuration dictionary (if None, loads from default)
            spark: Spark session (if None, creates new session)
            
        Why dependency injection:
        - Enables unit testing with mocked config/Spark
        - Follows Dependency Inversion Principle
        """
        self.config = config if config is not None else get_config()
        self.spark = spark if spark is not None else get_spark_session()
        self.base_path = self.config['storage']['base_path']
        self.gold_config = self.config['storage']['layers']['gold']
    
    def _aggregate_by_dimensions(
        self,
        df: DataFrame,
        dimensions: List[str],
        include_count: bool = True,
        include_percentage: bool = False,
        additional_aggs: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """
        GENERIC: Aggregate DataFrame by specified dimensions.
        
        This is a reusable utility for creating dimension-based aggregations.
        Domain-specific methods should call this with their business dimensions.
        
        Args:
            df: Source DataFrame
            dimensions: List of columns to group by (e.g., ['country', 'state'])
            include_count: Add 'record_count' column (default True)
            include_percentage: Add 'percentage_of_total' column (default False)
            additional_aggs: Dict of additional aggregations
                Format: {'alias': ('column', 'agg_func')} or {'alias': 'count'}
                Example: {'avg_score': ('data_quality_score', 'avg')}
        
        Returns:
            Aggregated DataFrame with _created_at timestamp
            
        Example:
            df = self._aggregate_by_dimensions(
                source_df,
                dimensions=['brewery_type'],
                include_count=True,
                include_percentage=True,
                additional_aggs={'avg_quality': ('data_quality_score', 'avg')}
            )
        """
        missing_dims = [d for d in dimensions if d not in df.columns]
        if missing_dims:
            raise ValueError(f"Missing dimensions in DataFrame: {missing_dims}")
        
        agg_list = []
        
        if include_count:
            agg_list.append(count('*').alias('record_count'))
        
        if additional_aggs:
            for alias, agg_spec in additional_aggs.items():
                if isinstance(agg_spec, str) and agg_spec == 'count':
                    agg_list.append(count('*').alias(alias))
                elif isinstance(agg_spec, tuple):
                    col_name, func_name = agg_spec
                    if func_name == 'avg':
                        agg_list.append(avg(col_name).alias(alias))
                    elif func_name == 'sum':
                        agg_list.append(_sum(col_name).alias(alias))
                    elif func_name == 'min':
                        agg_list.append(_min(col_name).alias(alias))
                    elif func_name == 'max':
                        agg_list.append(_max(col_name).alias(alias))
                    elif func_name == 'count_distinct':
                        agg_list.append(countDistinct(col_name).alias(alias))
                    elif func_name == 'collect_set':
                        agg_list.append(collect_set(col_name).alias(alias))
        
        result_df = df.groupBy(*dimensions).agg(*agg_list)
        
        if include_percentage and include_count:
            total = df.count()
            result_df = result_df.withColumn(
                'percentage_of_total',
                _round((col('record_count') / lit(total)) * 100, 2)
            )
        
        result_df = result_df.withColumn('_created_at', current_timestamp())
        
        return result_df
    
    def _write_aggregation(
        self,
        df: DataFrame,
        aggregation_name: str,
        execution_date: str,
        mode: str = 'overwrite'
    ) -> int:
        """
        GENERIC: Write aggregation DataFrame to Gold layer with daily partitioning.
        
        Uses timestamped folder naming for incremental daily runs:
        - Format: {aggregation_name}_gold_{execution_date}_{timestamp}
        - Example: breweries_by_location_gold_2026-01-26_20260127_054115
        
        If the same execution_date is re-run on the same day, a new folder
        with updated timestamp is created (partition overwrite pattern).
        
        Args:
            df: Aggregated DataFrame to write
            aggregation_name: Name for the aggregation (e.g., 'breweries_by_location')
            execution_date: Airflow logical date in YYYY-MM-DD format
            mode: Write mode ('overwrite' or 'append')
            
        Returns:
            Number of records written
            
        Why timestamped folders:
        - Incremental: Each daily run creates identifiable output
        - Idempotent: Re-runs on same day create new folders (no corruption)
        - Auditable: Execution date + creation timestamp for full lineage
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        folder_name = f"{aggregation_name}_gold_{execution_date}_{timestamp}"
        
        output_path = os.path.join(
            self.base_path,
            self.gold_config['path'],
            folder_name
        )
        
        df_with_metadata = df.withColumn(
            '_execution_date', lit(execution_date)
        ).withColumn(
            '_processing_timestamp', lit(timestamp)
        )
        
        df_with_metadata.write.mode(mode).format('delta').save(output_path)
        
        row_count = df.count()
        logger.info(
            f"Written {row_count} records to Gold/{folder_name} "
            f"(execution_date={execution_date})"
        )
        
        return row_count
    
    def read_gold_aggregation(
        self, 
        aggregation_name: str,
        execution_date: Optional[str] = None
    ) -> DataFrame:
        """
        GENERIC: Read a Gold layer aggregation.
        
        Supports the new timestamped folder naming convention:
        - If execution_date provided: finds latest folder for that date
        - If no execution_date: finds the most recent folder overall
        
        Args:
            aggregation_name: Base name of the aggregation (e.g., 'breweries_by_location')
            execution_date: Optional execution date filter (YYYY-MM-DD)
            
        Returns:
            DataFrame with aggregation data from the latest matching folder
            
        Example:
            # Read latest overall
            df = gold.read_gold_aggregation('breweries_by_location')
            
            # Read latest for specific execution date
            df = gold.read_gold_aggregation('breweries_by_location', '2026-01-26')
        """
        gold_base_path = os.path.join(
            self.base_path,
            self.gold_config['path']
        )
        
        folder_pattern = f"{aggregation_name}_gold_"
        matching_folders = []
        
        try:
            if os.path.exists(gold_base_path):
                for folder in os.listdir(gold_base_path):
                    if folder.startswith(folder_pattern):
                        if execution_date:
                            if f"_gold_{execution_date}_" in folder:
                                matching_folders.append(folder)
                        else:
                            matching_folders.append(folder)
        except Exception as e:
            logger.warning(f"Error listing Gold folders: {e}")
        
        if matching_folders:
            matching_folders.sort(reverse=True)
            latest_folder = matching_folders[0]
            gold_path = os.path.join(gold_base_path, latest_folder)
            logger.info(f"Reading Gold aggregation from: {latest_folder}")
        else:
            raise FileNotFoundError(
                f"No Gold aggregation found for '{aggregation_name}' "
                f"with execution_date='{execution_date}'"
            )
        
        return self.spark.read.format('delta').load(gold_path)
