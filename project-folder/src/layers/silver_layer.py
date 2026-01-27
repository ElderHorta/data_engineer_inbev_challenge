"""
Silver Layer - Cleaned and Standardized Data

Responsibilities:
- Load and deduplicate raw data from Bronze layer JSON files
- Parse JSON payloads from Bronze layer
- Standardize data formats
- Type conversions
- Data validation
- Partition by location
"""

import os
import json
from datetime import datetime
from typing import Dict, Optional, Callable, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, trim, upper, lower, when, regexp_replace,
    coalesce, lit, current_timestamp, 
    row_number, desc, from_json, get_json_object, max as spark_max
)
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType, DoubleType, StructType, StructField, StringType

from src.utils.logger import get_logger
from src.utils.config import get_config
from src.utils.spark_session import get_spark_session
from src.utils.schema import build_json_parsing_schema, get_field_types, get_required_fields

logger = get_logger(__name__)


class SilverLayer:
    """Handles Silver layer operations - cleaned and standardized data.
    
    Generic Silver layer processing for any data source.
    Provides common data quality transformations that can be customized
    for specific business needs.
    """
    
    def __init__(self, config: Optional[Dict] = None, spark: Optional[SparkSession] = None):
        """
        Initialize Silver layer with dependency injection.
        
        Args:
            config: Configuration dictionary (if None, loads from default)
            spark: Spark session (if None, creates new session)
            
        Why dependency injection:
        - Enables unit testing with mocked config/Spark
        - Follows SOLID principles for better testability
        """
        self.config = config if config is not None else get_config()
        self.spark = spark if spark is not None else get_spark_session()
        self.base_path = self.config['storage']['base_path']
        self.silver_config = self.config['storage']['layers']['silver']
        self.bronze_config = self.config['storage']['layers']['bronze']
        
        self.schema_config = self.config.get('schemas', {}).get('silver', {})
        self._json_parsing_schema = None
    
    def load_from_bronze_json(
        self,
        ingestion_date: str,
        id_column: str,
        payload_parser: Callable[[DataFrame], DataFrame],
        source_filter: Optional[str] = None,
    ) -> DataFrame:
        """
        GENERIC method to load and deduplicate data from Bronze layer JSON files.
        
        This is a reusable method for any data source stored in Bronze layer as JSON files.
        Bronze layer stores raw data (append-only, immutable).
        Multiple files may exist for the same date from pipeline reruns.
        
        This method:
        1. Reads all JSON files matching the ingestion_date pattern
        2. Converts records to Spark DataFrame with payload column
        3. Parses JSON structure using provided parser function
        4. Deduplicates by id_column, keeping most recent record
        
        Args:
            ingestion_date: Date to load (YYYY-MM-DD format)
                           Matches Airflow execution date for daily pipelines
            id_column: Column name to use as unique identifier for deduplication
                       Examples: 'id' (breweries), 'order_id' (orders), 'station_id' (weather)
            payload_parser: Function that takes Bronze DataFrame and returns
                           parsed DataFrame with typed columns extracted from payload.
                           Each data source needs its own parser (e.g., _parse_brewery_payload)
            source_filter: Optional filter for source in filename (e.g., 'api', 'kafka', 'batch')
                          If None, loads all sources for that date
            
        Returns:
            DataFrame with deduplicated records (most recent version of each)
            
        Example - Brewery data:
            >>> df = silver.load_from_bronze_json(
            ...     ingestion_date="2026-01-23",
            ...     id_column="id",
            ...     payload_parser=self._parse_brewery_payload,
            ...     source_filter="api"
            ... )
            
        Example - Weather data (hypothetical):
            >>> df = silver.load_from_bronze_json(
            ...     ingestion_date="2026-01-23",
            ...     id_column="station_id",
            ...     payload_parser=self._parse_weather_payload,
            ...     source_filter="noaa"
            ... )
            
        Why this design (Open/Closed Principle):
        - Open for extension: Add new data sources by creating new parsers
        - Closed for modification: Core loading/deduplication logic unchanged
        - Each data source only needs to implement its payload_parser function
        """

        logger.info(f"Loading Bronze data for ingestion_date={ingestion_date}")
        
        bronze_dir = os.path.join(
            self.base_path,
            self.bronze_config['path'],
        )
        
        logger.info(f"Reading Bronze JSON files from: {bronze_dir}")
        
        all_records = []
        files_found = 0
        
        if not os.path.exists(bronze_dir):
            raise FileNotFoundError(
                f"Bronze directory does not exist: {bronze_dir}"
            )
        
        for filename in os.listdir(bronze_dir):
            if filename.endswith('.json') and f"_{ingestion_date}_" in filename:
                if source_filter and f"_{source_filter}_" not in filename:
                    continue
                    
                filepath = os.path.join(bronze_dir, filename)
                with open(filepath, 'r', encoding='utf-8') as f:
                    records = json.load(f)
                    all_records.extend(records)
                    files_found += 1
                    logger.info(f"  Loaded {len(records)} records from {filename}")
        
        if files_found == 0:
            raise FileNotFoundError(
                f"No Bronze data found for ingestion_date={ingestion_date} at {bronze_dir}"
            )
        
        total_records = len(all_records)
        logger.info(f"Loaded {total_records} records from {files_found} Bronze file(s)")
        
        bronze_records = [
            {
                '_ingest_ts': datetime.now(),
                'payload': json.dumps(record)
            }
            for record in all_records
        ]
        
        bronze_df = self.spark.createDataFrame(bronze_records)
        
        logger.info("Parsing JSON payloads from Bronze")
        parsed_df = payload_parser(bronze_df)
        logger.info(f"Deduplicating by '{id_column}', keeping most recent (_ingest_ts)")
        
        window = Window.partitionBy(id_column).orderBy(desc("_ingest_ts"))
        
        deduplicated_df = (
            parsed_df
            .withColumn("_row_num", row_number().over(window))
            .filter(col("_row_num") == 1)
            .drop("_row_num")
        )
        
        final_count = deduplicated_df.count()
        duplicates_removed = total_records - final_count
        
        logger.info(f"✓ Deduplication complete: {final_count} unique records")
        if duplicates_removed > 0:
            logger.info(f"  - Removed {duplicates_removed} duplicates (from reruns)")
        
        return deduplicated_df
    
    def transform_brewery_to_silver(self, ingestion_date: str) -> Dict:
        """
        Transform Brewery data from Bronze to Silver layer for a specific date.
        
        This is a BREWERY-SPECIFIC orchestration method that:
        1. Loads Bronze JSON data using the generic load_from_bronze_json method
        2. Applies brewery-specific parsing via _parse_brewery_payload
        3. Applies brewery-specific transformations (schema, standardization, quality)
        4. Saves to Silver layer with brewery partitioning
        
        For other data sources (e.g., weather, orders), create similar methods:
        - transform_weather_to_silver()
        - transform_orders_to_silver()
        
        Each would use load_from_bronze_json() with their own:
        - id_column (unique identifier for that data)
        - payload_parser (parsing function for that data's JSON structure)
        - source_filter (filename pattern for that data)
        
        Args:
            ingestion_date: Airflow execution date (YYYY-MM-DD format)
                           Determines which Bronze partition to process
            
        Returns:
            Dict with transformation metrics
            
        Why brewery-specific:
        - Uses 'id' as unique identifier (brewery ID from Open Brewery DB)
        - Uses 'api' source filter (matches brewery_bronze_api_*.json files)
        - Applies brewery-specific transformations (type, location standardization)
        """
        logger.info(f"Starting Brewery Bronze to Silver transformation for {ingestion_date}")
        
        df = self.load_from_bronze_json(
            ingestion_date=ingestion_date,
            id_column="id",
            payload_parser=self._parse_brewery_payload,
            source_filter="api"
        )
        
        initial_count = df.count()
        logger.info(f"Loaded {initial_count} unique brewery records from Bronze")
        
        df = self._enforce_schema(df)
        df = self._standardize_fields(df)
        df = self._handle_nulls(df)
        df = self._add_quality_flags(df)
        df = df.withColumn('_processing_timestamp', current_timestamp())
        df = df.withColumn('_layer', lit('silver'))
        
        final_count = df.count()
        logger.info(f"After transformations, {final_count} records ready for Silver")
        
        save_metrics = self.save_to_silver(
            df=df,
            processing_date=ingestion_date,
            partition_columns=self.silver_config.get('partition_by', ['country', 'state']),
        )
        
        metrics = {
            'initial_count': initial_count,
            'final_count': final_count,
            'records_processed': final_count,
            'save_metrics': save_metrics,
        }
        
        logger.info(f"Brewery Silver transformation complete. Metrics: {metrics}")
        
        return metrics
    
    def _get_json_parsing_schema(self) -> StructType:
        """
        Get schema for JSON parsing from configuration.
        
        Lazy-loads schema from config on first call, then caches.
        Uses brewery_config.yaml schemas.silver.fields definition.
        
        Returns:
            PySpark StructType for parsing JSON (all fields as StringType)
            
        Why all fields as StringType:
        - JSON numbers can be inconsistent (42 vs 42.0)
        - Avoids CANNOT_MERGE_TYPE errors during parsing
        - Type conversion happens in _enforce_schema step
        """
        if self._json_parsing_schema is None:
            fields_config = self.schema_config.get('fields', [])
            if fields_config:
                self._json_parsing_schema = build_json_parsing_schema(fields_config)
                logger.info(f"Built JSON parsing schema from config: {len(fields_config)} fields")
            else:
                logger.warning("No schema config found, using fallback schema")
                self._json_parsing_schema = StructType([
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("brewery_type", StringType(), True),
                    StructField("address_1", StringType(), True),
                    StructField("address_2", StringType(), True),
                    StructField("address_3", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("state_province", StringType(), True),
                    StructField("postal_code", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("longitude", StringType(), True),
                    StructField("latitude", StringType(), True),
                    StructField("phone", StringType(), True),
                    StructField("website_url", StringType(), True),
                    StructField("state", StringType(), True),
                    StructField("street", StringType(), True),
                ])
        return self._json_parsing_schema

    def _parse_brewery_payload(self, bronze_df: DataFrame) -> DataFrame:
        """
        Parse brewery JSON payload from Bronze layer into typed columns.
        
        This is the payload_parser function for brewery data.
        Extracts brewery-specific fields from the raw JSON payload.
        Uses schema from configuration (schemas.silver.fields in brewery_config.yaml).
        
        Args:
            bronze_df: DataFrame with columns (_ingest_ts, _source, _ingestion_date, payload)
            
        Returns:
            DataFrame with parsed brewery columns + audit metadata
            
        Why this is a separate function:
        - Brewery-specific parsing logic
        - Can be passed to generic load_from_bronze_json method
        - Other Silver processes will have their own parsers
        """
        logger.info("Parsing brewery JSON payloads using config-driven schema")
        
        json_schema = self._get_json_parsing_schema()
        df = bronze_df.withColumn("parsed", from_json(col("payload"), json_schema))
        
        select_exprs = []
        for field in json_schema.fields:
            select_exprs.append(col(f"parsed.{field.name}").alias(field.name))
        
        select_exprs.append(col("_ingest_ts"))
        
        df = df.select(*select_exprs)
        
        logger.info(f"Parsed {len(df.columns)} columns from brewery payload")
        return df
    
    def _enforce_schema(self, df: DataFrame) -> DataFrame:
        """
        Enforce schema with config-driven type conversion and validation.
        
        Converts columns to their target types as defined in brewery_config.yaml
        under schemas.silver.fields. This is the AUTHORITATIVE schema enforcement
        step for Silver layer.
        
        Why schema enforcement AFTER deduplication:
        - Deduplication removes duplicate records first
        - We only type-cast and validate unique records
        - More efficient: don't process duplicates unnecessarily
        - Validation errors are cleaner (no duplicate error messages)
        
        Args:
            df: DataFrame with parsed columns (all as StringType)
            
        Returns:
            DataFrame with proper types as defined in config
        """
        logger.info("Enforcing schema from configuration")
        
        fields_config = self.schema_config.get('fields', [])
        if not fields_config:
            logger.warning("No schema config found, skipping schema enforcement")
            return df
        
        field_types = get_field_types(fields_config)
        
        type_cast_map = {
            'double': DoubleType(),
            'float': FloatType(),
            'string': StringType(),
        }
        
        for field_name, type_str in field_types.items():
            if field_name in df.columns:
                target_type = type_cast_map.get(type_str.lower())
                if target_type and type_str.lower() in ('double', 'float'):
                    df = df.withColumn(field_name, col(field_name).cast(target_type))
                    logger.debug(f"Cast {field_name} to {type_str}")
        
        required_fields = self.schema_config.get('required_fields', [])
        if required_fields:
            missing_in_df = [f for f in required_fields if f not in df.columns]
            if missing_in_df:
                raise ValueError(f"Missing required fields in DataFrame: {missing_in_df}")
            
            for field in required_fields:
                null_count = df.filter(col(field).isNull()).count()
                if null_count > 0:
                    logger.warning(f"Required field '{field}' has {null_count} null values")
        
        valid_types = self.schema_config.get('valid_brewery_types', [])
        if valid_types and 'brewery_type' in df.columns:
            invalid_count = df.filter(
                ~col('brewery_type').isin(valid_types) & col('brewery_type').isNotNull()
            ).count()
            if invalid_count > 0:
                logger.warning(f"Found {invalid_count} records with invalid brewery_type")
        
        logger.info("Schema enforcement complete")
        return df
    
    def _standardize_fields(self, df):
        """Standardize text fields.
        
        Generic text field standardization:
        - Trims whitespace from all string columns
        - Converts empty strings to NULL for consistency
        - Applies config-driven standardization rules (country, state, phone, postal_code)
        
        Override this method in subclasses for domain-specific standardization.
        """
        logger.info("Standardizing fields")
        
        string_columns = [field.name for field in df.schema.fields 
                         if str(field.dataType) == 'StringType()']
        
        for column in string_columns:
            if not column.startswith('_'):
                df = df.withColumn(column, trim(col(column)))
        
        for column in string_columns:
            df = df.withColumn(column,
                when(col(column) == '', None)
                .otherwise(col(column))
            )
        
        transformations_config = self.config.get('transformations', {}).get('silver', {})
        standardization = transformations_config.get('standardization', {})
        
        if 'country' in df.columns and 'country' in standardization:
            country_mappings = standardization['country']
            for from_value, to_value in country_mappings.items():
                df = df.withColumn('country',
                    when(lower(col('country')) == from_value.lower(), to_value)
                    .otherwise(col('country'))
                )
            logger.info(f"Applied country standardization: {len(country_mappings)} mappings")
        
        if 'state' in df.columns and 'state' in standardization:
            if standardization['state'].get('normalize'):
                df = df.withColumn('state', upper(col('state')))
                logger.info("Normalized state codes to uppercase")
        
        if 'phone' in df.columns and 'phone' in standardization:
            phone_config = standardization['phone']
            if phone_config.get('strip_non_digits'):
                df = df.withColumn('phone', 
                    regexp_replace(col('phone'), r'[^\d]', '')
                )
            if phone_config.get('format') == 'us':
                # Format as (XXX) XXX-XXXX for 10-digit US numbers
                df = df.withColumn('phone',
                    when(
                        (col('phone').isNotNull()) & 
                        (col('phone').rlike(r'^\d{10}$')),
                        regexp_replace(col('phone'), r'(\d{3})(\d{3})(\d{4})', '($1) $2-$3')
                    ).otherwise(col('phone'))
                )
            logger.info("Applied phone number standardization")
        
        if 'postal_code' in df.columns and 'postal_code' in standardization:
            postal_config = standardization['postal_code']
            if postal_config.get('strip_trailing_dash'):
                # Remove trailing dash (e.g., "48450-" -> "48450")
                df = df.withColumn('postal_code',
                    regexp_replace(col('postal_code'), r'-$', '')
                )
            logger.info("Applied postal code standardization")
        
        return df
    
    def _handle_nulls(self, df):
        """Handle null values with defaults.
        
        Generic null handling that sets defaults for common location fields.
        Uses default values from config (transformations.silver.null_handling.default_values).
        
        Override this method in subclasses for domain-specific null handling.
        """
        logger.info("Handling null values")
        
        transformations_config = self.config.get('transformations', {}).get('silver', {})
        null_handling = transformations_config.get('null_handling', {})
        default_values = null_handling.get('default_values', {})
        
        for field, default_value in default_values.items():
            if field in df.columns:
                df = df.withColumn(field,
                    coalesce(col(field), lit(default_value))
                )
                logger.info(f"Set default value for '{field}': {default_value}")
        
        return df
    
    def _add_quality_flags(self, df):
        """Add data quality flags.
        
        Generic quality flags for common patterns:
        - Location validity (if coordinates exist)
        - Address completeness (if address fields exist)
        - Basic data quality score
        
        Override this method in subclasses for domain-specific quality metrics.
        """
        logger.info("Adding quality flags")
        
        if 'latitude' in df.columns and 'longitude' in df.columns:
            df = df.withColumn('is_valid_location',
                when(
                    (col('latitude').isNotNull()) & 
                    (col('longitude').isNotNull()) &
                    (col('latitude').between(-90, 90)) &
                    (col('longitude').between(-180, 180)),
                    True
                ).otherwise(False)
            )
        
        address_columns = ['street', 'city', 'state', 'postal_code']
        if all(col_name in df.columns for col_name in address_columns):
            df = df.withColumn('has_complete_address',
                when(
                    (col('street').isNotNull()) &
                    (col('city').isNotNull()) &
                    (col('state').isNotNull()) &
                    (col('postal_code').isNotNull()),
                    True
                ).otherwise(False)
            )
        
        quality_score = lit(0)
        if 'id' in df.columns:
            quality_score = quality_score + when(col('id').isNotNull(), 25).otherwise(0)
        if 'name' in df.columns:
            quality_score = quality_score + when(col('name').isNotNull(), 25).otherwise(0)
        if 'is_valid_location' in df.columns:
            quality_score = quality_score + when(col('is_valid_location'), 25).otherwise(0)
        if 'has_complete_address' in df.columns:
            quality_score = quality_score + when(col('has_complete_address'), 25).otherwise(0)
        
        df = df.withColumn('data_quality_score', quality_score)
        
        return df
    
    def save_to_silver(
        self,
        df: DataFrame,
        processing_date: str,
        partition_columns: Optional[List[str]] = None,
        table_path: Optional[str] = None,
    ) -> Dict:
        """
        Save DataFrame to Silver layer with partition overwrite.
        
        Generic method to save any transformed data to Silver layer.
        Uses partition overwrite mode to handle daily reruns - if the pipeline
        runs again for the same date, only that date's partition is replaced.
        
        Args:
            df: DataFrame to save (must have processing_date column or it will be added)
            processing_date: Date string (YYYY-MM-DD) for partition
                            Typically matches Airflow execution date
            partition_columns: List of columns to partition by (default from config)
                              processing_date is always added as first partition
            table_path: Custom path for the table (default from config)
            
        Returns:
            Dict with save metrics (records_written, partitions, path)
            
        Example:
            >>> silver = SilverLayer()
            >>> metrics = silver.save_to_silver(
            ...     df=transformed_df,
            ...     processing_date="2026-01-23",
            ...     partition_columns=["country", "state"]
            ... )
            
        Why partition overwrite:
        - Daily pipeline may rerun (manual trigger, failure recovery)
        - Full overwrite would delete historical data
        - Partition overwrite replaces ONLY the current date's data
        - Safe for idempotent reruns without data loss
        
        Why processing_date as first partition:
        - Enables efficient partition pruning for date-based queries
        - Allows reprocessing specific dates without affecting others
        - Matches Airflow execution_date for pipeline observability
        """
        if table_path:
            silver_path = table_path
        else:
            silver_path = os.path.join(
                self.base_path,
                self.silver_config['path']
            )
        
        if partition_columns is None:
            partition_columns = self.silver_config.get('partition_by', [])
        
        if '_processing_date' not in df.columns:
            df = df.withColumn('_processing_date', lit(processing_date))
        
        final_partitions = ['_processing_date'] + [
            col for col in partition_columns if col != '_processing_date'
        ]
        
        record_count = df.count()
        logger.info(f"Saving {record_count} records to Silver layer: {silver_path}")
        logger.info(f"Partitions: {final_partitions}")
        logger.info(f"Processing date partition: {processing_date}")
        
        df.write \
            .format('delta') \
            .mode('overwrite') \
            .option('partitionOverwriteMode', 'dynamic') \
            .partitionBy(*final_partitions) \
            .save(silver_path)
        
        logger.info("Silver layer write complete (Delta format)")
        
        metrics = {
            'records_written': record_count,
            'partitions': final_partitions,
            'processing_date': processing_date,
            'path': silver_path,
        }
        
        logger.info(f"Save metrics: {metrics}")
        return metrics
    
    def _write_silver(self, df, processing_date: Optional[str] = None):
        """
        Write to Silver layer with partitioning.
        
        Legacy method that wraps save_to_silver for backward compatibility.
        New code should use save_to_silver directly for more control.
        
        Args:
            df: DataFrame to write
            processing_date: Optional date for partition (uses current date if None)
        """
        from datetime import date
        
        if processing_date is None:
            processing_date = date.today().strftime('%Y-%m-%d')
        
        self.save_to_silver(
            df=df,
            processing_date=processing_date,
            partition_columns=self.silver_config.get('partition_by', ['country', 'state']),
        )
    
    def read_silver_data(self, processing_date: Optional[str] = None) -> DataFrame:
        """
        Read Silver layer data.
        
        Args:
            processing_date: Optional date filter (YYYY-MM-DD)
                            If provided, reads only that date's partition
                            If None, reads all data
                            
        Returns:
            DataFrame with Silver layer data
        """
        silver_path = os.path.join(
            self.base_path,
            self.silver_config['path']
        )
        
        logger.info(f"Reading Silver data from {silver_path}")
        
        df = self.spark.read.format('delta').load(silver_path)
        logger.info("Read Silver data (Delta format)")
        
        if processing_date and '_processing_date' in df.columns:
            df = df.filter(col('_processing_date') == processing_date)
            logger.info(f"Filtered to processing_date={processing_date}")
        
        return df
