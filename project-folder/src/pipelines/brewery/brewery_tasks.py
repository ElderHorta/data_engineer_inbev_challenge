from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, avg, min as _min, max as _max,
    sum as _sum, round as _round, when, desc, current_timestamp
)

from src.utils.logger import get_logger
from src.utils.api_helpers import check_api_health
from src.pipelines.brewery.brewery_api_client import BreweryAPIClient
from src.layers.bronze_layer import BronzeLayer
from src.layers.silver_layer import SilverLayer
from src.layers.gold_layer import GoldLayer
from src.quality.validators import BronzeValidator, SilverValidator, GoldValidator
from src.monitoring.metrics import MetricsCollector
from src.utils.config import get_config


logger = get_logger(__name__)


def check_brewery_api_health(**context) -> bool:
    """
    Check if Open Brewery DB API is available and healthy.
    
    Wrapper function for Airflow that instantiates the BreweryAPIClient
    and calls the generic check_api_health utility.
    
    Args:
        **context: Airflow context dictionary (not used but required by PythonOperator)
        
    Returns:
        bool: True if API is healthy
        
    Raises:
        Exception: If API health check fails
    """
    logger.info("Checking Open Brewery DB API health")
    client = BreweryAPIClient()
    return check_api_health(client, api_name="Open Brewery DB", raise_on_failure=True)


def extract_brewery_db_api(**context) -> int:
    """
    Extract data from Open Brewery DB API and save to Bronze layer.
    
    Fetches all breweries using pagination and persists raw data
    with audit metadata (ingestion timestamp, source, etc.).
    
    Args:
        **context: Airflow context dictionary
            - ds: Execution date in YYYY-MM-DD format
            - task_instance: For XCom push
        
    Returns:
        int: Number of records saved to Bronze layer
        
    XCom:
        Pushes 'bronze_record_count' for downstream tasks
    """
    execution_date = context['ds']
    logger.info(f"Starting Bronze extraction for {execution_date}")
    
    client = BreweryAPIClient()
    bronze = BronzeLayer()
    
    breweries = client.fetch_all_breweries()
    record_count = len(breweries)
    logger.info(f"Fetched {record_count} breweries from API")
    
    bronze.save_json_data(breweries, execution_date, source='api')
    
    context['task_instance'].xcom_push(key='bronze_record_count', value=record_count)
    logger.info(f"Bronze extraction complete: {record_count} records saved")
    return record_count


def validate_brewery_bronze(**context) -> Dict[str, Any]:
    """
    Validate Bronze layer data quality for brewery data.
    
    Uses BronzeValidator for layer-specific validation rules:
    - Fields validation (required fields present)
    - Record count thresholds
    - No empty records
    
    Args:
        **context: Airflow context dictionary
            - ds: Execution date for partition path
        
    Returns:
        Dict with validation results (passed, errors, metrics)
        
    Raises:
        Exception: If validation fails
    """
    execution_date = context['ds']
    config = get_config()
    
    logger.info(f"Starting Bronze validation for {execution_date}")
    
    validator = BronzeValidator()
    
    base_path = config['storage']['base_path']
    bronze_layer_path = config['storage']['layers']['bronze']['path']
    bronze_path = f"{base_path}{bronze_layer_path}"

    results = validator.validate(bronze_path, execution_date=execution_date)
    
    if not results['passed']:
        raise Exception(f"Bronze validation failed: {results['errors']}")
    
    logger.info(f"Bronze validation passed: {results.get('metrics', {})}")
    return results


def transform_brewery_to_silver(**context) -> Dict[str, Any]:
    """
    Transform Bronze brewery data to Silver layer.
    
    Applies transformations defined in brewery_config.yaml:
    - Deduplication (keep latest by ID)
    - Field standardization (country/state normalization)
    - Data type conversion (coordinates to float)
    - Null handling (empty strings to null)
    - Partitioning by country/state
    
    Args:
        **context: Airflow context dictionary
            - ds: Execution date to read from Bronze
            - task_instance: For XCom push
        
    Returns:
        Dict with transformation metrics (records_in, records_out, etc.)
        
    XCom:
        Pushes 'silver_metrics' for downstream tasks
    """
    execution_date = context['ds']
    logger.info(f"Starting Silver transformation for {execution_date}")
    
    silver = SilverLayer()
    metrics = silver.transform_brewery_to_silver(execution_date)
    
    # Push to XCom for metrics collection
    context['task_instance'].xcom_push(key='silver_metrics', value=metrics)
    
    logger.info(f"Silver transformation complete: {metrics}")
    return metrics


def validate_brewery_silver(**context) -> Dict[str, Any]:
    """
    Validate Silver layer data quality for brewery data.
    
    Uses SilverValidator for layer-specific validation rules:
    - No duplicate IDs
    - Required fields non-null
    - Valid coordinate ranges
    - Valid brewery types
    
    Args:
        **context: Airflow context dictionary
        
    Returns:
        Dict with validation results (passed, errors, metrics)
        
    Raises:
        Exception: If validation fails
    """
    config = get_config()
    execution_date = context['ds']
    
    logger.info(f"Starting Silver validation for {execution_date}")
    
    validator = SilverValidator()
    
    # Build path from configuration (not hardcoded)
    base_path = config['storage']['base_path']
    silver_layer_path = config['storage']['layers']['silver']['path']
    silver_path = f"{base_path}{silver_layer_path}"
    
    # Pass processing_date to validate only the current run's data
    # This is important because Silver stores daily snapshots partitioned by date
    # Same IDs in different dates is expected (daily full refresh)
    results = validator.validate(silver_path, processing_date=execution_date)
    
    if not results['passed']:
        raise Exception(f"Silver validation failed: {results['errors']}")
    
    logger.info(f"Silver validation passed: {results.get('metrics', {})}")
    return results


def aggregate_brewery_to_gold(**context) -> Dict[str, Any]:
    """
    Create brewery business aggregations in Gold layer.
    
    Creates brewery-specific aggregated views with daily partitioning:
    - breweries_by_type_gold_{date}_{timestamp}: Distribution of brewery types
    - breweries_by_location_gold_{date}_{timestamp}: Geographic distribution
    - breweries_by_type_location_gold_{date}_{timestamp}: Type by location
    - brewery_data_quality_metrics_gold_{date}_{timestamp}: Quality summary
    
    Folder naming includes execution_date (Airflow logical date) and
    processing timestamp for full audit trail and idempotent re-runs.
    
    Args:
        **context: Airflow context dictionary
            - ds: Execution date in YYYY-MM-DD format (logical date)
            - task_instance: For XCom push
        
    Returns:
        Dict with aggregation metrics (tables created, record counts)
        
    XCom:
        Pushes 'gold_metrics' for downstream tasks
    """
    execution_date = context['ds']
    logger.info(f"Starting Gold aggregation for {execution_date}")
    
    metrics = create_brewery_aggregations(execution_date)
    
    # Push to XCom for metrics collection
    context['task_instance'].xcom_push(key='gold_metrics', value=metrics)
    
    logger.info(f"Gold aggregation complete for {execution_date}: {metrics}")
    return metrics


def create_brewery_aggregations(execution_date: str) -> Dict[str, Any]:
    """
    Create all brewery business metrics in Gold layer.
    
    Orchestrates the creation of brewery-specific Gold tables using
    GoldLayer's generic aggregation utilities.
    
    Gold Tables Created (with daily partitioning):
    1. breweries_by_type_gold_{date}_{timestamp}: Distribution of brewery types
    2. breweries_by_location_gold_{date}_{timestamp}: Geographic distribution
    3. breweries_by_type_location_gold_{date}_{timestamp}: Type by location
    4. brewery_data_quality_metrics_gold_{date}_{timestamp}: Data health metrics
    
    Args:
        execution_date: Airflow logical date in YYYY-MM-DD format
        
    Returns:
        Dict with metrics for each aggregation created
    """
    logger.info(f"Starting brewery Gold layer aggregations for {execution_date}")
    
    gold = GoldLayer()
    silver = SilverLayer()
    
    df = silver.read_silver_data()
    total_records = df.count()
    logger.info(f"Total brewery records in Silver: {total_records}")
    
    metrics = {
        'total_source_records': total_records,
        'execution_date': execution_date,
        'aggregations': {}
    }
    
    metrics['aggregations']['breweries_by_type'] = _create_breweries_by_type(
        gold, df, execution_date
    )
    
    metrics['aggregations']['breweries_by_location'] = _create_breweries_by_location(
        gold, df, execution_date
    )
    
    metrics['aggregations']['breweries_by_type_location'] = _create_breweries_by_type_location(
        gold, df, execution_date
    )
    
    metrics['aggregations']['brewery_data_quality_metrics'] = _create_brewery_quality_metrics(
        gold, df, execution_date
    )
    
    logger.info(f"Brewery Gold aggregations complete for {execution_date}. Metrics: {metrics}")
    return metrics


def _create_breweries_by_type(
    gold: GoldLayer,
    df: DataFrame,
    execution_date: str
) -> Dict[str, int]:
    """
    Business Metric: Brewery distribution by type.
    
    Purpose: Understand market composition - how many micro vs large breweries?
    
    Business Questions Answered:
    - What's the dominant brewery type in the market?
    - How does type distribution compare to industry benchmarks?
    - Which types are underrepresented (opportunity analysis)?
    """
    logger.info(f"Creating breweries_by_type aggregation for {execution_date}")
    
    agg_df = gold._aggregate_by_dimensions(
        df,
        dimensions=['brewery_type'],
        include_count=True,
        include_percentage=True
    ).orderBy(desc('record_count'))
    
    row_count = gold._write_aggregation(agg_df, 'breweries_by_type', execution_date)
    
    return {'record_count': row_count}


def _create_breweries_by_location(
    gold: GoldLayer,
    df: DataFrame,
    execution_date: str
) -> Dict[str, int]:
    """
    Business Metric: Brewery distribution by geographic location.
    
    Purpose: Geographic market analysis for expansion and distribution planning.
    
    Business Questions Answered:
    - Which countries/states have the most breweries?
    - Where are the underserved markets?
    - How diverse is each region (type variety)?
    """
    logger.info(f"Creating breweries_by_location aggregation for {execution_date}")
    
    agg_df = gold._aggregate_by_dimensions(
        df,
        dimensions=['country', 'state'],
        include_count=True,
        include_percentage=True,
        additional_aggs={
            'city_count': ('city', 'count_distinct'),
            'brewery_types': ('brewery_type', 'collect_set')
        }
    ).orderBy(desc('record_count'))
    
    row_count = gold._write_aggregation(agg_df, 'breweries_by_location', execution_date)
    
    return {'record_count': row_count}


def _create_breweries_by_type_location(
    gold: GoldLayer,
    df: DataFrame,
    execution_date: str
) -> Dict[str, int]:
    """
    Business Metric: Brewery type distribution by location.
    
    Purpose: Detailed breakdown for competitive and market analysis.
    
    Business Questions Answered:
    - Which locations have the most microbreweries?
    - Where are large commercial breweries concentrated?
    - What's the competitive density by type in each market?
    """
    logger.info(f"Creating breweries_by_type_location aggregation for {execution_date}")
    
    agg_df = gold._aggregate_by_dimensions(
        df,
        dimensions=['country', 'state', 'city', 'brewery_type'],
        include_count=True,
        include_percentage=True
    ).orderBy('country', 'state', 'city', 'brewery_type')
    
    row_count = gold._write_aggregation(agg_df, 'breweries_by_type_location', execution_date)
    
    return {'record_count': row_count}


def _create_brewery_quality_metrics(
    gold: GoldLayer,
    df: DataFrame,
    execution_date: str
) -> Dict[str, Any]:
    """
    Operational Metric: Data quality health summary.
    
    Purpose: Monitor data pipeline health and identify data quality issues.
    
    Business Questions Answered:
    - What percentage of records have valid coordinates?
    - What's the average data quality score?
    - How complete is our address data?
    """
    logger.info(f"Creating brewery_data_quality_metrics aggregation for {execution_date}")
    
    agg_list = [
        count('*').alias('total_records'),
        countDistinct('country').alias('country_count'),
        countDistinct('state').alias('state_count'),
        countDistinct('city').alias('city_count'),
        countDistinct('brewery_type').alias('brewery_type_count')
    ]
    
    if 'data_quality_score' in df.columns:
        agg_list.extend([
            _round(avg('data_quality_score'), 2).alias('avg_quality_score'),
            _min('data_quality_score').alias('min_quality_score'),
            _max('data_quality_score').alias('max_quality_score')
        ])
    
    if 'is_valid_location' in df.columns:
        agg_list.append(
            _sum(when(col('is_valid_location'), 1).otherwise(0)).alias('valid_location_count')
        )
    
    if 'has_complete_address' in df.columns:
        agg_list.append(
            _sum(when(col('has_complete_address'), 1).otherwise(0)).alias('complete_address_count')
        )
    
    if 'phone' in df.columns:
        agg_list.append(
            _sum(when(col('phone').isNotNull(), 1).otherwise(0)).alias('records_with_phone')
        )
    
    if 'website_url' in df.columns:
        agg_list.append(
            _sum(when(col('website_url').isNotNull(), 1).otherwise(0)).alias('records_with_website')
        )
    
    metrics_df = df.agg(*agg_list).withColumn('_created_at', current_timestamp())
    
    row_count = gold._write_aggregation(
        metrics_df, 'brewery_data_quality_metrics', execution_date
    )
    
    metrics_row = metrics_df.collect()[0]
    metrics_dict = metrics_row.asDict()
    return {
        'record_count': row_count,
        'total_records': metrics_dict['total_records'],
        'avg_quality_score': metrics_dict.get('avg_quality_score'),
        'country_count': metrics_dict['country_count']
    }


def validate_brewery_gold(**context) -> Dict[str, Any]:
    """
    Validate Gold layer data quality for brewery aggregations.
    
    Uses GoldValidator for layer-specific validation rules:
    - All expected aggregation tables exist (with timestamped naming)
    - No negative values in counts
    - Aggregation sums match source data
    
    Supports new folder naming convention:
    - Format: {aggregation_name}_gold_{execution_date}_{timestamp}
    - Example: breweries_by_type_gold_2026-01-26_20260127_054115
    
    Args:
        **context: Airflow context dictionary
            - ds: Execution date for filtering Gold folders
        
    Returns:
        Dict with validation results (passed, errors, metrics)
        
    Raises:
        Exception: If validation fails
    """
    config = get_config()
    execution_date = context['ds']
    
    logger.info(f"Starting Gold validation for {execution_date}")
    
    validator = GoldValidator()
    
    base_path = config['storage']['base_path']
    gold_layer_path = config['storage']['layers']['gold']['path']
    gold_path = f"{base_path}{gold_layer_path}"
    
    expected_aggregations = [
        'breweries_by_type',
        'breweries_by_location',
        'breweries_by_type_location',
        'brewery_data_quality_metrics'
    ]
    
    # Pass execution_date to validate only the current run's Gold data
    results = validator.validate(
        gold_path, 
        expected_aggregations=expected_aggregations,
        execution_date=execution_date
    )
    
    if not results['passed']:
        raise Exception(f"Gold validation failed: {results['errors']}")
    
    logger.info(f"Gold validation passed for {execution_date}: {results.get('metrics', {})}")
    return results


def collect_metrics(**context) -> bool:
    """
    Collect and publish pipeline metrics.
    
    Gathers metrics from all previous tasks via XCom and publishes
    to the configured metrics backend (file, Prometheus, etc.).
    
    Args:
        **context: Airflow context dictionary
            - ds: Pipeline run date
            - task_instance: For XCom pull and duration
        
    Returns:
        bool: True if metrics published successfully
        
    XCom Dependencies:
        - bronze_record_count from extract_brewery_db_api
        - silver_metrics from transform_to_silver
        - gold_metrics from aggregate_to_gold
    """
    ti = context['task_instance']
    
    logger.info("Collecting pipeline metrics")
    
    metrics_collector = MetricsCollector()
    
    # Gather metrics from previous tasks via XCom
    # Note: task_ids use the full path including task group prefix
    bronze_count = ti.xcom_pull(
        key='bronze_record_count', 
        task_ids='bronze_layer.extract_brewery_db_api'
    )
    silver_metrics = ti.xcom_pull(
        key='silver_metrics', 
        task_ids='silver_layer.transform_to_silver'
    )
    gold_metrics = ti.xcom_pull(
        key='gold_metrics', 
        task_ids='gold_layer.aggregate_to_gold'
    )
    
    # Publish aggregated metrics
    metrics_collector.publish({
        'pipeline_run_date': context['ds'],
        'bronze_record_count': bronze_count,
        'silver_metrics': silver_metrics,
        'gold_metrics': gold_metrics,
        'execution_time': ti.duration,
    })
    
    logger.info("Pipeline metrics published successfully")
    return True
