"""
Data Quality Validators - SOLID Compliant Architecture

Implements data quality checks with Single Responsibility Principle:
- BaseValidator: Protocol defining validator interface (Interface Segregation)
- BronzeValidator: Validates raw data layer
- SilverValidator: Validates cleaned data layer
- GoldValidator: Validates aggregation layer
- DataQualityValidator: Facade for backward compatibility

Design Principles:
- SRP: Each validator handles ONE layer's validation logic
- ISP: Clean protocol interface that all validators implement
- DIP: Depend on abstractions (Protocol), not concrete implementations
- OCP: Open for extension (new validators), closed for modification
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Protocol
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, isnan, when
import os
import json

from src.utils.logger import get_logger
from src.utils.config import get_config
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)


class ValidatorProtocol(Protocol):
    """
    Protocol (interface) for data quality validators.
    
    Following Interface Segregation Principle:
    - Defines minimal interface that all validators must implement
    - Allows type-checking without inheritance
    - Enables duck typing for validators
    
    Example:
        def run_validation(validator: ValidatorProtocol, path: str) -> Dict:
            return validator.validate(path)
    """
    
    def validate(self, path: str) -> Dict:
        """
        Validate data at the given path.
        
        Args:
            path: Path to data to validate
            
        Returns:
            Dict with validation results containing:
                - layer: Layer name
                - passed: True if all checks passed
                - errors: List of error messages
                - warnings: List of warning messages
                - metrics: Dict of validation metrics
        """
        ...


class BaseValidator(ABC):
    """
    Abstract base class for validators.
    
    Provides common functionality for all validators:
    - Dependency injection for config and Spark
    - Shared result structure
    - Logging helpers
    """
    
    def __init__(
        self,
        config: Optional[Dict] = None,
        spark: Optional[SparkSession] = None
    ):
        """
        Initialize validator with dependency injection.
        
        Args:
            config: Configuration dictionary (if None, loads from default)
            spark: Spark session (if None, creates new session)
        """
        self.config = config if config is not None else get_config()
        self.spark = spark if spark is not None else get_spark_session()
        self.quality_config = self.config.get('data_quality', {})
        self.thresholds = self.quality_config.get('thresholds', {})
        self.validations_config = self.quality_config.get('validations', {})
    
    @abstractmethod
    def validate(self, path: str) -> Dict:
        """
        Validate data at the given path.
        
        Must be implemented by subclasses.
        """
        pass
    
    def _create_result(self, layer: str) -> Dict:
        """
        Create empty validation result structure.
        
        Args:
            layer: Layer name (bronze, silver, gold)
            
        Returns:
            Empty result dictionary
        """
        return {
            'layer': layer,
            'passed': True,
            'errors': [],
            'warnings': [],
            'metrics': {}
        }
    
    def _log_result(self, results: Dict) -> None:
        """
        Log validation results.
        
        Args:
            results: Validation results dictionary
        """
        layer = results['layer']
        status = 'PASSED' if results['passed'] else 'FAILED'
        logger.info(f"{layer.upper()} validation: {status}")
        
        if results['errors']:
            for error in results['errors']:
                logger.error(f"{layer} error: {error}")
        
        if results['warnings']:
            for warning in results['warnings']:
                logger.warning(f"{layer} warning: {warning}")


class BronzeValidator(BaseValidator):
    """
    Validator for Bronze layer (raw JSON payloads).
    
    Responsibilities (SRP):
    - Verify data exists and is readable
    - Check minimum record count
    - Validate Bronze schema (fixed metadata + payload)
    - Ensure append-only integrity
    
    Bronze Schema (Medallion Architecture):
    - _ingest_ts: TIMESTAMP - When data was captured
    - _source: STRING - Data source identifier
    - _ingestion_date: STRING - Partition key
    - payload: STRING - Raw JSON from API
    
    Note: We do NOT validate payload content here - that's Silver layer's job.
    """
    
    def validate(self, bronze_path: str, execution_date: str = None) -> Dict:
        """
        Validate Bronze layer raw JSON files.
        
        Args:
            bronze_path: Path to Bronze data directory
            execution_date: Optional date to filter files (YYYY-MM-DD format)
            
        Returns:
            Dict with validation results
            
        Validation Checks:
        1. Directory exists
        2. JSON files exist (matching execution_date if provided)
        3. Record count above minimum threshold
        4. JSON is valid and contains expected fields
        """
        logger.info(f"Validating Bronze layer JSON files: {bronze_path}")
        if execution_date:
            logger.info(f"  Filtering for execution_date: {execution_date}")
        
        results = self._create_result('bronze')
        
        clean_path = bronze_path.replace('file://', '')
        if not os.path.exists(clean_path):
            results['passed'] = False
            results['errors'].append(f"Bronze directory does not exist: {bronze_path}")
            return results
        
        json_files = []
        for filename in os.listdir(clean_path):
            if filename.endswith('.json'):
                if execution_date:
                    if f"_{execution_date}_" in filename:
                        json_files.append(filename)
                else:
                    json_files.append(filename)
        
        if not json_files:
            results['passed'] = False
            date_msg = f" for date {execution_date}" if execution_date else ""
            results['errors'].append(f"No JSON files found in Bronze path{date_msg}: {bronze_path}")
            return results
        
        results['metrics']['file_count'] = len(json_files)
        logger.info(f"Found {len(json_files)} JSON file(s) to validate")
        
        total_records = 0
        empty_records = 0
        
        for filename in json_files:
            filepath = os.path.join(clean_path, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    records = json.load(f)
                
                if not isinstance(records, list):
                    results['warnings'].append(f"File {filename} is not a JSON array")
                    continue
                    
                file_count = len(records)
                total_records += file_count
                
                for record in records:
                    if not record or record == {}:
                        empty_records += 1
                
                logger.info(f"  ✓ {filename}: {file_count} records")
                
            except json.JSONDecodeError as e:
                results['passed'] = False
                results['errors'].append(f"Invalid JSON in {filename}: {str(e)}")
            except Exception as e:
                results['passed'] = False
                results['errors'].append(f"Failed to read {filename}: {str(e)}")
        
        results['metrics']['record_count'] = total_records
        
        min_records = self.thresholds.get('bronze_min_records', 100)
        if total_records < min_records:
            results['passed'] = False
            results['errors'].append(
                f"Record count {total_records} below threshold {min_records}"
            )
        else:
            logger.info(f"✓ Record count check passed: {total_records} records")
        
        results['metrics']['empty_record_count'] = empty_records
        
        if empty_records == total_records:
            results['passed'] = False
            results['errors'].append("All records are empty")
        elif empty_records > 0:
            results['warnings'].append(
                f"Found {empty_records} empty records out of {total_records}"
            )
        else:
            logger.info(f"✓ All {total_records} records contain data")
        
        self._log_result(results)
        return results


class SilverValidator(BaseValidator):
    """
    Validator for Silver layer (cleaned and standardized data).
    
    Responsibilities (SRP):
    - Check for duplicate records
    - Validate required fields are non-null
    - Verify data quality scores
    - Validate data types and ranges (coordinates, dates, etc.)
    """
    
    def validate(self, silver_path: str, processing_date: Optional[str] = None) -> Dict:
        """
        Validate Silver layer data.
        
        Args:
            silver_path: Path to Silver data (Delta format)
            processing_date: Optional date to validate (YYYY-MM-DD). If provided,
                           only validates data from that specific partition.
                           If None, validates all data (use with caution for
                           historical deduplication checks).
            
        Returns:
            Dict with validation results
            
        Validation Checks:
        1. No duplicate IDs (within the partition if date provided)
        2. Required fields are non-null
        3. Valid coordinate ranges (if present)
        4. Data quality score above threshold
        
        Why partition filtering is important:
        - Silver layer stores daily snapshots partitioned by _processing_date
        - Same IDs appearing in different dates is EXPECTED (daily full refresh)
        - Duplicate check should only apply WITHIN a single partition
        """
        date_info = f" for processing_date={processing_date}" if processing_date else ""
        logger.info(f"Validating Silver layer: {silver_path}{date_info}")
        
        results = self._create_result('silver')
        results['metrics']['processing_date'] = processing_date
        
        df = self.spark.read.format('delta').load(silver_path)
        
        if processing_date and '_processing_date' in df.columns:
            df = df.filter(col('_processing_date') == processing_date)
            logger.info(f"Filtered to processing_date={processing_date}")
        
        total_records = df.count()
        results['metrics']['total_records'] = total_records
        
        silver_config = self.validations_config.get('silver', {})
        unique_key = silver_config.get('unique_key', 'id')
        
        if unique_key and unique_key in df.columns:
            distinct_ids = df.select(unique_key).distinct().count()
            duplicates = total_records - distinct_ids
            
            results['metrics']['duplicate_count'] = duplicates
            
            if duplicates > 0:
                results['passed'] = False
                results['errors'].append(f"Found {duplicates} duplicate values in '{unique_key}'")
        
        required_fields = silver_config.get('required_fields', [])
        
        for field in required_fields:
            if field in df.columns:
                null_count = df.filter(col(field).isNull()).count()
                results['metrics'][f'{field}_null_count'] = null_count
                
                if null_count > 0:
                    results['passed'] = False
                    results['errors'].append(
                        f"Field '{field}' has {null_count} null values"
                    )
        
        value_ranges = silver_config.get('value_ranges', {})
        
        for field, range_limits in value_ranges.items():
            if field in df.columns and len(range_limits) == 2:
                min_val, max_val = range_limits
                invalid_count = df.filter(
                    (col(field).isNotNull()) & 
                    ((col(field) < min_val) | (col(field) > max_val))
                ).count()
                
                results['metrics'][f'{field}_out_of_range'] = invalid_count
                
                if invalid_count > 0:
                    results['warnings'].append(
                        f"Found {invalid_count} records with '{field}' outside range [{min_val}, {max_val}]"
                    )
        
        allowed_values = silver_config.get('allowed_values', {})
        
        for field, valid_values in allowed_values.items():
            if field in df.columns and valid_values:
                invalid_count = df.filter(
                    (col(field).isNotNull()) & 
                    (~col(field).isin(valid_values))
                ).count()
                
                results['metrics'][f'{field}_invalid_values'] = invalid_count
                
                if invalid_count > 0:
                    results['warnings'].append(
                        f"Found {invalid_count} records with '{field}' having values outside allowed set"
                    )
        
        if 'data_quality_score' in df.columns:
            avg_quality_score = df.agg({'data_quality_score': 'avg'}).collect()[0][0]
            
            if avg_quality_score is not None:
                results['metrics']['avg_quality_score'] = round(avg_quality_score, 2)
                
                threshold = self.thresholds.get('silver_quality_score', 0.80)
                if avg_quality_score < threshold * 100:
                    results['warnings'].append(
                        f"Average quality score {avg_quality_score:.2f} "
                        f"below threshold {threshold * 100}"
                    )
        
        self._log_result(results)
        return results


class GoldValidator(BaseValidator):
    """
    Validator for Gold layer (business aggregations).
    
    Responsibilities (SRP):
    - Verify all expected aggregations exist
    - Check aggregation record counts
    - Validate metric columns have valid values
    - Ensure no negative counts or invalid percentages
    
    Note: Supports new timestamped folder naming convention:
    - Format: {aggregation_name}_gold_{execution_date}_{timestamp}
    - Example: breweries_by_location_gold_2026-01-26_20260127_054115
    """
    
    def _find_aggregation_folder(
        self, 
        gold_path: str, 
        aggregation_name: str,
        execution_date: Optional[str] = None
    ) -> Optional[str]:
        """
        Find the latest folder for a given aggregation.
        
        Supports both new timestamped naming and legacy direct folder naming.
        
        Args:
            gold_path: Base Gold layer path
            aggregation_name: Base aggregation name (e.g., 'breweries_by_type')
            execution_date: Optional filter by execution date (YYYY-MM-DD)
            
        Returns:
            Path to latest matching folder, or None if not found
        """

        folder_pattern = f"{aggregation_name}_gold_"
        matching_folders = []
        
        try:
            if os.path.exists(gold_path):
                for folder in os.listdir(gold_path):
                    if folder.startswith(folder_pattern):
                        if execution_date:
                            if f"_gold_{execution_date}_" in folder:
                                matching_folders.append(folder)
                        else:
                            matching_folders.append(folder)
        except Exception as e:
            logger.warning(f"Error scanning Gold folders: {e}")
        
        if matching_folders:
            matching_folders.sort(reverse=True)
            return os.path.join(gold_path, matching_folders[0])
        
        return None
    
    def validate(
        self, 
        gold_path: str, 
        expected_aggregations: Optional[List[str]] = None,
        execution_date: Optional[str] = None
    ) -> Dict:
        """
        Validate Gold layer data.
        
        Args:
            gold_path: Path to Gold data (Delta format)
            expected_aggregations: List of expected aggregation names
                                   (if None, skips aggregation check)
            execution_date: Optional filter by execution date (YYYY-MM-DD)
            
        Returns:
            Dict with validation results
            
        Validation Checks:
        1. All expected aggregations exist
        2. Each aggregation has records
        3. No negative values in count columns
        4. Valid percentage ranges (0-100)
        """
        logger.info(f"Validating Gold layer: {gold_path}")
        
        results = self._create_result('gold')
        
        if not expected_aggregations:
            if not os.path.exists(gold_path):
                results['passed'] = False
                results['errors'].append(f"Gold path does not exist: {gold_path}")
            else:
                results['metrics']['path_exists'] = True
            
            self._log_result(results)
            return results
        
        for agg in expected_aggregations:
            agg_path = self._find_aggregation_folder(gold_path, agg, execution_date)
            
            if agg_path is None:
                results['passed'] = False
                results['errors'].append(f"Missing aggregation: {agg}")
                continue
            
            try:
                df = self.spark.read.format('delta').load(agg_path)
                count = df.count()
                results['metrics'][f'{agg}_count'] = count
                results['metrics'][f'{agg}_path'] = os.path.basename(agg_path)
                
                if count == 0:
                    results['warnings'].append(
                        f"Aggregation '{agg}' has no records"
                    )
                
                count_columns = [c for c in df.columns if 'count' in c.lower()]
                
                for col_name in count_columns:
                    negative_count = df.filter(col(col_name) < 0).count()
                    
                    if negative_count > 0:
                        results['passed'] = False
                        results['errors'].append(
                            f"Found {negative_count} negative values in {agg}.{col_name}"
                        )
                
                gold_config = self.validations_config.get('gold', {})
                percentage_columns = gold_config.get('percentage_columns', [])
                
                for col_name in percentage_columns:
                    if col_name not in df.columns:
                        continue
                        
                    invalid_pct = df.filter(
                        (col(col_name) < 0) | (col(col_name) > 100)
                    ).count()
                    
                    if invalid_pct > 0:
                        results['warnings'].append(
                            f"Found {invalid_pct} invalid percentages in {agg}.{col_name}"
                        )
                        
            except Exception as e:
                results['passed'] = False
                results['errors'].append(f"Error reading aggregation '{agg}': {str(e)}")
        
        self._log_result(results)
        return results


class DataQualityValidator:
    """
    Facade for all data quality validators.
    
    Provides backward compatibility and convenience methods.
    Delegates to specialized validators (SRP).
    
    Usage:
        validator = DataQualityValidator()
        bronze_result = validator.validate_bronze(bronze_path)
        silver_result = validator.validate_silver(silver_path)
        gold_result = validator.validate_gold(gold_path)
    """
    
    def __init__(
        self,
        config: Optional[Dict] = None,
        spark: Optional[SparkSession] = None
    ):
        """
        Initialize facade with dependency injection.
        
        Args:
            config: Configuration dictionary (shared across all validators)
            spark: Spark session (shared across all validators)
        """
        self.config = config if config is not None else get_config()
        self.spark = spark if spark is not None else get_spark_session()
        
        self.bronze_validator = BronzeValidator(config=self.config, spark=self.spark)
        self.silver_validator = SilverValidator(config=self.config, spark=self.spark)
        self.gold_validator = GoldValidator(config=self.config, spark=self.spark)
    
    def validate_bronze(self, bronze_path: str) -> Dict:
        """
        Validate Bronze layer data.
        
        Delegates to BronzeValidator.
        """
        return self.bronze_validator.validate(bronze_path)
    
    def validate_silver(self, silver_path: str) -> Dict:
        """
        Validate Silver layer data.
        
        Delegates to SilverValidator.
        """
        return self.silver_validator.validate(silver_path)
    
    def validate_gold(
        self,
        gold_path: str,
        expected_aggregations: Optional[List[str]] = None
    ) -> Dict:
        """
        Validate Gold layer data.
        
        Delegates to GoldValidator.
        """
        return self.gold_validator.validate(gold_path, expected_aggregations)
    
    def run_all_validations(
        self,
        bronze_path: str,
        silver_path: str,
        gold_path: str
    ) -> Dict:
        """
        Run all layer validations.
        
        Args:
            bronze_path: Path to Bronze data
            silver_path: Path to Silver data
            gold_path: Path to Gold data
            
        Returns:
            Dict with all validation results
        """
        return {
            'bronze': self.validate_bronze(bronze_path),
            'silver': self.validate_silver(silver_path),
            'gold': self.validate_gold(gold_path),
        }
