"""
Bronze Layer - Raw Data Persistence

Responsibilities (Single Responsibility Principle):
- Save raw API data WITHOUT any transformation or typing
- Store raw JSON files (append-only, immutable)
- Human-readable format for debugging and audit

Design Pattern:
- Uses dependency injection for config and Spark session (testability)
- Pure functions for data transformations (functional programming)
- Immutable raw data storage (each save creates new file)

Bronze Layer File Format:
- Raw JSON array of API records
- Filename: brewery_bronze_{source}_{execution_date}_{timestamp}.json
- Example: brewery_bronze_api_2026-01-25_20260126_193015.json
"""

import os
import json
from datetime import datetime
from typing import List, Dict, Optional
from pyspark.sql import SparkSession

from src.utils.logger import get_logger
from src.utils.config import get_config
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)


class BronzeLayer:
    """
    Handles Bronze layer operations - raw data persistence.
    
    The Bronze layer is the landing zone for raw data from any source.
    Data is stored as raw JSON files (append-only, immutable).
    NO schema enforcement or type conversion happens here.
    
    File naming convention:
        brewery_bronze_{source}_{execution_date}_{timestamp}.json
        Example: brewery_bronze_api_2026-01-25_20260126_193015.json
    
    Why raw JSON files:
    - Human-readable for debugging and audit
    - Append-only immutable files (true raw data lake)
    - No schema inference issues
    - Easy to reprocess if needed
    - Silver layer parses and types the data (separation of concerns)
    """
    
    def __init__(self, config: Optional[Dict] = None, spark: Optional[SparkSession] = None):
        """
        Initialize Bronze layer with dependency injection.
        
        Args:
            config: Configuration dictionary (if None, loads from default)
            spark: Spark session (if None, creates new session)
            
        Why dependency injection:
        - Enables unit testing with mocked config/Spark
        - Allows multiple instances with different configurations
        - Follows SOLID principles for better testability
        """
        self.config = config if config is not None else get_config()
        self.spark = spark if spark is not None else get_spark_session()
        self.base_path = self.config['storage']['base_path']
        self.bronze_config = self.config['storage']['layers']['bronze']
    
    def _add_audit_metadata(self, records: List[Dict], ingestion_date: str, source: str) -> List[Dict]:
        """
        Pure function to convert raw records to Bronze format with audit metadata.
        
        Each API record is serialized to JSON string and wrapped with metadata.
        
        Args:
            records: Raw data records from any source (list of dicts)
            ingestion_date: Date of ingestion (YYYY-MM-DD)
            source: Data source identifier (e.g., 'open_brewery_db_api')
            
        Returns:
            List of Bronze records with schema: {_ingest_ts, _source, _ingestion_date, payload}
            
        Why serialize to JSON string:
        - Eliminates PySpark schema inference issues (int vs float, etc.)
        - Preserves exact API response for compliance/audit
        - Silver layer handles parsing and typing (separation of concerns)
        
        Why pure function:
        - No side effects (doesn't modify input)
        - Testable without Spark
        - Composable in transformation pipelines
        """
        bronze_records = []
        ingest_ts = datetime.now()
        
        for record in records:
            bronze_record = {
                '_ingest_ts': ingest_ts,
                '_source': source,
                '_ingestion_date': ingestion_date,
                'payload': json.dumps(record),
            }
            bronze_records.append(bronze_record)
        
        return bronze_records
    
    def save_json_data(self, records: List[Dict], execution_date: str, source: str = 'api') -> None:
        """
        Save raw data to Bronze layer as append-only JSON file.
        
        Single Responsibility: This function ONLY saves data.
        Logging the record count is an internal concern, not a return value.
        
        Args:
            records: List of raw data records from API (dicts)
            execution_date: Airflow execution date (YYYY-MM-DD format)
            source: Data source identifier (default: 'api')
            
        Returns:
            None - follows Single Responsibility Principle
            
        Raises:
            ValueError: If records list is empty
            
        File naming convention:
            brewery_bronze_{source}_{execution_date}_{now_timestamp}.json
            Example: brewery_bronze_api_2026-01-25_20260126_193015.json
            
        Why JSON format for Bronze:
        - Human-readable for debugging and audit
        - Append-only immutable files (true raw data lake)
        - No schema inference issues
        - Easy to reprocess if needed
        """
        if not records:
            raise ValueError("Cannot save empty record list to Bronze layer")
        
        record_count = len(records)
        
        now = datetime.now()
        now_timestamp = now.strftime("%Y%m%d_%H%M%S")
        filename = f"brewery_bronze_{source}_{execution_date}_{now_timestamp}.json"
        
        bronze_path = os.path.join(
            self.base_path,
            self.bronze_config['path'],
            filename
        )
        
        os.makedirs(os.path.dirname(bronze_path), exist_ok=True)
        
        with open(bronze_path, 'w', encoding='utf-8') as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✓ Saved {record_count} records to Bronze layer: {bronze_path}")
    
    def read_bronze_data(self, execution_date: str) -> List[Dict]:
        """
        Read Bronze layer data for a specific execution date.
        
        Reads all JSON files matching the execution_date pattern and combines them.
        
        Args:
            execution_date: Airflow execution date (YYYY-MM-DD)
            
        Returns:
            List of raw data records from all matching JSON files
            
        Raises:
            FileNotFoundError: If no data files exist for the specified date
            
        Example:
            >>> bronze = BronzeLayer()
            >>> records = bronze.read_bronze_data("2026-01-21")
            >>> print(f"Loaded {len(records)} records")
            
        File matching pattern:
            brewery_bronze_*_{execution_date}_*.json
        """
        bronze_dir = os.path.join(
            self.base_path,
            self.bronze_config['path'],
        )
        
        logger.info(f"Reading Bronze data from {bronze_dir} for execution_date={execution_date}")
        
        all_records = []
        files_found = 0
        
        if not os.path.exists(bronze_dir):
            raise FileNotFoundError(
                f"Bronze directory does not exist: {bronze_dir}"
            )
        
        for filename in os.listdir(bronze_dir):
            if filename.endswith('.json') and f"_{execution_date}_" in filename:
                filepath = os.path.join(bronze_dir, filename)
                with open(filepath, 'r', encoding='utf-8') as f:
                    records = json.load(f)
                    all_records.extend(records)
                    files_found += 1
                    logger.info(f"  Loaded {len(records)} records from {filename}")
        
        if files_found == 0:
            raise FileNotFoundError(
                f"No Bronze data found for execution_date={execution_date} at {bronze_dir}"
            )
        
        logger.info(f"✓ Loaded {len(all_records)} total records from {files_found} file(s)")
        
        return all_records
