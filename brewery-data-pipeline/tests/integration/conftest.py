"""
Integration test fixtures for the brewery data pipeline.

These fixtures provide real Spark sessions and test data for 
end-to-end integration testing. They are scoped appropriately
to balance performance and isolation.
"""

import os
import json
import pytest
import shutil
from datetime import datetime

from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def integration_spark():
    """
    Create a real Spark session for integration tests.
    
    Session-scoped for performance - Spark startup is expensive.
    Uses local[2] to test with parallelism.
    
    Delta Lake Configuration:
    - delta-spark 3.0.0 requires PySpark 3.5.x
    - Maven artifact: io.delta:delta-spark_2.12:3.0.0
    - Must match requirements.txt versions for consistency
    """
    spark = (
        SparkSession.builder
        .master('local[2]')
        .appName('brewery-integration-tests')
        .config('spark.jars.packages', 'io.delta:delta-spark_2.12:3.0.0')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.ui.enabled', 'false')
        .config('spark.driver.memory', '1g')
        .config('spark.executor.memory', '1g')
        .config('spark.sql.shuffle.partitions', '2')
        .getOrCreate()
    )
    
    yield spark
    spark.stop()


@pytest.fixture(scope='function')
def integration_test_dir(tmp_path):
    """
    Create a temporary directory structure for integration tests.
    
    Function-scoped to ensure test isolation.
    Mimics the production data directory structure.
    """
    # Create data layer directories
    bronze_dir = tmp_path / 'data' / 'bronze' / 'breweries'
    silver_dir = tmp_path / 'data' / 'silver' / 'breweries'
    gold_dir = tmp_path / 'data' / 'gold'
    
    bronze_dir.mkdir(parents=True)
    silver_dir.mkdir(parents=True)
    gold_dir.mkdir(parents=True)
    
    return tmp_path


@pytest.fixture
def integration_config(integration_test_dir):
    """
    Create a test configuration for integration tests.
    
    Mirrors the production config structure but uses test paths.
    """
    return {
        'storage': {
            'base_path': str(integration_test_dir / 'data'),
            'layers': {
                'bronze': {'path': 'bronze/breweries/', 'format': 'json'},
                'silver': {'path': 'silver/breweries/', 'format': 'delta'},
                'gold': {'path': 'gold/', 'format': 'delta'}
            }
        },
        'schema': {
            'fields': [
                {'name': 'latitude', 'type': 'double'},
                {'name': 'longitude', 'type': 'double'}
            ],
            'required_fields': ['id', 'name'],
            'partition_by': ['country', 'state'],
            'valid_brewery_types': [
                'micro', 'nano', 'regional', 'brewpub', 
                'large', 'planning', 'bar', 'contract', 
                'proprietor', 'closed'
            ]
        },
        'transformations': {
            'silver': {
                'standardization': {
                    'country': {
                        'usa': 'United States',
                        'united states': 'United States',
                        'us': 'United States'
                    },
                    'state': {'normalize': True}
                },
                'null_handling': {
                    'default_values': {
                        'country': 'Unknown',
                        'state': 'Unknown'
                    }
                },
                'quality_flags': {
                    'scored_fields': [
                        'name', 'city', 'state', 'country',
                        'latitude', 'longitude'
                    ]
                }
            }
        },
        'quality': {
            'thresholds': {
                'min_record_count': 1,
                'silver_quality_score': 0.80
            }
        }
    }


@pytest.fixture
def sample_api_response():
    """
    Sample brewery API response data for integration tests.
    
    Represents realistic data as returned from Open Brewery DB API.
    """
    return [
        {
            'id': 'integration-test-1',
            'name': 'Integration Brewery One',
            'brewery_type': 'micro',
            'address_1': '123 Test Street',
            'address_2': None,
            'address_3': None,
            'city': 'Denver',
            'state_province': 'Colorado',
            'postal_code': '80202',
            'country': 'United States',
            'longitude': '-104.9903',
            'latitude': '39.7392',
            'phone': '3035551234',
            'website_url': 'https://integration1.com',
            'state': 'Colorado',
            'street': '123 Test Street'
        },
        {
            'id': 'integration-test-2',
            'name': 'Integration Brewery Two',
            'brewery_type': 'brewpub',
            'address_1': '456 Sample Ave',
            'address_2': 'Suite 100',
            'address_3': None,
            'city': 'Boulder',
            'state_province': 'Colorado',
            'postal_code': '80301',
            'country': 'United States',
            'longitude': '-105.2705',
            'latitude': '40.015',
            'phone': None,
            'website_url': 'https://integration2.com',
            'state': 'Colorado',
            'street': '456 Sample Ave'
        },
        {
            'id': 'integration-test-3',
            'name': 'Integration Brewery Three',
            'brewery_type': 'large',
            'address_1': '789 Main Blvd',
            'address_2': None,
            'address_3': None,
            'city': 'Golden',
            'state_province': 'Colorado',
            'postal_code': '80401',
            'country': 'United States',
            'longitude': None,  # Missing coordinates
            'latitude': None,
            'phone': '7205559999',
            'website_url': None,
            'state': 'Colorado',
            'street': '789 Main Blvd'
        },
        {
            'id': 'integration-test-4',
            'name': 'Canadian Test Brewery',
            'brewery_type': 'micro',
            'address_1': '100 Maple Street',
            'address_2': None,
            'address_3': None,
            'city': 'Toronto',
            'state_province': 'Ontario',
            'postal_code': 'M5V 1A1',
            'country': 'Canada',
            'longitude': '-79.3832',
            'latitude': '43.6532',
            'phone': '4165550100',
            'website_url': 'https://canadiantest.ca',
            'state': 'Ontario',
            'street': '100 Maple Street'
        }
    ]


@pytest.fixture
def bronze_json_file(integration_test_dir, sample_api_response):
    """
    Create a Bronze layer JSON file with sample data.
    
    Returns the path to the created file.
    """
    bronze_dir = integration_test_dir / 'data' / 'bronze' / 'breweries'
    
    # Use realistic filename pattern
    execution_date = '2026-01-25'
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'brewery_bronze_api_{execution_date}_{timestamp}.json'
    
    filepath = bronze_dir / filename
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(sample_api_response, f)
    
    return str(filepath), execution_date
