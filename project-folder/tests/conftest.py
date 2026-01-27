"""pytest configuration and fixtures."""

import pytest
import os
import shutil
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for tests.
    
    Delta Lake Configuration:
    - delta-spark 3.0.0 requires PySpark 3.5.x
    - Maven artifact: io.delta:delta-spark_2.12:3.0.0
    - Must match requirements.txt versions for consistency
    """
    spark = SparkSession.builder \
        .appName("brewery_pipeline_tests") \
        .master("local[2]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", 
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    spark.stop()


@pytest.fixture(scope="function")
def test_data_dir(tmp_path):
    """Create temporary directory for test data."""
    data_dir = tmp_path / "test_data"
    data_dir.mkdir()
    
    yield str(data_dir)
    
    # Cleanup
    if data_dir.exists():
        shutil.rmtree(data_dir)


@pytest.fixture
def sample_brewery_data():
    """Sample brewery data for testing."""
    return [
        {
            "id": "test-brewery-1",
            "name": "Test Brewery One",
            "brewery_type": "micro",
            "address_1": "123 Main St",
            "city": "Portland",
            "state_province": "Oregon",
            "state": "Oregon",
            "postal_code": "97201",
            "country": "United States",
            "longitude": "-122.6765",
            "latitude": "45.5231",
            "phone": "5035551234",
            "website_url": "http://testbrewery1.com",
            "street": "123 Main St"
        },
        {
            "id": "test-brewery-2",
            "name": "Test Brewery Two",
            "brewery_type": "brewpub",
            "address_1": "456 Oak Ave",
            "city": "Denver",
            "state_province": "Colorado",
            "state": "Colorado",
            "postal_code": "80202",
            "country": "United States",
            "longitude": "-104.9903",
            "latitude": "39.7392",
            "phone": "3035559876",
            "website_url": "http://testbrewery2.com",
            "street": "456 Oak Ave"
        },
        {
            "id": "test-brewery-3",
            "name": "Test Brewery Three",
            "brewery_type": "micro",
            "address_1": None,
            "city": "Austin",
            "state_province": "Texas",
            "state": "Texas",
            "postal_code": "78701",
            "country": "United States",
            "longitude": None,
            "latitude": None,
            "phone": None,
            "website_url": None,
            "street": None
        }
    ]
