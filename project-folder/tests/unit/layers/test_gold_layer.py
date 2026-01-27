"""
Unit tests for GoldLayer class.

Tests the business aggregation functionality for the Gold layer
in the Medallion Architecture pattern.

Test Strategy:
- Dependency injection with mock config and spark session
- Test generic aggregation utilities (GoldLayer)
- Brewery-specific methods are tested in brewery_tasks tests

Coverage Targets:
- __init__: Config loading, path resolution
- _aggregate_by_dimensions: Generic aggregation logic
- _write_aggregation: Delta table writing
- read_gold_aggregation: Delta table reading
"""

import os
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
)
from pyspark.sql.functions import col, lit

from src.layers.gold_layer import GoldLayer


@pytest.fixture
def silver_test_data(spark):
    """Create sample Silver layer data for Gold aggregation tests."""
    data = [
        {
            'id': '1',
            'name': 'Brewery A',
            'brewery_type': 'micro',
            'city': 'Denver',
            'state': 'Colorado',
            'country': 'United States',
            'latitude': 39.7392,
            'longitude': -104.9903,
            'data_quality_score': 100.0,
            'is_valid_location': True,
            'has_complete_address': True,
            'phone': '(303) 555-0100',
            'website_url': 'https://brewerya.com'
        },
        {
            'id': '2',
            'name': 'Brewery B',
            'brewery_type': 'brewpub',
            'city': 'Denver',
            'state': 'Colorado',
            'country': 'United States',
            'latitude': 39.7500,
            'longitude': -104.9800,
            'data_quality_score': 85.0,
            'is_valid_location': True,
            'has_complete_address': True,
            'phone': None,
            'website_url': 'https://breweryb.com'
        },
        {
            'id': '3',
            'name': 'Brewery C',
            'brewery_type': 'micro',
            'city': 'Boulder',
            'state': 'Colorado',
            'country': 'United States',
            'latitude': 40.0150,
            'longitude': -105.2705,
            'data_quality_score': 75.0,
            'is_valid_location': True,
            'has_complete_address': False,
            'phone': '(720) 555-0200',
            'website_url': None
        },
        {
            'id': '4',
            'name': 'Brewery D',
            'brewery_type': 'large',
            'city': 'Golden',
            'state': 'Colorado',
            'country': 'United States',
            'latitude': None,
            'longitude': None,
            'data_quality_score': 50.0,
            'is_valid_location': False,
            'has_complete_address': False,
            'phone': None,
            'website_url': None
        },
        {
            'id': '5',
            'name': 'Canadian Brewery',
            'brewery_type': 'micro',
            'city': 'Toronto',
            'state': 'Ontario',
            'country': 'Canada',
            'latitude': 43.6532,
            'longitude': -79.3832,
            'data_quality_score': 90.0,
            'is_valid_location': True,
            'has_complete_address': True,
            'phone': '(416) 555-0300',
            'website_url': 'https://canadianbrew.ca'
        }
    ]
    return spark.createDataFrame(data)


class TestGoldLayerInit:
    """Test GoldLayer initialization and configuration."""
    
    def test_init_with_dependency_injection(self, spark, test_data_dir):
        """Test that GoldLayer accepts injected config and spark session."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'silver': {'path': 'silver/breweries/', 'format': 'delta'},
                    'gold': {'path': 'gold/', 'format': 'delta'}
                }
            }
        }
        
        gold = GoldLayer(config=config, spark=spark)
        
        assert gold.config == config
        assert gold.spark == spark
        assert gold.base_path == test_data_dir
    
    def test_init_extracts_gold_config(self, spark, test_data_dir):
        """Test that GoldLayer correctly extracts Gold config."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'silver': {'path': 'silver/data/', 'format': 'delta'},
                    'gold': {'path': 'gold/data/', 'format': 'delta'}
                }
            }
        }
        
        gold = GoldLayer(config=config, spark=spark)
        
        assert gold.gold_config == {'path': 'gold/data/', 'format': 'delta'}


class TestGoldLayerAggregateDimensions:
    """Test the generic _aggregate_by_dimensions utility."""
    
    def test_aggregate_single_dimension(self, spark, test_data_dir, silver_test_data):
        """Test aggregation by a single dimension (brewery_type)."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'silver': {'path': 'silver/', 'format': 'delta'},
                    'gold': {'path': 'gold/', 'format': 'delta'}
                }
            }
        }
        
        gold = GoldLayer(config=config, spark=spark)
        
        result = gold._aggregate_by_dimensions(
            silver_test_data,
            dimensions=['brewery_type'],
            include_count=True,
            include_percentage=False
        )
        
        # Should have 3 unique brewery types: micro, brewpub, large
        assert result.count() == 3
        assert 'brewery_type' in result.columns
        assert 'record_count' in result.columns
        
        # Verify counts
        rows = {r['brewery_type']: r for r in result.collect()}
        assert rows['micro']['record_count'] == 3
        assert rows['brewpub']['record_count'] == 1
        assert rows['large']['record_count'] == 1
    
    def test_aggregate_multiple_dimensions(self, spark, test_data_dir, silver_test_data):
        """Test aggregation by multiple dimensions (country, state)."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'silver': {'path': 'silver/', 'format': 'delta'},
                    'gold': {'path': 'gold/', 'format': 'delta'}
                }
            }
        }
        
        gold = GoldLayer(config=config, spark=spark)
        
        result = gold._aggregate_by_dimensions(
            silver_test_data,
            dimensions=['country', 'state'],
            include_count=True,
            include_percentage=True
        )
        
        # Should have 2 rows: (USA, Colorado) and (Canada, Ontario)
        assert result.count() == 2
        assert 'country' in result.columns
        assert 'state' in result.columns
        assert 'record_count' in result.columns
        assert 'percentage_of_total' in result.columns
        
        # Verify percentages sum to ~100 (may have small rounding difference)
        total_pct = sum(r['percentage_of_total'] for r in result.collect())
        assert abs(total_pct - 100.0) < 1.0  # Allow 1% tolerance for rounding
    
    def test_aggregate_with_additional_aggregations(self, spark, test_data_dir, silver_test_data):
        """Test aggregation with custom additional aggregations."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'silver': {'path': 'silver/', 'format': 'delta'},
                    'gold': {'path': 'gold/', 'format': 'delta'}
                }
            }
        }
        
        gold = GoldLayer(config=config, spark=spark)
        
        result = gold._aggregate_by_dimensions(
            silver_test_data,
            dimensions=['country'],
            include_count=True,
            additional_aggs={
                'city_count': ('city', 'count_distinct'),
                'brewery_types': ('brewery_type', 'collect_set')
            }
        )
        
        assert 'city_count' in result.columns
        assert 'brewery_types' in result.columns
        
        # USA has 3 distinct cities: Denver, Boulder, Golden
        us_row = result.filter(col('country') == 'United States').collect()[0]
        assert us_row['city_count'] == 3


class TestGoldLayerWriteAggregation:
    """Test the _write_aggregation method."""
    
    def test_write_aggregation_creates_delta_table(self, spark, test_data_dir):
        """Test that _write_aggregation creates a Delta table with timestamped folder."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'silver': {'path': 'silver/', 'format': 'delta'},
                    'gold': {'path': 'gold/', 'format': 'delta'}
                }
            }
        }
        
        gold = GoldLayer(config=config, spark=spark)
        
        data = [
            {'brewery_type': 'micro', 'record_count': 10},
            {'brewery_type': 'brewpub', 'record_count': 5}
        ]
        df = spark.createDataFrame(data)
        execution_date = '2026-01-25'
        
        row_count = gold._write_aggregation(df, 'test_aggregation', execution_date)
        
        # Verify row count returned
        assert row_count == 2
        
        # Verify timestamped Delta table exists (format: test_aggregation_gold_2026-01-25_*)
        gold_base = os.path.join(test_data_dir, 'gold')
        folders = [f for f in os.listdir(gold_base) if f.startswith('test_aggregation_gold_2026-01-25_')]
        assert len(folders) == 1
        
        agg_path = os.path.join(gold_base, folders[0])
        
        # Verify we can read it back and has metadata columns
        read_df = spark.read.format('delta').load(agg_path)
        assert read_df.count() == 2
        assert '_execution_date' in read_df.columns
        assert '_processing_timestamp' in read_df.columns
    
    def test_write_aggregation_creates_unique_folders_on_rerun(self, spark, test_data_dir):
        """Test that re-runs create new timestamped folders."""
        import time
        
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'silver': {'path': 'silver/', 'format': 'delta'},
                    'gold': {'path': 'gold/', 'format': 'delta'}
                }
            }
        }
        
        gold = GoldLayer(config=config, spark=spark)
        execution_date = '2026-01-25'
        
        # First write
        data1 = [{'type': 'A', 'count': 1}]
        gold._write_aggregation(spark.createDataFrame(data1), 'rerun_test', execution_date)
        
        time.sleep(1)  # Ensure different timestamp
        
        # Second write (re-run)
        data2 = [
            {'type': 'B', 'count': 2},
            {'type': 'C', 'count': 3}
        ]
        gold._write_aggregation(spark.createDataFrame(data2), 'rerun_test', execution_date)
        
        # Verify two separate folders created
        gold_base = os.path.join(test_data_dir, 'gold')
        folders = sorted([f for f in os.listdir(gold_base) if f.startswith('rerun_test_gold_2026-01-25_')])
        assert len(folders) == 2
        
        # Most recent folder should have the second write data
        latest_path = os.path.join(gold_base, folders[-1])
        read_df = spark.read.format('delta').load(latest_path)
        assert read_df.count() == 2


class TestGoldLayerReadAggregation:
    """Test the read_gold_aggregation method."""
    
    def test_read_gold_aggregation_returns_dataframe_legacy_path(self, spark, test_data_dir):
        """Test that read_gold_aggregation works with legacy (non-timestamped) paths."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'silver': {'path': 'silver/', 'format': 'delta'},
                    'gold': {'path': 'gold/', 'format': 'delta'}
                }
            }
        }
        
        gold = GoldLayer(config=config, spark=spark)
        
        data = [
            {'dimension': 'X', 'metric': 100},
            {'dimension': 'Y', 'metric': 200}
        ]
        agg_path = os.path.join(test_data_dir, 'gold', 'readable_agg_gold_2026-01-25_20260125_100000')
        spark.createDataFrame(data).write.format('delta').mode('overwrite').save(agg_path)
        
        # Read it back using the method
        result = gold.read_gold_aggregation('readable_agg')
        
        assert result.count() == 2
        assert 'dimension' in result.columns
        assert 'metric' in result.columns
    
    def test_read_gold_aggregation_returns_latest_timestamped(self, spark, test_data_dir):
        """Test that read_gold_aggregation returns most recent timestamped folder."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'silver': {'path': 'silver/', 'format': 'delta'},
                    'gold': {'path': 'gold/', 'format': 'delta'}
                }
            }
        }
        
        gold = GoldLayer(config=config, spark=spark)
        
        # Create two timestamped folders
        folder1 = 'my_agg_gold_2026-01-25_20260125_100000'
        folder2 = 'my_agg_gold_2026-01-25_20260125_120000'  # Later timestamp
        
        data1 = [{'value': 'old'}]
        data2 = [{'value': 'new'}]
        
        spark.createDataFrame(data1).write.format('delta').mode('overwrite').save(
            os.path.join(test_data_dir, 'gold', folder1)
        )
        spark.createDataFrame(data2).write.format('delta').mode('overwrite').save(
            os.path.join(test_data_dir, 'gold', folder2)
        )
        
        # Read should return the latest
        result = gold.read_gold_aggregation('my_agg')
        
        assert result.count() == 1
        assert result.collect()[0]['value'] == 'new'
    
    def test_read_gold_aggregation_filters_by_execution_date(self, spark, test_data_dir):
        """Test that read_gold_aggregation filters by execution_date."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'silver': {'path': 'silver/', 'format': 'delta'},
                    'gold': {'path': 'gold/', 'format': 'delta'}
                }
            }
        }
        
        gold = GoldLayer(config=config, spark=spark)
        
        folder_day1 = 'test_agg_gold_2026-01-25_20260125_100000'
        folder_day2 = 'test_agg_gold_2026-01-26_20260126_100000'
        
        data1 = [{'day': '25'}]
        data2 = [{'day': '26'}]
        
        spark.createDataFrame(data1).write.format('delta').mode('overwrite').save(
            os.path.join(test_data_dir, 'gold', folder_day1)
        )
        spark.createDataFrame(data2).write.format('delta').mode('overwrite').save(
            os.path.join(test_data_dir, 'gold', folder_day2)
        )
        
        result = gold.read_gold_aggregation('test_agg', execution_date='2026-01-25')
        assert result.collect()[0]['day'] == '25'
        
        result = gold.read_gold_aggregation('test_agg', execution_date='2026-01-26')
        assert result.collect()[0]['day'] == '26'
    
    def test_read_gold_aggregation_nonexistent_raises_error(self, spark, test_data_dir):
        """Test that reading nonexistent aggregation raises appropriate error."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'silver': {'path': 'silver/', 'format': 'delta'},
                    'gold': {'path': 'gold/', 'format': 'delta'}
                }
            }
        }
        
        gold = GoldLayer(config=config, spark=spark)
        
        with pytest.raises(Exception):  # Delta throws AnalysisException
            gold.read_gold_aggregation('nonexistent_aggregation')
