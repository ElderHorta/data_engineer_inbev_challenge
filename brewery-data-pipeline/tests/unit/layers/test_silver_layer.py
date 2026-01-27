"""
Unit tests for SilverLayer class.

Tests the data cleaning and standardization functionality
following the Medallion Architecture pattern.

Test Strategy:
- Dependency injection with mock config and spark session
- Test each pure transformation function independently
- Use pytest fixtures for reusable test data

Coverage Targets:
- __init__: Config loading, path resolution
- _enforce_schema: Type casting, required field validation
- _standardize_fields: Text trimming, empty string handling
- _handle_nulls: Default value application
- _add_quality_flags: Quality score calculation, flag generation
"""

import os
import json
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from pyspark.sql.functions import col

from src.layers.silver_layer import SilverLayer


class TestSilverLayerInit:
    """Test SilverLayer initialization and configuration."""
    
    def test_init_with_dependency_injection(self, spark, test_data_dir):
        """Test that SilverLayer accepts injected config and spark session."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {'path': 'bronze/breweries/', 'format': 'json'},
                    'silver': {'path': 'silver/breweries/', 'format': 'delta'}
                }
            },
            'schema': {
                'fields': [],
                'required_fields': ['id', 'name']
            }
        }
        
        silver = SilverLayer(config=config, spark=spark)
        
        assert silver.config == config
        assert silver.spark == spark
        assert silver.base_path == test_data_dir
    
    def test_init_extracts_layer_configs(self, spark, test_data_dir):
        """Test that SilverLayer correctly extracts Bronze and Silver configs."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {'path': 'bronze/data/', 'format': 'json'},
                    'silver': {'path': 'silver/data/', 'format': 'delta'}
                }
            },
            'schema': {'fields': []}
        }
        
        silver = SilverLayer(config=config, spark=spark)
        
        assert silver.bronze_config == {'path': 'bronze/data/', 'format': 'json'}
        assert silver.silver_config == {'path': 'silver/data/', 'format': 'delta'}


class TestSilverLayerEnforceSchema:
    """Test schema enforcement logic."""
    
    def test_enforce_schema_casts_numeric_types(self, spark, test_data_dir):
        """Test that numeric string fields are cast to proper types when config has fields."""
        # NOTE: SilverLayer._enforce_schema requires 'schemas.silver.fields' to be non-empty
        # to actually cast types. With empty fields, it returns DataFrame unchanged.
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {'path': 'bronze/', 'format': 'json'},
                    'silver': {'path': 'silver/', 'format': 'delta'}
                }
            },
            # Schema config path is 'schemas.silver' not 'schema'
            'schemas': {
                'silver': {
                    'fields': [
                        {'name': 'latitude', 'type': 'double'},
                        {'name': 'longitude', 'type': 'double'}
                    ],
                    'required_fields': []
                }
            }
        }
        
        silver = SilverLayer(config=config, spark=spark)
        
        # Create DataFrame with string numbers
        data = [
            {'id': '1', 'name': 'Test', 'latitude': '40.7128', 'longitude': '-74.0060'},
            {'id': '2', 'name': 'Test2', 'latitude': '34.0522', 'longitude': '-118.2437'}
        ]
        df = spark.createDataFrame(data)
        
        result = silver._enforce_schema(df)
        
        # Verify types were cast
        lat_type = [f for f in result.schema.fields if f.name == 'latitude'][0].dataType
        lon_type = [f for f in result.schema.fields if f.name == 'longitude'][0].dataType
        
        assert isinstance(lat_type, DoubleType)
        assert isinstance(lon_type, DoubleType)
        
        # Verify values are correct
        row = result.filter(col('id') == '1').collect()[0]
        assert abs(row['latitude'] - 40.7128) < 0.0001
        assert abs(row['longitude'] - (-74.0060)) < 0.0001
    
    def test_enforce_schema_validates_required_fields(self, spark, test_data_dir):
        """Test that missing required fields raise ValueError when fields config exists."""
        # NOTE: Required fields validation only happens if schemas.silver.fields is non-empty
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {'path': 'bronze/', 'format': 'json'},
                    'silver': {'path': 'silver/', 'format': 'delta'}
                }
            },
            'schemas': {
                'silver': {
                    'fields': [{'name': 'id', 'type': 'string'}],  # Need at least one field
                    'required_fields': ['id', 'name', 'missing_field']
                }
            }
        }
        
        silver = SilverLayer(config=config, spark=spark)
        
        # Create DataFrame missing 'missing_field'
        data = [{'id': '1', 'name': 'Test'}]
        df = spark.createDataFrame(data)
        
        with pytest.raises(ValueError, match="Missing required fields"):
            silver._enforce_schema(df)
    
    def test_enforce_schema_handles_missing_config_gracefully(self, spark, test_data_dir):
        """Test schema enforcement works with minimal config."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {'path': 'bronze/', 'format': 'json'},
                    'silver': {'path': 'silver/', 'format': 'delta'}
                }
            },
            'schemas': {'silver': {'fields': []}}  # No required fields or type mappings
        }
        
        silver = SilverLayer(config=config, spark=spark)
        
        data = [{'id': '1', 'name': 'Test', 'value': '100'}]
        df = spark.createDataFrame(data)
        
        # Should not raise, just return DataFrame unchanged
        result = silver._enforce_schema(df)
        assert result.count() == 1


class TestSilverLayerStandardizeFields:
    """Test field standardization logic."""
    
    def test_standardize_converts_empty_strings_to_null(self, spark, test_data_dir):
        """Test that empty strings are converted to NULL."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {'path': 'bronze/', 'format': 'json'},
                    'silver': {'path': 'silver/', 'format': 'delta'}
                }
            },
            'schema': {'fields': []}
        }
        
        silver = SilverLayer(config=config, spark=spark)
        
        data = [
            {'id': '1', 'name': 'Test', 'phone': ''},
            {'id': '2', 'name': 'Test2', 'phone': '123-456-7890'}
        ]
        df = spark.createDataFrame(data)
        
        result = silver._standardize_fields(df)
        
        rows = result.collect()
        assert rows[0]['phone'] is None  # Empty string -> NULL
        assert rows[1]['phone'] == '123-456-7890'
    
    def test_standardize_trims_leading_trailing_spaces(self, spark, test_data_dir):
        """Test that leading/trailing spaces are trimmed from string fields."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {'path': 'bronze/', 'format': 'json'},
                    'silver': {'path': 'silver/', 'format': 'delta'}
                }
            },
            'schema': {'fields': []}
        }
        
        silver = SilverLayer(config=config, spark=spark)
        
        data = [
            {'id': '1', 'name': '  Test Brewery  ', 'city': '  Denver  '},
        ]
        df = spark.createDataFrame(data)
        
        result = silver._standardize_fields(df)
        
        row = result.collect()[0]
        # NOTE: PySpark's trim() only removes leading/trailing whitespace, not tabs
        assert row['name'] == 'Test Brewery'
        assert row['city'] == 'Denver'
    
    def test_standardize_applies_country_mapping(self, spark, test_data_dir):
        """Test country standardization from config mappings."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {'path': 'bronze/', 'format': 'json'},
                    'silver': {'path': 'silver/', 'format': 'delta'}
                }
            },
            'schema': {'fields': []},
            'transformations': {
                'silver': {
                    'standardization': {
                        'country': {
                            'united states': 'United States',
                            'usa': 'United States',
                            'us': 'United States'
                        }
                    }
                }
            }
        }
        
        silver = SilverLayer(config=config, spark=spark)
        
        data = [
            {'id': '1', 'name': 'Test1', 'country': 'usa'},
            {'id': '2', 'name': 'Test2', 'country': 'United States'},
            {'id': '3', 'name': 'Test3', 'country': 'Canada'}
        ]
        df = spark.createDataFrame(data)
        
        result = silver._standardize_fields(df)
        
        rows = {r['id']: r for r in result.collect()}
        assert rows['1']['country'] == 'United States'
        assert rows['2']['country'] == 'United States'
        assert rows['3']['country'] == 'Canada'  # Unchanged


class TestSilverLayerHandleNulls:
    """Test null value handling."""
    
    def test_handle_nulls_applies_default_values(self, spark, test_data_dir):
        """Test that configured default values are applied to nulls."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {'path': 'bronze/', 'format': 'json'},
                    'silver': {'path': 'silver/', 'format': 'delta'}
                }
            },
            'schema': {'fields': []},
            'transformations': {
                'silver': {
                    'null_handling': {
                        'default_values': {
                            'country': 'Unknown',
                            'state': 'Unknown'
                        }
                    }
                }
            }
        }
        
        silver = SilverLayer(config=config, spark=spark)
        
        data = [
            {'id': '1', 'name': 'Test1', 'country': None, 'state': None},
            {'id': '2', 'name': 'Test2', 'country': 'USA', 'state': 'CO'}
        ]
        df = spark.createDataFrame(data)
        
        result = silver._handle_nulls(df)
        
        rows = {r['id']: r for r in result.collect()}
        assert rows['1']['country'] == 'Unknown'
        assert rows['1']['state'] == 'Unknown'
        assert rows['2']['country'] == 'USA'  # Not null, unchanged
        assert rows['2']['state'] == 'CO'
    
    def test_handle_nulls_no_config_returns_unchanged(self, spark, test_data_dir):
        """Test that missing null_handling config doesn't modify data."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {'path': 'bronze/', 'format': 'json'},
                    'silver': {'path': 'silver/', 'format': 'delta'}
                }
            },
            'schema': {'fields': []}
            # No transformations.silver.null_handling
        }
        
        silver = SilverLayer(config=config, spark=spark)
        
        # Need to provide explicit types to avoid inference issues with None
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField('id', StringType(), True),
            StructField('name', StringType(), True),
            StructField('country', StringType(), True)
        ])
        data = [('1', 'Test', None)]
        df = spark.createDataFrame(data, schema)
        
        result = silver._handle_nulls(df)
        
        row = result.collect()[0]
        assert row['country'] is None  # Unchanged


class TestSilverLayerQualityFlags:
    """Test data quality flag generation."""
    
    def test_add_quality_flags_creates_quality_score(self, spark, test_data_dir):
        """Test that data_quality_score column is created."""
        # NOTE: The actual score calculation is based on:
        # - id non-null: +25
        # - name non-null: +25
        # - is_valid_location True: +25
        # - has_complete_address True: +25
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {'path': 'bronze/', 'format': 'json'},
                    'silver': {'path': 'silver/', 'format': 'delta'}
                }
            },
            'schema': {'fields': []}
        }
        
        silver = SilverLayer(config=config, spark=spark)
        
        # Create record with all fields for max score
        data = [
            {
                'id': '1', 
                'name': 'Complete Brewery',
                'street': '123 Main St',
                'city': 'Denver',
                'state': 'CO',
                'postal_code': '80202',
                'latitude': 39.7392,
                'longitude': -104.9903
            },
            {
                'id': '2',
                'name': 'Partial Brewery',
                'street': None,
                'city': None,
                'state': 'CO',
                'postal_code': None,
                'latitude': None,
                'longitude': None
            }
        ]
        df = spark.createDataFrame(data)
        
        result = silver._add_quality_flags(df)
        
        # Verify quality score column exists
        assert 'data_quality_score' in result.columns
        
        rows = {r['id']: r for r in result.collect()}
        
        # Complete record: id(25) + name(25) + valid_location(25) + complete_address(25) = 100
        assert rows['1']['data_quality_score'] == 100
        
        # Partial record: id(25) + name(25) + valid_location(0) + complete_address(0) = 50
        assert rows['2']['data_quality_score'] == 50
    
    def test_add_quality_flags_sets_valid_location_flag(self, spark, test_data_dir):
        """Test that is_valid_location flag is set correctly."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {'path': 'bronze/', 'format': 'json'},
                    'silver': {'path': 'silver/', 'format': 'delta'}
                }
            },
            'schema': {'fields': []}
        }
        
        silver = SilverLayer(config=config, spark=spark)
        
        data = [
            {'id': '1', 'name': 'Valid', 'latitude': 40.0, 'longitude': -105.0},
            {'id': '2', 'name': 'Invalid Lat', 'latitude': 200.0, 'longitude': -105.0},
            {'id': '3', 'name': 'No Coords', 'latitude': None, 'longitude': None}
        ]
        df = spark.createDataFrame(data)
        
        result = silver._add_quality_flags(df)
        
        assert 'is_valid_location' in result.columns
        
        rows = {r['id']: r for r in result.collect()}
        assert rows['1']['is_valid_location'] == True
        assert rows['2']['is_valid_location'] == False  # Invalid latitude
        assert rows['3']['is_valid_location'] == False  # Null coordinates


class TestSilverLayerSaveAndRead:
    """Test save and read operations."""
    
    def test_save_to_silver_creates_delta_table(self, spark, test_data_dir):
        """Test that save_to_silver creates a Delta table at correct path."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {'path': 'bronze/', 'format': 'json'},
                    'silver': {'path': 'silver/breweries/', 'format': 'delta'}
                }
            },
            'schemas': {'silver': {'fields': [], 'partition_by': ['country', 'state']}}
        }
        
        silver = SilverLayer(config=config, spark=spark)
        
        data = [
            {'id': '1', 'name': 'Test1', 'country': 'USA', 'state': 'CO'},
            {'id': '2', 'name': 'Test2', 'country': 'USA', 'state': 'CA'}
        ]
        df = spark.createDataFrame(data)
        
        # save_to_silver requires processing_date
        silver.save_to_silver(df, processing_date='2026-01-25')
        
        # Verify Delta table was created
        silver_path = os.path.join(test_data_dir, 'silver/breweries/')
        assert os.path.exists(silver_path)
        
        # Verify we can read it back
        read_df = spark.read.format('delta').load(silver_path)
        assert read_df.count() == 2
    
    def test_read_silver_data_returns_dataframe(self, spark, test_data_dir):
        """Test that read_silver_data returns correct DataFrame."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {'path': 'bronze/', 'format': 'json'},
                    'silver': {'path': 'silver/breweries/', 'format': 'delta'}
                }
            },
            'schemas': {'silver': {'fields': []}}
        }
        
        # First create some data using save_to_silver
        silver = SilverLayer(config=config, spark=spark)
        
        data = [{'id': '1', 'name': 'Test Brewery', 'country': 'USA', 'state': 'CO'}]
        df = spark.createDataFrame(data)
        silver.save_to_silver(df, processing_date='2026-01-25')
        
        # Now test read
        result = silver.read_silver_data()
        
        assert result.count() == 1
        assert result.collect()[0]['name'] == 'Test Brewery'


class TestSilverLayerTransformBrewery:
    """Test the brewery-specific transformation chain."""
    
    def test_transformation_chain_adds_processing_metadata(self, spark, test_data_dir):
        """Test that the transformation chain adds processing metadata.
        
        NOTE: We test individual transformation methods since transform_brewery_to_silver
        requires actual Bronze JSON files. This test validates that chaining
        _enforce_schema -> _standardize_fields -> _handle_nulls -> _add_quality_flags
        produces expected results.
        """
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {'path': 'bronze/', 'format': 'json'},
                    'silver': {'path': 'silver/', 'format': 'delta'}
                }
            },
            'schemas': {
                'silver': {
                    'fields': [
                        {'name': 'latitude', 'type': 'double'},
                        {'name': 'longitude', 'type': 'double'}
                    ],
                    'required_fields': []
                }
            }
        }
        
        silver = SilverLayer(config=config, spark=spark)
        
        # Create a DataFrame simulating parsed Bronze data
        data = [
            {
                'id': '1',
                'name': 'Test Brewery',
                'city': 'Denver',
                'state': 'CO',
                'country': 'USA',
                'latitude': '39.7392',  # String to test type casting
                'longitude': '-104.9903'
            }
        ]
        df = spark.createDataFrame(data)
        
        # Apply the transformation chain that transform_brewery_to_silver uses
        df = silver._enforce_schema(df)
        df = silver._standardize_fields(df)
        df = silver._handle_nulls(df)
        result = silver._add_quality_flags(df)
        
        # Verify quality flags added
        assert 'data_quality_score' in result.columns
        assert 'is_valid_location' in result.columns
        
        # Verify type casting happened
        lat_type = [f for f in result.schema.fields if f.name == 'latitude'][0].dataType
        assert isinstance(lat_type, DoubleType)
