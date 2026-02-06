"""
Unit tests for Data Quality Validators.

Tests the validation functionality for Bronze, Silver, and Gold layers
following the Protocol-based design pattern.

Test Strategy:
- Dependency injection with mock config and spark session
- Test each validator class independently
- Test the DataQualityValidator facade
- Verify validation result structure

Coverage Targets:
- BronzeValidator: Directory existence, JSON validity, record counts
- SilverValidator: Duplicates, null checks, coordinate validation
- GoldValidator: Aggregation existence, count validation, percentage ranges
- DataQualityValidator: Facade delegation
"""

import os
import json
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from src.quality.validators import (
    BronzeValidator,
    SilverValidator,
    GoldValidator,
    DataQualityValidator
)


@pytest.fixture
def sample_bronze_json_data():
    """Sample raw JSON data for Bronze validation tests."""
    return [
        {
            'id': '1',
            'name': 'Test Brewery 1',
            'brewery_type': 'micro',
            'city': 'Denver',
            'state': 'Colorado',
            'country': 'United States'
        },
        {
            'id': '2',
            'name': 'Test Brewery 2',
            'brewery_type': 'brewpub',
            'city': 'Boulder',
            'state': 'Colorado',
            'country': 'United States'
        },
        {
            'id': '3',
            'name': 'Test Brewery 3',
            'brewery_type': 'large',
            'city': 'Golden',
            'state': 'Colorado',
            'country': 'United States'
        }
    ]


@pytest.fixture
def sample_silver_data(spark):
    """Sample cleaned Silver data for validation tests."""
    data = [
        {
            'id': '1',
            'name': 'Test Brewery 1',
            'brewery_type': 'micro',
            'latitude': 39.7392,
            'longitude': -104.9903,
            'data_quality_score': 95.0
        },
        {
            'id': '2',
            'name': 'Test Brewery 2',
            'brewery_type': 'brewpub',
            'latitude': 40.0150,
            'longitude': -105.2705,
            'data_quality_score': 85.0
        },
        {
            'id': '3',
            'name': 'Test Brewery 3',
            'brewery_type': 'large',
            'latitude': 39.7555,
            'longitude': -105.2211,
            'data_quality_score': 75.0
        }
    ]
    return spark.createDataFrame(data)


class TestBronzeValidator:
    """Test BronzeValidator class."""
    
    def test_init_with_dependency_injection(self, spark, test_data_dir):
        """Test that BronzeValidator accepts injected config and spark."""
        config = {'quality': {'thresholds': {'min_record_count': 1}}}
        
        validator = BronzeValidator(config=config, spark=spark)
        
        assert validator.config == config
        assert validator.spark == spark
    
    def test_validate_returns_correct_structure(self, spark, test_data_dir, sample_bronze_json_data):
        """Test that validate() returns dict with required keys."""
        config = {'quality': {'thresholds': {'min_record_count': 1}}}
        
        # Create Bronze JSON file
        bronze_path = os.path.join(test_data_dir, 'bronze')
        os.makedirs(bronze_path, exist_ok=True)
        
        json_file = os.path.join(bronze_path, 'brewery_bronze_api_2026-01-25_20260125_120000.json')
        with open(json_file, 'w') as f:
            json.dump(sample_bronze_json_data, f)
        
        validator = BronzeValidator(config=config, spark=spark)
        result = validator.validate(bronze_path)
        
        # Verify result structure
        assert 'layer' in result
        assert result['layer'] == 'bronze'
        assert 'passed' in result
        assert 'errors' in result
        assert 'warnings' in result
        assert 'metrics' in result
        assert isinstance(result['errors'], list)
        assert isinstance(result['warnings'], list)
        assert isinstance(result['metrics'], dict)
    
    def test_validate_nonexistent_path_fails(self, spark, test_data_dir):
        """Test that validation fails for non-existent path."""
        config = {'quality': {'thresholds': {'min_record_count': 1}}}
        
        validator = BronzeValidator(config=config, spark=spark)
        result = validator.validate(os.path.join(test_data_dir, 'nonexistent'))
        
        assert result['passed'] == False
        assert len(result['errors']) > 0
        assert 'does not exist' in result['errors'][0].lower()
    
    def test_validate_empty_directory_fails(self, spark, test_data_dir):
        """Test that validation fails for empty directory."""
        config = {'quality': {'thresholds': {'min_record_count': 1}}}
        
        bronze_path = os.path.join(test_data_dir, 'empty_bronze')
        os.makedirs(bronze_path, exist_ok=True)
        
        validator = BronzeValidator(config=config, spark=spark)
        result = validator.validate(bronze_path)
        
        assert result['passed'] == False
        assert len(result['errors']) > 0
    
    def test_validate_valid_json_passes(self, spark, test_data_dir, sample_bronze_json_data):
        """Test that validation passes for valid JSON files."""
        config = {'data_quality': {'thresholds': {'bronze_min_records': 1}}}
        
        bronze_path = os.path.join(test_data_dir, 'valid_bronze')
        os.makedirs(bronze_path, exist_ok=True)
        
        json_file = os.path.join(bronze_path, 'brewery_bronze_api_2026-01-25_20260125_120000.json')
        with open(json_file, 'w') as f:
            json.dump(sample_bronze_json_data, f)
        
        validator = BronzeValidator(config=config, spark=spark)
        result = validator.validate(bronze_path)
        
        assert result['passed'] == True
        assert result['metrics']['record_count'] == 3
        assert result['metrics']['file_count'] == 1
    
    def test_validate_invalid_json_fails(self, spark, test_data_dir):
        """Test that validation fails for invalid JSON content."""
        config = {'quality': {'thresholds': {'min_record_count': 1}}}
        
        bronze_path = os.path.join(test_data_dir, 'invalid_bronze')
        os.makedirs(bronze_path, exist_ok=True)
        
        json_file = os.path.join(bronze_path, 'brewery_bronze_api_2026-01-25_20260125_120000.json')
        with open(json_file, 'w') as f:
            f.write('{ invalid json content')
        
        validator = BronzeValidator(config=config, spark=spark)
        result = validator.validate(bronze_path)
        
        assert result['passed'] == False
        assert len(result['errors']) > 0
    
    def test_validate_below_min_record_count_fails(self, spark, test_data_dir):
        """Test that validation fails when below minimum record count."""
        config = {'quality': {'thresholds': {'min_record_count': 100}}}
        
        bronze_path = os.path.join(test_data_dir, 'low_count_bronze')
        os.makedirs(bronze_path, exist_ok=True)
        
        json_file = os.path.join(bronze_path, 'brewery_bronze_api_2026-01-25_20260125_120000.json')
        with open(json_file, 'w') as f:
            json.dump([{'id': '1', 'name': 'Only One'}], f)
        
        validator = BronzeValidator(config=config, spark=spark)
        result = validator.validate(bronze_path)
        
        assert result['passed'] == False
        assert 'minimum' in result['errors'][0].lower() or 'below' in result['errors'][0].lower()


class TestSilverValidator:
    """Test SilverValidator class."""
    
    def test_init_with_dependency_injection(self, spark, test_data_dir):
        """Test that SilverValidator accepts injected config and spark."""
        config = {'quality': {'thresholds': {'silver_quality_score': 0.80}}}
        
        validator = SilverValidator(config=config, spark=spark)
        
        assert validator.config == config
        assert validator.spark == spark
    
    def test_validate_returns_correct_structure(self, spark, test_data_dir, sample_silver_data):
        """Test that validate() returns dict with required keys."""
        config = {'quality': {'thresholds': {'silver_quality_score': 0.80}}}
        
        # Save Silver data
        silver_path = os.path.join(test_data_dir, 'silver')
        sample_silver_data.write.format('delta').mode('overwrite').save(silver_path)
        
        validator = SilverValidator(config=config, spark=spark)
        result = validator.validate(silver_path)
        
        # Verify result structure
        assert 'layer' in result
        assert result['layer'] == 'silver'
        assert 'passed' in result
        assert 'errors' in result
        assert 'warnings' in result
        assert 'metrics' in result
    
    def test_validate_detects_duplicates(self, spark, test_data_dir):
        """Test that validation detects duplicate IDs."""
        config = {'quality': {'thresholds': {}}}
        
        # Create data with duplicates
        data = [
            {'id': '1', 'name': 'Brewery 1'},
            {'id': '1', 'name': 'Brewery 1 Duplicate'},  # Duplicate ID
            {'id': '2', 'name': 'Brewery 2'}
        ]
        df = spark.createDataFrame(data)
        
        silver_path = os.path.join(test_data_dir, 'dup_silver')
        df.write.format('delta').mode('overwrite').save(silver_path)
        
        validator = SilverValidator(config=config, spark=spark)
        result = validator.validate(silver_path)
        
        assert result['passed'] == False
        assert result['metrics']['duplicate_count'] == 1
        assert any('duplicate' in e.lower() for e in result['errors'])
    
    def test_validate_detects_null_required_fields(self, spark, test_data_dir):
        """Test that validation detects null values in required fields."""
        config = {
            'data_quality': {
                'validations': {
                    'silver': {
                        'unique_key': 'id',
                        'required_fields': ['name']
                    }
                }
            },
            'quality': {'thresholds': {}}
        }

        data = [
            {'id': '1', 'name': 'Brewery 1'},
            {'id': '2', 'name': None},
            {'id': '3', 'name': 'Brewery 3'}
        ]
        df = spark.createDataFrame(data)

        silver_path = os.path.join(test_data_dir, 'null_silver')
        df.write.format('delta').mode('overwrite').save(silver_path)

        validator = SilverValidator(config=config, spark=spark)
        result = validator.validate(silver_path)

        assert result['passed'] is False
        assert result['metrics']['name_null_count'] == 1
    
    def test_validate_detects_invalid_coordinates(self, spark, test_data_dir):
        """Test that validation warns about invalid coordinates."""
        config = {
            'data_quality': {
                'validations': {
                    'silver': {
                        'unique_key': 'id',
                        'value_ranges': {
                            'latitude': [-90, 90],
                            'longitude': [-180, 180]
                        }
                    }
                }
            },
            'quality': {'thresholds': {}}
        }

        data = [
            {'id': '1', 'name': 'Valid', 'latitude': 39.7392, 'longitude': -104.9903},
            {'id': '2', 'name': 'Invalid Lat', 'latitude': 200.0, 'longitude': -105.0},
            {'id': '3', 'name': 'Invalid Lon', 'latitude': 40.0, 'longitude': -300.0}
        ]
        df = spark.createDataFrame(data)

        silver_path = os.path.join(test_data_dir, 'coords_silver')
        df.write.format('delta').mode('overwrite').save(silver_path)

        validator = SilverValidator(config=config, spark=spark)
        result = validator.validate(silver_path)

        assert result['metrics']['latitude_out_of_range'] == 1
        assert result['metrics']['longitude_out_of_range'] == 1
        assert any('latitude' in w.lower() or 'longitude' in w.lower() for w in result['warnings'])
    
    def test_validate_valid_data_passes(self, spark, test_data_dir, sample_silver_data):
        """Test that validation passes for valid data."""
        config = {'quality': {'thresholds': {'silver_quality_score': 0.70}}}
        
        silver_path = os.path.join(test_data_dir, 'valid_silver')
        sample_silver_data.write.format('delta').mode('overwrite').save(silver_path)
        
        validator = SilverValidator(config=config, spark=spark)
        result = validator.validate(silver_path)
        
        assert result['passed'] == True
        assert result['metrics']['total_records'] == 3
        assert result['metrics']['duplicate_count'] == 0
    
    def test_validate_with_processing_date_filter(self, spark, test_data_dir):
        """Test that validation can filter by processing date."""
        config = {'quality': {'thresholds': {}}}
        
        # Create data with processing dates
        data = [
            {'id': '1', 'name': 'Brewery 1', '_processing_date': '2026-01-25'},
            {'id': '2', 'name': 'Brewery 2', '_processing_date': '2026-01-25'},
            {'id': '3', 'name': 'Brewery 3', '_processing_date': '2026-01-24'}  # Different date
        ]
        df = spark.createDataFrame(data)
        
        silver_path = os.path.join(test_data_dir, 'date_silver')
        df.write.format('delta').mode('overwrite').save(silver_path)
        
        validator = SilverValidator(config=config, spark=spark)
        result = validator.validate(silver_path, processing_date='2026-01-25')
        
        # Should only count records from 2026-01-25
        assert result['metrics']['total_records'] == 2
        assert result['metrics']['processing_date'] == '2026-01-25'


class TestGoldValidator:
    """Test GoldValidator class."""
    
    def test_init_with_dependency_injection(self, spark, test_data_dir):
        """Test that GoldValidator accepts injected config and spark."""
        config = {'quality': {'thresholds': {}}}
        
        validator = GoldValidator(config=config, spark=spark)
        
        assert validator.config == config
        assert validator.spark == spark
    
    def test_validate_returns_correct_structure(self, spark, test_data_dir):
        """Test that validate() returns dict with required keys."""
        config = {'quality': {'thresholds': {}}}
        
        gold_path = os.path.join(test_data_dir, 'gold')
        os.makedirs(gold_path, exist_ok=True)
        
        validator = GoldValidator(config=config, spark=spark)
        result = validator.validate(gold_path)
        
        # Verify result structure
        assert 'layer' in result
        assert result['layer'] == 'gold'
        assert 'passed' in result
        assert 'errors' in result
        assert 'warnings' in result
        assert 'metrics' in result
    
    def test_validate_nonexistent_path_fails(self, spark, test_data_dir):
        """Test that validation fails for non-existent path."""
        config = {'quality': {'thresholds': {}}}
        
        validator = GoldValidator(config=config, spark=spark)
        result = validator.validate(
            os.path.join(test_data_dir, 'nonexistent_gold'),
            expected_aggregations=['some_agg']
        )
        
        assert result['passed'] == False
    
    def test_validate_missing_aggregation_fails(self, spark, test_data_dir):
        """Test that validation fails when expected aggregation is missing."""
        config = {'quality': {'thresholds': {}}}
        
        gold_path = os.path.join(test_data_dir, 'partial_gold')
        os.makedirs(gold_path, exist_ok=True)
        
        # Create only one aggregation
        agg1_data = [{'type': 'A', 'count': 10}]
        spark.createDataFrame(agg1_data).write.format('delta').mode('overwrite').save(
            os.path.join(gold_path, 'aggregation1')
        )
        
        validator = GoldValidator(config=config, spark=spark)
        result = validator.validate(
            gold_path,
            expected_aggregations=['aggregation1', 'missing_aggregation']
        )
        
        assert result['passed'] == False
        assert any('missing' in e.lower() for e in result['errors'])
    
    def test_validate_all_aggregations_exist_passes(self, spark, test_data_dir):
        """Test that validation passes when all expected aggregations exist."""
        config = {'quality': {'thresholds': {}}}

        gold_path = os.path.join(test_data_dir, 'complete_gold')
        os.makedirs(gold_path, exist_ok=True)

        for agg_name in ['breweries_by_type', 'breweries_by_location']:
            folder_name = f'{agg_name}_gold_2026-01-25_20260125_120000'
            data = [{'dimension': 'X', 'record_count': 10, 'percentage': 50.0}]
            spark.createDataFrame(data).write.format('delta').mode('overwrite').save(
                os.path.join(gold_path, folder_name)
            )

        validator = GoldValidator(config=config, spark=spark)
        result = validator.validate(
            gold_path,
            expected_aggregations=['breweries_by_type', 'breweries_by_location']
        )

        assert result['passed'] is True
        assert result['metrics']['breweries_by_type_count'] == 1
        assert result['metrics']['breweries_by_location_count'] == 1
    
    def test_validate_finds_timestamped_folders(self, spark, test_data_dir):
        """Test that validation finds aggregations in timestamped folders."""
        config = {'quality': {'thresholds': {}}}
        
        gold_path = os.path.join(test_data_dir, 'timestamped_gold')
        os.makedirs(gold_path, exist_ok=True)
        
        # Create timestamped folder (new naming convention)
        timestamped_folder = 'breweries_by_type_gold_2026-01-25_20260125_120000'
        data = [{'type': 'micro', 'record_count': 10}]
        spark.createDataFrame(data).write.format('delta').mode('overwrite').save(
            os.path.join(gold_path, timestamped_folder)
        )
        
        validator = GoldValidator(config=config, spark=spark)
        result = validator.validate(
            gold_path,
            expected_aggregations=['breweries_by_type']
        )
        
        assert result['passed'] == True
        assert result['metrics']['breweries_by_type_count'] == 1
        assert result['metrics']['breweries_by_type_path'] == timestamped_folder
    
    def test_validate_finds_latest_timestamped_folder(self, spark, test_data_dir):
        """Test that validation finds the most recent timestamped folder."""
        config = {'quality': {'thresholds': {}}}
        
        gold_path = os.path.join(test_data_dir, 'multi_timestamped_gold')
        os.makedirs(gold_path, exist_ok=True)
        
        # Create older folder
        older_folder = 'breweries_by_type_gold_2026-01-25_20260125_100000'
        data_old = [{'type': 'old', 'record_count': 5}]
        spark.createDataFrame(data_old).write.format('delta').mode('overwrite').save(
            os.path.join(gold_path, older_folder)
        )
        
        # Create newer folder
        newer_folder = 'breweries_by_type_gold_2026-01-25_20260125_120000'
        data_new = [{'type': 'new', 'record_count': 15}]
        spark.createDataFrame(data_new).write.format('delta').mode('overwrite').save(
            os.path.join(gold_path, newer_folder)
        )
        
        validator = GoldValidator(config=config, spark=spark)
        result = validator.validate(
            gold_path,
            expected_aggregations=['breweries_by_type']
        )
        
        # Should find the newer folder
        assert result['passed'] == True
        assert result['metrics']['breweries_by_type_path'] == newer_folder
    
    def test_validate_filters_by_execution_date(self, spark, test_data_dir):
        """Test that validation filters timestamped folders by execution_date."""
        config = {'quality': {'thresholds': {}}}
        
        gold_path = os.path.join(test_data_dir, 'date_filtered_gold')
        os.makedirs(gold_path, exist_ok=True)
        
        # Create folder for day 25
        folder_day25 = 'breweries_by_type_gold_2026-01-25_20260125_120000'
        data25 = [{'day': '25', 'record_count': 10}]
        spark.createDataFrame(data25).write.format('delta').mode('overwrite').save(
            os.path.join(gold_path, folder_day25)
        )
        
        # Create folder for day 26
        folder_day26 = 'breweries_by_type_gold_2026-01-26_20260126_120000'
        data26 = [{'day': '26', 'record_count': 20}]
        spark.createDataFrame(data26).write.format('delta').mode('overwrite').save(
            os.path.join(gold_path, folder_day26)
        )
        
        validator = GoldValidator(config=config, spark=spark)
        
        # Filter by day 25
        result = validator.validate(
            gold_path,
            expected_aggregations=['breweries_by_type'],
            execution_date='2026-01-25'
        )
        
        assert result['passed'] == True
        assert result['metrics']['breweries_by_type_path'] == folder_day25
    
    def test_validate_detects_negative_counts(self, spark, test_data_dir):
        """Test that validation fails when count columns have negative values."""
        config = {
            'data_quality': {
                'validations': {
                    'gold': {
                        'count_columns': ['record_count']
                    }
                }
            },
            'quality': {'thresholds': {}}
        }

        gold_path = os.path.join(test_data_dir, 'negative_gold')
        os.makedirs(gold_path, exist_ok=True)

        folder_name = 'bad_counts_gold_2026-01-25_20260125_120000'
        data = [
            {'type': 'A', 'record_count': 10},
            {'type': 'B', 'record_count': -5}
        ]
        spark.createDataFrame(data).write.format('delta').mode('overwrite').save(
            os.path.join(gold_path, folder_name)
        )

        validator = GoldValidator(config=config, spark=spark)
        result = validator.validate(gold_path, expected_aggregations=['bad_counts'])

        assert result['passed'] is False
        assert any('negative' in e.lower() for e in result['errors'])
    
    def test_validate_warns_invalid_percentages(self, spark, test_data_dir):
        """Test that validation warns about invalid percentage values."""
        config = {
            'data_quality': {
                'validations': {
                    'gold': {
                        'percentage_columns': ['percentage']
                    }
                }
            },
            'quality': {'thresholds': {}}
        }

        gold_path = os.path.join(test_data_dir, 'pct_gold')
        os.makedirs(gold_path, exist_ok=True)

        folder_name = 'bad_pct_gold_2026-01-25_20260125_120000'
        data = [
            {'type': 'A', 'record_count': 10, 'percentage': 50.0},
            {'type': 'B', 'record_count': 5, 'percentage': 150.0}
        ]
        spark.createDataFrame(data).write.format('delta').mode('overwrite').save(
            os.path.join(gold_path, folder_name)
        )

        validator = GoldValidator(config=config, spark=spark)
        result = validator.validate(gold_path, expected_aggregations=['bad_pct'])

        assert any('percentage' in w.lower() for w in result['warnings'])
    
    def test_validate_empty_aggregation_warns(self, spark, test_data_dir):
        """Test that validation warns when aggregation has no records."""
        config = {'quality': {'thresholds': {}}}

        gold_path = os.path.join(test_data_dir, 'empty_gold')
        os.makedirs(gold_path, exist_ok=True)

        folder_name = 'empty_agg_gold_2026-01-25_20260125_120000'
        schema = "type STRING, record_count INT"
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format('delta').mode('overwrite').save(
            os.path.join(gold_path, folder_name)
        )

        validator = GoldValidator(config=config, spark=spark)
        result = validator.validate(gold_path, expected_aggregations=['empty_agg'])

        assert any('no records' in w.lower() for w in result['warnings'])


class TestDataQualityValidatorFacade:
    """Test DataQualityValidator facade class."""
    
    def test_init_creates_all_validators(self, spark, test_data_dir):
        """Test that facade creates all specialized validators."""
        config = {'quality': {'thresholds': {}}}
        
        facade = DataQualityValidator(config=config, spark=spark)
        
        assert facade.bronze_validator is not None
        assert facade.silver_validator is not None
        assert facade.gold_validator is not None
        assert isinstance(facade.bronze_validator, BronzeValidator)
        assert isinstance(facade.silver_validator, SilverValidator)
        assert isinstance(facade.gold_validator, GoldValidator)
    
    def test_validate_bronze_delegates(self, spark, test_data_dir, sample_bronze_json_data):
        """Test that validate_bronze delegates to BronzeValidator."""
        config = {'data_quality': {'thresholds': {'bronze_min_records': 1}}}
        
        bronze_path = os.path.join(test_data_dir, 'facade_bronze')
        os.makedirs(bronze_path, exist_ok=True)
        
        json_file = os.path.join(bronze_path, 'brewery_bronze_api_2026-01-25_20260125_120000.json')
        with open(json_file, 'w') as f:
            json.dump(sample_bronze_json_data, f)
        
        facade = DataQualityValidator(config=config, spark=spark)
        result = facade.validate_bronze(bronze_path)
        
        assert result['layer'] == 'bronze'
        assert result['passed'] == True
    
    def test_validate_silver_delegates(self, spark, test_data_dir, sample_silver_data):
        """Test that validate_silver delegates to SilverValidator."""
        config = {'quality': {'thresholds': {'silver_quality_score': 0.70}}}
        
        silver_path = os.path.join(test_data_dir, 'facade_silver')
        sample_silver_data.write.format('delta').mode('overwrite').save(silver_path)
        
        facade = DataQualityValidator(config=config, spark=spark)
        result = facade.validate_silver(silver_path)
        
        assert result['layer'] == 'silver'
        assert result['passed'] == True
    
    def test_validate_gold_delegates(self, spark, test_data_dir):
        """Test that validate_gold delegates to GoldValidator."""
        config = {'quality': {'thresholds': {}}}

        gold_path = os.path.join(test_data_dir, 'facade_gold')
        os.makedirs(gold_path, exist_ok=True)

        folder_name = 'test_agg_gold_2026-01-25_20260125_120000'
        data = [{'type': 'A', 'record_count': 10}]
        spark.createDataFrame(data).write.format('delta').mode('overwrite').save(
            os.path.join(gold_path, folder_name)
        )

        facade = DataQualityValidator(config=config, spark=spark)
        result = facade.validate_gold(gold_path, expected_aggregations=['test_agg'])

        assert result['layer'] == 'gold'
        assert result['passed'] is True
    
    def test_run_all_validations(self, spark, test_data_dir, sample_bronze_json_data, sample_silver_data):
        """Test that run_all_validations runs all three validators."""
        config = {'quality': {'thresholds': {'min_record_count': 1, 'silver_quality_score': 0.70}}}
        
        # Setup Bronze
        bronze_path = os.path.join(test_data_dir, 'all_bronze')
        os.makedirs(bronze_path, exist_ok=True)
        json_file = os.path.join(bronze_path, 'brewery_bronze_api_2026-01-25_20260125_120000.json')
        with open(json_file, 'w') as f:
            json.dump(sample_bronze_json_data, f)
        
        # Setup Silver
        silver_path = os.path.join(test_data_dir, 'all_silver')
        sample_silver_data.write.format('delta').mode('overwrite').save(silver_path)
        
        # Setup Gold
        gold_path = os.path.join(test_data_dir, 'all_gold')
        os.makedirs(gold_path, exist_ok=True)
        
        facade = DataQualityValidator(config=config, spark=spark)
        results = facade.run_all_validations(bronze_path, silver_path, gold_path)
        
        assert 'bronze' in results
        assert 'silver' in results
        assert 'gold' in results
        
        assert results['bronze']['layer'] == 'bronze'
        assert results['silver']['layer'] == 'silver'
        assert results['gold']['layer'] == 'gold'
