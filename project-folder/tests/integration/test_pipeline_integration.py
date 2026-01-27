"""
Integration tests for the complete brewery data pipeline.

Tests end-to-end data flow through Bronze → Silver → Gold layers
with real Spark sessions and Delta Lake operations.

These tests validate:
- Complete pipeline execution
- Data transformations across layers
- Delta Lake read/write operations
- Data quality validations
"""

import os
import json
import pytest
from datetime import datetime

from src.layers.bronze_layer import BronzeLayer
from src.layers.silver_layer import SilverLayer
from src.layers.gold_layer import GoldLayer
from src.quality.validators import DataQualityValidator
from src.pipelines.brewery.brewery_tasks import create_brewery_aggregations


class TestBronzeToSilverIntegration:
    """Test Bronze to Silver layer data flow."""
    
    def test_bronze_save_and_load(
        self,
        integration_spark,
        integration_test_dir,
        integration_config,
        sample_api_response
    ):
        """Test saving raw data to Bronze and loading it."""
        bronze = BronzeLayer(config=integration_config, spark=integration_spark)
        execution_date = '2026-01-25'
        
        # Save to Bronze
        bronze.save_json_data(sample_api_response, execution_date, source='api')
        
        # Read back
        records = bronze.read_bronze_data(execution_date)
        
        assert len(records) == 4
        assert records[0]['id'] == 'integration-test-1'
        assert records[0]['brewery_type'] == 'micro'
    
    def test_silver_transform_from_bronze(
        self,
        integration_spark,
        integration_test_dir,
        integration_config,
        sample_api_response
    ):
        """Test Silver layer transformation of Bronze data."""
        # First save Bronze data
        bronze = BronzeLayer(config=integration_config, spark=integration_spark)
        execution_date = '2026-01-25'
        bronze.save_json_data(sample_api_response, execution_date, source='api')
        
        # Transform to Silver
        silver = SilverLayer(config=integration_config, spark=integration_spark)
        silver_df = silver.load_from_bronze_json(ingestion_date=execution_date)
        
        # Apply transformations
        transformed_df = silver.transform_brewery_to_silver(silver_df)
        
        # Verify transformations
        assert transformed_df.count() == 4
        assert 'data_quality_score' in transformed_df.columns
        assert '_processing_date' in transformed_df.columns
        
        # Verify type casting
        lat_type = str(transformed_df.schema['latitude'].dataType)
        assert 'Double' in lat_type
    
    def test_silver_save_creates_partitioned_delta(
        self,
        integration_spark,
        integration_test_dir,
        integration_config,
        sample_api_response
    ):
        """Test that Silver save creates partitioned Delta table."""
        # Save Bronze
        bronze = BronzeLayer(config=integration_config, spark=integration_spark)
        execution_date = '2026-01-25'
        bronze.save_json_data(sample_api_response, execution_date, source='api')
        
        # Transform and save to Silver
        silver = SilverLayer(config=integration_config, spark=integration_spark)
        silver_df = silver.load_from_bronze_json(ingestion_date=execution_date)
        transformed_df = silver.transform_brewery_to_silver(silver_df)
        silver.save_to_silver(transformed_df)
        
        # Verify Delta table exists
        silver_path = os.path.join(
            str(integration_test_dir / 'data'),
            'silver/breweries/'
        )
        assert os.path.exists(silver_path)
        
        # Verify we can read it back
        read_df = integration_spark.read.format('delta').load(silver_path)
        assert read_df.count() == 4


class TestSilverToGoldIntegration:
    """Test Silver to Gold layer data flow."""
    
    def test_gold_aggregations_from_silver(
        self,
        integration_spark,
        integration_test_dir,
        integration_config,
        sample_api_response
    ):
        """Test Gold layer aggregations from Silver data."""
        # Setup Bronze → Silver
        bronze = BronzeLayer(config=integration_config, spark=integration_spark)
        execution_date = '2026-01-25'
        bronze.save_json_data(sample_api_response, execution_date, source='api')
        
        silver = SilverLayer(config=integration_config, spark=integration_spark)
        silver_df = silver.load_from_bronze_json(ingestion_date=execution_date)
        transformed_df = silver.transform_brewery_to_silver(silver_df)
        silver.save_to_silver(transformed_df)
        
        # Create Gold aggregations using brewery-specific function
        metrics = create_brewery_aggregations(execution_date)
        
        # Verify metrics
        assert metrics['total_source_records'] == 4
        assert 'aggregations' in metrics
        
        # Verify all 4 aggregation tables created
        expected_aggs = [
            'breweries_by_type',
            'breweries_by_location',
            'breweries_by_type_location',
            'brewery_data_quality_metrics'
        ]
        
        for agg_name in expected_aggs:
            assert agg_name in metrics['aggregations']
            assert metrics['aggregations'][agg_name]['record_count'] > 0
            
            # Verify Delta table exists
            agg_path = os.path.join(
                str(integration_test_dir / 'data'),
                'gold',
                agg_name
            )
            assert os.path.exists(agg_path), f"Missing: {agg_name}"
    
    def test_breweries_by_type_aggregation(
        self,
        integration_spark,
        integration_test_dir,
        integration_config,
        sample_api_response
    ):
        """Test breweries_by_type aggregation values."""
        # Setup full pipeline
        bronze = BronzeLayer(config=integration_config, spark=integration_spark)
        execution_date = '2026-01-25'
        bronze.save_json_data(sample_api_response, execution_date, source='api')
        
        silver = SilverLayer(config=integration_config, spark=integration_spark)
        silver_df = silver.load_from_bronze_json(ingestion_date=execution_date)
        transformed_df = silver.transform_brewery_to_silver(silver_df)
        silver.save_to_silver(transformed_df)
        
        create_brewery_aggregations(execution_date)
        
        # Read and verify breweries_by_type
        gold = GoldLayer(config=integration_config, spark=integration_spark)
        agg_df = gold.read_gold_aggregation('breweries_by_type')
        
        assert agg_df.count() == 3  # micro, brewpub, large
        
        rows = {r['brewery_type']: r for r in agg_df.collect()}
        assert rows['micro']['record_count'] == 2  # 2 micros in test data
        assert rows['brewpub']['record_count'] == 1
        assert rows['large']['record_count'] == 1
    
    def test_brewery_quality_metrics_aggregation(
        self,
        integration_spark,
        integration_test_dir,
        integration_config,
        sample_api_response
    ):
        """Test brewery_data_quality_metrics aggregation."""
        # Setup full pipeline
        bronze = BronzeLayer(config=integration_config, spark=integration_spark)
        execution_date = '2026-01-25'
        bronze.save_json_data(sample_api_response, execution_date, source='api')
        
        silver = SilverLayer(config=integration_config, spark=integration_spark)
        silver_df = silver.load_from_bronze_json(ingestion_date=execution_date)
        transformed_df = silver.transform_brewery_to_silver(silver_df)
        silver.save_to_silver(transformed_df)
        
        create_brewery_aggregations(execution_date)
        
        # Read quality metrics
        gold = GoldLayer(config=integration_config, spark=integration_spark)
        metrics_df = gold.read_gold_aggregation('brewery_data_quality_metrics')
        
        assert metrics_df.count() == 1  # Single summary row
        
        row = metrics_df.collect()[0]
        assert row['total_records'] == 4
        assert row['country_count'] == 2  # USA and Canada
        assert row['brewery_type_count'] == 3  # micro, brewpub, large


class TestEndToEndValidation:
    """Test data quality validation across all layers."""
    
    def test_validate_complete_pipeline(
        self,
        integration_spark,
        integration_test_dir,
        integration_config,
        sample_api_response
    ):
        """Test that validators pass for correctly processed pipeline."""
        # Run full pipeline
        bronze = BronzeLayer(config=integration_config, spark=integration_spark)
        execution_date = '2026-01-25'
        bronze.save_json_data(sample_api_response, execution_date, source='api')
        
        silver = SilverLayer(config=integration_config, spark=integration_spark)
        silver_df = silver.load_from_bronze_json(ingestion_date=execution_date)
        transformed_df = silver.transform_brewery_to_silver(silver_df)
        silver.save_to_silver(transformed_df)
        
        create_brewery_aggregations(execution_date)
        
        # Validate all layers
        validator = DataQualityValidator(
            config=integration_config,
            spark=integration_spark
        )
        
        bronze_path = os.path.join(
            str(integration_test_dir / 'data'),
            'bronze/breweries/'
        )
        silver_path = os.path.join(
            str(integration_test_dir / 'data'),
            'silver/breweries/'
        )
        gold_path = os.path.join(
            str(integration_test_dir / 'data'),
            'gold/'
        )
        
        # Bronze validation
        bronze_result = validator.validate_bronze(bronze_path)
        assert bronze_result['passed'] == True
        assert bronze_result['metrics']['total_records'] == 4
        
        # Silver validation
        silver_result = validator.validate_silver(silver_path)
        assert silver_result['passed'] == True
        assert silver_result['metrics']['duplicate_count'] == 0
        
        # Gold validation
        gold_result = validator.validate_gold(
            gold_path,
            expected_aggregations=[
                'breweries_by_type',
                'breweries_by_location',
                'breweries_by_type_location',
                'brewery_data_quality_metrics'
            ]
        )
        assert gold_result['passed'] == True


class TestPipelineRerunIdempotency:
    """Test that pipeline reruns produce consistent results."""
    
    def test_silver_deduplication_on_rerun(
        self,
        integration_spark,
        integration_test_dir,
        integration_config,
        sample_api_response
    ):
        """Test that Silver deduplicates data on pipeline rerun."""
        bronze = BronzeLayer(config=integration_config, spark=integration_spark)
        silver = SilverLayer(config=integration_config, spark=integration_spark)
        execution_date = '2026-01-25'
        
        # First run
        bronze.save_json_data(sample_api_response, execution_date, source='api')
        silver_df1 = silver.load_from_bronze_json(ingestion_date=execution_date)
        
        # Simulate rerun - save same data again
        import time
        time.sleep(1)  # Ensure different timestamp
        bronze.save_json_data(sample_api_response, execution_date, source='api')
        
        # Load should deduplicate
        silver_df2 = silver.load_from_bronze_json(ingestion_date=execution_date)
        
        # Should still have 4 unique records (not 8)
        assert silver_df2.count() == 4
    
    def test_gold_overwrite_on_rerun(
        self,
        integration_spark,
        integration_test_dir,
        integration_config,
        sample_api_response
    ):
        """Test that Gold aggregations overwrite on rerun."""
        # Setup pipeline
        bronze = BronzeLayer(config=integration_config, spark=integration_spark)
        execution_date = '2026-01-25'
        bronze.save_json_data(sample_api_response, execution_date, source='api')
        
        silver = SilverLayer(config=integration_config, spark=integration_spark)
        silver_df = silver.load_from_bronze_json(ingestion_date=execution_date)
        transformed_df = silver.transform_brewery_to_silver(silver_df)
        silver.save_to_silver(transformed_df)
        
        # First run
        metrics1 = create_brewery_aggregations(execution_date)
        
        # Second run (creates new timestamped folder, not append)
        metrics2 = create_brewery_aggregations(execution_date)
        
        # Counts should be identical
        assert metrics1['total_source_records'] == metrics2['total_source_records']
        
        gold = GoldLayer(config=integration_config, spark=integration_spark)
        agg_df = gold.read_gold_aggregation('breweries_by_type')
        total_count = sum(r['record_count'] for r in agg_df.collect())
        assert total_count == 4  # Still 4, not 8


class TestDataQualityTransformations:
    """Test specific data quality transformations."""
    
    def test_country_standardization(
        self,
        integration_spark,
        integration_test_dir,
        integration_config,
        sample_api_response
    ):
        """Test that country values are standardized correctly."""
        # Modify sample data with variations
        varied_data = sample_api_response.copy()
        varied_data[0]['country'] = 'usa'  # lowercase
        varied_data[1]['country'] = 'United States'  # proper
        
        bronze = BronzeLayer(config=integration_config, spark=integration_spark)
        execution_date = '2026-01-25'
        bronze.save_json_data(varied_data, execution_date, source='api')
        
        silver = SilverLayer(config=integration_config, spark=integration_spark)
        silver_df = silver.load_from_bronze_json(ingestion_date=execution_date)
        transformed_df = silver.transform_brewery_to_silver(silver_df)
        
        # All US entries should be standardized
        us_records = transformed_df.filter(
            transformed_df['country'] == 'United States'
        ).count()
        
        assert us_records == 3  # 3 US breweries, all standardized
    
    def test_quality_score_calculation(
        self,
        integration_spark,
        integration_test_dir,
        integration_config,
        sample_api_response
    ):
        """Test that quality scores are calculated correctly."""
        bronze = BronzeLayer(config=integration_config, spark=integration_spark)
        execution_date = '2026-01-25'
        bronze.save_json_data(sample_api_response, execution_date, source='api')
        
        silver = SilverLayer(config=integration_config, spark=integration_spark)
        silver_df = silver.load_from_bronze_json(ingestion_date=execution_date)
        transformed_df = silver.transform_brewery_to_silver(silver_df)
        
        # Verify quality scores exist
        assert 'data_quality_score' in transformed_df.columns
        
        scores = transformed_df.select('id', 'data_quality_score').collect()
        scores_dict = {r['id']: r['data_quality_score'] for r in scores}
        
        # Complete record should have higher score than incomplete
        # integration-test-1 has all fields
        # integration-test-3 has missing lat/lon
        assert scores_dict['integration-test-1'] > scores_dict['integration-test-3']
    
    def test_valid_location_flag(
        self,
        integration_spark,
        integration_test_dir,
        integration_config,
        sample_api_response
    ):
        """Test that is_valid_location flag is set correctly."""
        bronze = BronzeLayer(config=integration_config, spark=integration_spark)
        execution_date = '2026-01-25'
        bronze.save_json_data(sample_api_response, execution_date, source='api')
        
        silver = SilverLayer(config=integration_config, spark=integration_spark)
        silver_df = silver.load_from_bronze_json(ingestion_date=execution_date)
        transformed_df = silver.transform_brewery_to_silver(silver_df)
        
        # Verify location flag exists
        assert 'is_valid_location' in transformed_df.columns
        
        rows = {r['id']: r for r in transformed_df.collect()}
        
        # Records with valid coordinates
        assert rows['integration-test-1']['is_valid_location'] == True
        assert rows['integration-test-2']['is_valid_location'] == True
        
        # Record with missing coordinates
        assert rows['integration-test-3']['is_valid_location'] == False
