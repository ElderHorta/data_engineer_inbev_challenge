"""
Unit tests for Generic Bronze Layer.

These tests validate the GENERIC BronzeLayer class (src/layers/bronze_layer.py)
which stores raw data as JSON files (Medallion Architecture).

Test Strategy:
- Test Bronze layer stores raw JSON files (append-only, immutable)
- Verify file naming convention: brewery_bronze_{source}_{execution_date}_{timestamp}.json
- Verify append-only behavior (each save creates new file)
- No Spark dependency for Bronze write operations

Bronze Layer File Format:
- Raw JSON array of API records
- Filename includes execution date and timestamp for traceability
- Human-readable for debugging and audit

Following Clean Code & SOLID principles:
- Single Responsibility: This file tests ONLY the generic BronzeLayer
- Open/Closed: BronzeLayer is generic, domain logic is in tasks modules
- save_json_data returns None (SRP: only saves, doesn't count)
"""

import pytest
import os
import json
import glob
from datetime import datetime
from src.layers.bronze_layer import BronzeLayer


# Generic test data - simple records that could represent ANY data source
@pytest.fixture
def generic_sample_data():
    """
    Generic sample data for testing BronzeLayer.
    
    Uses simple, minimal structure that could represent any data source.
    NOT brewery-specific - just id, name, and a few common fields.
    """
    return [
        {"id": "1", "name": "Record One", "category": "A", "value": 100},
        {"id": "2", "name": "Record Two", "category": "B", "value": 200},
        {"id": "3", "name": "Record Three", "category": "A", "value": None},
    ]


class TestBronzeLayer:
    """Test suite for BronzeLayer with dependency injection."""
    
    def test_init_with_dependency_injection(self, spark, test_data_dir):
        """Test BronzeLayer initialization with injected dependencies."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {
                        'path': 'bronze/breweries/',
                        'format': 'json'
                    }
                }
            }
        }
        
        bronze = BronzeLayer(config=config, spark=spark)
        
        assert bronze.config == config
        assert bronze.spark == spark
        assert bronze.base_path == test_data_dir
    
    def test_add_audit_metadata(self, test_data_dir):
        """Test pure function converts records to Bronze format with JSON payload."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {'bronze': {'path': 'bronze/data/', 'format': 'json'}}
            }
        }
        
        # BronzeLayer doesn't need Spark for this pure function test
        bronze = BronzeLayer(config=config, spark=None)
        
        sample_data = [
            {"id": "1", "name": "Record One"},
            {"id": "2", "name": "Record Two"}
        ]
        
        bronze_records = bronze._add_audit_metadata(sample_data, "2026-01-21", "test_source")
        
        assert len(bronze_records) == 2
        
        # Verify Bronze schema fields
        assert '_ingest_ts' in bronze_records[0]
        assert bronze_records[0]['_ingestion_date'] == "2026-01-21"
        assert bronze_records[0]['_source'] == 'test_source'
        assert 'payload' in bronze_records[0]
        
        # Verify payload is JSON string (NOT parsed)
        payload = json.loads(bronze_records[0]['payload'])
        assert payload['id'] == "1"
        assert payload['name'] == "Record One"
        
        # Verify original data not mutated (functional programming)
        assert '_ingest_ts' not in sample_data[0]
    
    def test_save_json_data(self, spark, test_data_dir, generic_sample_data):
        """Test saving raw data to Bronze layer as JSON file."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {
                        'path': 'bronze/data/',
                        'format': 'json'
                    }
                }
            }
        }
        
        bronze = BronzeLayer(config=config, spark=spark)
        execution_date = "2026-01-21"
        
        # save_json_data returns None (Single Responsibility Principle)
        result = bronze.save_json_data(generic_sample_data, execution_date, source="test_source")
        assert result is None
        
        # Verify JSON file was created at expected path
        bronze_dir = os.path.join(test_data_dir, 'bronze/data/')
        assert os.path.exists(bronze_dir)
        
        # Verify JSON file exists with correct naming pattern
        json_files = [f for f in os.listdir(bronze_dir) if f.endswith('.json')]
        assert len(json_files) == 1
        
        # Verify filename follows convention: brewery_bronze_{source}_{execution_date}_{timestamp}.json
        filename = json_files[0]
        assert filename.startswith('brewery_bronze_test_source_2026-01-21_')
        assert filename.endswith('.json')
        
        # Verify file content is valid JSON with correct data
        filepath = os.path.join(bronze_dir, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            saved_data = json.load(f)
        assert len(saved_data) == 3
        assert saved_data[0]['id'] == '1'
    
    def test_save_json_data_empty_list_raises_error(self, spark, test_data_dir):
        """Test that saving empty list raises ValueError."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {'bronze': {'path': 'bronze/data/', 'format': 'json'}}
            }
        }
        
        bronze = BronzeLayer(config=config, spark=spark)
        
        with pytest.raises(ValueError, match="Cannot save empty record list"):
            bronze.save_json_data([], "2026-01-21", source="test_source")
    
    def test_read_bronze_data(self, spark, test_data_dir, generic_sample_data):
        """Test reading Bronze layer data returns list of records."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {
                        'path': 'bronze/data/',
                        'format': 'json'
                    }
                }
            }
        }
        
        bronze = BronzeLayer(config=config, spark=spark)
        execution_date = "2026-01-21"
        
        bronze.save_json_data(generic_sample_data, execution_date, source="test_source")
        records = bronze.read_bronze_data(execution_date)
        
        # read_bronze_data now returns List[Dict], not DataFrame
        assert isinstance(records, list)
        assert len(records) == 3

        # Verify record structure matches original data
        assert records[0]['id'] == '1'
        assert records[0]['name'] == 'Record One'
        assert records[1]['id'] == '2'
        assert records[2]['id'] == '3'
    
    def test_read_bronze_data_nonexistent_date_raises_error(self, spark, test_data_dir):
        """Test that reading non-existent date raises FileNotFoundError."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {'bronze': {'path': 'bronze/data/', 'format': 'json'}}
            }
        }
        
        bronze = BronzeLayer(config=config, spark=spark)
        
        # First create the directory so we test the "no files" case
        bronze_dir = os.path.join(test_data_dir, 'bronze/data/')
        os.makedirs(bronze_dir, exist_ok=True)
        
        with pytest.raises(FileNotFoundError, match="No Bronze data found"):
            bronze.read_bronze_data("2026-01-01")
    
    def test_read_bronze_data_nonexistent_directory_raises_error(self, spark, test_data_dir):
        """Test that reading from non-existent directory raises FileNotFoundError."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {'bronze': {'path': 'nonexistent/path/', 'format': 'json'}}
            }
        }
        
        bronze = BronzeLayer(config=config, spark=spark)
        
        with pytest.raises(FileNotFoundError, match="Bronze directory does not exist"):
            bronze.read_bronze_data("2026-01-01")
    
    def test_json_file_preserves_all_fields(self, spark, test_data_dir, generic_sample_data):
        """Test that JSON file preserves all original fields including nulls."""
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {
                    'bronze': {
                        'path': 'bronze/data/',
                        'format': 'json'
                    }
                }
            }
        }
        
        bronze = BronzeLayer(config=config, spark=spark)
        execution_date = "2026-01-21"
        
        bronze.save_json_data(generic_sample_data, execution_date, source="test_source")
        records = bronze.read_bronze_data(execution_date)
        
        # Verify all fields preserved including null
        assert records[2]['value'] is None  # Third record has null value
        assert records[0]['category'] == 'A'
        assert records[0]['value'] == 100
    
    def test_append_only_behavior_creates_multiple_files(self, spark, test_data_dir, generic_sample_data):
        """Test that Bronze layer is append-only - each save creates new file."""
        import time
        
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {'bronze': {'path': 'bronze/data/', 'format': 'json'}}
            }
        }
        
        bronze = BronzeLayer(config=config, spark=spark)
        execution_date = "2026-01-21"
        
        # First save
        bronze.save_json_data(generic_sample_data, execution_date, source="test_source")
        
        # Small delay to ensure different timestamp
        time.sleep(1)
        
        # Second save (should create NEW file, not overwrite)
        bronze.save_json_data(generic_sample_data, execution_date, source="test_source")
        
        # Verify two separate JSON files exist
        bronze_dir = os.path.join(test_data_dir, 'bronze/data/')
        json_files = [f for f in os.listdir(bronze_dir) if f.endswith('.json')]
        assert len(json_files) == 2  # Append-only: two files created
        
        # read_bronze_data should combine both files
        records = bronze.read_bronze_data(execution_date)
        assert len(records) == 6  # 3 + 3 records from both files
    
    def test_multiple_saves_same_date_appends(self, spark, test_data_dir, generic_sample_data):
        """Test that saving same date multiple times creates multiple files (append-only)."""
        import time
        
        config = {
            'storage': {
                'base_path': test_data_dir,
                'layers': {'bronze': {'path': 'bronze/data/', 'format': 'json'}}
            }
        }
        
        bronze = BronzeLayer(config=config, spark=spark)
        execution_date = "2026-01-21"
        
        bronze.save_json_data(generic_sample_data, execution_date, source="test_source")
        time.sleep(1)  # Ensure different timestamp
        bronze.save_json_data(generic_sample_data, execution_date, source="test_source")
        
        records = bronze.read_bronze_data(execution_date)
        
        # Append-only: Bronze layer should have 6 records (3 + 3)
        assert len(records) == 6  # This is correct for append-only architecture
