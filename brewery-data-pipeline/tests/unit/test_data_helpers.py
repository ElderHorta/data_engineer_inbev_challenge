"""
Unit tests for generic data transformation utilities.

Tests the reusable helper functions in src/utils/data_helpers.py
that can be used across different pipelines and data sources.
"""

import pytest
from src.utils.data_helpers import (
    convert_fields_to_float,
    convert_fields_to_int,
    normalize_empty_strings,
)


class TestConvertFieldsToFloat:
    """Tests for convert_fields_to_float function."""

    def test_converts_integer_to_float(self):
        """Test that integer values are converted to float."""
        records = [
            {"id": "1", "temperature": 25, "humidity": 60},
            {"id": "2", "temperature": 30, "humidity": 55},
        ]
        
        result = convert_fields_to_float(records, ['temperature', 'humidity'])
        
        assert isinstance(result[0]['temperature'], float)
        assert result[0]['temperature'] == 25.0
        assert isinstance(result[0]['humidity'], float)
        assert result[0]['humidity'] == 60.0

    def test_converts_string_to_float(self):
        """Test that string numeric values are converted to float."""
        records = [
            {"id": "1", "latitude": "40.7128", "longitude": "-74.0060"},
            {"id": "2", "latitude": "34.0522", "longitude": "-118.2437"},
        ]
        
        result = convert_fields_to_float(records, ['latitude', 'longitude'])
        
        assert isinstance(result[0]['latitude'], float)
        assert abs(result[0]['latitude'] - 40.7128) < 0.0001
        assert isinstance(result[0]['longitude'], float)
        assert abs(result[0]['longitude'] - (-74.0060)) < 0.0001

    def test_preserves_existing_floats(self):
        """Test that float values remain unchanged."""
        records = [
            {"id": "1", "value": 3.14159, "score": 98.5},
        ]
        
        result = convert_fields_to_float(records, ['value', 'score'])
        
        assert result[0]['value'] == 3.14159
        assert result[0]['score'] == 98.5

    def test_handles_none_values(self):
        """Test that None values are preserved."""
        records = [
            {"id": "1", "temperature": 25, "humidity": None},
            {"id": "2", "temperature": None, "humidity": None},
        ]
        
        result = convert_fields_to_float(records, ['temperature', 'humidity'])
        
        assert result[0]['temperature'] == 25.0
        assert result[0]['humidity'] is None
        assert result[1]['temperature'] is None
        assert result[1]['humidity'] is None

    def test_handles_missing_fields(self):
        """Test that records without specified fields are handled gracefully."""
        records = [
            {"id": "1", "name": "Record One"},
            {"id": "2", "name": "Record Two", "latitude": 40},
        ]
        
        result = convert_fields_to_float(records, ['latitude', 'longitude'])
        
        # First record doesn't have the fields - should be unchanged
        assert result[0] == {"id": "1", "name": "Record One"}
        # Second record has latitude - should be converted
        assert result[1]['latitude'] == 40.0
        assert 'longitude' not in result[1]

    def test_invalid_value_sets_to_none_by_default(self):
        """Test that invalid values are set to None with default error handling."""
        records = [
            {"id": "1", "temperature": "invalid", "pressure": 1013},
        ]
        
        result = convert_fields_to_float(records, ['temperature', 'pressure'])
        
        assert result[0]['temperature'] is None
        assert result[0]['pressure'] == 1013.0

    def test_invalid_value_with_skip_on_error(self):
        """Test that invalid values are kept with on_error='skip'."""
        records = [
            {"id": "1", "temperature": "invalid", "pressure": 1013},
        ]
        
        result = convert_fields_to_float(records, ['temperature', 'pressure'], on_error='skip')
        
        assert result[0]['temperature'] == "invalid"  # Original value kept
        assert result[0]['pressure'] == 1013.0

    def test_invalid_value_raises_with_raise_on_error(self):
        """Test that invalid values raise exception with on_error='raise'."""
        records = [
            {"id": "1", "temperature": "invalid"},
        ]
        
        with pytest.raises(ValueError, match="Failed to convert temperature"):
            convert_fields_to_float(records, ['temperature'], on_error='raise')

    def test_does_not_mutate_original_list(self):
        """Test that the function does not modify the original input."""
        records = [
            {"id": "1", "value": 42},
        ]
        
        result = convert_fields_to_float(records, ['value'])
        
        # Original should still have integer
        assert records[0]['value'] == 42
        assert isinstance(records[0]['value'], int)
        
        # Result should have float
        assert result[0]['value'] == 42.0
        assert isinstance(result[0]['value'], float)

    def test_empty_list_returns_empty_list(self):
        """Test that empty list returns empty list."""
        result = convert_fields_to_float([], ['any_field'])
        assert result == []

    def test_empty_field_list_returns_unchanged_records(self):
        """Test that empty field list returns unchanged records."""
        records = [{"id": "1", "value": 42}]
        result = convert_fields_to_float(records, [])
        
        assert result[0] == {"id": "1", "value": 42}

    def test_mixed_types_in_single_record(self):
        """Test converting mixed types (int, str, float, None) in single record."""
        records = [
            {
                "id": "1",
                "int_val": 100,
                "str_val": "200.5",
                "float_val": 300.75,
                "none_val": None,
            },
        ]
        
        result = convert_fields_to_float(
            records, 
            ['int_val', 'str_val', 'float_val', 'none_val']
        )
        
        assert result[0]['int_val'] == 100.0
        assert result[0]['str_val'] == 200.5
        assert result[0]['float_val'] == 300.75
        assert result[0]['none_val'] is None


class TestConvertFieldsToInt:
    """Tests for convert_fields_to_int function."""

    def test_converts_string_to_int(self):
        """Test that string numeric values are converted to int."""
        records = [{"count": "42", "total": "100"}]
        result = convert_fields_to_int(records, ['count', 'total'])
        
        assert isinstance(result[0]['count'], int)
        assert result[0]['count'] == 42
        assert result[0]['total'] == 100

    def test_converts_float_to_int(self):
        """Test that float values are converted to int."""
        records = [{"count": 42.0, "total": 100.9}]
        result = convert_fields_to_int(records, ['count', 'total'])
        
        assert result[0]['count'] == 42
        assert result[0]['total'] == 100  # Truncated

    def test_converts_string_float_to_int(self):
        """Test that string float values are converted to int."""
        records = [{"count": "42.7"}]
        result = convert_fields_to_int(records, ['count'])
        
        assert result[0]['count'] == 42  # Truncated

    def test_handles_none_values(self):
        """Test that None values are preserved."""
        records = [{"count": None, "total": 100}]
        result = convert_fields_to_int(records, ['count', 'total'])
        
        assert result[0]['count'] is None
        assert result[0]['total'] == 100


class TestNormalizeEmptyStrings:
    """Tests for normalize_empty_strings function."""

    def test_converts_empty_strings_to_none(self):
        """Test that empty strings are converted to None."""
        records = [
            {"name": "Test", "city": "", "state": ""},
        ]
        
        result = normalize_empty_strings(records)
        
        assert result[0]['name'] == "Test"
        assert result[0]['city'] is None
        assert result[0]['state'] is None

    def test_preserves_non_empty_strings(self):
        """Test that non-empty strings are preserved."""
        records = [
            {"name": "Test", "city": "Austin", "state": "TX"},
        ]
        
        result = normalize_empty_strings(records)
        
        assert result[0]['name'] == "Test"
        assert result[0]['city'] == "Austin"
        assert result[0]['state'] == "TX"

    def test_only_specified_fields(self):
        """Test that only specified fields are normalized."""
        records = [
            {"name": "", "city": "", "state": ""},
        ]
        
        result = normalize_empty_strings(records, ['city', 'state'])
        
        assert result[0]['name'] == ""  # Not normalized
        assert result[0]['city'] is None
        assert result[0]['state'] is None

    def test_handles_none_values(self):
        """Test that None values are preserved."""
        records = [
            {"name": None, "city": "", "state": "TX"},
        ]
        
        result = normalize_empty_strings(records)
        
        assert result[0]['name'] is None
        assert result[0]['city'] is None
        assert result[0]['state'] == "TX"

    def test_does_not_mutate_original(self):
        """Test that original records are not modified."""
        records = [{"city": "", "state": ""}]
        result = normalize_empty_strings(records)
        
        # Original unchanged
        assert records[0]['city'] == ""
        
        # Result changed
        assert result[0]['city'] is None
