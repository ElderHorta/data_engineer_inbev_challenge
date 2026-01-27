"""
Unit tests for brewery-specific Airflow tasks.

This file tests DOMAIN-SPECIFIC brewery transformations and tasks:
- Brewery coordinate normalization using generic utilities
- Brewery API data handling
- Business logic specific to brewery data

For GENERIC Bronze layer tests (usable with any data source), 
see test_bronze_layer.py.

Test Philosophy (Clean Code / SOLID):
- Single Responsibility: Each test file tests ONE concern
- Generic layer tests (test_bronze_layer.py) → Test reusable framework
- Domain tests (this file) → Test brewery-specific business logic
"""

import pytest

from src.utils.data_helpers import convert_fields_to_float


class TestBreweryCoordinateNormalization:
    """Tests for brewery coordinate normalization.
    
    Tests the application of convert_fields_to_float to brewery-specific
    longitude/latitude fields. This handles API data where coordinates 
    may come as integers instead of floats due to JSON parsing quirks.
    The normalization happens BEFORE DataFrame creation.
    """

    def test_converts_integer_coordinates_to_float(self):
        """Test that integer coordinates are converted to float."""
        breweries = [
            {"id": "1", "name": "Brewery A", "latitude": 40, "longitude": -74},
            {"id": "2", "name": "Brewery B", "latitude": 34, "longitude": -118},
        ]
        
        result = convert_fields_to_float(breweries, ['longitude', 'latitude'])
        
        # Verify coordinates are now floats
        assert isinstance(result[0]["latitude"], float)
        assert isinstance(result[0]["longitude"], float)
        assert result[0]["latitude"] == 40.0
        assert result[0]["longitude"] == -74.0
        
        assert isinstance(result[1]["latitude"], float)
        assert result[1]["latitude"] == 34.0
        assert result[1]["longitude"] == -118.0

    def test_preserves_float_coordinates(self):
        """Test that float coordinates remain unchanged."""
        breweries = [
            {"id": "1", "name": "Brewery A", "latitude": 40.7128, "longitude": -74.0060},
            {"id": "2", "name": "Brewery B", "latitude": 34.0522, "longitude": -118.2437},
        ]
        
        result = convert_fields_to_float(breweries, ['longitude', 'latitude'])
        
        # Verify coordinates remain floats with precision
        assert isinstance(result[0]["latitude"], float)
        assert abs(result[0]["latitude"] - 40.7128) < 0.0001
        assert abs(result[0]["longitude"] - (-74.0060)) < 0.0001

    def test_handles_null_coordinates(self):
        """Test that null coordinates are handled gracefully."""
        breweries = [
            {"id": "1", "name": "Brewery A", "latitude": 40, "longitude": -74},
            {"id": "2", "name": "Brewery B", "latitude": None, "longitude": None},
            {"id": "3", "name": "Brewery C", "latitude": 34, "longitude": None},
        ]
        
        result = convert_fields_to_float(breweries, ['longitude', 'latitude'])
        
        # Verify null handling
        assert result[1]["latitude"] is None
        assert result[1]["longitude"] is None
        
        # Partial null
        assert result[2]["latitude"] == 34.0
        assert result[2]["longitude"] is None

    def test_handles_missing_coordinate_columns(self):
        """Test that records without coordinate fields pass through unchanged."""
        breweries = [
            {"id": "1", "name": "Brewery A", "brewery_type": "micro"},
            {"id": "2", "name": "Brewery B", "brewery_type": "regional"},
        ]
        
        result = convert_fields_to_float(breweries, ['longitude', 'latitude'])
        
        # Verify original fields preserved
        assert result[0]["id"] == "1"
        assert result[0]["name"] == "Brewery A"
        assert result[0]["brewery_type"] == "micro"
        assert "latitude" not in result[0]
        assert "longitude" not in result[0]

    def test_preserves_other_fields(self):
        """Test that non-coordinate fields are preserved unchanged."""
        breweries = [
            {
                "id": "1",
                "name": "Test Brewery",
                "brewery_type": "micro",
                "city": "Austin",
                "state": "Texas",
                "latitude": 30,
                "longitude": -97,
            },
        ]
        
        result = convert_fields_to_float(breweries, ['longitude', 'latitude'])
        
        assert result[0]["id"] == "1"
        assert result[0]["name"] == "Test Brewery"
        assert result[0]["brewery_type"] == "micro"
        assert result[0]["city"] == "Austin"
        assert result[0]["state"] == "Texas"
        assert result[0]["latitude"] == 30.0
        assert result[0]["longitude"] == -97.0

    def test_does_not_mutate_original_list(self):
        """Test that the function does not modify the original input."""
        breweries = [
            {"id": "1", "latitude": 40, "longitude": -74},
        ]
        
        result = convert_fields_to_float(breweries, ['longitude', 'latitude'])
        
        # Original should still have integer
        assert breweries[0]["latitude"] == 40
        assert isinstance(breweries[0]["latitude"], int)
        
        # Result should have float
        assert result[0]["latitude"] == 40.0
        assert isinstance(result[0]["latitude"], float)

    def test_handles_empty_list(self):
        """Test that empty list returns empty list."""
        result = convert_fields_to_float([], ['longitude', 'latitude'])
        assert result == []

    def test_handles_string_coordinates_gracefully(self):
        """Test that invalid coordinate types are handled gracefully."""
        breweries = [
            {"id": "1", "latitude": "invalid", "longitude": -74},
        ]
        
        result = convert_fields_to_float(breweries, ['longitude', 'latitude'])
        
        # Invalid should be set to None
        assert result[0]["latitude"] is None
        assert result[0]["longitude"] == -74.0


class TestBreweryDataFixture:
    """Tests using full brewery data structure.
    
    These tests verify brewery-specific data handling with the complete
    brewery schema as returned by the Open Brewery DB API.
    """

    @pytest.fixture
    def sample_brewery_records(self):
        """Sample brewery records matching API response structure."""
        return [
            {
                "id": "brewery-1",
                "name": "Austin Beerworks",
                "brewery_type": "micro",
                "address_1": "3001 Industrial Terrace",
                "city": "Austin",
                "state_province": "Texas",
                "postal_code": "78758",
                "country": "United States",
                "latitude": 30,  # Integer from API
                "longitude": -97,  # Integer from API
                "phone": "5123617900",
                "website_url": "http://austinbeerworks.com",
                "state": "Texas",
                "street": "3001 Industrial Terrace"
            },
            {
                "id": "brewery-2",
                "name": "Jester King Brewery",
                "brewery_type": "micro",
                "address_1": "13187 Fitzhugh Rd",
                "city": "Austin",
                "state_province": "Texas",
                "postal_code": "78736",
                "country": "United States",
                "latitude": 30.2234,  # Float from API
                "longitude": -98.0123,  # Float from API
                "phone": None,
                "website_url": "https://jesterkingbrewery.com",
                "state": "Texas",
                "street": "13187 Fitzhugh Rd"
            },
            {
                "id": "brewery-3",
                "name": "International Brewery",
                "brewery_type": "regional",
                "address_1": None,
                "city": "Mexico City",
                "state_province": None,
                "postal_code": None,
                "country": "Mexico",
                "latitude": None,
                "longitude": None,
                "phone": None,
                "website_url": None,
                "state": None,
                "street": None
            },
        ]

    def test_brewery_records_have_expected_fields(self, sample_brewery_records):
        """Validate brewery record structure matches API schema."""
        required_fields = [
            "id", "name", "brewery_type", "city", "country"
        ]
        
        for record in sample_brewery_records:
            for field in required_fields:
                assert field in record, f"Missing required field: {field}"

    def test_brewery_coordinates_can_be_mixed_types(self, sample_brewery_records):
        """Test that brewery data can have mixed coordinate types."""
        # First brewery has integer coordinates
        assert isinstance(sample_brewery_records[0]["latitude"], int)
        
        # Second brewery has float coordinates
        assert isinstance(sample_brewery_records[1]["latitude"], float)
        
        # Third brewery has null coordinates
        assert sample_brewery_records[2]["latitude"] is None

    def test_normalize_full_brewery_records(self, sample_brewery_records):
        """Test normalizing complete brewery records with mixed coordinate types."""
        result = convert_fields_to_float(sample_brewery_records, ['longitude', 'latitude'])
        
        # All coordinates should now be floats (or None)
        assert result[0]["latitude"] == 30.0
        assert isinstance(result[0]["latitude"], float)
        
        assert result[1]["latitude"] == 30.2234
        assert isinstance(result[1]["latitude"], float)
        
        assert result[2]["latitude"] is None
