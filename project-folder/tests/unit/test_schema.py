"""
Unit tests for schema utilities.

Tests config-driven schema building without requiring Spark.
"""

import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, DoubleType, BooleanType
)

from src.utils.schema import (
    build_schema_from_config,
    build_json_parsing_schema,
    get_required_fields,
    get_field_types,
)


class TestBuildSchemaFromConfig:
    """Tests for build_schema_from_config function."""
    
    def test_builds_schema_from_field_list(self):
        """Test that schema is built correctly from config fields."""
        fields_config = [
            {"name": "id", "type": "string", "nullable": False},
            {"name": "count", "type": "integer", "nullable": True},
            {"name": "latitude", "type": "double", "nullable": True},
        ]
        
        schema = build_schema_from_config(fields_config)
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 3
        
        # Check field names
        field_names = [f.name for f in schema.fields]
        assert field_names == ["id", "count", "latitude"]
    
    def test_maps_types_correctly(self):
        """Test that config types are mapped to PySpark types."""
        fields_config = [
            {"name": "string_field", "type": "string"},
            {"name": "int_field", "type": "integer"},
            {"name": "float_field", "type": "float"},
            {"name": "double_field", "type": "double"},
            {"name": "bool_field", "type": "boolean"},
        ]
        
        schema = build_schema_from_config(fields_config)
        
        # Check types
        type_map = {f.name: type(f.dataType) for f in schema.fields}
        assert type_map["string_field"] == StringType
        assert type_map["int_field"] == IntegerType
        assert type_map["float_field"] == FloatType
        assert type_map["double_field"] == DoubleType
        assert type_map["bool_field"] == BooleanType
    
    def test_respects_nullable_setting(self):
        """Test that nullable is correctly set from config."""
        fields_config = [
            {"name": "required_field", "type": "string", "nullable": False},
            {"name": "optional_field", "type": "string", "nullable": True},
            {"name": "default_nullable", "type": "string"},  # Should default to True
        ]
        
        schema = build_schema_from_config(fields_config)
        
        nullable_map = {f.name: f.nullable for f in schema.fields}
        assert nullable_map["required_field"] is False
        assert nullable_map["optional_field"] is True
        assert nullable_map["default_nullable"] is True  # Default
    
    def test_unknown_type_defaults_to_string(self):
        """Test that unknown types default to StringType."""
        fields_config = [
            {"name": "unknown_field", "type": "custom_type"},
        ]
        
        schema = build_schema_from_config(fields_config)
        
        assert type(schema.fields[0].dataType) == StringType


class TestBuildJsonParsingSchema:
    """Tests for build_json_parsing_schema function."""
    
    def test_all_fields_are_string_type(self):
        """Test that all fields are StringType for JSON parsing."""
        fields_config = [
            {"name": "id", "type": "string"},
            {"name": "count", "type": "integer"},
            {"name": "latitude", "type": "double"},
            {"name": "active", "type": "boolean"},
        ]
        
        schema = build_json_parsing_schema(fields_config)
        
        # All should be StringType regardless of config type
        for field in schema.fields:
            assert type(field.dataType) == StringType
    
    def test_preserves_field_names(self):
        """Test that field names are preserved."""
        fields_config = [
            {"name": "brewery_id"},
            {"name": "brewery_name"},
            {"name": "latitude"},
        ]
        
        schema = build_json_parsing_schema(fields_config)
        
        field_names = [f.name for f in schema.fields]
        assert field_names == ["brewery_id", "brewery_name", "latitude"]


class TestGetRequiredFields:
    """Tests for get_required_fields function."""
    
    def test_returns_non_nullable_fields(self):
        """Test that only non-nullable fields are returned."""
        fields_config = [
            {"name": "id", "type": "string", "nullable": False},
            {"name": "name", "type": "string", "nullable": False},
            {"name": "optional", "type": "string", "nullable": True},
            {"name": "default", "type": "string"},  # defaults to nullable=True
        ]
        
        required = get_required_fields(fields_config)
        
        assert required == ["id", "name"]
    
    def test_returns_empty_list_if_all_nullable(self):
        """Test that empty list is returned if all fields are nullable."""
        fields_config = [
            {"name": "field1", "type": "string", "nullable": True},
            {"name": "field2", "type": "string"},  # defaults to nullable=True
        ]
        
        required = get_required_fields(fields_config)
        
        assert required == []


class TestGetFieldTypes:
    """Tests for get_field_types function."""
    
    def test_returns_type_mapping(self):
        """Test that correct type mapping is returned."""
        fields_config = [
            {"name": "id", "type": "string"},
            {"name": "count", "type": "integer"},
            {"name": "latitude", "type": "double"},
        ]
        
        types = get_field_types(fields_config)
        
        assert types == {
            "id": "string",
            "count": "integer",
            "latitude": "double",
        }
    
    def test_defaults_to_string(self):
        """Test that missing type defaults to string."""
        fields_config = [
            {"name": "field_without_type"},
        ]
        
        types = get_field_types(fields_config)
        
        assert types == {"field_without_type": "string"}
