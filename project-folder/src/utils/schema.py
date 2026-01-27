"""
Schema Utilities

Provides utilities to build PySpark schemas from configuration.
Follows configuration-driven design pattern - schema definitions live in YAML,
not hardcoded in Python.

Why configuration-driven schemas:
- Single source of truth (brewery_config.yaml)
- Easy to modify without code changes
- Supports multiple data sources with different schemas
- Enables schema validation against documented contracts
"""

from typing import Dict, List, Optional
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    FloatType, DoubleType, BooleanType, TimestampType, DateType
)

from src.utils.logger import get_logger

logger = get_logger(__name__)


# Mapping from config type strings to PySpark types
# Why separate mapping: Decouples YAML config format from PySpark internals
TYPE_MAPPING = {
    "string": StringType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "long": LongType(),
    "float": FloatType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "timestamp": TimestampType(),
    "date": DateType(),
}


def build_schema_from_config(
    fields_config: List[Dict],
    include_audit: bool = False,
    audit_fields_config: Optional[List[Dict]] = None
) -> StructType:
    """
    Build PySpark StructType schema from configuration.
    
    Converts YAML schema definition to PySpark schema.
    Used by Silver layer to parse JSON payloads from Bronze.
    
    Args:
        fields_config: List of field definitions from config, each with:
            - name: field name (required)
            - type: data type string (required)
            - nullable: whether field allows nulls (default: True)
        include_audit: Whether to include audit fields in schema
        audit_fields_config: List of audit field definitions (optional)
        
    Returns:
        PySpark StructType schema
        
    Example config format:
        fields:
          - name: "id"
            type: "string"
            nullable: false
          - name: "latitude"
            type: "double"
            nullable: true
            
    Example usage:
        >>> config = get_config()
        >>> schema = build_schema_from_config(
        ...     config['schemas']['silver']['fields']
        ... )
    """
    struct_fields = []
    
    for field_def in fields_config:
        name = field_def['name']
        type_str = field_def.get('type', 'string').lower()
        nullable = field_def.get('nullable', True)
        
        # Get PySpark type from mapping
        spark_type = TYPE_MAPPING.get(type_str)
        if spark_type is None:
            logger.warning(
                f"Unknown type '{type_str}' for field '{name}', defaulting to StringType"
            )
            spark_type = StringType()
        
        struct_fields.append(StructField(name, spark_type, nullable))
    
    # Add audit fields if requested
    if include_audit and audit_fields_config:
        for audit_field in audit_fields_config:
            name = audit_field['name']
            type_str = audit_field.get('type', 'string').lower()
            nullable = audit_field.get('nullable', True)
            
            spark_type = TYPE_MAPPING.get(type_str, StringType())
            struct_fields.append(StructField(name, spark_type, nullable))
    
    schema = StructType(struct_fields)
    logger.debug(f"Built schema with {len(struct_fields)} fields")
    
    return schema


def build_json_parsing_schema(fields_config: List[Dict]) -> StructType:
    """
    Build schema for JSON parsing (all fields as StringType initially).
    
    When parsing JSON from Bronze layer, we first parse all fields as strings,
    then convert to proper types in Silver layer transformations.
    This avoids type casting errors during JSON parsing.
    
    Args:
        fields_config: List of field definitions from config
        
    Returns:
        PySpark StructType with all fields as StringType
        
    Why parse as strings first:
    - JSON numbers can be inconsistent (42 vs 42.0)
    - Avoids CANNOT_MERGE_TYPE errors during parsing
    - Type conversion happens in controlled transformation step
    """
    struct_fields = []
    
    for field_def in fields_config:
        name = field_def['name']
        # All fields as StringType for JSON parsing
        struct_fields.append(StructField(name, StringType(), True))
    
    schema = StructType(struct_fields)
    logger.debug(f"Built JSON parsing schema with {len(struct_fields)} string fields")
    
    return schema


def get_required_fields(fields_config: List[Dict]) -> List[str]:
    """
    Get list of required (non-nullable) field names from config.
    
    Used for validation to ensure required fields are present and non-null.
    
    Args:
        fields_config: List of field definitions from config
        
    Returns:
        List of field names where nullable=false
    """
    return [
        field['name']
        for field in fields_config
        if not field.get('nullable', True)
    ]


def get_field_types(fields_config: List[Dict]) -> Dict[str, str]:
    """
    Get mapping of field names to their target types.
    
    Used by Silver layer to know which fields need type conversion.
    
    Args:
        fields_config: List of field definitions from config
        
    Returns:
        Dict mapping field name to type string
    """
    return {
        field['name']: field.get('type', 'string')
        for field in fields_config
    }
