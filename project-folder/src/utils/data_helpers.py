"""
Generic data transformation utilities.

Reusable helper functions for data transformations across different pipelines.
These functions are domain-agnostic and can be used with any data source.

Usage:
    from src.utils.data_helpers import convert_fields_to_float
    
    records = [{'temperature': '25', 'humidity': 60}]
    converted = convert_fields_to_float(records, ['temperature', 'humidity'])
"""

from typing import List, Dict, Any, Optional
from src.utils.logger import get_logger

logger = get_logger(__name__)


def convert_fields_to_float(
    records: List[Dict[str, Any]], 
    field_names: List[str],
    on_error: str = 'null'
) -> List[Dict[str, Any]]:
    """
    Convert specified fields from int/str to float in dictionary records.
    
    This is useful for APIs that return inconsistent data types:
    - Sometimes int: 36
    - Sometimes float: 36.123
    - Sometimes string: "36.5"
    - Sometimes null: None
    
    PySpark's DoubleType() requires consistent float types, not mixed int/str.
    
    Args:
        records: List of dictionary records to transform
        field_names: List of field names to convert to float
        on_error: How to handle conversion errors:
            - 'null': Set field to None (default)
            - 'skip': Keep original value
            - 'raise': Raise ValueError
            
    Returns:
        New list of records with converted fields (original list unchanged)
        
    Examples:
        >>> records = [
        ...     {'id': '1', 'lat': 40, 'lon': -74.5},
        ...     {'id': '2', 'lat': '34.05', 'lon': -118},
        ...     {'id': '3', 'lat': None, 'lon': None},
        ... ]
        >>> result = convert_fields_to_float(records, ['lat', 'lon'])
        >>> result[0]['lat']
        40.0
        >>> result[1]['lat']
        34.05
        >>> result[2]['lat'] is None
        True
        
        >>> records = [{'temperature': 'invalid', 'pressure': 1013}]
        >>> result = convert_fields_to_float(records, ['temperature', 'pressure'])
        >>> result[0]['temperature'] is None  # Error → None
        True
        >>> result[0]['pressure']
        1013.0
    """
    if on_error not in ('null', 'skip', 'raise'):
        raise ValueError(f"Invalid on_error value: {on_error}. Must be 'null', 'skip', or 'raise'")
    
    converted = []
    for record in records:
        new_record = record.copy()
        
        for field in field_names:
            if field not in new_record:
                continue  # Field doesn't exist in this record
                
            value = new_record[field]
            
            if value is None:
                continue  # Already None, no conversion needed
            
            try:
                new_record[field] = float(value)
            except (ValueError, TypeError) as e:
                if on_error == 'null':
                    logger.warning(
                        f"Could not convert {field}={value} to float, setting to None. Error: {e}"
                    )
                    new_record[field] = None
                elif on_error == 'skip':
                    logger.debug(f"Could not convert {field}={value} to float, keeping original value")
                    # Keep original value (already in new_record)
                elif on_error == 'raise':
                    raise ValueError(f"Failed to convert {field}={value} to float") from e
        
        converted.append(new_record)
    
    return converted


def convert_fields_to_int(
    records: List[Dict[str, Any]], 
    field_names: List[str],
    on_error: str = 'null'
) -> List[Dict[str, Any]]:
    """
    Convert specified fields from str/float to int in dictionary records.
    
    Args:
        records: List of dictionary records to transform
        field_names: List of field names to convert to int
        on_error: How to handle conversion errors ('null', 'skip', 'raise')
            
    Returns:
        New list of records with converted fields
        
    Example:
        >>> records = [{'count': '42', 'total': 100.0}]
        >>> result = convert_fields_to_int(records, ['count', 'total'])
        >>> result[0]['count']
        42
        >>> result[0]['total']
        100
    """
    if on_error not in ('null', 'skip', 'raise'):
        raise ValueError(f"Invalid on_error value: {on_error}. Must be 'null', 'skip', or 'raise'")
    
    converted = []
    for record in records:
        new_record = record.copy()
        
        for field in field_names:
            if field not in new_record:
                continue
                
            value = new_record[field]
            
            if value is None:
                continue
            
            try:
                # Convert to float first to handle "42.0" strings, then to int
                new_record[field] = int(float(value))
            except (ValueError, TypeError) as e:
                if on_error == 'null':
                    logger.warning(
                        f"Could not convert {field}={value} to int, setting to None. Error: {e}"
                    )
                    new_record[field] = None
                elif on_error == 'skip':
                    logger.debug(f"Could not convert {field}={value} to int, keeping original value")
                elif on_error == 'raise':
                    raise ValueError(f"Failed to convert {field}={value} to int") from e
        
        converted.append(new_record)
    
    return converted


def normalize_empty_strings(
    records: List[Dict[str, Any]],
    field_names: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
    Convert empty strings to None in dictionary records.
    
    Useful for cleaning API data where empty values come as "" instead of null.
    
    Args:
        records: List of dictionary records to transform
        field_names: List of specific fields to normalize (None = all fields)
            
    Returns:
        New list of records with normalized empty strings
        
    Example:
        >>> records = [{'name': 'Test', 'city': '', 'state': ''}]
        >>> result = normalize_empty_strings(records)
        >>> result[0]['city'] is None
        True
    """
    normalized = []
    for record in records:
        new_record = record.copy()
        
        fields_to_check = field_names if field_names else new_record.keys()
        
        for field in fields_to_check:
            if field in new_record and new_record[field] == '':
                new_record[field] = None
        
        normalized.append(new_record)
    
    return normalized
