"""Shared utility functions and helpers."""

from .logger import get_logger
from .config import get_config
from .spark_session import get_spark_session
from .data_helpers import (
    convert_fields_to_float,
    convert_fields_to_int,
    normalize_empty_strings,
)
from .api_helpers import (
    check_api_health,
)
from .schema import (
    build_schema_from_config,
    build_json_parsing_schema,
    get_required_fields,
    get_field_types,
)

__all__ = [
    'get_logger',
    'get_config',
    'get_spark_session',
    'convert_fields_to_float',
    'convert_fields_to_int',
    'normalize_empty_strings',
    'check_api_health',
    'build_schema_from_config',
    'build_json_parsing_schema',
    'get_required_fields',
    'get_field_types',
]
