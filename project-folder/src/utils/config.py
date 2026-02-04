"""
Configuration Management

Loads and manages configuration from files and environment variables.
"""

import os
import yaml
from typing import Dict, Any


_config_cache: Dict[str, Any] = None


def load_config(config_path: str) -> Dict:
    """
    Load configuration from YAML file.
    
    Generic function to load any YAML configuration file.
    
    Args:
        config_path: Path to configuration file (required)
        
    Returns:
        Configuration dictionary
    """
    global _config_cache
    
    if _config_cache is not None:
        return _config_cache
    
    if not os.path.exists(config_path):
        return get_default_config()
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Override with environment variables
    config = merge_env_vars(config)
    
    _config_cache = config
    return config


def get_default_config() -> Dict:
    """Get default configuration."""
    return {
        'api': {
            'base_url': 'https://api.openbrewerydb.org/v1',
            'timeout': 30,
            'retries': 3,
            'per_page': 200,
        },
        'storage': {
            'type': 'local',
            'base_path': '/data/',
            'layers': {
                'bronze': {
                    'path': 'bronze/breweries/',
                    'format': 'parquet',
                },
                'silver': {
                    'path': 'silver/breweries/',
                    'format': 'delta',
                },
                'gold': {
                    'path': 'gold/',
                    'format': 'delta',
                }
            }
        },
        'data_quality': {
            'thresholds': {
                'bronze_min_records': 100,
                'silver_quality_score': 0.80,
            }
        },
        'monitoring': {
            'alerts': {
                'slack': {
                    'enabled': False,
                },
                'email': {
                    'enabled': True,
                }
            }
        }
    }


def merge_env_vars(config: Dict) -> Dict:
    """
    Merge environment variables into configuration.
    
    Args:
        config: Base configuration
        
    Returns:
        Configuration with environment overrides
    """
    # Storage type
    if os.getenv('STORAGE_TYPE'):
        config['storage']['type'] = os.getenv('STORAGE_TYPE')
    
    # Base path
    if os.getenv('BASE_PATH'):
        config['storage']['base_path'] = os.getenv('BASE_PATH')
    
    # API configuration
    if os.getenv('API_BASE_URL'):
        config['api']['base_url'] = os.getenv('API_BASE_URL')
    
    if os.getenv('API_TIMEOUT'):
        config['api']['timeout'] = int(os.getenv('API_TIMEOUT'))
    
    return config


def get_config() -> Dict:
    """
    Get application configuration.
    
    Loads brewery pipeline configuration from default path.
    Path can be overridden via CONFIG_PATH environment variable.
    
    Returns:
        Configuration dictionary
    """
    config_path = os.getenv('CONFIG_PATH', '/opt/airflow/dags/brewery/brewery_config.yaml')
    return load_config(config_path)


def reload_config() -> Dict:
    """
    Reload configuration (clear cache).
    
    Clears cached configuration and reloads from file.
    Useful for testing or dynamic configuration updates.
    
    Returns:
        Fresh configuration dictionary
    """
    global _config_cache
    _config_cache = None
    config_path = os.getenv('CONFIG_PATH', '/opt/airflow/dags/brewery/brewery_config.yaml')
    return load_config(config_path)
