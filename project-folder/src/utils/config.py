"""
Configuration Management

Loads and manages configuration from files and environment variables.
"""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path


def load_config(config_path: str) -> Dict:
    """
    Load all YAML configuration files from a directory.
    
    Recursively loads all .yaml and .yml files from the given directory
    and merges them into a single configuration dictionary.
    
    Args:
        config_path: Path to directory containing YAML files
        
    Returns:
        Merged configuration dictionary from all YAML files
        
    Raises:
        FileNotFoundError: If config_path doesn't exist or no YAML files found
        yaml.YAMLError: If YAML files are malformed
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration path does not exist: {config_path}")
    
    yaml_files = _find_yaml_files(config_path)
    yaml_config = _load_and_merge_yaml_files(yaml_files)

    config = merge_env_vars(yaml_config)
    
    return config


def _find_yaml_files(directory_path: str) -> list[Path]:
    """
    Find all YAML files in directory and subdirectories.
    
    Args:
        directory_path: Path to directory to search
        
    Returns:
        Sorted list of Path objects for YAML files
    """
    config_dir = Path(directory_path)
    yaml_files = list(config_dir.glob('**/*.yaml')) + list(config_dir.glob('**/*.yml'))

    if not yaml_files:
        raise FileNotFoundError(f"No YAML files found in: {directory_path}")

    return sorted(yaml_files)


def _load_and_merge_yaml_files(yaml_files: list[Path]) -> Dict:
    """
    Load and deep merge multiple YAML files.
    
    Args:
        yaml_files: List of Path objects to YAML files
        
    Returns:
        Merged configuration dictionary
        
    Raises:
        yaml.YAMLError: If any YAML file is malformed
    """
    config = {}
    
    for yaml_file in yaml_files:
        try:
            with open(yaml_file, 'r', encoding='utf-8') as f:
                file_config = yaml.safe_load(f)
                
            if file_config:
                config = _deep_merge(config, file_config)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Failed to parse {yaml_file}: {e}") from e
        except Exception as e:
            raise IOError(f"Failed to read {yaml_file}: {e}") from e
    
    return config


def _deep_merge(base: Dict, update: Dict) -> Dict:
    """
    Deep merge two dictionaries.
    
    Args:
        base: Base dictionary
        update: Dictionary to merge into base
        
    Returns:
        Merged dictionary
    """
    result = base.copy()
    
    for key, value in update.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    
    return result


def merge_env_vars(config: Dict) -> Dict:
    """
    Merge environment variables into configuration.
    
    Args:
        config: Base configuration
        
    Returns:
        Configuration with environment overrides
    """
    if os.getenv('STORAGE_TYPE'):
        config['storage']['type'] = os.getenv('STORAGE_TYPE')
    
    if os.getenv('BASE_PATH'):
        config['storage']['base_path'] = os.getenv('BASE_PATH')
    
    if os.getenv('API_BASE_URL'):
        config['api']['base_url'] = os.getenv('API_BASE_URL')
    
    if os.getenv('API_TIMEOUT'):
        config['api']['timeout'] = int(os.getenv('API_TIMEOUT'))
    
    return config


def get_config(dag_name: Optional[str] = None) -> Dict:
    """
    Get application configuration.
    
    Loads configuration from YAML files in the specified DAG directory.
    Falls back to default config if directory doesn't exist or has no YAML files.
    
    Args:
        dag_name: Optional DAG subfolder name (e.g., 'brewery')
                  If None, loads from entire dags/ directory
    
    Returns:
        Configuration dictionary
    """
    if os.getenv('CONFIG_PATH'):
        config_path = os.getenv('CONFIG_PATH')
    else:
        base_dags_path = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        if dag_name:
            config_path = f"{base_dags_path}/dags/{dag_name}"
        else:
            config_path = f"{base_dags_path}/dags"
    
    try:
        return load_config(config_path)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Configuration not found at {config_path}.\nError: {e}")


def reload_config(dag_name: Optional[str] = None) -> Dict:
    """
    Reload configuration from file.
    
    Note: Without caching, this is equivalent to get_config().
    Kept for API compatibility.
    
    Args:
        dag_name: Optional DAG subfolder name (e.g., 'brewery')
                  If None, loads from entire dags/ directory
    
    Returns:
        Configuration dictionary
    """
    return get_config(dag_name)
