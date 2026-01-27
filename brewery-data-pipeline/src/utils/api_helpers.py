"""
Generic API helper utilities.

Reusable functions for working with REST APIs across different pipelines.
These utilities are domain-agnostic and can be used with any API client.

Usage:
    from src.utils.api_helpers import check_api_health
    from src.pipelines.brewery.brewery_api_client import BreweryAPIClient
    
    client = BreweryAPIClient()
    is_healthy = check_api_health(client, api_name="Open Brewery DB")
"""

from typing import Protocol, Optional
from src.utils.logger import get_logger

logger = get_logger(__name__)


class HealthCheckable(Protocol):
    """Protocol for API clients that support health checks.
    
    Any API client implementing a health_check() method can be used
    with the generic check_api_health utility function.
    """
    
    def health_check(self) -> bool:
        """Check if the API is available and responding.
        
        Returns:
            bool: True if API is healthy, False otherwise
        """
        ...


def check_api_health(
    api_client: HealthCheckable,
    api_name: Optional[str] = None,
    raise_on_failure: bool = True
) -> bool:
    """
    Check if an API is available and healthy.
    
    This is a generic utility that works with any API client implementing
    a health_check() method. Use this for fail-fast behavior at the start
    of data pipelines.
    
    Args:
        api_client: API client instance with health_check() method
        api_name: Human-readable API name for logging (e.g., "Open Brewery DB")
                  If None, uses the client's class name
        raise_on_failure: If True, raises Exception on health check failure
                         If False, returns False on failure
        
    Returns:
        bool: True if API is healthy
        
    Raises:
        Exception: If raise_on_failure=True and health check fails
        
    Examples:
        >>> from src.pipelines.brewery.brewery_api_client import BreweryAPIClient
        >>> client = BreweryAPIClient()
        >>> check_api_health(client, api_name="Open Brewery DB")
        True
        
        >>> # With custom API client
        >>> from src.ingestion.weather_api_client import WeatherAPIClient
        >>> weather_client = WeatherAPIClient()
        >>> is_healthy = check_api_health(
        ...     weather_client, 
        ...     api_name="Weather API",
        ...     raise_on_failure=False
        ... )
        >>> if not is_healthy:
        ...     print("Weather API is down, using cached data")
    """
    # Use class name if api_name not provided
    if api_name is None:
        api_name = api_client.__class__.__name__.replace('APIClient', '')
    
    logger.info(f"Starting {api_name} API health check")
    
    try:
        is_healthy = api_client.health_check()
    except Exception as e:
        logger.error(f"{api_name} API health check failed with exception: {e}")
        is_healthy = False
    
    if not is_healthy:
        error_msg = f"{api_name} API health check failed - API is not available"
        logger.error(error_msg)
        
        if raise_on_failure:
            raise Exception(error_msg)
        return False
    
    logger.info(f"{api_name} API health check passed")
    return True


def check_url_availability(url: str, timeout: int = 5) -> bool:
    """
    Simple check if a URL is reachable.
    
    This is a lightweight alternative to full API health checks when you just
    need to verify basic connectivity.
    
    Args:
        url: URL to check (e.g., "https://api.example.com")
        timeout: Request timeout in seconds (default: 5)
        
    Returns:
        bool: True if URL responds with 2xx or 3xx status code
        
    Example:
        >>> is_reachable = check_url_availability("https://api.openbrewerydb.org")
        >>> if not is_reachable:
        ...     print("API endpoint is not reachable")
    """
    import requests
    
    try:
        response = requests.get(url, timeout=timeout)
        return response.status_code < 400
    except requests.exceptions.RequestException as e:
        logger.warning(f"URL {url} is not available: {e}")
        return False
