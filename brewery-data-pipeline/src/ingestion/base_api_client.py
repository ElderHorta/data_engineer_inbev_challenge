"""
Base API Client

Generic HTTP client with retry logic, rate limiting, and error handling.
Designed to be extended by specific API clients (BreweryAPI, etc.).

This class handles all generic HTTP concerns:
- Session management with connection pooling
- Retry logic with exponential backoff
- Health check pattern
- Timeout configuration
- Error handling and logging

Usage:
    class MyAPIClient(BaseAPIClient):
        def __init__(self):
            super().__init__(
                base_url="https://api.example.com",
                timeout=30,
                retries=3
            )
        
        def fetch_data(self):
            return self._get("/endpoint")
"""

import requests
from typing import Dict, Optional, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

from src.utils.logger import get_logger

logger = get_logger(__name__)


class BaseAPIClient:
    """
    Generic base class for REST API clients.
    
    Provides common HTTP functionality that can be reused across different APIs.
    Child classes should implement domain-specific methods using the protected
    _get(), _post(), etc. methods.
    """
    
    def __init__(
        self,
        base_url: str,
        timeout: int = 30,
        retries: int = 3,
        backoff_factor: float = 2.0,
        status_forcelist: Optional[list] = None
    ):
        """
        Initialize base API client with HTTP configuration.
        
        Args:
            base_url: Base URL for the API (e.g., "https://api.example.com")
            timeout: Request timeout in seconds
            retries: Number of retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier (wait = backoff_factor * (2 ^ retry_number))
            status_forcelist: HTTP status codes to retry on (default: [429, 500, 502, 503, 504])
        
        Example:
            # For Open Brewery DB API
            client = BaseAPIClient(
                base_url="https://api.openbrewerydb.org/v1",
                timeout=30,
                retries=3
            )
        """
        self.base_url = base_url.rstrip('/')  # Remove trailing slash for consistency
        self.timeout = timeout
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.status_forcelist = status_forcelist or [429, 500, 502, 503, 504]
        self.session = self._create_session()
        
        logger.info(f"Initialized API client for {self.base_url}")
    
    def _create_session(self) -> requests.Session:
        """
        Create requests session with retry logic and connection pooling.
        
        Why this pattern:
        - Session reuses TCP connections (faster than creating new connection per request)
        - HTTPAdapter enables automatic retries with exponential backoff
        - Retry on transient failures (429 rate limit, 5xx server errors)
        
        Returns:
            Configured requests.Session with retry strategy
        """
        session = requests.Session()
        
        retry_strategy = Retry(
            total=self.retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=self.status_forcelist,
            allowed_methods=["GET"]  # Only GET is implemented
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        logger.debug(f"Created session with {self.retries} retries and {self.backoff_factor}s backoff")
        return session
    
    def health_check(self, endpoint: str = "/", expected_status: int = 200) -> bool:
        """
        Generic health check for API availability.
        
        Args:
            endpoint: Health check endpoint (default: root "/")
            expected_status: Expected HTTP status code for healthy API (default: 200)
        
        Returns:
            bool: True if API is healthy, False otherwise
        
        Example:
            # Check if API is available
            if client.health_check("/health"):
                data = client.fetch_data()
        """
        try:
            url = f"{self.base_url}{endpoint}"
            response = self.session.get(url, timeout=self.timeout)
            
            is_healthy = response.status_code == expected_status
            
            if is_healthy:
                logger.info(f"API health check passed for {self.base_url}")
            else:
                logger.error(
                    f"API health check failed for {self.base_url}. "
                    f"Expected {expected_status}, got {response.status_code}"
                )
            
            return is_healthy
            
        except Exception as e:
            logger.error(f"API health check failed for {self.base_url}: {str(e)}")
            return False
    
    def _get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> requests.Response:
        """
        Generic GET request method.
        
        Protected method (underscore prefix) - used by child classes to implement
        domain-specific methods like fetch_breweries(), fetch_weather(), etc.
        
        Args:
            endpoint: API endpoint (e.g., "/breweries" or "/weather")
            params: Query parameters (e.g., {"page": 1, "per_page": 50})
            headers: Custom headers (e.g., {"Authorization": "Bearer token"})
        
        Returns:
            requests.Response object (child classes call .json() on this)
        
        Raises:
            requests.exceptions.RequestException: On HTTP errors or network issues
        
        Example (in child class):
            def fetch_data(self, page=1):
                response = self._get("/data", params={"page": page})
                return response.json()
        """
        url = f"{self.base_url}{endpoint}"
        
        try:
            logger.debug(f"GET {url} with params={params}")
            response = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=self.timeout
            )
            response.raise_for_status()  # Raise HTTPError for 4xx/5xx
            
            logger.debug(f"GET {url} returned {response.status_code}")
            return response
            
        except requests.exceptions.RequestException as e:
            logger.error(f"GET {url} failed: {str(e)}")
            raise
    
    def close(self):
        """Close the session and release connections."""
        if self.session:
            self.session.close()
            logger.debug(f"Closed session for {self.base_url}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures session is closed."""
        self.close()
