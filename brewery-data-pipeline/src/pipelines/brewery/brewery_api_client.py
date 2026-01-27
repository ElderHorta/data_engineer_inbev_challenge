"""
Brewery API Client

Specific implementation for Open Brewery DB API.
Extends BaseAPIClient with brewery-specific domain logic.

This class only contains brewery-specific concerns:
- Brewery pagination logic with metadata-aware progress tracking
- Brewery-specific endpoints (/breweries, /breweries/{id}, /breweries/meta)
- Brewery data validation
- Rate limiting specific to Open Brewery DB (configurable)

All generic HTTP logic (retries, session management, health checks) is inherited
from BaseAPIClient - no code duplication!

API Documentation: https://www.openbrewerydb.org/documentation
- per_page: Default 50, Maximum 200
- Metadata endpoint: /breweries/meta returns {"total": N, "page": 1, "per_page": 50}

Usage:
    from src.pipelines.brewery.brewery_api_client import BreweryAPIClient
    
    client = BreweryAPIClient()
    
    # Get total count first (for progress tracking)
    total = client.get_total_breweries()
    
    # Fetch all breweries with progress tracking
    breweries = client.fetch_all_breweries()
"""

import time
import math
from typing import List, Dict, Optional

from src.ingestion.base_api_client import BaseAPIClient
from src.utils.logger import get_logger
from src.utils.config import get_config

logger = get_logger(__name__)


class BreweryAPIClient(BaseAPIClient):
    """
    Client for Open Brewery DB API.
    
    Extends BaseAPIClient with brewery-specific methods for fetching brewery data.
    Inherits all generic HTTP functionality (retries, health checks, session management).
    
    Features:
        - Metadata-aware pagination (knows total count before fetching)
        - Progress tracking for enterprise observability
        - Configurable rate limiting
        - Max per_page of 200 (API limit)
    """
    
    # API constraints (from documentation)
    API_MAX_PER_PAGE = 200  # Maximum allowed by Open Brewery DB
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize Brewery API client with configuration from brewery_config.yaml.
        
        Configuration is loaded from src/utils/config.py which reads:
        - dags/brewery/brewery_config.yaml (base configuration)
        - Environment variables (override: API_BASE_URL, API_TIMEOUT, etc.)
        
        Args:
            config: Optional configuration dict for dependency injection (testing).
                    If None, loads from get_config().
        """
        self.config = config or get_config()
        
        # Extract brewery API specific config
        api_config = self.config['api']
        
        # Initialize parent class with brewery API settings
        super().__init__(
            base_url=api_config['base_url'],
            timeout=api_config['timeout'],
            retries=api_config['retries'],
            backoff_factor=2.0,  # 2s, 4s, 8s exponential backoff
            status_forcelist=[429, 500, 502, 503, 504]
        )
        
        # Brewery-specific configuration with validation
        configured_per_page = api_config.get('per_page', 50)
        self.per_page = min(configured_per_page, self.API_MAX_PER_PAGE)
        
        if configured_per_page > self.API_MAX_PER_PAGE:
            logger.warning(
                f"Configured per_page ({configured_per_page}) exceeds API maximum ({self.API_MAX_PER_PAGE}). "
                f"Using {self.API_MAX_PER_PAGE}."
            )
        
        # Rate limiting configuration (configurable, not hardcoded)
        rate_limit_config = api_config.get('rate_limit', {})
        requests_per_minute = rate_limit_config.get('requests_per_minute', 60)
        self.request_delay = 60.0 / requests_per_minute  # Convert to delay between requests
        
        logger.info(
            f"Initialized BreweryAPIClient with base_url={self.base_url}, "
            f"per_page={self.per_page}, request_delay={self.request_delay:.2f}s"
        )
    
    def health_check(self) -> bool:
        """
        Check if Open Brewery DB API is accessible.
        
        Overrides BaseAPIClient.health_check() with brewery-specific endpoint.
        Uses /breweries endpoint with per_page=1 to minimize data transfer.
        
        Returns:
            bool: True if API is healthy, False otherwise
        
        Example:
            client = BreweryAPIClient()
            if not client.health_check():
                raise Exception("Brewery API is down")
        """
        try:
            # Use inherited _get() method from BaseAPIClient
            response = self._get(
                endpoint="/breweries",
                params={'per_page': 1}
            )
            
            is_healthy = response.status_code == 200
            
            if is_healthy:
                logger.info("Brewery API health check passed")
            else:
                logger.error(f"Brewery API health check failed with status: {response.status_code}")
            
            return is_healthy
            
        except Exception as e:
            logger.error(f"Brewery API health check failed: {str(e)}")
            return False
    
    def get_total_breweries(self) -> int:
        """
        Get total number of breweries from API metadata endpoint.
        
        Uses /breweries/meta endpoint which returns count without fetching data.
        This enables progress tracking during pagination.
        
        API Endpoint: https://api.openbrewerydb.org/v1/breweries/meta
        Response: {"total": "8000", "page": "1", "per_page": "50"}
        
        Returns:
            int: Total number of breweries available in the API
        
        Raises:
            requests.exceptions.RequestException: On HTTP errors
            
        Example:
            client = BreweryAPIClient()
            total = client.get_total_breweries()
            print(f"API has {total} breweries")
        """
        try:
            logger.info("Fetching brewery metadata to get total count")
            response = self._get(endpoint="/breweries/meta")
            metadata = response.json()
            
            # API returns total as string, convert to int
            total = int(metadata.get('total', 0))
            logger.info(f"API reports {total} total breweries")
            
            return total
            
        except Exception as e:
            logger.error(f"Failed to fetch brewery metadata: {str(e)}")
            raise
    
    def fetch_breweries_page(self, page: int = 1) -> Dict:
        """
        Fetch a single page of breweries.
        
        Uses inherited _get() method from BaseAPIClient for the actual HTTP request.
        This method only handles brewery-specific pagination logic.
        
        Args:
            page: Page number to fetch (1-indexed)
            
        Returns:
            Dict containing:
                - breweries: List of brewery records
                - page: Current page number
                - count: Number of breweries in this page
                - per_page: Breweries per page setting
        
        Raises:
            requests.exceptions.RequestException: On HTTP errors or network issues
        
        Example:
            client = BreweryAPIClient()
            result = client.fetch_breweries_page(page=1)
            print(f"Fetched {result['count']} breweries from page {result['page']}")
        """
        params = {
            'page': page,
            'per_page': self.per_page
        }
        
        try:
            logger.info(f"Fetching breweries page {page}")
            
            # Use inherited _get() method - no HTTP code duplication!
            response = self._get(endpoint="/breweries", params=params)
            breweries = response.json()
            
            return {
                'breweries': breweries,
                'page': page,
                'count': len(breweries),
                'per_page': self.per_page
            }
            
        except Exception as e:
            logger.error(f"Failed to fetch breweries page {page}: {str(e)}")
            raise
    
    def fetch_all_breweries(self) -> List[Dict]:
        """
        Fetch all breweries with automatic pagination and progress tracking.
        
        Uses metadata endpoint first to get total count, enabling:
        - Accurate progress percentage logging
        - Estimated time remaining calculations
        - Better observability for enterprise monitoring
        
        Implements configurable rate limiting to avoid overwhelming the API.
        Rate limit is read from brewery_config.yaml (api.rate_limit.requests_per_minute).
        
        Returns:
            List of all brewery records from all pages
        
        Raises:
            requests.exceptions.RequestException: On unrecoverable HTTP errors
            
        Example:
            client = BreweryAPIClient()
            all_breweries = client.fetch_all_breweries()
            print(f"Total breweries: {len(all_breweries)}")
        """
        all_breweries = []
        page = 1
        
        # Get total count for progress tracking (enterprise observability)
        try:
            total_breweries = self.get_total_breweries()
            total_pages = math.ceil(total_breweries / self.per_page)
            logger.info(
                f"Starting brewery fetch: {total_breweries} breweries across ~{total_pages} pages "
                f"(per_page={self.per_page})"
            )
        except Exception as e:
            # Fallback: continue without total count if metadata fails
            logger.warning(f"Could not get total count from metadata: {e}. Continuing without progress tracking.")
            total_breweries = None
            total_pages = None
        
        while True:
            try:
                result = self.fetch_breweries_page(page)
                breweries = result['breweries']
                
                # Empty page means we've reached the end
                if not breweries:
                    logger.info(f"No more breweries found at page {page}. Pagination complete.")
                    break
                
                all_breweries.extend(breweries)
                
                # Debug: Log first page sample for troubleshooting (shows data ASAP)
                if page == 1:
                    first_page_sample = breweries[:min(5, len(breweries))]
                    logger.info(f"First page sample ({len(first_page_sample)} records):")
                    for idx, brewery in enumerate(first_page_sample, 1):
                        logger.info(
                            f"  [{idx}] {brewery.get('name', 'N/A')} - "
                            f"Type: {brewery.get('brewery_type', 'N/A')}, "
                            f"Location: {brewery.get('city', 'N/A')}, {brewery.get('state', 'N/A')}, "
                            f"Coords: ({brewery.get('longitude', 'N/A')}, {brewery.get('latitude', 'N/A')})"
                        )
                
                # Progress logging with percentage (for enterprise monitoring dashboards)
                if total_breweries:
                    progress_pct = (len(all_breweries) / total_breweries) * 100
                    logger.info(
                        f"Progress: {len(all_breweries)}/{total_breweries} ({progress_pct:.1f}%) - "
                        f"Page {page}/{total_pages}"
                    )
                else:
                    logger.info(
                        f"Fetched {len(breweries)} breweries from page {page}. "
                        f"Total so far: {len(all_breweries)}"
                    )
                
                # Rate limiting: configurable delay between requests
                # Why: Respectful to free API, prevents 429 errors
                time.sleep(self.request_delay)
                
                page += 1
                
            except Exception as e:
                logger.error(f"Error fetching breweries page {page}: {str(e)}")
                raise
        
        # Final validation
        if total_breweries and len(all_breweries) != total_breweries:
            logger.warning(
                f"Record count mismatch: expected {total_breweries}, got {len(all_breweries)}. "
                f"This may indicate API data changed during fetch."
            )
        
        logger.info(f"Successfully fetched {len(all_breweries)} total breweries")
        return all_breweries
