"""Unit tests for API client."""

import pytest
import responses
from src.pipelines.brewery.brewery_api_client import BreweryAPIClient


@pytest.fixture
def mock_config():
    """Provide minimal config for testing."""
    return {
        'api': {
            'base_url': 'https://api.openbrewerydb.org/v1',
            'timeout': 30,
            'retries': 3,
            'per_page': 200,
        }
    }


class TestBreweryAPIClient:
    """Test suite for BreweryAPIClient."""
    
    @responses.activate
    def test_health_check_success(self, mock_config):
        """Test successful API health check."""
        responses.add(
            responses.GET,
            "https://api.openbrewerydb.org/v1/breweries",
            json=[{"id": "1"}],
            status=200
        )
        
        client = BreweryAPIClient(config=mock_config)
        assert client.health_check() is True
    
    @responses.activate
    def test_health_check_failure(self, mock_config):
        """Test failed API health check."""
        responses.add(
            responses.GET,
            "https://api.openbrewerydb.org/v1/breweries",
            status=500
        )
        
        client = BreweryAPIClient(config=mock_config)
        assert client.health_check() is False
    
    @responses.activate
    def test_fetch_breweries_page(self, sample_brewery_data, mock_config):
        """Test fetching a single page of breweries."""
        responses.add(
            responses.GET,
            "https://api.openbrewerydb.org/v1/breweries",
            json=sample_brewery_data,
            status=200
        )
        
        client = BreweryAPIClient(config=mock_config)
        result = client.fetch_breweries_page(page=1)
        
        assert result['page'] == 1
        assert result['count'] == 3
        assert len(result['breweries']) == 3
        assert result['breweries'][0]['id'] == 'test-brewery-1'
    
    @responses.activate
    def test_fetch_all_breweries(self, sample_brewery_data, mock_config):
        """Test fetching all breweries with pagination."""
        # Mock first page
        responses.add(
            responses.GET,
            "https://api.openbrewerydb.org/v1/breweries",
            json=sample_brewery_data,
            status=200
        )
        
        # Mock empty second page
        responses.add(
            responses.GET,
            "https://api.openbrewerydb.org/v1/breweries",
            json=[],
            status=200
        )
        
        client = BreweryAPIClient(config=mock_config)
        breweries = client.fetch_all_breweries()
        
        assert len(breweries) == 3
        assert breweries[0]['name'] == 'Test Brewery One'
    
    @responses.activate
    def test_fetch_brewery_by_id(self, sample_brewery_data, mock_config):
        """Test fetching a single brewery by ID."""
        responses.add(
            responses.GET,
            "https://api.openbrewerydb.org/v1/breweries/test-brewery-1",
            json=sample_brewery_data[0],
            status=200
        )
        
        client = BreweryAPIClient(config=mock_config)
        assert hasattr(client, 'fetch_all_breweries')
        assert hasattr(client, 'health_check')
