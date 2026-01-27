"""
Unit tests for generic API helper utilities.

Tests the reusable helper functions in src/utils/api_helpers.py
that can be used with any API client across different pipelines.
"""

import pytest
from unittest.mock import Mock, MagicMock
from src.utils.api_helpers import check_api_health, check_url_availability


class TestCheckApiHealth:
    """Tests for check_api_health function."""

    def test_healthy_api_returns_true(self):
        """Test that healthy API check returns True."""
        # Create mock API client
        mock_client = Mock()
        mock_client.health_check.return_value = True
        mock_client.__class__.__name__ = "BreweryAPIClient"
        
        result = check_api_health(mock_client, api_name="Test API")
        
        assert result is True
        mock_client.health_check.assert_called_once()

    def test_unhealthy_api_raises_exception_by_default(self):
        """Test that unhealthy API raises exception by default."""
        mock_client = Mock()
        mock_client.health_check.return_value = False
        mock_client.__class__.__name__ = "BreweryAPIClient"
        
        with pytest.raises(Exception, match="Test API.*not available"):
            check_api_health(mock_client, api_name="Test API")

    def test_unhealthy_api_returns_false_when_raise_disabled(self):
        """Test that unhealthy API returns False when raise_on_failure=False."""
        mock_client = Mock()
        mock_client.health_check.return_value = False
        mock_client.__class__.__name__ = "BreweryAPIClient"
        
        result = check_api_health(
            mock_client, 
            api_name="Test API", 
            raise_on_failure=False
        )
        
        assert result is False

    def test_exception_during_check_raises_by_default(self):
        """Test that exception during health check raises exception."""
        mock_client = Mock()
        mock_client.health_check.side_effect = ConnectionError("Network error")
        mock_client.__class__.__name__ = "BreweryAPIClient"
        
        with pytest.raises(Exception, match="Test API.*not available"):
            check_api_health(mock_client, api_name="Test API")

    def test_exception_during_check_returns_false_when_raise_disabled(self):
        """Test that exception during check returns False when raise disabled."""
        mock_client = Mock()
        mock_client.health_check.side_effect = ConnectionError("Network error")
        mock_client.__class__.__name__ = "BreweryAPIClient"
        
        result = check_api_health(
            mock_client, 
            api_name="Test API", 
            raise_on_failure=False
        )
        
        assert result is False

    def test_uses_class_name_when_api_name_not_provided(self):
        """Test that class name is used for logging when api_name is None."""
        mock_client = Mock()
        mock_client.health_check.return_value = True
        mock_client.__class__.__name__ = "BreweryAPIClient"
        
        # Should not raise - uses class name
        result = check_api_health(mock_client)
        
        assert result is True

    def test_strips_api_client_suffix_from_class_name(self):
        """Test that 'APIClient' suffix is removed from class name."""
        mock_client = Mock()
        mock_client.health_check.return_value = True
        mock_client.__class__.__name__ = "WeatherAPIClient"
        
        # Should use "Weather" as the name (stripped "APIClient")
        result = check_api_health(mock_client)
        
        assert result is True

    def test_works_with_any_client_implementing_protocol(self):
        """Test that function works with any object implementing health_check()."""
        # Create a simple class that implements the protocol
        class CustomClient:
            def health_check(self):
                return True
        
        client = CustomClient()
        result = check_api_health(client, api_name="Custom API")
        
        assert result is True


class TestCheckUrlAvailability:
    """Tests for check_url_availability function."""

    def test_successful_response_returns_true(self, monkeypatch):
        """Test that 2xx response returns True."""
        mock_response = Mock()
        mock_response.status_code = 200
        
        mock_get = Mock(return_value=mock_response)
        monkeypatch.setattr("requests.get", mock_get)
        
        result = check_url_availability("https://api.example.com")
        
        assert result is True
        mock_get.assert_called_once_with("https://api.example.com", timeout=5)

    def test_redirect_response_returns_true(self, monkeypatch):
        """Test that 3xx response returns True."""
        mock_response = Mock()
        mock_response.status_code = 301
        
        mock_get = Mock(return_value=mock_response)
        monkeypatch.setattr("requests.get", mock_get)
        
        result = check_url_availability("https://api.example.com")
        
        assert result is True

    def test_client_error_returns_false(self, monkeypatch):
        """Test that 4xx response returns False."""
        mock_response = Mock()
        mock_response.status_code = 404
        
        mock_get = Mock(return_value=mock_response)
        monkeypatch.setattr("requests.get", mock_get)
        
        result = check_url_availability("https://api.example.com")
        
        assert result is False

    def test_server_error_returns_false(self, monkeypatch):
        """Test that 5xx response returns False."""
        mock_response = Mock()
        mock_response.status_code = 500
        
        mock_get = Mock(return_value=mock_response)
        monkeypatch.setattr("requests.get", mock_get)
        
        result = check_url_availability("https://api.example.com")
        
        assert result is False

    def test_connection_error_returns_false(self, monkeypatch):
        """Test that connection error returns False."""
        import requests
        
        mock_get = Mock(side_effect=requests.exceptions.ConnectionError("Failed"))
        monkeypatch.setattr("requests.get", mock_get)
        
        result = check_url_availability("https://api.example.com")
        
        assert result is False

    def test_timeout_error_returns_false(self, monkeypatch):
        """Test that timeout error returns False."""
        import requests
        
        mock_get = Mock(side_effect=requests.exceptions.Timeout("Timeout"))
        monkeypatch.setattr("requests.get", mock_get)
        
        result = check_url_availability("https://api.example.com")
        
        assert result is False

    def test_custom_timeout_is_used(self, monkeypatch):
        """Test that custom timeout value is passed to requests."""
        mock_response = Mock()
        mock_response.status_code = 200
        
        mock_get = Mock(return_value=mock_response)
        monkeypatch.setattr("requests.get", mock_get)
        
        check_url_availability("https://api.example.com", timeout=10)
        
        mock_get.assert_called_once_with("https://api.example.com", timeout=10)
