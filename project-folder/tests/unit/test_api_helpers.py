"""
Unit tests for generic API helper utilities.

Tests the reusable helper functions in src/utils/api_helpers.py
that can be used with any API client across different pipelines.
"""

import pytest
from unittest.mock import Mock, MagicMock
from src.utils.api_helpers import check_api_health


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
