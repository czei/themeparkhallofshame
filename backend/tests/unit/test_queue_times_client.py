"""
Theme Park Downtime Tracker - Queue-Times Client Unit Tests

Tests QueueTimesClient with HTTP mocking:
- Client initialization and configuration
- get_parks() - successful fetch, error handling
- get_park_wait_times() - successful fetch, error handling
- Retry logic for transient failures (timeout, connection error)
- HTTP error handling (404, 500)
- Session management and cleanup
- Singleton pattern for get_queue_times_client()

Priority: P2 - Infrastructure testing for coverage increase
"""

import pytest
import requests
from unittest.mock import Mock, patch
from tenacity import RetryError
from collector.queue_times_client import QueueTimesClient, get_queue_times_client


class TestQueueTimesClientInit:
    """Test QueueTimesClient initialization."""

    def test_init_with_default_base_url(self):
        """QueueTimesClient should initialize with default base URL."""
        client = QueueTimesClient()

        # Base URL should be set (exact value from config)
        assert client.base_url is not None
        assert isinstance(client.base_url, str)
        # Should not have trailing slash
        assert not client.base_url.endswith('/')

    def test_init_with_custom_base_url(self):
        """QueueTimesClient should accept custom base URL."""
        custom_url = "https://custom-api.example.com/v2"
        client = QueueTimesClient(base_url=custom_url)

        assert client.base_url == custom_url

    def test_init_strips_trailing_slash(self):
        """QueueTimesClient should strip trailing slash from base URL."""
        client = QueueTimesClient(base_url="https://api.example.com/")

        assert client.base_url == "https://api.example.com"

    def test_init_creates_session(self):
        """QueueTimesClient should create requests Session."""
        client = QueueTimesClient()

        assert client.session is not None
        assert isinstance(client.session, requests.Session)

    def test_init_sets_user_agent(self):
        """QueueTimesClient should set User-Agent header."""
        client = QueueTimesClient()

        assert 'User-Agent' in client.session.headers
        assert 'ThemeParkHallOfShame' in client.session.headers['User-Agent']

    def test_init_sets_accept_json(self):
        """QueueTimesClient should set Accept: application/json header."""
        client = QueueTimesClient()

        assert 'Accept' in client.session.headers
        assert client.session.headers['Accept'] == 'application/json'


class TestGetParks:
    """Test get_parks() method."""

    def test_get_parks_success(self):
        """get_parks() should fetch parks list successfully."""
        client = QueueTimesClient(base_url="https://api.example.com")

        # Mock the session.get method
        mock_response = Mock()
        mock_response.json.return_value = [
            {"id": 101, "name": "Magic Kingdom"},
            {"id": 102, "name": "Epcot"}
        ]
        mock_response.raise_for_status = Mock()

        with patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            parks = client.get_parks()

        # Verify request
        mock_get.assert_called_once_with(
            "https://api.example.com/parks.json",
            timeout=10
        )

        # Verify response
        assert len(parks) == 2
        assert parks[0]['name'] == "Magic Kingdom"
        assert parks[1]['name'] == "Epcot"

    def test_get_parks_http_error_404(self):
        """get_parks() should raise HTTPError for 404."""
        client = QueueTimesClient(base_url="https://api.example.com")

        # Mock 404 response
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")

        with patch.object(client.session, 'get', return_value=mock_response):
            with pytest.raises(requests.HTTPError):
                client.get_parks()

    def test_get_parks_http_error_500(self):
        """get_parks() should raise HTTPError for 500."""
        client = QueueTimesClient(base_url="https://api.example.com")

        # Mock 500 response
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")

        with patch.object(client.session, 'get', return_value=mock_response):
            with pytest.raises(requests.HTTPError):
                client.get_parks()

    def test_get_parks_timeout_no_retry(self):
        """get_parks() should raise RetryError after max timeout retries."""
        client = QueueTimesClient(base_url="https://api.example.com")

        # Mock timeout - tenacity will retry, then raise RetryError
        with patch.object(client.session, 'get', side_effect=requests.Timeout("Request timeout")):
            with pytest.raises(RetryError):
                # This will attempt retries, then raise RetryError
                client.get_parks()

    def test_get_parks_connection_error_no_retry(self):
        """get_parks() should raise RetryError after max connection retries."""
        client = QueueTimesClient(base_url="https://api.example.com")

        # Mock connection error - tenacity will retry, then raise RetryError
        with patch.object(client.session, 'get', side_effect=requests.ConnectionError("Connection refused")):
            with pytest.raises(RetryError):
                client.get_parks()

    def test_get_parks_empty_list(self):
        """get_parks() should handle empty parks list."""
        client = QueueTimesClient(base_url="https://api.example.com")

        mock_response = Mock()
        mock_response.json.return_value = []  # Empty list
        mock_response.raise_for_status = Mock()

        with patch.object(client.session, 'get', return_value=mock_response):
            parks = client.get_parks()

        assert parks == []


class TestGetParkWaitTimes:
    """Test get_park_wait_times() method."""

    def test_get_park_wait_times_success(self):
        """get_park_wait_times() should fetch wait times successfully."""
        client = QueueTimesClient(base_url="https://api.example.com")

        mock_response = Mock()
        mock_response.json.return_value = {
            "id": 101,
            "name": "Magic Kingdom",
            "lands": [
                {
                    "id": 1,
                    "name": "Tomorrowland",
                    "rides": [
                        {"id": 1001, "name": "Space Mountain", "wait_time": 45, "is_open": True}
                    ]
                }
            ]
        }
        mock_response.raise_for_status = Mock()

        with patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            data = client.get_park_wait_times(park_id=101)

        # Verify request
        mock_get.assert_called_once_with(
            "https://api.example.com/parks/101/queue_times.json",
            timeout=10
        )

        # Verify response
        assert data['id'] == 101
        assert data['name'] == "Magic Kingdom"
        assert len(data['lands']) == 1

    def test_get_park_wait_times_http_error_404(self):
        """get_park_wait_times() should raise HTTPError for invalid park ID."""
        client = QueueTimesClient(base_url="https://api.example.com")

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Park Not Found")

        with patch.object(client.session, 'get', return_value=mock_response):
            with pytest.raises(requests.HTTPError):
                client.get_park_wait_times(park_id=99999)

    def test_get_park_wait_times_timeout_no_retry(self):
        """get_park_wait_times() should raise RetryError after max timeout retries."""
        client = QueueTimesClient(base_url="https://api.example.com")

        with patch.object(client.session, 'get', side_effect=requests.Timeout("Request timeout")):
            with pytest.raises(RetryError):
                client.get_park_wait_times(park_id=101)

    def test_get_park_wait_times_connection_error_no_retry(self):
        """get_park_wait_times() should raise RetryError after max connection retries."""
        client = QueueTimesClient(base_url="https://api.example.com")

        with patch.object(client.session, 'get', side_effect=requests.ConnectionError("Connection refused")):
            with pytest.raises(RetryError):
                client.get_park_wait_times(park_id=101)

    def test_get_park_wait_times_multiple_park_ids(self):
        """get_park_wait_times() should handle different park IDs."""
        client = QueueTimesClient(base_url="https://api.example.com")

        mock_response = Mock()
        mock_response.json.return_value = {"id": 102, "name": "Epcot"}
        mock_response.raise_for_status = Mock()

        with patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            # Test with different park ID
            data = client.get_park_wait_times(park_id=102)

        # Verify correct URL was called
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "parks/102/queue_times.json" in call_args[0][0]


class TestSessionManagement:
    """Test session management and cleanup."""

    def test_close_session(self):
        """close() should close the HTTP session."""
        client = QueueTimesClient()

        # Mock the session.close method
        with patch.object(client.session, 'close') as mock_close:
            client.close()

        mock_close.assert_called_once()

    def test_session_reuse(self):
        """QueueTimesClient should reuse the same session for multiple requests."""
        client = QueueTimesClient(base_url="https://api.example.com")

        mock_response = Mock()
        mock_response.json.return_value = []
        mock_response.raise_for_status = Mock()

        with patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            # Make multiple requests
            client.get_parks()
            client.get_park_wait_times(park_id=101)

        # Session should be reused (same session object for both calls)
        assert mock_get.call_count == 2


class TestSingletonPattern:
    """Test singleton pattern for get_queue_times_client()."""

    def test_get_queue_times_client_returns_instance(self):
        """get_queue_times_client() should return QueueTimesClient instance."""
        # Reset singleton
        import collector.queue_times_client as module
        module._client = None

        client = get_queue_times_client()

        assert client is not None
        assert isinstance(client, QueueTimesClient)

    def test_get_queue_times_client_singleton(self):
        """get_queue_times_client() should return same instance on multiple calls."""
        # Reset singleton
        import collector.queue_times_client as module
        module._client = None

        client1 = get_queue_times_client()
        client2 = get_queue_times_client()

        # Should be the same instance
        assert client1 is client2

    def test_get_queue_times_client_lazy_initialization(self):
        """get_queue_times_client() should create client on first call only."""
        # Reset singleton
        import collector.queue_times_client as module
        module._client = None

        # First call should create instance
        assert module._client is None
        client1 = get_queue_times_client()
        assert module._client is not None

        # Second call should reuse instance
        client2 = get_queue_times_client()
        assert client2 is client1


class TestEdgeCases:
    """Test edge cases for QueueTimesClient."""

    def test_base_url_without_protocol(self):
        """QueueTimesClient should handle base URL without protocol."""
        # This is an edge case - API would likely fail, but client should handle it
        client = QueueTimesClient(base_url="api.example.com")

        assert client.base_url == "api.example.com"

    def test_get_parks_malformed_json(self):
        """get_parks() should raise error for malformed JSON."""
        client = QueueTimesClient(base_url="https://api.example.com")

        mock_response = Mock()
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_response.raise_for_status = Mock()

        with patch.object(client.session, 'get', return_value=mock_response):
            with pytest.raises(ValueError):
                client.get_parks()

    def test_get_park_wait_times_zero_park_id(self):
        """get_park_wait_times() should handle park_id=0."""
        client = QueueTimesClient(base_url="https://api.example.com")

        mock_response = Mock()
        mock_response.json.return_value = {"id": 0, "name": "Test Park"}
        mock_response.raise_for_status = Mock()

        with patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            data = client.get_park_wait_times(park_id=0)

        # Should construct URL with park_id=0
        assert "parks/0/queue_times.json" in mock_get.call_args[0][0]
