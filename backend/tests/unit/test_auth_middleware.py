"""
Theme Park Downtime Tracker - API Key Authentication Middleware Tests

Tests APIKeyAuth middleware:
- Initialization with API keys from config
- require_api_key decorator - successful auth
- Missing X-API-Key header (401)
- Invalid API key (401)
- No API keys configured (dev mode - authentication skipped)
- Multiple valid API keys
- API key logging (prefix only for security)

Priority: P2 - Infrastructure testing for coverage increase
"""

import pytest
from flask import Flask, jsonify
from unittest.mock import patch, MagicMock
from api.middleware.auth import APIKeyAuth, api_key_auth


class TestAPIKeyAuthInit:
    """Test APIKeyAuth initialization."""

    def test_init_with_api_keys(self):
        """APIKeyAuth should parse comma-separated API keys from config."""
        mock_config = MagicMock()
        mock_config.get.return_value = "key1,key2,key3"

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        assert len(auth.valid_api_keys) == 3
        assert "key1" in auth.valid_api_keys
        assert "key2" in auth.valid_api_keys
        assert "key3" in auth.valid_api_keys

    def test_init_strips_whitespace(self):
        """APIKeyAuth should strip whitespace from API keys."""
        mock_config = MagicMock()
        mock_config.get.return_value = " key1 , key2  ,  key3 "

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        assert len(auth.valid_api_keys) == 3
        assert "key1" in auth.valid_api_keys
        assert "key2" in auth.valid_api_keys
        assert "key3" in auth.valid_api_keys

    def test_init_ignores_empty_keys(self):
        """APIKeyAuth should ignore empty keys."""
        mock_config = MagicMock()
        mock_config.get.return_value = "key1,,key2,  ,key3"

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        # Should only have 3 valid keys (ignoring empty strings)
        assert len(auth.valid_api_keys) == 3
        assert "key1" in auth.valid_api_keys
        assert "key2" in auth.valid_api_keys
        assert "key3" in auth.valid_api_keys

    def test_init_no_api_keys(self):
        """APIKeyAuth should handle no API keys configured."""
        mock_config = MagicMock()
        mock_config.get.return_value = ""

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        assert len(auth.valid_api_keys) == 0

    def test_init_single_api_key(self):
        """APIKeyAuth should handle single API key."""
        mock_config = MagicMock()
        mock_config.get.return_value = "single-key-12345"

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        assert len(auth.valid_api_keys) == 1
        assert "single-key-12345" in auth.valid_api_keys


class TestRequireAPIKeyDecorator:
    """Test require_api_key decorator with Flask."""

    def test_require_api_key_valid_key(self):
        """require_api_key should allow request with valid API key."""
        app = Flask(__name__)

        # Create auth instance with test API key
        mock_config = MagicMock()
        mock_config.get.return_value = "test-api-key-123"

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        @app.route('/test')
        @auth.require_api_key
        def test_endpoint():
            return jsonify({"success": True})

        client = app.test_client()
        response = client.get('/test', headers={'X-API-Key': 'test-api-key-123'})

        assert response.status_code == 200
        assert response.json['success'] is True

    def test_require_api_key_missing_header(self):
        """require_api_key should return 401 when X-API-Key header is missing."""
        app = Flask(__name__)

        mock_config = MagicMock()
        mock_config.get.return_value = "test-api-key-123"

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        @app.route('/test')
        @auth.require_api_key
        def test_endpoint():
            return jsonify({"success": True})

        client = app.test_client()
        response = client.get('/test')  # No X-API-Key header

        assert response.status_code == 401
        assert response.json['error'] == "Unauthorized"
        assert "Missing X-API-Key header" in response.json['message']

    def test_require_api_key_invalid_key(self):
        """require_api_key should return 401 for invalid API key."""
        app = Flask(__name__)

        mock_config = MagicMock()
        mock_config.get.return_value = "valid-key-123"

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        @app.route('/test')
        @auth.require_api_key
        def test_endpoint():
            return jsonify({"success": True})

        client = app.test_client()
        response = client.get('/test', headers={'X-API-Key': 'invalid-key-456'})

        assert response.status_code == 401
        assert response.json['error'] == "Unauthorized"
        assert "Invalid API key" in response.json['message']

    def test_require_api_key_no_keys_configured(self):
        """require_api_key should skip auth when no keys configured (dev mode)."""
        app = Flask(__name__)

        mock_config = MagicMock()
        mock_config.get.return_value = ""  # No API keys

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        @app.route('/test')
        @auth.require_api_key
        def test_endpoint():
            return jsonify({"success": True})

        client = app.test_client()
        response = client.get('/test')  # No X-API-Key header, but should still work

        assert response.status_code == 200
        assert response.json['success'] is True

    def test_require_api_key_multiple_valid_keys(self):
        """require_api_key should accept any valid API key from the list."""
        app = Flask(__name__)

        mock_config = MagicMock()
        mock_config.get.return_value = "key1,key2,key3"

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        @app.route('/test')
        @auth.require_api_key
        def test_endpoint():
            return jsonify({"success": True})

        client = app.test_client()

        # Test each valid key
        response1 = client.get('/test', headers={'X-API-Key': 'key1'})
        response2 = client.get('/test', headers={'X-API-Key': 'key2'})
        response3 = client.get('/test', headers={'X-API-Key': 'key3'})

        assert response1.status_code == 200
        assert response2.status_code == 200
        assert response3.status_code == 200

    def test_require_api_key_empty_header(self):
        """require_api_key should return 401 for empty X-API-Key header."""
        app = Flask(__name__)

        mock_config = MagicMock()
        mock_config.get.return_value = "test-api-key-123"

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        @app.route('/test')
        @auth.require_api_key
        def test_endpoint():
            return jsonify({"success": True})

        client = app.test_client()
        response = client.get('/test', headers={'X-API-Key': ''})  # Empty string

        assert response.status_code == 401
        assert response.json['error'] == "Unauthorized"

    def test_require_api_key_case_sensitive(self):
        """require_api_key should be case-sensitive for API keys."""
        app = Flask(__name__)

        mock_config = MagicMock()
        mock_config.get.return_value = "TestKey123"

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        @app.route('/test')
        @auth.require_api_key
        def test_endpoint():
            return jsonify({"success": True})

        client = app.test_client()

        # Valid key (exact match)
        response_valid = client.get('/test', headers={'X-API-Key': 'TestKey123'})
        assert response_valid.status_code == 200

        # Invalid (lowercase)
        response_invalid = client.get('/test', headers={'X-API-Key': 'testkey123'})
        assert response_invalid.status_code == 401

    def test_require_api_key_preserves_function_name(self):
        """require_api_key decorator should preserve function name (functools.wraps)."""
        mock_config = MagicMock()
        mock_config.get.return_value = "test-key"

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        @auth.require_api_key
        def test_function():
            """Test docstring."""
            return "result"

        # functools.wraps should preserve function metadata
        assert test_function.__name__ == 'test_function'
        assert test_function.__doc__ == "Test docstring."


class TestGlobalInstance:
    """Test global api_key_auth instance."""

    def test_global_instance_exists(self):
        """Global api_key_auth instance should be initialized."""
        assert api_key_auth is not None
        assert isinstance(api_key_auth, APIKeyAuth)

    def test_global_instance_has_require_api_key(self):
        """Global api_key_auth should have require_api_key decorator."""
        assert hasattr(api_key_auth, 'require_api_key')
        assert callable(api_key_auth.require_api_key)


class TestEdgeCases:
    """Test edge cases for API key authentication."""

    def test_api_key_with_special_characters(self):
        """APIKeyAuth should handle API keys with special characters."""
        mock_config = MagicMock()
        mock_config.get.return_value = "key-with-dashes_underscores.dots"

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        assert "key-with-dashes_underscores.dots" in auth.valid_api_keys

    def test_very_long_api_key(self):
        """APIKeyAuth should handle very long API keys."""
        long_key = "a" * 256  # 256 character key
        mock_config = MagicMock()
        mock_config.get.return_value = long_key

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        assert long_key in auth.valid_api_keys

    def test_require_api_key_post_request(self):
        """require_api_key should work with POST requests."""
        app = Flask(__name__)

        mock_config = MagicMock()
        mock_config.get.return_value = "test-key-post"

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        @app.route('/test', methods=['POST'])
        @auth.require_api_key
        def test_endpoint():
            return jsonify({"success": True})

        client = app.test_client()
        response = client.post('/test', headers={'X-API-Key': 'test-key-post'})

        assert response.status_code == 200
        assert response.json['success'] is True

    def test_require_api_key_with_arguments(self):
        """require_api_key should work with endpoints that have arguments."""
        app = Flask(__name__)

        mock_config = MagicMock()
        mock_config.get.return_value = "test-key-args"

        with patch('api.middleware.auth.config', mock_config):
            auth = APIKeyAuth()

        @app.route('/test/<int:id>')
        @auth.require_api_key
        def test_endpoint(id):
            return jsonify({"id": id})

        client = app.test_client()
        response = client.get('/test/123', headers={'X-API-Key': 'test-key-args'})

        assert response.status_code == 200
        assert response.json['id'] == 123
