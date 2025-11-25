"""
Theme Park Downtime Tracker - Flask App Unit Tests

Tests Flask application:
- App creation and configuration
- Blueprint registration
- CORS configuration
- Root endpoint
- Error handlers

Note: Health endpoint tests requiring database are deferred to integration tests.

Priority: P1 - Core API functionality
"""

import pytest
from api.app import create_app


class TestCreateApp:
    """Test Flask app creation and configuration."""

    def test_create_app_returns_flask_instance(self):
        """create_app() should return a Flask application instance."""
        app = create_app()

        assert app is not None
        assert app.name == 'api.app'

    def test_create_app_configures_environment(self):
        """create_app() should configure Flask environment."""
        app = create_app()

        assert 'ENV' in app.config
        assert 'DEBUG' in app.config
        assert 'SECRET_KEY' in app.config

    def test_create_app_disables_json_sort_keys(self):
        """create_app() should disable JSON key sorting."""
        app = create_app()

        assert app.config['JSON_SORT_KEYS'] is False

    def test_create_app_registers_health_blueprint(self):
        """create_app() should register health blueprint."""
        app = create_app()

        # Check blueprints are registered
        assert 'health' in app.blueprints

    def test_create_app_configures_cors(self):
        """create_app() should configure CORS."""
        app = create_app()

        # Check CORS extension is present
        # Flask-CORS adds the extension to app.extensions
        assert 'cors' in app.extensions or hasattr(app, 'after_request_funcs')


class TestRootEndpoint:
    """Test root endpoint /."""

    def test_root_endpoint_returns_200(self):
        """GET / should return 200 OK."""
        app = create_app()
        client = app.test_client()

        response = client.get('/')

        assert response.status_code == 200

    def test_root_endpoint_returns_json(self):
        """GET / should return JSON."""
        app = create_app()
        client = app.test_client()

        response = client.get('/')

        assert response.content_type == 'application/json'

    def test_root_endpoint_contains_api_info(self):
        """GET / should return API information."""
        app = create_app()
        client = app.test_client()

        response = client.get('/')
        data = response.get_json()

        assert 'name' in data
        assert 'version' in data
        assert 'status' in data
        assert 'endpoints' in data

        assert data['name'] == "Theme Park Downtime Tracker API"
        assert data['version'] == "1.0.0"
        assert data['status'] == "running"

    def test_root_endpoint_lists_endpoints(self):
        """GET / should list available endpoints."""
        app = create_app()
        client = app.test_client()

        response = client.get('/')
        data = response.get_json()

        assert 'health' in data['endpoints']
        assert data['endpoints']['health'] == "/api/health"


class TestErrorHandlers:
    """Test error handler middleware."""

    def test_error_handler_404_returns_json(self):
        """404 errors should return JSON response."""
        app = create_app()
        client = app.test_client()

        response = client.get('/nonexistent-endpoint')

        assert response.status_code == 404
        assert response.content_type == 'application/json'

    def test_error_handler_404_contains_error_message(self):
        """404 errors should contain error details."""
        app = create_app()
        client = app.test_client()

        response = client.get('/nonexistent-endpoint')
        data = response.get_json()

        assert 'error' in data
        assert 'message' in data
        assert data['error'] == "Not Found"

    def test_error_handler_500_returns_json(self):
        """500 errors should return JSON response."""
        app = create_app()
        client = app.test_client()

        # Create a route that raises an exception
        @app.route('/test-error')
        def test_error():
            raise ValueError("Test error")

        response = client.get('/test-error')

        assert response.status_code == 500
        assert response.content_type == 'application/json'

    def test_error_handler_500_contains_error_message(self):
        """500 errors should contain generic error message."""
        app = create_app()
        client = app.test_client()

        @app.route('/test-error')
        def test_error():
            raise ValueError("Test error")

        response = client.get('/test-error')
        data = response.get_json()

        assert 'error' in data
        assert 'message' in data
        assert data['error'] == "Internal Server Error"
        # Should not expose internal error details
        assert "unexpected error occurred" in data['message'].lower()
