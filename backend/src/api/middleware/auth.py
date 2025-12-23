"""
Theme Park Downtime Tracker - API Key Authentication Middleware
Validates X-API-Key header for protected endpoints.
"""

from functools import wraps
from flask import request, jsonify

from src.utils.config import config
from src.utils.logger import logger


class APIKeyAuth:
    """
    API key authentication middleware.

    Validates X-API-Key header against configured API keys.
    For production: Store API keys in AWS SSM Parameter Store.
    For local: Store in .env file as comma-separated list.
    """

    def __init__(self):
        """Initialize with configured API keys."""
        # Load API keys from config
        api_keys_str = config.get('API_KEYS', '')
        self.valid_api_keys = set(
            key.strip()
            for key in api_keys_str.split(',')
            if key.strip()
        )

        if not self.valid_api_keys:
            logger.warning("No API keys configured - authentication disabled")

    def require_api_key(self, f):
        """
        Decorator to require valid API key.

        Usage:
            @app.route('/api/protected')
            @api_key_auth.require_api_key
            def protected_endpoint():
                return jsonify({"data": "secret"})
        """
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # If no API keys configured, skip authentication (development mode)
            if not self.valid_api_keys:
                logger.debug("API key authentication skipped (no keys configured)")
                return f(*args, **kwargs)

            # Get API key from header
            api_key = request.headers.get('X-API-Key')

            if not api_key:
                logger.warning("Missing X-API-Key header", extra={
                    "path": request.path,
                    "remote_addr": request.remote_addr
                })
                return jsonify({
                    "error": "Unauthorized",
                    "message": "Missing X-API-Key header"
                }), 401

            # Validate API key
            if api_key not in self.valid_api_keys:
                logger.warning("Invalid API key", extra={
                    "path": request.path,
                    "remote_addr": request.remote_addr,
                    "api_key_prefix": api_key[:8] if len(api_key) >= 8 else "***"
                })
                return jsonify({
                    "error": "Unauthorized",
                    "message": "Invalid API key"
                }), 401

            # Valid API key
            logger.debug("API key validated successfully")
            return f(*args, **kwargs)

        return decorated_function


# Global instance
api_key_auth = APIKeyAuth()
