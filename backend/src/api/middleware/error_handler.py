"""
Theme Park Downtime Tracker - Error Handler Middleware
Standardized error responses for all API endpoints.
"""

from flask import jsonify, Flask
from werkzeug.exceptions import HTTPException

from ...utils.logger import logger


def register_error_handlers(app: Flask):
    """
    Register error handlers for Flask app.

    Args:
        app: Flask application instance
    """

    @app.errorhandler(400)
    def bad_request(error):
        """Handle 400 Bad Request errors."""
        logger.warning(f"Bad request: {error}")
        return jsonify({
            "error": "Bad Request",
            "message": str(error.description) if hasattr(error, 'description') else "Invalid request"
        }), 400

    @app.errorhandler(401)
    def unauthorized(error):
        """Handle 401 Unauthorized errors."""
        logger.warning(f"Unauthorized access: {error}")
        return jsonify({
            "error": "Unauthorized",
            "message": "Invalid or missing authentication credentials"
        }), 401

    @app.errorhandler(404)
    def not_found(error):
        """Handle 404 Not Found errors."""
        logger.info(f"Not found: {error}")
        return jsonify({
            "error": "Not Found",
            "message": "The requested resource was not found"
        }), 404

    @app.errorhandler(429)
    def too_many_requests(error):
        """Handle 429 Too Many Requests errors."""
        logger.warning(f"Rate limit exceeded: {error}")
        return jsonify({
            "error": "Too Many Requests",
            "message": "Rate limit exceeded. Please try again later."
        }), 429

    @app.errorhandler(500)
    def internal_server_error(error):
        """Handle 500 Internal Server Error."""
        logger.error(f"Internal server error: {error}", exc_info=True)
        return jsonify({
            "error": "Internal Server Error",
            "message": "An unexpected error occurred. Please try again later."
        }), 500

    @app.errorhandler(Exception)
    def handle_unexpected_error(error):
        """Handle all unhandled exceptions."""
        # If it's an HTTP exception, pass through to specific handler
        if isinstance(error, HTTPException):
            return error

        # Log unexpected error
        logger.error(f"Unexpected error: {error}", exc_info=True)

        return jsonify({
            "error": "Internal Server Error",
            "message": "An unexpected error occurred. Please try again later."
        }), 500

    logger.info("Error handlers registered")
