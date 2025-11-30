"""
Theme Park Downtime Tracker - Flask API Application
Main API application with Blueprints, CORS, and middleware.
"""

from flask import Flask, jsonify
from flask_cors import CORS

from utils.config import FLASK_ENV, FLASK_DEBUG, SECRET_KEY
from utils.logger import logger
from api.routes.health import health_bp
from api.routes.parks import parks_bp
from api.routes.rides import rides_bp
from api.routes.trends import trends_bp
from api.routes.audit import audit_bp
from api.middleware.error_handler import register_error_handlers


def create_app() -> Flask:
    """
    Create and configure Flask application.

    Returns:
        Configured Flask app instance
    """
    app = Flask(__name__)

    # Configuration
    app.config['ENV'] = FLASK_ENV
    app.config['DEBUG'] = FLASK_DEBUG
    app.config['SECRET_KEY'] = SECRET_KEY
    app.config['JSON_SORT_KEYS'] = False  # Preserve JSON key order

    # CORS configuration
    CORS(app, resources={
        r"/api/*": {
            "origins": "*",  # Configure for production
            "methods": ["GET", "POST", "OPTIONS"],
            "allow_headers": ["Content-Type", "X-API-Key"]
        }
    })

    # Register blueprints
    app.register_blueprint(health_bp, url_prefix='/api')
    app.register_blueprint(parks_bp, url_prefix='/api')
    app.register_blueprint(rides_bp, url_prefix='/api')
    app.register_blueprint(trends_bp, url_prefix='/api')
    app.register_blueprint(audit_bp, url_prefix='/api')

    # Register error handlers
    register_error_handlers(app)

    # Log startup
    logger.info(f"Flask app created (env={FLASK_ENV}, debug={FLASK_DEBUG})")

    # Root endpoint
    @app.route('/')
    def index():
        """Root endpoint with API information."""
        return jsonify({
            "name": "Theme Park Downtime Tracker API",
            "version": "1.0.0",
            "status": "running",
            "endpoints": {
                "health": "/api/health",
                "parks": "/api/parks",
                "rides": "/api/rides",
                "trends": "/api/trends",
                "audit": "/api/audit"
            }
        })

    return app


# Create app instance for WSGI deployment
app = create_app()


if __name__ == '__main__':
    # Development server
    app = create_app()
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=FLASK_DEBUG
    )
