#!/usr/bin/env python3
"""
WSGI entry point for Theme Park Hall of Shame API.

This module provides the WSGI application interface for production deployment
with Gunicorn behind Apache mod_proxy.

Usage:
    gunicorn --bind 127.0.0.1:5001 wsgi:application

For development, use the Flask development server instead:
    cd src && python -m api.app
"""

import sys
import os

# Add the src directory to Python path
# This allows imports like "from api.app import create_app"
src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Load environment variables from .env file
from dotenv import load_dotenv
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
if os.path.exists(env_path):
    load_dotenv(env_path)

# Import and create the Flask application
from api.app import create_app

# Create the WSGI application
application = create_app()

# For local testing with: python wsgi.py
if __name__ == "__main__":
    application.run(host='0.0.0.0', port=5001, debug=False)
