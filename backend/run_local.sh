#!/bin/bash
# Local development server startup script
# Usage: ./run_local.sh

# Kill any existing servers on port 5001
lsof -ti:5001 | xargs kill -9 2>/dev/null

# Set environment for local development
export DB_NAME=themepark_tracker_dev
export PYTHONPATH=src

echo "Starting local Flask server..."
echo "  Database: $DB_NAME"
echo "  Port: 5001"
echo ""

python3 -m flask run --port 5001
