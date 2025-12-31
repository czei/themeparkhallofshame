#!/bin/bash
# =============================================================================
# Pre-Service-Start Validation Script
# =============================================================================
#
# Purpose: Validate deployment on PRODUCTION SERVER before starting gunicorn
#
# This script runs as systemd ExecStartPre, validating that:
#   1. Required environment variables are set
#   2. Python can import the Flask application
#   3. Database schema has expected tables
#   4. Dependencies are compatible (no conflicts)
#
# Usage:
#   Called automatically by systemd before service start
#   Can also be run manually: ./pre-service-validate.sh
#
# Exit Codes:
#   0 - All validations passed (service can start)
#   1 - One or more validations failed (service will not start)
#
# Integration:
#   Add to systemd service file:
#   ExecStartPre=/opt/themeparkhallofshame/deployment/scripts/pre-service-validate.sh
#
# =============================================================================

set -euo pipefail

# Configuration
APP_DIR="${APP_DIR:-/opt/themeparkhallofshame}"
VENV="${APP_DIR}/venv"
BACKEND="${APP_DIR}/backend"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}✓${NC} $1"
}

log_error() {
    echo -e "${RED}✗${NC} $1" >&2
}

log_warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

# =============================================================================
# Validation 1: Environment Variables Check
# =============================================================================
validate_environment() {
    echo "=== Validation 1: Environment Variables ==="

    local required_vars=(
        "DB_HOST" "DB_PORT" "DB_NAME" "DB_USER" "DB_PASSWORD"
        "FLASK_ENV" "SECRET_KEY" "ENVIRONMENT"
    )

    local missing=()

    for var in "${required_vars[@]}"; do
        if [ -z "${!var:-}" ]; then
            missing+=("$var")
        fi
    done

    if [ ${#missing[@]} -gt 0 ]; then
        log_error "Missing required environment variables:"
        for var in "${missing[@]}"; do
            echo "  - $var" >&2
        done
        echo "" >&2
        echo "Check that .env file is loaded correctly" >&2
        return 1
    fi

    log_info "All ${#required_vars[@]} required environment variables present"
    return 0
}

# =============================================================================
# Validation 2: Python Import Test
# =============================================================================
validate_python_imports() {
    echo ""
    echo "=== Validation 2: Python Import Test ==="

    if [ ! -d "$BACKEND" ]; then
        log_error "Backend directory not found: $BACKEND"
        return 1
    fi

    if [ ! -f "${VENV}/bin/python" ]; then
        log_error "Python venv not found: $VENV"
        return 1
    fi

    cd "$BACKEND"
    "${VENV}/bin/python" -c "
import sys
import os

sys.path.insert(0, 'src')

# Environment should already be set, but verify
required_env = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'SECRET_KEY']
missing = [var for var in required_env if not os.getenv(var)]
if missing:
    print(f'ERROR: Missing environment variables: {missing}', file=sys.stderr)
    sys.exit(1)

try:
    # Test critical imports
    from api.app import create_app

    # Try to create the Flask app
    app = create_app()

    if app is None:
        raise Exception('create_app() returned None')

    if not hasattr(app, 'route'):
        raise Exception('application is not a Flask app')

    print(f'✓ Flask application created successfully')
    print(f'  App name: {app.name}')
    print(f'  Environment: {os.getenv(\"ENVIRONMENT\")}')

except ImportError as e:
    print(f'ERROR: Import failed: {e}', file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f'ERROR: Failed to create Flask app: {e}', file=sys.stderr)
    sys.exit(1)
"

    if [ $? -eq 0 ]; then
        log_info "Python import test passed"
        return 0
    else
        log_error "Python import test failed"
        return 1
    fi
}

# =============================================================================
# Validation 3: Database Schema Validation
# =============================================================================
validate_database_schema() {
    echo ""
    echo "=== Validation 3: Database Schema ==="

    cd "$BACKEND"
    "${VENV}/bin/python" -c "
import sys
sys.path.insert(0, 'src')

from database.connection import get_db_connection
from sqlalchemy import text

# Required tables for application to function
# Note: ride_hourly_stats was dropped in migration 003_drop_hourly_stats
# Ride hourly queries now use on-the-fly aggregation from ride_status_snapshots
required_tables = [
    'parks',
    'rides',
    'ride_status_snapshots',
    'park_activity_snapshots',
    'park_daily_stats',
    'ride_daily_stats',
    'park_hourly_stats',
    'park_live_rankings',
    'ride_live_rankings',
]

try:
    with get_db_connection() as conn:
        # Get list of existing tables
        result = conn.execute(text('SHOW TABLES'))
        existing_tables = {row[0] for row in result}

        # Check for missing tables
        missing = [t for t in required_tables if t not in existing_tables]

        if missing:
            print(f'ERROR: Missing required database tables:', file=sys.stderr)
            for table in missing:
                print(f'  - {table}', file=sys.stderr)
            print('', file=sys.stderr)
            print('Run database migrations to create missing tables', file=sys.stderr)
            sys.exit(1)

        print(f'✓ All {len(required_tables)} required tables exist')

        # Quick sanity check: verify tables have data
        result = conn.execute(text('SELECT COUNT(*) FROM parks WHERE is_active = TRUE'))
        active_parks = result.scalar()
        print(f'  Active parks: {active_parks}')

        result = conn.execute(text('SELECT COUNT(*) FROM rides WHERE is_active = TRUE'))
        active_rides = result.scalar()
        print(f'  Active rides: {active_rides}')

except Exception as e:
    print(f'ERROR: Database validation failed: {e}', file=sys.stderr)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)
"

    if [ $? -eq 0 ]; then
        log_info "Database schema validation passed"
        return 0
    else
        log_error "Database schema validation failed"
        return 1
    fi
}

# =============================================================================
# Validation 4: Dependency Compatibility Check
# =============================================================================
validate_dependencies() {
    echo ""
    echo "=== Validation 4: Dependency Compatibility ==="

    # Check for dependency conflicts
    "${VENV}/bin/pip" check 2>&1 | tee /tmp/pip-check-output.txt
    local result=${PIPESTATUS[0]}

    if [ $result -eq 0 ]; then
        log_info "No dependency conflicts detected"
        return 0
    else
        log_error "Dependency conflicts detected:"
        cat /tmp/pip-check-output.txt >&2
        return 1
    fi
}

# =============================================================================
# Main Execution
# =============================================================================
main() {
    echo "========================================"
    echo " Pre-Service Validation"
    echo "========================================"
    echo "Application directory: $APP_DIR"
    echo "Environment: ${ENVIRONMENT:-unknown}"
    echo ""

    # Track failures
    local failed=0

    # Run all validations
    validate_environment || failed=1
    validate_python_imports || failed=1
    validate_database_schema || failed=1
    validate_dependencies || failed=1

    # Summary
    echo ""
    echo "========================================"
    if [ $failed -eq 0 ]; then
        log_info "All pre-service validations passed"
        log_info "Service is ready to start"
        echo "========================================"
        return 0
    else
        log_error "Pre-service validation failed"
        echo "========================================"
        echo ""
        log_error "Service will NOT start until these issues are resolved"
        return 1
    fi
}

# Run main and exit with its exit code
main
exit $?
