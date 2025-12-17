#!/bin/bash
# =============================================================================
# Post-Deployment Smoke Tests
# =============================================================================
#
# Purpose: Verify deployment works with real API requests
#
# Tests:
#   1. Service status (systemd is running)
#   2. Health endpoint (/api/health returns valid response)
#   3. Critical API endpoints (parks, rides, trends return data)
#   4. Data freshness (recent snapshots exist in database)
#   5. Apache proxy (frontend can reach API)
#   6. Frontend loads correctly
#
# Usage:
#   ./smoke-tests.sh
#
# Exit Codes:
#   0 - All smoke tests passed
#   1 - One or more smoke tests failed
#
# Integration:
#   Called by deploy.sh after service restart
#   If tests fail, triggers automatic rollback
#
# =============================================================================

set -euo pipefail

# Configuration
API_URL="${API_URL:-http://127.0.0.1:5001}"
APACHE_URL="${APACHE_URL:-http://127.0.0.1}"
MAX_WAIT=30  # seconds to wait for service to start
APP_DIR="${APP_DIR:-/opt/themeparkhallofshame}"
VENV="${APP_DIR}/venv"
BACKEND="${APP_DIR}/backend"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

log_test() {
    echo -en "${BLUE}→${NC} $1... "
}

# =============================================================================
# Helper: Wait for Service
# =============================================================================
wait_for_service() {
    local elapsed=0
    while [ $elapsed -lt $MAX_WAIT ]; do
        if systemctl is-active --quiet themepark-api 2>/dev/null; then
            return 0
        fi
        sleep 1
        ((elapsed++))
    done
    return 1
}

# =============================================================================
# Helper: Test API Endpoint
# =============================================================================
test_endpoint() {
    local name="$1"
    local url="$2"
    local expected_key="$3"

    log_test "Testing ${name}"

    # Make request with timeout
    local response
    if ! response=$(curl -sf --max-time 10 "$url" 2>&1); then
        echo -e "${RED}FAILED${NC} (connection error)"
        echo "  URL: $url" >&2
        echo "  Error: $response" >&2
        return 1
    fi

    # Validate JSON and check for expected key
    if ! echo "$response" | python3 -c "
import sys
import json
try:
    data = json.load(sys.stdin)
    if '$expected_key' and '$expected_key' not in data:
        print('FAILED (missing key: $expected_key)', file=sys.stderr)
        sys.exit(1)
    print('OK')
except json.JSONDecodeError as e:
    print(f'FAILED (invalid JSON: {e})', file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f'FAILED ({e})', file=sys.stderr)
    sys.exit(1)
" 2>/dev/null; then
        echo -e "${RED}FAILED${NC}"
        return 1
    fi

    echo -e "${GREEN}OK${NC}"
    return 0
}

# =============================================================================
# Test 1: Service Status
# =============================================================================
test_service_status() {
    echo ""
    echo "=== Test 1: Service Status ==="

    log_test "Waiting for service to start"
    if wait_for_service; then
        echo -e "${GREEN}OK${NC}"
        log_info "Service is running"
        return 0
    else
        echo -e "${RED}FAILED${NC} (timeout)"
        log_error "Service failed to start within ${MAX_WAIT}s"
        systemctl status themepark-api --no-pager || true
        return 1
    fi
}

# =============================================================================
# Test 2: Health Endpoint
# =============================================================================
test_health_endpoint() {
    echo ""
    echo "=== Test 2: Health Endpoint ==="

    test_endpoint "Health endpoint" "${API_URL}/api/health" "status"
    return $?
}

# =============================================================================
# Test 3: Critical API Endpoints
# =============================================================================
test_critical_endpoints() {
    echo ""
    echo "=== Test 3: Critical API Endpoints ==="

    local failed=0

    # Parks endpoint
    test_endpoint "Parks endpoint" "${API_URL}/api/parks/downtime?period=today&filter=all-parks" "parks" || failed=1

    # Rides endpoint
    test_endpoint "Rides endpoint" "${API_URL}/api/rides/downtime?period=today&filter=all-rides" "rides" || failed=1

    # Trends endpoint
    test_endpoint "Trends endpoint" "${API_URL}/api/trends/longest-wait-times?period=today" "rides" || failed=1

    # Search endpoint
    test_endpoint "Search endpoint" "${API_URL}/api/search?q=space" "results" || failed=1

    if [ $failed -eq 0 ]; then
        log_info "All critical endpoints responded correctly"
        return 0
    else
        log_error "One or more critical endpoints failed"
        return 1
    fi
}

# =============================================================================
# Test 4: Data Freshness
# =============================================================================
test_data_freshness() {
    echo ""
    echo "=== Test 4: Data Freshness ==="

    log_test "Checking data freshness"

    cd "$BACKEND"
    "${VENV}/bin/python" - <<'EOF'
import sys
sys.path.insert(0, 'src')

from database.connection import get_db_connection
from sqlalchemy import text
from datetime import datetime, timedelta

try:
    with get_db_connection() as conn:
        # Check for recent snapshots (within last hour)
        result = conn.execute(text("""
            SELECT MAX(recorded_at) as latest
            FROM ride_status_snapshots
        """))
        latest = result.fetchone()[0]

        if not latest:
            print("FAILED (no snapshots)", file=sys.stderr)
            sys.exit(1)

        age_minutes = (datetime.now() - latest).total_seconds() / 60

        if age_minutes > 60:
            print(f"FAILED (data is {age_minutes:.0f} minutes old)", file=sys.stderr)
            sys.exit(1)

        print(f"OK (last snapshot: {age_minutes:.1f} minutes ago)")

except Exception as e:
    print(f"FAILED ({e})", file=sys.stderr)
    sys.exit(1)
EOF

    if [ $? -eq 0 ]; then
        log_info "Data freshness check passed"
        return 0
    else
        log_error "Data freshness check failed"
        return 1
    fi
}

# =============================================================================
# Test 5: Apache Proxy
# =============================================================================
test_apache_proxy() {
    echo ""
    echo "=== Test 5: Apache Proxy ==="

    # Test that Apache can proxy to API
    test_endpoint "Apache proxy to API" "${APACHE_URL}/api/health" "status"
    return $?
}

# =============================================================================
# Test 6: Frontend Loads
# =============================================================================
test_frontend() {
    echo ""
    echo "=== Test 6: Frontend ==="

    log_test "Testing frontend loads"

    # Check that frontend HTML loads and contains expected content
    local response
    if ! response=$(curl -sf --max-time 10 "${APACHE_URL}/" 2>&1); then
        echo -e "${RED}FAILED${NC} (connection error)"
        log_error "Frontend failed to load"
        return 1
    fi

    # Check for expected content (app title or similar)
    if echo "$response" | grep -qi "theme.*park\|downtime"; then
        echo -e "${GREEN}OK${NC}"
        log_info "Frontend loaded successfully"
        return 0
    else
        echo -e "${RED}FAILED${NC} (unexpected content)"
        log_error "Frontend loaded but doesn't contain expected content"
        return 1
    fi
}

# =============================================================================
# Main Execution
# =============================================================================
main() {
    echo "========================================"
    echo " Post-Deployment Smoke Tests"
    echo "========================================"
    echo "API URL: $API_URL"
    echo "Apache URL: $APACHE_URL"
    echo ""

    # Track failures
    local failed=0

    # Run all tests
    test_service_status || failed=1
    test_health_endpoint || failed=1
    test_critical_endpoints || failed=1
    test_data_freshness || failed=1
    test_apache_proxy || failed=1
    test_frontend || failed=1

    # Summary
    echo ""
    echo "========================================"
    if [ $failed -eq 0 ]; then
        log_info "All smoke tests passed!"
        log_info "Deployment is healthy"
        echo "========================================"
        return 0
    else
        log_error "Smoke tests failed!"
        echo "========================================"
        echo ""
        log_error "Deployment may be unhealthy - consider rollback"
        return 1
    fi
}

# Run main and exit with its exit code
main
exit $?
