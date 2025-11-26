#!/bin/bash
# Theme Park Hall of Shame - Health Check Script
# Purpose: Verify deployment is working correctly
# Usage: ./health-check.sh

set -euo pipefail

APP_DIR="/opt/themeparkhallofshame"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASSED=0
FAILED=0
TOTAL=0

check() {
    local name=$1
    shift
    ((TOTAL++))
    echo -n "  $name: "
    if "$@" &>/dev/null; then
        echo -e "${GREEN}OK${NC}"
        ((PASSED++))
    else
        echo -e "${RED}FAILED${NC}"
        ((FAILED++))
    fi
}

check_with_output() {
    local name=$1
    shift
    ((TOTAL++))
    echo -n "  $name: "
    local output
    if output=$("$@" 2>&1); then
        echo -e "${GREEN}$output${NC}"
        ((PASSED++))
    else
        echo -e "${RED}FAILED${NC}"
        ((FAILED++))
    fi
}

echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}Theme Park Hall of Shame - Health Check${NC}"
echo -e "${GREEN}======================================${NC}"
echo ""

# System Services
echo "System Services:"
check "Apache running" systemctl is-active httpd
check "themepark-api running" systemctl is-active themepark-api

echo ""

# Application Endpoints
echo "Application Endpoints:"

# Check API health
check "API /api/health" bash -c "curl -sf http://127.0.0.1:5001/api/health | grep -q 'status'"

# Check API parks endpoint
check "API /api/parks/downtime" bash -c "curl -sf 'http://127.0.0.1:5001/api/parks/downtime?period=today&filter=all-parks' | grep -q 'parks'"

# Check frontend via Apache
check "Frontend (Apache)" bash -c "curl -sf http://127.0.0.1/ | grep -q 'Theme Park'"

# Check API via Apache proxy
check "API via Apache proxy" bash -c "curl -sf http://127.0.0.1/api/health | grep -q 'status'"

echo ""

# Configuration
echo "Configuration:"
check "Production .env exists" test -f "${APP_DIR}/backend/.env"
check "Virtual environment exists" test -d "${APP_DIR}/venv"
check "Backend code exists" test -f "${APP_DIR}/backend/src/api/app.py"
check "Frontend exists" test -f "/var/www/themeparkhallofshame/index.html"

echo ""

# Cron Jobs
echo "Cron Jobs:"
check "collect_snapshots cron" bash -c "crontab -l 2>/dev/null | grep -q collect_snapshots"
check "aggregate_daily cron" bash -c "crontab -l 2>/dev/null | grep -q aggregate_daily"

echo ""

# Database (via API)
echo "Database:"
check_with_output "Database connection" bash -c "curl -sf http://127.0.0.1:5001/api/health | python3 -c \"import sys,json; d=json.load(sys.stdin); print('connected' if d.get('database') else 'error')\""

echo ""

# Logs
echo "Log Files:"
check "Access log writable" test -w "${APP_DIR}/logs" -o -d "${APP_DIR}/logs"
check "Apache error log" test -f /var/log/httpd/error_log -o -f "${APP_DIR}/logs/apache-error.log"

echo ""
echo "========================================"
echo -e "Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC} (of $TOTAL checks)"
echo "========================================"

if [ $FAILED -gt 0 ]; then
    echo ""
    echo -e "${YELLOW}Troubleshooting:${NC}"
    echo "  - Check service logs: sudo journalctl -u themepark-api -n 50"
    echo "  - Check Apache logs: sudo tail -50 /var/log/httpd/error_log"
    echo "  - Check app logs: tail -50 ${APP_DIR}/logs/error.log"
    exit 1
fi

exit 0
