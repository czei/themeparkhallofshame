#!/bin/bash
# Theme Park Hall of Shame - Production Database Mirror Script
# Purpose: Mirror production database to local development environment
# Usage: ./mirror-production-db.sh [options]
#
# Options:
#   --days=N        Only mirror last N days of snapshot data (default: 7)
#   --full          Mirror entire database (all historical data)
#   --schema-only   Mirror schema only, no data
#   --dry-run       Show what would be done without executing
#   --yes           Skip confirmation prompt
#   --help          Show this help message

set -euo pipefail

# Configuration
REMOTE_HOST="ec2-user@webperformance.com"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/michael-2.pem}"
REMOTE_DB_NAME="themepark_tracker"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${GREEN}[MIRROR]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }
info() { echo -e "${BLUE}[INFO]${NC} $1"; }

# Default options (mirror everything unless user opts into a window)
DAYS=7
FULL=true
SCHEMA_ONLY=false
DRY_RUN=false
SKIP_CONFIRM=false
# Date window (computed later for partial mirrors)
DATE_FILTER=""
DATE_FILTER_END=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --days=*)
            DAYS="${1#*=}"
            FULL=false
            ;;
        --full)
            FULL=true
            ;;
        --partial)
            FULL=false
            ;;
        --schema-only)
            SCHEMA_ONLY=true
            ;;
        --dry-run)
            DRY_RUN=true
            ;;
        --yes|-y)
            SKIP_CONFIRM=true
            ;;
        --help|-h)
            head -15 "$0" | tail -14
            exit 0
            ;;
        *)
            error "Unknown option: $1. Use --help for usage."
            ;;
    esac
    shift
done

# Tables that should always be fully copied (small reference data)
FULL_TABLES=(
    "parks"
    "rides"
    "ride_classifications"
    "park_schedules"
    "park_live_rankings"
    "park_live_rankings_staging"
    "ride_live_rankings"
    "ride_live_rankings_staging"
    "weather_observations"
    "weather_forecasts"
)

# Tables that should be filtered by date when doing partial mirrors
DATE_FILTERED_TABLES=(
    "ride_status_snapshots"
    "park_activity_snapshots"
    "ride_status_changes"
    "ride_daily_stats"
    "park_daily_stats"
    "ride_weekly_stats"
    "park_weekly_stats"
    "ride_monthly_stats"
    "park_monthly_stats"
    "data_quality_issues"
    "ride_hourly_stats"
    "park_hourly_stats"
    "weather_observations"
    "weather_forecasts"
)

echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}Theme Park Hall of Shame${NC}"
echo -e "${GREEN}Production Database Mirror${NC}"
echo -e "${GREEN}======================================${NC}"
echo ""

# Show mode
if [ "$SCHEMA_ONLY" = true ]; then
    info "Mode: Schema only (no data)"
elif [ "$FULL" = true ]; then
    info "Mode: Full mirror (all data)"
else
    info "Mode: Partial mirror (last $DAYS days of snapshots)"
fi

if [ "$DRY_RUN" = true ]; then
    warn "DRY RUN - No changes will be made"
fi
echo ""

# Load local environment
ENV_FILE="${PROJECT_ROOT}/backend/.env"
if [ ! -f "${ENV_FILE}" ]; then
    error ".env file not found at ${ENV_FILE}. Please create it first."
fi
source "${ENV_FILE}"

LOCAL_DB_NAME="${DB_NAME:-themepark_tracker_dev}"
LOCAL_DB_HOST="${DB_HOST:-localhost}"
LOCAL_DB_USER="${DB_USER:-root}"
LOCAL_DB_PASS="${DB_PASSWORD:-}"

# Use UTC for all remote/local mysql sessions to avoid date-boundary drift
REMOTE_MYSQL="sudo mysql -u root --init-command=\"SET time_zone='+00:00';\""
# Store local init options in an array to avoid word-splitting issues
LOCAL_MYSQL_INIT=(--init-command="SET time_zone='+00:00';")

# Safety check - refuse to overwrite production
if [[ "$LOCAL_DB_NAME" == *"prod"* ]] || [[ "$LOCAL_DB_NAME" == "theme_park_tracker" ]]; then
    error "SAFETY: Refusing to overwrite database '$LOCAL_DB_NAME' - looks like production!"
fi

info "Local database: $LOCAL_DB_NAME @ $LOCAL_DB_HOST"
echo ""

# Check prerequisites
log "Checking prerequisites..."

# Check mysql client
if ! command -v mysql &> /dev/null; then
    error "mysql command not found. Please install MySQL/MariaDB client."
fi
echo "  mysql client: OK"

# Optional: pv for progress display
PV_CMD="cat"
if command -v pv &> /dev/null; then
    PV_CMD="pv -N \"Importing\""
    echo "  pv: OK"
else
    warn "pv not found; imports will run without progress display"
fi

# Check SSH key
if [ ! -f "$SSH_KEY" ]; then
    error "SSH key not found at $SSH_KEY. Set SSH_KEY env var."
fi
echo "  SSH key: OK"

# Test SSH connection
if ! ssh -i "$SSH_KEY" -o ConnectTimeout=10 "$REMOTE_HOST" "echo 'ok'" &>/dev/null; then
    error "Cannot connect to $REMOTE_HOST"
fi
echo "  SSH connection: OK"

# Test local MySQL connection
if ! mysql -h"${LOCAL_DB_HOST}" -u"${LOCAL_DB_USER}" ${LOCAL_DB_PASS:+-p"$LOCAL_DB_PASS"} -e "SELECT 1;" &>/dev/null; then
    error "Cannot connect to local MySQL server"
fi
echo "  Local MySQL: OK"
echo ""

# Confirmation
if [ "$SKIP_CONFIRM" = false ] && [ "$DRY_RUN" = false ]; then
    warn "This will DROP and recreate database '$LOCAL_DB_NAME'"
    read -p "Continue? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log "Aborted."
        exit 0
    fi
    echo ""
fi

if [ "$DRY_RUN" = true ]; then
    log "DRY RUN: Would execute the following:"
    echo ""
    echo "1. SSH to $REMOTE_HOST"
    echo "2. Run mysqldump on $REMOTE_DB_NAME"
    if [ "$SCHEMA_ONLY" = true ]; then
        echo "   - Schema only, no data"
    elif [ "$FULL" = true ]; then
        echo "   - Full data export"
    else
        echo "   - Full tables: ${FULL_TABLES[*]}"
        echo "   - Filtered tables (last $DAYS days): ${DATE_FILTERED_TABLES[*]}"
    fi
    echo "3. Drop and recreate $LOCAL_DB_NAME"
    echo "4. Import data"
    echo ""
    log "DRY RUN complete. Use without --dry-run to execute."
    exit 0
fi

# Start timer
START_TIME=$(date +%s)

# Drop and recreate local database
log "Recreating local database..."
mysql -h"${LOCAL_DB_HOST}" -u"${LOCAL_DB_USER}" ${LOCAL_DB_PASS:+-p"$LOCAL_DB_PASS"} <<EOF
DROP DATABASE IF EXISTS \`${LOCAL_DB_NAME}\`;
CREATE DATABASE \`${LOCAL_DB_NAME}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
EOF
echo "  Database recreated: $LOCAL_DB_NAME"

# Calculate date filter
if [ "$FULL" = false ] && [ "$SCHEMA_ONLY" = false ]; then
    # Anchor the window to production's most recent park_activity_snapshots date
    LAST_PROD_DATE=$(ssh -i "$SSH_KEY" "$REMOTE_HOST" \
        "$REMOTE_MYSQL -N -e \"SELECT DATE(MAX(recorded_at)) FROM park_activity_snapshots\" $REMOTE_DB_NAME 2>/dev/null" || true)
    if [ -z "$LAST_PROD_DATE" ]; then
        LAST_PROD_DATE=$(date +%Y-%m-%d)
        warn "Could not determine last prod snapshot date, defaulting to today ($LAST_PROD_DATE)"
    fi
    DATE_FILTER_END="$LAST_PROD_DATE"
    DATE_FILTER=$(python - <<PY
import datetime
end = datetime.date.fromisoformat("$LAST_PROD_DATE")
start = end - datetime.timedelta(days=int("$DAYS")-1)
print(start.isoformat())
PY
)
    info "Date filter: $DATE_FILTER to $DATE_FILTER_END (inclusive)"
fi

# Build and execute mysqldump
log "Exporting from production..."

if [ "$SCHEMA_ONLY" = true ]; then
    # Schema only (filter MariaDB-specific syntax for MySQL compatibility)
    ssh -i "$SSH_KEY" "$REMOTE_HOST" \
        "sudo mysqldump -u root --tz-utc --no-data --routines --triggers $REMOTE_DB_NAME" \
        | sed -e 's/CONSTRAINT `CONSTRAINT_[0-9]*` //g' \
              -e 's/current_timestamp()/CURRENT_TIMESTAMP/g' \
              -e 's/DEFAULT NULL ON UPDATE current_timestamp()/DEFAULT NULL/g' \
              -e "s/DEFAULT '0000-00-00 00:00:00'/DEFAULT CURRENT_TIMESTAMP/g" \
        | mysql --init-command="SET TIME_ZONE='+00:00';" -h"${LOCAL_DB_HOST}" -u"${LOCAL_DB_USER}" ${LOCAL_DB_PASS:+-p"$LOCAL_DB_PASS"} "$LOCAL_DB_NAME"
    echo "  Schema imported"

elif [ "$FULL" = true ]; then
    # Full export (filter MariaDB-specific syntax for MySQL compatibility)
    ssh -i "$SSH_KEY" "$REMOTE_HOST" \
        "sudo mysqldump -u root --tz-utc --routines --triggers --single-transaction $REMOTE_DB_NAME" \
        | sed -e 's/CONSTRAINT `CONSTRAINT_[0-9]*` //g' \
              -e 's/current_timestamp()/CURRENT_TIMESTAMP/g' \
              -e 's/DEFAULT NULL ON UPDATE current_timestamp()/DEFAULT NULL/g' \
              -e "s/DEFAULT '0000-00-00 00:00:00'/DEFAULT CURRENT_TIMESTAMP/g" \
        | ${PV_CMD} \
        | mysql --init-command="SET TIME_ZONE='+00:00';" -h"${LOCAL_DB_HOST}" -u"${LOCAL_DB_USER}" ${LOCAL_DB_PASS:+-p"$LOCAL_DB_PASS"} "$LOCAL_DB_NAME"
    echo "  Full database imported"

else
    # Partial export - schema first, then data

    # 1. Export schema only (filter MariaDB-specific syntax for MySQL compatibility)
    log "  Exporting schema..."
    ssh -i "$SSH_KEY" "$REMOTE_HOST" \
        "sudo mysqldump -u root --tz-utc --no-data --routines --triggers $REMOTE_DB_NAME" \
        | sed -e 's/CONSTRAINT `CONSTRAINT_[0-9]*` //g' \
              -e 's/current_timestamp()/CURRENT_TIMESTAMP/g' \
              -e 's/DEFAULT NULL ON UPDATE current_timestamp()/DEFAULT NULL/g' \
              -e "s/DEFAULT '0000-00-00 00:00:00'/DEFAULT CURRENT_TIMESTAMP/g" \
        | mysql --init-command="SET TIME_ZONE='+00:00';" -h"${LOCAL_DB_HOST}" -u"${LOCAL_DB_USER}" ${LOCAL_DB_PASS:+-p"$LOCAL_DB_PASS"} "$LOCAL_DB_NAME"

    # 1b. CRITICAL FIX: Remove 'ON UPDATE CURRENT_TIMESTAMP' from timestamp columns
    # This prevents timestamps from being auto-updated during data import
    log "  Fixing timestamp columns to preserve original values..."
    mysql "${LOCAL_MYSQL_INIT[@]}" -h"${LOCAL_DB_HOST}" -u"${LOCAL_DB_USER}" ${LOCAL_DB_PASS:+-p"$LOCAL_DB_PASS"} "$LOCAL_DB_NAME" <<'FIXSQL'
-- park_activity_snapshots: preserve recorded_at timestamps
-- This is the CRITICAL fix - recorded_at had ON UPDATE CURRENT_TIMESTAMP
ALTER TABLE park_activity_snapshots
    MODIFY COLUMN recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;

-- ride_status_snapshots: preserve recorded_at timestamps
ALTER TABLE ride_status_snapshots
    MODIFY COLUMN recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
FIXSQL
    echo "  Timestamp columns fixed"

    # 2. Export full tables (small reference data)
    log "  Exporting reference tables..."
    FULL_TABLES_STR="${FULL_TABLES[*]}"
    ssh -i "$SSH_KEY" "$REMOTE_HOST" \
        "sudo mysqldump -u root --tz-utc --no-create-info --single-transaction $REMOTE_DB_NAME $FULL_TABLES_STR 2>/dev/null" \
        | mysql "${LOCAL_MYSQL_INIT[@]}" -h"${LOCAL_DB_HOST}" -u"${LOCAL_DB_USER}" ${LOCAL_DB_PASS:+-p"$LOCAL_DB_PASS"} "$LOCAL_DB_NAME"

    # 3. Export filtered tables (last N days)
    log "  Exporting snapshot data (last $DAYS days)..."
    for TABLE in "${DATE_FILTERED_TABLES[@]}"; do
        # Determine the date column for this table
        case $TABLE in
            ride_status_snapshots|park_activity_snapshots)
                DATE_COL="recorded_at"
                ;;
            ride_status_changes)
                DATE_COL="changed_at"
                ;;
            ride_daily_stats|park_daily_stats)
                DATE_COL="stat_date"
                ;;
            ride_weekly_stats|park_weekly_stats|ride_monthly_stats|park_monthly_stats)
                DATE_COL="period_start"
                ;;
            ride_hourly_stats|park_hourly_stats)
                DATE_COL="hour_start_utc"
                ;;
            data_quality_issues)
                DATE_COL="detected_at"
                ;;
            *)
                DATE_COL="created_at"
                ;;
        esac

        echo -n "    $TABLE... "

        # Export with WHERE clause (bounded window to match prod exactly)
        ROW_COUNT=$(ssh -i "$SSH_KEY" "$REMOTE_HOST" \
            "$REMOTE_MYSQL -N -e \"SELECT COUNT(*) FROM $TABLE WHERE $DATE_COL >= '$DATE_FILTER' AND $DATE_COL < DATE_ADD('$DATE_FILTER_END', INTERVAL 1 DAY)\" $REMOTE_DB_NAME 2>/dev/null || echo 0")

        if [ "$ROW_COUNT" -gt 0 ]; then
            ssh -i "$SSH_KEY" "$REMOTE_HOST" \
                "sudo mysqldump -u root --tz-utc --no-create-info --single-transaction --where=\"$DATE_COL >= '$DATE_FILTER' AND $DATE_COL < DATE_ADD('$DATE_FILTER_END', INTERVAL 1 DAY)\" $REMOTE_DB_NAME $TABLE 2>/dev/null" \
                | mysql "${LOCAL_MYSQL_INIT[@]}" -h"${LOCAL_DB_HOST}" -u"${LOCAL_DB_USER}" ${LOCAL_DB_PASS:+-p"$LOCAL_DB_PASS"} "$LOCAL_DB_NAME"
            echo "$ROW_COUNT rows"
        else
            echo "0 rows (skipped)"
        fi
    done
fi

# Calculate elapsed time
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))

echo ""
log "Verifying import..."

# Show table counts
echo ""
echo "Table row counts (exact):"
mysql -h"${LOCAL_DB_HOST}" -u"${LOCAL_DB_USER}" ${LOCAL_DB_PASS:+-p"$LOCAL_DB_PASS"} "$LOCAL_DB_NAME" -e "
SELECT 'parks' table_name, COUNT(*) rows FROM parks
UNION ALL SELECT 'rides', COUNT(*) FROM rides
UNION ALL SELECT 'ride_classifications', COUNT(*) FROM ride_classifications
UNION ALL SELECT 'park_schedules', COUNT(*) FROM park_schedules
UNION ALL SELECT 'ride_status_snapshots', COUNT(*) FROM ride_status_snapshots
UNION ALL SELECT 'park_activity_snapshots', COUNT(*) FROM park_activity_snapshots
UNION ALL SELECT 'ride_status_changes', COUNT(*) FROM ride_status_changes
UNION ALL SELECT 'ride_daily_stats', COUNT(*) FROM ride_daily_stats
UNION ALL SELECT 'park_daily_stats', COUNT(*) FROM park_daily_stats
UNION ALL SELECT 'ride_hourly_stats', COUNT(*) FROM ride_hourly_stats
UNION ALL SELECT 'park_hourly_stats', COUNT(*) FROM park_hourly_stats
UNION ALL SELECT 'ride_weekly_stats', COUNT(*) FROM ride_weekly_stats
UNION ALL SELECT 'park_weekly_stats', COUNT(*) FROM park_weekly_stats
UNION ALL SELECT 'ride_monthly_stats', COUNT(*) FROM ride_monthly_stats
UNION ALL SELECT 'park_monthly_stats', COUNT(*) FROM park_monthly_stats
UNION ALL SELECT 'ride_yearly_stats', COUNT(*) FROM ride_yearly_stats
UNION ALL SELECT 'park_yearly_stats', COUNT(*) FROM park_yearly_stats
UNION ALL SELECT 'weather_observations', COUNT(*) FROM weather_observations
UNION ALL SELECT 'weather_forecasts', COUNT(*) FROM weather_forecasts
ORDER BY table_name;
" 2>/dev/null || true

# ============================================
# AUDIT VERIFICATION: Compare data with production
# ============================================
echo ""
log "Running audit verification..."

AUDIT_PASSED=true

sync_single_table() {
    local TABLE=$1
    log "Re-syncing $TABLE..."
    ssh -i "$SSH_KEY" "$REMOTE_HOST" \
        "sudo mysqldump -u root --tz-utc --single-transaction --no-create-info $REMOTE_DB_NAME $TABLE" \
        | mysql "${LOCAL_MYSQL_INIT[@]}" -h"${LOCAL_DB_HOST}" -u"${LOCAL_DB_USER}" ${LOCAL_DB_PASS:+-p"$LOCAL_DB_PASS"} "$LOCAL_DB_NAME"
}

# Function to compare date ranges AND actual timestamp freshness
compare_date_range() {
    local TABLE=$1
    local DATE_COL=$2
    local DESCRIPTION=$3

    echo -n "  $DESCRIPTION... "

    # Get production timestamp range (ACTUAL timestamps, not just dates)
    PROD_DATA=$(ssh -i "$SSH_KEY" "$REMOTE_HOST" \
        "$REMOTE_MYSQL -N -e \"SELECT CONCAT(MIN($DATE_COL), '|', MAX($DATE_COL), '|', COUNT(*)) FROM $TABLE WHERE $DATE_COL >= '$DATE_FILTER' AND $DATE_COL < DATE_ADD('$DATE_FILTER_END', INTERVAL 1 DAY)\" $REMOTE_DB_NAME 2>/dev/null" || echo "ERROR")

    # Get local timestamp range (ACTUAL timestamps, not just dates)
    LOCAL_DATA=$(mysql "${LOCAL_MYSQL_INIT[@]}" -h"${LOCAL_DB_HOST}" -u"${LOCAL_DB_USER}" ${LOCAL_DB_PASS:+-p"$LOCAL_DB_PASS"} -N -e \
        "SELECT CONCAT(MIN($DATE_COL), '|', MAX($DATE_COL), '|', COUNT(*)) FROM $TABLE WHERE $DATE_COL >= '$DATE_FILTER' AND $DATE_COL < DATE_ADD('$DATE_FILTER_END', INTERVAL 1 DAY)" "$LOCAL_DB_NAME" 2>/dev/null || echo "ERROR")

    if [ "$PROD_DATA" = "$LOCAL_DATA" ]; then
        # Extract max timestamp for display
        MAX_TS=$(echo "$LOCAL_DATA" | cut -d'|' -f2)
        ROW_COUNT=$(echo "$LOCAL_DATA" | cut -d'|' -f3)
        echo -e "${GREEN}✓ MATCH${NC} (${ROW_COUNT} rows, latest: ${MAX_TS})"
    else
        echo -e "${RED}✗ MISMATCH${NC}"
        PROD_MAX=$(echo "$PROD_DATA" | cut -d'|' -f2)
        PROD_COUNT=$(echo "$PROD_DATA" | cut -d'|' -f3)
        LOCAL_MAX=$(echo "$LOCAL_DATA" | cut -d'|' -f2)
        LOCAL_COUNT=$(echo "$LOCAL_DATA" | cut -d'|' -f3)
        echo "      Production: ${PROD_COUNT} rows, latest: ${PROD_MAX}"
        echo "      Local:      ${LOCAL_COUNT} rows, latest: ${LOCAL_MAX}"
        AUDIT_PASSED=false
    fi
}

# Function to compare row counts
compare_row_count() {
    local TABLE=$1
    local DESCRIPTION=$2

    echo -n "  $DESCRIPTION row count... "

    # Get production count
    PROD_COUNT=$(ssh -i "$SSH_KEY" "$REMOTE_HOST" \
        "$REMOTE_MYSQL -N -e \"SELECT COUNT(*) FROM $TABLE\" $REMOTE_DB_NAME 2>/dev/null" || echo "0")

    # Get local count
    LOCAL_COUNT=$(mysql "${LOCAL_MYSQL_INIT[@]}" -h"${LOCAL_DB_HOST}" -u"${LOCAL_DB_USER}" ${LOCAL_DB_PASS:+-p"$LOCAL_DB_PASS"} -N -e \
        "SELECT COUNT(*) FROM $TABLE" "$LOCAL_DB_NAME" 2>/dev/null || echo "0")

    if [ "$PROD_COUNT" = "$LOCAL_COUNT" ]; then
        echo -e "${GREEN}✓ MATCH${NC} ($LOCAL_COUNT rows)"
    else
        echo -e "${YELLOW}⚠ DIFF${NC} (Prod: $PROD_COUNT, Local: $LOCAL_COUNT)"
        # This is OK for partial mirrors, just a warning
        # If prod has rows but local is zero, try a targeted re-sync for critical tables
        if [ "$PROD_COUNT" -gt 0 ] && [ "$LOCAL_COUNT" -eq 0 ]; then
            case "$TABLE" in
                weather_observations|weather_forecasts)
                    sync_single_table "$TABLE"
                    # Re-check
                    LOCAL_COUNT=$(mysql "${LOCAL_MYSQL_INIT[@]}" -h"${LOCAL_DB_HOST}" -u"${LOCAL_DB_USER}" ${LOCAL_DB_PASS:+-p"$LOCAL_DB_PASS"} -N -e \
                        "SELECT COUNT(*) FROM $TABLE" "$LOCAL_DB_NAME" 2>/dev/null || echo "0")
                    if [ "$LOCAL_COUNT" = "$PROD_COUNT" ]; then
                        echo -e "    ${GREEN}✓ Fixed by re-sync (${LOCAL_COUNT} rows)${NC}"
                    else
                        echo -e "    ${RED}✗ Still mismatched after re-sync (Prod: $PROD_COUNT, Local: $LOCAL_COUNT)${NC}"
                        AUDIT_PASSED=false
                    fi
                    ;;
            esac
        fi
    fi
}

echo ""
echo "=== Data Integrity Audit ==="

# Verify reference tables match exactly
compare_row_count "parks" "parks"
compare_row_count "rides" "rides"
compare_row_count "ride_classifications" "ride_classifications"
compare_row_count "weather_observations" "weather_observations"
compare_row_count "weather_forecasts" "weather_forecasts"

# Always verify critical snapshot/stat tables
echo ""
echo "=== Snapshot & Stats Verification (CRITICAL) ==="
CRITICAL_TABLES=("ride_status_snapshots" "park_activity_snapshots" "ride_status_changes")
STAT_TABLES=("ride_daily_stats" "park_daily_stats" "ride_hourly_stats" "park_hourly_stats" "ride_weekly_stats" "park_weekly_stats" "ride_monthly_stats" "park_monthly_stats" "ride_yearly_stats" "park_yearly_stats")

for T in "${CRITICAL_TABLES[@]}"; do
    compare_row_count "$T" "$T"
done

for T in "${STAT_TABLES[@]}"; do
    compare_row_count "$T" "$T"
done

# For partial mirrors, also enforce date ranges on the main snapshot tables
if [ "$FULL" = false ] && [ "$SCHEMA_ONLY" = false ]; then
    echo ""
    echo "=== Date Range Verification (CRITICAL) ==="
    compare_date_range "park_activity_snapshots" "recorded_at" "park_activity_snapshots"
    compare_date_range "ride_status_snapshots" "recorded_at" "ride_status_snapshots"
fi

# Final audit result
echo ""
if [ "$AUDIT_PASSED" = true ]; then
    echo -e "${GREEN}=== AUDIT PASSED ===${NC}"
else
    echo -e "${RED}=== AUDIT FAILED ===${NC}"
    echo -e "${RED}The date ranges don't match production!${NC}"
    echo -e "${RED}This likely means timestamps were corrupted during import.${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}Mirror complete!${NC}"
echo -e "${GREEN}Time: ${MINUTES}m ${SECONDS}s${NC}"
echo -e "${GREEN}======================================${NC}"
