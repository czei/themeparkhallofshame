#!/bin/bash
# Theme Park Downtime Tracker - Database Setup Script
# Purpose: Initialize MySQL database schema and run migrations
# Usage: ./setup-database.sh [environment]
#   environment: local | production (default: local)

set -e  # Exit on error

ENVIRONMENT=${1:-local}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
MIGRATIONS_DIR="${PROJECT_ROOT}/backend/src/database/migrations"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}Theme Park Tracker - Database Setup${NC}"
echo -e "${GREEN}Environment: ${ENVIRONMENT}${NC}"
echo -e "${GREEN}======================================${NC}"
echo ""

# Load environment variables
if [ "${ENVIRONMENT}" = "local" ]; then
    ENV_FILE="${PROJECT_ROOT}/backend/.env"
    if [ ! -f "${ENV_FILE}" ]; then
        echo -e "${RED}Error: .env file not found at ${ENV_FILE}${NC}"
        echo -e "${YELLOW}Please copy .env.example to .env and configure your database settings${NC}"
        exit 1
    fi
    source "${ENV_FILE}"
elif [ "${ENVIRONMENT}" = "production" ]; then
    # Production uses environment variables set by systemd or deployment scripts
    if [ -z "${DB_HOST}" ] || [ -z "${DB_NAME}" ] || [ -z "${DB_USER}" ] || [ -z "${DB_PASSWORD}" ]; then
        echo -e "${RED}Error: Required environment variables not set (DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)${NC}"
        exit 1
    fi
fi

echo -e "${YELLOW}Database Configuration:${NC}"
echo "  Host: ${DB_HOST}"
echo "  Database: ${DB_NAME}"
echo "  User: ${DB_USER}"
echo ""

# Check if MySQL is accessible
if ! command -v mysql &> /dev/null; then
    echo -e "${RED}Error: mysql command not found. Please install MySQL client.${NC}"
    exit 1
fi

# Test database connection
echo -e "${YELLOW}Testing database connection...${NC}"
if ! mysql -h"${DB_HOST}" -u"${DB_USER}" -p"${DB_PASSWORD}" -e "SELECT 1;" &> /dev/null; then
    echo -e "${RED}Error: Cannot connect to MySQL server${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Database connection successful${NC}"
echo ""

# Create database if it doesn't exist
echo -e "${YELLOW}Creating database if not exists...${NC}"
mysql -h"${DB_HOST}" -u"${DB_USER}" -p"${DB_PASSWORD}" -e "CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
echo -e "${GREEN}✓ Database created/verified${NC}"
echo ""

# Run migrations in order
echo -e "${YELLOW}Running migrations...${NC}"
for migration in $(ls -1 "${MIGRATIONS_DIR}"/*.sql | sort); do
    migration_name=$(basename "${migration}")
    echo -n "  Applying ${migration_name}... "
    if mysql -h"${DB_HOST}" -u"${DB_USER}" -p"${DB_PASSWORD}" "${DB_NAME}" < "${migration}"; then
        echo -e "${GREEN}✓${NC}"
    else
        echo -e "${RED}✗ Failed${NC}"
        exit 1
    fi
done
echo -e "${GREEN}✓ All migrations applied successfully${NC}"
echo ""

# Verify schema
echo -e "${YELLOW}Verifying schema...${NC}"
TABLE_COUNT=$(mysql -h"${DB_HOST}" -u"${DB_USER}" -p"${DB_PASSWORD}" "${DB_NAME}" -sN -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${DB_NAME}';")
echo "  Tables created: ${TABLE_COUNT}"
echo ""

echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}Database setup complete!${NC}"
echo -e "${GREEN}======================================${NC}"
