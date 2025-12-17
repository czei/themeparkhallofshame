#!/bin/bash
# Setup MySQL test database for integration tests
#
# This script clones the SCHEMA from themepark_tracker_dev (no data)
# to ensure tests run against the same structure as development.

set -e

echo "Setting up MySQL test database..."

# Database credentials from environment variables
if [ -z "${DB_ROOT_PASSWORD}" ]; then
    echo "Error: DB_ROOT_PASSWORD environment variable is not set"
    echo "Add to ~/.zshrc: export DB_ROOT_PASSWORD='your_password'"
    exit 1
fi

DB_NAME="themepark_test"
DB_USER="themepark_test"
DB_PASSWORD="${TEST_DB_PASSWORD:-test_password}"
SOURCE_DB="themepark_tracker_dev"

# Drop and recreate database (ensures clean state), create user
echo "Creating database and user..."
mysql -u root -p${DB_ROOT_PASSWORD} <<SQL
DROP DATABASE IF EXISTS ${DB_NAME};
CREATE DATABASE ${DB_NAME};
CREATE USER IF NOT EXISTS '${DB_USER}'@'localhost' IDENTIFIED BY '${DB_PASSWORD}';
GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'localhost';
FLUSH PRIVILEGES;
SQL

echo "Database and user created"

# Clone schema from dev database (structure only, no data)
echo "Cloning schema from ${SOURCE_DB}..."
mysqldump -u root -p${DB_ROOT_PASSWORD} --no-data ${SOURCE_DB} 2>/dev/null > /tmp/schema_dump.sql
mysql -u root -p${DB_ROOT_PASSWORD} ${DB_NAME} < /tmp/schema_dump.sql
rm /tmp/schema_dump.sql

echo "✅ Test database setup complete!"
echo ""
echo "Environment variables:"
echo "export TEST_DB_HOST=localhost"
echo "export TEST_DB_PORT=3306"
echo "export TEST_DB_NAME=${DB_NAME}"
echo "export TEST_DB_USER=${DB_USER}"
echo "export TEST_DB_PASSWORD=${DB_PASSWORD}"
