#!/bin/bash
# Setup MySQL test database for integration tests

set -e

echo "Setting up MySQL test database..."

# Database credentials
DB_ROOT_PASSWORD="294e043ww"
DB_NAME="themepark_test"
DB_USER="themepark_test"
DB_PASSWORD="test_password"

# Create database and user
mysql -u root -p${DB_ROOT_PASSWORD} <<SQL
CREATE DATABASE IF NOT EXISTS ${DB_NAME};
CREATE USER IF NOT EXISTS '${DB_USER}'@'localhost' IDENTIFIED BY '${DB_PASSWORD}';
GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'localhost';
FLUSH PRIVILEGES;
SQL

echo "Database and user created"

# Run migrations
echo "Running database migrations..."
cd "$(dirname "$0")/.."
for file in src/database/migrations/00{1,2,3,4,5}_*.sql; do
    if [ -f "$file" ]; then
        echo "  Running $file..."
        mysql -u ${DB_USER} -p${DB_PASSWORD} ${DB_NAME} < "$file" 2>&1 | grep -v "Warning" || true
    fi
done

echo "✅ Test database setup complete!"
echo ""
echo "Environment variables:"
echo "export TEST_DB_HOST=localhost"
echo "export TEST_DB_PORT=3306"
echo "export TEST_DB_NAME=${DB_NAME}"
echo "export TEST_DB_USER=${DB_USER}"
echo "export TEST_DB_PASSWORD=${DB_PASSWORD}"
