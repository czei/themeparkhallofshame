# Integration Tests

This directory contains integration tests that require external dependencies:

- **MySQL database** (real database queries)
- **File I/O** (CSV, JSON files)
- **MCP integration** (AI classification via Zen MCP)

## Setup

### MySQL Test Database

1. Install MySQL:
```bash
brew install mysql
```

2. Start MySQL server:
```bash
brew services start mysql
# OR
mysql.server start
```

3. Create test database:
```bash
mysql -u root -p
```

```sql
CREATE DATABASE themepark_test;
CREATE USER 'themepark_test'@'localhost' IDENTIFIED BY 'test_password';
GRANT ALL PRIVILEGES ON themepark_test.* TO 'themepark_test'@'localhost';
FLUSH PRIVILEGES;
```

4. Set environment variables:
```bash
export TEST_DB_HOST=localhost
export TEST_DB_PORT=3306
export TEST_DB_NAME=themepark_test
export TEST_DB_USER=themepark_test
export TEST_DB_PASSWORD=test_password
```

### Running Integration Tests

```bash
# Run all integration tests
pytest tests/integration/ -v

# Run specific integration test file
pytest tests/integration/test_health_endpoint_integration.py -v

# Run with MySQL connection
pytest tests/integration/ -v --mysql
```

## Test Categories

### Database Integration Tests
- Health endpoint with real MySQL queries
- Repository operations with MySQL-specific SQL (DATE_SUB, NOW, ON DUPLICATE KEY UPDATE)
- Aggregation service with complex queries
- Stats repository with window functions

### File I/O Integration Tests
- Classification service with CSV/JSON caching
- Manual overrides loading
- Exact matches caching

### MCP Integration Tests
- AI classification via Zen MCP
- Batch classification with parallel processing

## Current Status

**55 tests** marked for integration testing from unit tests:

- 31 tests require MySQL-specific SQL
- 19 tests require file I/O
- 5 tests require database connection
- Various tests require MCP integration

## Test Data

Integration tests use:
- Temporary MySQL database (created/destroyed per test)
- Temporary file system directories
- Mock MCP responses

## Notes

- Integration tests are slower than unit tests (database setup overhead)
- Tests should clean up after themselves (drop tables, delete files)
- Use transactions and rollback when possible
- Parallel execution may require database isolation
