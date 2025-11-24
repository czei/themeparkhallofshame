# Testing Guide - Theme Park Downtime Tracker

## Overview

This document describes the testing strategy and infrastructure for the Theme Park Downtime Tracker backend.

## Test Statistics

- **Total Tests**: 200 passing, 55 skipped
- **Code Coverage**: 39%
- **Test Files**: 16 unit test files
- **Testing Framework**: pytest with SQLite for unit tests

## Testing Strategy

### Unit Tests (SQLite)
Located in `tests/unit/`, these test business logic without external dependencies:

- Pure Python logic (calculations, transformations)
- Pattern matching and classification
- Flask app structure and routing
- Error handling

**Advantages:**
- Fast execution (< 2 seconds)
- No database setup required
- Easy to run locally
- Good for TDD workflow

### Integration Tests (MySQL)
Located in `tests/integration/`, these test with real dependencies:

- Database operations (MySQL-specific SQL)
- File I/O (CSV, JSON)
- MCP integration for AI classification
- Complex aggregations

**Currently**: 55 tests marked for integration phase

## Test Files

### Foundation Tests (100% coverage)
- `test_status_calculator.py` - 34 tests - Ride status logic
- `test_config.py` - 24 tests - Configuration management

### Repository Tests
- `test_park_repository.py` - Park CRUD operations
- `test_ride_repository.py` - Ride CRUD operations
- `test_snapshot_repository.py` - Snapshot operations
- `test_status_change_repository.py` - Status change tracking
- `test_aggregation_repository.py` - Aggregation logging

### Processor Tests
- `test_status_change_detector.py` - 15 tests - Status transition detection
- `test_operating_hours_detector.py` - 7 passing, 2 skipped - Operating hours
- `test_aggregation_service.py` - 4 passing, 14 skipped - Daily aggregation

### Classifier Tests (100% coverage)
- `test_pattern_matcher.py` - 28 tests - Keyword-based classification
- `test_ai_classifier.py` - 29 tests - AI JSON parsing
- `test_classification_service.py` - 2 passing, 19 skipped - Orchestration

### API Tests
- `test_api_app.py` - 13 passing, 5 skipped - Flask app & routes

## Running Tests

### Run All Unit Tests
```bash
cd backend
pytest tests/unit/ -v
```

### Run Specific Test File
```bash
pytest tests/unit/test_pattern_matcher.py -v
```

### Run with Coverage Report
```bash
pytest tests/unit/ --cov=src --cov-report=html
```

### Run Only Passing Tests (Skip Integration)
```bash
pytest tests/unit/ -v -m "not skip"
```

## Test Fixtures

Located in `tests/conftest.py`:

- `sqlite_connection` - In-memory SQLite database with schema
- `sample_park_data` - Mock park data
- `sample_ride_data` - Mock ride data
- Helper functions: `insert_sample_park()`, `insert_sample_ride()`

## SQLite vs MySQL Compatibility

### Issues Addressed

**DateTime Handling:**
```python
# SQLite returns datetime as strings
if isinstance(recorded_at, str):
    recorded_at = datetime.fromisoformat(recorded_at.replace(' ', 'T'))
```

**Boolean Handling:**
```python
# SQLite returns booleans as 0/1 integers
# Use truthiness instead of identity checks
if not status:  # Works with both 0 and False
    # instead of: if status is False
```

**SQL Compatibility:**
- Unit tests avoid MySQL-specific SQL (NOW(), DATE_SUB(), ON DUPLICATE KEY UPDATE)
- Integration tests will use real MySQL for complex queries

## Coverage Goals

- **Target**: 80% overall coverage
- **Current**: 39% (foundation established)
- **Strategy**: Focus on business logic coverage first, defer infrastructure code

## Skipped Tests (55 total)

Tests marked with `@pytest.mark.skip` and deferred to integration phase:

**Reasons for Skipping:**
1. **MySQL-specific SQL** (NOW(), ON DUPLICATE KEY UPDATE) - 31 tests
2. **File I/O required** (CSV, JSON) - 19 tests
3. **Database connection required** - 5 tests
4. **MCP integration required** - Various classifier tests

## Key Testing Patterns

### Repository Tests
```python
def test_get_by_id(self, sqlite_connection, sample_park_data):
    from tests.conftest import insert_sample_park

    park_id = insert_sample_park(sqlite_connection, sample_park_data)
    repo = ParkRepository(sqlite_connection)

    park = repo.get_by_id(park_id)

    assert park is not None
    assert park['name'] == sample_park_data['name']
```

### API Tests
```python
def test_root_endpoint_returns_200(self):
    app = create_app()
    client = app.test_client()

    response = client.get('/')

    assert response.status_code == 200
    assert response.content_type == 'application/json'
```

### Classifier Tests
```python
def test_classify_coaster(self):
    matcher = PatternMatcher()

    result = matcher.classify("Space Mountain Coaster")

    assert result.tier == 1
    assert result.confidence == 0.75
```

## Common Issues & Solutions

### Import Errors
**Problem**: `attempted relative import beyond top-level package`
**Solution**: Use absolute imports from `src/` root:
```python
# Bad:  from ..utils.logger import logger
# Good: from utils.logger import logger
```

### Database Connection Issues
**Problem**: Tests fail with "no such table"
**Solution**: Ensure conftest.py creates all required tables in SQLite schema

### Boolean Comparison Failures
**Problem**: `if status is False` fails with SQLite
**Solution**: Use `if not status` for SQLite/MySQL compatibility

## Code Quality

### Pre-commit Hooks
- Coverage threshold: 80% (currently enforced but not met)
- Tests run automatically on commit (currently skipped)

### CI/CD Integration
- GitHub Actions workflow (pending setup)
- Run tests on pull requests
- Coverage reports to codecov.io

## Next Steps

1. **Integration Test Setup**
   - MySQL test database configuration
   - Database fixtures with sample data
   - File I/O test fixtures (temp directories)

2. **Contract Tests**
   - API contract validation
   - Schema validation for JSON responses

3. **Performance Tests**
   - Aggregation performance benchmarks
   - Database query optimization

4. **End-to-End Tests**
   - Full workflow testing
   - Data collection → aggregation → API retrieval

## Contributing

When adding new code:

1. Write unit tests for pure logic
2. Mark integration tests with `@pytest.mark.skip` and clear reason
3. Aim for 80%+ coverage on new code
4. Use existing test patterns from this guide
5. Run tests before committing: `pytest tests/unit/ -v`

## Resources

- [pytest Documentation](https://docs.pytest.org/)
- [SQLAlchemy Testing](https://docs.sqlalchemy.org/en/20/orm/session_transaction.html)
- [Flask Testing](https://flask.palletsprojects.com/en/2.3.x/testing/)
