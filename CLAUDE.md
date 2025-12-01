# ThemeParkHallOfShame Development Guidelines

Auto-generated from all feature plans. Last updated: 2025-11-22

## Active Technologies

- Python 3.11+ (001-theme-park-tracker)

## Project Structure

```text
src/
tests/
```

## Commands

cd src [ONLY COMMANDS FOR ACTIVE TECHNOLOGIES][ONLY COMMANDS FOR ACTIVE TECHNOLOGIES] pytest [ONLY COMMANDS FOR ACTIVE TECHNOLOGIES][ONLY COMMANDS FOR ACTIVE TECHNOLOGIES] ruff check .

## Code Style

Python 3.11+: Follow standard conventions

## Recent Changes

- 001-theme-park-tracker: Added Python 3.11+

<!-- MANUAL ADDITIONS START -->

## Test-Driven Development (TDD) Process

This project follows classic Test-Driven Development. All code changes must adhere to the TDD cycle.

### The TDD Cycle: Red-Green-Refactor

```
    +-------+
    |  RED  |  <-- Write a failing test first
    +---+---+
        |
        v
    +-------+
    | GREEN |  <-- Write minimal code to pass
    +---+---+
        |
        v
    +--------+
    |REFACTOR|  <-- Clean up, then repeat
    +--------+
```

1. **RED:** Write a test that defines expected behavior. Run it and verify it fails.
2. **GREEN:** Write the minimum code necessary to make the test pass. No more.
3. **REFACTOR:** Clean up the code while keeping tests green. Remove duplication.

### Required Practices

#### Before Writing Any Code
- Write the test first
- Ensure the test fails for the right reason
- Test names should describe the behavior being tested

#### Before Any Commit
- Run the full test suite: `pytest`
- All tests must pass
- No skipped tests without documented justification

#### Before Any Deployment
- Run the complete test suite: `pytest`
- Run linting: `ruff check .`
- All tests must pass with zero failures
- Review test coverage for new code

### Test Commands

```bash
# Run all tests
pytest

# Run tests with verbose output
pytest -v

# Run specific test file
pytest tests/path/to/test_file.py

# Run tests matching a pattern
pytest -k "test_pattern"

# Run with coverage report
pytest --cov=src --cov-report=term-missing

# Run linting
ruff check .
```

### Test Organization

```
tests/
├── unit/           # Fast, isolated tests (mock dependencies)
├── integration/    # Tests with real database/services
├── fixtures/       # Shared test fixtures
└── conftest.py     # Pytest configuration
```

### Test Naming Convention

```python
def test_<unit>_<scenario>_<expected_result>():
    # Example: test_shame_score_with_tier1_ride_down_returns_weighted_value
```

### What to Test

- **Do test:** Business logic, calculations, edge cases, error handling
- **Don't test:** Framework code, third-party libraries, trivial getters/setters

### Deployment Checklist

1. [ ] All new code has corresponding tests
2. [ ] `pytest` runs with 0 failures
3. [ ] `ruff check .` passes
4. [ ] No decrease in test coverage
5. [ ] Integration tests pass against test database

<!-- MANUAL ADDITIONS END -->
