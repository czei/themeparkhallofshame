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

## Canonical Business Rules

These are the authoritative rules that govern how ride and park data is interpreted. All queries and calculations MUST follow these rules. The single source of truth for implementation is `src/utils/sql_helpers.py`.

### Rule 1: Park Status Takes Precedence Over Ride Status

**CRITICAL: If a park is closed, ignore ALL ride statuses.**

Many parks do not update ride status data when the park is closed:
- Parks closed for winter may leave rides showing as "CLOSED" or "DOWN" even though no downtime is occurring
- Parks may report bogus ride status values when closed
- Test rides may operate before official park opening

**Implementation:**
- `park_appears_open = TRUE` must be checked before counting any ride as "down"
- The `RideStatusSQL.rides_that_operated_cte()` helper enforces this automatically
- Rides should only count toward downtime/shame if they have operated while the park was open

### Rule 2: Rides Must Have Operated to Count

A ride only counts toward downtime calculations if it has "operated" during the analysis period. A ride has "operated" if and only if:
1. The ride had at least one snapshot with `status='OPERATING'` or `computed_is_open=TRUE`
2. AND the park was open at that time (`park_appears_open=TRUE`)

**Why this matters:**
- Prevents closed parks from appearing in reliability rankings
- Filters out rides that never opened (e.g., seasonal rides, rides under refurbishment)
- Ensures Michigan's Adventure doesn't show 0% uptime when it's closed for the season

**Implementation:**
- Use `RideStatusSQL.rides_that_operated_cte()` for all downtime/reliability queries
- This CTE joins `park_activity_snapshots` to verify park was open when ride operated

### Rule 3: Park-Type Aware Downtime Logic

Disney and Universal parks properly distinguish between:
- `DOWN` = Unexpected breakdown
- `CLOSED` = Scheduled closure (e.g., meal breaks, weather)

Other parks (Dollywood, Busch Gardens, etc.) only report `CLOSED` for all non-operating rides, so we must treat `CLOSED` as potential downtime for non-Disney/Universal parks.

**Implementation:**
- Use `RideStatusSQL.is_down(table_alias, parks_alias="p")` with the parks_alias parameter
- The helper automatically applies park-type-aware logic

### Single Source of Truth

**ALL** ride status logic lives in `src/utils/sql_helpers.py`:
- `RideStatusSQL` - Ride operating/down status checks
- `ParkStatusSQL` - Park open/closed checks
- `DowntimeSQL` - Downtime calculations
- `UptimeSQL` - Uptime percentage calculations

**NEVER** duplicate status logic inline in queries. Always use the centralized helpers.

---

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

---

## MANDATORY: Local Testing Before Production Deployment

**NEVER deploy to production without first testing ALL affected functionality locally with real historical data.**

### Critical Rule

Before ANY production deployment:

1. **Mirror production database to local dev environment** using `deployment/scripts/mirror-production-db.sh`
   - For testing historical periods (TODAY, YESTERDAY, last_week, last_month), use `--full` or sufficient `--days=N`
   - Default `--days=7` only provides 1 week of data

2. **Test ALL time periods that could be affected:**
   - LIVE period
   - TODAY period
   - YESTERDAY period
   - last_week period
   - last_month period

3. **Manual browser verification:**
   - Open the frontend locally (http://localhost:8080)
   - Click through each period tab
   - Click Details button on multiple parks
   - Verify shame scores match between Rankings table and Details modal
   - Check charts display correctly

4. **Only after ALL local testing passes**, consider production deployment

### Why This Matters

- Unit tests can pass while real queries against real data fail
- Mocked data doesn't catch timezone issues, NULL handling, or edge cases
- The user's time is wasted debugging production issues that should have been caught locally

### DO NOT:
- Deploy based on "tests pass" alone
- Skip manual browser testing
- Deploy when local dev DB is missing data needed to test the feature
- Rush to production without testing every affected period

---

## MANDATORY: Human Verification Before Task Completion

**CRITICAL: A task is NOT complete until a human has manually verified it works.**

### The Rule

Before marking ANY task as "completed":

1. **Keep servers running** - Never kill development servers while the human is verifying
2. **Provide verification URLs** - Give the human the exact URLs to test
3. **Wait for explicit confirmation** - The human must explicitly say "verified" or "looks good" before marking complete
4. **Do NOT mark as complete prematurely** - Running automated tests is NOT the same as human verification

### What Human Verification Means

The human must be able to:
- Open the frontend in their browser
- Click through the affected features
- See that things work correctly with their own eyes
- Report any issues they find

### Claude's Responsibilities

1. Run all automated tests first
2. Start local servers (backend API + frontend)
3. Provide clear testing instructions and URLs
4. **WAIT** for the human to verify
5. Only mark task complete after human says it's verified

### Example Workflow

```
Claude: "Tests pass. Starting servers for manual verification..."
Claude: "Backend: http://localhost:5001"
Claude: "Frontend: http://localhost:8080"
Claude: "Please verify the shame scores display correctly on the Rankings page."
Claude: [WAITS - does NOT mark task complete]

User: "Looks good!"
Claude: [NOW marks task as complete]
```

### Why This Matters

- Automated tests can pass while the actual UI is broken
- Claude cannot see what the user sees in their browser
- Only a human can verify the full user experience
- Killing servers before verification wastes the user's time

<!-- MANUAL ADDITIONS END -->
