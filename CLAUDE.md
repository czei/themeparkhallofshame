# ThemeParkHallOfShame Development Guidelines

Auto-generated from all feature plans. Last updated: 2025-12-08

## Active Technologies
- Python 3.11+ + Flask 3.0+, SQLAlchemy 2.0+ (Core only, no ORM models), mysqlclient 2.2+ (001-aggregation-tables)
- MySQL/MariaDB with existing schema (park_activity_snapshots, ride_status_snapshots, park_daily_stats, etc.) (001-aggregation-tables)
- Python 3.11+ + Flask 3.0+, SQLAlchemy 2.0+ (Core only), mysqlclient 2.2+, tenacity, requests (002-weather-collection)
- MySQL/MariaDB (existing database, new tables: weather_observations, weather_forecasts) (002-weather-collection)

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
- 002-weather-collection: Added Python 3.11+ + Flask 3.0+, SQLAlchemy 2.0+ (Core only), mysqlclient 2.2+, tenacity, requests
- 001-aggregation-tables: Added Python 3.11+ + Flask 3.0+, SQLAlchemy 2.0+ (Core only, no ORM models), mysqlclient 2.2+
- 001-aggregation-tables: Added Python 3.11+ + Flask 3.0+, SQLAlchemy 2.0+ (Core only, no ORM models), mysqlclient 2.2+


<!-- MANUAL ADDITIONS START -->

## 🚨 CRITICAL: Git Commit Policy 🚨

**NEVER commit files without explicit user approval.**

### The Rule

1. **Make changes** to files as requested
2. **Show the user** what was changed (using `git diff` or explaining the changes)
3. **WAIT for explicit approval** - User must say "commit", "looks good, commit", or similar
4. **NEVER auto-commit** after completing work
5. **NEVER batch commits** - If user says "commit and continue", commit ONLY the current changes, then wait for approval on the next set of changes

### Why This Matters

- The user needs to review all changes before they go into git history
- Automated commits are annoying and disrespectful of the user's workflow
- The user may want to:
  - Review the changes in their editor
  - Test the changes first
  - Make additional modifications
  - Write their own commit message

### What to Do Instead

```
✅ CORRECT:
Claude: "I've updated docs/deployment.md with the Mermaid diagrams. Here's what changed:"
Claude: [Shows git diff or explains changes]
Claude: "Would you like to review these changes before committing?"
User: "looks good, commit it"
Claude: [NOW commits]

❌ WRONG:
Claude: "I've updated the file and committed the changes."
[User never had a chance to review!]
```

### Exceptions

The ONLY time you can commit without asking is if the user explicitly says:
- "commit these changes"
- "commit and push"
- "make the change and commit it"

Even then, show what you're committing!

---

## Production Deployment Configuration

### SSH Access to Production Server

**CRITICAL: All SSH and rsync commands to production MUST use the SSH key.**

**Production Server:** `ec2-user@webperformance.com`
**SSH Key:** `~/.ssh/michael-2.pem`

**SSH Command Pattern:**
```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "<command>"
```

**Rsync Command Pattern:**
```bash
rsync -av -e "ssh -i ~/.ssh/michael-2.pem" <local-file> ec2-user@webperformance.com:<remote-path>
```

**Common Production Commands:**
```bash
# Restart API service
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "sudo systemctl restart themepark-api"

# Check service status
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "sudo systemctl status themepark-api"

# View service logs
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "sudo journalctl -u themepark-api -f"

# Test API health
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "curl -s http://127.0.0.1:5001/api/health | python3 -m json.tool"

# Check cron jobs
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "crontab -l"
```

### Deployment Process

**Standard Deployment:**
```bash
# Use the deployment script (includes validation and rollback)
./deployment/deploy.sh all
```

**Emergency Deployment (skip validation):**
```bash
# Only use for critical hotfixes
SKIP_VALIDATION=1 ./deployment/deploy.sh all
```

**Manual Deployment Steps:**
1. Pre-flight validation runs locally (syntax, imports, dependencies)
2. Deployment snapshot created on production (for rollback)
3. Code deployed via rsync
4. Database migrations run
5. Service restarted (pre-service validation runs before gunicorn starts)
6. Smoke tests verify deployment (automatic rollback if tests fail)

**Rollback:**
```bash
# List available snapshots
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "/opt/themeparkhallofshame/deployment/scripts/snapshot-manager.sh list"

# Restore a snapshot
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "/opt/themeparkhallofshame/deployment/scripts/snapshot-manager.sh restore <snapshot-name>"
```

### Environment Configuration

Production environment variables are in: `/opt/themeparkhallofshame/backend/.env`

**Required Variables:**
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` - Database connection
- `FLASK_ENV`, `SECRET_KEY`, `ENVIRONMENT` - Flask configuration
- `SENDGRID_API_KEY` - Email alerts (optional, for cron failure notifications)
- `ALERT_EMAIL_FROM`, `ALERT_EMAIL_TO` - Alert email addresses

### Monitoring

**Health Endpoint:**
```bash
curl http://127.0.0.1:5001/api/health
```

Monitors:
- Database connectivity
- Data collection freshness
- Hourly/daily aggregation status and lag
- Disk space usage

**Cron Job Logs:**
- `/opt/themeparkhallofshame/logs/cron_wrapper.log` - All cron job execution logs
- `/opt/themeparkhallofshame/logs/collect_snapshots.log` - Data collection logs
- `/opt/themeparkhallofshame/logs/aggregate_hourly.log` - Hourly aggregation logs
- `/opt/themeparkhallofshame/logs/aggregate_daily.log` - Daily aggregation logs

**Service Logs:**
```bash
# Real-time service logs
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "sudo journalctl -u themepark-api -f"

# API error logs
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "tail -f /opt/themeparkhallofshame/logs/error.log"

# API access logs
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "tail -f /opt/themeparkhallofshame/logs/access.log"
```

---

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

A ride only counts toward downtime calculations if it has "operated" during the analysis period.

**For Hourly Aggregation:**
A ride has "operated" if it operated at ANY point during the Pacific calendar day:
1. The ride had at least one snapshot with `status='OPERATING'` or `computed_is_open=TRUE`
2. AND the park was open at that time (`park_appears_open=TRUE`)
3. AND the snapshot occurred anywhere during the Pacific calendar day (not just the specific hour)

**Why this matters for hourly metrics:**
- **CRITICAL FIX**: Multi-hour outages must persist across all hours of the day after the ride operated
- Example: Ride operates at 10:00am, goes down at 10:30am → counts as down in 10am, 11am, 12pm, etc.
- Without "operated today" logic, multi-hour outages disappear after the first hour

**For Daily/Weekly/Monthly Aggregation:**
A ride has "operated" if it operated during the specific aggregation period (day/week/month):
1. The ride had at least one snapshot with `status='OPERATING'` or `computed_is_open=TRUE`
2. AND the park was open at that time (`park_appears_open=TRUE`)
3. AND the snapshot occurred during the aggregation period

**Why this matters:**
- Prevents closed parks from appearing in reliability rankings
- Filters out rides that never opened (e.g., seasonal rides, rides under refurbishment)
- Ensures Michigan's Adventure doesn't show 0% uptime when it's closed for the season

**Implementation:**
- **Hourly**: Use `rides_operated_today` CTE that checks Pacific calendar day
- **Daily/Weekly/Monthly**: Use `RideStatusSQL.rides_that_operated_cte()` for the aggregation period
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

## MANDATORY: Test-Driven Development (TDD) Process

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
├── contract/       # API contract validation tests
├── golden_data/    # Hand-computed expected values for regression testing
├── performance/    # Query timing and performance tests
├── fixtures/       # Shared test fixtures
└── conftest.py     # Pytest configuration
```

### Layered Testing Philosophy

This project uses a **layered testing strategy** with 935+ tests across 64 files. **BOTH unit and integration tests are necessary** - they serve different purposes.

#### Unit Tests (43 files, ~800 tests)

**Purpose:** Fast verification of business logic

- **Database:** Mocked (`mock_db_connection` fixture with `MagicMock`)
- **Speed:** <5 seconds for all 800 unit tests
- **Value:** Catch logic errors, enable TDD red-green-refactor cycle
- **Focus:** Pure business logic, calculations, transformations, error handling

**Why mocking is NOT useless:**
- Enables **fast TDD iteration** - instant feedback during development
- Tests **pure logic** without infrastructure dependencies
- **Isolates** the code under test from database, network, or file system issues
- Makes tests **deterministic** - no flaky failures from external systems

**Example:**
```python
def test_shame_score_calculation(mock_db_connection):
    # Fast, isolated, tests pure calculation logic
    calculator = ShameScoreCalculator(mock_db_connection)
    score = calculator.calculate(downtime_hours=10, tier=1)
    assert score == 100  # 10 hours * tier 1 weight (10) = 100
```

#### Integration Tests (21 files, ~135 tests)

**Purpose:** Verify database interactions and queries work correctly

- **Database:** Real MySQL with automatic transaction rollback
- **Speed:** ~30 seconds for all 135 integration tests
- **Value:** Catch SQL errors, schema issues, real-world data patterns
- **Focus:** Database queries, schema assumptions, aggregations, API endpoints

**Why integration tests matter:**
- Catch **SQL syntax errors** and **schema mismatches**
- Verify **real data patterns** (NULL handling, edge cases, time zones)
- Test **complex joins** and **subqueries** that can't be mocked
- Validate **end-to-end** API flows (request → business logic → database → response)

**Transaction safety:**
Each integration test runs in a transaction that rolls back automatically, ensuring test isolation without recreating tables.

**Example:**
```python
def test_park_rankings_query(mysql_connection):
    # Real MySQL, verifies actual query results
    insert_test_park(mysql_connection, name="Test Park")
    insert_test_rides(mysql_connection, park_id=1, count=10)

    result = execute_park_rankings_query(mysql_connection, period="today")

    assert result[0]['park_name'] == "Test Park"
    assert result[0]['shame_score'] > 0
    assert result[0]['total_rides'] == 10
```

#### When to Use Each Test Type

**Write unit tests when:**
- Testing business logic (calculations, transformations)
- Testing pure functions without side effects
- Testing error handling and edge cases
- You need fast feedback during TDD (<5 second cycle)

**Write integration tests when:**
- Testing SQL queries against real MySQL
- Testing database schema assumptions
- Testing aggregations that depend on actual data
- Testing API endpoints end-to-end
- Verifying complex joins, subqueries, or window functions

**BOTH are necessary:**
- **Unit tests** enable fast iteration during development
- **Integration tests** catch real-world bugs that mocks miss
- Together they provide **comprehensive coverage** (935+ tests, 80% minimum)

#### Contract Tests (1 file)

Validate that API responses match declared OpenAPI schema.

#### Golden Data Tests (4 files)

Regression testing with hand-computed expected values to catch calculation discrepancies.

#### Performance Tests (1 file)

Query timing baselines marked with `@pytest.mark.performance`.

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

### Test Failure Policy

**CRITICAL: "Database out of sync" is NOT a valid reason to ignore test failures.**

When a test fails:
1. The test failure **MUST** block deployment
2. Investigate the actual cause - do **NOT** assume it's a data issue
3. If the test genuinely requires fresh data, it should use `freezegun` + fixtures
4. Only a human can explicitly waive a test failure (and must document why)

Tests are designed to be deterministic. If a test is flaky due to data:
- The test is **broken** and needs to be fixed
- **NOT**: The data is "out of sync" and can be ignored

**Why this policy exists:**
- Serious bugs reached production because test failures were dismissed as "database sync issues"
- Test infrastructure now includes safety checks to prevent running against wrong databases
- All time-sensitive tests should use `freezegun` for deterministic behavior

### Test Directory Structure

```
tests/
├── unit/           # Pure logic tests with mocks. NO external I/O (database, network)
├── integration/    # Database interaction tests. Use mysql_connection fixture
├── contract/       # API contract validation tests
├── golden_data/    # Hand-computed expected values for regression testing
└── conftest.py     # Pytest configuration
```

**Database Rules:**
- **Unit tests**: Use `mock_db_connection` fixture (MagicMock)
- **Integration tests**: Use `mysql_connection` fixture (creates isolated transaction, rolls back after test)
- **NEVER** run automated tests against production or development databases
- Test database is `themepark_test` - protected databases will cause immediate test failure

### Time-Sensitive Tests

For any test involving date/time logic (TODAY, YESTERDAY, last_week, etc.):

1. **MUST** use `freezegun.freeze_time()` decorator with explicit timestamp
2. **MUST** define constants like `MOCKED_NOW_UTC` at module level
3. **MUST** create test data with timestamps relative to the mocked time

**Example pattern** (from `test_today_api_contract.py`):
```python
from freezegun import freeze_time
from datetime import datetime, timezone

MOCKED_NOW_UTC = datetime(2025, 12, 6, 4, 0, 0, tzinfo=timezone.utc)  # 8 PM PST Dec 5th
TODAY_START_UTC = datetime(2025, 12, 5, 8, 0, 0, tzinfo=timezone.utc)  # Midnight PST Dec 5th

@freeze_time(MOCKED_NOW_UTC)
def test_today_data(self):
    # Test runs with deterministic "now"
    # Create test data with timestamps between TODAY_START_UTC and MOCKED_NOW_UTC
```

### Production Replica Testing (Optional)

For tests that validate real-world time-based aggregations against fresh data:

1. **Setup**: Configure read-only MySQL replica with ≤5 min lag
2. **Environment Variables**:
   - `REPLICA_DB_HOST` - Replica hostname
   - `REPLICA_DB_PORT` - Replica port (default: 3306)
   - `REPLICA_DB_NAME` - Replica database name
   - `REPLICA_DB_USER` - Read-only user
   - `REPLICA_DB_PASSWORD` - Password

3. **Usage**: Mark tests with `@pytest.mark.requires_replica`
4. **Purpose**: Catch time-boundary bugs that deterministic fixtures might miss
5. **Note**: Replica tests are **optional** and **non-blocking** for CI

**When to use replica tests:**
- Validating aggregation logic against real data patterns
- Testing timezone edge cases with actual park schedules
- Smoke testing before production deployment

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

## Zen AI Capabilities for Cost and Context Efficiency

**CRITICAL: Use Zen's specialized tools instead of expensive Claude reasoning whenever possible.** Zen provides cheaper, faster access to expert models and reduces Claude context usage.

### 🚨 MANDATORY CHECK-FIRST RULE 🚨

**BEFORE doing ANY of these tasks yourself, you MUST check if Zen can do it:**

1. **Writing tests** → Use `testgen` tool FIRST
2. **Code review** → Use `codereview` tool FIRST
3. **Debugging** → Use `debug` tool FIRST
4. **Planning implementation** → Use `planner` tool FIRST
5. **Analyzing code** → Use `analyze` tool FIRST
6. **Refactoring** → Use `refactor` tool FIRST
7. **Security audits** → Use `secaudit` tool FIRST
8. **Generating docs** → Use `docgen` tool FIRST
9. **Complex decisions** → Use `consensus` tool FIRST
10. **Pre-commit validation** → Use `precommit` tool FIRST

**If you find yourself about to write code, tests, or analysis manually, STOP and ask: "Can Zen do this for me?"**

The answer is almost always YES. Use Zen first, then execute based on Zen's output.

### When to Use Each Zen Tool

| Tool | Purpose | When to Use |
|------|---------|-------------|
| **chat** | Collaborative thinking, brainstorming, getting second opinions | When you need to discuss ideas, validate approaches, or explore concepts with an external model |
| **thinkdeep** | Multi-stage investigation and reasoning for complex problems | For architecture decisions, complex bugs, performance challenges, security analysis requiring systematic hypothesis testing |
| **challenge** | Prevents reflexive agreement, forces critical thinking | Automatically triggered when user pushes back; use manually to sanity-check contentious claims |
| **planner** | Interactive step-by-step planning with revision capabilities | For complex project planning, system design, migration strategies, architectural decisions |
| **consensus** | Multi-model consensus analysis through structured debate | For complex decisions, architectural choices, feature proposals, technology evaluations |
| **codereview** | Systematic code review covering quality, security, performance, architecture | After writing significant code, before merging PRs, for comprehensive analysis |
| **precommit** | Validates git changes and repository state before committing | Before any commit to check for security issues, change impact, completeness |
| **debug** | Systematic debugging and root cause analysis | For complex bugs, mysterious errors, performance issues, race conditions, memory leaks |
| **analyze** | Comprehensive code analysis for architecture, performance, maintainability | When exploring codebase structure, assessing tech debt, planning improvements |
| **refactor** | Code refactoring analysis with decomposition focus | For detecting code smells, planning decomposition, modernization, organization improvements |
| **tracer** | Call-flow mapping and dependency tracing | To understand execution flow (precision mode) or structural relationships (dependencies mode) |
| **testgen** | Test generation with edge case coverage | When creating comprehensive test suites for specific functions/classes/modules |
| **secaudit** | Security audit with OWASP analysis | For OWASP Top 10 analysis, compliance evaluation, threat modeling, security architecture review |
| **docgen** | Documentation generation with complexity analysis | When generating API docs, analyzing function complexity, creating maintainer documentation |

### Benefits of Using Zen

1. **Cost Savings**: Zen tools use cheaper models (GPT-5.1, Gemini, etc.) instead of expensive Claude reasoning
2. **Context Efficiency**: Reduces Claude context usage by offloading complex analysis to external models
3. **Expert Validation**: Provides expert analysis from multiple model perspectives
4. **Structured Workflows**: Enforces systematic investigation patterns (hypothesis testing, evidence gathering)

### Usage Guidelines

- **Default to Zen**: When a task matches a Zen tool's purpose, use the Zen tool first
- **Combine with Claude**: Use Claude for execution after Zen provides analysis/planning
- **Continuation IDs**: Always reuse `continuation_id` to preserve conversation context across Zen calls
- **Model Selection**: Let Zen auto-select models unless user specifies a preference

### Example Workflow

1. **Planning**: Use `planner` for complex implementation planning
2. **Analysis**: Use `analyze` to understand existing codebase structure
3. **Implementation**: Use Claude for actual coding with TDD
4. **Review**: Use `codereview` to validate code quality
5. **Testing**: Use `testgen` to create comprehensive tests
6. **Validation**: Use `precommit` before committing changes

<!-- MANUAL ADDITIONS END -->
