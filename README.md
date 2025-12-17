# Theme Park Hall of Shame

A data-driven web application that tracks and ranks theme park ride reliability across North America. See which parks have the most downtime, which rides are the least reliable, and how performance trends over time.

**Live Site**: [themeparkhallofshame.com](http://themeparkhallofshame.com)

## What It Does

- **Collects** ride status data from 80+ North American theme parks every 5 minutes
- **Tracks** when rides go down and calculates downtime statistics
- **Ranks** parks and rides by reliability with daily, weekly, and monthly aggregates
- **Displays** interactive dashboards showing the "Hall of Shame" - parks and rides with the worst downtime

## Screenshots

*Coming soon*

## Tech Stack

| Component | Technology |
|-----------|------------|
| **Backend** | Python 3.11+, Flask |
| **Database** | MySQL/MariaDB |
| **Frontend** | HTML, CSS, JavaScript (no framework) |
| **Data Source** | [ThemeParks.wiki](https://themeparks.wiki) API |
| **Hosting** | Apache + Gunicorn on Linux |

## Project Structure

```
ThemeParkHallOfShame/
├── backend/
│   ├── src/
│   │   ├── api/           # Flask REST API
│   │   ├── database/      # SQLAlchemy models & repositories
│   │   ├── collector/     # ThemeParks.wiki API client
│   │   ├── processor/     # Status detection & aggregation
│   │   └── scripts/       # Cron job entry points
│   └── tests/             # pytest unit & integration tests
├── frontend/
│   ├── index.html         # Main dashboard
│   ├── css/               # Stylesheets
│   └── js/                # API client & UI logic
├── docs/                  # Architecture & deployment docs
└── deployment/            # Server configuration files
```

## Quick Start

### Prerequisites

- Python 3.11+
- MySQL or MariaDB
- Node.js (optional, for frontend dev server)

### Backend Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/ThemeParkHallOfShame.git
cd ThemeParkHallOfShame

# Create virtual environment
cd backend
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy and configure environment
cp .env.example .env
# Edit .env with your database credentials

# Initialize database (run migrations)
mysql -u root -p < src/database/migrations/001_initial_schema.sql
# Continue with 002, 003, etc.

# Run the API server
python -m flask --app src.api.app run --port 5001
```

### Frontend Setup

```bash
cd frontend

# Serve locally (Python)
python3 -m http.server 8000

# Or with Node
npx serve .
```

Visit `http://localhost:8000` to see the dashboard.

### Running Tests

```bash
cd backend
pytest tests/ -v
```

---

## Production Operations (webperformance.com)

### System Architecture
- **Server**: ec2-user@webperformance.com
- **SSH Key**: `~/.ssh/michael-2.pem` (required for all SSH/rsync commands)
- **Installation**: `/opt/themeparkhallofshame/`
- **Service Management**: systemd (`themepark-api.service`)
- **API Server**: Gunicorn (2 workers) on `127.0.0.1:5001`
- **Web Server**: Apache proxy to Gunicorn

### Service Management

```bash
# Restart API service
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "sudo systemctl restart themepark-api"

# Check service status
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "sudo systemctl status themepark-api"

# View service logs (real-time)
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "sudo journalctl -u themepark-api -f"

# Check API health
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "curl -s http://127.0.0.1:5001/api/health | python3 -m json.tool"
```

### Deployment System

The production deployment includes **fail-fast validation** at multiple stages:

1. **Pre-flight validation** (local): Syntax, imports, WSGI, dependencies
2. **Deployment snapshot**: Automatic backup before changes
3. **Pre-service validation**: Environment, imports, database schema
4. **Smoke tests**: API endpoint validation with automatic rollback

```bash
# Standard deployment (with validation)
./deployment/deploy.sh all

# Emergency deployment (skip validation)
SKIP_VALIDATION=1 ./deployment/deploy.sh all
```

### Logs

| Log File | Purpose |
|----------|---------|
| `/opt/themeparkhallofshame/logs/error.log` | API errors |
| `/opt/themeparkhallofshame/logs/access.log` | HTTP requests |
| `/opt/themeparkhallofshame/logs/cron_wrapper.log` | Cron job execution |
| `/opt/themeparkhallofshame/logs/aggregate_hourly.log` | Hourly aggregation |
| `/opt/themeparkhallofshame/logs/aggregate_daily.log` | Daily aggregation |

### Cron Jobs

All cron jobs are wrapped with failure alerting:

| Job | Schedule | Description |
|-----|----------|-------------|
| `collect_snapshots` | Every 10 min | Captures ride status from ThemeParks.wiki |
| `aggregate_hourly` | :05 past each hour | Computes hourly stats |
| `aggregate_daily` | 1:00 AM server time | Computes daily stats |
| `check_data_collection` | Hourly | Monitors data collection health |
| `send_data_quality_alert` | 8:00 AM PT | Reports stale data issues |

### Monitoring

**Health Endpoint**: `http://127.0.0.1:5001/api/health`

Monitors:
- Database connectivity
- Data collection freshness
- Hourly/daily aggregation lag
- Disk space usage

**Cron Failure Alerts**: All cron jobs automatically send email alerts on failure (<90 seconds).

See [CLAUDE.md](CLAUDE.md#production-deployment-configuration) for complete deployment documentation.

## Testing

This project has comprehensive test coverage with **935+ tests across 64 files**:

- **Unit tests** (43 files, ~800 tests) - Fast, isolated business logic verification
- **Integration tests** (21 files, ~135 tests) - Real MySQL database interaction tests
- **Contract tests** - API schema validation
- **Golden data tests** - Regression testing with hand-computed values
- **Performance tests** - Query timing baselines

### Quick Start

```bash
# Run all tests (unit + integration + contract)
cd backend && pytest

# Run specific test categories
pytest tests/unit/           # Fast unit tests (<5 sec)
pytest tests/integration/    # Integration tests (~30 sec, requires test DB)
pytest tests/contract/       # API contract validation

# Run with coverage report
pytest --cov=src --cov-report=term-missing
pytest --cov=src --cov-report=html  # Generate HTML report in htmlcov/

# Run linting
ruff check .
```

### Test Categories

| Type | Count | Speed | Database | Purpose |
|------|-------|-------|----------|---------|
| Unit | ~800 tests | <5 sec | Mocked | Business logic verification |
| Integration | ~135 tests | ~30 sec | Real MySQL | Database interaction tests |
| Contract | Small | <1 sec | None | API schema validation |

### Why Both Unit and Integration Tests?

**Unit tests** (with mocks):
- ✅ Enable fast TDD iteration (<5 second feedback)
- ✅ Test pure business logic without infrastructure
- ✅ Make tests deterministic (no flaky failures)

**Integration tests** (real MySQL):
- ✅ Catch SQL syntax errors and schema mismatches
- ✅ Verify real data patterns (NULLs, time zones, edge cases)
- ✅ Test complex joins and aggregations
- ✅ Validate end-to-end API flows

**Both are necessary** for comprehensive coverage. See [Development Guide](docs/development.md#testing-strategy) for detailed documentation.

### Before Committing

All tests must pass before committing:

```bash
# 1. Run full test suite
pytest  # Must pass all 935+ tests

# 2. Run linting
ruff check .  # Must show no errors

# 3. Manual browser testing (for UI changes)
# See CLAUDE.md for detailed manual testing requirements
```

---

## Development Workflow

This section covers the complete development lifecycle, from writing code to deploying to production.

### TDD Cycle: Red-Green-Refactor

This project follows Test-Driven Development. Every code change follows this cycle:

```
┌─────────────────────────────────────────────────────────┐
│                                                         │
│   1. RED     →   Write a failing test first             │
│                                                         │
│   2. GREEN   →   Write minimal code to pass             │
│                                                         │
│   3. REFACTOR →  Clean up, keeping tests green          │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**Before writing any code**: Write the test first and verify it fails for the right reason.

---

### Daily Development Workflow

#### Scenario 1: Starting a New Feature

```bash
# 1. Create feature branch
git checkout -b feature/add-ride-alerts

# 2. Mirror production data for realistic testing (optional but recommended)
./deployment/scripts/mirror-production-db.sh --days=7

# 3. Start the local API server
cd backend
source venv/bin/activate
python -m flask --app src.api.app run --port 5001

# 4. In another terminal, serve the frontend
cd frontend
python3 -m http.server 8000

# 5. Write tests first (TDD!)
# Create tests/unit/test_ride_alerts.py

# 6. Run tests (they should fail - RED)
pytest tests/unit/test_ride_alerts.py -v

# 7. Implement the feature (GREEN)
# Edit src/api/... or src/processor/...

# 8. Run tests again (should pass)
pytest tests/unit/test_ride_alerts.py -v

# 9. Run full test suite before committing
pytest tests/ -v

# 10. Commit and push
git add .
git commit -m "Add ride alert feature"
git push -u origin feature/add-ride-alerts
```

#### Scenario 2: Fixing a Bug

```bash
# 1. Reproduce the bug locally with production data
./deployment/scripts/mirror-production-db.sh --days=7

# 2. Start local servers
cd backend && source venv/bin/activate
python -m flask --app src.api.app run --port 5001
# (In another terminal)
cd frontend && python3 -m http.server 8000

# 3. Confirm you can reproduce the bug at http://localhost:8000

# 4. Write a failing test that captures the bug
# tests/unit/test_shame_score_calculation.py
def test_shame_score_not_negative():
    """Parks should never have negative shame scores."""
    ...

# 5. Run the test - it should FAIL (reproducing the bug)
pytest tests/unit/test_shame_score_calculation.py::test_shame_score_not_negative -v

# 6. Fix the bug in the code

# 7. Run the test again - should PASS
pytest tests/unit/test_shame_score_calculation.py::test_shame_score_not_negative -v

# 8. Run full test suite to ensure no regressions
pytest tests/ -v

# 9. Manually verify the fix in browser

# 10. Commit with descriptive message
git commit -m "Fix negative shame scores for parks with no operating hours"
```

#### Scenario 3: Refactoring Existing Code

```bash
# 1. Ensure all tests pass BEFORE refactoring
pytest tests/ -v

# 2. Make refactoring changes (small incremental steps)

# 3. Run tests after EACH change
pytest tests/ -v

# 4. If tests fail, undo and try smaller changes

# 5. Once complete, run full suite
pytest tests/ -v && ruff check .

# 6. Commit
git commit -m "Refactor status calculator for clarity"
```

---

### Running Tests

#### Test Categories

| Directory | Purpose | Speed | Requires DB |
|-----------|---------|-------|-------------|
| `tests/unit/` | Business logic, calculations | Fast | No |
| `tests/integration/` | Database, API endpoints | Medium | Yes |
| `tests/contract/` | API response format validation | Fast | No |
| `tests/performance/` | Query timing, load testing | Slow | Yes |

#### Common Test Commands

```bash
cd backend
source venv/bin/activate

# Run all tests
pytest tests/ -v

# Run only unit tests (fast, no DB needed)
pytest tests/unit/ -v

# Run integration tests (needs local database)
pytest tests/integration/ -v

# Run a specific test file
pytest tests/unit/test_shame_score_calculation.py -v

# Run tests matching a pattern
pytest -k "shame_score" -v

# Run with coverage report
pytest --cov=src --cov-report=term-missing

# Run and stop on first failure
pytest -x

# Run last failed tests only
pytest --lf

# Verbose output with print statements
pytest -v -s
```

#### Pre-Commit Checklist

Before every commit:

```bash
# 1. All tests pass
pytest tests/ -v

# 2. Linting passes
ruff check .

# 3. No debug code left behind
grep -r "print(" src/ --include="*.py" | grep -v "logger"
```

---

### Manual Testing with Production Data

The database mirror script lets you test with real production data locally.

#### Mirror Production Database

```bash
# Default: Last 7 days of data (recommended for daily work)
./deployment/scripts/mirror-production-db.sh

# More historical data for trend analysis
./deployment/scripts/mirror-production-db.sh --days=30

# Full database (large, use sparingly)
./deployment/scripts/mirror-production-db.sh --full

# Schema only (for testing migrations)
./deployment/scripts/mirror-production-db.sh --schema-only

# Preview what would happen
./deployment/scripts/mirror-production-db.sh --dry-run
```

**What gets mirrored:**

| Table Type | Examples | Strategy |
|------------|----------|----------|
| Reference (small) | parks, rides, schedules | Full copy |
| Snapshots (large) | ride_status_snapshots | Filtered by `--days` |
| Aggregates | daily_stats, weekly_stats | Filtered by `--days` |

#### Local Testing Checklist

After mirroring, verify the application works:

```bash
# 1. Start the API server
cd backend && source venv/bin/activate
python -m flask --app src.api.app run --port 5001

# 2. Test API endpoints directly
curl http://localhost:5001/api/health
curl http://localhost:5001/api/parks/downtime?period=live
curl http://localhost:5001/api/parks/downtime?period=today

# 3. Start frontend
cd frontend && python3 -m http.server 8000

# 4. Open browser and test each view:
#    - http://localhost:8000 (Downtime - Live)
#    - Switch to Today, Last Week, Last Month
#    - Check Wait Times tab
#    - Check Awards tab
#    - Check Charts tab
#    - Click on a park to see details modal
```

---

### Deployment

#### Deployment Options

```bash
cd deployment

# Deploy everything (backend + frontend + restart services)
./deploy.sh all

# Deploy only backend (Python code)
./deploy.sh backend

# Deploy only frontend (HTML/CSS/JS)
./deploy.sh frontend

# Run database migrations only
./deploy.sh migrations

# Restart services without deploying code
./deploy.sh restart

# Check production health
./deploy.sh health
```

#### Scenario: Deploy a Bug Fix

```bash
# 1. Ensure all tests pass
cd backend
pytest tests/ -v

# 2. Commit your changes
git add .
git commit -m "Fix calculation error in weekly aggregation"

# 3. Push to remote
git push

# 4. Deploy to production
cd deployment
./deploy.sh backend

# 5. Verify the fix in production
./deploy.sh health
curl https://themeparkhallofshame.com/api/health
```

#### Scenario: Deploy Frontend-Only Changes

For CSS, JavaScript, or HTML changes:

```bash
# 1. Test locally first
cd frontend && python3 -m http.server 8000
# Verify at http://localhost:8000

# 2. Commit changes
git add .
git commit -m "Improve mobile responsiveness on park details modal"
git push

# 3. Deploy frontend only (faster than full deploy)
cd deployment
./deploy.sh frontend

# 4. Hard refresh production site to see changes
# (Ctrl+Shift+R or Cmd+Shift+R)
```

#### Scenario: Database Schema Migration

```bash
# 1. Create migration file
# backend/src/database/migrations/XXX_add_alerts_table.sql

# 2. Test migration locally first
mysql -u root -p themepark_tracker_dev < backend/src/database/migrations/XXX_add_alerts_table.sql

# 3. Verify it worked
mysql -u root -p themepark_tracker_dev -e "DESCRIBE alerts;"

# 4. Commit migration
git add .
git commit -m "Add alerts table for ride notifications"
git push

# 5. Deploy migration to production
cd deployment
./deploy.sh migrations

# 6. Restart backend to pick up schema changes
./deploy.sh restart
```

#### Scenario: Full Production Deploy

For major releases or when unsure:

```bash
# 1. Final test suite
cd backend
pytest tests/ -v
ruff check .

# 2. Commit everything
git add .
git commit -m "Release v1.2.0: Add weekly awards feature"
git push

# 3. Full deployment
cd deployment
./deploy.sh all

# 4. Comprehensive health check
./deploy.sh health
curl https://themeparkhallofshame.com/api/parks/downtime?period=live
curl https://themeparkhallofshame.com/api/rides/downtime?period=live
```

---

### Troubleshooting

#### Tests Failing After Database Mirror

```bash
# Reset local database and re-mirror
./deployment/scripts/mirror-production-db.sh --yes

# Or just get fresh schema
./deployment/scripts/mirror-production-db.sh --schema-only
```

#### API Returns 500 Error Locally

```bash
# Check Flask logs
cd backend
python -m flask --app src.api.app run --port 5001 --debug

# Verify database connection
mysql -u root -p -e "SELECT 1"

# Check .env file exists
cat backend/.env
```

#### Production Deploy Failed

```bash
# Check remote service status
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com \
    "sudo systemctl status themepark-api"

# View remote logs
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com \
    "sudo journalctl -u themepark-api -n 50"

# Manual restart
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com \
    "sudo systemctl restart themepark-api"
```

#### Frontend Changes Not Showing

```bash
# Increment version query string in index.html
# Change: js/app.js?v=9
# To:     js/app.js?v=10

# Or hard refresh in browser: Ctrl+Shift+R
```

---

## Data Collection

The system collects data via cron jobs:

| Job | Schedule | Description |
|-----|----------|-------------|
| `collect_snapshots` | Every 5 min | Captures current ride status from ThemeParks.wiki |
| `aggregate_daily` | 1 AM UTC | Calculates daily statistics |
| `collect_parks` | Sunday 2 AM | Refreshes park/ride metadata |

Raw snapshots are retained for 24 hours, then aggregated into permanent daily/weekly/monthly statistics.

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Health check |
| `GET /api/parks/downtime` | Park rankings by downtime |
| `GET /api/rides/downtime` | Ride rankings by downtime |
| `GET /api/parks/{id}` | Individual park details |
| `GET /api/live/status-summary` | Current system status |

See [API documentation](docs/architecture.md) for full details.

## Documentation

- [Architecture & Technology Decisions](docs/architecture.md)
- [Database Schema](docs/database_schema.md)
- [Deployment Guide](docs/deployment.md)
- [Future Ideas & AI Integration](docs/future-ideas.md)

## How Downtime is Calculated

1. **Data Collection**: Ride status is captured every 5 minutes from ThemeParks.wiki
2. **Status Logic**: A ride is considered "open" if `wait_time > 0` OR `is_open = true`
3. **Operating Hours**: Only time when the park is actually open counts toward uptime calculations
4. **Aggregation**: Daily stats are computed from raw snapshots, then rolled up to weekly/monthly

### Weighted Scoring

Rides are classified into tiers to fairly compare parks with different attraction portfolios:

| Tier | Weight | Examples |
|------|--------|----------|
| Tier 1 | 3x | Major coasters, E-tickets |
| Tier 2 | 2x | Standard attractions |
| Tier 3 | 1x | Kiddie rides, shows |

```
Park Score = Σ(downtime_hours × tier_weight) / Σ(all_ride_weights)
```

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Submit a pull request

## Data Attribution

This project uses the open-source [ThemeParks.wiki](https://themeparks.wiki) API.

> **Powered by [ThemeParks.wiki](https://themeparks.wiki)** — Open-source API providing live wait times & ride status for 50+ parks worldwide.

The ThemeParks.wiki project is available on GitHub: [github.com/ThemeParks/parksapi](https://github.com/ThemeParks/parksapi)

## License

*License to be determined*

## Disclaimer

This project tracks publicly available ride status data for informational purposes. "Hall of Shame" rankings reflect operational data and are not intended as criticism of park maintenance professionals, who work hard to keep attractions running safely.

Theme park operations involve complex factors including weather, maintenance schedules, and safety protocols that may not be reflected in simple uptime metrics.
