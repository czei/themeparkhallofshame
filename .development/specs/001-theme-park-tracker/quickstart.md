# Quickstart Guide: Theme Park Downtime Tracker

**Version**: 1.0
**Last Updated**: 2025-11-23

This guide will help you set up the Theme Park Downtime Tracker locally for development and testing, then deploy to production on the webperformance.com server.

## Development & Deployment Strategy

**Recommended Workflow**: Local Development → Production Deployment

```
┌─────────────────────────────────────────────────────────────┐
│ Phase 1: Local Development (Weeks 1-4)                     │
│ - Set up local MySQL database on your laptop/desktop       │
│ - Develop and test all components locally                  │
│ - Run pytest suite (>80% coverage)                         │
│ - Test with real Queue-Times.com API                       │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Phase 2: Production Deployment (Days 1-3)                  │
│ - Deploy to webperformance.com server (co-located)         │
│ - Set up production MySQL database                         │
│ - Configure Apache VirtualHost + systemd services          │
│ - Apply systemd resource limits (CPUQuota, MemoryMax)      │
│ - Set up CloudWatch monitoring and alarms                  │
└─────────────────────────────────────────────────────────────┘
```

**Deployment Architecture**: Co-located on existing webperformance.com AWS server
- **Why**: $0 incremental cost, webperformance.com has low utilization
- **Safeguards**: systemd resource limits prevent contention
- **Migration Path**: Move to dedicated EC2 ($200/year) if traffic exceeds 1000 req/day or CPU >60%

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Local Database Setup](#local-database-setup)
3. [Python Environment Setup](#python-environment-setup)
4. [Configuration](#configuration)
5. [Running the Ride Classification Script](#running-the-ride-classification-script)
6. [Running the Data Collector](#running-the-data-collector)
7. [Running the Flask API](#running-the-flask-api)
8. [Testing with Sample API Calls](#testing-with-sample-api-calls)
9. [Production Deployment](#production-deployment)
10. [Troubleshooting](#troubleshooting)
11. [Development Workflow](#development-workflow)

---

## Prerequisites

Before you begin, ensure you have the following installed on your system:

### Required Software

- **Python 3.11+** - [Download](https://www.python.org/downloads/)
  ```bash
  python3 --version  # Should show 3.11 or higher
  ```

- **MySQL 8.0+** - [Download](https://dev.mysql.com/downloads/mysql/)
  ```bash
  mysql --version  # Should show 8.0 or higher
  ```

- **virtualenv** or **venv** - Python virtual environment tool
  ```bash
  pip3 install virtualenv
  ```

- **Git** - Version control
  ```bash
  git --version
  ```

### Optional (Recommended)

- **MySQL Workbench** - GUI tool for database management
- **Postman** or **Insomnia** - API testing tools
- **VS Code** or **PyCharm** - Code editor with Python support

### API Access

- **Queue-Times.com API Key** - Sign up at [Queue-Times.com](https://queue-times.com/en-US/pages/api) for free API access
  - Required for data collection
  - Free tier allows sufficient requests for development (5-minute update frequency)

---

## Local Database Setup

### Step 1: Install and Start MySQL

**macOS** (using Homebrew):
```bash
brew install mysql@8.0
brew services start mysql@8.0
```

**Linux** (Ubuntu/Debian):
```bash
sudo apt update
sudo apt install mysql-server
sudo systemctl start mysql
sudo systemctl enable mysql
```

**Windows**:
- Download MySQL installer from [MySQL Downloads](https://dev.mysql.com/downloads/installer/)
- Run installer and follow setup wizard
- Ensure MySQL service is running

### Step 2: Secure MySQL Installation

```bash
sudo mysql_secure_installation
```

Follow prompts to:
- Set root password
- Remove anonymous users
- Disallow root login remotely
- Remove test database

### Step 3: Create Database

Log in to MySQL:
```bash
mysql -u root -p
```

Create the database and user:
```sql
-- Create database
CREATE DATABASE theme_park_tracker CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create dedicated user (replace 'your_password' with a strong password)
CREATE USER 'theme_park_user'@'localhost' IDENTIFIED BY 'your_password';

-- Grant privileges
GRANT ALL PRIVILEGES ON theme_park_tracker.* TO 'theme_park_user'@'localhost';
FLUSH PRIVILEGES;

-- Verify database creation
SHOW DATABASES;

-- Exit MySQL
EXIT;
```

### Step 4: Run Database Migrations

From the project root directory:

```bash
# Test database connection
mysql -u theme_park_user -p theme_park_tracker -e "SELECT 1;"

# Run schema creation script
mysql -u theme_park_user -p theme_park_tracker < db/schema.sql

# Verify tables were created
mysql -u theme_park_user -p theme_park_tracker -e "SHOW TABLES;"
```

**Expected output:**
```
+--------------------------------+
| Tables_in_theme_park_tracker   |
+--------------------------------+
| park_activity_snapshots        |
| park_daily_stats               |
| park_monthly_stats             |
| park_operating_sessions        |
| park_weekly_stats              |
| park_yearly_stats              |
| parks                          |
| ride_daily_stats               |
| ride_monthly_stats             |
| ride_status_changes            |
| ride_status_snapshots          |
| ride_weekly_stats              |
| ride_yearly_stats              |
| rides                          |
+--------------------------------+
```

### Step 5: Seed Initial Park Data (Optional)

Load sample park and ride data for testing:

```bash
mysql -u theme_park_user -p theme_park_tracker < db/seed_data.sql
```

This populates the `parks` and `rides` tables with North American theme park data.

---

## Python Environment Setup

### Step 1: Clone the Repository

```bash
cd ~/Projects
git clone https://github.com/yourusername/ThemeParkHallOfShame.git
cd ThemeParkHallOfShame
```

### Step 2: Create Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# macOS/Linux:
source venv/bin/activate

# Windows:
venv\Scripts\activate
```

Your terminal prompt should now show `(venv)` prefix.

### Step 3: Install Dependencies

```bash
# Upgrade pip
pip install --upgrade pip

# Install project dependencies
pip install -r requirements.txt
```

**Common dependencies** (check `requirements.txt` for full list):
```txt
Flask==3.0.0
flask-cors==4.0.0
mysql-connector-python==8.2.0
requests==2.31.0
python-dotenv==1.0.0
pytz==2023.3
APScheduler==3.10.4
```

### Step 4: Verify Installation

```bash
# Verify Flask installation
flask --version

# Verify Python can import MySQL connector
python -c "import mysql.connector; print('MySQL connector OK')"

# List installed packages
pip list
```

---

## Configuration

### Step 1: Create Environment File

Create a `.env` file in the project root:

```bash
cp .env.example .env
```

If `.env.example` doesn't exist, create `.env` manually:

```bash
touch .env
```

### Step 2: Configure Environment Variables

Edit `.env` with your settings:

```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=3306
DB_NAME=theme_park_tracker
DB_USER=theme_park_user
DB_PASSWORD=your_password

# Queue-Times.com API
QUEUE_TIMES_API_KEY=your_api_key_here
QUEUE_TIMES_BASE_URL=https://queue-times.com/api/v1

# Data Collection Settings
COLLECTION_INTERVAL_MINUTES=10
COLLECTION_ENABLED=true

# Flask API Settings
FLASK_ENV=development
FLASK_DEBUG=true
FLASK_HOST=0.0.0.0
FLASK_PORT=5000

# Logging
LOG_LEVEL=DEBUG
LOG_FILE=logs/app.log

# Timezone
DEFAULT_TIMEZONE=America/New_York

# Data Retention
RAW_DATA_RETENTION_HOURS=24
```

**Security Note**: Never commit `.env` to version control. Ensure `.env` is in `.gitignore`.

### Step 3: Create Required Directories

```bash
# Create logs directory
mkdir -p logs

# Create data directory (if needed for exports)
mkdir -p data

# Verify structure
ls -la
```

### Step 4: Validate Configuration

```bash
# Test database connection
python scripts/test_db_connection.py

# Expected output:
# Database connection: SUCCESS
# Tables found: 14
```

If connection test script doesn't exist, create a quick test:

```bash
python -c "
import os
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

try:
    conn = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT', 3306)),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    print('Database connection: SUCCESS')
    cursor = conn.cursor()
    cursor.execute('SHOW TABLES')
    print(f'Tables found: {cursor.rowcount}')
    conn.close()
except Exception as e:
    print(f'Database connection: FAILED - {e}')
"
```

---

## Running the Ride Classification Script

**IMPORTANT**: Run this once before starting data collection to populate ride tier classifications (Tier 1/2/3) needed for weighted downtime scoring.

### What is Ride Classification?

The classification script assigns each ride to one of three tiers based on importance:
- **Tier 1 (3x weight)**: Major E-ticket attractions (Space Mountain, Rise of the Resistance, major coasters)
- **Tier 2 (2x weight)**: Standard attractions (regular coasters, dark rides, water rides)
- **Tier 3 (1x weight)**: Minor attractions (kiddie rides, carousels, shows)

This enables **fair park-to-park comparisons** using weighted downtime scores instead of raw hours.

### How Classification Works: 4-Tier Hierarchical System

The classification system balances accuracy and cost using a priority-based approach:

1. **Manual Overrides** (~100 rides, 2%): Human corrections in `data/manual_overrides.csv`
2. **Cached AI Decisions** (~500 rides, 10%): High-confidence AI classifications reused from `data/exact_matches.json`
3. **Pattern Matching** (~4,000 rides, 76%): Fast keyword rules (e.g., "kiddie" → Tier 3, "carousel" → Tier 3)
4. **AI Agent with Web Search** (~647 rides, 12%): Researches ambiguous rides using Gemini-2.5-pro

**Cost**: ~$19 initial run, ~$0.30/month incremental (only ambiguous rides need AI research)

### Step 1: Verify Classification Script Files

```bash
# Check if classification script exists
ls -l ride_classifier/

# Expected structure:
# ride_classifier/
#   __init__.py
#   config.py
#   api_client.py
#   pattern_matcher.py      # Keyword rules (Priority 3)
#   ai_classifier.py        # AI agent integration (Priority 4)
#   cache_manager.py        # Cached decisions handler (Priority 2)
#   manual_overrides.py     # CSV reader (Priority 1)
#   main.py                 # Orchestration
#   models.py               # Data structures
```

If the directory doesn't exist yet, it will be created during implementation.

### Step 2: Run Initial Classification

```bash
# Activate virtual environment (if not already active)
source venv/bin/activate

# Run classification for all North American parks
python -m ride_classifier.main --fetch-all

# Expected output:
# Fetching parks from Queue-Times.com API...
# Found 85 North American parks
# Classifying 5,247 rides...
#
# Priority 1: Checking manual overrides... 0 found
# Priority 2: Loading cached AI decisions... 0 found (first run)
# Priority 3: Pattern matching... 4,012 classified (76.5%)
# Priority 4: AI agent classification... 647 ambiguous rides
#   [Parallel processing with 10 workers]
#   Progress: 100% [========================================] 647/647
#   Duration: 4 min 32 sec
#
# Classification Summary:
#   Tier 1 (Major):      524 rides (10.0%) - VERIFY MANUALLY
#   Tier 2 (Standard):  2,099 rides (40.0%)
#   Tier 3 (Minor):     2,624 rides (50.0%)
#
# Confidence Scores:
#   High (> 0.85):      4,758 rides (90.7%) - cached for reuse
#   Medium (0.50-0.85):   245 rides (4.7%)
#   Low (< 0.50):         244 rides (4.6%) - REVIEW REQUIRED
#
# AI Research Cost: $19.41 (647 calls × $0.03)
#
# Output saved to:
#   - data/ride_classifications.csv
#   - data/exact_matches.json (558 cached AI decisions)
#   - data/classification_summary.json
```

**Processing Time**:
- **First run**: ~10 minutes (5 min API fetch + 5 min AI classification)
- **Subsequent runs**: ~30 seconds (reuses cached decisions)

### Step 3: Review Classifications

```bash
# View Tier 1 classifications for manual review
cat data/ride_classifications.csv | grep ",1," | head -20

# Example output:
# park_id,ride_id,park_name,ride_name,tier,confidence,method,reasoning,sources
# 16,1234,Disneyland,Space Mountain,1,1.00,cached_ai,"Iconic indoor coaster, flagship attraction","https://disney.com/..."
# 16,1235,Disneyland,Big Thunder Mountain,1,1.00,pattern_match,"Contains 'Thunder Mountain' keyword",""
# 57,2341,Cedar Point,Millennium Force,1,0.95,ai_agent,"310 ft giga coaster, world-renowned","https://rcdb.com/..."

# View low-confidence rides requiring human review
cat data/ride_classifications.csv | awk -F',' '$6 < 0.50 {print}' | head -20

# Example output:
# 42,567,Six Flags,Dragon,2,0.45,ai_agent,"Ambiguous name, classified as standard coaster","..."
```

**Action**:
1. **MUST review ALL Tier 1 rides** (~524 rides) - verify flagship attractions are correct
2. **MUST review all confidence < 0.50** (~244 rides) - add corrections to manual_overrides.csv
3. **SHOULD spot-check 5%** of Tier 2/3 (~250 rides) - random sample verification

### Step 4: Apply Manual Overrides (Optional)

If you find misclassifications:

```bash
# Edit manual overrides CSV
nano data/manual_overrides.csv

# Add entries (format: park_id,ride_id,override_tier,reason):
# 57,123,1,Actually a major coaster despite generic name
# 6,456,3,This is a minor flat ride not a coaster
```

Re-run with overrides:
```bash
python -m ride_classifier.main --apply-overrides
```

### Step 5: Import Classifications into Database

```bash
# Import classifications into rides and ride_classifications tables
python scripts/import_classifications.py

# Expected output:
# Importing 5,247 ride classifications...
# Updated rides.tier column: 5,247 rows
# Inserted into ride_classifications: 5,247 rows
# Import complete!
```

Verify in MySQL:
```sql
-- Check tier distribution
SELECT tier, COUNT(*) as count
FROM rides
WHERE tier IS NOT NULL
GROUP BY tier;

-- Expected result:
-- +------+-------+
-- | tier | count |
-- +------+-------+
-- |    1 |   524 |
-- |    2 |  2099 |
-- |    3 |  2624 |
-- +------+-------+
```

### Troubleshooting Classification

**Issue**: "Module 'ride_classifier' not found"
```bash
# Ensure you're in project root
pwd  # Should show .../ThemeParkHallOfShame

# Verify virtual environment is active
which python  # Should show venv/bin/python

# Check PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

**Issue**: "Low confidence scores (< 0.70) for many rides"
- This is expected for ~5% of rides with ambiguous names
- Review data/ride_classifications.csv filtered by confidence < 0.70
- Add manual overrides for important rides

**Issue**: "API rate limit exceeded"
```bash
# Increase delay between requests in ride_classifier/config.py
# RATE_LIMIT_DELAY = 1.0  # Change from 0.5 to 1.0 seconds
```

---

## Running the Data Collector

**Prerequisites**: Ride classifications must be imported (see previous section).

The data collector fetches ride status data from Queue-Times.com API every 10 minutes.

### Step 1: Test API Connection

```bash
# Test Queue-Times API access
python scripts/test_api_connection.py

# Expected output:
# Queue-Times API: SUCCESS
# Retrieved data for 85 parks
```

If test script doesn't exist, quick test:

```bash
python -c "
import os
from dotenv import load_dotenv
import requests

load_dotenv()

api_key = os.getenv('QUEUE_TIMES_API_KEY')
if not api_key:
    print('ERROR: QUEUE_TIMES_API_KEY not set in .env')
    exit(1)

try:
    response = requests.get(
        'https://queue-times.com/api/v1/parks',
        headers={'X-API-Key': api_key},
        timeout=10
    )
    if response.status_code == 200:
        parks = response.json()
        print(f'Queue-Times API: SUCCESS')
        print(f'Retrieved data for {len(parks)} parks')
    else:
        print(f'API returned status {response.status_code}')
except Exception as e:
    print(f'API connection: FAILED - {e}')
"
```

### Step 2: Run Data Collector (Development Mode)

```bash
# Activate virtual environment (if not already active)
source venv/bin/activate

# Run collector
python src/data_collector.py

# Expected output:
# [2025-11-22 14:00:00] INFO: Data collector started
# [2025-11-22 14:00:05] INFO: Fetching parks from Queue-Times API
# [2025-11-22 14:00:08] INFO: Retrieved 85 North American parks
# [2025-11-22 14:00:10] INFO: Processing Magic Kingdom (16)
# [2025-11-22 14:00:12] INFO: Recorded 45 ride snapshots for Magic Kingdom
# ...
# [2025-11-22 14:05:00] INFO: Collection cycle complete. Next run in 10 minutes.
```

### Step 3: Run in Background (Production-like)

```bash
# Using nohup
nohup python src/data_collector.py > logs/collector.log 2>&1 &

# Get process ID
echo $!

# View logs
tail -f logs/collector.log

# Stop collector
kill <process_id>
```

**Alternative**: Use `systemd` service (Linux) or `launchd` (macOS) for production deployment.

### Step 4: Verify Data Collection

```bash
# Check snapshots table
mysql -u theme_park_user -p theme_park_tracker -e "
SELECT COUNT(*) as snapshot_count, MAX(recorded_at) as last_snapshot
FROM ride_status_snapshots;
"

# Expected output (after first collection):
# +----------------+---------------------+
# | snapshot_count | last_snapshot       |
# +----------------+---------------------+
# |           4250 | 2025-11-22 14:10:00 |
# +----------------+---------------------+

# Check park activity
mysql -u theme_park_user -p theme_park_tracker -e "
SELECT p.name, pas.rides_open, pas.rides_closed, pas.recorded_at
FROM park_activity_snapshots pas
JOIN parks p ON pas.park_id = p.park_id
ORDER BY pas.recorded_at DESC
LIMIT 10;
"
```

---

## Running the Flask API

The Flask API serves downtime rankings, ride performance data, and wait times to the frontend.

### Step 1: Start Flask Development Server

```bash
# Activate virtual environment
source venv/bin/activate

# Run Flask app
python src/api/app.py

# Expected output:
# * Serving Flask app 'app'
# * Debug mode: on
# * Running on http://0.0.0.0:5000
# * Press CTRL+C to quit
```

**Alternative**: Use Flask CLI:
```bash
export FLASK_APP=src/api/app.py
export FLASK_ENV=development
flask run --host=0.0.0.0 --port=5000
```

### Step 2: Verify API is Running

Open browser or use curl:
```bash
curl http://localhost:5000/v1/health
```

**Expected response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-11-22T14:15:00Z",
  "version": "1.0.0",
  "checks": {
    "database": {
      "status": "healthy",
      "response_time_ms": 12
    },
    "data_collection": {
      "status": "healthy",
      "last_collection": "2025-11-22T14:10:00Z",
      "minutes_since_last_collection": 5
    },
    "api": {
      "status": "healthy",
      "uptime_seconds": 300
    }
  }
}
```

### Step 3: Enable Hot Reload (Development)

Flask should auto-reload on code changes when `FLASK_DEBUG=true`. Test by editing a file:

```bash
# Edit a Python file
echo "# Test change" >> src/api/app.py

# Flask should show:
# * Detected change in 'src/api/app.py', reloading
```

### Step 4: Run with Production Server (Optional)

For production-like testing, use Gunicorn:

```bash
# Install Gunicorn
pip install gunicorn

# Run with 4 workers
gunicorn -w 4 -b 0.0.0.0:5000 src.api.app:app

# Or with auto-reload for development
gunicorn -w 1 -b 0.0.0.0:5000 --reload src.api.app:app
```

---

## Testing with Sample API Calls

### Health Check

```bash
curl -X GET http://localhost:5000/v1/health | jq
```

### Get Park Downtime Rankings (Today)

```bash
curl -X GET "http://localhost:5000/v1/parks/downtime?period=today&filter=all-parks&limit=10" | jq
```

**Expected response:**
```json
{
  "success": true,
  "period": "today",
  "filter": "all-parks",
  "aggregate_stats": {
    "total_parks_tracked": 85,
    "peak_downtime_hours": 12.5,
    "currently_down_rides": 23
  },
  "data": [
    {
      "rank": 1,
      "park_id": 16,
      "park_name": "Magic Kingdom",
      "location": "Orlando, FL",
      "total_downtime_hours": 12.5,
      "affected_rides_count": 5,
      "uptime_percentage": 89.2,
      "trend_percentage": 15.3,
      "queue_times_url": "https://queue-times.com/parks/16"
    }
  ]
}
```

### Get Park Downtime Rankings (7 Days, Disney & Universal Only)

```bash
curl -X GET "http://localhost:5000/v1/parks/downtime?period=7days&filter=disney-universal&limit=20" | jq
```

### Get Park Details

```bash
curl -X GET "http://localhost:5000/v1/parks/16/details?period=today" | jq
```

### Get Ride Downtime Rankings

```bash
curl -X GET "http://localhost:5000/v1/rides/downtime?period=7days&filter=all-parks&limit=20" | jq
```

### Get Current Wait Times

```bash
curl -X GET "http://localhost:5000/v1/rides/waittimes?mode=live&filter=all-parks&limit=50" | jq
```

### Get 7-Day Average Wait Times

```bash
curl -X GET "http://localhost:5000/v1/rides/waittimes?mode=7day-average&filter=disney-universal&limit=50" | jq
```

### Error Handling Test (Invalid Parameter)

```bash
curl -X GET "http://localhost:5000/v1/parks/downtime?period=invalid" | jq

# Expected error response:
{
  "success": false,
  "error": {
    "code": "INVALID_PARAMETER",
    "message": "Invalid period parameter. Must be one of: today, 7days, 30days",
    "details": {
      "parameter": "period",
      "provided_value": "invalid"
    }
  }
}
```

### Postman Collection

Import the OpenAPI spec into Postman for interactive testing:

1. Open Postman
2. Click **Import** > **File**
3. Select `specs/001-theme-park-tracker/contracts/api.yaml`
4. Postman will auto-generate requests from the OpenAPI spec
5. Update base URL to `http://localhost:5000/v1`
6. Start testing!

---

## Troubleshooting

### Database Connection Errors

**Problem**: `mysql.connector.errors.ProgrammingError: 1045 (28000): Access denied`

**Solution**:
```bash
# Verify credentials in .env
cat .env | grep DB_

# Test MySQL login manually
mysql -u theme_park_user -p

# Reset password if needed
mysql -u root -p -e "
ALTER USER 'theme_park_user'@'localhost' IDENTIFIED BY 'new_password';
FLUSH PRIVILEGES;
"
```

---

**Problem**: `mysql.connector.errors.DatabaseError: 2003 (HY000): Can't connect to MySQL server`

**Solution**:
```bash
# Check if MySQL is running
sudo systemctl status mysql  # Linux
brew services list | grep mysql  # macOS

# Start MySQL if stopped
sudo systemctl start mysql  # Linux
brew services start mysql@8.0  # macOS

# Verify MySQL port
sudo lsof -i :3306
```

---

### API Key Issues

**Problem**: `401 Unauthorized` from Queue-Times API

**Solution**:
```bash
# Verify API key is set
echo $QUEUE_TIMES_API_KEY

# Test API manually
curl -H "X-API-Key: your_api_key" https://queue-times.com/api/v1/parks

# If key is invalid, get new key at:
# https://queue-times.com/en-US/pages/api
```

---

### Python Import Errors

**Problem**: `ModuleNotFoundError: No module named 'flask'`

**Solution**:
```bash
# Ensure virtual environment is activated
source venv/bin/activate

# Verify pip is using venv
which pip  # Should show path inside venv/

# Reinstall dependencies
pip install -r requirements.txt
```

---

### Flask Port Already in Use

**Problem**: `OSError: [Errno 48] Address already in use`

**Solution**:
```bash
# Find process using port 5000
lsof -i :5000

# Kill the process
kill -9 <PID>

# Or use different port
export FLASK_PORT=5001
python src/api/app.py
```

---

### No Data in Database

**Problem**: API returns empty arrays `"data": []`

**Solution**:
```bash
# Check if data collector is running
ps aux | grep data_collector

# Verify snapshots exist
mysql -u theme_park_user -p theme_park_tracker -e "
SELECT COUNT(*) FROM ride_status_snapshots;
"

# If 0, run data collector manually once
python src/data_collector.py

# Check aggregation job ran
mysql -u theme_park_user -p theme_park_tracker -e "
SELECT COUNT(*) FROM ride_daily_stats WHERE stat_date = CURDATE();
"

# If 0, run aggregation manually
python src/jobs/aggregate_daily_stats.py
```

---

### Slow Query Performance

**Problem**: API responses take >500ms

**Solution**:
```bash
# Enable query logging
mysql -u root -p -e "
SET GLOBAL slow_query_log = 'ON';
SET GLOBAL long_query_time = 0.1;  # Log queries >100ms
SET GLOBAL slow_query_log_file = '/var/log/mysql/slow.log';
"

# Test query and check EXPLAIN
mysql -u theme_park_user -p theme_park_tracker -e "
EXPLAIN SELECT * FROM park_daily_stats WHERE stat_date = CURDATE() ORDER BY total_downtime_hours DESC LIMIT 50;
"

# Verify indexes exist
mysql -u theme_park_user -p theme_park_tracker -e "
SHOW INDEX FROM park_daily_stats;
"

# Add missing indexes if needed (refer to data-model.md)
```

---

### Logs Not Working

**Problem**: No logs generated in `logs/` directory

**Solution**:
```bash
# Verify logs directory exists and is writable
ls -la logs/
chmod 755 logs/

# Check LOG_FILE path in .env
cat .env | grep LOG_FILE

# Test logging manually
python -c "
import logging
logging.basicConfig(filename='logs/test.log', level=logging.INFO)
logging.info('Test log entry')
"

# Check log file created
cat logs/test.log
```

---

## Production Deployment

Once local development is complete and all tests pass, deploy to the webperformance.com server.

### Prerequisites

- SSH access to webperformance.com server
- Sudo privileges on the server
- Git repository set up (GitHub, GitLab, or Bitbucket)

### Step 1: Assess Server Resources

```bash
# SSH to the server
ssh user@webperformance.com

# Check current resource utilization
top  # Press 'q' to quit
df -h  # Check disk space
free -h  # Check memory
mysql --version  # Check MySQL version
python3 --version  # Check Python version
apache2 -v  # Check Apache version
```

### Step 2: Install Dependencies on Server

```bash
# Install Python 3.11 if not present
sudo apt update
sudo apt install python3.11 python3.11-venv python3.11-dev

# Install MySQL 8.0 if not present
sudo apt install mysql-server mysql-client

# Install Apache mod_wsgi for Python 3.11
sudo apt install libapache2-mod-wsgi-py3

# Install git if not present
sudo apt install git
```

### Step 3: Deploy Code to Server

**Option A: Git Clone (Recommended)**
```bash
# Create deployment directory
sudo mkdir -p /var/www/themeparkwaits
sudo chown $USER:$USER /var/www/themeparkwaits

# Clone repository
cd /var/www/themeparkwaits
git clone https://github.com/yourusername/themepark-tracker.git .

# Pull updates later with
git pull origin main
```

**Option B: rsync (Alternative)**
```bash
# From your local machine, sync files to server
rsync -avz --exclude 'venv' --exclude '.git' --exclude '*.pyc' \
  ~/Projects/ThemeParkHallOfShame/ \
  user@webperformance.com:/var/www/themeparkwaits/
```

### Step 4: Set Up Production Python Environment

```bash
# On the server
cd /var/www/themeparkwaits/backend

# Create virtual environment
python3.11 -m venv venv

# Activate and install dependencies
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 5: Create Production Database

```bash
# Create production database and user
sudo mysql -u root -p

CREATE DATABASE themepark_tracker_prod;
CREATE USER 'themepark_prod'@'localhost' IDENTIFIED BY 'SECURE_PASSWORD_HERE';
GRANT ALL PRIVILEGES ON themepark_tracker_prod.* TO 'themepark_prod'@'localhost';
FLUSH PRIVILEGES;
EXIT;
```

### Step 6: Run Database Migrations

```bash
cd /var/www/themeparkwaits/backend

# Run migrations
mysql -u themepark_prod -p themepark_tracker_prod < src/database/migrations/001_initial_schema.sql
mysql -u themepark_prod -p themepark_tracker_prod < src/database/migrations/002_raw_data_tables.sql
mysql -u themepark_prod -p themepark_tracker_prod < src/database/migrations/003_aggregates_tables.sql
mysql -u themepark_prod -p themepark_tracker_prod < src/database/migrations/004_indexes.sql
mysql -u themepark_prod -p themepark_tracker_prod < src/database/migrations/005_cleanup_events.sql

# Verify tables created
mysql -u themepark_prod -p themepark_tracker_prod -e "SHOW TABLES;"
```

### Step 7: Configure Production Environment

```bash
# Create production .env file
cd /var/www/themeparkwaits/backend
vi .env

# Add production configuration:
ENVIRONMENT=production
DB_HOST=localhost
DB_NAME=themepark_tracker_prod
DB_USER=themepark_prod
DB_PASSWORD=SECURE_PASSWORD_HERE
DB_PORT=3306
QUEUE_TIMES_API_BASE_URL=https://queue-times.com/api
QUEUE_TIMES_API_KEY=your_production_api_key_here
FLASK_ENV=production
FLASK_DEBUG=False
LOG_LEVEL=INFO
```

### Step 8: Configure systemd Services

**Data Collector Service:**
```bash
# Create collector service with resource limits
sudo vi /etc/systemd/system/themepark-collector.service
```

```ini
[Unit]
Description=Theme Park Downtime Tracker - Data Collector
After=network.target mysql.service

[Service]
Type=simple
User=www-data
Group=www-data
WorkingDirectory=/var/www/themeparkwaits/backend
Environment="PATH=/var/www/themeparkwaits/backend/venv/bin"
ExecStart=/var/www/themeparkwaits/backend/venv/bin/python src/scripts/collect.py

# Resource Limits (prevent contention with webperformance.com)
CPUQuota=25%
MemoryMax=512M
IOWeight=50

Restart=on-failure
RestartSec=30s

[Install]
WantedBy=multi-user.target
```

**API Service (if running as daemon - alternative to Apache mod_wsgi):**
```bash
sudo vi /etc/systemd/system/themepark-api.service
```

```ini
[Unit]
Description=Theme Park Downtime Tracker - Flask API
After=network.target mysql.service

[Service]
Type=simple
User=www-data
Group=www-data
WorkingDirectory=/var/www/themeparkwaits/backend
Environment="PATH=/var/www/themeparkwaits/backend/venv/bin"
ExecStart=/var/www/themeparkwaits/backend/venv/bin/gunicorn -w 2 -b 127.0.0.1:5001 src.api.app:app

# Resource Limits
CPUQuota=30%
MemoryMax=512M

Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
```

**Enable and start services:**
```bash
sudo systemctl daemon-reload
sudo systemctl enable themepark-collector
sudo systemctl start themepark-collector
sudo systemctl status themepark-collector

# Optional: Only if using separate API service (not Apache mod_wsgi)
# sudo systemctl enable themepark-api
# sudo systemctl start themepark-api
```

### Step 9: Configure Apache VirtualHost

```bash
# Create Apache VirtualHost configuration
sudo vi /etc/apache2/sites-available/themeparkwaits.conf
```

```apache
<VirtualHost *:80>
    ServerName api.themeparkwaits.com
    ServerAdmin admin@themeparkwaits.com

    DocumentRoot /var/www/themeparkwaits/frontend

    # WSGI Configuration for Flask API
    WSGIDaemonProcess themeparkwaits user=www-data group=www-data threads=5 \
        python-home=/var/www/themeparkwaits/backend/venv \
        python-path=/var/www/themeparkwaits/backend
    WSGIScriptAlias /api /var/www/themeparkwaits/backend/src/api/wsgi.py

    <Directory /var/www/themeparkwaits/backend/src/api>
        WSGIProcessGroup themeparkwaits
        WSGIApplicationGroup %{GLOBAL}
        Require all granted
    </Directory>

    # Frontend static files
    <Directory /var/www/themeparkwaits/frontend>
        Options -Indexes +FollowSymLinks
        AllowOverride None
        Require all granted
    </Directory>

    # Logs
    ErrorLog ${APACHE_LOG_DIR}/themeparkwaits_error.log
    CustomLog ${APACHE_LOG_DIR}/themeparkwaits_access.log combined
</VirtualHost>
```

**Enable site and restart Apache:**
```bash
sudo a2ensite themeparkwaits
sudo a2enmod wsgi  # Enable mod_wsgi if not already enabled
sudo systemctl restart apache2
```

### Step 10: Set Up Cron Jobs

```bash
# Edit crontab for www-data user
sudo crontab -e -u www-data

# Add aggregation and cleanup jobs
# Run daily aggregation at 12:10 AM (with retries at 1:10 AM, 2:10 AM)
10 0 * * * flock -n /tmp/aggregate.lock /var/www/themeparkwaits/backend/venv/bin/python /var/www/themeparkwaits/backend/src/scripts/aggregate_daily.py >> /var/log/themepark/aggregate.log 2>&1
10 1 * * * flock -n /tmp/aggregate.lock /var/www/themeparkwaits/backend/venv/bin/python /var/www/themeparkwaits/backend/src/scripts/aggregate_daily.py >> /var/log/themepark/aggregate.log 2>&1
10 2 * * * flock -n /tmp/aggregate.lock /var/www/themeparkwaits/backend/venv/bin/python /var/www/themeparkwaits/backend/src/scripts/aggregate_daily.py >> /var/log/themepark/aggregate.log 2>&1

# Create log directory
sudo mkdir -p /var/log/themepark
sudo chown www-data:www-data /var/log/themepark
```

### Step 11: Set Up Monitoring

**CloudWatch Alarms (via AWS CLI or Console):**
```bash
# Install CloudWatch agent if not present
wget https://s3.amazonaws.com/amazoncloudwatch-agent/linux/amd64/latest/amazon-cloudwatch-agent.deb
sudo dpkg -i amazon-cloudwatch-agent.deb

# Configure alarms for:
# - CPU > 60% sustained
# - Memory > 75%
# - Disk > 80%
# - No data collection in 15 minutes
```

### Step 12: Test Production Deployment

```bash
# Test API health endpoint
curl http://localhost/api/health

# Test data collection
sudo journalctl -u themepark-collector -f

# Check database has data
mysql -u themepark_prod -p themepark_tracker_prod \
  -e "SELECT COUNT(*) FROM ride_status_snapshots WHERE recorded_at > NOW() - INTERVAL 1 HOUR;"

# Test frontend
curl http://api.themeparkwaits.com
```

### Step 13: SSL Certificate (Let's Encrypt)

```bash
# Install certbot
sudo apt install certbot python3-certbot-apache

# Obtain SSL certificate
sudo certbot --apache -d api.themeparkwaits.com

# Auto-renewal is configured automatically
# Test renewal with:
sudo certbot renew --dry-run
```

---

## Development Workflow

### Daily Development

1. **Start MySQL** (if not auto-started):
   ```bash
   sudo systemctl start mysql  # Linux
   brew services start mysql@8.0  # macOS
   ```

2. **Activate virtual environment**:
   ```bash
   cd ~/Projects/ThemeParkHallOfShame
   source venv/bin/activate
   ```

3. **Start data collector** (separate terminal):
   ```bash
   python src/data_collector.py
   ```

4. **Start Flask API** (separate terminal):
   ```bash
   python src/api/app.py
   ```

5. **Make code changes** and test with curl/Postman

6. **Run tests** (if test suite exists):
   ```bash
   pytest tests/
   ```

7. **Commit changes**:
   ```bash
   git add .
   git commit -m "Add feature XYZ"
   git push origin feature/xyz
   ```

### Database Migrations

When schema changes are needed:

1. **Edit schema**:
   ```bash
   vim db/schema.sql
   ```

2. **Test migration locally**:
   ```bash
   # Backup database first
   mysqldump -u theme_park_user -p theme_park_tracker > backup.sql

   # Drop and recreate (dev only!)
   mysql -u root -p -e "DROP DATABASE theme_park_tracker;"
   mysql -u root -p -e "CREATE DATABASE theme_park_tracker CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

   # Run new schema
   mysql -u theme_park_user -p theme_park_tracker < db/schema.sql

   # Verify
   mysql -u theme_park_user -p theme_park_tracker -e "SHOW TABLES;"
   ```

3. **For production**: Create migration script in `db/migrations/`:
   ```sql
   -- db/migrations/002_add_ride_category.sql
   ALTER TABLE rides ADD COLUMN category VARCHAR(50) DEFAULT NULL AFTER land_area;
   ```

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-cov

# Run all tests
pytest

# Run with coverage
pytest --cov=src tests/

# Run specific test file
pytest tests/test_api.py

# Run specific test
pytest tests/test_api.py::test_health_check
```

### Code Quality

```bash
# Install linters
pip install flake8 black pylint

# Format code with Black
black src/

# Check style with flake8
flake8 src/

# Run pylint
pylint src/
```

---

## Next Steps

After completing this quickstart:

1. **Review API Documentation**: Read `contracts/api.yaml` for full endpoint details
2. **Understand Data Model**: Study `data-model.md` for database schema
3. **Implement Frontend**: Build UI consuming the API endpoints
4. **Set Up Monitoring**: Configure logging and alerting for production
5. **Deploy to Production**: Follow deployment guide (separate document)

---

## Additional Resources

- **Queue-Times API Docs**: https://queue-times.com/en-US/pages/api
- **Flask Documentation**: https://flask.palletsprojects.com/
- **MySQL Documentation**: https://dev.mysql.com/doc/
- **OpenAPI Specification**: https://swagger.io/specification/

---

**End of Quickstart Guide**
