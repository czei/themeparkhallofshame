# Data Collection Scripts

This directory contains the Phase 3 data collection scripts that populate and maintain the Theme Park Downtime Tracker database.

## Overview

The data collection system consists of three main scripts:

1. **collect_parks.py** - One-time setup to populate parks and rides
2. **collect_snapshots.py** - Continuous collection of wait time snapshots (every 10 minutes)
3. **aggregate_daily.py** - Daily aggregation of statistics (once per day)

## Prerequisites

- MySQL database set up and running (see `deployment/scripts/setup-database.sh`)
- Python dependencies installed (`pip install -r requirements.txt`)
- `.env` file configured with database credentials

## Geographic Filter (Testing Phase)

Currently configured to collect **US parks only** via the `FILTER_COUNTRY=US` setting in `.env`.

To expand to all countries in production:
```bash
# In backend/.env
FILTER_COUNTRY=
```

## Script 1: collect_parks.py

**Purpose:** Fetch all parks and rides from Queue-Times.com API and populate the database.

**When to run:**
- Once during initial setup
- When new parks/rides are added (monthly/quarterly)

**Usage:**
```bash
cd /Users/czei/Projects/ThemeParkHallOfShame/backend

# Initial setup (first time)
python -m scripts.collect_parks

# Force refresh (clear and re-fetch everything)
python -m scripts.collect_parks --force
```

**What it does:**
1. Fetches all parks from Queue-Times.com
2. Filters to US parks only (testing phase)
3. Inserts/updates parks in database
4. Fetches all rides for each park
5. Classifies rides into 4 tiers using PatternMatcher
6. Stores rides and classifications in database

**Output:**
```
============================================================
PARK & RIDE COLLECTION - Starting
============================================================
Geographic Filter: US only
Step 1: Fetching parks from Queue-Times.com...
Found 87 parks from API
Filtered to 45 parks in US
Step 2: Processing parks and rides...
Processing park: Magic Kingdom (ID: 16)
  ✓ Inserted park: Magic Kingdom
  Found 42 rides
    ✓ Space Mountain → Tier 1 🌟 (confidence: 0.75)
    ✓ Pirates of the Caribbean → Tier 2 🌟🌟 (confidence: 0.60)
    ...
============================================================
COLLECTION SUMMARY
============================================================
Parks:
  - Processed: 45
  - Inserted:  45
  - Updated:   0
  - Skipped:   0

Rides:
  - Processed:  1,847
  - Inserted:   1,847
  - Updated:    0
  - Classified: 1,847

Errors: 0
============================================================
```

## Script 2: collect_snapshots.py

**Purpose:** Collect current wait times for all rides every 10 minutes.

**When to run:** Every 10 minutes via cron or scheduler

**Usage:**
```bash
cd /Users/czei/Projects/ThemeParkHallOfShame/backend

# Manual run
python -m scripts.collect_snapshots

# Cron setup (every 10 minutes)
crontab -e
# Add this line:
*/10 * * * * cd /Users/czei/Projects/ThemeParkHallOfShame/backend && python -m scripts.collect_snapshots >> logs/collection.log 2>&1
```

**What it does:**
1. Fetches current wait times for all parks
2. Stores ride status snapshots (wait time, open/closed)
3. Stores park activity snapshots (how many rides are active)
4. Detects status changes (ride went from open → closed or closed → open)
5. Records downtime durations

**Output:**
```
============================================================
SNAPSHOT COLLECTION - 2025-11-23 14:30:00
============================================================
Processing 45 active parks...
Processing: Magic Kingdom
  ✓ Processed 42 rides (38 active, park appears open)
Processing: EPCOT
  ⚠ Status change detected for ride 127: OPEN → CLOSED
  ✓ Processed 35 rides (31 active, park appears open)
...

Parks processed:     45
Rides processed:     1,847
Snapshots created:   1,847
Status changes:      12
Errors:              0
============================================================
```

## Script 3: aggregate_daily.py

**Purpose:** Calculate daily statistics from raw snapshots.

**When to run:** Once per day, typically at 1 AM

**Usage:**
```bash
cd /Users/czei/Projects/ThemeParkHallOfShame/backend

# Aggregate yesterday's data (default)
python -m scripts.aggregate_daily

# Aggregate specific date
python -m scripts.aggregate_daily --date 2025-11-22

# Cron setup (daily at 1 AM)
crontab -e
# Add this line:
0 1 * * * cd /Users/czei/Projects/ThemeParkHallOfShame/backend && python -m scripts.aggregate_daily >> logs/aggregation.log 2>&1
```

**What it does:**
1. Calculates ride-level statistics (uptime %, downtime minutes, wait times)
2. Calculates park-level statistics (average uptime, total downtime hours)
3. Stores results in permanent aggregate tables
4. Creates aggregation log entry for tracking

**Output:**
```
============================================================
DAILY AGGREGATION - 2025-11-22
============================================================
Step 1: Aggregating ride statistics...
  ✓ Aggregated 1,847 rides
Step 2: Aggregating park statistics...
  ✓ Aggregated 45 parks

============================================================
AGGREGATION SUMMARY
============================================================
Date:             2025-11-22
Parks processed:  45
Rides processed:  1,847
Errors:           0
============================================================
```

## Data Flow

```
Queue-Times.com API
        ↓
1. collect_parks.py (one-time)
        ↓
   parks, rides tables
        ↓
2. collect_snapshots.py (every 10 min)
        ↓
   ride_status_snapshots (24h retention)
   park_activity_snapshots (24h retention)
   ride_status_changes (24h retention)
        ↓
3. aggregate_daily.py (daily)
        ↓
   ride_daily_stats (permanent)
   park_daily_stats (permanent)
        ↓
    Flask API
        ↓
  Frontend Dashboard
```

## Initial Setup Workflow

1. **Set up database:**
   ```bash
   cd /Users/czei/Projects/ThemeParkHallOfShame/deployment/scripts
   ./setup-database.sh
   ```

2. **Collect parks and rides (one-time):**
   ```bash
   cd /Users/czei/Projects/ThemeParkHallOfShame/backend
   python -m scripts.collect_parks
   ```

3. **Start snapshot collection (continuous):**
   ```bash
   # Set up cron job to run every 10 minutes
   crontab -e
   # Add: */10 * * * * cd /path/to/backend && python -m scripts.collect_snapshots
   ```

4. **Wait 24 hours** for data to accumulate

5. **Run first aggregation:**
   ```bash
   python -m scripts.aggregate_daily
   ```

6. **Set up daily aggregation (cron):**
   ```bash
   # Add: 0 1 * * * cd /path/to/backend && python -m scripts.aggregate_daily
   ```

## Testing During Development

For initial testing without waiting 24 hours:

1. Run `collect_parks.py` to populate database
2. Run `collect_snapshots.py` manually 3-4 times (waiting 2-3 minutes between runs)
3. Check database for snapshot data:
   ```sql
   SELECT COUNT(*) FROM ride_status_snapshots;
   SELECT * FROM ride_status_changes LIMIT 10;
   ```
4. Run `aggregate_daily.py --date 2025-11-23` to test aggregation

## Database Tables Used

**Populated by collect_parks.py:**
- `parks` - Theme park information
- `rides` - Ride information
- `ride_classifications` - Tier classifications

**Populated by collect_snapshots.py:**
- `ride_status_snapshots` - Wait time snapshots (24h retention)
- `park_activity_snapshots` - Park activity tracking (24h retention)
- `ride_status_changes` - Status transitions (24h retention)

**Populated by aggregate_daily.py:**
- `ride_daily_stats` - Ride statistics (permanent)
- `park_daily_stats` - Park statistics (permanent)
- `aggregation_log` - Aggregation tracking (permanent)

## Monitoring & Alerts

### Hourly Data Collection Health Check (`check_data_collection.py`) ⚠️ NEW

**Purpose:** Immediate alerting when data collection stops.

**Background:** On 2025-12-04, we lost a full day of data because `collect_snapshots.py` stopped running and we didn't discover it until the next morning. This script provides early warning within 1 hour.

**Schedule:** Every hour on the hour via cron

**What it checks:**
- ✅ Recent ride status snapshots (within last 30 minutes)
- ✅ Recent park activity snapshots (within last 30 minutes)
- ✅ Minimum number of parks reporting data (≥5)
- ✅ Minimum number of rides reporting data (≥50)
- ✅ Whether `collect_snapshots` process is running

**Alert trigger:** If no recent snapshots OR insufficient data

**Email alert includes:**
- Current status table with timestamps
- Process status (running/stopped)
- Last 30 lines of collection log
- Recommended troubleshooting steps

**Usage:**
```bash
# Test locally
cd /Users/czei/Projects/ThemeParkHallOfShame/backend
python -m src.scripts.check_data_collection

# Cron job (already configured in deployment/config/crontab.prod)
0 * * * * cd /opt/themeparkhallofshame/backend && source .env && /opt/themeparkhallofshame/venv/bin/python -m src.scripts.check_data_collection >> /opt/themeparkhallofshame/logs/data_collection_check.log 2>&1
```

**Thresholds (adjustable):**
```python
MAX_DATA_AGE_MINUTES = 30    # Alert if no data in last 30 minutes
MIN_PARKS_EXPECTED = 5       # Alert if fewer than 5 parks reporting
MIN_RIDES_EXPECTED = 50      # Alert if fewer than 50 rides reporting
```

### Daily Data Quality Alert (`send_data_quality_alert.py`)

**Purpose:** Report stale data from external APIs and validate awards data.

**Schedule:** Daily at 8:00 AM Pacific (16:00 UTC)

**What it checks:**
- Stale data from ThemeParks.wiki (data older than 24 hours)
- Awards validation (downtime/wait time bounds)
- Routing bugs (all-zero wait times)

**Alert trigger:** If ≥5 stale data issues OR critical awards issues

**Usage:**
```bash
# Test locally
python -m src.scripts.send_data_quality_alert

# Cron job (already configured)
0 16 * * * cd /opt/themeparkhallofshame/backend && source .env && /opt/themeparkhallofshame/venv/bin/python -m src.scripts.send_data_quality_alert >> /opt/themeparkhallofshame/logs/data_quality_alert.log 2>&1
```

### Check logs for errors

```bash
# View recent collection activity
tail -f logs/collection.log

# Check for errors
grep ERROR logs/collection.log

# View aggregation results
tail -f logs/aggregation.log

# View data collection health checks
tail -f /opt/themeparkhallofshame/logs/data_collection_check.log

# View data quality alerts
tail -f /opt/themeparkhallofshame/logs/data_quality_alert.log
```

Check database status:
```sql
-- How many parks/rides?
SELECT COUNT(*) FROM parks;
SELECT COUNT(*) FROM rides;

-- Recent snapshots?
SELECT COUNT(*) FROM ride_status_snapshots WHERE DATE(recorded_at) = CURDATE();

-- Recent status changes?
SELECT * FROM ride_status_changes ORDER BY changed_at DESC LIMIT 10;

-- Aggregation status?
SELECT * FROM aggregation_log ORDER BY started_at DESC LIMIT 5;
```

## Troubleshooting

### Data collection stopped (received alert from `check_data_collection.py`)

If you receive a CRITICAL alert that data collection has stopped:

1. **SSH to production:**
   ```bash
   ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com
   ```

2. **Check if cron job exists:**
   ```bash
   crontab -l | grep collect_snapshots
   ```

3. **Check recent logs:**
   ```bash
   tail -100 /opt/themeparkhallofshame/logs/collect_snapshots.log
   ```

4. **Check for errors in system log:**
   ```bash
   journalctl -u cron -n 50
   ```

5. **Manually run collection to test:**
   ```bash
   cd /opt/themeparkhallofshame/backend
   source .env
   /opt/themeparkhallofshame/venv/bin/python -m src.scripts.collect_snapshots
   ```

6. **Check disk space:**
   ```bash
   df -h
   ```

7. **Check database connectivity:**
   ```bash
   mysql -u admin -p themepark_tracker -e "SELECT 1;"
   ```

### False positives during maintenance

If you're getting alerts during expected downtime (e.g., maintenance windows):

- Temporarily disable the hourly check: `crontab -e` and comment out the line
- Or adjust `MAX_DATA_AGE_MINUTES` threshold in `check_data_collection.py`

### Other issues

**No parks collected:**
- Check `FILTER_COUNTRY` setting in `.env`
- Verify Queue-Times.com API is accessible
- Check logs for API errors

**No snapshots being created:**
- Verify `collect_parks.py` ran successfully first
- Check that rides exist in database
- Verify API connectivity

**Aggregation fails:**
- Ensure snapshots exist for the target date
- Check that parks/rides are marked as `is_active=1`
- Review aggregation_log table for error messages

## Production Deployment

When moving to production:

1. Update `.env` to remove geographic filter:
   ```bash
   FILTER_COUNTRY=
   ```

2. Run `collect_parks.py --force` to collect all parks worldwide

3. Set up production cron jobs on EC2 instance

4. Monitor CloudWatch logs for errors

5. Set up alerts for failed aggregations
