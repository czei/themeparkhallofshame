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

## Monitoring

Check logs for errors:
```bash
# View recent collection activity
tail -f logs/collection.log

# Check for errors
grep ERROR logs/collection.log

# View aggregation results
tail -f logs/aggregation.log
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
