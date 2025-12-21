# Weather Collection Cron Job Setup

## Overview

Weather data is collected every 10 minutes, synchronized with park data collection.

## Production Cron Job

The weather collection cron job is defined in `deployment/config/crontab.prod`:

```bash
# Collect weather observations every 10 minutes (synchronized with park data)
# This captures current weather conditions from Open-Meteo API
# Wrapped with cron_wrapper for failure alerts (timeout: 5 minutes)
*/10 * * * * cd /opt/themeparkhallofshame/backend && source .env && /opt/themeparkhallofshame/venv/bin/python -m src.scripts.cron_wrapper collect_weather --timeout=300 >> /opt/themeparkhallofshame/logs/collect_weather.log 2>&1
```

## How It Works

1. **Schedule**: Runs every 10 minutes (`*/10 * * * *`)
2. **Script**: `src/scripts/collect_weather.py`
   - Defaults to `--current` mode (collects current weather for all parks)
   - Uses OpenMeteo API with rate limiting (1 req/sec)
   - Concurrent execution with 10 workers
   - Graceful error handling
3. **Wrapper**: Uses `cron_wrapper.py` for failure monitoring
   - 5-minute timeout (300 seconds)
   - Sends email alert on failure
   - Logs to `/opt/themeparkhallofshame/logs/collect_weather.log`

## Installation

### Production

1. Ensure `.env` file has `PYTHONPATH` set:
   ```bash
   PYTHONPATH=/opt/themeparkhallofshame/backend/src
   ```

2. Install the crontab:
   ```bash
   crontab /opt/themeparkhallofshame/deployment/config/crontab.prod
   ```

3. Verify installation:
   ```bash
   crontab -l | grep collect_weather
   ```

### Development (Local Testing)

For local testing, run the script manually:

```bash
# Test mode (5 parks only)
PYTHONPATH=backend/src python3 backend/src/scripts/collect_weather.py --test

# Full collection
PYTHONPATH=backend/src python3 backend/src/scripts/collect_weather.py
```

**Note**: Local cron is NOT recommended for development. Use manual runs or launchd on macOS.

## Monitoring

### Check Logs

```bash
# Real-time log viewing
tail -f /opt/themeparkhallofshame/logs/collect_weather.log

# Check for errors
grep -i error /opt/themeparkhallofshame/logs/collect_weather.log
```

### Verify Data Collection

```bash
# Check latest weather observations
mysql -u themepark_app -p themepark_tracker -e "
  SELECT park_id, observation_time, temperature_f, precipitation_probability
  FROM weather_observations
  ORDER BY observation_time DESC
  LIMIT 10;
"
```

### Email Alerts

On failure, the cron_wrapper will send an email alert with:
- Exit code and error message
- Last 50 lines of output
- Debugging instructions
- Recommended actions

## Troubleshooting

### Cron Job Not Running

1. Check cron service status:
   ```bash
   systemctl status crond
   ```

2. Check system logs:
   ```bash
   journalctl -u crond -n 100
   ```

### Data Not Updating

1. Check if script is running:
   ```bash
   ps aux | grep collect_weather
   ```

2. Check database connection:
   ```bash
   mysql -u themepark_app -p themepark_tracker -e "SELECT 1;"
   ```

3. Check API connectivity:
   ```bash
   curl -I https://api.open-meteo.com/v1/forecast
   ```

### Rate Limiting Issues

If the API is being rate limited:
- The script enforces 1 req/sec (60 req/min)
- Open-Meteo has no strict rate limits for non-commercial use
- If issues persist, check for other API clients on the same IP

## Integration with Existing Jobs

Weather collection is synchronized with park data collection:

- **Park snapshots**: Every 10 minutes (collect_snapshots)
- **Weather observations**: Every 10 minutes (collect_weather)
- **Hourly aggregation**: :05 past each hour (aggregate_hourly)
- **Daily aggregation**: 1:00 AM server time (aggregate_daily)

This ensures weather data is available for aggregation alongside park/ride data.
