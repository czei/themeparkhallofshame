# Weather Collection Monitoring & Alerting

## Overview

Weather collection is monitored through multiple layers to ensure data quality and reliability.

## Monitoring Layers

### 1. Cron Wrapper (Immediate Failure Alerts)

**What**: Every weather collection job is wrapped with `cron_wrapper.py`

**When**: Runs every 10 minutes (synchronized with collection schedule)

**Alerts on**:
- Script exits with non-zero code
- Script times out (>5 minutes)
- Python exceptions or errors

**Alert Details**:
- Exit code and error message
- Last 50 lines of script output
- Debugging instructions
- Recommended actions

**Email**: Sent immediately to `michael@czei.org`

### 2. Hourly Data Collection Health Check

**What**: `check_data_collection.py` monitors all data collection pipelines

**When**: Runs hourly (at :00)

**Checks**:
- Recent ride status snapshots (last 30 min)
- Recent park activity snapshots (last 30 min)
- **Recent weather observations (last 30 min)** ← NEW
- Hourly aggregate freshness

**Weather Monitoring Criteria**:
```python
# Alert if ANY of these conditions are true:
- No weather observations in last 30 minutes
- < 5 parks have recent weather data
- Weather observation count = 0
```

**Alert Email Includes**:
- Last weather observation timestamp
- Parks with recent weather (expected ≥5)
- Weather observations count
- Minutes since last weather observation
- Status (OK/ERROR) for each metric

### 3. Log Files

**Location**: `/opt/themeparkhallofshame/logs/collect_weather.log`

**Contents**:
- Collection start/end timestamps
- Parks processed
- Success/failure status per park
- API errors and timeouts
- Database insertion results

**Rotation**: Weekly (Sunday 2 AM)
- Logs >10MB are gzipped
- Compressed logs >30 days are deleted

## Alert Flow

```
Weather Collection
      ↓
  [SUCCESS?]
      ↓
     NO → cron_wrapper sends immediate alert
      ↓
     YES → Logs to collect_weather.log
      ↓
  (1 hour later)
      ↓
Health Check runs
      ↓
[Recent weather data?]
      ↓
     NO → Health check sends alert
      ↓
     YES → All healthy
```

## Monitoring Dashboard

### Check Weather Collection Status

```bash
# View real-time logs
tail -f /opt/themeparkhallofshame/logs/collect_weather.log

# Check latest weather data
mysql -u themepark_app -p themepark_tracker -e "
  SELECT
    park_id,
    observation_time,
    temperature_f,
    precipitation_probability,
    TIMESTAMPDIFF(MINUTE, observation_time, NOW()) as minutes_ago
  FROM weather_observations
  ORDER BY observation_time DESC
  LIMIT 20;
"

# Count weather observations by hour
mysql -u themepark_app -p themepark_tracker -e "
  SELECT
    DATE_FORMAT(observation_time, '%Y-%m-%d %H:00') as hour,
    COUNT(DISTINCT park_id) as parks,
    COUNT(*) as observations
  FROM weather_observations
  WHERE observation_time >= NOW() - INTERVAL 24 HOUR
  GROUP BY hour
  ORDER BY hour DESC;
"
```

### Check Health Check Status

```bash
# View health check logs
tail -f /opt/themeparkhallofshame/logs/check_data_collection.log

# Manually run health check
cd /opt/themeparkhallofshame/backend
source .env
python -m src.scripts.check_data_collection
```

## Alert Response Procedures

### Alert: Weather Collection Failure (cron_wrapper)

**Symptoms**:
- Email: "Cron Job Failure: collect_weather on [hostname]"
- Exit code: Non-zero
- Script output shows errors

**Troubleshooting**:

1. **Check API connectivity**:
   ```bash
   curl -I https://api.open-meteo.com/v1/forecast
   ```
   Expected: `200 OK`

2. **Check database connectivity**:
   ```bash
   mysql -u themepark_app -p themepark_tracker -e "SELECT 1;"
   ```
   Expected: Returns `1`

3. **Check disk space**:
   ```bash
   df -h
   ```
   Expected: <80% usage

4. **Review error logs**:
   ```bash
   tail -100 /opt/themeparkhallofshame/logs/collect_weather.log
   tail -100 /opt/themeparkhallofshame/logs/error.log
   ```

5. **Test manual collection**:
   ```bash
   cd /opt/themeparkhallofshame/backend
   source .env
   python -m src.scripts.collect_weather --test
   ```

**Common Issues**:
- **API rate limit**: Check if other processes are calling Open-Meteo API
- **Database connection**: Restart MySQL if needed
- **Network issue**: Check firewall/outbound connections
- **Disk full**: Clean up old logs/data

### Alert: No Recent Weather Data (health check)

**Symptoms**:
- Email: "Data Collection Alert: CRITICAL"
- "Last Weather Observation: X minutes ago"
- "Parks with Recent Weather: 0" (expected ≥5)

**Troubleshooting**:

1. **Check if weather collection cron is running**:
   ```bash
   ps aux | grep collect_weather
   ```

2. **Check cron status**:
   ```bash
   systemctl status crond
   journalctl -u crond -n 100
   ```

3. **Verify crontab entry**:
   ```bash
   crontab -l | grep collect_weather
   ```
   Expected:
   ```
   */10 * * * * cd /opt/themeparkhallofshame/backend && source .env && ...
   ```

4. **Check recent cron runs**:
   ```bash
   grep "collect_weather" /var/log/cron | tail -20
   ```

5. **Run manual collection**:
   ```bash
   cd /opt/themeparkhallofshame/backend
   source .env
   python -m src.scripts.collect_weather --test
   ```

**Recovery**:
- If cron stopped: Restart cron service
- If script failing: Fix error and wait for next 10-minute run
- If missing data: Manual backfill not needed (weather is point-in-time)

## Metrics & KPIs

### Collection Success Rate

**Target**: >95% success rate for weather collection

**Measure**:
```sql
SELECT
  DATE(observation_time) as date,
  COUNT(DISTINCT park_id) as parks_collected,
  (SELECT COUNT(*) FROM parks WHERE latitude IS NOT NULL) as total_parks,
  ROUND(100.0 * COUNT(DISTINCT park_id) / (SELECT COUNT(*) FROM parks WHERE latitude IS NOT NULL), 2) as success_rate_pct
FROM weather_observations
WHERE observation_time >= CURDATE() - INTERVAL 7 DAY
GROUP BY DATE(observation_time)
ORDER BY date DESC;
```

### Data Freshness

**Target**: Latest weather observation <15 minutes old

**Measure**:
```sql
SELECT
  MAX(observation_time) as latest_observation,
  TIMESTAMPDIFF(MINUTE, MAX(observation_time), NOW()) as minutes_ago
FROM weather_observations;
```

Expected: `minutes_ago < 15`

### Park Coverage

**Target**: Weather data for all parks with coordinates

**Measure**:
```sql
SELECT
  COUNT(DISTINCT p.park_id) as parks_with_coords,
  COUNT(DISTINCT w.park_id) as parks_with_weather,
  ROUND(100.0 * COUNT(DISTINCT w.park_id) / COUNT(DISTINCT p.park_id), 2) as coverage_pct
FROM parks p
LEFT JOIN weather_observations w ON p.park_id = w.park_id
  AND w.observation_time >= NOW() - INTERVAL 30 MINUTE
WHERE p.latitude IS NOT NULL;
```

Expected: `coverage_pct > 90%`

## Maintenance

### Weekly Tasks

- Review weather collection logs for patterns
- Check alert email delivery
- Verify cron jobs are running on schedule

### Monthly Tasks

- Analyze weather data quality and coverage
- Review and tune alert thresholds if needed
- Verify data retention policy (30 days)

### On-Demand Tasks

- Test alert emails (trigger false positive)
- Validate weather data accuracy against external sources
- Review API rate limiting and adjust if needed

## Integration with Existing Monitoring

Weather collection integrates seamlessly with existing monitoring infrastructure:

- **Same cron_wrapper** as ride/park collection
- **Same health check script** (check_data_collection.py)
- **Same log rotation** policy
- **Same alert email** recipients and format

No additional infrastructure needed!

## Production Deployment Checklist

Before enabling weather monitoring in production:

- [ ] Verify `.env` has `PYTHONPATH` set correctly
- [ ] Verify `SENDGRID_API_KEY` is set for email alerts
- [ ] Test weather collection manually: `python -m src.scripts.collect_weather --test`
- [ ] Test health check manually: `python -m src.scripts.check_data_collection`
- [ ] Verify crontab entries are correct
- [ ] Install crontab: `crontab deployment/config/crontab.prod`
- [ ] Monitor logs for first 24 hours
- [ ] Verify alert emails are received

## Troubleshooting Reference

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| No weather data | Cron not running | Restart cron: `systemctl restart crond` |
| Partial weather data | API timeout for some parks | Check logs, retry will happen in 10 min |
| All weather fails | API down or network issue | Check API status, verify network |
| Old timestamps | Clock skew | Sync server time: `ntpdate` |
| Duplicate data | Multiple cron entries | Check `crontab -l`, remove duplicates |
| Missing alerts | SendGrid issue | Check SendGrid dashboard and API key |

## Support

For issues or questions:
- Check logs first: `/opt/themeparkhallofshame/logs/`
- Review this documentation
- Test manually: `python -m src.scripts.collect_weather --test`
- Contact: michael@czei.org
