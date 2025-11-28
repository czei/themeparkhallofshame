# Deployment Status

**Last Updated**: 2025-11-27 (PST)
**Status**: DEPLOYED - Live with Data

## Access

# =====================================================
# THE SITE URL IS: http://themeparkhallofshame.com
# =====================================================
# DO NOT USE https:// (no SSL cert yet)
# DO NOT USE www. prefix
# =====================================================

- **URL**: http://themeparkhallofshame.com
- **Basic Auth**: `demo` / `5y3j30q4i`
- **Server**: webperformance.com (35.173.113.141)
- **SSH**: `ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com`

## Verification Commands

Run these to verify the deployment is working:

```bash
# Check API health
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com \
  "curl -s http://127.0.0.1:5001/api/health | python3 -m json.tool"

# Check service status
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com \
  "sudo systemctl status themepark-api"

# Check cron jobs are installed
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "crontab -l"

# Check database counts
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com \
  "mysql -u themepark_app -p294e043ww themepark_tracker -e '
   SELECT \"parks\" as tbl, COUNT(*) FROM parks
   UNION ALL SELECT \"rides\", COUNT(*) FROM rides
   UNION ALL SELECT \"snapshots\", COUNT(*) FROM ride_status_snapshots
   UNION ALL SELECT \"ride_daily_stats\", COUNT(*) FROM ride_daily_stats;'"

# Check collection log
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com \
  "tail -20 /opt/themeparkhallofshame/logs/collect.log"

# Manually run aggregation (to see data sooner)
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com \
  "cd /opt/themeparkhallofshame/backend && /opt/themeparkhallofshame/venv/bin/python -m src.scripts.aggregate_daily"
```

## What's Running

| Component | Status | Details |
|-----------|--------|---------|
| Apache VirtualHost | Running | Serves frontend + proxies /api |
| Gunicorn (systemd) | Running | `themepark-api.service` on port 5001 |
| MariaDB | Running | Database: `themepark_tracker` |
| Cron: Snapshots | Every 10 min | Collects ride status data |
| Cron: Aggregation | 1 AM UTC | Processes daily stats |
| Cron: Parks refresh | Sunday 2 AM | Updates park/ride metadata |

## Why Site Shows No Data (as of deployment)

Data collection started at ~8:20 PM PST when parks were closed. The system correctly shows no data because:
1. All stats have 0 operating hours (parks were closed)
2. API filters out parks with no operating time

**Data will appear** after parks open and the aggregation runs (1 AM UTC = 5 PM PST).

## Key Fix Applied

Changed `ENVIRONMENT=production` to `ENVIRONMENT=server` in `.env` because:
- `production` mode tried to read from AWS SSM (not configured)
- `server` mode reads directly from environment variables via python-dotenv

## Server File Locations

```
/opt/themeparkhallofshame/
├── backend/
│   ├── src/          # Application code
│   ├── .env          # Production config (ENVIRONMENT=server)
│   └── wsgi.py       # Gunicorn entry point
├── venv/             # Python virtual environment
└── logs/             # Application logs

/var/www/themeparkhallofshame/   # Frontend static files
/etc/httpd/conf.d/themeparkhallofshame.conf  # Apache config
/etc/systemd/system/themepark-api.service    # Systemd service
```

## Next Steps (After ~30 Days of Data)

1. Remove Basic Auth from `/etc/httpd/conf.d/themeparkhallofshame.conf`
2. Run: `sudo certbot --apache -d themeparkhallofshame.com -d www.themeparkhallofshame.com`
3. Site goes public!
