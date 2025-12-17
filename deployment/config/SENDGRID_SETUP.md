# SendGrid Email Alert Configuration

## Purpose

Enable automatic email alerts when cron jobs fail. The cron wrapper will send immediate notifications (<90 seconds) when any job exits with a non-zero status.

## Prerequisites

1. **SendGrid Account**: Sign up at https://sendgrid.com (free tier allows 100 emails/day)
2. **API Key**: Create an API key with "Mail Send" permissions

## Setup Steps

### 1. Create SendGrid API Key

1. Log in to SendGrid: https://app.sendgrid.com
2. Navigate to Settings → API Keys
3. Click "Create API Key"
4. Name it: `ThemeParkHallOfShame-Production`
5. Set permissions: "Restricted Access" → Enable "Mail Send" only
6. Click "Create & View"
7. **Copy the API key** (you won't be able to see it again!)

### 2. Add to Production Environment

SSH to production and add the following to `.env`:

```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com
cd /opt/themeparkhallofshame/backend
sudo nano .env
```

Add these lines:

```bash
# SendGrid Email Alerts
SENDGRID_API_KEY=SG.XXXXXXXXXXXXXXXXXX  # Replace with your actual key
ALERT_EMAIL_FROM=alerts@webperformance.com
ALERT_EMAIL_TO=michael@czei.org
```

Save and exit (Ctrl+X, Y, Enter).

### 3. Restart Service

```bash
sudo systemctl restart themepark-api
```

### 4. Test Email Alerts

Trigger a test failure to verify emails work:

```bash
cd /opt/themeparkhallofshame/backend
source .env
/opt/themeparkhallofshame/venv/bin/python -c '
import sys
sys.path.insert(0, "src")
from scripts.cron_wrapper import CronJobWrapper
from datetime import datetime, timezone

wrapper = CronJobWrapper("test_alert", 30)
wrapper.exit_code = 1
wrapper.output = ["Test failure alert"]
wrapper.start_time = datetime.now(timezone.utc)
wrapper.end_time = datetime.now(timezone.utc)
wrapper._log_failure()
wrapper._send_failure_alert()
print("Test alert sent!")
'
```

You should receive an email titled: **"🚨 Cron Job Failure: test_alert on ..."**

## Alert Email Contents

Each failure alert includes:
- Job name and exit code
- Execution duration
- Last 50 lines of output
- SSH command to debug
- Recommended troubleshooting actions

## Troubleshooting

**No email received?**
- Verify `SENDGRID_API_KEY` is set correctly in `.env`
- Check SendGrid dashboard for send attempts
- Check spam folder
- Verify API key has "Mail Send" permission

**Email quota exceeded?**
- Free tier: 100 emails/day
- Monitor SendGrid dashboard for usage
- Upgrade to paid plan if needed (starts at $15/month for 40k emails)

## Cost Estimate

With current cron job frequency:
- ~144 snapshots/day (every 10 min)
- ~24 health checks/day (hourly)
- ~25 aggregations/day (hourly + daily)
- **Total: ~200 executions/day**

Even with a 5% failure rate → 10 alert emails/day (well within free tier).

## Security Notes

- Keep API key secret (never commit to git)
- Use restricted permissions (Mail Send only, not full access)
- Rotate API key if compromised
- Consider separate API keys for dev vs production
