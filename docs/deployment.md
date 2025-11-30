# Deployment Guide

This guide covers deploying both the backend API and frontend for Theme Park Hall of Shame.

## Live Site

- **URL**: http://themeparkhallofshame.com
- **Status**: Deployed and collecting data

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Production Server                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│   Apache VirtualHost                                         │
│   ├── Serves frontend static files                          │
│   └── Proxies /api → Gunicorn (port 5001)                   │
│                                                              │
│   Gunicorn (systemd service)                                 │
│   └── Flask API application                                  │
│                                                              │
│   MariaDB                                                    │
│   └── themepark_tracker database                            │
│                                                              │
│   Cron Jobs                                                  │
│   ├── Every 10 min: Collect ride status snapshots           │
│   ├── 1 AM UTC: Daily aggregation                           │
│   └── Sunday 2 AM: Refresh parks/rides metadata             │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Server File Locations

```
/opt/themeparkhallofshame/
├── backend/
│   ├── src/          # Application code
│   ├── .env          # Production config
│   └── wsgi.py       # Gunicorn entry point
├── venv/             # Python virtual environment
└── logs/             # Application logs

/var/www/themeparkhallofshame/   # Frontend static files
/etc/httpd/conf.d/themeparkhallofshame.conf  # Apache config
/etc/systemd/system/themepark-api.service    # Systemd service
```

---

## Backend Deployment

### Prerequisites

- Python 3.11+
- MariaDB/MySQL 8.0+
- Apache with mod_proxy (or Nginx)

### Setup Steps

1. **Clone repository**
   ```bash
   cd /opt
   git clone https://github.com/yourusername/ThemeParkHallOfShame.git themeparkhallofshame
   ```

2. **Create virtual environment**
   ```bash
   cd /opt/themeparkhallofshame
   python3 -m venv venv
   source venv/bin/activate
   pip install -r backend/requirements.txt
   ```

3. **Configure environment**
   ```bash
   cp backend/.env.example backend/.env
   # Edit .env with your database credentials
   ```

4. **Initialize database**
   ```bash
   mysql -u root -p < backend/src/database/migrations/001_initial_schema.sql
   # Run all migration files in order
   ```

5. **Set up systemd service**
   ```bash
   sudo cp deployment/themepark-api.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable themepark-api
   sudo systemctl start themepark-api
   ```

6. **Configure cron jobs**
   ```bash
   crontab -e
   # Add:
   */10 * * * * cd /opt/themeparkhallofshame/backend && /opt/themeparkhallofshame/venv/bin/python -m src.scripts.collect_snapshots >> /opt/themeparkhallofshame/logs/collect.log 2>&1
   10 1 * * * cd /opt/themeparkhallofshame/backend && /opt/themeparkhallofshame/venv/bin/python -m src.scripts.aggregate_daily >> /opt/themeparkhallofshame/logs/aggregate.log 2>&1
   0 2 * * 0 cd /opt/themeparkhallofshame/backend && /opt/themeparkhallofshame/venv/bin/python -m src.scripts.collect_parks >> /opt/themeparkhallofshame/logs/parks.log 2>&1
   ```

### Verification

```bash
# Check API health
curl -s http://127.0.0.1:5001/api/health | python3 -m json.tool

# Check service status
sudo systemctl status themepark-api

# Check database counts
mysql -u themepark_app -p themepark_tracker -e '
  SELECT "parks" as tbl, COUNT(*) FROM parks
  UNION ALL SELECT "rides", COUNT(*) FROM rides
  UNION ALL SELECT "snapshots", COUNT(*) FROM ride_status_snapshots;'
```

---

## Frontend Deployment

The frontend is pure HTML/CSS/JavaScript with no build step.

### Configuration

Edit `frontend/js/config.js` and set your production API URL:

```javascript
const CONFIG = {
    API_BASE_URL: 'https://your-backend-api.com/api',
    // ...
};
```

### Deployment Options

#### Option 1: Same Server as Backend (Current Setup)

Copy frontend files to web server document root:
```bash
cp -r frontend/* /var/www/themeparkhallofshame/
```

Configure Apache to serve static files and proxy API requests.

#### Option 2: Netlify (Recommended for Static Hosting)

1. Connect GitHub repository to Netlify
2. Configure:
   - **Base directory**: `frontend`
   - **Publish directory**: `.`
3. Deploy

#### Option 3: Vercel

1. Import GitHub repository
2. Configure:
   - **Root Directory**: `frontend`
   - **Framework Preset**: Other
3. Deploy

#### Option 4: Any Static Host

Upload the `frontend` directory to any static hosting service:
- Cloudflare Pages
- GitHub Pages
- AWS S3 + CloudFront
- Firebase Hosting

### CORS Configuration

If frontend and backend are on different domains, update `backend/src/api/app.py`:

```python
from flask_cors import CORS

CORS(app, resources={
    r"/api/*": {
        "origins": ["https://your-frontend-domain.com"],
        "methods": ["GET", "OPTIONS"],
        "allow_headers": ["Content-Type"]
    }
})
```

---

## Post-Deployment Checklist

- [ ] API health endpoint responds
- [ ] Cron jobs are installed and running
- [ ] Database has parks and rides data
- [ ] Frontend loads without errors
- [ ] All views display data correctly
- [ ] External links to Queue-Times.com work
- [ ] No CORS errors in browser console

---

## Troubleshooting

### API returns 404
- Verify Gunicorn is running: `sudo systemctl status themepark-api`
- Check Apache proxy configuration

### No data appears on frontend
- Check if parks are operating (data only collected during park hours)
- Verify aggregation job has run: check `aggregation_log` table
- Run manual aggregation if needed

### CORS errors
- Verify frontend domain is in CORS allowed origins
- Check that API URL includes `/api` prefix

---

## SSL/HTTPS Setup

After the site is ready for public access:

```bash
sudo certbot --apache -d themeparkhallofshame.com -d www.themeparkhallofshame.com
```

This will automatically configure Apache for HTTPS.
