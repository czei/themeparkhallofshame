# Theme Park Hall of Shame - Deployment Guide

## Overview

This directory contains all scripts and configuration files needed to deploy the Theme Park Hall of Shame application to production.

**Target Server**: webperformance.com (Amazon Linux, ec2-user)
**Domain**: themeparkhallofshame.com

## Staged Rollout Plan

### Phase 1: Soft Launch (Weeks 1-4)
- Deploy application and start data collection
- Site is live but protected with **HTTP Basic Authentication**
- Only you can access with username/password
- Collect ~30 days of data to validate accuracy
- Monitor cron jobs and check data quality

**Basic Auth Setup** (done automatically by `setup-services.sh`):
```bash
# Create htpasswd file with your username
sudo htpasswd -c /etc/httpd/.htpasswd-themepark <your-username>

# Add additional users (without -c flag)
sudo htpasswd /etc/httpd/.htpasswd-themepark <another-user>
```

### Phase 2: Public Launch (After Data Validation)
1. Remove Basic Auth from Apache config:
   ```bash
   sudo nano /etc/httpd/conf.d/themeparkhallofshame.conf
   # Delete or comment out the <Location /> ... </Location> auth block
   sudo systemctl reload httpd
   ```
2. Run SSL setup: `sudo certbot --apache -d themeparkhallofshame.com -d www.themeparkhallofshame.com`
3. Site goes public!

## Directory Structure

```
deployment/
├── deploy.sh                     # Main deployment script (run from local)
├── scripts/
│   ├── setup-server.sh           # Install system packages (one-time)
│   ├── setup-python.sh           # Create Python venv (one-time)
│   ├── setup-database.sh         # Run database migrations
│   ├── setup-services.sh         # Install systemd/Apache/cron (one-time)
│   └── health-check.sh           # Verify deployment
├── config/
│   ├── themepark-api.service     # Systemd service file
│   ├── themeparkhallofshame.conf # Apache VirtualHost
│   └── crontab.prod              # Production cron jobs
└── templates/
    └── production.env.example    # Template for .env file
```

## First-Time Deployment

### 1. SSH to Server

```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com
```

### 2. Clone Repository

```bash
sudo mkdir -p /opt/themeparkhallofshame
sudo chown ec2-user:ec2-user /opt/themeparkhallofshame
git clone <repo-url> /opt/themeparkhallofshame
```

### 3. Run Setup Scripts (on server)

```bash
cd /opt/themeparkhallofshame/deployment

# Install system packages
./scripts/setup-server.sh

# Create Python virtual environment
./scripts/setup-python.sh

# Create production .env (REQUIRED - fill in real values)
cp templates/production.env.example /opt/themeparkhallofshame/backend/.env
nano /opt/themeparkhallofshame/backend/.env

# Setup database
./scripts/setup-database.sh production

# Install services (systemd, Apache, cron)
./scripts/setup-services.sh

# Verify everything works
./scripts/health-check.sh
```

### 4. DNS Configuration

Point these records to the server IP:
- `themeparkhallofshame.com` → A record
- `www.themeparkhallofshame.com` → A record (or CNAME)

### 5. SSL Certificate

```bash
sudo certbot --apache -d themeparkhallofshame.com -d www.themeparkhallofshame.com
```

## Ongoing Deployments

After initial setup, deploy updates from your **local machine**:

```bash
# Full deployment
./deployment/deploy.sh all

# Backend only
./deployment/deploy.sh backend

# Frontend only
./deployment/deploy.sh frontend

# Database migrations only
./deployment/deploy.sh migrations

# Just restart services
./deployment/deploy.sh restart

# Health check
./deployment/deploy.sh health
```

## Environment Variables

Set `SSH_KEY` if your key is not at `~/.ssh/michael-2.pem`:

```bash
export SSH_KEY=/path/to/your/key.pem
./deployment/deploy.sh all
```

## Troubleshooting

### Check Service Status

```bash
sudo systemctl status themepark-api
sudo systemctl status httpd
```

### View Logs

```bash
# Application logs
tail -f /opt/themeparkhallofshame/logs/error.log
tail -f /opt/themeparkhallofshame/logs/access.log

# Apache logs
tail -f /opt/themeparkhallofshame/logs/apache-error.log

# Cron job logs
tail -f /opt/themeparkhallofshame/logs/collect_snapshots.log
tail -f /opt/themeparkhallofshame/logs/aggregate_daily.log
```

### Restart Services

```bash
sudo systemctl restart themepark-api
sudo systemctl reload httpd
```

### Test API Directly

```bash
curl http://127.0.0.1:5001/api/health
curl http://127.0.0.1:5001/api/parks/downtime?period=today
```

## Server Paths

| Component | Path |
|-----------|------|
| Application | `/opt/themeparkhallofshame/` |
| Python venv | `/opt/themeparkhallofshame/venv/` |
| Backend code | `/opt/themeparkhallofshame/backend/src/` |
| Production .env | `/opt/themeparkhallofshame/backend/.env` |
| Logs | `/opt/themeparkhallofshame/logs/` |
| Frontend (static) | `/var/www/themeparkhallofshame/` |
| Systemd service | `/etc/systemd/system/themepark-api.service` |
| Apache config | `/etc/httpd/conf.d/themeparkhallofshame.conf` |
