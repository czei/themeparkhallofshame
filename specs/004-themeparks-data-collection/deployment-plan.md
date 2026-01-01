# Production Deployment Plan: Feature 004

**Feature**: ThemeParks Data Collection & Permanent Retention
**Branch**: `004-themeparks-data-collection`
**Status**: Ready for deployment review

---

## Pre-Deployment Checklist

### Code Validation
- [x] All tests pass: 1472 passed, 1 skipped due to data freshness
- [x] Linting passes: `ruff check .` - All checks passed
- [x] Branch up to date with main
- [ ] PR created and reviewed

### Local Testing Required Before Deployment
- [ ] Mirror production database: `./deployment/scripts/mirror-production-db.sh --full`
- [ ] Run migrations locally against dev DB
- [ ] Test admin endpoints manually (see validation section below)
- [ ] Verify existing API endpoints still work

---

## Migration Order (CRITICAL)

Migrations MUST run in this exact order due to dependencies:

| Order | Revision | Description | Duration Est. |
|-------|----------|-------------|---------------|
| 1 | `004a_data_source` | Add `data_source` ENUM column to ride_status_snapshots | ~5 min |
| 2 | `004b_import_checkpoints` | Create import_checkpoints table | <1 min |
| 3 | `004c_entity_metadata` | Create entity_metadata table | <1 min |
| 4 | `004d_queue_data` | Create queue_data table | <1 min |
| 5 | `004e_storage_metrics` | Create storage_metrics table | <1 min |
| 6 | `004f_data_quality_log` | Create data_quality_log table | <1 min |
| 7 | `004g_partition_snapshots` | Partition ride_status_snapshots by month | **15-60 min** |

**WARNING**: Migration 7 (partitioning) will:
- Lock the `ride_status_snapshots` table
- Require significant disk I/O
- Take 15-60 minutes depending on data volume
- Should be run during low-traffic window (e.g., 3-5 AM UTC)

---

## Environment Variables

Add to `/opt/themeparkhallofshame/backend/.env`:

```bash
# Historical Import Configuration (Feature 004)
ARCHIVE_S3_BUCKET=archive.themeparks.wiki
ARCHIVE_S3_REGION=us-east-1
IMPORT_BATCH_SIZE=10000
IMPORT_CHECKPOINT_INTERVAL=10
```

---

## Cron Job Changes

### New Jobs to Add

```cron
# Daily metadata sync at 3:00 AM UTC
0 3 * * * cd /opt/themeparkhallofshame/backend && source .env && /opt/themeparkhallofshame/venv/bin/python -m src.scripts.cron_wrapper sync_metadata --timeout=900 >> /opt/themeparkhallofshame/logs/sync_metadata.log 2>&1

# Daily storage measurement at 2:00 AM UTC
0 2 * * * cd /opt/themeparkhallofshame/backend && source .env && /opt/themeparkhallofshame/venv/bin/python -m src.scripts.cron_wrapper measure_storage --timeout=300 >> /opt/themeparkhallofshame/logs/measure_storage.log 2>&1

# Weekly storage alert check at 9:00 AM Pacific on Monday (17:00 UTC)
0 17 * * 1 cd /opt/themeparkhallofshame/backend && source .env && /opt/themeparkhallofshame/venv/bin/python -m src.scripts.measure_storage --skip-measure --check-alerts >> /opt/themeparkhallofshame/logs/storage_alerts.log 2>&1
```

### Job to DISABLE (Permanent Retention)

**CRITICAL**: Comment out the cleanup job - data is now retained permanently:

```cron
# DISABLED - Feature 004 Permanent Retention
# 0 4 * * * cd /opt/themeparkhallofshame/backend && source .env && /opt/themeparkhallofshame/venv/bin/python scripts/cleanup_raw_data.py --force >> /opt/themeparkhallofshame/logs/cleanup.log 2>&1
```

---

## Deployment Steps

### Phase 1: Pre-Deployment (Day Before)

```bash
# 1. Create deployment snapshot
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  /opt/themeparkhallofshame/deployment/scripts/snapshot-manager.sh create pre-feature-004
"

# 2. Verify current database state
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  cd /opt/themeparkhallofshame/backend && source .env
  mysql -h \$DB_HOST -u \$DB_USER -p\$DB_PASSWORD \$DB_NAME -e 'SELECT COUNT(*) FROM ride_status_snapshots;'
"
```

### Phase 2: Deploy Code (Maintenance Window)

```bash
# 1. Deploy code via standard deployment
./deployment/deploy.sh all

# 2. Verify deployment
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  curl -s http://127.0.0.1:5001/api/health | python3 -m json.tool
"
```

### Phase 3: Run Migrations

```bash
# 1. Run migrations (this will take time for partitioning)
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  cd /opt/themeparkhallofshame/backend && source .env
  /opt/themeparkhallofshame/venv/bin/python -m alembic upgrade head 2>&1 | tee /tmp/migration.log
"

# 2. Verify migrations completed
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  cd /opt/themeparkhallofshame/backend && source .env
  /opt/themeparkhallofshame/venv/bin/python -m alembic current
"

# 3. Verify new tables exist
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  cd /opt/themeparkhallofshame/backend && source .env
  mysql -h \$DB_HOST -u \$DB_USER -p\$DB_PASSWORD \$DB_NAME -e \"SHOW TABLES LIKE '%import%'; SHOW TABLES LIKE '%metadata%'; SHOW TABLES LIKE '%queue%'; SHOW TABLES LIKE '%storage%'; SHOW TABLES LIKE '%quality%';\"
"

# 4. Verify partitioning
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  cd /opt/themeparkhallofshame/backend && source .env
  mysql -h \$DB_HOST -u \$DB_USER -p\$DB_PASSWORD \$DB_NAME -e \"
    SELECT partition_name, table_rows
    FROM information_schema.partitions
    WHERE table_name = 'ride_status_snapshots'
    AND partition_name IS NOT NULL
    ORDER BY partition_name DESC
    LIMIT 5;
  \"
"
```

### Phase 4: Update Cron Jobs

```bash
# 1. Update crontab
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  crontab /opt/themeparkhallofshame/deployment/config/crontab.prod
"

# 2. Verify crontab
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  crontab -l | grep -E '(sync_metadata|measure_storage|cleanup_raw)'
"
```

### Phase 5: Post-Deployment Validation

```bash
# 1. Run initial storage measurement
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  cd /opt/themeparkhallofshame/backend && source .env
  /opt/themeparkhallofshame/venv/bin/python -m src.scripts.measure_storage
"

# 2. Test admin endpoints
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  echo '=== Storage Summary ==='
  curl -s http://127.0.0.1:5001/api/admin/storage/summary

  echo ''
  echo '=== Quality Summary ==='
  curl -s http://127.0.0.1:5001/api/admin/quality/summary

  echo ''
  echo '=== Import List ==='
  curl -s http://127.0.0.1:5001/api/admin/import/list
"

# 3. Verify existing API endpoints still work
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  echo '=== Parks Downtime ==='
  curl -s 'http://127.0.0.1:5001/api/parks/downtime?period=yesterday&limit=3'

  echo ''
  echo '=== Rides Downtime ==='
  curl -s 'http://127.0.0.1:5001/api/rides/downtime?period=yesterday&limit=3'
"
```

---

## Rollback Procedure

If deployment fails, rollback using:

```bash
# 1. List available snapshots
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  /opt/themeparkhallofshame/deployment/scripts/snapshot-manager.sh list
"

# 2. Restore pre-deployment snapshot
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  /opt/themeparkhallofshame/deployment/scripts/snapshot-manager.sh restore pre-feature-004
"

# 3. Rollback migrations (if needed)
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  cd /opt/themeparkhallofshame/backend && source .env
  /opt/themeparkhallofshame/venv/bin/python -m alembic downgrade e7b787f62d36
"

# 4. Restart service
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  sudo systemctl restart themepark-api
"
```

For partitioning rollback specifically, see: `backend/docs/partitioning-rollback.md`

---

## Post-Deployment Tasks (Optional)

These tasks are NOT blocking for deployment but should be done eventually:

### Historical Data Import

**WARNING**: This is a one-time operation that imports years of historical data. Run only after deployment is stable.

```bash
# Start historical import for all parks (runs in background, takes hours)
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  cd /opt/themeparkhallofshame/backend && source .env
  nohup /opt/themeparkhallofshame/venv/bin/python -m src.scripts.import_historical --all-parks > /opt/themeparkhallofshame/logs/historical_import.log 2>&1 &
"

# Monitor progress
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  curl -s http://127.0.0.1:5001/api/admin/import/list
"
```

### Metadata Sync

```bash
# Run initial metadata sync
SSH_KEY="$HOME/.ssh/michael-2.pem" ssh -i "$SSH_KEY" ec2-user@webperformance.com "
  cd /opt/themeparkhallofshame/backend && source .env
  /opt/themeparkhallofshame/venv/bin/python -m src.scripts.sync_metadata
"
```

---

## Validation Checklist (Manual Testing)

### Before Deployment
- [ ] Local migrations run successfully against dev DB
- [ ] Admin endpoints respond correctly locally
- [ ] Existing API endpoints unchanged locally

### After Deployment
- [ ] `GET /api/health` returns healthy status
- [ ] `GET /api/parks/downtime?period=yesterday` returns data
- [ ] `GET /api/rides/downtime?period=yesterday` returns data
- [ ] `GET /api/admin/storage/summary` returns storage info
- [ ] `GET /api/admin/quality/summary` returns quality info
- [ ] Partitioning active (check information_schema)
- [ ] New cron jobs scheduled (check crontab -l)
- [ ] Cleanup job disabled (verify commented out)

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Partitioning migration timeout | Medium | High | Run during low-traffic window; have rollback ready |
| Existing queries break | Low | High | All queries tested; partition pruning verified |
| Storage growth too fast | Low | Medium | Storage monitoring in place; alerts configured |
| Historical import data mismatch | Medium | Medium | UUID mapping validated; quality logging enabled |

---

## Contact

- **Feature Owner**: czei
- **Deployment Window**: TBD (recommend 3-5 AM UTC)
- **Estimated Downtime**: 15-60 min (during partitioning migration)
