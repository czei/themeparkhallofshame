# Feature 004: Rollback Runbook

**Purpose**: Procedures to rollback Feature 004 schema changes if issues occur after deployment.

---

## Quick Reference

| Rollback Type | Time Estimate | When to Use |
|---------------|---------------|-------------|
| Partitioning rollback | 10-15 minutes | Query performance issues, partition-related errors |
| Column removal (`data_source`) | 2-3 minutes | Compatibility issues with existing code |
| New table removal | 1-2 minutes | If new tables cause conflicts |
| Full feature rollback | 20-30 minutes | Complete Feature 004 reversal |

---

## Pre-Rollback Checklist

Before any rollback:

1. [ ] Stop all cron jobs: `crontab -r` (save backup first: `crontab -l > crontab.backup`)
2. [ ] Stop the API service: `sudo systemctl stop themepark-api`
3. [ ] Create database backup: `mysqldump themepark_tracker > backup_$(date +%Y%m%d_%H%M%S).sql`
4. [ ] Notify stakeholders of planned downtime

---

## Rollback Procedure: Partitioning

If partitioning causes performance issues or errors:

### Step 1: Verify Current State

```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "mysql -u root -p themepark_tracker -e '
SELECT partition_name, table_rows,
       ROUND(data_length / 1024 / 1024, 2) as data_mb
FROM information_schema.partitions
WHERE table_name = \"ride_status_snapshots\"
ORDER BY partition_name;
'"
```

### Step 2: Create Non-Partitioned Copy

```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "mysql -u root -p themepark_tracker -e '
-- Create table with identical schema but no partitions
CREATE TABLE ride_status_snapshots_unpartitioned LIKE ride_status_snapshots;
ALTER TABLE ride_status_snapshots_unpartitioned REMOVE PARTITIONING;

-- Copy all data (may take 10+ minutes for large datasets)
INSERT INTO ride_status_snapshots_unpartitioned
SELECT * FROM ride_status_snapshots;
'"
```

**Monitor progress:**
```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "mysql -u root -p -e '
SELECT COUNT(*) as copied_rows FROM themepark_tracker.ride_status_snapshots_unpartitioned;
'"
```

### Step 3: Swap Tables

```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "mysql -u root -p themepark_tracker -e '
-- Rename in single transaction (minimal downtime)
RENAME TABLE
    ride_status_snapshots TO ride_status_snapshots_partitioned_backup,
    ride_status_snapshots_unpartitioned TO ride_status_snapshots;
'"
```

### Step 4: Verify Application

```bash
# Restart API
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "sudo systemctl start themepark-api"

# Run smoke tests
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "curl -s http://127.0.0.1:5001/api/health"
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "curl -s 'http://127.0.0.1:5001/api/parks/downtime?period=today'"
```

### Step 5: Cleanup (after 24h validation)

```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "mysql -u root -p themepark_tracker -e '
-- Only after confirming rollback success
DROP TABLE ride_status_snapshots_partitioned_backup;
'"
```

**Estimated Time**: 10-15 minutes for current data volume (~135k rows/day)

---

## Rollback Procedure: data_source Column

If the `data_source` column causes issues:

### Remove Column

```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "mysql -u root -p themepark_tracker -e '
-- Remove from snapshots table
ALTER TABLE ride_status_snapshots DROP COLUMN data_source;
'"
```

### Revert Alembic Migration

```bash
cd /opt/themeparkhallofshame/backend
source ../venv/bin/activate
DB_PASSWORD=<password> alembic downgrade -1  # For single migration
# OR
DB_PASSWORD=<password> alembic downgrade 004_add_data_source_column  # For specific migration
```

**Estimated Time**: 2-3 minutes

---

## Rollback Procedure: New Tables

If new Feature 004 tables cause conflicts:

### Remove Individual Tables

```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "mysql -u root -p themepark_tracker -e '
-- Remove in dependency order
DROP TABLE IF EXISTS queue_data;
DROP TABLE IF EXISTS entity_metadata;
DROP TABLE IF EXISTS import_checkpoints;
DROP TABLE IF EXISTS storage_metrics;
DROP TABLE IF EXISTS data_quality_log;
'"
```

### Remove themeparks_wiki_id Columns

```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "mysql -u root -p themepark_tracker -e '
ALTER TABLE parks DROP COLUMN IF EXISTS themeparks_wiki_id;
ALTER TABLE rides DROP COLUMN IF EXISTS themeparks_wiki_id;
'"
```

**Estimated Time**: 1-2 minutes

---

## Rollback Procedure: Full Feature 004

Complete reversal of all Feature 004 changes:

### Step 1: Stop Services

```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "
crontab -l > /tmp/crontab.backup
crontab -r
sudo systemctl stop themepark-api
"
```

### Step 2: Rollback All Migrations

```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "
cd /opt/themeparkhallofshame/backend
source ../venv/bin/activate
# Downgrade to pre-Feature-004 state
DB_PASSWORD=<password> alembic downgrade 003_last_orm_migration
"
```

### Step 3: Remove Partitioning (if applied)

```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "mysql -u root -p themepark_tracker -e '
-- Check if partitioned
SELECT COUNT(*) FROM information_schema.partitions
WHERE table_name = \"ride_status_snapshots\" AND partition_name IS NOT NULL;
'"
```

If partitioned, follow the partitioning rollback procedure above.

### Step 4: Deploy Previous Code Version

```bash
# Restore from git
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "
cd /opt/themeparkhallofshame
git checkout main~1  # Or specific commit before Feature 004
pip install -r backend/requirements.txt
"
```

### Step 5: Restore Cron Jobs and Start Services

```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "
crontab /tmp/crontab.backup
sudo systemctl start themepark-api
"
```

### Step 6: Verify

```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com "
curl -s http://127.0.0.1:5001/api/health
curl -s 'http://127.0.0.1:5001/api/parks/downtime?period=today' | head -500
"
```

**Estimated Time**: 20-30 minutes

---

## Emergency Contacts

| Role | Contact |
|------|---------|
| Database Admin | (add contact) |
| DevOps | (add contact) |
| Product Owner | (add contact) |

---

## Post-Rollback Actions

After any rollback:

1. [ ] Document the issue that triggered rollback
2. [ ] Create incident report
3. [ ] Restore monitoring and alerts
4. [ ] Notify stakeholders of resolution
5. [ ] Schedule post-mortem review

---

## Related Documentation

- [Deployment Guide](../../docs/deployment.md)
- [Feature 004 Quickstart](../../specs/004-themeparks-data-collection/quickstart.md)
- [Data Model](../../specs/004-themeparks-data-collection/data-model.md)
