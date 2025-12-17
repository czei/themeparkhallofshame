# MySQL Master-Replica Replication Setup

## Overview

This guide sets up continuous database replication from production (master) to development (replica), ensuring tests always run against fresh production data.

## Architecture

```
Production Server (Master)              Dev Machine (Replica)
┌─────────────────────┐                ┌────────────────────┐
│  MySQL Master       │                │  MySQL Replica     │
│  (Read/Write)       │   Replication  │  (Read-Only)       │
│                     │───────────────>│                    │
│  App writes here    │   Binary Log   │  Tests read here   │
└─────────────────────┘   Streaming    └────────────────────┘
                          < 5 sec lag
```

## Benefits

✅ **No Environment Drift**: Dev database always matches production exactly
✅ **Always Fresh**: Replication lag < 5 seconds
✅ **Real Data Patterns**: Tests catch edge cases from production data
✅ **No Manual Sync**: Automatic continuous replication
✅ **Time-Accurate**: Tests see actual "today", "yesterday", "last_week" data
✅ **Safe**: Read-only replica can't corrupt production

## Prerequisites

- Production MySQL server (master)
- Dev machine with MySQL installed (replica)
- Network connectivity between production and dev
- Sufficient disk space on dev for full database copy

## Step 1: Configure Production Server (Master)

### 1.1 Enable Binary Logging

SSH to production and edit MySQL configuration:

```bash
ssh -i ~/.ssh/michael-2.pem ec2-user@webperformance.com
sudo nano /etc/my.cnf
```

Add to `[mysqld]` section:

```ini
[mysqld]
# Replication Configuration (Master)
server-id = 1
log_bin = /var/log/mysql/mysql-bin.log
binlog_format = ROW
binlog_expire_logs_days = 7
max_binlog_size = 100M

# Only replicate the theme park database
binlog_do_db = themepark_tracker
```

### 1.2 Create Log Directory

```bash
sudo mkdir -p /var/log/mysql
sudo chown mysql:mysql /var/log/mysql
```

### 1.3 Restart MySQL

```bash
sudo systemctl restart mariadb
# Or: sudo systemctl restart mysqld
```

### 1.4 Create Replication User

```bash
mysql -u root -p
```

```sql
-- Create replication user with limited permissions
CREATE USER 'repl_user'@'%' IDENTIFIED BY 'STRONG_PASSWORD_HERE';
GRANT REPLICATION SLAVE ON *.* TO 'repl_user'@'%';
FLUSH PRIVILEGES;

-- Verify user was created
SELECT user, host FROM mysql.user WHERE user = 'repl_user';
```

### 1.5 Get Master Status

```sql
-- Lock tables temporarily
FLUSH TABLES WITH READ LOCK;

-- Record these values (you'll need them for replica setup)
SHOW MASTER STATUS;
-- Note: File (e.g., mysql-bin.000001) and Position (e.g., 12345)

-- Unlock tables
UNLOCK TABLES;
```

**Important**: Record the `File` and `Position` values!

### 1.6 Create Initial Database Dump

```bash
# Export production database
mysqldump -u root -p \
  --single-transaction \
  --master-data=1 \
  --databases themepark_tracker \
  > /tmp/themepark_initial_dump.sql

# Verify dump was created
ls -lh /tmp/themepark_initial_dump.sql
```

### 1.7 Open Firewall (if needed)

```bash
# Allow MySQL from dev machine IP (replace with your dev IP)
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="YOUR_DEV_IP" port port="3306" protocol="tcp" accept'
sudo firewall-cmd --reload
```

## Step 2: Configure Dev Machine (Replica)

### 2.1 Copy Database Dump

On your dev machine:

```bash
# Copy dump from production
scp -i ~/.ssh/michael-2.pem \
  ec2-user@webperformance.com:/tmp/themepark_initial_dump.sql \
  /tmp/

# Import dump to local MySQL
mysql -u root -p < /tmp/themepark_initial_dump.sql
```

### 2.2 Configure Replica

Edit MySQL configuration on dev machine:

```bash
sudo nano /etc/my.cnf
# Or on Mac with Homebrew: nano /opt/homebrew/etc/my.cnf
```

Add to `[mysqld]` section:

```ini
[mysqld]
# Replication Configuration (Replica)
server-id = 2
relay-log = /var/log/mysql/relay-bin
relay-log-index = /var/log/mysql/relay-bin.index
log_slave_updates = 1
read_only = 1

# Only replicate the theme park database
replicate-do-db = themepark_tracker
```

### 2.3 Create Log Directory

```bash
sudo mkdir -p /var/log/mysql
sudo chown _mysql:_mysql /var/log/mysql  # Mac
# Or: sudo chown mysql:mysql /var/log/mysql  # Linux
```

### 2.4 Restart MySQL

```bash
# Mac with Homebrew
brew services restart mysql

# Linux
sudo systemctl restart mysql
# Or: sudo systemctl restart mariadb
```

### 2.5 Configure Replication

```bash
mysql -u root -p
```

```sql
-- Configure master connection
-- Replace with values from SHOW MASTER STATUS above
CHANGE MASTER TO
  MASTER_HOST='webperformance.com',
  MASTER_USER='repl_user',
  MASTER_PASSWORD='STRONG_PASSWORD_HERE',
  MASTER_LOG_FILE='mysql-bin.000001',  -- From SHOW MASTER STATUS
  MASTER_LOG_POS=12345;                -- From SHOW MASTER STATUS

-- Start replication
START SLAVE;

-- Check status (should show both IO and SQL threads running)
SHOW SLAVE STATUS\G
```

### 2.6 Verify Replication

Look for these key indicators:

```sql
SHOW SLAVE STATUS\G
```

**Must be YES:**
- `Slave_IO_Running: Yes`
- `Slave_SQL_Running: Yes`

**Should be 0 or small:**
- `Seconds_Behind_Master: 0` (or < 5 seconds)

**Should be empty:**
- `Last_IO_Error: ` (empty = no errors)
- `Last_SQL_Error: ` (empty = no errors)

## Step 3: Test Replication

### 3.1 Insert Test Data on Production

On production:

```sql
-- Insert a test record
INSERT INTO parks (name, slug, themeparks_wiki_id, timezone)
VALUES ('Test Park', 'test-park', 'test-123', 'America/Los_Angeles');

-- Get the ID
SELECT id, name FROM parks WHERE slug = 'test-park';
```

### 3.2 Verify on Replica

On dev machine (within 5 seconds):

```sql
-- Should see the test record
SELECT id, name FROM parks WHERE slug = 'test-park';
```

### 3.3 Clean Up

On production:

```sql
-- Remove test record
DELETE FROM parks WHERE slug = 'test-park';
```

✅ If you see the record on replica, replication is working!

## Step 4: Monitor Replication Health

### Check Replication Status

```sql
-- On replica
SHOW SLAVE STATUS\G

-- Key metrics to monitor:
-- Seconds_Behind_Master: Should be < 5
-- Slave_IO_Running: YES
-- Slave_SQL_Running: YES
```

### Create Monitoring Query

Save this as `check_replication.sql`:

```sql
SELECT
  CASE
    WHEN Slave_IO_Running = 'Yes'
         AND Slave_SQL_Running = 'Yes'
         AND Seconds_Behind_Master < 5
    THEN 'HEALTHY'
    WHEN Seconds_Behind_Master > 60
    THEN 'LAGGING'
    WHEN Slave_IO_Running = 'No' OR Slave_SQL_Running = 'No'
    THEN 'STOPPED'
    ELSE 'UNKNOWN'
  END AS replication_status,
  Slave_IO_Running,
  Slave_SQL_Running,
  Seconds_Behind_Master,
  Last_IO_Error,
  Last_SQL_Error
FROM
  (SELECT * FROM performance_schema.replication_connection_status
   JOIN performance_schema.replication_applier_status USING (channel_name)) AS status;
```

## Troubleshooting

### Problem: Replication Not Starting

```sql
-- Check error logs
SHOW SLAVE STATUS\G
-- Look at Last_IO_Error and Last_SQL_Error

-- Common fixes:
STOP SLAVE;
RESET SLAVE;
-- Re-run CHANGE MASTER TO command
START SLAVE;
```

### Problem: High Replication Lag

```sql
-- Check lag
SHOW SLAVE STATUS\G
-- Seconds_Behind_Master > 60

-- Possible causes:
-- 1. Network latency
-- 2. Slow dev machine
-- 3. Large production writes

-- Temporary fix: Skip ahead (DANGEROUS - only for dev)
STOP SLAVE;
-- Get current master position
SHOW MASTER STATUS;  -- Run on production
-- Set replica to that position
CHANGE MASTER TO MASTER_LOG_FILE='xxx', MASTER_LOG_POS=yyy;
START SLAVE;
```

### Problem: Replica is Read-Only

This is by design! Tests should not write to replica.

```sql
-- Verify read-only is enabled
SHOW VARIABLES LIKE 'read_only';
-- Should show: ON

-- Temporarily disable (for manual testing only):
SET GLOBAL read_only = OFF;
-- ... do your testing ...
SET GLOBAL read_only = ON;
```

## Security Considerations

- Replication user has minimal permissions (REPLICATION SLAVE only)
- Replica is read-only (prevents accidental writes)
- Binary logs are expired after 7 days (saves disk space)
- Network traffic should use SSL (optional but recommended)

## Maintenance

### Rotate Binary Logs

Production logs will grow over time:

```sql
-- Check log size
SHOW BINARY LOGS;

-- Purge old logs (keeps last 7 days)
PURGE BINARY LOGS BEFORE DATE_SUB(NOW(), INTERVAL 7 DAY);
```

### Backup Strategy

Replication is NOT a backup! Continue regular backups:

```bash
# Production backups (existing process)
mysqldump -u root -p --single-transaction themepark_tracker \
  > backup_$(date +%Y%m%d).sql
```

### Monitoring Checklist

Daily:
- [ ] Check `Seconds_Behind_Master` < 5
- [ ] Verify both IO and SQL threads running
- [ ] Check disk space on both servers

Weekly:
- [ ] Review binary log size on production
- [ ] Purge old binary logs if needed

## Next Steps

After replication is working:

1. Update test configuration to use replica (see `TEST_CONFIGURATION.md`)
2. Create replica freshness fixtures (see `backend/tests/integration/conftest.py`)
3. Run full test suite against replica
4. Update documentation

## Reference

- [MySQL Replication Guide](https://dev.mysql.com/doc/refman/8.0/en/replication.html)
- [Binary Log Configuration](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html)
