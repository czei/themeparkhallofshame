# Partition Verification Checklist (T044)

Manual testing checklist to verify frontend APIs work correctly after applying the `ride_status_snapshots` partition migration.

## Prerequisites

- [ ] Partition migration `004_partition_snapshots.py` has been applied
- [ ] Backend API server is running
- [ ] Frontend is accessible

## API Endpoint Verification

### Parks Downtime Rankings

- [ ] `GET /api/parks/downtime?period=live` - Returns data, no errors
- [ ] `GET /api/parks/downtime?period=today` - Returns data, no errors
- [ ] `GET /api/parks/downtime?period=yesterday` - Returns data, no errors
- [ ] `GET /api/parks/downtime?period=last_week` - Returns data, no errors
- [ ] `GET /api/parks/downtime?period=last_month` - Returns data, no errors

### Rides Downtime Rankings

- [ ] `GET /api/rides/downtime?period=live` - Returns data, no errors
- [ ] `GET /api/rides/downtime?period=today` - Returns data, no errors
- [ ] `GET /api/rides/downtime?period=yesterday` - Returns data, no errors
- [ ] `GET /api/rides/downtime?period=last_week` - Returns data, no errors
- [ ] `GET /api/rides/downtime?period=last_month` - Returns data, no errors

### Park Details (Modal)

- [ ] Click on any park in rankings → Details modal opens
- [ ] Shame score in modal matches rankings table
- [ ] Chart data loads correctly

### Ride Details

- [ ] Click on any ride in rankings → Details display correctly
- [ ] Wait time history chart loads

### Heatmap

- [ ] `GET /api/trends/heatmap-data?type=parks-shame` - Returns data
- [ ] `GET /api/trends/heatmap-data?type=rides-downtime` - Returns data

### Health Check

- [ ] `GET /api/health` - Returns healthy status
- [ ] `data_collection.status` is "healthy" or "stale" (not "no_data")

## Performance Verification

Run these queries directly against the database to verify partition pruning:

```sql
-- Should show only relevant partition(s)
EXPLAIN PARTITIONS
SELECT COUNT(*) FROM ride_status_snapshots
WHERE recorded_at >= '2025-12-30 00:00:00'
AND recorded_at < '2025-12-31 00:00:00';

-- Check partition access stats
SELECT
    partition_name,
    table_rows,
    data_length + index_length as total_size
FROM information_schema.partitions
WHERE table_schema = DATABASE()
AND table_name = 'ride_status_snapshots'
AND partition_name IS NOT NULL
ORDER BY partition_name;
```

## Rollback Verification

If issues are found:

1. Check error logs: `tail -f /opt/themeparkhallofshame/logs/error.log`
2. Verify database state: `SELECT COUNT(*) FROM ride_status_snapshots;`
3. If needed, restore from pre-partition backup

## Sign-off

- [ ] All API endpoints return correct data
- [ ] Performance is same or better than before
- [ ] No errors in logs
- [ ] Verified by: _________________ Date: _________
