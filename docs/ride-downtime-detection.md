# Ride Downtime Detection Criteria

This document explains how the Theme Park Hall of Shame determines when a ride is considered "down" and contributes to a park's shame score.

## Status Values from ThemeParks.wiki API

The ThemeParks.wiki API provides the following ride status values:

| Status | Meaning |
|--------|---------|
| `OPERATING` | Ride is currently running and accepting guests |
| `DOWN` | Unexpected outage/breakdown |
| `CLOSED` | Scheduled closure (not currently operating) |
| `REFURBISHMENT` | Extended maintenance period |

## The Problem: Parks Report Status Differently

Not all parks use these status values the same way:

- **Disney & Universal parks**: Properly distinguish between `DOWN` (unexpected outage) and `CLOSED` (scheduled closure)
- **Other parks (Dollywood, Busch Gardens, SeaWorld, Six Flags, etc.)**: Only report `CLOSED` for everything - they never use `DOWN` status

This means if we treat `CLOSED` the same as `DOWN` for all parks, we'd incorrectly count scheduled closures as outages for non-Disney parks.

## Solution: Park-Type-Aware Detection

### 1. What Counts as "Down"

| Park Type | Status Values Counted as Down |
|-----------|-------------------------------|
| Disney & Universal | `status = 'DOWN'` only |
| Other Parks | `status IN ('DOWN', 'CLOSED')` |

**Rationale**: Disney/Universal properly use `DOWN` for outages, so we only count that. Other parks only report `CLOSED`, so we must include it to detect any outages at all.

### 2. Minimum Operating Threshold

To filter out rides that haven't truly "opened" yet (seasonal closures, late openings), we require rides to have operated for a minimum time before counting downtime:

| Park Type | Minimum Operating Snapshots | Time Equivalent |
|-----------|----------------------------|-----------------|
| Disney & Universal | 1 snapshot | 5 minutes |
| Other Parks | 6 snapshots | 30 minutes |

**Rationale**:
- Disney/Universal have reliable status reporting, so 1 operating snapshot is sufficient proof a ride is "in service"
- Other parks may briefly show a ride as operating before settling into their daily pattern. Requiring 30 minutes of operation filters out false positives from parks that open later in the day.

### 3. Park Must Be Open

Downtime is only counted when `park_appears_open = TRUE`. This is determined by:
- At least 50% of the park's rides showing as operating in the current snapshot

This prevents counting rides as "down" before the park opens or after it closes.

## Code Implementation

The logic is centralized in `sql_helpers.py`:

```python
# Park-type-aware is_down check
RideStatusSQL.is_down("rss", parks_alias="p")
# Generates:
# CASE
#     WHEN (p.is_disney = TRUE OR p.is_universal = TRUE) THEN
#         rss.status = 'DOWN'
#     ELSE
#         rss.status IN ('DOWN', 'CLOSED') OR (rss.status IS NULL AND rss.computed_is_open = FALSE)
# END

# Park-type-aware has_operated check
RideStatusSQL.has_operated_for_park_type("r.ride_id", "p")
# Generates:
# CASE
#     WHEN (p.is_disney = TRUE OR p.is_universal = TRUE) THEN
#         EXISTS (SELECT 1 FROM ride_status_snapshots WHERE status = 'OPERATING' ...)
#     ELSE
#         (SELECT COUNT(*) >= 6 FROM ride_status_snapshots WHERE status = 'OPERATING' ...)
# END
```

## Shame Score Formula

Once a ride is determined to be "down", it contributes to the park's shame score:

```
Shame Score = (Sum of Weighted Rides Down / Total Park Weight) * 10
```

Where:
- **Tier 1 rides** (flagships like Space Mountain): weight = 5
- **Tier 2 rides** (standard attractions): weight = 2
- **Tier 3 rides** (minor attractions): weight = 1

## Example: Dollywood

Before park-type-aware logic:
- Big Bear Mountain: 1 operating snapshot, 107 down snapshots → counted as down
- Shame score inflated due to rides that hadn't truly opened yet

After park-type-aware logic:
- Big Bear Mountain: 1 operating snapshot < 6 required → filtered out
- Only rides with 30+ minutes of operation counted
- Accurate shame score reflecting actual outages

## Database Schema

The detection relies on these tables:
- `parks.is_disney` - Boolean flag for Disney parks
- `parks.is_universal` - Boolean flag for Universal parks
- `ride_status_snapshots.status` - Current status from API
- `ride_status_snapshots.computed_is_open` - Derived boolean (TRUE if operating)
- `park_activity_snapshots.park_appears_open` - Whether park is currently open
