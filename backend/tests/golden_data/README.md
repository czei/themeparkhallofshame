# Golden Datasets for Audit Validation

Golden datasets contain hand-computed expected values used to verify that
our calculation logic produces correct results.

## Purpose

These datasets test the complete calculation pipeline:
1. Raw snapshots → Ride-level stats
2. Ride stats → Park-level aggregation
3. Tier weighting → Shame score

If any formula changes, these tests will catch discrepancies.

## Test Scenarios

### Scenario A: Simple Park (simple_park.json)
- **Park characteristics:** Rides only open during park hours
- **What it tests:** Basic downtime calculation
- **Key cases:**
  - Rides with 0 downtime
  - Rides with partial downtime
  - Standard tier weighting

### Scenario B: Complex Hours (complex_hours_park.json)
- **Park characteristics:** Rides with independent hours (early entry, extended evening)
- **What it tests:** Park-aware status calculations
- **Key cases:**
  - Rides opening before park
  - Rides closing after park
  - Status during park closed vs park open

### Scenario C: Maintenance Scenarios (maintenance_park.json)
- **Park characteristics:** Various maintenance states
- **What it tests:** Status categorization and exclusions
- **Key cases:**
  - Long-term refurbishment (should not count as downtime)
  - Rides that didn't open (CLOSED status)
  - Mix of DOWN vs CLOSED vs REFURBISHMENT

## File Format

Each scenario has two files:
1. `{scenario}_snapshots.json` - Raw snapshot data
2. `{scenario}_expected.json` - Hand-calculated expected results

### Snapshot Format
```json
{
  "metadata": {
    "scenario": "simple_park",
    "date_range": {"start": "2024-11-18", "end": "2024-11-24"},
    "park_id": 1,
    "description": "Basic scenario with standard operations"
  },
  "snapshots": [
    {
      "ride_id": 101,
      "recorded_at": "2024-11-18T09:00:00Z",
      "status": "OPERATING",
      "computed_is_open": true,
      "park_appears_open": true
    }
  ]
}
```

### Expected Results Format
```json
{
  "metadata": {
    "scenario": "simple_park",
    "computed_by": "Hand calculation",
    "last_verified": "2024-11-30"
  },
  "rides": {
    "101": {
      "ride_name": "Space Mountain",
      "total_snapshots": 288,
      "park_open_snapshots": 200,
      "operating_snapshots": 180,
      "down_snapshots": 20,
      "downtime_hours": 1.67,
      "tier": 1,
      "tier_weight": 3,
      "weighted_downtime": 5.0
    }
  },
  "park_totals": {
    "total_rides": 10,
    "total_weighted_downtime": 15.0,
    "total_park_weight": 22,
    "shame_score": 0.68
  }
}
```

## How to Add New Scenarios

1. Create new `{scenario}_snapshots.json` with raw data
2. Hand-calculate expected results
3. Create `{scenario}_expected.json` with verification
4. Add test case in `tests/integration/test_golden_datasets.py`

## Calculation Reference

### Downtime Hours
```
downtime_hours = (down_snapshots × 5 minutes) ÷ 60
```

### Weighted Downtime
```
weighted_downtime = downtime_hours × tier_weight
```

### Shame Score (CRITICAL: Updated 2025-12-29)

**The shame score is a RATE (0-10 scale), NOT a cumulative value.**

```
                     Σ(weighted_downtime_hours)
shame_score = ──────────────────────────────────────── × 10
              effective_park_weight × operating_hours
```

Where:
- `weighted_downtime_hours` = SUM(ride_downtime_hours × tier_weight) for all rides
- `effective_park_weight` = SUM(tier_weight) for rides that operated
- `operating_hours` = Average hours rides were tracked (park open duration)

**Why this matters:**
- If a park has shame_score = 1.0 for each hour, the DAILY shame should also be ~1.0
- The `operating_hours` in the denominator normalizes the score to a rate
- Without time normalization, daily shame would be ~10x hourly shame (WRONG)

**Example:**
```
Park: Disney California Adventure (Dec 27, 2025)
- weighted_downtime_hours = 39.33
- effective_park_weight = 47
- operating_hours = 14

shame_score = (39.33 / (47 × 14)) × 10
            = (39.33 / 658) × 10
            = 0.6
```

### Tier Weights
- Tier 1 (flagship): weight = 3
- Tier 2 (standard): weight = 2
- Tier 3 (minor): weight = 1
- Default (unclassified): weight = 2
