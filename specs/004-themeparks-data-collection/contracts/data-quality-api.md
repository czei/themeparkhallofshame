# Data Quality API Contract

**Feature**: 004-themeparks-data-collection
**Date**: 2025-12-31
**Purpose**: API endpoints for monitoring data quality, gaps, and collection health

---

## Overview

These endpoints provide visibility into data collection health, gap detection, and quality issues for both live collection and historical imports.

---

## Endpoints

### GET /api/admin/quality/summary

Get overall data quality summary.

**Query Parameters**:
- `period`: Time period (default: `last_24h`, options: `last_24h`, `last_7d`, `last_30d`)

**Response** (200 OK):
```json
{
  "summary": {
    "period": "last_24h",
    "overall_health": "HEALTHY",
    "collection_uptime_percent": 99.8,
    "data_freshness_minutes": 3,
    "gaps_detected": 0,
    "issues_open": 2
  },
  "collection_stats": {
    "snapshots_collected": 135000,
    "snapshots_expected": 135000,
    "coverage_percent": 100.0,
    "parks_tracked": 45,
    "rides_tracked": 2100
  },
  "data_sources": {
    "LIVE": {
      "records_24h": 135000,
      "last_collection": "2025-12-31T10:05:00Z",
      "status": "ACTIVE"
    },
    "ARCHIVE": {
      "records_imported": 49000000,
      "last_import": "2025-12-30T12:30:00Z",
      "status": "COMPLETE"
    }
  },
  "metadata": {
    "timestamp": "2025-12-31T10:08:00Z"
  }
}
```

**Health Status Values**:
- `HEALTHY`: All systems operating normally
- `DEGRADED`: Minor issues, collection continuing
- `CRITICAL`: Major issues requiring attention

---

### GET /api/admin/quality/gaps

Get detected data gaps.

**Query Parameters**:
- `park_id`: Filter by park (optional)
- `min_gap_hours`: Minimum gap duration (default: 1)
- `limit`: Max results (default: 100)
- `offset`: Pagination offset (default: 0)

**Response** (200 OK):
```json
{
  "gaps": [
    {
      "gap_id": "gap_001",
      "park_id": 134,
      "park_name": "Disneyland",
      "ride_id": null,
      "gap_type": "COLLECTION_FAILURE",
      "start_time": "2025-12-25T02:00:00Z",
      "end_time": "2025-12-25T04:30:00Z",
      "duration_hours": 2.5,
      "data_source": "LIVE",
      "resolution_status": "RESOLVED",
      "resolution_notes": "Server maintenance window"
    },
    {
      "gap_id": "gap_002",
      "park_id": 192,
      "park_name": "Magic Kingdom",
      "ride_id": 1234,
      "ride_name": "Space Mountain",
      "gap_type": "MISSING_ARCHIVE",
      "start_time": "2021-08-15T00:00:00Z",
      "end_time": "2021-08-17T00:00:00Z",
      "duration_hours": 48,
      "data_source": "ARCHIVE",
      "resolution_status": "WONTFIX",
      "resolution_notes": "Archive files not available for this period"
    }
  ],
  "summary": {
    "total_gaps": 2,
    "total_gap_hours": 50.5,
    "gaps_by_source": {
      "LIVE": 1,
      "ARCHIVE": 1
    },
    "gaps_by_status": {
      "RESOLVED": 1,
      "WONTFIX": 1
    }
  },
  "metadata": {
    "timestamp": "2025-12-31T10:00:00Z"
  }
}
```

**Gap Types**:
- `COLLECTION_FAILURE`: Live collection failed
- `API_UNAVAILABLE`: Source API was down
- `MISSING_ARCHIVE`: Archive data not available
- `RATE_LIMITED`: Collection throttled
- `INVALID_DATA`: Data received but unusable

---

### GET /api/admin/quality/issues

Get open data quality issues.

**Query Parameters**:
- `status`: Filter by status (optional: `OPEN`, `INVESTIGATING`, `RESOLVED`, `WONTFIX`)
- `issue_type`: Filter by type (optional)
- `limit`: Max results (default: 100)
- `offset`: Pagination offset (default: 0)

**Response** (200 OK):
```json
{
  "issues": [
    {
      "log_id": 1001,
      "issue_type": "DUPLICATE",
      "entity_type": "ride",
      "entity_id": 1234,
      "entity_name": "Space Mountain",
      "description": "Duplicate records detected: 15 records with same (ride_id, recorded_at)",
      "timestamp_start": "2025-12-30T14:00:00Z",
      "timestamp_end": "2025-12-30T14:15:00Z",
      "records_affected": 15,
      "resolution_status": "OPEN",
      "created_at": "2025-12-30T15:00:00Z"
    },
    {
      "log_id": 1002,
      "issue_type": "INVALID",
      "entity_type": "ride",
      "entity_id": 5678,
      "entity_name": "Splash Mountain",
      "description": "Wait time 999 exceeds maximum allowed value (300)",
      "timestamp_start": "2025-12-31T09:30:00Z",
      "timestamp_end": null,
      "records_affected": 3,
      "resolution_status": "INVESTIGATING",
      "created_at": "2025-12-31T09:35:00Z"
    }
  ],
  "summary": {
    "total_issues": 2,
    "by_status": {
      "OPEN": 1,
      "INVESTIGATING": 1,
      "RESOLVED": 0,
      "WONTFIX": 0
    },
    "by_type": {
      "DUPLICATE": 1,
      "INVALID": 1
    }
  },
  "metadata": {
    "timestamp": "2025-12-31T10:00:00Z"
  }
}
```

**Issue Types**:
- `GAP`: Missing data for time period
- `DUPLICATE`: Duplicate records detected
- `INVALID`: Data failed validation rules
- `MISSING_FIELD`: Required field is NULL

---

### PATCH /api/admin/quality/issues/{log_id}

Update issue status.

**Request**:
```json
{
  "resolution_status": "RESOLVED",
  "resolution_notes": "Duplicates removed by deduplication job"
}
```

**Response** (200 OK):
```json
{
  "log_id": 1001,
  "resolution_status": "RESOLVED",
  "resolution_notes": "Duplicates removed by deduplication job",
  "updated_at": "2025-12-31T10:15:00Z"
}
```

---

### GET /api/admin/quality/freshness

Get data freshness metrics by park.

**Response** (200 OK):
```json
{
  "freshness": [
    {
      "park_id": 134,
      "park_name": "Disneyland",
      "last_snapshot": "2025-12-31T10:05:00Z",
      "minutes_since_last": 3,
      "status": "FRESH"
    },
    {
      "park_id": 192,
      "park_name": "Magic Kingdom",
      "last_snapshot": "2025-12-31T10:05:00Z",
      "minutes_since_last": 3,
      "status": "FRESH"
    },
    {
      "park_id": 245,
      "park_name": "Cedar Point",
      "last_snapshot": "2025-12-31T09:00:00Z",
      "minutes_since_last": 68,
      "status": "STALE"
    }
  ],
  "thresholds": {
    "FRESH": "< 15 minutes",
    "NORMAL": "15-30 minutes",
    "STALE": "> 30 minutes"
  },
  "summary": {
    "parks_fresh": 43,
    "parks_normal": 1,
    "parks_stale": 1,
    "total_parks": 45
  },
  "metadata": {
    "timestamp": "2025-12-31T10:08:00Z"
  }
}
```

---

### GET /api/admin/quality/coverage

Get data coverage statistics by time period.

**Query Parameters**:
- `park_id`: Filter by park (optional)
- `start_date`: Start date (required)
- `end_date`: End date (required)

**Response** (200 OK):
```json
{
  "coverage": {
    "park_id": 134,
    "park_name": "Disneyland",
    "date_range": {
      "start": "2020-01-01",
      "end": "2025-12-31"
    },
    "total_days": 2192,
    "days_with_data": 2180,
    "coverage_percent": 99.5,
    "missing_days": [
      "2021-08-15",
      "2021-08-16",
      "2021-08-17"
    ]
  },
  "daily_breakdown": [
    {
      "date": "2025-12-30",
      "snapshots_count": 3000,
      "expected_count": 3000,
      "coverage_percent": 100.0,
      "data_source": "LIVE"
    },
    {
      "date": "2025-12-29",
      "snapshots_count": 3000,
      "expected_count": 3000,
      "coverage_percent": 100.0,
      "data_source": "LIVE"
    }
  ],
  "metadata": {
    "timestamp": "2025-12-31T10:00:00Z"
  }
}
```

---

## Validation Rules

Data quality monitoring validates against these rules:

| Entity | Field | Rule | Severity |
|--------|-------|------|----------|
| RideStatusSnapshot | recorded_at | Must be UTC, not in future | CRITICAL |
| RideStatusSnapshot | wait_time | Must be 0-300 or NULL | WARNING |
| RideStatusSnapshot | status | Must be valid enum value | CRITICAL |
| RideStatusSnapshot | (ride_id, recorded_at) | Must be unique | WARNING |
| EntityMetadata | latitude | Must be -90 to 90 | WARNING |
| EntityMetadata | longitude | Must be -180 to 180 | WARNING |

---

## Internal Contracts

### DataQualityLog Schema

```python
class DataQualityLog:
    """Stored in data_quality_log table"""
    log_id: int                 # PK
    issue_type: IssueType       # ENUM: GAP, DUPLICATE, INVALID, MISSING_FIELD
    entity_type: str            # 'ride', 'park', etc.
    entity_id: Optional[int]    # FK to entity
    timestamp_start: datetime   # When issue started
    timestamp_end: Optional[datetime]  # When issue ended (NULL if ongoing)
    description: str            # Human-readable description
    resolution_status: ResolutionStatus  # ENUM: OPEN, INVESTIGATING, RESOLVED, WONTFIX
    created_at: datetime
```

---

## Alerting Integration

When issues are detected:
1. Record in `data_quality_log` table
2. Return via `/api/admin/quality/issues` endpoint
3. Include in `/api/health` response if critical
4. Send email alert for CRITICAL issues (if configured)

---

## Authentication

All `/api/admin/*` endpoints require admin authentication via API key:

```
Authorization: Bearer <admin-api-key>
```
