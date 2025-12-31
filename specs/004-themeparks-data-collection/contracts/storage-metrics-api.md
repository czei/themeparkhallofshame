# Storage Metrics API Contract

**Feature**: 004-themeparks-data-collection
**Date**: 2025-12-31
**Purpose**: API endpoints for monitoring database storage and capacity planning

---

## Overview

These endpoints provide visibility into database storage usage, growth rates, and capacity projections for permanent data retention planning.

---

## Endpoints

### GET /api/admin/storage/summary

Get current storage usage summary across all tables.

**Response** (200 OK):
```json
{
  "summary": {
    "total_data_size_gb": 45.2,
    "total_index_size_gb": 18.3,
    "total_size_gb": 63.5,
    "capacity_limit_gb": 100.0,
    "percent_used": 63.5
  },
  "tables": [
    {
      "table_name": "ride_status_snapshots",
      "row_count": 49000000,
      "data_size_mb": 28500,
      "index_size_mb": 12000,
      "total_size_mb": 40500,
      "percent_of_total": 63.8,
      "partitions": 48
    },
    {
      "table_name": "queue_data",
      "row_count": 12000000,
      "data_size_mb": 8500,
      "index_size_mb": 3200,
      "total_size_mb": 11700,
      "percent_of_total": 18.4
    },
    {
      "table_name": "entity_metadata",
      "row_count": 5000,
      "data_size_mb": 2.5,
      "index_size_mb": 1.0,
      "total_size_mb": 3.5,
      "percent_of_total": 0.01
    }
  ],
  "metadata": {
    "measurement_date": "2025-12-31",
    "timestamp": "2025-12-31T10:00:00Z"
  }
}
```

---

### GET /api/admin/storage/growth

Get storage growth analysis and projections.

**Query Parameters**:
- `days`: Historical analysis period (default: 30, max: 365)

**Response** (200 OK):
```json
{
  "growth_analysis": {
    "period_days": 30,
    "start_date": "2025-12-01",
    "end_date": "2025-12-31",
    "start_size_gb": 58.2,
    "end_size_gb": 63.5,
    "growth_gb": 5.3,
    "growth_rate_mb_per_day": 176.7,
    "growth_rate_gb_per_month": 5.3
  },
  "table_growth": [
    {
      "table_name": "ride_status_snapshots",
      "rows_added": 4050000,
      "size_added_mb": 4200,
      "growth_rate_mb_per_day": 140.0,
      "rows_per_day": 135000
    },
    {
      "table_name": "queue_data",
      "rows_added": 1000000,
      "size_added_mb": 900,
      "growth_rate_mb_per_day": 30.0,
      "rows_per_day": 33333
    }
  ],
  "projections": {
    "1_year": {
      "estimated_size_gb": 127.4,
      "capacity_exceeded": true,
      "days_until_full": 207
    },
    "3_year": {
      "estimated_size_gb": 255.6,
      "capacity_exceeded": true
    },
    "5_year": {
      "estimated_size_gb": 383.8,
      "capacity_exceeded": true
    }
  },
  "recommendations": [
    {
      "type": "WARNING",
      "message": "Storage will exceed capacity in 207 days at current growth rate",
      "action": "Consider increasing storage capacity or implementing data archival"
    }
  ],
  "metadata": {
    "timestamp": "2025-12-31T10:00:00Z"
  }
}
```

---

### GET /api/admin/storage/partitions

Get partition-level storage details for ride_status_snapshots.

**Response** (200 OK):
```json
{
  "table_name": "ride_status_snapshots",
  "partition_strategy": "RANGE_MONTH",
  "partitions": [
    {
      "partition_name": "p202501",
      "date_range": {
        "start": "2025-01-01",
        "end": "2025-01-31"
      },
      "row_count": 4185000,
      "data_size_mb": 2430,
      "index_size_mb": 980,
      "is_compressed": false
    },
    {
      "partition_name": "p202502",
      "date_range": {
        "start": "2025-02-01",
        "end": "2025-02-28"
      },
      "row_count": 3780000,
      "data_size_mb": 2195,
      "index_size_mb": 885,
      "is_compressed": false
    }
  ],
  "summary": {
    "total_partitions": 48,
    "oldest_partition": "p202101",
    "newest_partition": "p202512",
    "avg_partition_size_mb": 844,
    "total_rows": 49000000
  },
  "metadata": {
    "timestamp": "2025-12-31T10:00:00Z"
  }
}
```

---

### GET /api/admin/storage/retention-comparison

Compare storage requirements for different retention strategies.

**Response** (200 OK):
```json
{
  "current_strategy": {
    "name": "PERMANENT_RAW",
    "description": "Keep all raw snapshots permanently",
    "current_size_gb": 63.5,
    "10_year_projection_gb": 108.0
  },
  "alternative_strategies": [
    {
      "name": "TIERED_90_DAY",
      "description": "Raw snapshots for 90 days, then hourly aggregates",
      "current_size_gb": 28.4,
      "10_year_projection_gb": 42.0,
      "savings_vs_current_percent": 61.1,
      "data_loss": "Individual snapshot granularity after 90 days"
    },
    {
      "name": "HOURLY_ONLY",
      "description": "Only hourly aggregates, no raw snapshots",
      "current_size_gb": 8.5,
      "10_year_projection_gb": 25.0,
      "savings_vs_current_percent": 76.9,
      "data_loss": "All individual snapshot granularity"
    },
    {
      "name": "DAILY_ONLY",
      "description": "Only daily aggregates",
      "current_size_gb": 0.5,
      "10_year_projection_gb": 2.0,
      "savings_vs_current_percent": 98.1,
      "data_loss": "Intraday patterns, hourly trends"
    }
  ],
  "recommendation": {
    "strategy": "PERMANENT_RAW",
    "rationale": "User explicitly requested permanent retention for maximum analytics capability. Storage is cheap (~$0.02/GB/month)."
  },
  "metadata": {
    "timestamp": "2025-12-31T10:00:00Z"
  }
}
```

---

### GET /api/admin/storage/alerts

Get active storage-related alerts.

**Response** (200 OK):
```json
{
  "alerts": [
    {
      "alert_id": "alert_001",
      "severity": "WARNING",
      "type": "CAPACITY_THRESHOLD",
      "message": "Storage usage at 63.5% of capacity",
      "threshold": 60,
      "current_value": 63.5,
      "created_at": "2025-12-30T08:00:00Z"
    },
    {
      "alert_id": "alert_002",
      "severity": "INFO",
      "type": "GROWTH_RATE_CHANGE",
      "message": "Growth rate increased 15% from last month",
      "previous_rate_mb_per_day": 153.7,
      "current_rate_mb_per_day": 176.7,
      "created_at": "2025-12-31T00:00:00Z"
    }
  ],
  "thresholds": {
    "capacity_warning": 60,
    "capacity_critical": 80,
    "growth_rate_change_percent": 10
  },
  "metadata": {
    "timestamp": "2025-12-31T10:00:00Z"
  }
}
```

---

### POST /api/admin/storage/measure

Trigger an immediate storage measurement (normally runs daily via cron).

**Response** (202 Accepted):
```json
{
  "job_id": "measure_abc123",
  "status": "QUEUED",
  "estimated_duration_seconds": 30,
  "queued_at": "2025-12-31T10:00:00Z"
}
```

---

## Alert Configuration

### Alert Thresholds

| Alert Type | Warning | Critical |
|------------|---------|----------|
| Capacity Usage | 60% | 80% |
| Growth Rate Change | 10% | 25% |
| Partition Count | 100 | 150 |
| Days Until Full | 90 days | 30 days |

### Alert Actions

When thresholds are exceeded:
1. Record alert in `data_quality_log` table
2. Send email notification (if configured)
3. Return alert in `/api/admin/storage/alerts` response
4. Include recommendation in `/api/admin/storage/growth` response

---

## Internal Contracts

### StorageMetrics Schema

```python
class StorageMetrics:
    """Stored in storage_metrics table"""
    metric_id: int              # PK
    table_name: str             # Table being measured
    measurement_date: date      # Date of measurement
    row_count: int              # Total rows
    data_size_mb: Decimal       # Data size in MB
    index_size_mb: Decimal      # Index size in MB
    growth_rate_mb_per_day: Decimal  # Calculated growth rate
```

---

## Authentication

All `/api/admin/*` endpoints require admin authentication via API key:

```
Authorization: Bearer <admin-api-key>
```
