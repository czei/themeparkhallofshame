# Historical Import API Contract

**Feature**: 004-themeparks-data-collection
**Date**: 2025-12-31
**Purpose**: API endpoints for managing and monitoring historical data imports

---

## Overview

These endpoints manage the historical import process from archive.themeparks.wiki. They support resumable imports, progress monitoring, and error tracking.

---

## Endpoints

### POST /api/admin/import/start

Start a new historical import job for a specific park.

**Request**:
```json
{
  "park_id": 134,
  "start_date": "2020-01-01",
  "end_date": "2025-12-31"
}
```

**Response** (201 Created):
```json
{
  "import_id": "imp_abc123",
  "park_id": 134,
  "park_name": "Disneyland",
  "status": "IN_PROGRESS",
  "start_date": "2020-01-01",
  "end_date": "2025-12-31",
  "estimated_records": 2500000,
  "created_at": "2025-12-31T10:00:00Z"
}
```

**Error Responses**:
- 400: Invalid date range or park_id
- 409: Import already in progress for this park
- 503: Archive service unavailable

---

### GET /api/admin/import/status/{import_id}

Get current status of an import job.

**Response** (200 OK):
```json
{
  "import_id": "imp_abc123",
  "park_id": 134,
  "park_name": "Disneyland",
  "status": "IN_PROGRESS",
  "progress": {
    "records_imported": 1250000,
    "records_estimated": 2500000,
    "percent_complete": 50.0,
    "current_date": "2022-06-15",
    "files_processed": 912,
    "files_total": 1826
  },
  "performance": {
    "records_per_second": 2500,
    "eta_minutes": 8
  },
  "errors": {
    "count": 3,
    "latest": [
      {
        "date": "2021-03-15",
        "error": "Malformed JSON: Unexpected token",
        "skipped_records": 288
      }
    ]
  },
  "created_at": "2025-12-31T10:00:00Z",
  "updated_at": "2025-12-31T10:08:20Z"
}
```

**Status Values**:
- `PENDING`: Import queued but not started
- `IN_PROGRESS`: Actively importing data
- `COMPLETED`: Successfully finished
- `FAILED`: Stopped due to unrecoverable error
- `PAUSED`: Manually paused (resumable)

---

### POST /api/admin/import/resume/{import_id}

Resume a paused or failed import from its last checkpoint.

**Response** (200 OK):
```json
{
  "import_id": "imp_abc123",
  "status": "IN_PROGRESS",
  "resumed_from_date": "2022-06-15",
  "resumed_at": "2025-12-31T10:30:00Z"
}
```

---

### POST /api/admin/import/pause/{import_id}

Pause a running import (creates checkpoint for later resume).

**Response** (200 OK):
```json
{
  "import_id": "imp_abc123",
  "status": "PAUSED",
  "checkpoint_date": "2022-06-15",
  "records_imported": 1250000,
  "paused_at": "2025-12-31T10:15:00Z"
}
```

---

### DELETE /api/admin/import/cancel/{import_id}

Cancel a running or paused import (cannot cancel completed imports).

**Response** (200 OK):
```json
{
  "import_id": "imp_abc123",
  "status": "FAILED",
  "cancelled_at": "2025-12-31T10:20:00Z",
  "records_imported": 1250000,
  "message": "Import cancelled by user"
}
```

**Error Responses**:
- 400: Cannot cancel completed import
- 404: Import not found

---

### GET /api/admin/import/list

List all import jobs with optional filtering.

**Query Parameters**:
- `status`: Filter by status (optional)
- `park_id`: Filter by park (optional)
- `limit`: Max results (default: 50)
- `offset`: Pagination offset (default: 0)

**Response** (200 OK):
```json
{
  "data": [
    {
      "import_id": "imp_abc123",
      "park_id": 134,
      "park_name": "Disneyland",
      "status": "COMPLETED",
      "records_imported": 2500000,
      "created_at": "2025-12-30T10:00:00Z",
      "completed_at": "2025-12-30T12:30:00Z"
    },
    {
      "import_id": "imp_def456",
      "park_id": 192,
      "park_name": "Walt Disney World - Magic Kingdom",
      "status": "IN_PROGRESS",
      "records_imported": 800000,
      "created_at": "2025-12-31T10:00:00Z"
    }
  ],
  "metadata": {
    "count": 2,
    "timestamp": "2025-12-31T10:00:00Z"
  }
}
```

---

## Data Quality Endpoints

### GET /api/admin/import/quality/{import_id}

Get data quality report for an import.

**Response** (200 OK):
```json
{
  "import_id": "imp_abc123",
  "quality_metrics": {
    "total_records": 2500000,
    "valid_records": 2499500,
    "invalid_records": 500,
    "duplicate_records": 1200,
    "gaps_detected": 15
  },
  "gap_analysis": [
    {
      "start_date": "2021-08-15",
      "end_date": "2021-08-17",
      "gap_hours": 48,
      "reason": "Archive files missing"
    }
  ],
  "validation_issues": [
    {
      "issue_type": "INVALID_TIMESTAMP",
      "count": 200,
      "sample_record": {
        "ride_id": "abc-123-uuid",
        "timestamp": "2021-13-45T99:99:99Z"
      }
    },
    {
      "issue_type": "WAIT_TIME_OUT_OF_RANGE",
      "count": 300,
      "sample_record": {
        "ride_id": "def-456-uuid",
        "wait_time": 999
      }
    }
  ]
}
```

---

## Internal Contracts

### Checkpoint Schema

```python
class ImportCheckpoint:
    """Stored in import_checkpoints table"""
    checkpoint_id: int          # PK (internal)
    import_id: str              # Public identifier (e.g., "imp_abc123")
    park_id: int                # FK to parks
    last_processed_date: date   # Resume from here + 1 day
    last_processed_file: str    # Filename for audit
    records_imported: int       # Running total
    errors_encountered: int     # Running total
    status: ImportStatus        # ENUM: PENDING, IN_PROGRESS, COMPLETED, FAILED, PAUSED
    created_at: datetime
    updated_at: datetime
```

### Archive File Format

```json
// archive.themeparks.wiki/<park-slug>/2025-12-31.json
{
  "entities": [
    {
      "id": "abc123-uuid",
      "name": "Space Mountain",
      "entityType": "ATTRACTION",
      "status": "OPERATING",
      "queue": {
        "STANDBY": {"waitTime": 45}
      },
      "lastUpdated": "2025-12-31T16:05:33Z"
    }
  ]
}
```

---

## Error Handling

All endpoints return standard error format:

```json
{
  "error": {
    "code": "IMPORT_IN_PROGRESS",
    "message": "An import is already running for this park",
    "details": {
      "existing_import_id": "imp_abc123",
      "started_at": "2025-12-31T10:00:00Z"
    }
  }
}
```

**Error Codes**:
- `PARK_NOT_FOUND`: Invalid park_id
- `IMPORT_NOT_FOUND`: Invalid import_id
- `IMPORT_IN_PROGRESS`: Duplicate import attempt
- `ARCHIVE_UNAVAILABLE`: Cannot reach archive.themeparks.wiki
- `INVALID_DATE_RANGE`: start_date > end_date or future dates
- `CHECKPOINT_CORRUPTED`: Cannot resume from saved state

---

## Rate Limits

Import endpoints are admin-only and rate-limited:
- Start import: 5 requests/hour per park
- Status checks: 60 requests/minute
- Pause/Resume/Cancel: 10 requests/minute

---

## Authentication

All `/api/admin/*` endpoints require admin authentication via API key:

```
Authorization: Bearer <admin-api-key>
```
