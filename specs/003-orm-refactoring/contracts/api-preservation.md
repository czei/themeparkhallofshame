# API Contract Preservation

**Feature**: 003-orm-refactoring
**Date**: 2025-12-21
**CRITICAL CONSTRAINT**: Zero changes to Flask REST API contracts

---

## Purpose

This ORM refactoring is **internal only** - replacing raw SQL with SQLAlchemy ORM models. The Flask API exposed to the frontend **MUST remain unchanged** to avoid frontend modifications.

---

## Preserved API Contracts

### All Existing Endpoints Unchanged

**Current Flask Routes** (preserved exactly):
- `GET /api/parks/downtime?period={period}&filter={filter}&limit={limit}`
- `GET /api/rides/downtime?period={period}&filter={filter}&limit={limit}`
- `GET /api/rides/waittimes?period={period}&filter={filter}&limit={limit}`
- `GET /api/trends/heatmap-data?period={period}&type={type}&limit={limit}`
- `GET /api/health`

**Response Schema** (unchanged):
```json
{
  "data": [...],
  "metadata": {
    "period": "today",
    "timestamp": "2025-12-21T10:00:00Z",
    "count": 42
  }
}
```

### Query Parameter Contracts

**ALL query parameters preserved**:
- `period`: `today`, `yesterday`, `last_week`, `last_month`, `all_time`
- `filter`: `all-parks`, `disney-parks`, `universal-parks`, specific park IDs
- `limit`: Integer (default: 100)
- `type`: `parks-shame`, `parks-downtime`, `rides-downtime`, `rides-waittimes`

### Response Field Contracts

**ALL response fields preserved** (same field names, same data types):
- `park_id`, `park_name`, `park_type`
- `ride_id`, `ride_name`, `ride_tier`
- `shame_score`, `total_downtime_minutes`, `uptime_percentage`
- `total_rides_down`, `total_rides_operated`
- `wait_time`, `status`
- `timestamp`, `period`, `count`

---

## Implementation Strategy

### Repository Pattern Maintains Function Signatures

```python
# OLD implementation (raw SQL)
class StatsRepository:
    def get_park_rankings(self, period: str, filter: str, limit: int) -> List[Dict]:
        """Get park rankings for period"""
        sql = "SELECT park_id, SUM(...) FROM hourly_stats WHERE ..."
        cursor.execute(sql)
        return cursor.fetchall()

# NEW implementation (ORM)
class StatsRepository:
    def get_park_rankings(self, period: str, filter: str, limit: int) -> List[Dict]:
        """Get park rankings for period (ORM-based, SAME SIGNATURE)"""
        from src.models.ride import Ride
        from src.models.snapshots import RideStatusSnapshot
        from src.utils.query_helpers import RideStatusQuery

        # ORM query replaces raw SQL
        results = (
            session.query(...)
            .filter(...)
            .all()
        )

        # SAME RETURN FORMAT (List[Dict] with same keys)
        return [
            {
                'park_id': r.park_id,
                'park_name': r.park.name,
                'shame_score': r.shame_score,
                'total_downtime_minutes': r.downtime,
                ...
            }
            for r in results
        ]
```

### Flask Routes Unchanged

```python
# src/api/routes/parks.py - NO CHANGES
@bp.route('/downtime', methods=['GET'])
def get_park_downtime():
    """Park downtime rankings API endpoint (UNCHANGED)"""
    period = request.args.get('period', 'today')
    filter_param = request.args.get('filter', 'all-parks')
    limit = int(request.args.get('limit', 100))

    # Internal implementation uses ORM (repository pattern abstraction)
    repo = StatsRepository(db_session)  # Injected ORM session instead of cursor
    data = repo.get_park_rankings(period, filter_param, limit)

    # Response format UNCHANGED
    return jsonify({
        'data': data,
        'metadata': {
            'period': period,
            'timestamp': datetime.utcnow().isoformat(),
            'count': len(data)
        }
    })
```

---

## Validation Strategy

### Contract Tests Prevent Breakage

**Test Pattern** (existing tests remain valid):
```python
# tests/contract/test_api_contracts.py
def test_parks_downtime_api_contract(client):
    """Validate /api/parks/downtime response schema unchanged"""
    response = client.get('/api/parks/downtime?period=today')

    assert response.status_code == 200
    data = response.json

    # Validate response structure
    assert 'data' in data
    assert 'metadata' in data

    # Validate metadata fields
    assert 'period' in data['metadata']
    assert 'timestamp' in data['metadata']
    assert 'count' in data['metadata']

    # Validate data fields (first park)
    if data['data']:
        park = data['data'][0]
        assert 'park_id' in park
        assert 'park_name' in park
        assert 'shame_score' in park
        assert 'total_downtime_minutes' in park
        assert 'uptime_percentage' in park

    # Validate data types
    assert isinstance(data['data'], list)
    assert isinstance(data['metadata']['period'], str)
    assert isinstance(data['metadata']['count'], int)
```

### Regression Testing

**Before and After Comparison**:
1. Capture API responses with current raw SQL implementation
2. Deploy ORM implementation
3. Capture API responses with ORM implementation
4. Compare JSON responses (MUST be identical)

---

## Deployment Checklist

Before deploying ORM refactoring:

- [ ] All contract tests pass (existing tests run against ORM code)
- [ ] Manual API testing confirms identical responses for all periods (today, yesterday, last_week)
- [ ] Load testing confirms <500ms response times maintained
- [ ] Frontend developer confirms no API changes detected
- [ ] OpenAPI schema validation passes (if present)

---

## Non-Functional Changes (Internal Only)

These changes are **invisible to the frontend**:

### ✅ Allowed (Internal Refactoring)
- Replacing raw SQL with ORM queries
- Changing repository method internal logic
- Adding ORM models and relationships
- Dropping hourly_stats table (served via on-the-fly queries)
- Adding composite indexes for performance
- Changing database connection management (raw cursor → SQLAlchemy session)

### ❌ Prohibited (Breaking Changes)
- Changing endpoint URLs (`/api/parks/downtime` → anything else)
- Changing query parameter names (`period` → `time_period`)
- Changing response field names (`shame_score` → `downtime_score`)
- Changing response field data types (`shame_score: float` → `string`)
- Removing response fields
- Changing error response format

---

## Summary

**API Contract Preservation: MANDATORY**

- ✅ All Flask routes unchanged
- ✅ All query parameters unchanged
- ✅ All response schemas unchanged
- ✅ Repository pattern maintains function signatures
- ✅ Contract tests validate API parity
- ✅ Frontend requires ZERO changes

**ORM refactoring is purely internal** - frontend is unaware of SQL → ORM migration.
