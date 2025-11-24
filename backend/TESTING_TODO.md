# Testing TODO - Remaining Work

## Current Status
- **279 tests passing**, 55 skipped
- **44% coverage** (target: 80%)
- **36% remaining** to reach coverage goal

## Progress Update
**Phase 1 Quick Wins: COMPLETED** ✅
- test_statistics.py: 23 tests, 100% coverage (models/statistics.py: 0% → 100%)
- test_logger.py: 22 tests, 74% coverage (utils/logger.py: 50% → 74%)
- test_park_model.py: 15 tests, 100% coverage (models/park.py: 81% → 100%)
- test_ride_model.py: 19 tests, 100% coverage (models/ride.py: 77% → 100%)
- **Total**: 79 new tests added
- **Coverage increase**: 39% → 44% (5% gain)

## Untested Modules (0% Coverage)

### High Priority (Core Functionality)

#### 1. scripts/collect_parks.py (186 lines, 0% coverage)
**Purpose**: Fetches park data from Queue-Times API
**Why untested**: Requires API mocking, database connection
**Effort**: Medium (needs API fixtures)
**Integration test**: Yes

```python
# Will need:
- Mock Queue-Times API responses
- Test park data transformation
- Test database insertion
- Test error handling (API failures, network errors)
```

#### 2. scripts/collect_snapshots.py (127 lines, 0% coverage)
**Purpose**: Collects ride wait time snapshots
**Why untested**: Requires API mocking, database connection
**Effort**: Medium (needs API fixtures)
**Integration test**: Yes

```python
# Will need:
- Mock Queue-Times API responses
- Test snapshot data transformation
- Test batch insertion
- Test rate limiting
```

#### 3. scripts/aggregate_daily.py (131 lines, 0% coverage)
**Purpose**: Daily aggregation scheduler script
**Why untested**: Complex workflow, database operations
**Effort**: Medium (can use existing fixtures)
**Integration test**: Yes

```python
# Will need:
- Test date range processing
- Test backfill logic
- Test error recovery
- Integration with aggregation_service
```

#### 4. database/repositories/stats_repository.py (65 lines, 0% coverage)
**Purpose**: Statistics queries and aggregations
**Why untested**: Complex SQL queries, MySQL-specific
**Effort**: Medium
**Integration test**: Yes (MySQL window functions)

```python
# Will need:
- Test daily/weekly/monthly stats retrieval
- Test park rankings
- Test ride downtime leaderboards
- Test date range filtering
```

### Medium Priority (Infrastructure)

#### 5. api/middleware/auth.py (31 lines, 0% coverage)
**Purpose**: API key authentication
**Why untested**: Requires Flask app context
**Effort**: Low (Flask testing)
**Integration test**: No (unit testable)

```python
# Will need:
- Test API key validation
- Test missing API key (401)
- Test invalid API key (401)
- Test rate limiting integration
```

#### 6. api/middleware/rate_limiter.py (58 lines, 0% coverage)
**Purpose**: Rate limiting middleware
**Why untested**: Requires Flask app context, time mocking
**Effort**: Medium (needs time control)
**Integration test**: No (unit testable with mocking)

```python
# Will need:
- Test rate limit enforcement
- Test sliding window algorithm
- Test 429 responses
- Test rate limit headers
```

#### 7. collector/queue_times_client.py (38 lines, 0% coverage)
**Purpose**: Queue-Times API client wrapper
**Why untested**: Requires API mocking
**Effort**: Low (simple HTTP client)
**Integration test**: No (unit testable with requests-mock)

```python
# Will need:
- Mock HTTP requests
- Test error handling (timeout, 404, 500)
- Test response parsing
- Test retry logic
```

#### 8. models/statistics.py (77 lines, 0% coverage)
**Purpose**: Statistics data models
**Why untested**: Pure data classes
**Effort**: Very Low
**Integration test**: No (unit testable)

```python
# Will need:
- Test dataclass field validation
- Test serialization/deserialization
- Test calculation methods
```

### Lower Priority (Partial Coverage)

#### 9. database/connection.py (58 lines, 40% coverage)
**Current coverage**: Connection pooling, basic operations
**Missing coverage**: Error handling, SSL configuration, connection retry
**Effort**: Low
**Integration test**: Partial (pool management needs MySQL)

#### 10. api/routes/health.py (41 lines, 20% coverage)
**Current coverage**: Endpoint structure
**Missing coverage**: Database connectivity checks, data freshness checks
**Effort**: Low
**Integration test**: Yes (needs real database)

#### 11. classification_service.py (135 lines, 30% coverage)
**Current coverage**: Dataclass, initialization
**Missing coverage**: File I/O, classification orchestration, caching
**Effort**: High (complex logic)
**Integration test**: Yes (file I/O, MCP integration)

#### 12. aggregation_service.py (120 lines, 21% coverage)
**Current coverage**: Timezone logic, initialization
**Missing coverage**: Aggregation calculations, database operations
**Effort**: High (complex business logic)
**Integration test**: Yes (MySQL-specific SQL)

## Partially Tested Modules (50-80% Coverage)

### Can easily reach 80%+

#### 13. utils/logger.py (34 lines, 50% coverage)
**Missing**: Error logging functions, context injection
**Effort**: Very Low (just call the functions)

#### 14. models/park.py (36 lines, 81% coverage)
**Missing**: Edge case validation
**Effort**: Very Low

#### 15. models/ride.py (30 lines, 77% coverage)
**Missing**: Edge case validation
**Effort**: Very Low

## Recommended Testing Order

### Phase 1: Quick Wins (Get to 50% coverage fast) - ✅ COMPLETED
1. ✅ **models/statistics.py** - Pure dataclasses (1 hour actual)
2. ✅ **utils/logger.py** - Call logging functions (1 hour actual)
3. ✅ **models/park.py + models/ride.py** - Edge cases (1 hour actual)
4. **collector/queue_times_client.py** - Mock HTTP (2 hours)
5. **api/middleware/auth.py** - Flask testing (2 hours)

**Completed**: 3 hours → **44% coverage**
**Remaining to 50%**: 4 hours (items 4-5)

### Phase 2: Infrastructure (Get to 60% coverage)
6. **api/middleware/rate_limiter.py** - Time mocking (3 hours)
7. **database/connection.py** - Connection handling (2 hours)
8. **api/routes/health.py** - Integration tests (3 hours)

**Estimated**: 8 hours → **60% coverage**

### Phase 3: Complex Business Logic (Get to 80% coverage)
9. **database/repositories/stats_repository.py** - MySQL integration (5 hours)
10. **classification_service.py** - File I/O integration (5 hours)
11. **aggregation_service.py** - MySQL integration (5 hours)
12. **scripts/collect_parks.py** - API integration (4 hours)
13. **scripts/collect_snapshots.py** - API integration (4 hours)
14. **scripts/aggregate_daily.py** - Workflow integration (3 hours)

**Estimated**: 26 hours → **80% coverage**

## Total Effort Estimate

- **Phase 1 (Quick Wins)**: 7 hours
- **Phase 2 (Infrastructure)**: 8 hours
- **Phase 3 (Complex Logic)**: 26 hours
- **Total**: **~41 hours** to reach 80% coverage

## Testing Infrastructure Needed

### For Integration Tests

**MySQL Test Database:**
```bash
# Already documented in tests/integration/README.md
export TEST_DB_HOST=localhost
export TEST_DB_NAME=themepark_test
export TEST_DB_USER=themepark_test
export TEST_DB_PASSWORD=test_password
```

**API Mocking:**
```bash
pip install requests-mock responses
```

**Time Control:**
```bash
pip install freezegun
```

**Temporary File System:**
```python
# Already in Python stdlib
import tempfile
```

## Coverage by Category

| Category | Current | Missing | Target | Effort |
|----------|---------|---------|--------|--------|
| Models | 77% | 23% | 90% | Low |
| Utils | 73% | 27% | 90% | Low |
| Repositories | 59% | 21% | 80% | Medium |
| Processors | 63% | 17% | 80% | High |
| Classifiers | 77% | 23% | 90% | Medium |
| API | 62% | 38% | 80% | Medium |
| Scripts | 0% | 100% | 70% | High |
| **Overall** | **39%** | **41%** | **80%** | **41 hrs** |

## Next Steps

1. **Immediate (this session)**:
   - Quick wins: models/statistics.py, utils/logger.py
   - Get to 45% coverage easily

2. **Next session**:
   - API middleware tests
   - Queue-Times client tests
   - Reach 50-55% coverage

3. **Future sessions**:
   - Integration test infrastructure
   - Complex business logic
   - Scripts testing
   - Reach 80% coverage

## Notes

- **55 tests already marked** for integration phase
- **Integration fixtures ready** (tests/integration/conftest.py)
- **Documentation complete** (TESTING.md)
- **Solid foundation** established (200 passing tests)

The path to 80% coverage is clear and well-documented!
