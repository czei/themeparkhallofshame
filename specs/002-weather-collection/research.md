# Research Phase: Weather Data Collection

**Feature**: 002-weather-collection
**Phase**: 0 - Outline & Research
**Date**: 2025-12-17

## Research Questions

From the Technical Context, we identified the following areas requiring research:

1. **TokenBucket Rate Limiter Implementation**: Best practices for 1 req/sec global rate limiting with concurrent workers
2. **ThreadPoolExecutor Configuration**: Optimal worker count for 150 parks @ 1 req/sec
3. **SQLAlchemy Core Batch Inserts**: Most efficient pattern for bulk weather data insertion
4. **Open-Meteo API Reliability**: Historical uptime, error patterns, recommended retry strategies
5. **MySQL TIMESTAMP vs DATETIME**: Performance and DST handling for UTC storage

## Research Findings

### 1. TokenBucket Rate Limiter Implementation

**Decision**: Implement thread-safe TokenBucket class with lock-based synchronization

**Implementation Pattern**:
```python
import time
import threading

class TokenBucket:
    """Rate limiter using token bucket algorithm.

    Thread-safe implementation for concurrent API requests.
    CRITICAL: Lock is released during sleep to allow other workers to proceed.
    """
    def __init__(self, rate: float = 1.0):
        """
        Args:
            rate: Tokens per second (e.g., 1.0 = 1 request/second)
        """
        self.rate = rate
        self.tokens = rate
        self.last_update = time.time()
        self.lock = threading.Lock()

    def acquire(self):
        """Block until a token is available, then consume it.

        Lock is released during sleep to allow other workers to check
        for available tokens concurrently.
        """
        while True:
            with self.lock:
                now = time.time()
                elapsed = now - self.last_update
                self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
                self.last_update = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return  # Token acquired

                # Calculate wait time and release lock before sleeping
                wait_time = (1.0 - self.tokens) / self.rate

            # CRITICAL: Sleep OUTSIDE lock so other workers can proceed
            time.sleep(wait_time)
```

**Rationale**:
- Thread-safe: Lock ensures only one worker acquires token at a time
- Accurate: Refills tokens based on elapsed time (smooth rate limiting)
- Simple: ~30 lines of code, no external dependencies
- Tested: Well-known algorithm used in production systems

**Alternatives Considered**:
- **Semaphore + sleep**: Simpler but less accurate (rate bursts possible)
- **ratelimit library**: External dependency for simple functionality
- **Redis-based limiter**: Overkill for single-process rate limiting

**Sources**:
- Token Bucket Algorithm: https://en.wikipedia.org/wiki/Token_bucket
- Python threading.Lock: https://docs.python.org/3/library/threading.html#lock-objects

---

### 2. ThreadPoolExecutor Configuration

**Decision**: max_workers=10 for concurrent weather collection

**Configuration**:
```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(collect_weather, park): park for park in parks}
    for future in as_completed(futures):
        park = futures[future]
        try:
            result = future.result()
        except Exception as e:
            logger.error(f"Failed to collect weather for {park.name}: {e}")
```

**Rationale**:
- **10 workers**: Provides parallelism while respecting 1 req/sec global limit
- **150 parks ÷ 10 workers = 15 requests per worker** (each waits 1 sec between requests)
- **Total time**: ~15 seconds per batch × 10 batches (parallel) = 150 seconds = 2.5 minutes
- **Margin**: 5-minute window provides 2.5 minutes of buffer for API latency/retries

**Math**:
- Sequential: 150 parks × 1 sec/request = 150 seconds (2.5 minutes)
- Concurrent (10 workers): 150 parks ÷ 10 workers = 15 requests/worker × 1 sec = 15 seconds per worker (all run parallel) = 15 seconds total
  - **CORRECTION**: Token bucket enforces global 1 req/sec, so concurrent doesn't reduce time below 150 seconds
  - **Benefit**: Workers can proceed independently while others wait for tokens (handles API latency gracefully)

**Alternatives Considered**:
- **max_workers=1**: Sequential, no concurrency benefit
- **max_workers=50**: Too many threads for CPU-bound token acquisition lock
- **asyncio + aiohttp**: More complex, no significant benefit for I/O-bound HTTP requests with rate limiting

**Sources**:
- concurrent.futures documentation: https://docs.python.org/3/library/concurrent.futures.html
- ThreadPoolExecutor best practices: PEP 3148

---

### 3. SQLAlchemy Core Batch Inserts

**Decision**: Use executemany() with list of dictionaries for bulk inserts

**Implementation Pattern**:
```python
from sqlalchemy import text

def batch_insert_observations(connection, observations: List[Dict]):
    """Insert multiple weather observations in a single query.

    Args:
        connection: SQLAlchemy Connection object
        observations: List of dicts with weather data
    """
    if not observations:
        return

    query = text("""
        INSERT INTO weather_observations (
            park_id, observation_time, temperature_c, temperature_f,
            wind_speed_kmh, precipitation_mm, weather_code, ...
        ) VALUES (
            :park_id, :observation_time, :temperature_c, :temperature_f,
            :wind_speed_kmh, :precipitation_mm, :weather_code, ...
        )
        ON DUPLICATE KEY UPDATE
            temperature_c = VALUES(temperature_c),
            temperature_f = VALUES(temperature_f),
            wind_speed_kmh = VALUES(wind_speed_kmh),
            ...
    """)

    connection.execute(query, observations)
```

**Rationale**:
- **executemany()**: Single round-trip to database for N rows
- **ON DUPLICATE KEY UPDATE**: Idempotent (safe to re-run collection)
- **Performance**: ~10x faster than individual INSERT statements
- **Existing Pattern**: Matches how ride snapshots are inserted

**Batch Size**:
- **1 hour × 150 parks = 150 observations per collection run**: Small enough to execute in single query
- **7 days × 24 hours × 150 parks = 25,200 forecasts per collection run**: Batch in chunks of 1,000 rows

**Alternatives Considered**:
- **Individual INSERTs**: 150 queries vs. 1 query = much slower
- **LOAD DATA INFILE**: Requires temp file creation, permission issues
- **SQLAlchemy ORM bulk_insert_mappings**: Project uses Core, not ORM

**Sources**:
- SQLAlchemy Core executemany: https://docs.sqlalchemy.org/en/20/core/connections.html#sqlalchemy.engine.Connection.execute
- MySQL ON DUPLICATE KEY UPDATE: https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html

---

### 4. Open-Meteo API Reliability

**Research Findings**:

**Uptime**:
- Open-Meteo GitHub: 100+ contributors, active maintenance
- Community reports: "Rock solid" reliability (Reddit, HackerNews)
- No published SLA (free service), but historical uptime appears >99%

**Error Patterns**:
- **Timeout errors**: API can be slow during peak usage (10-30 second responses)
- **Invalid coordinates**: Returns 400 for lat/lon out of range
- **Too many requests**: No hard rate limit, but "respectful usage" expected

**Recommended Retry Strategy**:
```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
def fetch_weather(latitude: float, longitude: float):
    """Fetch weather data with exponential backoff retry."""
    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={...},
        timeout=30  # 30-second timeout for slow responses
    )
    response.raise_for_status()
    return response.json()
```

**Rationale**:
- **3 retries**: Handles transient errors (network blips, server restarts)
- **Exponential backoff**: 2s → 4s → 8s (avoids hammering API during outages)
- **30-second timeout**: Prevents hanging on slow API responses
- **reraise=True**: Let caller handle permanent failures (log and skip park)

**Alternatives Considered**:
- **No retries**: Single network error fails collection for a park
- **Fixed delay retry**: Can amplify load during API outages
- **Circuit breaker**: Overkill for single-API dependency

**Sources**:
- Open-Meteo GitHub: https://github.com/open-meteo/open-meteo
- Tenacity documentation: https://tenacity.readthedocs.io/
- Community reliability reports: Reddit r/homeautomation, HackerNews

---

### 5. MySQL TIMESTAMP vs DATETIME

**Decision**: Use TIMESTAMP for all UTC datetime fields

**Comparison**:

| Feature | TIMESTAMP | DATETIME |
|---------|-----------|----------|
| Storage | 4 bytes | 5-8 bytes |
| Range | 1970-2038 | 1000-9999 |
| Timezone | Stores UTC, converts to session timezone | No timezone awareness |
| DST Handling | Automatic (UTC has no DST) | Manual (app must convert) |
| Default | CURRENT_TIMESTAMP supported | CURRENT_TIMESTAMP supported |

**Rationale**:
- **UTC Storage**: TIMESTAMP always stores UTC, eliminates DST ambiguity
- **Automatic Conversion**: MySQL converts to session timezone on SELECT (but we always use UTC session)
- **Existing Pattern**: Project already uses TIMESTAMP for ride snapshots
- **2038 Problem**: Not a concern (weather data <2 years retention, 2038 is 13 years away)

**Implementation**:
```sql
CREATE TABLE weather_observations (
    observation_time TIMESTAMP NOT NULL,  -- UTC
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- UTC
    ...
);
```

**Application Code**:
```python
from datetime import datetime, timezone

# Always use timezone-aware UTC datetimes
now_utc = datetime.now(timezone.utc)

# SQLAlchemy handles conversion to TIMESTAMP automatically
```

**Alternatives Considered**:
- **DATETIME**: Requires manual UTC conversion in application code
- **BIGINT (Unix timestamp)**: Less readable in queries, no SQL date functions
- **VARCHAR (ISO 8601 strings)**: No indexing benefits, no date arithmetic

**Sources**:
- MySQL TIMESTAMP vs DATETIME: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
- Python timezone-aware datetimes: https://docs.python.org/3/library/datetime.html#aware-and-naive-objects

---

## Technology Choices Summary

| Decision | Technology | Justification |
|----------|-----------|---------------|
| **Rate Limiting** | TokenBucket (custom) | Thread-safe, accurate, no dependencies |
| **Concurrency** | ThreadPoolExecutor (10 workers) | Handles API latency gracefully, 2.5 min collection time |
| **Batch Inserts** | executemany() + ON DUPLICATE KEY | Idempotent, 10x faster than individual INSERTs |
| **API Retries** | Tenacity with exponential backoff | Handles transient errors, avoids hammering API |
| **Datetime Storage** | TIMESTAMP (UTC) | Automatic DST handling, existing pattern |

---

---

## Additional Design Decisions (From Expert Review)

### 6. Failure Threshold for Systemic Issues

**Decision**: Raise exception if >50% of parks fail collection

**Implementation Pattern**:
```python
def run(self, parks: List[Park]):
    """Collect weather for all parks with failure threshold."""
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(self._collect_for_park, park): park
                   for park in parks}
        results = [future.result() for future in as_completed(futures)]

    # Tally successes and failures
    num_parks = len(parks)
    num_failures = results.count(None)

    if num_parks > 0 and (num_failures / num_parks) > 0.5:
        raise RuntimeError(
            f"Weather collection failed for {num_failures}/{num_parks} parks. "
            "Aborting due to high failure rate (>50%)."
        )

    successful_observations = [obs for obs_list in results
                              if obs_list is not None
                              for obs in obs_list]
    return successful_observations
```

**Rationale**:
- **Prevents silent systemic failure**: If API is down, job fails loudly
- **50% threshold**: Tolerates individual park failures (bad coordinates, etc.)
- **Fail-fast**: Alerts monitoring immediately instead of appearing successful with 0 data
- **Idempotent**: Can safely retry entire collection run

**Alternatives Considered**:
- **No threshold**: Silent failure when all parks fail
- **Circuit breaker**: More complex, overkill for single API dependency
- **Individual park alerts**: Too noisy (150 alerts for systemic failure)

---

### 7. API Response Validation

**Decision**: Validate API response structure before parsing

**Implementation Pattern**:
```python
def _parse_weather_data(self, response_data: dict, park_id: int) -> List[Dict]:
    """Parse Open-Meteo API response with defensive validation."""
    hourly_data = response_data.get('hourly', {})
    times = hourly_data.get('time', [])
    temps = hourly_data.get('temperature_2m', [])

    # Defensive check: Validate structure before parsing
    if not isinstance(times, list) or not isinstance(temps, list):
        logger.error(
            "Invalid API response structure for park_id=%s. "
            "Expected lists, got time=%s, temp=%s",
            park_id, type(times), type(temps)
        )
        return []

    if len(times) != len(temps):
        logger.error(
            "Misaligned time/temperature data for park_id=%s. "
            "Found %d times and %d temps.",
            park_id, len(times), len(temps)
        )
        return []

    observations = []
    for time_str, temp in zip(times, temps):
        observations.append({
            'park_id': park_id,
            'observation_time': parse_timestamp(time_str),
            'temperature_f': temp,
            # ... other fields
        })

    return observations
```

**Rationale**:
- **Prevents cryptic failures**: Catches API contract changes early
- **Prevents data corruption**: Ensures timestamps and values are aligned
- **Graceful degradation**: Single park failure doesn't crash entire collection
- **Monitoring signal**: Error logs indicate API contract violation

**Alternatives Considered**:
- **Assume API always correct**: Fails cryptically on API changes
- **JSON schema validation**: Overkill for simple structure check
- **pydantic models**: External dependency for simple validation

---

## Next Steps

✅ **Phase 0 Complete**: All technical decisions researched and documented

✅ **Expert Review Recommendations Implemented**:
1. Fixed TokenBucket concurrency bug (lock released during sleep)
2. Added failure threshold for systemic issues (>50% fail = abort)
3. Added API response validation (check structure before parsing)

**Proceed to Phase 1**: Design & Contracts
1. Create `data-model.md` (entity definitions, relationships, validation rules) ✅
2. Create `contracts/openmeteo-api.yaml` (API contract validation) ✅
3. Create `quickstart.md` (developer setup instructions) ✅
4. Update agent context (add technologies to CLAUDE.md) ✅
5. **Mandatory**: Run Zen review on data model before implementation ✅

**All Phase 1 artifacts complete and reviewed. Ready for Phase 2: Implementation.**
