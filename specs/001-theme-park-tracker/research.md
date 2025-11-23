# Research: Technology Decisions

**Feature**: Theme Park Downtime Tracker
**Date**: 2025-11-22
**Target Platform**: Co-located on existing webperformance.com AWS server (Apache, MySQL)

---

## Decision 1: REST API Framework

### Decision: **Flask 3.0+**

### Rationale:
Flask is WSGI-native and integrates seamlessly with Apache via mod_wsgi, which is required by the project specification. FastAPI, while modern and performant, is ASGI-based and would lose its async advantages when forced to run through WSGI adapters. For the <100ms API response requirement, Flask is more than sufficient when combined with proper database indexing and connection pooling. The framework overhead (2-5ms) is negligible compared to database query time (10-30ms for indexed queries).

### Alternatives Considered:

**FastAPI (Rejected)**:
- **Pros**: Built-in OpenAPI docs, Pydantic validation, async support
- **Cons**: ASGI-based, incompatible with mod_wsgi without losing async benefits, requires Uvicorn/Gunicorn which adds deployment complexity with Apache
- **Verdict**: Not suitable for Apache/mod_wsgi requirement

**Django REST Framework (Rejected)**:
- **Pros**: Full-featured, admin interface, ORM
- **Cons**: Heavy for API-only service, unnecessary features (templates, forms, admin), slower than Flask for simple JSON APIs
- **Verdict**: Overkill for this use case

### Implementation Notes:
- Use **Flask Blueprints** to organize routes (`/api/parks`, `/api/rides`, `/api/health`)
- Integrate **Pydantic** for request/response validation (same validation library as FastAPI)
- Deploy with **mod_wsgi daemon mode** (not embedded) for better process isolation
- Use **Flask-CORS** for frontend AJAX calls
- Connection pooling via SQLAlchemy is critical for meeting <100ms target

---

## Decision 2: Database Library

### Decision: **SQLAlchemy Core + mysqlclient**

### Rationale:
SQLAlchemy Core (Expression Language API, not the full ORM) provides production-grade connection pooling with RDS-specific optimizations (connection recycling, health checks). The mysqlclient driver is C-based and ~30% faster than pure-Python alternatives like pymysql. Connection pooling is critical: without it, every API request incurs 100-200ms overhead to establish a new MySQL connection. With pooling, connection acquisition is <1ms.

**Performance Impact**:
- **Without pooling**: 110-150ms per request (connection overhead + query time)
- **With pooling**: 10-30ms per request (query time only)

### Alternatives Considered:

**pymysql (Rejected)**:
- **Pros**: Pure Python (no C compilation), widely used
- **Cons**: 30% slower than mysqlclient, no significant advantages for production
- **Verdict**: Performance loss not justified

**mysqlclient alone (Rejected)**:
- **Pros**: Fastest driver available
- **Cons**: No connection pooling built-in, manual connection management error-prone
- **Verdict**: Need SQLAlchemy's pooling for production reliability

**SQLAlchemy Full ORM (Rejected)**:
- **Pros**: Rich query API, relationship management
- **Cons**: Unnecessary overhead for this project (mostly raw aggregates and time-series queries), adds learning curve
- **Verdict**: Core Expression Language is sufficient and more explicit

### Implementation Notes:
- Configure `pool_recycle=3600` for MySQL (recycle connections before MySQL timeout)
- Use `pool_pre_ping=True` to health-check connections before use
- Pool size: `pool_size=10, max_overflow=20` for expected load
- Transaction management for aggregation jobs (atomicity)
- Local development: Connect to localhost MySQL instance
- Production: Connect to MySQL on same server (localhost or Unix socket)

---

## Decision 3: Job Scheduling

### Decision: **System cron**

### Rationale:
For fixed 10-minute intervals, OS-level cron is superior to application-level schedulers. It's rock-solid reliable (managed by the OS, not a Python daemon), separates concerns (data collection independent of web server), and requires no persistent process to monitor. The simplicity aligns with project constitution principle of avoiding unnecessary complexity.

**Reliability**: OS cron has been production-proven since 1975. It won't crash, doesn't need restarts, and survives system reboots.

### Alternatives Considered:

**APScheduler (Rejected)**:
- **Pros**: Python-native, flexible scheduling, job persistence
- **Cons**: Requires persistent daemon process, adds complexity (monitoring, restart policies), overkill for simple fixed intervals
- **Verdict**: Unnecessary daemon for fixed schedule

**Python `schedule` library (Rejected)**:
- **Pros**: Simple API, easy to understand
- **Cons**: Also requires daemon process, no missed-job handling, less reliable than OS cron
- **Verdict**: Not production-grade for critical data collection

**Celery Beat (Rejected)**:
- **Pros**: Distributed task queue, retry logic
- **Cons**: Massive overkill (requires Redis/RabbitMQ broker), operational complexity
- **Verdict**: Way too complex for this project

### Implementation Notes:
- Use **flock** to prevent overlapping executions: `/usr/bin/flock -n /tmp/collector.lock script.py`
- Absolute paths in crontab (cron has minimal $PATH)
- Environment variables via script wrapper or cron shell config
- CloudWatch "dead man's switch" monitoring (alarm if no collection in 15 minutes)
- Separate cron jobs for: data collection (*/10), daily aggregation (0 10 * * *), cleanup (0 15 * * *)

**Sample Crontab**:
```bash
# Data collection every 10 minutes
*/10 * * * * /usr/bin/flock -n /tmp/collector.lock /var/www/venv/bin/python /var/www/scripts/collect.py >> /var/log/collector.log 2>&1

# Daily aggregation at 12:10 AM
10 0 * * * /var/www/venv/bin/python /var/www/scripts/aggregate_daily.py >> /var/log/aggregation.log 2>&1
```

---

## Decision 4: API Retry Logic

### Decision: **tenacity library**

### Rationale:
The tenacity library provides declarative retry decorators with exponential backoff, jitter, and custom retry conditions. It's more robust than manual retry loops and handles edge cases (network timeouts, transient errors, rate limits).

### Implementation Pattern:
```python
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError))
)
def fetch_queue_times(park_id):
    response = requests.get(f"https://queue-times.com/parks/{park_id}/queue_times.json", timeout=10)
    response.raise_for_status()
    return response.json()
```

### Alternatives Considered:
- **Manual retry loops**: Error-prone, missing edge cases, hard to maintain
- **urllib3 Retry**: Lower-level, requires more configuration
- **Verdict**: tenacity provides best balance of power and simplicity

---

## Decision 5: Logging Strategy

### Decision: **python-json-logger (structured logging)**

### Rationale:
CloudWatch Logs Insights requires structured JSON for querying and alerting. Plain-text logs are difficult to parse and analyze. JSON logs enable queries like "show all API errors in the last hour" or "graph collection cycle duration over time."

### Implementation Pattern:
```python
from pythonjsonlogger import jsonlogger
import logging

logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter('%(asctime)s %(name)s %(levelname)s %(message)s')
logHandler.setFormatter(formatter)

logger = logging.getLogger()
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

# Usage
logger.info("Collection completed", extra={
    "park_count": 85,
    "duration_seconds": 142,
    "rides_updated": 1247
})
```

**Output**:
```json
{"asctime": "2025-11-22 10:23:14", "name": "root", "levelname": "INFO", "message": "Collection completed", "park_count": 85, "duration_seconds": 142, "rides_updated": 1247}
```

### Alternatives Considered:
- **Plain text logging**: No structured querying in CloudWatch
- **Custom JSON formatting**: Reinventing the wheel
- **Verdict**: python-json-logger is battle-tested and standardized

---

## Decision 6: Configuration Management

### Decision: **AWS Systems Manager (SSM) Parameter Store**

### Rationale:
Never commit credentials to version control. SSM Parameter Store provides centralized secrets management with IAM access control, audit logging, and encryption at rest. For local development, fall back to `.env` file (via python-dotenv), but production always uses SSM.

### Implementation Pattern:
```python
import boto3
import os

def get_config(key, default=None):
    """Fetch config from SSM (production) or environment (local)."""
    if os.getenv("ENVIRONMENT") == "production":
        ssm = boto3.client('ssm', region_name='us-east-1')
        try:
            response = ssm.get_parameter(Name=f"/themeparkhall/{key}", WithDecryption=True)
            return response['Parameter']['Value']
        except Exception as e:
            if default is not None:
                return default
            raise
    else:
        return os.getenv(key, default)

# Usage
DB_PASSWORD = get_config("DB_PASSWORD")
QUEUE_TIMES_API_KEY = get_config("QUEUE_TIMES_API_KEY", default="")
```

### Alternatives Considered:
- **.env files in production**: Security risk (committed to repo, exposed in logs)
- **AWS Secrets Manager**: More expensive ($0.40/secret/month vs SSM free tier), overkill for this project
- **Hardcoded values**: Unacceptable security practice
- **Verdict**: SSM strikes best balance of security and cost

---

## Decision 7: Testing Strategy

### Decision: **pytest with 70/20/10 split**

### Test Pyramid:
- **70% Unit Tests**: Fast, isolated tests of business logic (status calculation, aggregation, operating hours detection)
- **20% Integration Tests**: Database interactions, API collection pipeline
- **10% API Contract Tests**: Validate OpenAPI spec compliance

### Implementation Pattern:
```python
# Unit test example
def test_computed_is_open():
    assert computed_is_open(wait_time=45, is_open=False) == True  # wait_time overrides
    assert computed_is_open(wait_time=0, is_open=True) == True    # open with no wait
    assert computed_is_open(wait_time=0, is_open=False) == False  # closed

# Integration test example (with test database)
def test_collection_pipeline(test_db):
    # Mock Queue-Times API
    with patch('requests.get') as mock_get:
        mock_get.return_value.json.return_value = sample_api_response

        # Run collection
        collect_data()

        # Verify database state
        snapshots = test_db.query(RideStatusSnapshot).all()
        assert len(snapshots) == 50
```

### Tools:
- **pytest-cov**: Code coverage >80% requirement
- **pytest-mock**: Mocking API calls
- **pytest-flask**: Flask app testing
- **OpenAPI validator**: Contract test against API spec

---

## Summary: Technology Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| **REST API** | Flask 3.0+ | WSGI-native for Apache/mod_wsgi |
| **Database Driver** | mysqlclient | C-based performance (30% faster) |
| **Database Layer** | SQLAlchemy Core | Production connection pooling |
| **Scheduling** | System cron | OS reliability > daemon complexity |
| **Retry Logic** | tenacity | Exponential backoff patterns |
| **Logging** | python-json-logger | CloudWatch structured querying |
| **Secrets** | AWS SSM Parameter Store | Never commit credentials |
| **Testing** | pytest | 70% unit, 20% integration, 10% API |

---

## Performance Expectations

| Metric | Requirement | Expected | How Achieved |
|--------|------------|----------|--------------|
| API Response | <100ms | 30-50ms | Connection pooling + indexes |
| Collection Cycle | <5 min | 2-3 min | Async requests (tenacity retry) |
| DB Query (current) | <50ms | 10-30ms | Indexed queries on timestamps |
| DB Query (aggregate) | <100ms | 40-80ms | Pre-calculated daily/weekly stats |

---

## Cost Estimate

**Monthly AWS Cost**: ~$0 (co-located deployment)

Breakdown:
- **EC2 Instance**: $0 (using existing webperformance.com server)
- **MySQL Database**: $0 (local MySQL instance on existing server)
- **Storage + Data Transfer**: $0 (minimal incremental usage: ~500MB Year 1)

**Deployment Decision: Co-location Strategy**

After multi-model consensus analysis (Gemini-2.5-pro vs GPT-5-pro), the decision is to **co-locate on existing webperformance.com AWS server** with systemd resource limits and monitoring-based migration triggers.

**Rationale**:
- **Cost efficiency**: $0 incremental infrastructure cost vs $200-540/year for dedicated instance
- **Low traffic expectations**: Data enthusiast audience, not high-volume consumer traffic
- **Existing server has low utilization**: webperformance.com barely used
- **Clear migration path**: Can promote to dedicated instance if traffic/resource usage grows

**Safeguards**:
- **systemd resource limits**:
  - Data Collector: `CPUQuota=25%`, `MemoryMax=512M`, `IOWeight=50`
  - API Service: `CPUQuota=30%`, `MemoryMax=512M`
- **Separate MySQL database**: Dedicated schema or separate mysqld instance
- **CloudWatch monitoring with migration triggers**:
  - CPU utilization >60% sustained for 1 week
  - API traffic >1000 requests/day for 1 week
  - 6-month mandatory review

**Migration Path**:
If any trigger is met, automatic promotion to dedicated EC2 instance:
- **Option**: t3.small EC2 + local MySQL (~$200/year)
- **Transition**: Zero downtime (DNS cutover, database export/import)

---

## Deployment Architecture

**Co-location Strategy**: Theme Park Tracker runs alongside webperformance.com on the same AWS EC2 instance.

```
┌─────────────────────────────────────────────────────────────────┐
│    Existing webperformance.com AWS EC2 (Amazon Linux 2)         │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │           Apache 2.4 (Web Server)                        │   │
│  │  - webperformance.com (existing)                         │   │
│  │  - themeparkwaits.com static files (NEW)                 │   │
│  │  - mod_wsgi daemon for Flask API (NEW)                   │   │
│  └──────────────────────────────────────────────────────────┘   │
│                         ↓                                        │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │   Flask REST API (WSGI App) - NEW                        │   │
│  │   - systemd resource limits: CPUQuota=30%, MemoryMax=512M│   │
│  │   - SQLAlchemy connection pool                           │   │
│  │   - Routes: /api/parks, /api/rides, /api/trends          │   │
│  └──────────────────────────────────────────────────────────┘   │
│                         ↓                                        │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │   Cron Jobs (Data Collection) - NEW                      │   │
│  │   - systemd resource limits: CPUQuota=25%, MemoryMax=512M│   │
│  │   - */10: Fetch Queue-Times API                          │   │
│  │   - 0 0: Daily aggregation + cleanup                     │   │
│  └──────────────────────────────────────────────────────────┘   │
│                         ↓                                        │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │   Local MySQL 8.0+ Instance - NEW                        │   │
│  │   - Database: themepark_tracker_prod                     │   │
│  │   - Raw snapshots (24h retention)                        │   │
│  │   - Daily/weekly/monthly aggregates                      │   │
│  │   - Connection pooling (10 + 20 overflow)                │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │   CloudWatch Monitoring - NEW                            │   │
│  │   - CPU utilization alarm (>60% for 1 week)              │   │
│  │   - API traffic alarm (>1000 req/day)                    │   │
│  │   - Migration trigger notifications                      │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│              Queue-Times.com API                                │
│  - Fetched every 10 minutes                                     │
│  - Retry with exponential backoff (tenacity)                    │
└─────────────────────────────────────────────────────────────────┘
```

**Development Workflow**:

```
┌─────────────────────────────────────────────┐
│ Phase 1: Local Development (Weeks 1-4)     │
│ - Local MySQL database on laptop/desktop   │
│ - Full test suite (pytest >80% coverage)   │
│ - Real Queue-Times.com API testing         │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│ Phase 2: Production Deployment (Days 1-3)  │
│ - Deploy to webperformance.com server      │
│ - Set up production MySQL database         │
│ - Configure systemd services + limits      │
│ - Configure Apache VirtualHost             │
│ - Set up CloudWatch monitoring             │
└─────────────────────────────────────────────┘
```

---

## Next Phase: Data Model Design

All technology decisions are finalized and validated. Proceed to Phase 1:
1. Design MySQL schema (`data-model.md`)
2. Define REST API contracts (`contracts/api.yaml`)
3. Create local development quickstart (`quickstart.md`)
