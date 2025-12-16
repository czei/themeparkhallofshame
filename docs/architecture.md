# Architecture & Technology Decisions

This document explains the technology choices and architecture for the Theme Park Hall of Shame project.

| | |
|---|---|
| **Deployment Target** | Linux with Apache/Gunicorn |
| **Database** | MySQL/MariaDB |
| **Backend** | Python 3.11+ / Flask |
| **Frontend** | Static HTML/CSS/JS |

---

## Overview

This document outlines technology decisions for building a production-ready theme park tracking system with the following constraints:

- **Data Collection**: Queue-Times.com API every 10 minutes
- **Storage**: MySQL with 24-hour raw retention + permanent aggregates
- **API Layer**: REST API serving frontend queries
- **Deployment**: AWS Linux with Apache (organizational standard)
- **Performance Targets**: <100ms API responses, <5min collection cycles

**Key Decisions**:
1. Flask for REST API (native WSGI compatibility with Apache/mod_wsgi)
2. SQLAlchemy Core + mysqlclient for database layer (connection pooling + performance)
3. System cron for scheduling (reliability + separation of concerns)
4. Production patterns: tenacity for retries, structured logging, AWS SSM for config

---

## System Architecture Diagram

```mermaid
graph TB
    subgraph prod["Production Server"]
        subgraph apache["Apache Port 80/443"]
            Apache[Apache VirtualHost]
            Static["Static Files<br>var/www/themeparkhallofshame"]
            Proxy["API Proxy<br>/api to 127.0.0.1:5001"]
        end

        subgraph systemd["Systemd Service"]
            PreValidation["ExecStartPre<br>pre-service-validate.sh"]
            Gunicorn["Gunicorn WSGI Server<br>2 workers port 5001<br>CPU 30% RAM 512MB"]
            Flask["Flask API Application<br>wsgi:application"]
        end

        subgraph storage["Data Storage"]
            MariaDB[("MariaDB<br>themepark_tracker")]
            Logs["Logs Directory<br>access.log<br>error.log<br>cron_wrapper.log"]
        end

        subgraph cron["Cron Jobs Wrapped"]
            Collect["Every 10 min<br>collect_snapshots<br>timeout 300s"]
            HourlyAgg["Hourly at 05 past<br>aggregate_hourly<br>timeout 1800s"]
            DailyAgg["Daily 1 AM PT<br>aggregate_daily<br>timeout 1800s"]
            HealthCheck["Every hour<br>check_data_collection<br>timeout 300s"]
            QualityAlert["Daily 8 AM PT<br>send_data_quality_alert<br>timeout 180s"]
        end

        CronWrapper["cron_wrapper.py<br>Captures failures<br>Sends email alerts"]
    end

    subgraph external["External Services"]
        SendGrid["SendGrid Email API<br>Failure Alerts"]
        QueueTimes["Queue-Times.com API<br>Ride Status Data"]
    end

    subgraph client["Client"]
        Browser["Web Browser<br>themeparkhallofshame.com"]
    end

    %% Connections
    Browser -->|HTTP/HTTPS| Apache
    Apache --> Static
    Apache --> Proxy
    Proxy -->|127.0.0.1:5001| Gunicorn
    PreValidation -.->|validates before start| Gunicorn
    Gunicorn --> Flask
    Flask --> MariaDB
    Flask --> Logs

    Collect --> CronWrapper
    HourlyAgg --> CronWrapper
    DailyAgg --> CronWrapper
    HealthCheck --> CronWrapper
    QualityAlert --> CronWrapper

    CronWrapper -->|on failure| SendGrid
    CronWrapper --> Logs
    CronWrapper --> MariaDB

    Collect -->|fetches data| QueueTimes

    %% Styling
    classDef validation fill:#ff9999,stroke:#cc0000,stroke-width:2px
    classDef security fill:#99ccff,stroke:#0066cc,stroke-width:2px
    classDef monitoring fill:#99ff99,stroke:#00cc00,stroke-width:2px

    class PreValidation validation
    class Gunicorn security
    class CronWrapper,SendGrid monitoring
```

**Legend:**
- 🔴 Red: Validation components (pre-service checks)
- 🔵 Blue: Security hardening (resource limits)
- 🟢 Green: Monitoring (failure alerts)

---

## 1. REST API Framework: Flask vs FastAPI

### Decision: **Flask**

### Rationale

Flask is the correct choice for this stack due to the hard requirement for Apache with mod_wsgi. Flask is a **WSGI-native** framework designed to work seamlessly with WSGI servers like Apache/mod_wsgi or Gunicorn. FastAPI is an **ASGI** framework built for asynchronous workloads. While you can technically run FastAPI in a WSGI server, this approach:

- **Loses FastAPI's primary advantage**: Async I/O performance benefits are eliminated in a synchronous WSGI environment
- **Adds unnecessary complexity**: Requires an ASGI-to-WSGI translation layer (like `asgiref.wsgi`) with no performance benefit
- **Complicates deployment**: To run FastAPI effectively, you need Uvicorn/Hypercorn behind a reverse proxy (typically Nginx), not direct Apache integration

**Performance Characteristics**:
- For this read-heavy, I/O-bound workload (querying MySQL), the framework overhead is negligible
- The bottleneck is database query time (10-30ms with proper indexes), not framework processing
- The <100ms target is entirely achievable with Flask
- A well-optimized Flask app with SQLAlchemy pooling easily handles this requirement

**Framework Maturity**:
- Flask has been production-proven since 2010 with extensive Apache/mod_wsgi deployment patterns
- Massive ecosystem of extensions and middleware
- Stable, predictable behavior in enterprise environments

### Alternatives Considered

**FastAPI**:
- **Why rejected**: Fundamentally incompatible with the Apache/mod_wsgi constraint without architectural compromises
- To run FastAPI effectively requires Uvicorn (ASGI server) behind Nginx reverse proxy
- Adding this proxy layer contradicts the Apache requirement
- Would need to justify overhauling infrastructure standards for marginal benefit

**Django REST Framework**:
- **Why rejected**: Overly heavy for this use case
- Django's ORM, admin interface, and template system are unused overhead
- Slower startup time and higher memory footprint
- Flask provides the right level of abstraction for a focused API

### Implementation Notes

**Apache mod_wsgi Configuration** (Critical):

Always use **daemon mode** instead of embedded mode:
- Isolates application processes from Apache's main process
- Allows application reload without restarting Apache
- Better control over process user/group permissions
- Superior resource isolation

**Configuration Example** (`/etc/httpd/conf.d/themeparkhallofshame.conf`):

```apache
# Ensure mod_wsgi is loaded
LoadModule wsgi_module modules/mod_wsgi.so

# Define the WSGI daemon process
# python-home points to the virtual environment
WSGIDaemonProcess themeparkhallofshame \
    python-home=/var/www/themeparkhallofshame/venv \
    python-path=/var/www/themeparkhallofshame \
    processes=4 \
    threads=2 \
    maximum-requests=1000 \
    display-name=%{GROUP}

# Map the WSGI script to the root URL
WSGIScriptAlias / /var/www/themeparkhallofshame/app.wsgi

# Configure the application directory
<Directory /var/www/themeparkhallofshame>
    WSGIProcessGroup themeparkhallofshame
    WSGIApplicationGroup %{GLOBAL}
    Require all granted
</Directory>
```

**WSGI Entry Point** (`app.wsgi`):

```python
#!/usr/bin/env python3
import sys
import os

# Add application directory to Python path
sys.path.insert(0, '/var/www/themeparkhallofshame')

# Activate virtual environment
activate_this = '/var/www/themeparkhallofshame/venv/bin/activate_this.py'
with open(activate_this) as file_:
    exec(file_.read(), dict(__file__=activate_this))

# Import the Flask application
from app import create_app
application = create_app()
```

**Flask Application Structure** (Production Pattern):

```python
# app/__init__.py
from flask import Flask
from .extensions import db
from .api import api_bp

def create_app():
    app = Flask(__name__)
    app.config.from_object('config.ProductionConfig')

    # Initialize extensions
    db.init_app(app)

    # Register blueprints
    app.register_blueprint(api_bp, url_prefix='/api')

    return app

# app/api/parks.py
from flask import Blueprint, jsonify, request
from pydantic import BaseModel, ValidationError
from ..extensions import db
from sqlalchemy import text

api_bp = Blueprint('api', __name__)

class ParkDowntimeRequest(BaseModel):
    period: str = 'today'  # today|7days|30days
    filter_type: str = 'all'  # all|disney_universal

@api_bp.route('/parks/downtime', methods=['GET'])
def get_park_downtime():
    try:
        req_data = ParkDowntimeRequest.parse_obj(request.args)
    except ValidationError as e:
        return jsonify({"error": "Invalid parameters", "details": e.errors()}), 400

    # Use parameterized queries to prevent SQL injection
    with db.engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT park_id, park_name, total_downtime_hours, affected_rides
                FROM daily_park_stats
                WHERE stat_date >= DATE_SUB(NOW(), INTERVAL :days DAY)
                ORDER BY total_downtime_hours DESC
            """),
            {"days": 1 if req_data.period == 'today' else (7 if req_data.period == '7days' else 30)}
        )
        parks = [dict(row._mapping) for row in result]

    return jsonify(parks)
```

**Input Validation with Pydantic**:

While FastAPI has built-in Pydantic integration, Flask works excellently with Pydantic for request validation:

```python
from pydantic import BaseModel, Field, validator

class WaitTimeRequest(BaseModel):
    ride_id: int = Field(..., gt=0)
    period: str = Field(default='live')
    max_results: int = Field(default=100, le=500)

    @validator('period')
    def validate_period(cls, v):
        allowed = {'live', '7days', 'peak'}
        if v not in allowed:
            raise ValueError(f'period must be one of {allowed}')
        return v
```

**Common Gotchas**:

1. **Virtual Environment Path**: Ensure `python-home` in mod_wsgi config points to your venv, not system Python
2. **File Permissions**: Apache user (typically `apache` or `www-data`) needs read access to application files
3. **Process Reloading**: Touch the `.wsgi` file to reload application without restarting Apache
4. **Error Logging**: Configure Flask logging to write to a file accessible by Apache user

```python
# config.py
import logging
from logging.handlers import RotatingFileHandler

class ProductionConfig:
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    @staticmethod
    def init_app(app):
        # Production logging
        file_handler = RotatingFileHandler(
            '/var/log/themeparkhallofshame/app.log',
            maxBytes=10485760,  # 10MB
            backupCount=10
        )
        file_handler.setLevel(logging.INFO)
        app.logger.addHandler(file_handler)
```

---

## 2. Database Layer: pymysql vs SQLAlchemy vs mysqlclient

### Decision: **SQLAlchemy Core with mysqlclient driver**

### Rationale

This combination provides the optimal balance of performance, maintainability, and production-readiness:

**SQLAlchemy Core** (not the full ORM):
- **Connection pooling**: Production-grade `QueuePool` dramatically reduces latency for concurrent requests
- **SQL Expression Language**: Programmatic query building without ORM overhead
- **Transaction management**: Built-in context managers for safe transaction handling
- **Abstraction without bloat**: More structure than raw drivers, less overhead than full ORM

**mysqlclient driver**:
- **Performance**: C-based wrapper around libmysqlclient - significantly faster than pure-Python pymysql
- **Battle-tested**: MySQL-recommended Python driver
- **AWS RDS compatibility**: Full support for RDS MySQL features including SSL and IAM auth

**Why Connection Pooling Matters**:
- Without pooling, each API request creates a new MySQL connection (100-200ms overhead)
- With pooling, connections are reused (<1ms to acquire from pool)
- For concurrent requests, this is the difference between 50ms and 250ms response times

### Performance Characteristics

**Connection Pool Configuration**:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "mysql+mysqlclient://user:pass@host/db",
    pool_size=10,              # Connections kept open
    max_overflow=20,           # Additional connections allowed under load
    pool_recycle=3600,         # Recycle connections after 1 hour (RDS best practice)
    pool_pre_ping=True,        # Verify connection health before use
    pool_timeout=30,           # Max wait time for available connection
    echo=False                 # Set True for SQL debugging
)
```

**Benchmark Comparison** (typical query with connection overhead):
- **mysqlclient with pooling**: 10-15ms total (1ms pool acquire + 10ms query)
- **pymysql with pooling**: 15-25ms total (1ms pool acquire + 15ms query - pure Python slower)
- **mysqlclient without pooling**: 110-150ms (100ms connection + 10ms query)

### Alternatives Considered

**Raw pymysql**:
- **Why rejected**: Pure Python implementation ~30% slower than C-based mysqlclient
- No built-in connection pooling (would need to implement manually)
- For high-throughput APIs, the performance gap is measurable

**Raw mysqlclient**:
- **Why rejected**: Just a driver with no pooling or query builder
- Would need to manually implement connection pooling (complex and error-prone)
- SQLAlchemy's pooling is production-hardened with connection health checks

**SQLAlchemy ORM**:
- **Why rejected**: Unnecessary overhead for this use case
- ORM session management and object translation not needed for bulk inserts and aggregations
- Data collection jobs often better expressed in raw SQL than ORM abstractions
- The Core Expression Language provides enough structure without ORM complexity

### Implementation Notes

**AWS RDS Best Practices**:

1. **SSL Encryption** (production requirement):

```python
import os
from sqlalchemy import create_engine

# Download RDS CA bundle from AWS
# https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem

DATABASE_URI = f"mysql+mysqlclient://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}"

engine = create_engine(
    DATABASE_URI,
    connect_args={
        'ssl': {
            'ca': '/etc/pki/tls/certs/rds-ca-2019-root.pem'
        }
    },
    pool_recycle=3600
)
```

2. **IAM Database Authentication** (optional, enhanced security):

```python
import boto3
from sqlalchemy import create_engine

def get_iam_token():
    """Generate temporary password using IAM"""
    client = boto3.client('rds')
    token = client.generate_db_auth_token(
        DBHostname=os.environ['DB_HOST'],
        Port=3306,
        DBUsername=os.environ['DB_USER']
    )
    return token

# Use token as password
DATABASE_URI = f"mysql+mysqlclient://{os.environ['DB_USER']}:{get_iam_token()}@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}"
```

**Installation on Amazon Linux 2**:

```bash
# Install development libraries required for mysqlclient
sudo yum install python3-devel mysql-devel gcc

# Install Python packages
pip install SQLAlchemy mysqlclient
```

**Centralized Database Module** (`app/extensions.py`):

```python
import os
from sqlalchemy import create_engine, text

class Database:
    def __init__(self):
        self.engine = None

    def init_app(self, app):
        """Initialize database connection with Flask app context"""
        db_uri = app.config.get('SQLALCHEMY_DATABASE_URI')
        self.engine = create_engine(
            db_uri,
            pool_size=10,
            max_overflow=20,
            pool_recycle=3600,
            pool_pre_ping=True,
            echo=app.debug
        )

    def execute_query(self, query, params=None):
        """Execute read query and return results as dicts"""
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return [dict(row._mapping) for row in result]

    def execute_write(self, query, params=None):
        """Execute write query with automatic transaction"""
        with self.engine.begin() as conn:
            result = conn.execute(text(query), params or {})
            return result.rowcount

# Global instance
db = Database()
```

**Usage in API Endpoints**:

```python
from app.extensions import db

@api_bp.route('/rides/<int:ride_id>/status')
def get_ride_status(ride_id):
    results = db.execute_query(
        """
        SELECT r.name, s.computed_is_open, s.wait_time, s.recorded_at
        FROM rides r
        JOIN ride_status_snapshots s ON r.id = s.ride_id
        WHERE r.id = :ride_id
        ORDER BY s.recorded_at DESC
        LIMIT 1
        """,
        {"ride_id": ride_id}
    )

    if not results:
        return jsonify({"error": "Ride not found"}), 404

    return jsonify(results[0])
```

**Data Collection Script Pattern**:

```python
from sqlalchemy import create_engine, text
import os

engine = create_engine(os.environ['DATABASE_URL'], pool_recycle=3600)

def insert_ride_snapshots(snapshots):
    """Bulk insert ride status snapshots"""
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO ride_status_snapshots
                (ride_id, api_is_open, api_wait_time, computed_is_open, api_last_updated, recorded_at)
                VALUES (:ride_id, :api_is_open, :wait_time, :computed_is_open, :api_last_updated, NOW())
            """),
            snapshots  # List of dicts
        )
```

**Common Gotchas**:

1. **Connection Recycling**: RDS drops idle connections after 8 hours - use `pool_recycle=3600`
2. **Pre-ping Health Checks**: Enable `pool_pre_ping=True` to detect stale connections
3. **Transaction Context**: Use `engine.begin()` for writes to ensure proper commit/rollback
4. **Parameter Binding**: Always use parameterized queries with `text()` to prevent SQL injection
5. **Result Mapping**: Use `row._mapping` to convert SQLAlchemy Row to dict for JSON serialization

---

## 3. Job Scheduling: Python Libraries vs System Cron

### Decision: **System cron**

### Rationale

For fixed-interval scheduled tasks, system-level cron is superior to application-level schedulers:

**Reliability**:
- Cron is managed by the operating system - it's one of the most reliable components in Linux
- No long-running daemon process to monitor or manage
- Survives application crashes without impacting scheduled execution
- Decades of production hardening across millions of servers

**Separation of Concerns**:
- Data collection is independent of the web application
- Web server crashes don't affect data collection
- Collection script failures don't impact API availability
- Clear architectural boundary between collection and serving layers

**Resource Efficiency**:
- No persistent Python process consuming memory 24/7
- Script runs only when needed (10 minutes every hour)
- OS handles process lifecycle and resource cleanup

**Operational Simplicity**:
- No additional systemd service to configure for scheduler daemon
- Standard Linux tooling for monitoring (`crontab -l`, logs)
- Easier debugging - just run the script manually

### Alternatives Considered

**APScheduler**:
- **Why rejected**: Requires running a persistent Python daemon process
- That daemon becomes another point of failure requiring monitoring
- For simple fixed intervals, it's architectural complexity without benefit
- Better suited for dynamic scheduling ("run this job 5 hours from now")

**Python `schedule` library**:
- **Why rejected**: Same issues as APScheduler
- Lightweight but not designed for production daemon processes
- No built-in daemonization, monitoring, or failure recovery

**AWS EventBridge + Lambda**:
- **Why rejected**: Introduces serverless complexity
- Lambda cold starts could violate <5min collection window
- Additional AWS service dependencies and costs
- Collection script is simple enough to run directly on the server

### Implementation Notes

**Crontab Configuration**:

```bash
# Edit crontab with: crontab -e

# Data Collection - Every 10 minutes
*/10 * * * * /usr/bin/flock -n /tmp/data_collector.lock /var/www/themeparkhallofshame/venv/bin/python /var/www/themeparkhallofshame/scripts/collect_data.py >> /var/log/themeparkhallofshame/collector.log 2>&1

# Daily Aggregation - 12:10 AM every day
10 0 * * * /usr/bin/flock -n /tmp/aggregation.lock /var/www/themeparkhallofshame/venv/bin/python /var/www/themeparkhallofshame/scripts/aggregate_daily.py >> /var/log/themeparkhallofshame/aggregation.log 2>&1

# Weekly Stats - 1:00 AM every Monday
0 1 * * 1 /var/www/themeparkhallofshame/venv/bin/python /var/www/themeparkhallofshame/scripts/aggregate_weekly.py >> /var/log/themeparkhallofshame/aggregation.log 2>&1

# Monthly Stats - 2:00 AM on the 1st of each month
0 2 1 * * /var/www/themeparkhallofshame/venv/bin/python /var/www/themeparkhallofshame/scripts/aggregate_monthly.py >> /var/log/themeparkhallofshame/aggregation.log 2>&1
```

**Critical: File Locking with `flock`**:

The `/usr/bin/flock -n` wrapper prevents overlapping executions:
- `-n` = non-blocking (exit immediately if lock held)
- If previous collection is still running, new one exits gracefully
- Prevents database deadlocks from concurrent aggregations

**Collection Script Structure** (`scripts/collect_data.py`):

```python
#!/usr/bin/env python3
"""
Data collection script for Queue-Times API
Designed to be run via cron every 10 minutes
"""
import sys
import os
import logging
from datetime import datetime

# Add project root to path
sys.path.insert(0, '/var/www/themeparkhallofshame')

from app.collectors import QueueTimesCollector
from app.extensions import db, engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main collection routine"""
    start_time = datetime.now()
    logger.info("Starting data collection")

    try:
        collector = QueueTimesCollector(engine)
        stats = collector.collect_all_parks()

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Collection completed in {duration:.2f}s - Parks: {stats['parks_collected']}, Rides: {stats['rides_updated']}")

        # Exit code 0 = success
        sys.exit(0)

    except Exception as e:
        logger.error(f"Collection failed: {e}", exc_info=True)
        # Exit code 1 = failure (useful for monitoring)
        sys.exit(1)

if __name__ == '__main__':
    main()
```

**Environment Variables in Cron**:

Cron jobs run with a minimal environment - environment variables from shell profiles are NOT loaded.

**Option 1: Source environment file in crontab**:

```bash
# At top of crontab
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin

# Source environment before each job
*/10 * * * * source /var/www/themeparkhallofshame/.env && /usr/bin/flock -n /tmp/data_collector.lock /var/www/themeparkhallofshame/venv/bin/python /var/www/themeparkhallofshame/scripts/collect_data.py >> /var/log/themeparkhallofshame/collector.log 2>&1
```

**Option 2: Load environment in Python script** (recommended):

```python
from dotenv import load_dotenv
load_dotenv('/var/www/themeparkhallofshame/.env')
```

**Monitoring Cron Job Health**:

**"Dead Man's Switch" Pattern**:

```python
# At end of successful collection
import requests

def ping_healthcheck():
    """Notify monitoring service of successful completion"""
    healthcheck_url = os.environ.get('HEALTHCHECK_URL')
    if healthcheck_url:
        try:
            requests.get(healthcheck_url, timeout=10)
        except:
            pass  # Don't fail job if monitoring ping fails

# In main()
if stats['success']:
    ping_healthcheck()
```

Services like Healthchecks.io expect a ping every N minutes - if they don't receive it, they alert you.

**Alternative: CloudWatch Custom Metrics**:

```python
import boto3

def publish_success_metric():
    cloudwatch = boto3.client('cloudwatch')
    cloudwatch.put_metric_data(
        Namespace='ThemeParkHallOfShame',
        MetricData=[{
            'MetricName': 'CollectionSuccess',
            'Value': 1,
            'Unit': 'Count',
            'Timestamp': datetime.now()
        }]
    )
```

Then create a CloudWatch Alarm that triggers if no metric received in 15 minutes.

**Common Gotchas**:

1. **Absolute Paths Required**: Cron doesn't use your shell PATH - specify full paths to Python interpreter and scripts
2. **Working Directory**: Cron jobs don't inherit your working directory - use absolute paths or `cd` first
3. **Output Redirection**: Always redirect stdout/stderr to log files (`>> /path/to/log 2>&1`)
4. **No TTY**: Cron jobs run without a terminal - don't use interactive prompts
5. **Permissions**: Ensure cron user has read/write access to scripts, logs, and lock files
6. **Time Zone**: Cron uses system timezone - be aware for multi-region deployments

**Testing Cron Jobs**:

Before adding to crontab, test the exact command:

```bash
# Test as the cron user would run it
/usr/bin/flock -n /tmp/data_collector.lock /var/www/themeparkhallofshame/venv/bin/python /var/www/themeparkhallofshame/scripts/collect_data.py

# Check exit code
echo $?  # 0 = success, non-zero = failure
```

---

## 4. Production Best Practices

### 4.1 Error Handling for External API Calls

#### Decision: **tenacity library with exponential backoff**

**Rationale**:
- Declarative retry logic via decorators (cleaner than manual try/except loops)
- Exponential backoff prevents hammering failing APIs
- Configurable stop conditions (max attempts, time limits)
- Production-proven library used by major Python projects

**Implementation**:

```bash
pip install tenacity requests
```

```python
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)

@retry(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    reraise=True
)
def fetch_park_queue_times(park_id):
    """
    Fetch queue times from Queue-Times API with automatic retries

    Retry strategy:
    - Attempt 1: immediate
    - Attempt 2: 4s wait
    - Attempt 3: 8s wait
    - Attempt 4: 16s wait
    - Attempt 5: 32s wait
    - Give up after 5 attempts and raise exception
    """
    url = f"https://queue-times.com/parks/{park_id}/queue_times.json"

    response = requests.get(url, timeout=10)
    response.raise_for_status()  # Raises HTTPError for 4xx/5xx

    return response.json()
```

**Advanced: Retry on HTTP 429 (Rate Limit)**:

```python
from tenacity import retry_if_exception, retry_if_result

def is_rate_limited(exception):
    """Check if exception is due to rate limiting"""
    return (isinstance(exception, requests.HTTPError) and
            exception.response.status_code == 429)

@retry(
    retry=retry_if_exception(is_rate_limited),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=10, max=120)
)
def fetch_with_rate_limit_handling(url):
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()
```

### 4.2 Structured Logging

#### Decision: **python-json-logger with AWS CloudWatch integration**

**Rationale**:
- JSON logs are machine-readable for automated parsing
- CloudWatch Logs can query/filter JSON fields efficiently
- Structured fields enable metric extraction and alerting
- Compatible with ELK stack, Datadog, and other observability platforms

**Implementation**:

```bash
pip install python-json-logger boto3
```

**Logging Configuration** (`app/logging_config.py`):

```python
import logging
import os
from pythonjsonlogger import jsonlogger

def setup_logging(app_name='themeparkhallofshame'):
    """Configure structured JSON logging"""

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Console handler with JSON formatting
    console_handler = logging.StreamHandler()

    # Custom JSON formatter with additional fields
    class CustomJsonFormatter(jsonlogger.JsonFormatter):
        def add_fields(self, log_record, record, message_dict):
            super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
            log_record['app'] = app_name
            log_record['environment'] = os.environ.get('ENV', 'production')
            log_record['level'] = record.levelname

    formatter = CustomJsonFormatter(
        '%(timestamp)s %(level)s %(name)s %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

# Usage in application
logger = setup_logging()
logger.info("Data collection started", extra={
    "park_count": 85,
    "collection_cycle": 144
})
```

**Example JSON Output**:

```json
{
  "timestamp": "2025-11-22T14:30:00",
  "level": "INFO",
  "name": "collector",
  "message": "Data collection started",
  "app": "themeparkhallofshame",
  "environment": "production",
  "park_count": 85,
  "collection_cycle": 144
}
```

**CloudWatch Logs Integration**:

```python
import watchtower
import logging

logger = logging.getLogger()

# Add CloudWatch handler
cloudwatch_handler = watchtower.CloudWatchLogHandler(
    log_group='/aws/themeparkhallofshame/collector',
    stream_name='data-collection',
    use_queues=True  # Batch logs for efficiency
)
logger.addHandler(cloudwatch_handler)
```

**Querying CloudWatch Logs** (JSON field filtering):

```
# Find failed collections
fields @timestamp, park_id, error_message
| filter level = "ERROR"
| filter message like /collection failed/

# Calculate average collection duration
fields @timestamp, duration_seconds
| filter message = "Collection completed"
| stats avg(duration_seconds) by bin(5m)
```

### 4.3 Configuration Management

#### Decision: **AWS Systems Manager (SSM) Parameter Store for secrets + .env for non-secrets**

**Rationale**:
- **Never commit secrets to version control** - Parameter Store prevents this
- Centralized secret management across environments
- Automatic encryption at rest for SecureString parameters
- IAM-based access control
- Free tier covers typical usage
- .env files acceptable for non-sensitive config (debug flags, feature toggles)

**Implementation**:

**Store secrets in SSM Parameter Store**:

```bash
# Store database password as SecureString
aws ssm put-parameter \
    --name "/themeparkhallofshame/prod/db_password" \
    --value "your-secure-password" \
    --type SecureString \
    --description "RDS MySQL password"

# Store API keys
aws ssm put-parameter \
    --name "/themeparkhallofshame/prod/api_key" \
    --value "your-api-key" \
    --type SecureString
```

**Fetch secrets in application** (`app/config.py`):

```python
import os
import boto3
from functools import lru_cache

@lru_cache(maxsize=128)
def get_parameter(name, decrypt=True):
    """Fetch parameter from SSM Parameter Store with caching"""
    ssm = boto3.client('ssm', region_name=os.environ.get('AWS_REGION', 'us-east-1'))

    response = ssm.get_parameter(Name=name, WithDecryption=decrypt)
    return response['Parameter']['Value']

class ProductionConfig:
    """Production configuration loaded from SSM"""

    # Public config from environment
    ENV = 'production'
    DEBUG = False

    # Secrets from SSM Parameter Store
    DB_PASSWORD = get_parameter('/themeparkhallofshame/prod/db_password')
    DB_HOST = get_parameter('/themeparkhallofshame/prod/db_host', decrypt=False)
    DB_NAME = get_parameter('/themeparkhallofshame/prod/db_name', decrypt=False)
    DB_USER = get_parameter('/themeparkhallofshame/prod/db_user', decrypt=False)

    SQLALCHEMY_DATABASE_URI = f"mysql+mysqlclient://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
```

**IAM Policy for EC2/Lambda** (grant read access):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ssm:GetParameter",
        "ssm:GetParameters"
      ],
      "Resource": "arn:aws:ssm:us-east-1:123456789012:parameter/themeparkhallofshame/prod/*"
    },
    {
      "Effect": "Allow",
      "Action": "kms:Decrypt",
      "Resource": "arn:aws:kms:us-east-1:123456789012:key/*"
    }
  ]
}
```

**Local Development with .env**:

```python
# config.py
import os
from dotenv import load_dotenv

if os.environ.get('ENV') == 'development':
    load_dotenv('.env.local')

class DevelopmentConfig:
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL', 'mysql+mysqlclient://root@localhost/themeparkhallofshame_dev')
```

**.env.local** (never commit):

```
DATABASE_URL=mysql+mysqlclient://root:password@localhost/themeparkhallofshame_dev
DEBUG=True
ENV=development
```

**.gitignore**:

```
.env
.env.local
.env.*.local
```

**Alternative: AWS Secrets Manager**:

For applications requiring automatic secret rotation:

```python
import boto3
import json

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

# Usage
db_creds = get_secret('themeparkhallofshame/prod/database')
DB_PASSWORD = db_creds['password']
```

**Why SSM over Secrets Manager for this use case**:
- Simpler API (single key-value vs JSON blob)
- No automatic rotation needed (manually rotate DB password annually)
- Free tier more generous (10,000 parameter requests/month)

### 4.4 Testing Strategy

#### Decision: **pytest with layered test approach**

**Test Pyramid**:

1. **Unit Tests** (70%): Fast, isolated, test business logic
2. **Integration Tests** (20%): Test database interactions
3. **API Tests** (10%): Test HTTP endpoints end-to-end

**Implementation**:

```bash
pip install pytest pytest-cov pytest-flask
```

**Directory Structure**:

```
tests/
├── unit/
│   ├── test_collectors.py
│   ├── test_processors.py
│   └── test_utils.py
├── integration/
│   ├── test_database.py
│   └── test_api_endpoints.py
├── fixtures/
│   ├── sample_api_response.json
│   └── test_data.sql
└── conftest.py
```

**Unit Test Example** (`tests/unit/test_processors.py`):

```python
import pytest
from app.processors import compute_ride_status

def test_compute_ride_status_with_positive_wait():
    """Ride with wait time > 0 should always be open"""
    result = compute_ride_status(api_is_open=False, wait_time=45)
    assert result is True

def test_compute_ride_status_zero_wait_and_open():
    """Ride with zero wait but is_open=True should be open"""
    result = compute_ride_status(api_is_open=True, wait_time=0)
    assert result is True

def test_compute_ride_status_closed():
    """Ride with zero wait and is_open=False should be closed"""
    result = compute_ride_status(api_is_open=False, wait_time=0)
    assert result is False

def test_compute_ride_status_api_quirk():
    """Verify handling of API quirk: wait_time > 0 overrides is_open=False"""
    # This is the specific Queue-Times API quirk
    result = compute_ride_status(api_is_open=False, wait_time=30)
    assert result is True, "Ride with wait time should be considered open regardless of is_open flag"
```

**Integration Test Example** (`tests/integration/test_database.py`):

```python
import pytest
from sqlalchemy import create_engine, text
from datetime import datetime

@pytest.fixture(scope='module')
def test_db():
    """Create test database and tables"""
    engine = create_engine('mysql+mysqlclient://root@localhost/themeparkhallofshame_test')

    # Run schema creation
    with engine.begin() as conn:
        with open('schema.sql') as f:
            conn.execute(text(f.read()))

    yield engine

    # Cleanup
    with engine.begin() as conn:
        conn.execute(text('DROP DATABASE themeparkhallofshame_test'))

def test_insert_ride_snapshot(test_db):
    """Verify ride snapshot insertion"""
    with test_db.begin() as conn:
        # Insert test park and ride
        conn.execute(text("INSERT INTO parks (id, name, country) VALUES (1, 'Test Park', 'USA')"))
        conn.execute(text("INSERT INTO rides (id, park_id, name) VALUES (1, 1, 'Test Ride')"))

        # Insert snapshot
        conn.execute(text("""
            INSERT INTO ride_status_snapshots
            (ride_id, api_is_open, api_wait_time, computed_is_open, recorded_at)
            VALUES (1, :is_open, :wait, :computed, NOW())
        """), {"is_open": False, "wait": 45, "computed": True})

        # Verify
        result = conn.execute(text("SELECT computed_is_open FROM ride_status_snapshots WHERE ride_id = 1"))
        row = result.fetchone()
        assert row[0] is True
```

**API Test Example** (`tests/integration/test_api_endpoints.py`):

```python
import pytest
from app import create_app

@pytest.fixture
def client():
    """Flask test client"""
    app = create_app()
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_get_park_downtime(client):
    """Test /api/parks/downtime endpoint"""
    response = client.get('/api/parks/downtime?period=today')

    assert response.status_code == 200
    data = response.get_json()
    assert isinstance(data, list)

    # Verify response structure
    if len(data) > 0:
        park = data[0]
        assert 'park_id' in park
        assert 'park_name' in park
        assert 'total_downtime_hours' in park

def test_invalid_period_parameter(client):
    """Test validation of period parameter"""
    response = client.get('/api/parks/downtime?period=invalid')

    assert response.status_code == 400
    data = response.get_json()
    assert 'error' in data
```

**Running Tests**:

```bash
# Run all tests with coverage
pytest --cov=app --cov-report=html

# Run only unit tests
pytest tests/unit/

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/unit/test_processors.py

# Run tests matching pattern
pytest -k "test_compute"
```

**Coverage Goals**:
- Unit tests: >80% coverage of business logic
- Integration tests: Cover all database operations
- API tests: Cover all public endpoints

**CI/CD Integration** (GitHub Actions example):

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      mysql:
        image: mysql:8.0
        env:
          MYSQL_ROOT_PASSWORD: testpass
          MYSQL_DATABASE: themeparkhallofshame_test
        ports:
          - 3306:3306

    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install -r requirements-dev.txt

      - name: Run tests
        run: pytest --cov=app --cov-fail-under=80
        env:
          DATABASE_URL: mysql+mysqlclient://root:testpass@localhost/themeparkhallofshame_test
```

---

## 5. Deployment Architecture Summary

### Application Stack

```
┌─────────────────────────────────────────────────┐
│              AWS EC2 (Amazon Linux 2)           │
│                                                 │
│  ┌───────────────────────────────────────────┐ │
│  │         Apache + mod_wsgi (WSGI)          │ │
│  │                                           │ │
│  │  ┌─────────────────────────────────────┐ │ │
│  │  │    Flask Application                │ │ │
│  │  │    - REST API Endpoints             │ │ │
│  │  │    - SQLAlchemy Core                │ │ │
│  │  │    - Pydantic Validation            │ │ │
│  │  └─────────────────────────────────────┘ │ │
│  └───────────────────────────────────────────┘ │
│                                                 │
│  ┌───────────────────────────────────────────┐ │
│  │         System Cron Jobs                  │ │
│  │                                           │ │
│  │  • Data Collection (*/10 * * * *)        │ │
│  │  • Daily Aggregation (10 0 * * *)        │ │
│  │  • Weekly Stats (0 1 * * 1)              │ │
│  │  • Monthly Stats (0 2 1 * *)             │ │
│  └───────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
                        │
                        │ (Connection Pool)
                        ▼
┌─────────────────────────────────────────────────┐
│           AWS RDS MySQL (Multi-AZ)              │
│                                                 │
│  • Connection Pooling (10 base + 20 overflow)  │
│  • SSL/TLS Encryption                          │
│  • Automated Backups                           │
└─────────────────────────────────────────────────┘
```

### Technology Summary

| Component | Technology | Key Library/Tool |
|-----------|-----------|------------------|
| **Web Framework** | Flask 3.0+ | Flask, Pydantic |
| **WSGI Server** | Apache mod_wsgi | mod_wsgi daemon mode |
| **Database Driver** | mysqlclient | C-based MySQL driver |
| **Connection Pool** | SQLAlchemy Core | QueuePool with pre-ping |
| **Job Scheduler** | System Cron | flock for concurrency control |
| **HTTP Client** | requests | tenacity for retry logic |
| **Logging** | python-json-logger | CloudWatch integration |
| **Config Management** | AWS SSM Parameter Store | boto3 SDK |
| **Testing** | pytest | pytest-flask, pytest-cov |
| **Monitoring** | CloudWatch Logs/Metrics | Custom metrics + alarms |

### Performance Targets vs Expected Results

| Metric | Target | Expected with Stack |
|--------|--------|---------------------|
| API Response (<100ms) | <100ms | 30-50ms (with indexes) |
| Collection Cycle (<5min) | <5min | 2-3min for 85 parks |
| Database Query | <50ms | 10-30ms (indexed) |
| Connection Pool Acquire | N/A | <1ms |
| Daily Aggregation | <5min | 1-2min |

---

## 6. Dependencies & Installation

### System Requirements (Amazon Linux 2)

```bash
# Update system packages
sudo yum update -y

# Install Python 3.9+
sudo yum install python3 python3-devel -y

# Install MySQL development libraries (required for mysqlclient)
sudo yum install mysql-devel gcc -y

# Install Apache and mod_wsgi
sudo yum install httpd mod_wsgi -y

# Install git for deployment
sudo yum install git -y
```

### Python Dependencies

**requirements.txt** (production):

```
Flask==3.0.0
pydantic==2.5.0
SQLAlchemy==2.0.23
mysqlclient==2.2.0
requests==2.31.0
tenacity==8.2.3
python-json-logger==2.0.7
boto3==1.29.7
python-dotenv==1.0.0
```

**requirements-dev.txt** (development/testing):

```
pytest==7.4.3
pytest-flask==1.3.0
pytest-cov==4.1.0
black==23.11.0
flake8==6.1.0
mypy==1.7.1
```

### Installation Script

```bash
#!/bin/bash
# deploy.sh - Production deployment script

set -e  # Exit on error

APP_DIR="/var/www/themeparkhallofshame"
VENV_DIR="$APP_DIR/venv"

# Create application directory
sudo mkdir -p $APP_DIR
sudo chown ec2-user:ec2-user $APP_DIR
cd $APP_DIR

# Clone repository
git clone https://github.com/yourusername/themeparkhallofshame.git .

# Create virtual environment
python3 -m venv $VENV_DIR

# Activate and install dependencies
source $VENV_DIR/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Create log directory
sudo mkdir -p /var/log/themeparkhallofshame
sudo chown ec2-user:ec2-user /var/log/themeparkhallofshame

# Configure Apache
sudo cp deployment/apache/themeparkhallofshame.conf /etc/httpd/conf.d/
sudo systemctl restart httpd

# Install cron jobs
crontab deployment/cron/crontab.txt

echo "Deployment complete!"
```

---

## 7. Monitoring & Alerting

### CloudWatch Metrics to Track

**Application Metrics**:
- `CollectionSuccess` - Binary metric (1 or 0) published after each collection
- `CollectionDuration` - Time in seconds to complete collection cycle
- `ParksCollected` - Number of parks successfully queried
- `APIErrors` - Count of Queue-Times API failures

**Infrastructure Metrics**:
- Apache access logs → Request count, latency, error rates
- RDS metrics → Connection count, CPU, storage
- EC2 metrics → CPU, memory, disk

**Alarm Examples**:

```python
# In collection script
import boto3
from datetime import datetime

cloudwatch = boto3.client('cloudwatch')

def publish_collection_metrics(success, duration, park_count, error_count):
    cloudwatch.put_metric_data(
        Namespace='ThemeParkHallOfShame',
        MetricData=[
            {
                'MetricName': 'CollectionSuccess',
                'Value': 1 if success else 0,
                'Unit': 'Count',
                'Timestamp': datetime.now()
            },
            {
                'MetricName': 'CollectionDuration',
                'Value': duration,
                'Unit': 'Seconds',
                'Timestamp': datetime.now()
            },
            {
                'MetricName': 'ParksCollected',
                'Value': park_count,
                'Unit': 'Count',
                'Timestamp': datetime.now()
            },
            {
                'MetricName': 'APIErrors',
                'Value': error_count,
                'Unit': 'Count',
                'Timestamp': datetime.now()
            }
        ]
    )
```

**CloudWatch Alarms** (via AWS Console or Terraform):

```hcl
# Terraform example
resource "aws_cloudwatch_metric_alarm" "collection_failures" {
  alarm_name          = "themeparkhallofshame-collection-failures"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = "3"
  metric_name         = "CollectionSuccess"
  namespace           = "ThemeParkHallOfShame"
  period              = "600"  # 10 minutes
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Alert when data collection fails 3 times in a row"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "slow_collections" {
  alarm_name          = "themeparkhallofshame-slow-collections"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "CollectionDuration"
  namespace           = "ThemeParkHallOfShame"
  period              = "600"
  statistic           = "Average"
  threshold           = "300"  # 5 minutes
  alarm_description   = "Alert when collection takes longer than 5 minutes"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}
```

---

## 8. Security Considerations

### API Security

**Rate Limiting** (Flask-Limiter):

```python
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"]
)

@app.route('/api/parks/downtime')
@limiter.limit("10 per minute")
def get_park_downtime():
    # Endpoint implementation
    pass
```

**SQL Injection Prevention**:
- Always use parameterized queries via SQLAlchemy `text()` with bound parameters
- Never concatenate user input into SQL strings

**CORS Configuration** (if frontend hosted separately):

```python
from flask_cors import CORS

CORS(app, resources={
    r"/api/*": {
        "origins": ["https://themeparkwaits.com"],
        "methods": ["GET", "OPTIONS"],
        "allow_headers": ["Content-Type"]
    }
})
```

### Infrastructure Security

**EC2 Security Group**:
- Ingress: Port 80/443 (HTTP/HTTPS) from 0.0.0.0/0
- Ingress: Port 22 (SSH) from bastion host only
- Egress: Port 3306 (MySQL) to RDS security group
- Egress: Port 443 (HTTPS) for SSM and Queue-Times API

**RDS Security Group**:
- Ingress: Port 3306 from EC2 security group only
- No public access

**IAM Role for EC2**:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ssm:GetParameter",
        "ssm:GetParameters"
      ],
      "Resource": "arn:aws:ssm:*:*:parameter/themeparkhallofshame/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:log-group:/aws/themeparkhallofshame/*"
    }
  ]
}
```

---

## 9. Cost Estimates (AWS us-east-1)

| Service | Specification | Monthly Cost |
|---------|--------------|--------------|
| **EC2** | t3.small (2 vCPU, 2GB RAM) | $15.18 |
| **RDS MySQL** | db.t3.micro Multi-AZ | $29.93 |
| **EBS Storage** | 20GB gp3 | $1.60 |
| **RDS Storage** | 20GB gp3 | $2.30 |
| **Data Transfer** | ~10GB/month | $0.90 |
| **CloudWatch** | Logs + Metrics (within free tier) | $0 |
| **SSM Parameter Store** | Standard parameters (free) | $0 |
| **Route 53** | Hosted zone + queries | $0.50 |
| **Total** | | **~$50/month** |

**Optimizations**:
- Use Reserved Instances (1-year) for 40% savings: ~$30/month
- Single-AZ RDS for dev/staging: -$15/month
- Spot instances for non-critical workloads

---

## 10. Next Steps

### Phase 1: Foundation (Week 1)
1. Set up AWS infrastructure (EC2, RDS, security groups)
2. Configure Apache + mod_wsgi with Flask
3. Implement database schema and migrations
4. Build basic data collection script with cron

### Phase 2: Data Pipeline (Week 2)
5. Implement Queue-Times API client with retry logic
6. Build status change detection and aggregation logic
7. Set up daily/weekly/monthly aggregation jobs
8. Configure structured logging and CloudWatch

### Phase 3: API Layer (Week 3)
9. Build REST API endpoints with Flask
10. Add request validation with Pydantic
11. Implement connection pooling and query optimization
12. Add rate limiting and security headers

### Phase 4: Testing & Monitoring (Week 4)
13. Write unit, integration, and API tests
14. Set up CloudWatch dashboards and alarms
15. Configure SSM Parameter Store for secrets
16. Load testing and performance optimization

### Phase 5: Frontend Integration (Week 5)
17. Build frontend dashboard (separate effort)
18. Integrate with Shopify via iframe
19. Add attribution and links to Queue-Times
20. User acceptance testing

---

## Appendix A: Key Files Reference

### Apache Configuration
- `/etc/httpd/conf.d/themeparkhallofshame.conf` - Virtual host config
- `/var/www/themeparkhallofshame/app.wsgi` - WSGI entry point

### Application Code
- `/var/www/themeparkhallofshame/app/__init__.py` - Flask app factory
- `/var/www/themeparkhallofshame/app/extensions.py` - Database instance
- `/var/www/themeparkhallofshame/app/api/` - API blueprints
- `/var/www/themeparkhallofshame/config.py` - Configuration classes

### Scripts
- `/var/www/themeparkhallofshame/scripts/collect_data.py` - Data collection
- `/var/www/themeparkhallofshame/scripts/aggregate_daily.py` - Daily stats
- `/var/www/themeparkhallofshame/scripts/aggregate_weekly.py` - Weekly stats

### Configuration
- `/var/www/themeparkhallofshame/.env.local` - Local development (not committed)
- AWS SSM Parameter Store - Production secrets

### Logs
- `/var/log/themeparkhallofshame/collector.log` - Collection script logs
- `/var/log/themeparkhallofshame/aggregation.log` - Aggregation job logs
- `/var/log/themeparkhallofshame/app.log` - Flask application logs
- `/var/log/httpd/access_log` - Apache access logs
- `/var/log/httpd/error_log` - Apache error logs

---

## Appendix B: Useful Commands

### Application Management

```bash
# Reload Flask app without restarting Apache
touch /var/www/themeparkhallofshame/app.wsgi

# Restart Apache
sudo systemctl restart httpd

# View Apache error logs (live)
sudo tail -f /var/log/httpd/error_log

# View application logs (live)
tail -f /var/log/themeparkhallofshame/app.log

# Test data collection script manually
/var/www/themeparkhallofshame/venv/bin/python /var/www/themeparkhallofshame/scripts/collect_data.py

# Check cron jobs
crontab -l

# Edit cron jobs
crontab -e
```

### Database Operations

```bash
# Connect to RDS MySQL
mysql -h your-rds-endpoint.rds.amazonaws.com -u admin -p themeparkhallofshame

# Check table sizes
SELECT
    table_name AS 'Table',
    ROUND(((data_length + index_length) / 1024 / 1024), 2) AS 'Size (MB)'
FROM information_schema.TABLES
WHERE table_schema = 'themeparkhallofshame'
ORDER BY (data_length + index_length) DESC;

# Check 24-hour data retention
SELECT COUNT(*), DATE(recorded_at)
FROM ride_status_snapshots
GROUP BY DATE(recorded_at);
```

### Monitoring

```bash
# Publish test metric to CloudWatch
aws cloudwatch put-metric-data \
    --namespace ThemeParkHallOfShame \
    --metric-name TestMetric \
    --value 1 \
    --region us-east-1

# Query CloudWatch Logs
aws logs tail /aws/themeparkhallofshame/collector --follow

# Check CloudWatch alarms
aws cloudwatch describe-alarms \
    --alarm-name-prefix themeparkhallofshame
```

---

**Document Prepared By**: AI Research Analysis
**Review Status**: Ready for Engineering Review
**Last Updated**: 2025-11-22
