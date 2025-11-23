# Technology Stack - Quick Reference

**Project**: Theme Park Hall of Shame
**Target**: Production deployment on AWS Linux with Apache

---

## Final Technology Decisions

### 1. REST API Framework: **Flask** ✓

**Why**: Native WSGI compatibility with Apache/mod_wsgi. FastAPI requires ASGI (incompatible with mod_wsgi without losing async benefits).

**Key Points**:
- Use Flask Blueprints for API organization
- Pydantic for request validation
- mod_wsgi daemon mode (NOT embedded mode)
- <100ms response target achievable with proper indexing

---

### 2. Database Layer: **SQLAlchemy Core + mysqlclient** ✓

**Why**: Production-grade connection pooling + C-based driver performance.

**Key Points**:
- SQLAlchemy Core (Expression Language, NOT full ORM)
- mysqlclient driver (C-based, fastest available)
- Connection pooling essential for concurrent API requests
- pool_recycle=3600 for RDS compatibility

**Performance**:
- With pooling: 10-15ms query time
- Without pooling: 110-150ms (connection overhead)

---

### 3. Job Scheduling: **System cron** ✓

**Why**: OS-managed reliability, no daemon process to monitor, separation of concerns.

**Key Points**:
- Use `flock` to prevent overlapping executions
- Absolute paths required (cron has minimal environment)
- CloudWatch "dead man's switch" for monitoring
- APScheduler/schedule rejected (unnecessary daemon complexity)

**Crontab Entry**:
```bash
*/10 * * * * /usr/bin/flock -n /tmp/data_collector.lock /var/www/themeparkhallofshame/venv/bin/python /var/www/themeparkhallofshame/scripts/collect_data.py >> /var/log/themeparkhallofshame/collector.log 2>&1
```

---

### 4. Production Best Practices

| Aspect | Technology | Rationale |
|--------|-----------|-----------|
| **API Retry Logic** | tenacity library | Exponential backoff, declarative decorators |
| **Logging** | python-json-logger | Structured logs for CloudWatch querying |
| **Configuration** | AWS SSM Parameter Store | Secrets management, no .env in production |
| **Testing** | pytest | Unit (70%) + Integration (20%) + API (10%) |

---

## Installation Quick Start

### System Dependencies (Amazon Linux 2)
```bash
sudo yum install python3 python3-devel mysql-devel gcc httpd mod_wsgi -y
```

### Python Dependencies
```bash
pip install Flask==3.0.0 SQLAlchemy==2.0.23 mysqlclient==2.2.0 pydantic==2.5.0 tenacity==8.2.3 python-json-logger==2.0.7 boto3==1.29.7 requests==2.31.0
```

---

## Performance Targets vs Expected

| Metric | Target | Expected |
|--------|--------|----------|
| API Response | <100ms | 30-50ms |
| Collection Cycle | <5min | 2-3min |
| DB Query (indexed) | <50ms | 10-30ms |

---

## Key Architectural Decisions

1. **Flask over FastAPI**: Apache/mod_wsgi requirement makes WSGI-native Flask the obvious choice
2. **SQLAlchemy Core over ORM**: Connection pooling + query flexibility without ORM overhead
3. **Cron over APScheduler**: OS reliability > application-level scheduler daemon
4. **SSM over .env**: Never commit secrets; centralized AWS management

---

## Cost Estimate

**~$50/month** (EC2 t3.small + RDS db.t3.micro Multi-AZ)

**Optimizations**:
- Reserved Instances: ~$30/month (40% savings)
- Single-AZ RDS for dev: -$15/month

---

## Next Steps

1. Week 1: Infrastructure setup (EC2, RDS, Apache, mod_wsgi)
2. Week 2: Data collection pipeline + aggregation jobs
3. Week 3: REST API with Flask + validation
4. Week 4: Testing + monitoring + CloudWatch
5. Week 5: Frontend integration + Shopify iframe

---

**Full Documentation**: See `/docs/technology-decisions.md` for detailed implementation notes, code examples, and gotchas.
