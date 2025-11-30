# Database Options Analysis

**Date**: 2025-11-25
**Context**: Post-implementation review of database technology choices

## Current Implementation

The project uses **MySQL 8.0** as specified in `specs/001-theme-park-tracker/research.md`. This decision was infrastructure-driven rather than workload-driven - the existing webperformance.com server already runs MySQL.

## Workload Characteristics

This project is fundamentally a **time-series workload**:
- Ride status snapshots collected every 10 minutes
- ~720,000 rows/day of temporal data
- Queries primarily filter by timestamp ranges
- Aggregations over time windows (daily, weekly, monthly)
- 24-hour retention for raw data, permanent storage for aggregates

## Modern SQL-Compatible Alternatives

### TimescaleDB (PostgreSQL Extension)

**Strong candidate for this workload**

- Purpose-built for time-series data
- Automatic partitioning by time (hypertables)
- 10-100x better compression on time-series data
- SQL-compatible (PostgreSQL dialect)
- Continuous aggregation (auto-computed rollups would replace our manual aggregation jobs)
- Native time-bucket functions for analytics

**Trade-offs**:
- Requires PostgreSQL (different from existing MySQL infrastructure)
- Additional operational complexity for hypertable management
- Would need to learn PostgreSQL ecosystem

### ClickHouse

**Columnar analytical database**

- Extremely fast for aggregate queries (park rankings, downtime calculations)
- SQL-compatible (with some dialect differences)
- Better compression than row-stores for analytical data
- Excellent for "top N" queries that dominate this application

**Trade-offs**:
- Not ideal for frequent small writes (better for batch inserts)
- Different operational model than traditional RDBMS
- Less mature ecosystem for application development

### PlanetScale

**Serverless MySQL**

- MySQL-compatible (Vitess-based, same tech YouTube uses)
- Branching for schema changes (like git for databases)
- Automatic scaling and connection pooling
- Reduced operational burden

**Trade-offs**:
- Monthly cost (~$29/month for production tier)
- Vendor lock-in concerns
- Foreign key constraints handled differently

### DuckDB

**Embedded analytics engine**

- SQL-compatible
- Column-oriented for fast analytics
- Could run alongside primary database for analytics queries
- Zero operational overhead (embedded)

**Trade-offs**:
- Not designed as primary OLTP database
- Better suited as analytics complement to main database

### CockroachDB / TiDB

**Distributed SQL databases**

- MySQL/PostgreSQL compatible
- Horizontal scaling built-in
- Strong consistency guarantees

**Trade-offs**:
- Overkill for current scale (~500MB/year)
- Higher operational complexity
- Cost increases with distribution

## Why MySQL Was Chosen

From `research.md`:
```
- **MySQL Database**: $0 (local MySQL instance on existing server)
```

The decision was driven by:
1. **Existing infrastructure**: webperformance.com server already runs MySQL
2. **Zero incremental cost**: No new database service to provision
3. **Operational simplicity**: Team familiarity with MySQL
4. **Sufficient performance**: MySQL handles this workload adequately with proper indexing

## Recommendation for Future

MySQL 8.0 is adequate for current needs. However, if the project were greenfield or if scaling becomes necessary:

1. **Best fit for workload**: TimescaleDB - purpose-built for time-series with SQL compatibility
2. **Best for analytics**: ClickHouse - would dramatically speed up ranking/aggregation queries
3. **Best for reduced ops**: PlanetScale - serverless MySQL with modern DX

The 24-hour raw data retention + permanent aggregates pattern in the current design is essentially a manual implementation of what TimescaleDB's continuous aggregates do automatically.

## References

- TimescaleDB: https://www.timescale.com/
- ClickHouse: https://clickhouse.com/
- PlanetScale: https://planetscale.com/
- DuckDB: https://duckdb.org/
