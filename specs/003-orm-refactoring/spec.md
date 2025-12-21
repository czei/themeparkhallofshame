# Feature Specification: ORM Refactoring for Reliable Data Access

**Feature Branch**: `003-orm-refactoring`
**Created**: 2025-12-21
**Status**: Draft
**Input**: User description: "ORM refactoring"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Type-Safe Data Access Without Raw SQL (Priority: P1)

Developers need to write database queries using type-safe ORM models instead of raw SQL strings, reducing bugs from typos, schema mismatches, and SQL injection vulnerabilities.

**Why this priority**: This is the foundational benefit of ORM refactoring. Raw SQL strings are the root cause of current infrastructure fragility - typos cause runtime errors, schema changes break queries silently, and maintaining SQL across 64+ test files creates massive technical debt. Type-safe ORM eliminates this entire class of errors.

**Independent Test**: Can be fully tested by migrating one query from raw SQL to ORM (e.g., ride status query), running existing tests, and validating identical results with compile-time type safety.

**Acceptance Scenarios**:

1. **Given** a developer needs to query ride status snapshots, **When** they use ORM models instead of raw SQL, **Then** the IDE provides autocomplete for table columns and catches schema mismatches at development time (before runtime)
2. **Given** a query references a non-existent table column, **When** the developer runs type checking, **Then** the ORM raises a compile-time error (not a runtime database error)
3. **Given** the database schema changes (e.g., column renamed), **When** the developer runs the application, **Then** all affected queries are identified by the ORM at startup (not discovered through production errors)
4. **Given** a complex JOIN query across ride_status_snapshots and park_activity_snapshots, **When** written using ORM relationships, **Then** the query is easier to read, maintain, and test than equivalent raw SQL
5. **Given** a developer needs to filter rides by status, **When** using ORM query builder, **Then** SQL injection vulnerabilities are prevented automatically (parameterized queries enforced)

---

### User Story 2 - Flexible Hourly Analytics Without Pre-Calculated Tables (Priority: P1)

ML/analytics features need to query ride data across arbitrary time windows (e.g., "peak lunch hours 11am-2pm", "weekend mornings", "last 90 minutes") without being constrained to pre-calculated hourly aggregations.

**Why this priority**: Feature 004 (wait-time-analysis) requires flexible pattern discovery across custom time windows. Current hourly_stats table locks queries into fixed 1-hour buckets, blocking correlation analysis and predictive modeling. Removing hourly_stats and using indexed ORM queries unlocks this critical capability.

**Independent Test**: Can be tested by writing ORM query for custom time window (e.g., "rides between 11:30am-1:45pm on weekends"), verifying results match raw snapshots, and measuring query performance (<500ms target).

**Acceptance Scenarios**:

1. **Given** ML analysis needs wait times for "peak lunch hours" (11am-2pm), **When** ORM query filters snapshots by time range, **Then** results are returned within 500ms and match raw snapshot data exactly
2. **Given** pattern analysis needs hourly metrics for a specific day, **When** ORM calculates AVG, COUNT, SUM on-the-fly, **Then** results match previous hourly_stats table values (validated against historical data)
3. **Given** analytics query spans multiple hours with custom boundaries, **When** ORM groups snapshots by custom time buckets, **Then** query completes without performance degradation vs. fixed hourly buckets
4. **Given** correlation analysis needs ride data grouped by 15-minute intervals, **When** ORM query uses custom time bucketing, **Then** analysis completes successfully (impossible with hourly_stats table)
5. **Given** raw snapshots have proper composite indexes (ride_id, snapshot_time), **When** hourly aggregation queries run, **Then** database uses indexes efficiently (verified via EXPLAIN plan)

---

### User Story 3 - Bug Fixes Without Manual Backfills (Priority: P1)

When bugs are found in aggregation logic, fixes should apply to all historical data automatically without manual backfilling processes for hourly metrics.

**Why this priority**: The current painful backfill process for hourly_stats has caused production bugs to persist (e.g., Kennywood YESTERDAY bug) because fixing historical data requires risky manual intervention. Removing hourly_stats eliminates this entire problem - ORM query fixes apply instantly to all time periods.

**Independent Test**: Can be tested by introducing a deliberate bug in hourly calculation logic, deploying fix via ORM query update, and validating that both new and historical queries return corrected values immediately.

**Acceptance Scenarios**:

1. **Given** a bug is discovered in hourly shame score calculation, **When** developer fixes the ORM query logic, **Then** all historical hourly queries return corrected values immediately (no backfill required)
2. **Given** business rules change (e.g., how "down" rides are identified), **When** ORM query logic is updated, **Then** change applies to TODAY, YESTERDAY, and all historical periods instantly
3. **Given** edge case handling is improved (e.g., NULL wait time handling), **When** ORM query is updated, **Then** past data is automatically re-interpreted with new logic when queried
4. **Given** a schema assumption was incorrect (e.g., park_appears_open logic), **When** ORM query is corrected, **Then** no manual data migration or backfill scripts are needed
5. **Given** test suite validates aggregation calculations, **When** ORM query bug is fixed, **Then** all existing tests pass with updated logic (no test data regeneration required)

---

### User Story 4 - Safe Database Schema Migrations (Priority: P2)

Database schema changes (adding columns, creating indexes, renaming tables) should be managed through automated migration tooling with rollback capabilities, replacing manual SQL scripts.

**Why this priority**: As features 004-006 add new tables and columns, schema changes will become frequent. Migration tooling prevents production incidents from manual schema changes, enables rollbacks, and documents schema evolution history.

**Independent Test**: Can be tested by creating a migration that adds a new index, applying it to test database, verifying schema change, rolling back, and confirming schema returns to original state.

**Acceptance Scenarios**:

1. **Given** developer needs to add a new composite index, **When** migration script is created and applied, **Then** index is added to all environments (dev, test, production) consistently
2. **Given** a migration adds a new column to ride_status_snapshots, **When** migration runs, **Then** ORM models automatically reflect new column and queries can use it immediately
3. **Given** a production migration fails mid-execution, **When** rollback is triggered, **Then** database schema returns to pre-migration state without data loss
4. **Given** multiple developers work on schema changes, **When** migrations are committed to version control, **Then** merge conflicts in schema are detected early and migration order is preserved
5. **Given** new environment is provisioned (e.g., staging server), **When** all migrations are run in sequence, **Then** database schema matches production exactly (reproducible schema)

---

### User Story 5 - Idempotent Daily Aggregation Recomputation (Priority: P2)

Daily aggregation metrics can be recomputed for any date range using an idempotent job, enabling safe bug fixes for daily_stats table without manual intervention.

**Why this priority**: While hourly_stats is removed, daily_stats is retained for long-range performance. Daily metrics still need recomputation capability when bugs are found, but frequency is much lower (daily vs. hourly), making the maintenance burden acceptable.

**Independent Test**: Can be tested by running recompute job for a specific date range, verifying daily_stats values match calculation from raw snapshots, running job again (idempotent), and confirming no duplicate data or inconsistencies.

**Acceptance Scenarios**:

1. **Given** bug is found in daily shame score calculation, **When** recompute job runs for affected date range, **Then** daily_stats table is updated with corrected values (no duplicate rows)
2. **Given** recompute job is run twice for the same date range, **When** second execution completes, **Then** daily_stats contains identical values (idempotent, no accumulation errors)
3. **Given** daily_stats has a metrics_version column, **When** recompute job runs with new logic, **Then** old and new calculations can coexist temporarily for comparison/validation
4. **Given** recompute job processes 90 days of historical data, **When** job runs overnight, **Then** calculation completes within 6 hours and daily_stats is consistent with raw snapshots
5. **Given** recompute job encounters missing raw snapshot data, **When** job processes date range with gaps, **Then** job logs warnings but completes successfully (handles data quality issues gracefully)

---

### User Story 6 - Performance Validation and Monitoring (Priority: P2)

System must validate that ORM query performance meets targets (<500ms for 95th percentile hourly queries) and monitor for regressions as query complexity increases.

**Why this priority**: Removing hourly_stats trades pre-calculated performance for flexibility. Performance validation ensures this trade-off doesn't degrade user experience. Monitoring catches regressions early before they impact production dashboards.

**Independent Test**: Can be tested by running load test simulating concurrent dashboard queries, measuring response times, and validating 95th percentile stays under 500ms threshold.

**Acceptance Scenarios**:

1. **Given** dashboard loads "TODAY" park rankings (hourly metrics), **When** query executes via ORM, **Then** response time is under 500ms for 95% of requests (measured under production-like load)
2. **Given** API endpoint returns hourly ride downtime, **When** 50 concurrent requests hit endpoint, **Then** database CPU usage stays under 70% and query performance remains stable
3. **Given** slow query log is enabled, **When** hourly ORM queries execute, **Then** no queries exceed 1 second execution time (flagged for optimization if threshold exceeded)
4. **Given** composite indexes exist on (ride_id, snapshot_time), **When** EXPLAIN plan is run on hourly aggregation query, **Then** database uses indexes efficiently (key_len shows both columns used)
5. **Given** query performance metrics are collected for 30 days, **When** performance degrades by 25%+ vs. baseline, **Then** automated alert triggers for investigation

---

### Edge Cases

- What happens when ORM query hits 24-hour snapshot retention boundary (data deleted, query spans missing period)?
- How does system handle schema migration rollback when ORM models are already updated to new schema?
- What happens when composite index is missing or dropped (query still works but slow)?
- How are timezone conversions handled in ORM queries (Pacific time for aggregations, UTC storage)?
- What happens when daily_stats recompute job is triggered while aggregation job is running (concurrent writes)?
- How does ORM handle NULL values in calculations (AVG, SUM with missing data)?
- What happens when a query references a soft-deleted ride (ride still in database but marked inactive)?
- How are query performance regressions detected when load patterns change (sudden traffic spike)?
- What happens when migration script has syntax error (detected before execution, or fails mid-migration)?
- How does system handle ORM query timeout (long-running analytics query exceeds database timeout)?

## Requirements *(mandatory)*

### Functional Requirements

**ORM Models and Query Layer:**
- **FR-001**: System MUST provide ORM models for all existing tables: ride_status_snapshots, park_activity_snapshots, daily_stats, parks, rides, weather_observations, weather_forecasts
- **FR-002**: ORM models MUST enforce type safety, preventing queries from referencing non-existent columns or tables at development time
- **FR-003**: System MUST provide query abstraction layer replacing all raw SQL strings in repositories (RideStatusRepository, StatsRepository, etc.)
- **FR-004**: ORM query builder MUST generate parameterized queries preventing SQL injection vulnerabilities
- **FR-005**: System MUST support relationship definitions between entities (e.g., Ride belongs to Park, RideStatusSnapshot belongs to Ride)

**Hourly Aggregation Removal:**
- **FR-006**: System MUST remove hourly_stats table and all code that writes to it (hourly aggregation cron job)
- **FR-007**: System MUST implement ORM-based hourly aggregation queries that calculate AVG, COUNT, SUM on-the-fly from ride_status_snapshots
- **FR-008**: Hourly aggregation queries MUST support arbitrary time windows (not limited to fixed 1-hour buckets)
- **FR-009**: System MUST add composite database indexes (ride_id, snapshot_time) and (park_id, snapshot_time) to optimize hourly queries
- **FR-010**: Hourly ORM queries MUST achieve response time <500ms for 95th percentile requests under production-like load

**Daily Aggregation Improvements:**
- **FR-011**: System MUST retain daily_stats table for long-range performance (monthly, weekly, all-time aggregations)
- **FR-012**: System MUST add metrics_version column to daily_stats enabling side-by-side old/new calculation comparison
- **FR-013**: System MUST provide idempotent recompute job that can regenerate daily_stats for any date range from raw snapshots
- **FR-014**: Recompute job MUST handle date ranges with missing data gracefully (log warnings, continue processing)
- **FR-015**: Recompute job MUST complete processing 90 days of historical data within 6 hours

**Database Migration Tooling:**
- **FR-016**: System MUST use migration framework (e.g., Alembic for Python) to manage schema changes
- **FR-017**: All schema changes MUST be defined in version-controlled migration scripts (no manual SQL execution)
- **FR-018**: Migration system MUST support rollback to previous schema version
- **FR-019**: Migration scripts MUST be idempotent (safe to run multiple times without side effects)
- **FR-020**: Migration system MUST track applied migrations in database (prevent duplicate application)

**Performance and Indexing:**
- **FR-021**: System MUST create composite indexes optimized for common query patterns (ride_id + snapshot_time, park_id + snapshot_time)
- **FR-022**: System MUST validate index usage via EXPLAIN plan analysis for critical queries
- **FR-023**: System MUST monitor query performance and log slow queries (>1 second execution time)
- **FR-024**: System MUST provide query performance baseline metrics before and after ORM migration for comparison

**Testing and Validation:**
- **FR-025**: ORM models MUST have unit test coverage >80% (model definitions, relationships, query methods)
- **FR-026**: Integration tests MUST validate ORM queries return identical results to current raw SQL queries (regression testing)
- **FR-027**: System MUST validate hourly ORM query results match historical hourly_stats values (using golden data tests)
- **FR-028**: Load tests MUST validate concurrent query performance under production-like traffic patterns
- **FR-029**: System MUST validate daily_stats recompute job produces identical values to current daily aggregation job

**Code Migration:**
- **FR-030**: All database queries in src/database/repositories/ MUST be migrated from raw SQL to ORM
- **FR-031**: All SQL strings in src/utils/sql_helpers.py MUST be refactored into ORM query builder methods
- **FR-032**: System MUST remove all string concatenation for SQL query building (replaced with ORM query methods)
- **FR-033**: Migrated code MUST maintain existing API contracts (same function signatures, return types)

### Key Entities

- **ORM Model**: Python class representing a database table. Attributes: table_name, columns (with types), relationships to other models, validation rules. Provides type-safe query interface replacing raw SQL.

- **Composite Index**: Database index spanning multiple columns for query optimization. Attributes: index_name, table, columns (ordered), index_type (BTREE). Critical for hourly aggregation query performance on (ride_id, snapshot_time).

- **Migration Script**: Version-controlled schema change definition. Attributes: version_number, description, upgrade_sql, downgrade_sql, timestamp. Enables reproducible schema evolution and rollback capability.

- **Recompute Job**: Idempotent batch process regenerating daily_stats from raw snapshots. Attributes: date_range_start, date_range_end, metrics_version, run_status, rows_processed, errors_logged. Enables safe bug fixes for daily aggregations.

- **Query Abstraction Layer**: Wrapper around ORM providing domain-specific query methods. Attributes: model, filters, aggregations, time_range. Encapsulates complex query logic (e.g., "rides that operated today") for reuse.

- **Performance Baseline**: Metrics snapshot before ORM migration. Attributes: query_name, avg_response_time, p95_response_time, p99_response_time, timestamp. Used to validate ORM performance doesn't regress.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All database queries in src/database/repositories/ migrated from raw SQL to ORM (100% migration rate, 0 raw SQL strings remaining)

- **SC-002**: Hourly aggregation queries respond within 500ms for 95% of requests under production load (20 concurrent users, measured via load testing)

- **SC-003**: ORM query results match raw SQL results for all migrated queries (validated via golden data regression tests with hand-computed expected values)

- **SC-004**: Test coverage for ORM models and queries exceeds 80% (measured via pytest --cov, excluding generated ORM code)

- **SC-005**: Daily_stats recompute job processes 90 days of historical data within 6 hours (measured via timed execution)

- **SC-006**: Zero SQL injection vulnerabilities detected in ORM queries (validated via static analysis and security audit)

- **SC-007**: Database migrations apply successfully across all environments (dev, test, production) with zero manual intervention required

- **SC-008**: Query performance baseline shows <10% regression for daily_stats queries and <20% improvement for hourly queries (hourly improvement from better indexes)

- **SC-009**: Slow query log shows zero queries exceeding 1 second execution time for common dashboard queries (TODAY, YESTERDAY, last_week)

- **SC-010**: Development velocity improves by 30%+ for new queries (measured by time to implement new analytics query before vs. after ORM)

## Scope & Boundaries

### In Scope

- ORM model definitions for all existing tables
- Migration of all raw SQL queries to ORM
- Removal of hourly_stats table and aggregation job
- Retention of daily_stats table with improved recomputation tooling
- Database migration framework implementation (Alembic)
- Composite index creation for query performance
- Idempotent daily_stats recompute job
- Performance validation and baseline comparison
- Query abstraction layer for domain logic
- Comprehensive testing (unit, integration, load)

### Out of Scope (Future Enhancements)

- Removing daily_stats table (re-evaluate after 90 days based on performance)
- Real-time materialized views (only if performance proves insufficient)
- Database sharding or partitioning (not needed for current scale)
- NoSQL or data warehouse integration (raw snapshots stay in MySQL)
- GraphQL API layer (API contracts remain REST-based)
- Multi-database support (MySQL/MariaDB only for now)
- Automated query optimization tuning (manual index optimization only)

## Assumptions & Dependencies

### Assumptions

- MySQL/MariaDB database supports composite indexes efficiently (BTREE indexes)
- Raw snapshot retention (24 hours) is sufficient for hourly analytics (extended retention is future enhancement)
- Python SQLAlchemy ORM is suitable for time-series aggregation queries
- Current query load is <100 concurrent users (scalability testing for higher load is out of scope)
- Daily aggregation job completion within 6 hours is acceptable (no real-time requirement)
- Development team has experience with ORM frameworks (training not required)
- Database migration downtime <5 minutes is acceptable (hourly_stats table drop is fast)
- Existing test suite (935+ tests) provides sufficient regression coverage

### Dependencies

**BLOCKING DEPENDENCIES:**
- **Feature 001 (Aggregation Tables)**: Current production system must remain operational during migration
- **Feature 002 (Weather Collection)**: Weather data tables must be included in ORM model definitions

**TECHNICAL DEPENDENCIES:**
- **Database Access**: MySQL 5.7+ or MariaDB 10.3+ with composite index support
- **Python Environment**: Python 3.11+ with SQLAlchemy 2.0+ installed
- **Migration Framework**: Alembic 1.13+ for database migration management
- **Testing Framework**: pytest with freezegun for time-based test determinism

**DOWNSTREAM IMPACTS:**
- **Feature 004 (ThemeParks Data Collection)**: Will use ORM models for new tables (attraction metadata, show schedules)
- **Feature 005 (Wait Time Analysis)**: Relies on flexible ORM queries for pattern analysis
- **Feature 006 (Visit Optimization)**: Depends on performant ORM queries for real-time recommendations

### External Factors

- Database query performance depends on MySQL query optimizer effectiveness (may require ANALYZE TABLE periodically)
- Snapshot data quality impacts recompute job reliability (missing data, NULL values)
- Production load patterns may change, requiring index optimization adjustments
- Team velocity during migration depends on ORM learning curve for developers unfamiliar with SQLAlchemy
- Migration rollback testing requires staging environment matching production schema
