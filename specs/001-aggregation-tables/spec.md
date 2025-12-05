# Feature Specification: Pre-Computed Time-Series Aggregation Tables

**Feature Branch**: `001-aggregation-tables`
**Created**: 2025-12-05
**Status**: Draft
**Type**: Performance & Architecture Refactoring
**Input**: User description: "Implement pre-computed time-series aggregation tables (hourly, daily, yearly) to replace on-the-fly GROUP BY queries. Current problem: queries aggregate 8.4M snapshots on-the-fly causing performance issues. Previous ORM test took several minutes. Solution: create hourly_park_stats, daily_park_stats, yearly_park_stats tables with batch aggregation jobs. Use ORM for application queries, SQLAlchemy Core for batch jobs. Include schema design, backfill strategy, retention policies, and migration from current queries."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Fast TODAY Rankings (Priority: P1)

Users viewing the "Today" rankings page experience instant load times, regardless of how much data exists in the system.

**Why this priority**: TODAY is the most frequently accessed view. Current on-the-fly aggregation causes slow page loads. This story proves the new architecture works for the most critical use case.

**Independent Test**: Load TODAY rankings page with production-scale data and verify sub-second response times. Frontend displays same data format as before, just delivered faster.

**Acceptance Scenarios**:

1. **Given** user navigates to TODAY rankings, **When** page loads, **Then** rankings display in under 1 second
2. **Given** user refreshes TODAY rankings, **When** page reloads, **Then** data remains consistent
3. **Given** multiple users access TODAY rankings simultaneously, **When** 1000+ concurrent requests occur, **Then** all requests complete in under 2 seconds

---

### User Story 2 - Fast Historical Period Views (Priority: P2)

Users viewing YESTERDAY, last week, and last month rankings experience the same instant load times as TODAY.

**Why this priority**: Extends the performance improvement to all time periods users can access. Each period uses the same aggregation pattern, so this builds on P1 success.

**Independent Test**: Load each historical period page and verify sub-second response times. Compare response format to current implementation to ensure API contract maintained.

**Acceptance Scenarios**:

1. **Given** user selects YESTERDAY period, **When** page loads, **Then** rankings display in under 1 second
2. **Given** user selects "last week" period, **When** page loads, **Then** rankings display in under 1 second
3. **Given** user selects "last month" period, **When** page loads, **Then** rankings display in under 1 second
4. **Given** user switches between periods, **When** selecting different time ranges, **Then** each view loads instantly

---

### User Story 3 - Yearly Awards Rankings (Priority: P3)

Users can view yearly award rankings showing which parks had the best/worst performance over the entire year.

**Why this priority**: Enables new functionality (yearly awards) that would be impossible with current on-the-fly aggregation performance. This is the long-term payoff of the aggregation architecture.

**Independent Test**: Query yearly rankings for completed calendar years and verify results appear instantly. This demonstrates the system can scale to year-long timeframes.

**Acceptance Scenarios**:

1. **Given** calendar year has completed, **When** user requests yearly awards, **Then** rankings display in under 1 second
2. **Given** multiple years of data exist, **When** user compares year-over-year performance, **Then** results load instantly
3. **Given** new year begins, **When** system starts tracking new yearly data, **Then** current year shows partial results and prior year is complete

---

### User Story 4 - Automated Continuous Aggregation (Priority: P4)

The system automatically maintains aggregated data as new snapshots are collected, without manual intervention.

**Why this priority**: This is the operational requirement that makes the system sustainable long-term. Must work reliably after proving correctness in P1-P3.

**Independent Test**: Run data collection for 7 days, verify aggregation jobs execute on schedule, and confirm all ranking views show up-to-date data.

**Acceptance Scenarios**:

1. **Given** new snapshots collected every 5 minutes, **When** hour completes, **Then** hourly aggregate created within 15 minutes
2. **Given** day completes at midnight Pacific, **When** daily aggregation runs, **Then** daily aggregate available by 2 AM
3. **Given** aggregation job encounters error, **When** failure occurs, **Then** system logs error, alerts operator, retries automatically
4. **Given** year completes on Dec 31, **When** yearly aggregation runs, **Then** yearly aggregate ready for awards by Jan 2

---

### Edge Cases

- What happens when a park has zero snapshots for an hour/day? (Create aggregate record with zero/NULL values for consistency)
- How does system handle incomplete hours (e.g., during deployment)? (Wait for hour to fully complete before aggregating)
- What if aggregation job runs twice for the same period? (Use upsert/ON CONFLICT to make idempotent)
- How are timezone boundaries handled (midnight Pacific vs UTC)? (All aggregation uses UTC internally, converts for display)
- What happens to data during the initial implementation? (Wipe database and restart collection with new schema for clean implementation)

## Requirements *(mandatory)*

### Functional Requirements

**Performance Requirements:**

- **FR-001**: System MUST return TODAY rankings in under 1 second
- **FR-002**: System MUST return YESTERDAY/last_week/last_month rankings in under 1 second
- **FR-003**: System MUST return yearly rankings in under 1 second
- **FR-004**: System MUST support 10,000 concurrent users without performance degradation

**Data Architecture Requirements:**

- **FR-005**: System MUST store pre-computed hourly aggregates for all parks
- **FR-006**: System MUST store pre-computed daily aggregates for all parks
- **FR-007**: System MUST store pre-computed yearly aggregates for all parks
- **FR-008**: Hourly aggregates MUST include: park identifier, hour timestamp (UTC), average wait time, average shame score, sample count
- **FR-009**: Daily aggregates MUST include: park identifier, date, average wait time, average shame score, sample count
- **FR-010**: Yearly aggregates MUST include: park identifier, year, average wait time, average shame score, sample count

**Automation Requirements:**

- **FR-011**: System MUST automatically create hourly aggregates within 15 minutes of hour completion
- **FR-012**: System MUST automatically create daily aggregates by 2 AM Pacific Time following target day
- **FR-013**: System MUST automatically create yearly aggregates within 2 days of year completion
- **FR-014**: Aggregation jobs MUST be idempotent (safe to re-run)
- **FR-015**: Aggregation jobs MUST log success/failure with details for monitoring

**Data Retention Requirements:**

- **FR-016**: System MUST retain raw 5-minute snapshots for 90 days before deletion/archival
- **FR-017**: System MUST retain hourly aggregates for 3 years before deletion/archival
- **FR-018**: System MUST retain daily aggregates indefinitely
- **FR-019**: System MUST retain yearly aggregates indefinitely

**API Contract Requirements:**

- **FR-020**: Flask API endpoints MUST maintain current response format (frontend expects specific JSON structure)
- **FR-021**: API response times MUST NOT exceed 2 seconds for any ranking query
- **FR-022**: API MUST handle errors gracefully (return cached data or meaningful error message)

### Key Entities

- **RawParkSnapshot**: Raw data point collected every 5 minutes. Contains park identifier, timestamp, all ride statuses, computed metrics (wait times, shame score). Source of truth for short-term data and aggregation input.

- **HourlyParkAggregate**: One hour of pre-computed park performance. Contains park identifier, hour timestamp (UTC), average wait time, average shame score, total sample count. Enables instant TODAY/YESTERDAY queries.

- **DailyParkAggregate**: One day of pre-computed park performance. Contains park identifier, date, average wait time, average shame score, total sample count. Enables instant week/month queries and serves as input for yearly aggregates.

- **YearlyParkAggregate**: One year of pre-computed park performance. Contains park identifier, year, average wait time, average shame score. Enables yearly award rankings.

- **AggregationJobLog**: Audit trail of aggregation job executions. Contains job type (hourly/daily/yearly), target period, execution time, success/failure status, records processed, error details. Enables monitoring and debugging.

## Success Criteria *(mandatory)*

### Measurable Outcomes

**Performance Improvements:**

- **SC-001**: TODAY rankings load in under 1 second (currently 5-10 seconds)
- **SC-002**: YESTERDAY rankings load in under 1 second
- **SC-003**: Last week rankings load in under 1 second
- **SC-004**: Last month rankings load in under 1 second
- **SC-005**: Yearly rankings load in under 1 second (currently not feasible)

**Scalability:**

- **SC-006**: System handles 10,000 concurrent users with average response time under 2 seconds
- **SC-007**: Database query time for rankings improves by 10x or more
- **SC-008**: Storage costs remain flat or decrease over time (through retention policies)

**Reliability:**

- **SC-009**: Aggregation jobs run successfully 99.9% of the time
- **SC-010**: Rankings data is never more than 15 minutes stale
- **SC-011**: System continues serving cached/previous data if aggregation fails

**User Experience:**

- **SC-012**: Users see no difference in frontend UI (same charts, same data, just faster)
- **SC-013**: User complaints about slow page loads decrease by 90%
- **SC-014**: All existing functionality continues working (no regressions)

## Assumptions

1. **Current Architecture**: Flask backend with REST API endpoints serving JSON to frontend
2. **Data Volume**: ~8.4M snapshots/year (80 parks × 12/hour × 24 hours × 365 days)
3. **Collection Pattern**: New snapshots every 5 minutes (continues unchanged)
4. **Database**: MySQL/MariaDB (no migration to specialized time-series DB in this phase)
5. **API Contract**: Frontend expects specific JSON response format from Flask endpoints
6. **Timezone**: All data stored in UTC, converted to Pacific Time for display
7. **Frontend Independence**: Frontend code doesn't need changes (API layer provides decoupling)

## Dependencies

- Existing Flask API endpoints and response format documentation
- Batch job scheduler capability (cron, systemd timers, or application-level scheduler)
- Monitoring system for tracking job health and alerting on failures
- Database has sufficient storage for aggregate tables (much smaller than raw snapshots)

## Out of Scope

- Frontend UI changes or new visualizations (maintain current interface)
- Real-time streaming aggregation (5-minute collection cadence sufficient)
- Migration to specialized time-series database (TimescaleDB, InfluxDB, etc.)
- Adding new parks or changing collection frequency
- Mobile app or alternative frontends (web frontend only)
