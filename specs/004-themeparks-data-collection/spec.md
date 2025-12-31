# Feature Specification: Theme Park Data Warehouse for Analytics

**Feature Branch**: `004-themeparks-data-collection`
**Created**: 2025-12-31
**Status**: Draft
**Input**: User description: "Historical and real-time theme park data warehouse: Import years of historical wait time data from archive.themeparks.wiki, collect ongoing live data from themeparks.wiki API, and design a schema optimized for analytics, correlations, and predictions. Analyze storage requirements for permanent data retention. Schema redesign is acceptable, BUT any back-end changes must be reflected in the ORM code that the front-end uses to generate tables, charts, etc."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Import Historical Wait Time Data (Priority: P1)

The system needs to import years of historical wait time and status data from archive.themeparks.wiki to enable pattern analysis, seasonal trend detection, and predictive modeling that requires substantial historical context.

**Why this priority**: This is the foundational dataset required for all downstream analytics features (005, 006). Without multi-year historical data, correlation analysis lacks statistical power and seasonal patterns cannot be detected. This import must happen first before any analytics work begins.

**Independent Test**: Can be fully tested by downloading sample archive files, parsing them into the target schema, verifying data integrity (no duplicates, correct timestamps, complete coverage), and confirming the frontend displays historical data correctly.

**Acceptance Scenarios**:

1. **Given** archive.themeparks.wiki contains historical data, **When** the import process runs, **Then** wait time snapshots are imported with original timestamps preserved and stored in the permanent archive table
2. **Given** historical data spans multiple years, **When** import completes for a park, **Then** the system contains continuous data (allowing for gaps) from the earliest available date to present
3. **Given** the archive may contain data in varying formats over time, **When** format variations are detected, **Then** the import normalizes data to a consistent schema and logs any transformation decisions
4. **Given** import is running on large datasets, **When** the process is interrupted, **Then** it can resume from the last successful checkpoint without re-importing completed data
5. **Given** historical data is imported, **When** querying via API or frontend, **Then** the system correctly identifies data source (archive vs live) for audit purposes

---

### User Story 2 - Collect Real-Time Data with Permanent Retention (Priority: P1)

The system needs to continuously collect live wait time, status, and queue data from themeparks.wiki API and store it permanently (no deletion) to build an ever-growing dataset for analytics.

**Why this priority**: Real-time data collection is the continuous feed that extends historical data into the future. Permanent retention ensures we never lose data needed for long-term trend analysis. This runs alongside the historical import.

**Independent Test**: Can be tested by running the collector for 24+ hours, verifying data is stored permanently, checking that aggregation jobs process the data correctly, and confirming the frontend displays both live and aggregated views.

**Acceptance Scenarios**:

1. **Given** themeparks.wiki API is available, **When** collection runs every 5 minutes, **Then** all queue types (standby, single rider, Lightning Lane, virtual queue) are captured and stored permanently
2. **Given** permanent retention is enabled, **When** data is 30+ days old, **Then** the system does NOT delete it (unlike current 24-hour retention)
3. **Given** storage grows continuously, **When** querying recent data (today, yesterday), **Then** query performance remains under 500ms using optimized indexes
4. **Given** the collector encounters API rate limits, **When** throttled, **Then** it backs off gracefully and resumes without data loss
5. **Given** live data is collected, **When** aggregation runs, **Then** hourly and daily stats are computed and the frontend accurately reflects current park status

---

### User Story 3 - Optimize Schema for Analytics Queries (Priority: P1)

The system needs a redesigned database schema optimized for analytical queries (correlations, aggregations, time-series analysis) while maintaining compatibility with the existing frontend through updated ORM models.

**Why this priority**: The current schema was designed for 24-hour retention. Permanent retention with multi-year history requires different indexing strategies, partitioning, and potentially columnar storage considerations. Schema must be optimized before data import to avoid costly migrations later.

**Independent Test**: Can be tested by loading sample historical data, running typical analytical queries (week-over-week comparison, seasonal patterns, correlation analysis), verifying query plans use indexes efficiently, and confirming frontend continues to work via updated ORM models.

**Acceptance Scenarios**:

1. **Given** the schema is redesigned, **When** the ORM models are updated, **Then** all existing frontend features continue to work without visible changes to users
2. **Given** multi-year data exists, **When** querying "same week last year" comparison, **Then** the query completes in under 2 seconds
3. **Given** the schema supports partitioning, **When** archiving old data or querying date ranges, **Then** only relevant partitions are scanned
4. **Given** the schema includes correlation-friendly columns, **When** running weather-vs-wait-time correlation queries, **Then** the necessary data can be joined efficiently
5. **Given** schema changes require migrations, **When** migration runs, **Then** it completes without data loss and includes rollback capability

---

### User Story 4 - Analyze and Report Storage Requirements (Priority: P2)

The system needs to provide storage analysis tools that help operators understand current usage, project future growth, and make informed decisions about data retention and archival strategies.

**Why this priority**: With permanent retention, storage costs become a significant concern. Understanding growth rates and compression effectiveness helps plan infrastructure and budget. Lower priority than data import but important for operational sustainability.

**Independent Test**: Can be tested by running the storage analyzer on current database, comparing projections against actual growth over a test period, and generating reports that accurately reflect table sizes and growth rates.

**Acceptance Scenarios**:

1. **Given** the database contains imported data, **When** storage analysis runs, **Then** a report shows per-table size, row counts, index overhead, and growth rate
2. **Given** historical import adds significant data, **When** projecting future storage needs, **Then** the analyzer estimates 1-year, 3-year, and 5-year requirements
3. **Given** different retention strategies exist (raw vs hourly aggregation), **When** comparing strategies, **Then** the analyzer shows storage/query-performance tradeoffs for each
4. **Given** storage thresholds are configured, **When** usage exceeds 80% of threshold, **Then** alerts are generated to notify operators

---

### User Story 5 - Collect Rich Entity Metadata (Priority: P2)

The system needs to collect and store comprehensive attraction metadata (coordinates, indoor/outdoor, ride type, height requirements) from themeparks.wiki to enable location-based optimization and weather-aware recommendations.

**Why this priority**: Metadata enriches the analytics dataset beyond wait times. Geographic coordinates enable walking time calculations for visit optimization. Indoor/outdoor classification enables weather-based predictions. Lower priority than raw data import.

**Independent Test**: Can be tested by querying themeparks.wiki entity endpoints, storing metadata, and verifying downstream features can query location, type, and classification data for all tracked attractions.

**Acceptance Scenarios**:

1. **Given** themeparks.wiki entity endpoint is available, **When** metadata collection runs, **Then** coordinates (lat/long), entity type, and tags are stored for all attractions
2. **Given** attraction metadata exists, **When** a downstream feature queries "indoor rides near (lat,long)", **Then** relevant results are returned with distances
3. **Given** metadata may change over time (new rides, rethemes), **When** daily sync runs, **Then** changes are detected and version history is maintained
4. **Given** height requirements are available, **When** queried for family planning, **Then** min/max height restrictions are accessible per ride

---

### Edge Cases

- What happens when archive.themeparks.wiki is unavailable or rate-limited during historical import?
- How do we handle duplicate data if the same timestamp exists in both archive and live collection?
- What happens when a park is added or removed from tracking during the import period?
- How do we handle timezone changes (DST) in historical data that may have been stored inconsistently?
- What happens when archive data format changes between years (schema evolution)?
- How do we handle attractions that have been renamed, merged, or split over time?
- What happens when storage approaches capacity limits?
- How do we handle gaps in historical data (missing days/hours)?
- What happens when archive themeparks.wiki UUIDs don't match our existing internal IDs?
- How do we handle corrupted or malformed archive files?

## Requirements *(mandatory)*

### Functional Requirements

**Historical Data Import:**
- **FR-001**: System MUST import historical wait time data from archive.themeparks.wiki S3 bucket
- **FR-002**: System MUST preserve original timestamps (UTC) during import without modification
- **FR-003**: System MUST support resumable imports with checkpoint tracking to handle interruptions
- **FR-004**: System MUST detect and skip duplicate records based on (entity_id, timestamp) composite key
- **FR-005**: System MUST log import progress, errors, and data quality issues for monitoring
- **FR-006**: System MUST map themeparks.wiki entity IDs to existing internal ride/park IDs where possible

**Real-Time Data Collection:**
- **FR-007**: System MUST collect live data from themeparks.wiki API every 5 minutes (configurable)
- **FR-008**: System MUST capture all queue types: STANDBY, SINGLE_RIDER, RETURN_TIME, PAID_RETURN_TIME, BOARDING_GROUP
- **FR-009**: System MUST store snapshots permanently (no automatic deletion)
- **FR-010**: System MUST handle API rate limiting with exponential backoff and retry
- **FR-011**: System MUST track data freshness and alert on collection gaps exceeding 30 minutes

**Schema and Storage:**
- **FR-012**: System MUST partition historical data by month/year for efficient querying
- **FR-013**: System MUST maintain separate tables for raw snapshots vs aggregated stats
- **FR-014**: System MUST update all SQLAlchemy ORM models to reflect schema changes
- **FR-015**: System MUST provide Alembic migrations for all schema changes with rollback support
- **FR-016**: System MUST maintain indexes optimized for: time-range queries, entity lookups, and aggregation queries
- **FR-017**: System MUST support querying "same period last year" comparisons efficiently

**Storage Analysis:**
- **FR-018**: System MUST calculate and report current storage usage per table
- **FR-019**: System MUST project future storage needs based on current growth rate
- **FR-020**: System MUST compare storage requirements for different retention strategies (raw vs aggregated)
- **FR-021**: System MUST alert when storage usage exceeds configurable thresholds

**Entity Metadata:**
- **FR-022**: System MUST collect entity coordinates (latitude, longitude) from themeparks.wiki
- **FR-023**: System MUST classify entities as indoor/outdoor/hybrid based on available metadata
- **FR-024**: System MUST collect entity type (ATTRACTION, SHOW, RESTAURANT, etc.) using themeparks.wiki taxonomy
- **FR-025**: System MUST maintain metadata version history to track changes over time

**ORM and Frontend Compatibility:**
- **FR-026**: System MUST maintain backward compatibility for all existing API endpoints
- **FR-027**: System MUST update ORM models to expose new fields and relationships to frontend
- **FR-028**: System MUST preserve existing frontend functionality (rankings, charts, details) during migration

### Key Entities

- **WaitTimeSnapshot**: Point-in-time wait time observation. Includes themeparks_wiki_id (UUID), recorded_at (UTC timestamp), park_id, ride_id, status (OPERATING/CLOSED/DOWN/REFURBISHMENT), wait_time_minutes, queue_type (STANDBY/SINGLE_RIDER/etc), data_source (ARCHIVE/LIVE). Primary table for raw time-series data. Partitioned by month.

- **QueueData**: Extended queue information beyond standby. Includes snapshot_id (FK), queue_type, wait_time_minutes, return_time_start, return_time_end, price_amount, price_currency, boarding_group_status, boarding_group_current. Captures Lightning Lane, virtual queue, and paid queue details.

- **EntityMetadata**: Comprehensive attraction/entity reference data. Includes themeparks_wiki_id, internal_ride_id, entity_name, entity_type, park_id, latitude, longitude, indoor_outdoor_classification, height_min_cm, height_max_cm, tags (JSON), last_updated. Updated daily, stored permanently with version history.

- **ImportCheckpoint**: Tracks historical import progress. Includes import_id, park_id, last_processed_date, last_processed_file, records_imported, errors_encountered, status (IN_PROGRESS/COMPLETED/FAILED), created_at, updated_at. Enables resumable imports.

- **StorageMetrics**: Database storage tracking. Includes table_name, measurement_date, row_count, data_size_mb, index_size_mb, growth_rate_mb_per_day. Updated daily for capacity planning.

- **DataQualityLog**: Data quality issues and gaps. Includes issue_type (GAP/DUPLICATE/INVALID), entity_id, timestamp_start, timestamp_end, description, resolution_status. Tracks data integrity issues.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Historical data import completes for all tracked parks, with 95%+ of available archive data successfully imported

- **SC-002**: System stores raw snapshots permanently, verified by confirming data older than 30 days is retained and queryable

- **SC-003**: Query performance for "today" and "yesterday" views remains under 500ms after schema migration

- **SC-004**: Query performance for "year-over-year comparison" completes in under 3 seconds

- **SC-005**: All existing frontend features (rankings, park details, ride details, charts) continue to function correctly after ORM updates

- **SC-006**: Storage analysis accurately predicts growth within 10% of actual over a 30-day validation period

- **SC-007**: Data collection maintains 99%+ uptime with gaps logged and alertable

- **SC-008**: Entity metadata is complete for 90%+ of tracked attractions (coordinates, type, classification)

- **SC-009**: Schema migration includes tested rollback procedure that completes in under 10 minutes

- **SC-010**: Historical import processes at least 1 million records per hour (throughput)

## Scope & Boundaries

### In Scope

- Importing historical wait time data from archive.themeparks.wiki
- Transitioning from 24-hour deletion to permanent retention
- Schema redesign optimized for analytics queries
- SQLAlchemy ORM model updates for all schema changes
- Alembic migrations with rollback support
- Storage analysis and capacity planning tools
- Collecting all queue types from themeparks.wiki API
- Entity metadata collection (coordinates, type, classification)
- Mapping themeparks.wiki IDs to existing internal IDs
- Data quality monitoring and gap detection

### Out of Scope (Future Enhancements)

- Pattern analysis and correlation algorithms (feature 005)
- Predictive modeling and forecasting (feature 005)
- Visit optimization and route planning (feature 006)
- Real-time push notifications
- Integration with park-specific mobile apps (Disneyland app, etc.)
- User-submitted wait time data or crowd-sourced information
- Data visualization dashboards beyond current frontend
- Machine learning model training infrastructure

## Assumptions & Dependencies

### Assumptions

- archive.themeparks.wiki S3 bucket is publicly accessible for reading historical data
- Archive data is organized by date/park in a consistent structure (exact format TBD during implementation)
- Historical data uses UTC timestamps consistently (or timezone is determinable)
- themeparks.wiki API continues to be available with current rate limits
- MySQL/MariaDB can handle the storage requirements (~10-50 GB for multi-year data)
- Partitioning by month provides adequate query performance
- Internal ride/park IDs can be mapped to themeparks.wiki UUIDs via name matching or existing mappings
- Server has sufficient disk space for estimated storage growth

### Dependencies

**CRITICAL:**
- **archive.themeparks.wiki**: Must be accessible and contain usable historical data in parseable format
- **themeparks.wiki API**: Must remain available for ongoing live data collection
- **Database capacity**: Must have storage headroom for ~50-100 GB growth

**OTHER DEPENDENCIES:**
- **Feature 003 (ORM Refactoring)**: Must be complete - provides SQLAlchemy ORM foundation for schema changes
- **Alembic**: Migration framework for schema changes
- **Existing parks/rides tables**: Must have themeparks_wiki_id column or allow adding one
- **Cron infrastructure**: For scheduled collection and aggregation jobs

### External Factors

- archive.themeparks.wiki availability and data format may change without notice
- themeparks.wiki API rate limits may be adjusted
- Historical data quality varies - some parks/periods may have gaps or inconsistencies
- Storage costs scale with data retention (permanent retention = continuous growth)
- Query performance depends on hardware capacity and MySQL configuration

## Storage Analysis

### Current State (24-hour retention)
- ride_status_snapshots: ~68 MB data + 288 MB indexes = ~356 MB
- Collection rate: ~135,000 rows/day
- Average row size: ~59 bytes

### Projected State (Permanent retention)

**Raw Snapshots (if kept indefinitely):**
| Period | Data Size | With Indexes |
|--------|-----------|--------------|
| 1 year | 2.7 GB | 10.8 GB |
| 5 years | 13.5 GB | 54 GB |
| 10 years | 27 GB | 108 GB |

**Hourly Aggregation Alternative:**
| Period | Data Size | With Indexes |
|--------|-----------|--------------|
| 1 year | 835 MB | 2.5 GB |
| 5 years | 4.2 GB | 12.5 GB |
| 10 years | 8.4 GB | 25 GB |

**Decision:** Permanent raw snapshot retention (as requested by user):
- **Storage**: ~108 GB for 10 years (manageable, ~$0.02/GB/month)
- **Rationale**: Raw data enables future ML features, pattern analysis, and correlations that aggregates cannot support
- **Implementation**: Monthly RANGE partitioning on `recorded_at` for query performance
- **Risk Mitigation**: StorageMetrics table monitors growth; alerts at 80% threshold

*Note: Tiered storage (30-90 days raw, then hourly aggregates) was considered but rejected by user in favor of simplicity and maximum analytics capability.*
