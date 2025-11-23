<!--
Version: 1.0.0 → 1.0.0
Modified Principles: N/A (Initial creation)
Added Sections: All sections (initial)
Removed Sections: None
Templates Updated:
  ✅ plan-template.md - Verified consistency with data quality principles
  ✅ spec-template.md - Verified alignment with real-time data requirements
  ✅ tasks-template.md - Verified task categories align with observability and testing principles
Follow-up TODOs: None
-->

# Theme Park Hall of Shame Constitution

## Core Principles

### I. Data Accuracy First
All data collection, processing, and display MUST prioritize accuracy over completeness. The project exists to provide truthful insights about theme park performance. When API data is ambiguous or incomplete, the system MUST apply documented business logic consistently (e.g., `computed_is_open` logic) rather than displaying raw, potentially misleading values.

**Rationale:** Users rely on this data to understand park performance. Inaccurate data undermines the entire project's credibility.

### II. Real-Time with Historical Context
The system MUST maintain both real-time status (current conditions) and historical summaries (trends over time). Real-time data has a 24-hour retention window, after which it MUST be aggregated into permanent daily/weekly/monthly/yearly summaries and then deleted.

**Rationale:** Storage efficiency while preserving historical insights. Raw data older than 24 hours provides minimal additional value compared to calculated summaries.

### III. API Source Attribution
Every page displaying data MUST prominently attribute Queue-Times.com as the data source with a visible, clickable link to https://queue-times.com. This is both a legal requirement and an ethical obligation.

**Rationale:** Respect for the free API provider that makes this project possible. Required by Queue-Times.com terms of service.

### IV. Performance Over Features
Database queries MUST complete in under 100ms for current status, 200ms for historical data. Collection cycles MUST complete within 5 minutes. If a feature cannot meet these performance targets, it MUST be redesigned or rejected.

**Rationale:** User experience depends on responsive data access. Slow queries defeat the purpose of real-time tracking.

### V. Fail Gracefully
API failures, database errors, or missing data MUST NOT crash the system or corrupt existing data. The application MUST log errors, retry with exponential backoff, and continue operating with partial data when necessary.

**Rationale:** External APIs are unreliable. System resilience ensures continuous operation despite inevitable failures.

### VI. Test Coverage for Data Integrity
All data transformation logic (status calculations, aggregations, operating hours detection) MUST have unit tests with >80% code coverage. Integration tests MUST verify end-to-end data flow from API to database.

**Rationale:** Data transformation bugs directly impact accuracy. Testing is non-negotiable for business-critical calculations.

## Data Quality Standards

### Validation Rules
- API responses MUST be validated before database insertion
- Unrealistic values (e.g., `wait_time > 300` minutes) MUST be flagged and logged
- NULL handling MUST be explicit in all queries and application logic
- Timestamp fields MUST always be stored in UTC

### Business Logic Consistency
- `computed_is_open` logic MUST be applied consistently in collection and query layers
- Operating hours detection logic MUST follow documented rules (first/last activity)
- Uptime percentage calculations MUST only consider park operating hours, never 24-hour periods

### Data Retention Compliance
- Raw data older than 24 hours MUST be deleted after aggregation
- Daily aggregation MUST run before cleanup to prevent data loss
- Summary tables are permanent and MUST NOT be automatically deleted

## Development Workflow

### Code Changes
1. All database schema changes MUST include migration scripts
2. Performance-impacting changes MUST include benchmark results
3. Data transformation logic changes MUST include before/after validation
4. API integration changes MUST handle backwards compatibility

### Review Requirements
- Database changes require review of indexes and query performance
- Scheduled job changes require verification of timing and dependencies
- Frontend changes must verify API attribution display

### Deployment Process
1. Test migrations on staging database first
2. Verify scheduled jobs run successfully in test environment
3. Monitor collection success rates for 24 hours post-deployment
4. Rollback plan required for schema changes

## Governance

This constitution establishes the non-negotiable principles for the Theme Park Hall of Shame project. All code, features, and architectural decisions MUST comply with these principles.

**Amendment Process:**
- Proposed amendments require documented justification
- Breaking changes to data accuracy or attribution principles require user notification
- Performance threshold changes require benchmark validation

**Compliance Review:**
- All pull requests MUST verify alignment with data accuracy and performance principles
- Scheduled jobs MUST be reviewed for data retention compliance
- API integration changes MUST verify attribution requirements

**Version**: 1.0.0 | **Ratified**: 2025-11-22 | **Last Amended**: 2025-11-22
