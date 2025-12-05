<!--
Version: 1.0.0 → 1.1.0
Modified Principles:
  - VI. Test Coverage for Data Integrity → VI. Test-Driven Development (expanded to make TDD mandatory)
Added Sections:
  - VII. DRY Principles & Single Source of Truth (new principle addressing code duplication root cause)
  - VIII. Architecture Stability (new principle for long-term maintainability)
  - IX. Production Integrity & Local-First Development (new principle prohibiting direct production modifications)
  - X. Mandatory AI-Assisted Expert Review (new principle requiring Zen review and implementation of recommendations)
Removed Sections: None
Templates Updated:
  ✅ plan-template.md - Verified consistency with DRY and TDD principles
  ✅ spec-template.md - Verified alignment with real-time data requirements
  ✅ tasks-template.md - Verified task categories align with TDD workflow and observability principles
Follow-up TODOs: None
Rationale:
  - Elevated TDD from best practice to constitutional principle based on project requirement (882 tests in suite)
  - Added explicit DRY principle after Zen Consensus identified duplicated logic across 20+ files as root cause of "fix one, break another" regression cycles
  - Added Architecture Stability principle to prevent future architectural drift based on ORM migration decision
  - Added Production Integrity principle after repeated incidents of attempting bug fixes directly on production server without local testing
  - Added Mandatory AI Review principle to catch architectural problems, security vulnerabilities, and code smells early via systematic expert analysis
  - Strengthened Deployment Process and Review Requirements sections with mandatory workflows and quality gates
  - These changes codify lessons learned during two weeks of debugging shame-score-consistency issues
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

### VI. Test-Driven Development (TDD)
All code changes MUST follow the TDD cycle: Red (write failing test) → Green (implement minimal code to pass) → Refactor (clean up while keeping tests green). Integration tests MUST validate cross-feature consistency with real data to prevent "fix one, break another" regressions. Unit tests alone are insufficient—tests must reflect production reality.

**Rationale:** This project maintains 882 tests in the suite. However, production experience showed that unit tests can pass while production breaks, indicating test design problems. Tests must validate business requirements against real data, not just implementation details. The TDD cycle ensures testable, maintainable code.

### VII. DRY Principles & Single Source of Truth
Business logic MUST NOT be duplicated. Every calculation, validation, or business rule (shame scores, period logic, operating hours checks, ride status filters) MUST have exactly one canonical implementation. Use centralized functions, classes, ORM model methods, or SQL helper modules. Copy-pasting logic is prohibited.

**Rationale:** Zen Consensus identified duplicated logic across 20+ query files as the root cause of endless regression cycles. When the same shame score calculation appears with subtle variations in multiple files:
- Fixing one breaks another (TODAY chart fixed → YESTERDAY broke)
- Testing becomes impossible (each variation needs separate tests)
- Consistency cannot be guaranteed
- Maintenance burden grows exponentially

**Enforcement:** Code reviews MUST reject pull requests containing duplicated business logic.

### VIII. Architecture Stability
Architectural changes (ORM adoption, framework switches, major refactors) require documented justification including: (1) Root cause analysis of problems not solvable with current architecture, (2) Cost-benefit analysis of migration vs. improvement, (3) Staged migration plan with independent validation steps. Architecture changes MUST be informed by production lessons, not trends.

**Rationale:** Two weeks of debugging revealed that architectural problems (scattered SQL logic) compounded with inadequate integration testing created the bug cycle. Future architectural decisions must be evidence-based and include migration validation against production data.

### IX. Production Integrity & Local-First Development
NEVER modify code, queries, or configurations directly on the production server. ALL bug fixes and new features MUST be developed locally, tested against a mirrored production database, and validated in a browser before deployment. Production is read-only for debugging (logs, queries) but write-forbidden for code changes. Use `deployment/scripts/mirror-production-db.sh` to sync production data to local dev environment before starting any work.

**Rationale:** Attempting to fix bugs directly on the production server led to:
- Untested changes breaking live functionality
- No rollback capability when fixes failed
- Inability to reproduce issues locally
- Risk of data corruption or service outages
- Wasted debugging time in production environment

**Enforcement:** Production server access is for deployment, monitoring, and emergency rollback only. Code changes made directly on production are prohibited and constitute a critical violation.

### X. Mandatory AI-Assisted Expert Review
After each significant design phase or coding phase, the design or implementation MUST be reviewed using Zen MCP tools (codereview, analyze, refactor, secaudit, or thinkdeep as appropriate). ALL recommendations from the expert review MUST be implemented before proceeding to the next phase. Reviews are not optional suggestions—they are mandatory quality gates.

**Rationale:** Human developers can miss architectural problems, security vulnerabilities, code smells, and design flaws that AI expert models can systematically identify. The two-week bug cycle demonstrated that self-review alone is insufficient. Mandatory expert review catches issues early when they're cheap to fix, rather than discovering them in production when they're expensive.

**When to Use Zen Review:**
- **Design Phase:** Use `analyze` or `thinkdeep` to validate architectural decisions before implementation
- **After Implementation:** Use `codereview` to validate code quality, security, and maintainability
- **Before Refactoring:** Use `refactor` to identify code smells and improvement opportunities
- **Security-Critical Changes:** Use `secaudit` for authentication, authorization, data handling, or API changes
- **Complex Bugs:** Use `debug` or `thinkdeep` to ensure root cause is properly identified

**Enforcement:** Pull requests that skip Zen review or ignore recommendations without documented justification are grounds for rejection. The review report and implementation of recommendations MUST be included in PR description.

## Data Quality Standards

### Validation Rules
- API responses MUST be validated before database insertion
- Unrealistic values (e.g., `wait_time > 300` minutes) MUST be flagged and logged
- NULL handling MUST be explicit in all queries and application logic
- Timestamp fields MUST always be stored in UTC

### Business Logic Consistency
- `computed_is_open` logic MUST be applied consistently—use centralized helper
- Operating hours detection logic MUST follow documented rules (first/last activity)
- Uptime percentage calculations MUST only consider park operating hours, never 24-hour periods
- Business logic MUST NOT be duplicated across files—see Principle VII

### Data Retention Compliance
- Raw data older than 24 hours MUST be deleted after aggregation
- Daily aggregation MUST run before cleanup to prevent data loss
- Summary tables are permanent and MUST NOT be automatically deleted

## Development Workflow

### TDD Cycle (Mandatory)
1. **RED:** Write a test that defines expected behavior. Run it and verify it fails.
2. **GREEN:** Write minimum code necessary to make the test pass. No more.
3. **REFACTOR:** Clean up code while keeping tests green. Remove duplication.

### Code Changes
1. All database schema changes MUST include migration scripts
2. Performance-impacting changes MUST include benchmark results
3. Data transformation logic changes MUST include before/after validation
4. API integration changes MUST handle backwards compatibility
5. Business logic changes MUST verify single source of truth is maintained (DRY compliance)

### Review Requirements (see Principle X)
- **Zen AI Review (MANDATORY):** All significant design and code changes MUST undergo expert review using appropriate Zen MCP tools before PR approval
- Database changes require review of indexes and query performance
- Scheduled job changes require verification of timing and dependencies
- Frontend changes must verify API attribution display
- Code reviews MUST flag duplicated business logic and require refactoring to centralized helpers
- Code reviews MUST verify tests exist and follow TDD principles
- PR descriptions MUST include Zen review report and evidence that recommendations were implemented

### Testing Requirements
- Unit tests for isolated logic (>80% coverage)
- Integration tests with real data for cross-feature validation
- Contract tests for API endpoint stability
- Tests MUST run before every commit (`pytest tests/ -v`)
- Tests MUST validate business requirements, not implementation details

### Deployment Process (see Principle IX)

**MANDATORY Pre-Deployment Workflow:**
1. **Mirror production database:** Run `deployment/scripts/mirror-production-db.sh --days=7` (or `--full` for historical changes)
2. **Develop locally:** All code changes happen in local dev environment, never on production server
3. **Test locally:** Run full test suite (`pytest tests/ -v`) and verify manually in browser
4. **Validate with production data:** Test all affected features (LIVE, TODAY, YESTERDAY, charts, rankings) with mirrored data
5. **Commit and push:** Only after local validation passes

**Deployment to Production:**
1. Test database migrations on mirrored local DB first (verify schema changes work)
2. Verify scheduled jobs run successfully in local test environment
3. Deploy code to production via deployment scripts (NEVER edit files directly on server)
4. Monitor collection success rates for 24 hours post-deployment
5. Rollback plan required for schema changes

**PROHIBITED:**
- SSHing to production and editing code files directly
- Testing SQL queries directly in production database
- "Quick fixes" deployed without local testing
- Deploying without mirroring and testing against production data first

## Governance

This constitution establishes the non-negotiable principles for the Theme Park Hall of Shame project. All code, features, and architectural decisions MUST comply with these principles.

**Amendment Process:**
- Proposed amendments require documented justification
- Breaking changes to data accuracy or attribution principles require user notification
- Performance threshold changes require benchmark validation
- Architecture principles may be amended based on production lessons learned
- Amendments follow semantic versioning: MAJOR (breaking changes), MINOR (new principles), PATCH (clarifications)

**Compliance Review:**
- All pull requests MUST verify alignment with data accuracy and performance principles
- All significant changes MUST include Zen AI expert review report (Principle X)
- Scheduled jobs MUST be reviewed for data retention compliance
- API integration changes MUST verify attribution requirements
- Code reviews MUST enforce TDD, DRY, and single source of truth principles
- Deployment process MUST follow local-first development workflow (Principle IX)
- Violations of Principles VI-VII-IX-X (TDD, DRY, Production Integrity, AI Review) are grounds for immediate PR rejection or deployment rollback
- PRs missing Zen review report or ignoring recommendations without justification will be rejected

**Version**: 1.1.0 | **Ratified**: 2025-11-22 | **Last Amended**: 2025-12-05
