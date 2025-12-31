<!--
Version: 1.2.0 → 1.3.0
Modified Principles:
  - II. Real-Time with Historical Context → II. Data Retention & Historical Context
    OLD: "Real-time data has a 24-hour retention window, after which it MUST be aggregated...and then deleted."
    NEW: "Raw snapshots MAY be retained permanently when explicitly required for analytics features. When permanent retention is enabled, table partitioning MUST be implemented."
Added Sections: None
Removed Sections: None
Templates Updated:
  ✅ plan-template.md - No changes needed (Constitution Check is dynamic)
  ✅ spec-template.md - No changes needed (no data retention references)
  ✅ tasks-template.md - No changes needed (task structure unchanged)
Follow-up TODOs: None
Rationale:
  - Feature 004 (Theme Park Data Warehouse) requires permanent raw snapshot retention for:
    - Multi-year historical analysis and seasonal pattern detection
    - ML features planned in 005/006 that need granular data
    - Year-over-year comparisons requiring raw timestamps
  - Storage is economical (~$0.02/GB/month, ~108 GB for 10 years)
  - Monthly RANGE partitioning maintains query performance at scale
  - Amendment enables analytics-focused features while preserving aggregation requirement
Previous Amendments:
  1.1.0 → 1.2.0: Added Cost Management & LLM Delegation (Principle XI)
  1.0.0 → 1.1.0: Added TDD, DRY, Architecture Stability, Production Integrity, Mandatory AI Review (Principles VI-X)
-->

# Theme Park Hall of Shame Constitution

## Core Principles

### I. Data Accuracy First
All data collection, processing, and display MUST prioritize accuracy over completeness. The project exists to provide truthful insights about theme park performance. When API data is ambiguous or incomplete, the system MUST apply documented business logic consistently (e.g., `computed_is_open` logic) rather than displaying raw, potentially misleading values.

**Rationale:** Users rely on this data to understand park performance. Inaccurate data undermines the entire project's credibility.

### II. Data Retention & Historical Context
The system MUST maintain both real-time status (current conditions) and historical summaries (trends over time). Real-time data is aggregated into permanent daily/weekly/monthly/yearly summaries. Raw snapshots MAY be retained permanently when explicitly required for analytics features. When permanent retention is enabled, table partitioning MUST be implemented to maintain query performance.

**Rationale:** Historical data enables multi-year trend analysis, seasonal pattern detection, and predictive modeling. Permanent retention of raw snapshots preserves granularity needed for ML features and correlations that aggregates cannot support. Monthly partitioning ensures query performance remains acceptable as data grows.

**When Permanent Retention Applies:**
- Features explicitly requiring multi-year raw data analysis (e.g., 004-themeparks-data-collection)
- ML/predictive features requiring granular historical context (e.g., 005, 006)
- Year-over-year comparisons at sub-daily granularity

**When 24-Hour Retention Applies:**
- Standard operational features not requiring historical raw data
- Features where aggregated summaries provide sufficient context

### III. API Source Attribution
Every page displaying data MUST prominently attribute the data source with a visible, clickable link. This is both a legal requirement and an ethical obligation.

**Rationale:** Respect for the API providers that make this project possible. Required by terms of service.

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

### XI. Cost Management & LLM Delegation
Claude Code usage is expensive and credits are continually depleted. Claude (Sonnet) MUST act as manager/orchestrator, delegating actual work to cheaper, specialized models via Zen/PAL MCP tools whenever possible. Claude's role is high-level coordination, planning, and quality control. Expert analysis, code generation, and detailed reviews should be delegated to cost-effective specialized models.

**Rationale:** Claude Code credits are limited and expensive. Maximizing delegation to Zen/PAL tools (which use cheaper models like GPT-4o, Gemini, etc.) preserves credits while maintaining quality through specialized expert models.

**When to Delegate:**
- **Research/Analysis:** Use Zen `thinkdeep` or `analyze` for technical investigations
- **Code Generation:** Use Zen `codegen` for implementation work
- **Code Review:** Use Zen `codereview` for quality validation
- **Security Audits:** Use Zen `secaudit` for security analysis
- **Refactoring:** Use Zen `refactor` for code improvements
- **Testing:** Use Zen `testgen` for test generation
- **Debugging:** Use Zen `debug` for systematic debugging
- **Complex Problem Solving:** Use PAL `consensus` for multi-model perspectives

**When Claude Should Act Directly:**
- High-level planning and coordination
- User communication and clarification
- Quality control and synthesis of delegated work
- Final decision making when expert models disagree
- Editing files based on expert recommendations
- Simple, quick tasks that don't justify delegation overhead

**Enforcement:** During planning and implementation phases, Claude MUST delegate technical work to Zen/PAL tools unless the task requires direct user interaction or synthesis of multiple expert outputs.

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
- Daily aggregation MUST run to ensure summary tables are populated
- Summary tables are permanent and MUST NOT be automatically deleted
- When permanent retention is enabled (per Principle II):
  - Raw snapshots are retained indefinitely
  - Table MUST be partitioned (monthly RANGE recommended)
  - Storage monitoring MUST be implemented with capacity alerts
- When 24-hour retention applies:
  - Raw data older than 24 hours MUST be deleted after aggregation
  - Daily aggregation MUST run before cleanup to prevent data loss

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
- Claude MUST demonstrate delegation to Zen/PAL tools for technical work (Principle XI)
- Violations of Principles VI-VII-IX-X-XI (TDD, DRY, Production Integrity, AI Review, Cost Management) are grounds for immediate PR rejection or deployment rollback
- PRs missing Zen review report or ignoring recommendations without justification will be rejected

**Version**: 1.3.0 | **Ratified**: 2025-11-22 | **Last Amended**: 2025-12-31
