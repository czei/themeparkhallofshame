# Feature Specification: Theme Park Downtime Tracker

**Feature Branch**: `001-theme-park-tracker`
**Created**: 2025-11-22
**Status**: Draft
**Input**: User description: "I want to implement the webpage described at /Users/czei/Projects/ThemeParkHallOfShame/hall_of_shame_mary_blair_updated.html. There has to be a program that collects data from queue-times.com, and then there has to be a mechanism for calculating the statistics that feature prominently."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - View Park Downtime Rankings (Priority: P1)

As a theme park enthusiast, I want to see which theme parks have the highest ride downtime so I can make informed decisions about which parks to visit and when.

**Why this priority**: This is the core value proposition of the entire application. It provides immediate utility to users and requires the full data pipeline to be operational (collection, processing, display).

**Independent Test**: Can be fully tested by navigating to the homepage and verifying that a ranked list of parks appears with downtime metrics. Delivers immediate value showing which parks have operational issues.

**Acceptance Scenarios**:

1. **Given** I am on the homepage, **When** I view the "Parks with Most Downtime" section, **Then** I see parks ranked by total downtime hours with location, affected ride counts, and trends
2. **Given** I select a time period filter (Today, 7 Days, 30 Days), **When** the page refreshes, **Then** I see updated rankings reflecting the selected time window
3. **Given** park data is available, **When** I view the statistics section, **Then** I see aggregate metrics including total parks tracked, peak downtime, and currently down rides
4. **Given** I am viewing a park's downtime bar, **When** I observe the visual indicator, **Then** the bar width accurately represents the relative downtime compared to other parks

---

### User Story 2 - View Individual Ride Performance (Priority: P2)

As a theme park visitor planning my day, I want to see which specific rides have the most downtime so I can prioritize visiting reliable attractions first.

**Why this priority**: Provides granular detail beyond park-level data, helping users make ride-specific decisions. Depends on P1's data collection infrastructure but focuses on different aggregation.

**Independent Test**: Can be tested by viewing the "Individual Ride Performance" table and verifying rides are sorted by downtime with accurate status indicators. Delivers value by identifying problematic attractions.

**Acceptance Scenarios**:

1. **Given** I am on the rides section, **When** I view the "Highest Downtime Attractions" table, **Then** I see rides ranked by downtime with current status badges (Down/Running)
2. **Given** a ride is currently operational, **When** I check its status badge, **Then** I see "Running" in turquoise color
3. **Given** a ride is currently closed, **When** I check its status badge, **Then** I see "Down" in coral color
4. **Given** I view a ride's 7-day average, **When** comparing to today's downtime, **Then** I see a trend indicator showing improvement or deterioration as a percentage

---

### User Story 3 - Monitor Real-Time Wait Times (Priority: P3)

As someone planning a theme park visit, I want to see current and average wait times for popular attractions so I can plan my visit during less crowded periods.

**Why this priority**: Complements the downtime data by showing operational capacity even when rides are functioning. Less critical than knowing if rides are down, but valuable for planning.

**Independent Test**: Can be tested by viewing the wait times section and confirming rides are sorted by current wait time with averages and peak values displayed. Helps users identify crowded attractions.

**Acceptance Scenarios**:

1. **Given** I am viewing the wait times section, **When** I see the "Longest Wait Times" table, **Then** rides are sorted by current wait time in descending order
2. **Given** wait time data is available, **When** I view a ride entry, **Then** I see current wait, 7-day average, peak wait, and trend percentage
3. **Given** I select the "Live" time filter, **When** the page loads, **Then** I see the most recent wait time data (within 10 minutes)
4. **Given** I select "7 Day Average" filter, **When** the data displays, **Then** current wait times reflect weekly averages rather than live data

---

### User Story 4 - View Weighted Downtime Rankings (Priority: P4)

As a data-driven analyst, I want parks to be ranked using weighted downtime scores that account for ride importance so I can fairly compare parks with different ride portfolios (e.g., parks with many major coasters vs. parks with mostly kiddie rides).

**Why this priority**: Critical for fair park-to-park comparisons. Raw downtime hours unfairly penalize parks with many minor attractions. A Tier 1 ride (major coaster) going down for 8 hours should count more than a carousel (Tier 3) being down for the same duration.

**Independent Test**: Can be tested by comparing two parks where one has a Tier 1 ride down vs another with multiple Tier 3 rides down, and verifying the weighted score properly reflects the impact. Ensures apples-to-apples comparisons.

**Acceptance Scenarios**:

1. **Given** I view park rankings, **When** Park A has a Tier 1 ride (3x weight) down for 8 hours and Park B has three Tier 3 rides (1x weight) down for 8 hours each, **Then** Park A's weighted score is higher (8×3=24 vs 8×3=24)
2. **Given** each ride has been classified into Tier 1/2/3, **When** I view the "Parks with Most Downtime" table, **Then** parks are ranked by weighted downtime score formula: `Σ(downtime_hours × tier_weight) / Σ(all_ride_weights)`
3. **Given** I want to understand a park's score, **When** I view park details, **Then** I see tier distribution (e.g., "11 Tier 1, 18 Tier 2, 12 Tier 3 rides") and total park weight
4. **Given** ride classifications exist, **When** viewing individual ride performance, **Then** each ride shows its tier classification (Tier 1/2/3) with a brief explanation

---

### User Story 5 - Filter by Park Type (Priority: P5)

As a user interested in specific park operators, I want to filter results to show only Disney and Universal parks versus all tracked parks so I can focus on the parks I care about.

**Why this priority**: Provides personalization without being essential. Users can still derive value from unfiltered data. This is a nice-to-have feature that enhances usability.

**Independent Test**: Can be tested by toggling between "Disney & Universal" and "All Parks" filters and verifying that table results update accordingly. Improves user experience for focused browsing.

**Acceptance Scenarios**:

1. **Given** I am viewing any data table, **When** I click "Disney & Universal" filter, **Then** only parks operated by Disney or Universal appear in results
2. **Given** I have the Disney & Universal filter active, **When** I click "All Parks", **Then** all tracked North American parks appear in the results
3. **Given** I apply a park filter, **When** aggregate statistics update, **Then** the statistics (parks tracked, peak downtime, currently down) reflect only the filtered subset
4. **Given** I switch between filters, **When** the page updates, **Then** the transition happens without full page reload

---

### User Story 6 - Access Detailed Statistics on Queue-Times.com (Priority: P6)

As a user wanting deeper insights, I want to easily navigate to Queue-Times.com for detailed hourly statistics and charts so I can access comprehensive data while respecting the original data source.

**Why this priority**: Drives traffic to Queue-Times.com out of respect for providing the free API, while keeping our site focused on the unique "Hall of Shame" rankings perspective. This is good citizenship in the data ecosystem.

**Independent Test**: Can be tested by clicking park/ride names or "View Detailed Stats" links and verifying navigation to appropriate Queue-Times.com pages. Demonstrates proper attribution and traffic sharing.

**Acceptance Scenarios**:

1. **Given** I am viewing a park in the rankings table, **When** I click the park name, **Then** I am navigated to that park's page on Queue-Times.com in a new tab
2. **Given** I am viewing a ride in the performance table, **When** I click the ride name, **Then** I am navigated to that ride's detailed page on Queue-Times.com
3. **Given** I want hourly statistics or charts, **When** I look for these features, **Then** I see prominent links directing me to Queue-Times.com rather than attempting to view them on Hall of Shame
4. **Given** I am on any page, **When** I see data visualizations or statistics, **Then** attribution to Queue-Times.com is clearly visible with actionable links

---

### User Story 7 - Learn About Project Mission (Priority: P7)

As a curious visitor, I want to understand the purpose and methodology of the Hall of Shame tracker so I can appreciate that this is data-driven analysis, not criticism of maintenance workers.

**Why this priority**: Important for context and credibility but not core functionality. Users can use the tracker without reading the "About" section, but it provides important framing.

**Independent Test**: Can be tested by clicking the "About This Project" link and verifying a modal appears with the mission statement and methodology explanation. Builds trust and transparency.

**Acceptance Scenarios**:

1. **Given** I am on any page, **When** I click "About This Project" link, **Then** a modal overlay appears with the project mission and methodology
2. **Given** the modal is open, **When** I click the close button (×), **Then** the modal closes and I return to the underlying page
3. **Given** the modal is open, **When** I click outside the modal content area, **Then** the modal closes
4. **Given** the modal is open, **When** I press the Escape key, **Then** the modal closes

---

### User Story 8 - View Performance Trends (Priority: P8)

As a theme park analyst, I want to see which parks and rides are improving or declining in reliability over time so I can identify positive maintenance trends and emerging problems.

**Why this priority**: Adds temporal context to the data, highlighting parks/rides that are getting better or worse. This is valuable for understanding maintenance investment trends and operational changes, but not essential for basic downtime tracking.

**Independent Test**: Can be tested by viewing the Trends section and verifying that parks/rides with significant uptime percentage changes (≥5% improvement or decline) appear in the appropriate tables, sorted by the magnitude of change. Delivers value by showing reliability trajectories.

**Acceptance Scenarios**:

1. **Given** I click the "Trends" navigation tab, **When** the page loads, **Then** I see four trend tables: Parks Most Improved, Parks Declining Performance, Rides Most Improved, Rides Declining Performance
2. **Given** I am viewing the "Parks - Most Improved" table, **When** examining the data, **Then** I see parks that have improved uptime percentage by 5% or more, ranked by percentage improvement, showing previous period uptime %, current period uptime %, absolute change, and improvement percentage
3. **Given** I am viewing the "Parks - Declining Performance" table, **When** examining the data, **Then** I see parks that have declined uptime percentage by 5% or more, ranked by percentage decline
4. **Given** I am viewing the "Rides - Most Improved" table, **When** examining the data, **Then** I see individual attractions that have improved uptime percentage by 5% or more, ranked by percentage improvement
5. **Given** I am viewing the "Rides - Declining Performance" table, **When** examining the data, **Then** I see individual attractions that have declined uptime percentage by 5% or more, ranked by percentage decline
6. **Given** I select "Today" time period, **When** viewing trends, **Then** comparisons show today vs. yesterday uptime percentages
7. **Given** I select "7 Days" time period, **When** viewing trends, **Then** comparisons show current 7 days vs. previous 7 days uptime percentages
8. **Given** I select "30 Days" time period, **When** viewing trends, **Then** comparisons show current 30 days vs. previous 30 days uptime percentages
9. **Given** I apply the "Disney & Universal" park filter, **When** viewing trends, **Then** only Disney and Universal parks/rides appear in all trend tables
10. **Given** I apply the "All Parks" filter, **When** viewing trends, **Then** all tracked North American parks/rides appear in trend tables
11. **Given** a park has improved from 70% uptime to 80% uptime, **When** I view the improvement metric, **Then** I see "+10%" improvement (the uptime percentage point increase)
12. **Given** a ride has declined from 95% uptime to 85% uptime, **When** I view the decline metric, **Then** I see "-10%" decline (the uptime percentage point decrease)

---

### Edge Cases

- What happens when Queue-Times API is unavailable or returns errors during data collection?
- How does the system handle parks that have no rides currently tracked or all rides show zero wait times?
- What should display when a park appears to be closed (no ride activity) during operating hours?
- How does the system handle time zone differences for parks across North America?
- What happens when a ride is permanently removed and no longer appears in API responses?
- How should the system handle historical data queries for time periods before tracking began?
- What happens when database aggregation jobs fail or fall behind schedule?
- How should malformed or unrealistic data from the API (e.g., 999-minute wait times) be handled?
- What happens when a new ride is added to a park that hasn't been classified yet (no tier assignment)?
- How should the system handle rides with low classification confidence scores (<0.7)?
- What should display when comparing parks with vastly different ride portfolios (e.g., 50 rides vs. 10 rides)?
- How should manual classification overrides be tracked and audited for quality control?
- What happens if the same ride appears under different names in the API over time?

## Requirements *(mandatory)*

### Functional Requirements

**Data Collection:**

- **FR-001**: System MUST fetch current ride status data from Queue-Times.com API for all North American theme parks every 10 minutes
- **FR-002**: System MUST correctly determine ride operational status by treating any ride with wait_time > 0 as open, regardless of is_open flag value from API
- **FR-003**: System MUST detect park operating hours by identifying when any ride first shows activity (opens) and when last activity occurs (closes) each day in the park's local timezone (using parks.timezone field to convert UTC timestamps), storing session_date as local date and session_start_utc/session_end_utc as UTC timestamps
- **FR-004**: System MUST record every status change (open to closed, closed to open) for each ride with timestamp and duration in previous status
- **FR-005**: System MUST calculate and store park-level aggregate metrics including total rides tracked, rides currently open/closed, and average wait times per collection cycle

**Data Retention:**

- **FR-006**: System MUST retain raw 10-minute snapshot data for 24 hours only
- **FR-007**: System MUST calculate daily summary statistics from 24-hour raw data with scheduled attempts at 12:10 AM, 1:10 AM, and 2:10 AM (up to 3 retry attempts), including uptime percentage (during park operating hours only), average/min/max wait times, and total downtime minutes, and MUST log successful completion in aggregation_log table before raw data cleanup
- **FR-008**: System MUST permanently store daily, weekly, monthly, and yearly aggregated statistics while deleting raw snapshots only for periods that have been successfully aggregated (verified via aggregation_log table with status='success')
- **FR-009**: System MUST calculate uptime percentage based only on time when the park was actually operating, excluding overnight closure periods
- **FR-045**: System MUST run daily aggregation jobs in a timezone-aware manner by iterating through all distinct park timezones and aggregating each timezone group separately, ensuring that the 24-hour aggregation window aligns with each park's local calendar day (e.g., parks in America/Los_Angeles are aggregated for their local midnight-to-midnight period, not UTC midnight)

**Display - Park Rankings:**

- **FR-010**: System MUST display parks ranked by total downtime hours for user-selected time periods (Today, 7 Days, 30 Days)
- **FR-011**: System MUST show for each park: rank number, park name, location (city/state), total downtime hours, affected ride count (currently closed), and trend indicator (percentage change)
- **FR-012**: System MUST calculate trend as percentage change comparing current period downtime to previous equivalent period (e.g., this week vs. last week)
- **FR-013**: System MUST display aggregate statistics at top of page showing total parks tracked, peak downtime value, and currently down ride count

**Display - Ride Performance:**

- **FR-014**: System MUST display individual rides ranked by downtime hours for selected time periods
- **FR-015**: System MUST show for each ride: rank, ride name, park name, total downtime, current status badge (Down/Running), and 7-day average downtime
- **FR-016**: System MUST update status badges to reflect current operational state based on most recent API collection (within 10 minutes)

**Display - Wait Times:**

- **FR-017**: System MUST display rides sorted by current wait time in descending order
- **FR-018**: System MUST show for each ride: current wait time (minutes), 7-day average wait, peak wait time, and trend percentage
- **FR-019**: System MUST support time filter modes: Live (current data), 7 Day Average (weekly averages), and Peak Times (maximum values)

**Filtering:**

- **FR-020**: System MUST allow users to filter all views by park operator: "Disney & Universal" showing only parks from these two operators, or "All Parks" showing all North American parks
- **FR-021**: System MUST update all statistics and aggregates when filters are applied to reflect only the filtered subset

**Ride Classification & Weighted Scoring:**

- **FR-022**: System MUST classify each ride into one of three tiers: Tier 1 (major attractions with 3x weight), Tier 2 (standard attractions with 2x weight), or Tier 3 (minor attractions with 1x weight)
- **FR-023**: System MUST use hierarchical 4-tier classification logic: (1) manual_overrides.csv (human corrections), (2) exact_matches.json (cached AI decisions with confidence > 0.85), (3) pattern matching (keyword rules for obvious cases like "kiddie", "carousel", "theater"), (4) AI agent with web search capability for ambiguous rides
- **FR-024**: System MUST calculate park downtime scores using weighted formula: `Park Score = Σ(downtime_hours × tier_weight) / Σ(all_ride_weights)` where tier_weight is 3, 2, or 1
- **FR-025**: System MUST support manual overrides for ride classifications via CSV import (park_id, ride_id, override_tier, reason)
- **FR-026**: System MUST generate confidence scores (0.0 to 1.0) for each automated classification based on match certainty
- **FR-027**: System MUST display ride tier (1/2/3) alongside ride name in all ride performance views
- **FR-028**: System MUST show park tier distribution (count of Tier 1/2/3 rides) in park detail views to explain weighted scoring
- **FR-029**: System MUST invoke AI agent with web search for rides not found in manual overrides, cached decisions, or high-confidence pattern matches to research ride specifications, park context, and guest perception before assigning tier
- **FR-030**: System MUST cache AI classification decisions with confidence > 0.85 in exact_matches.json using stable cache key format `{park_id}:{ride_id}` with schema versioning for cache invalidation
- **FR-031**: System MUST flag rides with classification confidence < 0.5 for mandatory human review and addition to manual_overrides.csv
- **FR-032**: System MUST use parallel processing (ThreadPoolExecutor) when classifying multiple rides via AI agent to reduce total processing time from 30+ minutes to under 5 minutes

**Display - Trends:**

- **FR-046**: System MUST display four trend tables when Trends navigation tab is selected: Parks Most Improved, Parks Declining Performance, Rides Most Improved, Rides Declining Performance
- **FR-047**: System MUST calculate trends by comparing current period uptime percentage to previous equivalent period (today vs yesterday, 7 days vs previous 7 days, 30 days vs previous 30 days)
- **FR-048**: System MUST only display parks/rides in trend tables if uptime percentage change is ≥5% (either improvement or decline)
- **FR-049**: System MUST rank "Most Improved" tables by uptime percentage improvement (largest improvement first)
- **FR-050**: System MUST rank "Declining Performance" tables by uptime percentage decline (largest decline first)
- **FR-051**: System MUST show for each park trend: park name, location, previous period uptime %, current period uptime %, absolute percentage point change, and improvement/decline percentage
- **FR-052**: System MUST show for each ride trend: ride name, park name, previous period uptime %, current period uptime %, absolute percentage point change, and improvement/decline percentage
- **FR-053**: System MUST apply park filter (Disney & Universal / All Parks) to trend tables
- **FR-054**: System MUST respect time period selector (Today, 7 Days, 30 Days) when calculating trend comparisons

**Attribution & Transparency:**

- **FR-033**: System MUST display "Data powered by Queue-Times.com" with clickable link on every page
- **FR-034**: System MUST provide accessible "About This Project" modal explaining the project mission emphasizing respect for maintenance professionals and data-driven transparency
- **FR-035**: System MUST provide prominent links directing users to Queue-Times.com for detailed hourly statistics, charts, and historical data rather than duplicating their analytical features
- **FR-036**: Park names and ride names MUST be clickable links that navigate users to the corresponding park or ride page on Queue-Times.com for detailed information

**Performance:**

- **FR-037**: System MUST return current status queries in under 50 milliseconds
- **FR-038**: System MUST return historical/aggregate queries in under 100 milliseconds
- **FR-039**: System MUST complete data collection for all parks within 5 minutes per cycle
- **FR-040**: System MUST update frontend data freshness within 10 minutes of real-world changes (API update frequency)

**Security & Rate Limiting:**

- **FR-041**: System MUST require API key authentication (X-API-Key header) for all REST API endpoints
- **FR-042**: System MUST enforce rate limiting of 100 requests per hour and 1000 requests per day per API key
- **FR-043**: System MUST return HTTP 401 Unauthorized for requests without valid API key
- **FR-044**: System MUST return HTTP 429 Too Many Requests when rate limits are exceeded with Retry-After header indicating wait time

### Key Entities

- **Park**: Represents a theme park with attributes including name, location (city, state/province, country), geographic coordinates, timezone, and operator/group affiliation. Related to multiple rides.

- **Ride**: Represents an individual attraction within a park with attributes including name, current operational status, tier classification (1/2/3), and association with a specific land/area. Related to a parent park and has historical status records.

- **Ride Classification**: Represents the tier assignment for a ride including tier level (1/2/3), tier weight (3/2/1), classification method (exact_match, pattern_match, manual_override), confidence score (0.0-1.0), and reasoning text. Used to calculate weighted downtime scores.

- **Ride Status Snapshot**: Represents a point-in-time capture of ride operational state including whether the ride is open/closed (computed from wait time and API flag), current wait time in minutes, and timestamp of observation. Retained for 24 hours only.

- **Status Change Event**: Represents a transition between operational states (open↔closed) including previous status, new status, timestamp of change, wait time at moment of change, and duration spent in previous status. Used to calculate downtime periods.

- **Park Activity Snapshot**: Represents park-wide operational metrics at a point in time including total rides tracked, count of open/closed rides, average and maximum wait times, and derived "park appears open" flag based on any ride activity.

- **Operating Session**: Represents a single day's operating period for a park including date, session start time (first detected ride activity), session end time (last detected activity), and total operating minutes. Used to calculate meaningful uptime percentages.

- **Daily Statistics**: Permanent aggregation of ride or park performance over a 24-hour period including uptime/downtime minutes and percentages (during operating hours only), average/min/max wait times, status change count, and longest single downtime period.

- **Weekly/Monthly/Yearly Statistics**: Similar to daily statistics but aggregated over longer time periods, including trend analysis fields comparing to previous equivalent periods. Calculated from daily aggregates, not raw snapshots.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Users can view ranked park downtime data within 2 seconds of page load
- **SC-002**: System successfully collects data from 80 or more North American theme parks
- **SC-003**: Data freshness remains within 10 minutes of real-time for 95% of collections
- **SC-004**: System maintains 99% uptime for data collection service
- **SC-005**: Frontend achieves 99.9% availability for user viewing
- **SC-006**: Database queries return results in under 100 milliseconds on average
- **SC-007**: Users can complete primary task (viewing park or ride rankings) within 30 seconds of arriving on site
- **SC-008**: Daily aggregation process completes within 5 minutes
- **SC-009**: Storage growth remains under 500 MB for first year of operation
- **SC-010**: Attribution to Queue-Times.com is visible on every page without requiring scrolling or interaction

## Assumptions

- Queue-Times.com API will remain free with required attribution and continue updating data every 5 minutes as documented
- North American parks constitute the primary market and other continents can be excluded without significant user impact
- 10-minute collection frequency provides sufficient granularity for downtime tracking without overwhelming the API or database
- Park operating hours can be reliably inferred from ride activity rather than requiring manual schedule entry or separate API
- Users are primarily interested in recent data (today, 7 days, 30 days) rather than deep historical analysis beyond yearly trends
- The "is_open" API flag quirk (showing false while wait_time > 0) is consistent and applying the documented logic is reliable
- Shopify hosting will be via iframe embedding of a separately hosted dashboard rather than native Shopify theme integration
- 24-hour retention of raw data provides sufficient resolution for daily statistics without excessive storage costs
- Users will access the site primarily during daytime hours (not expecting heavy 24/7 real-time monitoring usage)
- Downtime during overnight closure periods should not count toward uptime calculations (only park operating hours matter)
- Hall of Shame provides unique value through downtime-focused rankings while Queue-Times.com excels at detailed hourly statistics and charts - complementary rather than competitive positioning
- Users benefit from being directed to Queue-Times.com for detailed analysis, creating a symbiotic relationship that respects the data provider
- Rule-based ride classification (Tier 1/2/3) provides sufficient accuracy with manual override capability, avoiding need for machine learning
- Ride tier distribution will approximate 10% Tier 1, 40% Tier 2, 50% Tier 3 across all parks for balanced weighted scoring
- Automated classification confidence threshold of 0.7 provides acceptable balance between automation and manual review workload
- Ride names and characteristics from Queue-Times.com API remain stable enough for pattern-based classification
- Manual classification overrides will be needed for <5% of rides after initial automated run

## Dependencies

- Queue-Times.com API availability and continued free tier access with 5-minute update frequency
- Existing Shopify site at themeparkwaits.com for hosting the frontend dashboard
- AWS Linux server infrastructure (EC2) with MySQL 8.0 database for data collection and storage
- Python 3.11+ runtime environment for data collector and classification script
- Apache 2.4 web server with mod_wsgi for Flask API hosting
- Sufficient server storage to accommodate 24-hour rolling window plus permanent daily/weekly/monthly/yearly aggregates
- Network connectivity between data collector server and Queue-Times.com API
- Ability to embed iframe or similar integration method within Shopify theme
- Python libraries: requests, pandas, Flask, SQLAlchemy, tenacity, python-json-logger, tqdm

## Out of Scope

- International parks outside North America (Europe, Asia, etc.) - limited to North American continent only
- User authentication or personalized accounts - this is a read-only public dashboard
- Mobile native applications - web responsive design only
- Real-time alerts or notifications when rides go down - passive viewing only
- Weather data integration or correlation with downtime patterns
- Predictive analytics or machine learning models forecasting future downtime
- Social features such as user comments, reviews, or sharing functionality
- Integration with park ticketing systems or pricing data
- Historical data before the system launch date - no retroactive data collection
- Detailed ride technical specifications or maintenance schedules beyond operational status
- Multi-language support - English only
- Accessibility features beyond standard web best practices (no WCAG AAA compliance requirement)
- Custom reporting or data export features for users
- Administrative dashboard for manual data overrides or corrections (manual overrides via CSV only)
- Hourly charts and detailed historical visualizations - users will be directed to Queue-Times.com for these features out of respect for the data source
- Duplicate analytical features that Queue-Times.com already provides (e.g., wait time prediction, crowd calendars, detailed ride history graphs)
- Machine learning or AI-based ride classification - rule-based pattern matching with manual overrides only
- Automated re-classification when ride names change - manual review required
- User voting or crowdsourced ride tier assignments - classifications determined by project maintainers
- Real-time ride tier adjustments based on temporary changes (e.g., ride refurbishments) - tier represents permanent attraction class
