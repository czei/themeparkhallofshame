# Feature Specification: Comprehensive ThemeParks.wiki Data Collection

**Feature Branch**: `004-themeparks-data-collection`
**Created**: 2025-12-21
**Status**: Draft - Blocked by feature 003 (ORM refactoring)
**Input**: User description: "comprehensive themeparks.wiki data collection"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Collect Complete Attraction Metadata (Priority: P1)

The system needs complete metadata for all attractions (rides, shows, dining) to enable location-based optimization, indoor/outdoor classification, and attraction filtering.

**Why this priority**: This is foundational data required by features 005 (pattern analysis) and 006 (visit optimization). Without coordinates, indoor/outdoor classification, and attraction types, we cannot build walking routes, weather-based recommendations, or proper categorization.

**Independent Test**: Can be fully tested by querying the themeparks.wiki API for all entities, extracting metadata fields, storing them in the database, and verifying completeness (all entities have coordinates, type, indoor/outdoor status).

**Acceptance Scenarios**:

1. **Given** a park exists in themeparks.wiki, **When** the data collection runs, **Then** all attractions have geographic coordinates (latitude, longitude) stored for entrance and exit points
2. **Given** an attraction exists, **When** metadata is collected, **Then** the attraction is classified as indoor, outdoor, or hybrid (covered queue/outdoor ride)
3. **Given** an attraction has height requirements, **When** metadata is collected, **Then** minimum and maximum height restrictions are stored
4. **Given** an attraction has a specific type (roller coaster, dark ride, show, restaurant), **When** metadata is collected, **Then** the attraction type is stored using themeparks.wiki taxonomy
5. **Given** attraction metadata exists in the database, **When** queried by downstream features, **Then** all fields are non-null for critical attributes (coordinates, type, indoor/outdoor)

---

### User Story 2 - Collect All Queue Types and Virtual Queue Data (Priority: P1)

The system needs complete queue data beyond standby wait times, including Lightning Lane, Single Rider, and paid queue options, to enable virtual queue ROI analysis and optimization.

**Why this priority**: Critical for feature 005 (visit optimization with Lightning Lane). Currently only collecting standby wait times, missing 80% of queue data needed for virtual queue analysis.

**Independent Test**: Can be tested by comparing collected data against themeparks.wiki API response structure, verifying all queue types (STANDBY, SINGLE_RIDER, RETURN_TIME, PAID_RETURN_TIME, BOARDING_GROUP, PAID_STANDBY) are captured when available.

**Acceptance Scenarios**:

1. **Given** an attraction has single rider queue, **When** data is collected, **Then** single rider wait time is stored separately from standby wait time
2. **Given** an attraction has Lightning Lane (PAID_RETURN_TIME), **When** data is collected, **Then** Lightning Lane availability status, return time window (start/end), and price are stored
3. **Given** an attraction has Disney Genie+ (BOARDING_GROUP), **When** data is collected, **Then** boarding group status (AVAILABLE/PAUSED/CLOSED), current group range, next allocation time, and estimated wait are stored
4. **Given** queue data is collected over time, **When** analyzing historical patterns, **Then** we can correlate virtual queue availability with standby wait times
5. **Given** Lightning Lane pricing changes during the day, **When** data is collected, **Then** all price changes are captured with timestamps

---

### User Story 3 - Collect Show and Entertainment Schedules (Priority: P2)

The system needs complete show schedules, parade routes, and character meet-and-greet times to enable visit planning that includes non-ride attractions.

**Why this priority**: Required for feature 005 (visit optimization) user stories 4 and 8. Shows and parades are fixed-time events that constrain visit plans. Character meets are popular attractions that need planning time.

**Independent Test**: Can be tested by extracting showtimes and schedules from themeparks.wiki API, storing them with fixed start/end times, and verifying downstream features can query upcoming shows for any park and date.

**Acceptance Scenarios**:

1. **Given** a park has scheduled shows, **When** show data is collected, **Then** all showtimes (start, end, duration) are stored for the next 30 days
2. **Given** a park has parades, **When** parade data is collected, **Then** parade routes, viewing locations, start times, and expected duration are stored
3. **Given** a park has character meet-and-greet locations, **When** character data is collected, **Then** character names, locations, and scheduled times (if fixed) are stored
4. **Given** show schedules change seasonally, **When** data collection runs daily, **Then** schedule updates are detected and stored with version history
5. **Given** a show is cancelled due to weather, **When** real-time data is collected, **Then** the cancellation status is captured and reflected in availability

---

### User Story 4 - Collect Park Schedule and Special Event Data (Priority: P2)

The system needs detailed park operating hours (park-level and per-attraction), special event classifications, and ticket pricing to enable accurate availability predictions and crowd pattern analysis.

**Why this priority**: Essential for feature 004 (pattern analysis) to distinguish normal operating days from special events (After Hours, holiday parties, private events). Per-attraction hours reveal rides that open late or close early, impacting optimization.

**Independent Test**: Can be tested by collecting schedule data from themeparks.wiki `/schedule` endpoint, storing park hours and special events for 30+ days forward, and validating that hourly aggregations correctly filter out non-operating periods.

**Acceptance Scenarios**:

1. **Given** a park has published operating hours, **When** schedule data is collected, **Then** park open/close times are stored for the next 60 days
2. **Given** an attraction has different operating hours than the park, **When** per-attraction schedules are collected, **Then** attraction-specific open/close times are stored
3. **Given** a park has a special event (TICKETED_EVENT, PRIVATE_EVENT, EXTRA_HOURS), **When** schedule data is collected, **Then** event type, start/end times, and ticket requirements are stored
4. **Given** ticket pricing varies by date (surge pricing), **When** pricing data is collected, **Then** ticket prices by date and type (1-day, multi-day, park hopper) are stored
5. **Given** park hours change due to weather or capacity, **When** real-time updates occur, **Then** schedule changes are captured with timestamps for historical accuracy

---

### User Story 5 - Collect Dining Availability Data (Priority: P3)

The system should collect restaurant availability data to enable dining recommendations and meal break planning in visit optimization.

**Why this priority**: Lower priority than core ride/show data. Useful for complete visit planning (feature 005 user story 4) but not blocking for initial MVP. Dining reservations are handled externally by park apps.

**Independent Test**: Can be tested by collecting diningAvailability data from themeparks.wiki API, storing restaurant locations and availability windows, and providing query interface for "restaurants with availability at time X".

**Acceptance Scenarios**:

1. **Given** a restaurant accepts reservations, **When** dining data is collected, **Then** restaurant name, location, cuisine type, and reservation availability by party size are stored
2. **Given** a restaurant has walk-up availability, **When** dining data is collected, **Then** estimated wait times (if available) are stored
3. **Given** dining availability changes throughout the day, **When** data is refreshed, **Then** availability windows are updated and historical snapshots are retained
4. **Given** a restaurant is closed for refurbishment, **When** schedule data is collected, **Then** closure dates and expected reopening are stored

---

### Edge Cases

- What happens when themeparks.wiki API is unavailable or rate-limited during data collection?
- How do we handle attractions that exist in the park but are not yet in themeparks.wiki (new attractions)?
- What happens when an attraction changes type (e.g., restaurant becomes a shop, ride gets rethemed)?
- How do we handle seasonal attractions that only operate certain months (water rides in winter)?
- What happens when themeparks.wiki API schema changes (new fields added, fields removed, field types changed)?
- How do we handle attractions with multiple entrance points (e.g., FastPass entrance vs. standby entrance)?
- What happens when Lightning Lane pricing data is not available from API (requires manual data entry)?
- How do we handle parks that don't fully populate all API fields (incomplete data)?
- What happens when show schedules conflict (same performer in two shows at overlapping times)?
- How do we handle character meet-and-greets that are walk-up only (no fixed schedule)?

## Requirements *(mandatory)*

### Functional Requirements

**Data Collection - Attraction Metadata:**
- **FR-001**: System MUST collect geographic coordinates (latitude, longitude) for all attractions from themeparks.wiki `/entity/{id}` endpoint
- **FR-002**: System MUST store separate coordinates for attraction entrance and exit points where available
- **FR-003**: System MUST classify each attraction as indoor, outdoor, or hybrid using available metadata (tags, entity type)
- **FR-004**: System MUST collect and store attraction type using themeparks.wiki entityType taxonomy (RIDE, SHOW, RESTAURANT, SHOP, MEET_AND_GREET)
- **FR-005**: System MUST collect height requirements (minimum and maximum) for all rides where applicable
- **FR-006**: System MUST collect thrill level or intensity rating where provided by themeparks.wiki
- **FR-007**: System MUST collect attraction duration (average ride/show length) where available

**Data Collection - Queue Data:**
- **FR-008**: System MUST collect standby queue wait times (existing functionality, ensure preserved)
- **FR-009**: System MUST collect single rider queue wait times from `queue.SINGLE_RIDER.waitTime` when available
- **FR-010**: System MUST collect Lightning Lane / Genie+ data from `queue.PAID_RETURN_TIME` including status, return window (start/end), and price
- **FR-011**: System MUST collect Disney boarding group data from `queue.BOARDING_GROUP` including allocation status, current group range, next allocation time, and estimated wait
- **FR-012**: System MUST collect paid standby queue data from `queue.PAID_STANDBY.waitTime` when available
- **FR-013**: System MUST collect regular return time data from `queue.RETURN_TIME` for attractions with virtual queue systems

**Data Collection - Shows and Entertainment:**
- **FR-014**: System MUST collect show schedules from `showtimes[]` array including start time, end time, and duration
- **FR-015**: System MUST collect parade schedules, routes, and viewing locations where provided
- **FR-016**: System MUST collect character meet-and-greet locations and scheduled times where available
- **FR-017**: System MUST update show schedules daily to capture seasonal changes and cancellations

**Data Collection - Park Schedules:**
- **FR-018**: System MUST collect park operating hours from `/entity/{id}/schedule` endpoint for 60 days forward
- **FR-019**: System MUST collect per-attraction operating hours from `operatingHours[]` array when different from park hours
- **FR-020**: System MUST collect special event data including event type (TICKETED_EVENT, PRIVATE_EVENT, EXTRA_HOURS), start/end times, and description
- **FR-021**: System MUST collect ticket pricing data by date, ticket type, and pricing tier (standard/peak/premium) from `purchases[]` array

**Data Collection - Dining:**
- **FR-022**: System MUST collect restaurant metadata including location, cuisine type, and price range
- **FR-023**: System MUST collect dining availability windows from `diningAvailability[]` array by party size and time slot

**Data Storage and Management:**
- **FR-024**: System MUST store all collected data in persistent database with appropriate schema (new tables and columns for enhanced data points)
- **FR-025**: System MUST maintain historical snapshots of queue data (all queue types) with same 24-hour retention as existing ride_status_snapshots
- **FR-026**: System MUST maintain permanent reference data for attraction metadata (coordinates, type, indoor/outdoor, height requirements)
- **FR-027**: System MUST update attraction metadata daily to capture new attractions, closures, and rethemes
- **FR-028**: System MUST version show schedules and park hours to track changes over time
- **FR-029**: System MUST handle API rate limiting gracefully with exponential backoff and retry logic

**Data Quality and Completeness:**
- **FR-030**: System MUST log data collection gaps (missing fields, unavailable endpoints) for monitoring and alerting
- **FR-031**: System MUST validate collected data for completeness and flag attractions with missing critical fields (coordinates, type)
- **FR-032**: System MUST provide data freshness indicators (last updated timestamp) for all collected data points
- **FR-033**: System MUST distinguish between "data not available" and "data collection failed" in logs and monitoring

### Key Entities

- **Attraction Metadata**: Comprehensive reference data for each attraction. Includes themeparks_wiki_id (UUID), name, attraction_type (RIDE/SHOW/RESTAURANT/MEET_AND_GREET), indoor_outdoor_classification (INDOOR/OUTDOOR/HYBRID), entrance_latitude, entrance_longitude, exit_latitude, exit_longitude, height_min_cm, height_max_cm, thrill_level, average_duration_minutes. Updated daily, stored permanently.

- **Queue Snapshot (Enhanced)**: Expanded version of existing ride_status_snapshots to include all queue types. Adds fields: single_rider_wait_time, lightning_lane_status, lightning_lane_return_start, lightning_lane_return_end, lightning_lane_price_amount, lightning_lane_price_currency, boarding_group_status, boarding_group_current_range, boarding_group_next_allocation, paid_standby_wait_time. Retained for 24 hours like existing snapshots.

- **Show Schedule**: Fixed-time entertainment events. Includes show_id (generated), themeparks_wiki_id, show_name, show_type (SHOW/PARADE/CHARACTER_MEET), park_id, location_latitude, location_longitude, start_time, end_time, duration_minutes, days_of_week (bitmask for recurring shows), valid_from_date, valid_to_date. Versioned to track schedule changes.

- **Park Operating Schedule**: Daily park hours and special events. Includes park_id, schedule_date, park_open_time, park_close_time, event_type (NORMAL/TICKETED_EVENT/PRIVATE_EVENT/EXTRA_HOURS), event_description, requires_special_ticket (boolean), collected_at_timestamp. Stored for 60 days forward, updated daily.

- **Attraction Operating Hours**: Per-attraction hours when different from park hours. Includes attraction_id, schedule_date, attraction_open_time, attraction_close_time, closed_for_refurbishment (boolean), refurbishment_reason. Enables detection of rides that open late or close early.

- **Dining Availability**: Restaurant reservation and walk-up availability. Includes restaurant_id, themeparks_wiki_id, restaurant_name, location_latitude, location_longitude, cuisine_type, price_range, availability_date, availability_time, party_size, available_slots (count or boolean). Updated multiple times per day.

- **Data Collection Audit Log**: Tracks data collection success/failure for monitoring. Includes collection_run_id (generated), collection_type (METADATA/QUEUE/SHOW/SCHEDULE/DINING), park_id, started_at, completed_at, records_collected, errors_encountered, error_details (JSON). Enables alerting on collection failures.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: System collects geographic coordinates for 95%+ of all attractions across all tracked parks within 7 days of feature deployment

- **SC-002**: System captures all 6 queue types (STANDBY, SINGLE_RIDER, RETURN_TIME, PAID_RETURN_TIME, BOARDING_GROUP, PAID_STANDBY) when present in themeparks.wiki API responses, verified by comparing collected data to API response structure

- **SC-003**: Show schedules are updated daily with 99%+ success rate, verified by monitoring collection audit logs

- **SC-004**: Attraction metadata (indoor/outdoor classification, type, coordinates) is complete for 90%+ of attractions within 14 days, measured by counting non-null required fields

- **SC-005**: Data collection handles themeparks.wiki API rate limiting without data loss, achieving 99%+ success rate for all collection runs

- **SC-006**: Historical queue snapshots include Lightning Lane pricing and availability data for 100% of attractions that support Lightning Lane, verified by comparing snapshot data to live API responses

- **SC-007**: System detects and flags new attractions within 24 hours of appearing in themeparks.wiki API

- **SC-008**: Data collection runs complete within 10 minutes for all parks (maintaining existing 5-10 minute collection interval)

- **SC-009**: Database storage for enhanced queue snapshots remains within 2x current snapshot table size (acceptable overhead for 6x more queue data)

- **SC-010**: Downstream features (004 and 005) can query collected data through documented data access methods without requiring direct themeparks.wiki API integration

## Scope & Boundaries

### In Scope

- Collecting ALL available data from themeparks.wiki API endpoints (live, entity, schedule)
- Storing attraction metadata (coordinates, type, indoor/outdoor, height requirements)
- Collecting all queue types (standby, single rider, Lightning Lane, boarding groups)
- Collecting show schedules, parade data, character meet-and-greet times
- Collecting park operating hours and special event classifications
- Collecting per-attraction operating hours
- Collecting dining availability data
- Extending existing database schema with new tables and columns
- Maintaining 24-hour retention for queue snapshots (all types)
- Maintaining permanent storage for reference data (metadata, schedules)
- Data quality monitoring and alerting
- Graceful handling of API rate limiting and failures

### Out of Scope (Future Enhancements)

- Real-time push notifications from themeparks.wiki (API only supports polling)
- Integration with park-specific mobile apps for proprietary data (Disneyland app, Universal app)
- User-submitted wait time data or crowd-sourced information
- Historical data backfill beyond current 24-hour snapshot retention (would require separate archive strategy)
- Data visualization or reporting UI (data collection only, consumed by features 004 and 005)
- Predictive modeling or analysis (that's feature 004)
- Visit optimization algorithms (that's feature 005)

## Assumptions & Dependencies

### Assumptions

- ThemeParks.wiki API continues to provide comprehensive data for all tracked parks (currently supports 100+ parks worldwide)
- API schema remains relatively stable (no major breaking changes without notice)
- API rate limits are reasonable for our 5-10 minute collection interval (current rate: ~1 request per second is acceptable)
- Geographic coordinates in themeparks.wiki API are accurate to within 10 meters
- Indoor/outdoor classification can be inferred from entity tags and type (API doesn't explicitly provide this, requires heuristic)
- Lightning Lane pricing data is available in API responses (may not be available for all parks)
- Show schedules in themeparks.wiki are updated by their data collectors at least weekly
- MySQL database has sufficient storage capacity for 2x current snapshot table size (queue data expansion)
- Existing data collection infrastructure (cron jobs, error handling, retry logic) is stable and can be extended
- No significant changes to existing ride_status_snapshots table structure are needed (only additive columns)

### Dependencies

**CRITICAL BLOCKING DEPENDENCY:**
- **Feature 003 - ORM Refactoring**: Requires reliable data access layer with proper ORM implementation before expanding data collection. Current infrastructure using raw SQL strings is fragile and error-prone. ORM refactoring must complete first to provide stable foundation for new data tables and relationships.

**OTHER DEPENDENCIES:**
- **Existing Data Collection Infrastructure**: Requires existing `collect_snapshots.py` script, `themeparks_wiki_client.py`, and cron job setup (running every 5-10 minutes)
- **MySQL Database**: Requires existing database with write permissions to create new tables and add columns
- **ThemeParks.wiki API Access**: Requires reliable access to themeparks.wiki API with acceptable rate limits
- **Existing Reference Tables**: Requires `parks` and `rides` tables to have `themeparks_wiki_id` mapping (already exists)
- **Python Dependencies**: Requires requests library, tenacity (retry logic), and mysqlclient (already in use)

### External Factors

- ThemeParks.wiki API availability and uptime (out of our control, need graceful degradation)
- Changes to themeparks.wiki API schema or endpoint structure (may require code updates)
- Park operational changes not reflected in API (e.g., last-minute ride closures, show cancellations)
- Data quality variations across parks (some parks provide complete data, others have gaps)
- Rate limiting policies may change (need monitoring and adjustment of collection intervals)
- Storage costs increase with expanded data collection (monitor database size)
