# Feature Specification: Park Visit Optimization

**Feature Branch**: `006-park-visit-optimization`
**Created**: 2025-12-21
**Status**: Draft - Blocked by features 003, 004, and 005
**Input**: User description: "park visit optimization"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Receive Optimized Daily Visit Plan (Priority: P1)

A park visitor wants to maximize their experience by visiting attractions in an order that minimizes total wait time and walking distance while ensuring they experience their must-do attractions based on ride tier priorities.

**Why this priority**: This is the core value proposition - helping visitors save hours of waiting and experience more attractions. This story alone delivers immediate, measurable value.

**Independent Test**: Can be fully tested by requesting a visit plan for a specific park and date, receiving a sequenced list of attractions ordered by tier priority with predicted wait times and walking routes, and validating that the plan is executable within park operating hours.

**Acceptance Scenarios**:

1. **Given** a visitor selects a park and visit date, **When** they request an optimized visit plan, **Then** they receive a time-sequenced list of attractions showing arrival times, predicted wait times, and walking durations prioritized by ride tier
2. **Given** a visitor specifies must-do attractions (e.g., "Space Mountain", "Pirates of the Caribbean"), **When** they request a plan, **Then** all must-do attractions appear in the optimized sequence
3. **Given** a visitor requests a plan for tomorrow, **When** the plan is generated, **Then** it accounts for predicted weather conditions and their impact on wait times
4. **Given** a visitor receives a plan, **When** they view attraction details, **Then** each attraction shows estimated arrival time, predicted wait time, ride duration, walking time to next attraction, and tier classification

---

### User Story 2 - Optimize With Virtual Queue Access (Priority: P1)

A visitor with Lightning Lane access (Disney) or Express Pass (Universal) wants an optimized plan that accounts for their ability to skip standby lines on certain attractions.

**Why this priority**: Virtual queue systems fundamentally change visit optimization strategy. Supporting this is essential for Disney and Universal parks where these systems are prevalent.

**Independent Test**: Can be tested by selecting a Lightning Lane pass type (Multipass, Single Pass, or Premier Pass) and verifying the plan prioritizes virtual queue usage for high-wait attractions while sequencing standby-only attractions optimally.

**Acceptance Scenarios**:

1. **Given** a visitor has Disney Lightning Lane Multipass, **When** they generate a plan, **Then** the plan uses Lightning Lane reservations for high-tier attractions and sequences standby-only attractions in between
2. **Given** a visitor has Disney Lightning Lane Premier Pass, **When** they generate a plan, **Then** the plan allows one-time Lightning Lane access to any attraction and sequences usage to minimize overall wait time
3. **Given** a visitor has Universal Express Pass, **When** they generate a plan, **Then** the plan assumes Lightning Lane access for all Express-enabled attractions and prioritizes standby-only attractions during peak wait times
4. **Given** a visitor has Lightning Lane Single Pass for one attraction, **When** they generate a plan, **Then** the plan reserves that attraction for Lightning Lane and optimizes all others for standby
5. **Given** Lightning Lane return times have windows (e.g., 10:00-11:00 AM), **When** a plan includes Lightning Lane reservations, **Then** the sequence accommodates return time windows

---

### User Story 3 - Discover Wait Time Patterns (Priority: P2)

A visitor wants to understand when attractions are least crowded so they can make informed decisions about when to visit certain rides, even if they don't follow the optimized plan exactly.

**Why this priority**: Provides educational value and builds trust in the optimization system. Users can understand the "why" behind recommendations and make informed deviations.

**Independent Test**: Can be tested by viewing historical wait time patterns for any attraction, showing peak hours, low-traffic periods, and correlation with factors like weather, day of week, and special events.

**Acceptance Scenarios**:

1. **Given** a visitor views an attraction, **When** they check wait time patterns, **Then** they see average wait times by hour of day for the selected date
2. **Given** a visitor views wait time patterns, **When** special events are scheduled (e.g., parades, fireworks), **Then** pattern visualizations highlight impact on nearby attraction wait times
3. **Given** a visitor compares weekday vs weekend patterns, **When** they view the same attraction, **Then** they see clearly differentiated wait time curves
4. **Given** extreme weather is forecasted (high heat, thunderstorms), **When** patterns are displayed, **Then** weather-adjusted predictions are shown with confidence indicators

---

### User Story 4 - Plan Around Special Circumstances (Priority: P2)

A visitor wants to optimize their plan around specific constraints such as dining reservations, show times, early park entry, or accessibility needs.

**Why this priority**: Real-world visits have constraints that simple optimization ignores. This makes plans practical and executable.

**Independent Test**: Can be tested by adding time-based constraints (e.g., lunch reservation at 12:30 PM, parade viewing at 3:00 PM) and verifying the plan accommodates these while still optimizing remaining time.

**Acceptance Scenarios**:

1. **Given** a visitor has a dining reservation at 12:30 PM, **When** they generate a plan, **Then** the plan routes them near the restaurant by 12:15 PM and resumes optimization after their reservation
2. **Given** a visitor wants to see the 3:00 PM parade, **When** they mark it as a must-do, **Then** the plan positions them along the parade route 15 minutes early
3. **Given** a visitor has early park entry (resort guest benefit), **When** they generate a plan, **Then** the plan prioritizes high-demand attractions during the early entry window
4. **Given** a visitor indicates mobility constraints, **When** walking routes are calculated, **Then** distances are minimized and rest periods are suggested

---

### User Story 5 - Real-Time Plan Adjustments (Priority: P3)

A visitor wants to adapt their plan mid-day when circumstances change (e.g., unexpected ride closure, longer-than-predicted waits, spontaneous additions).

**Why this priority**: Plans rarely survive contact with reality. Real-time adaptation maintains value throughout the day.

**Independent Test**: Can be tested by simulating mid-day changes (mark an attraction as completed, skip an attraction, add a new one) and verifying the system recalculates optimal sequencing for remaining time.

**Acceptance Scenarios**:

1. **Given** a visitor marks an attraction as completed, **When** they request plan update, **Then** the remaining attractions are re-optimized for current time and conditions
2. **Given** an attraction experiences unexpected downtime, **When** the visitor checks their plan, **Then** the system automatically removes it and re-sequences remaining attractions
3. **Given** a visitor decides to skip an attraction, **When** they remove it from the plan, **Then** freed time is reallocated to other attractions or suggests adding new ones
4. **Given** current wait time exceeds prediction by 30+ minutes, **When** the visitor is in queue, **Then** the system recalculates downstream timing and suggests whether to stay or leave

---

### User Story 6 - Multi-Day Visit Optimization (Priority: P3)

A visitor with multi-day park tickets wants to distribute must-do attractions across days to minimize total wait time and avoid inefficiencies.

**Why this priority**: Enhances value for multi-day visitors, who represent high-value customers. Spreading must-dos across optimal days can save hours.

**Independent Test**: Can be tested by specifying a 3-day visit with 20 must-do attractions and verifying the system distributes them based on predicted crowd levels per day.

**Acceptance Scenarios**:

1. **Given** a visitor has 3-day park tickets, **When** they specify must-do attractions, **Then** the system distributes them across days based on forecasted crowd levels
2. **Given** certain attractions have lower crowds on specific days (e.g., weekdays), **When** multi-day plans are generated, **Then** high-tier attractions are scheduled on historically less-crowded days
3. **Given** a visitor completes all must-dos on Day 1, **When** they view Day 2 plan, **Then** the system suggests additional attractions they might enjoy based on tier priorities

---

### User Story 7 - Real-Time Weather Adaptation (Priority: P2)

A visitor wants to adapt their plan immediately when actual weather conditions differ from forecasts, such as an unexpected downpour or extreme heat.

**Why this priority**: Weather forecasts can be wrong, and real-time conditions require immediate tactical adjustments. This bridges the gap between pre-planned optimization and reality.

**Independent Test**: Can be tested by simulating a weather change event (visitor reports "heavy downpour") and verifying the system immediately recalculates the plan to prioritize indoor attractions and minimize outdoor walking.

**Acceptance Scenarios**:

1. **Given** a visitor reports "heavy downpour" during their park visit, **When** they request plan adjustment, **Then** the system immediately reprioritizes indoor attractions and minimizes outdoor walking segments
2. **Given** a visitor reports extreme heat (100°F+), **When** they request plan adjustment, **Then** the system suggests water rides, indoor attractions, and rest breaks with air conditioning
3. **Given** a visitor reports severe weather clearing earlier than forecast, **When** they update conditions, **Then** the system recalculates to take advantage of newly available outdoor attractions
4. **Given** a visitor reports current conditions, **When** the plan is adjusted, **Then** the system maintains must-do attractions but resequences based on current weather suitability

---

### User Story 8 - Virtual Queue Pass ROI Analysis (Priority: P2)

A visitor wants to quantify the cost-benefit of purchasing Lightning Lane/Express Pass to make an informed purchasing decision.

**Why this priority**: Virtual queue passes represent significant expense ($50-$150+ per person). Visitors need data-driven guidance on whether the investment is worthwhile for their specific visit.

**Independent Test**: Can be tested by requesting ROI analysis for a specific party size, date, and pass type, and verifying the system returns cost, time savings, and additional attractions enabled.

**Acceptance Scenarios**:

1. **Given** a visitor selects a park, date, and party size, **When** they request Lightning Lane ROI analysis, **Then** they receive total cost, estimated time savings (hours), and increase in attractions experienced (e.g., "10 rides → 20 rides")
2. **Given** a visitor views ROI analysis, **When** comparing low vs. high attendance days, **Then** they see how pass value changes (e.g., "3 hours saved on low days, 6 hours on high days")
3. **Given** a visitor compares pass types (Multipass vs. Premier Pass), **When** they view ROI, **Then** they see cost-benefit comparison for their specific must-do list
4. **Given** a visitor requests analysis for a specific date (e.g., Dec 25th), **When** the report is generated, **Then** it accounts for predicted crowd levels and wait times for that exact date
5. **Given** a visitor sees ROI analysis, **When** the report displays, **Then** it shows average value, best-case, and worst-case scenarios with confidence levels

---

### Edge Cases

- What happens when park operating hours change last-minute (e.g., extended hours announced)?
- How does the system handle Lightning Lane return time windows that conflict with optimal sequencing?
- What if a visitor arrives late to the park (e.g., 2 hours after opening)?
- How are seasonal ride closures handled (e.g., winter closures for water rides)?
- What happens when weather forecasts change significantly between plan generation and visit date?
- How does the system handle attractions that operate at reduced capacity (e.g., every other row empty)?
- What if a visitor has young children who need nap breaks or have height restrictions?
- How does the system handle when all Lightning Lane reservations are sold out for a high-tier attraction?
- What happens when a visitor reports weather conditions that conflict with forecast data (e.g., "it's sunny" but forecast says rain)?
- How does ROI analysis handle dynamic Lightning Lane pricing that changes throughout the day?
- What if a show or parade is cancelled due to weather - how does the plan adjust?
- How are non-ride attractions with unpredictable wait times (character meets) handled differently from fixed-schedule shows?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST generate a time-sequenced visit plan showing attraction order prioritized by ride tier, with arrival times, predicted wait times, and walking durations
- **FR-002**: System MUST allow visitors to specify must-do attractions that will be included in every generated plan
- **FR-003**: System MUST account for park operating hours when generating plans (attractions must be reachable before park closing)
- **FR-004**: System MUST incorporate predicted wait times based on historical patterns, weather forecasts, day of week, and school holiday schedules
- **FR-005**: System MUST calculate walking time between attractions using physical attraction locations (entrance and exit coordinates)
- **FR-006**: System MUST account for ride duration in time calculations (total time = walk time + wait time + ride duration)
- **FR-007**: System MUST update wait time predictions daily based on newly collected data and pattern analysis
- **FR-008**: System MUST allow visitors to specify time-based constraints (dining reservations, show times, parade viewing)
- **FR-009**: System MUST handle early park entry windows for eligible visitors (resort guests, annual passholders)
- **FR-010**: System MUST support plan regeneration mid-visit when circumstances change (completed attractions, skipped attractions, unexpected closures)
- **FR-011**: System MUST display confidence level for wait time predictions based on data recency and pattern stability
- **FR-012**: System MUST exclude attractions that are closed for refurbishment on the visit date
- **FR-013**: System MUST account for weather impact on wait times (e.g., thunderstorm risk increases indoor attraction waits)
- **FR-014**: System MUST support multi-day visit optimization when visitor has multi-day tickets
- **FR-015**: System MUST persist visitor preferences and must-do lists for future visits
- **FR-016**: System MUST identify data collection gaps that impact prediction accuracy for specific attractions (e.g., "Insufficient data for Tuesdays in January")
- **FR-017**: System MUST support Disney Lightning Lane Multipass (multiple Lightning Lane reservations per day with return time windows)
- **FR-018**: System MUST support Disney Lightning Lane Single Pass (one-time Lightning Lane access to one specific attraction)
- **FR-019**: System MUST support Disney Lightning Lane Premier Pass (one-time Lightning Lane access to any attraction without return time restrictions)
- **FR-020**: System MUST support Universal Express Pass (unlimited Lightning Lane access to all Express-enabled attractions)
- **FR-021**: System MUST prioritize attraction sequencing based on ride tier (Tier 1 flagship rides prioritized over Tier 2/3)
- **FR-022**: System MUST account for Lightning Lane return time windows when sequencing attractions
- **FR-023**: System MUST distinguish between Lightning Lane-enabled and standby-only attractions when optimizing for visitors with virtual queue access
- **FR-024**: System MUST accept real-time weather condition updates from visitors (current temperature, precipitation status, severe weather) and immediately recalculate plans
- **FR-025**: System MUST classify all attractions as indoor, outdoor, or hybrid (covered queue with outdoor ride) for weather-based optimization
- **FR-026**: System MUST calculate weather suitability scores for each attraction based on current conditions (rain makes outdoor attractions less desirable, heat makes water rides more desirable)
- **FR-027**: System MUST maintain current pricing data for all virtual queue pass types at each park (Lightning Lane Multipass/Single/Premier, Universal Express)
- **FR-028**: System MUST calculate ROI analysis for virtual queue passes showing total cost, estimated time savings, and increase in attractions experienced
- **FR-029**: System MUST collect and maintain data for non-ride attractions (shows, parades, character meet-and-greets) separate from ride statistics
- **FR-030**: System MUST include non-ride attractions (shows, parades) in visit plan optimization when visitor specifies them as must-dos
- **FR-031**: System MUST account for fixed show times and parade schedules when sequencing attractions in visit plans

### Key Entities *(include if feature involves data)*

- **Optimized Visit Plan**: A time-sequenced list of attractions for a specific park and date, including arrival times, predicted metrics, walking routes, and virtual queue usage. Represents the core output of the optimization system.

- **Attraction Sequence**: An ordered list of attractions with timing data. Contains arrival time, predicted wait time, ride duration, exit time, walking time to next attraction, tier classification, queue type (standby vs Lightning Lane), and confidence levels for predictions.

- **Visit Constraints**: Time-based or preference-based limitations that must be honored in the plan. Includes dining reservations, show times, early entry windows, mobility limitations, must-do attractions, and Lightning Lane return time windows.

- **Virtual Queue Access**: Visitor's entitlement to skip standby lines. Includes pass type (Lightning Lane Multipass/Single/Premier or Universal Express), applicable attractions, usage limitations, and return time windows.

- **Wait Time Pattern**: Historical and predicted wait time data for an attraction. Includes hourly patterns, day-of-week variations, seasonal trends, weather correlations, special event impacts, and tier-based demand patterns.

- **Pattern Influence Factor**: External factors that correlate with wait time changes. Includes school holidays, weather conditions (temperature, precipitation, UV index), special park events, day-of-week effects, and ride tier popularity.

- **Walking Route**: Physical path and distance between two attractions. Includes estimated walking time calculated from entrance/exit coordinates, accessibility considerations, and proximity to amenities (restrooms, dining).

- **Ride Tier Classification**: Categorization of attractions by demand level. Tier 1 (flagship/high-demand), Tier 2 (moderate-demand), Tier 3 (low-demand/filler attractions). Determines priority in optimization sequencing.

- **Visitor Profile**: Saved preferences and constraints for a specific visitor. Includes must-do attractions, dining preferences, mobility needs, early entry eligibility, and virtual queue pass type.

- **Real-Time Weather Condition**: Current weather reported by visitor or detected by system. Includes temperature, precipitation status (none/light/moderate/heavy), precipitation type (rain/snow), severe weather alerts, and timestamp. Used to override forecast data for immediate plan adjustments.

- **Weather Suitability Score**: Calculated score (0-100) indicating how suitable an attraction is given current weather conditions. Indoor attractions score high during rain, water rides score high during heat, outdoor attractions score low during severe weather.

- **Virtual Queue Pass Pricing**: Current cost data for Lightning Lane/Express Pass products. Includes pass type, park, date, per-person cost, party size discounts, and pricing tier (standard/peak/premium). Updated regularly from park pricing APIs or manual data entry.

- **ROI Analysis Report**: Cost-benefit analysis for virtual queue pass purchase. Includes total cost for party, estimated time savings (minutes/hours), additional attractions enabled, cost per hour saved, comparison across attendance levels (low/average/high), and confidence intervals.

- **Non-Ride Attraction**: Entertainment offerings that aren't rides. Includes shows (live performances with fixed showtimes), parades (scheduled processions with viewing locations), character meet-and-greets (scheduled or walk-up), and other experiences. Have duration but not wait times like rides. Critical for complete day planning but excluded from ride statistics.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Visitors following optimized plans experience 30% fewer total wait time minutes compared to unoptimized visits (measured via post-visit surveys and actual vs predicted comparisons)

- **SC-002**: Optimized plans allow visitors to experience 2-3 additional attractions per day compared to average unoptimized visits

- **SC-003**: Wait time predictions are accurate within ±15 minutes for 80% of predictions, and within ±30 minutes for 95% of predictions

- **SC-004**: System generates visit plans within 10 seconds for single-day visits and within 30 seconds for multi-day visits

- **SC-005**: 90% of visitors successfully complete all must-do attractions when following the optimized plan

- **SC-006**: Visitors rate the optimization system 4.5/5 or higher for usefulness and accuracy

- **SC-007**: System identifies and surfaces data collection gaps for any park-date combination within the prediction accuracy report

- **SC-008**: Mid-visit plan regeneration accounts for current time and completed attractions within 5 seconds

- **SC-009**: Multi-day visit plans distribute must-do attractions to reduce total wait time by at least 20% compared to random distribution

- **SC-010**: Plans accommodate 95% of visitor-specified constraints (dining, shows, accessibility, Lightning Lane return windows) without requiring manual adjustments

- **SC-011**: Visitors with Lightning Lane/Express Pass access save an additional 40-60 minutes of total wait time when following optimized virtual queue usage vs random usage

- **SC-012**: Tier 1 (flagship) attractions are prioritized in 100% of generated plans unless visitor explicitly excludes them

- **SC-013**: Real-time weather adaptation recalculates plans within 3 seconds of visitor reporting current conditions

- **SC-014**: Visitors who use real-time weather adaptation during adverse conditions report higher satisfaction (4.0/5+) than those who don't adapt plans

- **SC-015**: ROI analysis predictions are accurate within ±30 minutes of actual time savings for 75% of visitors who purchase passes based on recommendations

- **SC-016**: 60% of visitors who view ROI analysis report it significantly influenced their virtual queue pass purchase decision

- **SC-017**: Plans that include non-ride attractions (shows, parades) maintain 90%+ on-time arrival for fixed-schedule events

## Scope & Boundaries *(optional)*

### In Scope

- Optimizing visit plans for individual parks (single park per day, no park-hopping)
- Predicting wait times based on historical patterns and known influence factors
- Calculating walking routes between attractions using physical coordinates (entrance and exit points)
- Supporting common visitor constraints (dining, shows, early entry, Lightning Lane return windows)
- Real-time plan adjustments during park visits
- Multi-day visit optimization for visitors with multi-day tickets (same park, different days)
- Supporting Disney Lightning Lane Multipass, Single Pass, and Premier Pass
- Supporting Universal Express Pass
- Prioritizing attractions based on ride tier classification (Tier 1/2/3)
- Real-time weather condition reporting and plan adaptation
- Virtual queue pass ROI analysis and purchase decision support
- Non-ride attractions (shows, parades, character meets) integration in visit plans

### Out of Scope (Future Enhancements)

- Park-hopping optimization (visiting multiple parks in one day) - FUTURE VERSION
- Real-time GPS tracking of visitor location during park visit
- Integration with park-specific mobile apps for automated Lightning Lane booking
- Suggesting dining locations or making dining reservations
- Group coordination features for families or groups splitting up
- Gamification or social features (sharing plans, comparing experiences)
- Hotel or transportation recommendations
- Personalized attraction recommendations based on visitor preferences (beyond tier prioritization)

## Assumptions & Dependencies *(optional)*

### Assumptions

- Park operating hours are known at least 24 hours in advance
- Historical wait time data is available for at least 90 days for meaningful pattern analysis
- Attraction locations (entrance and exit coordinates) are available and stable
- Weather forecasts are available from reliable sources (already collecting via Open-Meteo)
- School holiday calendars for major metro areas are available and accurate
- Visitors will interact with the system via web interface (desktop or mobile browser)
- Ride durations are relatively stable (average ride lasts X minutes regardless of wait time)
- Walking speeds can be estimated at 3 mph for able-bodied adults with adjustments for accessibility needs
- Visitors want to maximize attractions experienced and minimize wait time (primary optimization goal)
- Ride tier classifications (1/2/3) accurately reflect attraction demand and visitor priorities
- Lightning Lane return time windows are known and can be accommodated in sequencing
- Visitors with virtual queue access will use it when recommended (not manually book conflicting times)
- Attraction indoor/outdoor classifications are accurate and maintained (critical for weather-based optimization)
- Virtual queue pass pricing data is available and reasonably current (may lag actual park pricing by 24-48 hours)
- Visitors can accurately report current weather conditions when prompted
- Non-ride attractions (shows, parades) have fixed schedules that are known in advance

### Dependencies

**CRITICAL PREREQUISITES (BLOCKING):**
- **Feature 004 - Comprehensive Attraction Data Collection**: Requires complete collection and storage of ALL themeparks.wiki data for parks and attractions, including:
  - Ride coordinates (lat/long for entrance and exit points)
  - Ride metadata (type, thrill level, height requirements, indoor/outdoor classification)
  - Show schedules with fixed showtimes
  - Parade schedules with routes and viewing locations
  - Character meet-and-greet schedules
  - Lightning Lane/Genie+/Express Pass availability and pricing data
  - Per-ride operating hours (not just park-level)
  - Dining availability data
  - Special event classifications
  - **Status**: Not yet implemented - spec to be created

- **Feature 005 - Wait Time Pattern Analysis & Modeling**: Requires validated predictive models built from correlation analysis of historical data, including:
  - Correlation analysis (wait times vs. weather, day of week, school calendars, special events, ride tier)
  - Pattern discovery (peak hours, seasonal trends, event impacts)
  - Predictive model training and validation
  - Feature importance ranking
  - Confidence interval calculations
  - Model accuracy benchmarking (target: ±15 min for 80% of predictions)
  - **Status**: Not yet implemented - spec to be created
  - **Critical for**: All wait time predictions (FR-004, FR-007, FR-011), ROI analysis (FR-028), optimization algorithm accuracy

**OTHER DEPENDENCIES:**
- **Weather Data Collection**: Requires completed implementation of UV index and weather alerts (feature 002/003)
- **School Calendar Data**: Requires collection of school holiday schedules for top 50 US metro areas
- **Ride Location Data**: Requires collection of attraction coordinates (entrance and exit points) from themeparks.wiki API
- **Ride Capacity Data**: Requires manual research or estimation of attraction throughput (guests per hour)
- **Show Schedule Data**: Requires collection of parade and show times from park schedules
- **Historical Wait Time Data**: Requires at least 90 days of wait time snapshot data for pattern stability
- **Ride Tier Classifications**: Requires tier assignments (1/2/3) for all attractions across all parks
- **Virtual Queue System Data**: Requires understanding of which attractions support Lightning Lane/Express Pass at each park
- **Attraction Indoor/Outdoor Classification**: Requires classification of all attractions as indoor, outdoor, or hybrid for weather-based optimization
- **Non-Ride Attractions Data**: Requires collection of shows, parades, and character meet-and-greet schedules from themeparks.wiki API (separate from ride data, NOT included in ride statistics)
- **Virtual Queue Pass Pricing Data**: Requires current pricing for Lightning Lane/Express Pass products at each park (manual data entry or pricing API integration)
- **Show and Parade Schedules**: Requires collection of fixed-time entertainment schedules from park calendars

### External Factors

- Accuracy depends on quality of historical data (gaps in collection reduce prediction confidence)
- Special park events not captured in schedules (celebrity appearances, limited-time offerings) can cause unpredicted crowd surges
- Park operational changes (ride closures, capacity restrictions) may not be known until day-of-visit
- Visitor behavior changes over time (e.g., viral TikTok trends causing sudden attraction popularity spikes)
- Lightning Lane availability and pricing changes frequently (Disney adjusts based on demand)
- Virtual queue return time windows may shift based on park operations (not always predictable)
