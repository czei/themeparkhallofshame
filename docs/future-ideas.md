# AI Integration Ideas for Theme Park Hall of Shame

**Created**: 2025-11-24
**Status**: Brainstorm / Research
**Context**: Beyond the existing ride tier classification system, these are additional opportunities to leverage AI in the project.

---

## Current AI Usage

The project currently uses AI for **ride tier classification** (FR-022 through FR-032):
- Hierarchical 4-tier classification: manual overrides → cached AI → pattern matching → AI agent with web search
- AI researches ambiguous rides that can't be classified via rules
- Confidence scores (0.0-1.0) generated for each classification
- Low-confidence (<0.5) rides flagged for manual review

---

## Proposed AI Integrations

### Tier 1: High Value, Low Effort

#### 1. Natural Language Park/Ride Summaries

**What it is**: Use an LLM to generate human-readable paragraphs summarizing performance data.

**Example output**:
> "Over the past 30 days, Cedar Point's overall ride uptime was 92%, a 3% improvement from the previous month. Millennium Force remained a standout with 98% uptime, while Top Thrill 2 experienced significant downtime, accounting for 40% of the park's total."

**Value**: Makes complex data instantly understandable and shareable. Perfect for homepage dashboards or social media.

**Implementation**:
- Create structured prompts that inject key statistics from the database
- Use LLM API (OpenAI, Anthropic, Google) to generate narrative
- Prompt engineering task, not model training
- Estimated effort: 1-2 days

**Data required**: Existing aggregated statistics (already available)

---

#### 2. Automated Social/Content Generation

**What it is**: Daily/weekly cron job that generates shareable content from interesting data patterns.

**Content types**:
- "Hall of Shame / Hall of Fame" daily spotlights
- Weekly reliability recaps
- Fun trivia / "Did you know?" facts
- Threshold crossing alerts ("X ride finally exceeded 95% uptime")

**Implementation approach**:
1. Compute interesting deltas algorithmically:
   - Biggest week-over-week uptime improvement/decline
   - Threshold crossings (95% uptime achieved, dropped below 80%, etc.)
   - Extremes (longest downtime streak, most reliable tier-1 ride)
2. Rank by "story potential" (high-tier rides, popular parks, large absolute numbers)
3. Feed top items into LLM with templates for different formats:
   - Twitter/X-length one-liner
   - Blog-style recap
   - Email newsletter segment

**Trivia mining examples**:
- "Ride with the highest total downtime in the last 12 months"
- "Park with the most reliable tier-1 lineup"
- "Which ride is most likely to be down on July 4th?"
- Longest continuous uptime streak vs longest downtime streak

**Value**: Growth and engagement with minimal ongoing effort

**Data required**: Existing aggregated statistics

---

#### 3. Cross-Park Benchmarking Intelligence

**What it is**: AI-powered comparison of similar rides across different parks.

**Features**:
- **Ride twins**: Automatically group obvious twins (Space Mountain MK ↔ Space Mountain DL, Batman clones, Boomerang coasters)
- **Ride families**: Find less obvious analogs via similarity embeddings
- **Per-ride benchmarking**: "Space Mountain (MK) has 96.2% uptime; median of its 5 closest analogs is 94.1%"
- **Operator-level intelligence**: "Among tier-1 coasters, Disney's median uptime is X% vs Six Flags' Y%"

**Implementation**:
1. Build similarity embeddings for rides:
   - Create description strings: `"{ride_name} at {park_name}, tier {tier}, operator {operator}"`
   - Optionally enrich with LLM-generated ride type tags ("indoor steel coaster", "log flume")
   - Generate text embeddings and compute nearest neighbors
2. Benchmark metrics:
   - "Better/worse than peers" badges
   - Percentile rankings within peer group
   - Operator-level aggregations

**UX applications**:
- Comparison pages: "Compare Space Mountain vs Space Mountain"
- "Similar but more reliable" suggestions on ride pages
- Operator dashboards

**Value**: Very on-brand for "Hall of Shame" concept; differentiating feature

**Data required**: Existing ride/park metadata + embeddings

---

### Tier 2: High Value, Medium Effort

#### 4. Predictive Downtime Duration

**What it is**: When a ride goes down, predict how long it will be closed (short/medium/long or time range).

**Value**: This is the #1 question a park guest has when their target ride closes. Answering "Should I wait or move on?" is massive value for in-park planning.

**Technical approach**:
- Frame as classification or regression problem
- Features:
  - `ride_id`, `park_id`
  - Time of day, day of week, season
  - Recent downtime frequency for that ride
  - Current wait times of adjacent rides (proxy for park busyness)
- Model: Gradient boosting (LightGBM, XGBoost) - efficient for tabular data
- Train on historical downtime events, using duration as label

**Data required**: Historical timestamped status changes (already collected)

**Estimated effort**: 1-2 weeks for initial model

---

#### 5. Anomaly Detection

**What it is**: Automated system that flags unusual downtime patterns.

**Detection targets**:
- A typically reliable ride suddenly down for 5+ hours
- Entire park's data feed stops updating
- Ride status flapping between open/closed every 10 minutes
- Park-wide simultaneous spikes (many rides down together)

**Value**:
- Improves data integrity by catching data source errors
- Surface "breaking news" content on the site
- Internal alerting for data quality issues

**Technical approach**:
- Unsupervised models: Isolation Forest or One-Class SVM
- Learn "normal" behavior per ride, flag deviations
- Can start with simpler statistical methods (>3 standard deviations from mean)

**Data required**: Raw timestamped ride status data (already collected)

---

#### 6. Seasonal/Calendar Intelligence

**What it is**: Beyond basic forecasting, extract insights about holidays, events, and maintenance patterns.

**Capabilities**:

**A. Event/holiday-aware pattern mining**:
- Compare uptime on holidays vs non-holidays
- Compare uptime during event weeks (Halloween, Christmas) vs baseline
- Per-ride coefficients: "This ride's uptime is ~3% lower on national holidays"

**B. Maintenance/refurbishment pattern hints**:
- Detect repeated long closures at similar times each year
- Label as "probable seasonal maintenance windows"
- User-facing: "This ride has historically had a multi-day closure in late January for the past 3 years"

**C. "Best time of year" reliability insights**:
- Compute uptime by month and day-of-week
- LLM summary: "For this park, reliability tends to be highest on weekdays in May and early September"

**Data to add**:
- Public holiday calendar (US/CA/MX)
- School holiday approximations (spring break, summer break)
- Park event schedules (manually curated for major parks)

**Technical approach**:
- Simple regression: uptime ~ day_of_week + month + holiday_flag + event_flag
- Extract feature importances for interpretable insights

---

### Tier 3: Advanced / Roadmap

#### 7. Park Reliability Forecast

**What it is**: Generate reliability forecast for upcoming days/weeks.

**Example**: "Magic Kingdom is forecasted to have 15% higher-than-average downtime this Saturday"

**Value**: Helps users plan which park to visit on which day of their vacation.

**Technical approach**:
- Time-series forecasting (Facebook Prophet, ARIMA)
- Features: historical downtime, day of week, seasonality
- Enhanced with: weather forecasts, school holidays, event schedules

**Data required**: Historical data (have) + external calendar/weather data (to add)

**Effort**: High - requires weather API integration and time-series ML

---

#### 8. RAG Chatbot for Trip Planning

**What it is**: Natural language interface for querying reliability data.

**Example queries**:
- "Which Disney World park has the most reliable tier-1 rides in September?"
- "Show me family rides at Universal Hollywood that are open more than 95% of the time"

**Implementation**:
- Retrieval-Augmented Generation (RAG) architecture
- Vector database (Pinecone, Weaviate, PGvector)
- User query → embedding → vector search → LLM generates answer

**Value**: Powerful, intuitive way to query data without complex filters

**Effort**: High - requires vector DB setup and RAG pipeline

---

#### 9. Downtime Pattern Clustering

**What it is**: Unsupervised grouping of downtime events by their characteristics.

**Cluster types (expected)**:
- "Transient / ops resets" - short, frequent, off-peak blips
- "Start-up/shut-down" - long outages at opening/closing hours
- "Peak-load sensitive" - mid-duration, afternoon, high utilization
- "Park/system-level events" - many rides down simultaneously

**User-facing application**:
- "This ride's downtime events are most similar to our 'short reset' cluster"
- Helps explain patterns without claiming to know exact causes

**Technical approach**:
- Build event-level feature table (duration, time-of-day, park context, etc.)
- Clustering models: k-means, GMM, or HDBSCAN

---

#### 10. Reliability Signature Embeddings

**What it is**: Vector representation of each ride's temporal reliability patterns.

**Construction**:
- Uptime by hour-of-day (24-d vector)
- Uptime by day-of-week (7-d vector)
- Uptime by month (12-d vector)
- Reduce via PCA or autoencoder

**Applications**:
- Cluster rides by temporal reliability patterns
- Find "rides with similar reliability profiles"
- Internal QA: flag rides whose signature diverges from peers

---

## Additional Data Sources to Consider

| Data Source | Purpose | Effort |
|-------------|---------|--------|
| Weather API (OpenWeather) | Correlation analysis, forecasts | Low - free tier available |
| Holiday calendar API | Seasonal intelligence | Low |
| Ride type tags (LLM-generated) | "Outdoor vs indoor", "water ride" patterns | Medium - similar to tier classification |
| Park event schedules | Halloween/Christmas overlay analysis | Medium - manual curation |

---

## Recommended Implementation Order

### Phase 1: Quick Wins (1-2 weeks)
1. **Natural Language Summaries** - Fast, impressive UX improvement
2. **Content Automation** - Feeds social growth with minimal effort

### Phase 2: Differentiating Features (2-4 weeks)
3. **Cross-Park Benchmarking** - Very "Hall of Shame", unique value
4. **Anomaly Detection** - Protects data quality, enables "breaking news"

### Phase 3: Advanced Features (1-2 months)
5. **Seasonal Calendar Intelligence** - Requires calendar integration
6. **Predictive Downtime Duration** - The killer feature for in-park users

### Phase 4: Roadmap (Future)
7. Park Reliability Forecasts
8. RAG Chatbot
9. Advanced clustering and signatures

---

## Notes

- All AI-generated content should be grounded in precomputed statistics, not invented by the model
- Messaging around inferred patterns must be careful ("suggests an association" vs "proves cause")
- The project philosophy emphasizes data transparency and respect for maintenance professionals
- Attribution to Queue-Times.com must remain visible per existing requirements

---

## References

Analysis conducted with:
- Gemini 2.5 Pro
- GPT-5.1

Based on project specifications in `specs/001-theme-park-tracker/`
