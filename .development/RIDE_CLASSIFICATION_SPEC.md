# Ride Classification Script Specification

## Project: Theme Park Hall of Shame
**Version:** 1.0  
**Date:** November 22, 2025  
**Author:** System Specification

---

## 1. PURPOSE

Create a Python script that automatically classifies all rides from queue-times.com API into three tiers based on their expected operational complexity, guest impact, and maintenance requirements. This classification will be used to create a weighted downtime scoring system that fairly compares parks with different ride portfolios.

### Weighted Downtime Scoring Formula

Once rides are classified, each park's downtime performance is calculated using:

```
Park Downtime Score = Σ(downtime_hours × tier_weight) / Σ(all_ride_weights)
```

Where:
- **downtime_hours** = Hours ride was non-operational during the measurement period
- **tier_weight** = 3 for Tier 1, 2 for Tier 2, 1 for Tier 3
- **Σ(all_ride_weights)** = Sum of weights for all rides in the park

**Example Calculation:**

Magic Kingdom has:
- 11 Tier 1 rides (weight 3 each) = 33 total weight
- 18 Tier 2 rides (weight 2 each) = 36 total weight  
- 12 Tier 3 rides (weight 1 each) = 12 total weight
- **Total park weight = 81**

If during a 7-day period:
- Space Mountain (Tier 1): 8 hours down = 8 × 3 = 24 weighted hours
- Big Thunder Mountain (Tier 1): 6 hours down = 6 × 3 = 18 weighted hours
- Haunted Mansion (Tier 2): 4 hours down = 4 × 2 = 8 weighted hours
- Carousel (Tier 3): 12 hours down = 12 × 1 = 12 weighted hours

**Total weighted downtime = 62 hours**

```
Park Downtime Score = 62 / 81 = 0.765 or 76.5%
```

This score represents the percentage of the park's weighted operational capacity that was offline. Parks are ranked with **higher scores indicating worse performance**.

### Alternative: Normalized Downtime Hours

For easier interpretation, you can also express as weighted downtime hours per day:

```
Normalized Downtime = Σ(downtime_hours × tier_weight) / number_of_days
```

Using the same example:
```
Normalized Downtime = 62 / 7 = 8.86 weighted downtime hours per day
```

This allows comparison: "Park A had 8.86 weighted downtime hours per day vs Park B's 5.2"

---

## 2. OBJECTIVES

1. **Fetch** comprehensive ride data from queue-times.com API for all North American parks
2. **Classify** each ride into Tier 1 (3x weight), Tier 2 (2x weight), or Tier 3 (1x weight)
3. **Generate** a reviewable CSV output with classification rationale
4. **Enable** manual review and override of automated classifications
5. **Support** incremental updates as new rides are added to parks

---

## 3. DATA SOURCES

### Primary API Endpoint
- **Base URL:** `https://queue-times.com`
- **Parks List:** `/parks.json`
- **Park Rides:** `/parks/{park_id}/queue_times.json`

### API Response Structure

#### Parks Response
```json
[
  {
    "id": 11,
    "name": "Cedar Fair Entertainment Company",
    "parks": [
      {
        "id": 57,
        "name": "California's Great America",
        "country": "United States",
        "continent": "North America",
        "latitude": "37.397799",
        "longitude": "-121.974717",
        "timezone": "America/Los_Angeles"
      }
    ]
  }
]
```

#### Rides Response (per park)
```json
{
  "lands": [
    {
      "id": 70,
      "name": "Land Name",
      "rides": [
        {
          "id": 118,
          "name": "Ride Name",
          "is_open": true,
          "wait_time": 10,
          "last_updated": "2025-05-15T13:55:50.000Z"
        }
      ]
    }
  ]
}
```

---

## 4. CLASSIFICATION METHODOLOGY

### Hierarchical 4-Tier Classification System

The classification system uses a **priority-based decision hierarchy** to balance accuracy, cost, and performance:

```
┌─────────────────────────────────────────────────────────┐
│  Classification Flow (5,247 total rides)               │
└─────────────────────────────────────────────────────────┘

Priority 1: manual_overrides.csv (~100 rides, 2%)
   └─ Human corrections - highest authority
   └─ Format: ride_id,park_id,tier,reason

Priority 2: exact_matches.json (~500 rides, 10%)
   └─ Cached AI decisions (confidence > 0.85)
   └─ Cache key: {park_id}:{ride_id} with schema_version

Priority 3: pattern_matcher.py (~4,000 rides, 76%)
   └─ Keyword rules for obvious cases (fast, free)
   └─ High-confidence patterns only

Priority 4: ai_classifier.py (~647 rides, 12%)
   └─ AI agent with web search (Gemini-2.5-pro)
   └─ Researches ambiguous rides
   └─ Returns tier + confidence + reasoning + sources
```

**Cost Optimization:**
- **Initial run**: ~$19 (647 ambiguous rides × $0.03/call)
- **Incremental updates**: ~$0.30/month (~10 new rides/month)
- **76% of rides** classified via free pattern matching
- **10% reuse** cached AI decisions (no additional cost)

### 4.1 Tier 1: Major Attractions (3x weight)

**Characteristics:**
- Signature/E-ticket attractions
- Major roller coasters (>100 ft tall or launched)
- Complex dark rides with advanced animatronics
- High-capacity (>1500 pph) flagship attractions

**Examples:**
- Rise of the Resistance
- Hagrid's Magical Creatures Motorbike Adventure
- Millennium Force
- Space Mountain
- Tower of Terror
- VelociCoaster
- Steel Vengeance
- Flight of Passage

**Pattern Matching Rules** (used by Priority 3):
- Known E-ticket attractions (maintain explicit list)
- Ride names containing: "Space Mountain", "Tower of Terror", "Big Thunder Mountain"
- Coaster keywords: "Giga", "Hyper", "Strata"
- Advanced dark ride keywords: "Rise of", "Flight of", "Forbidden Journey"

### 4.2 Tier 2: Standard Attractions (2x weight)

**Characteristics:**
- Standard roller coasters
- Traditional dark rides
- Major flat rides (drop towers, swing rides)
- Water rides (log flumes, river rapids)

**Examples:**
- Most named roller coasters
- Pirates of the Caribbean
- Haunted Mansion
- Drop towers
- Log flumes

**Pattern Matching Rules:**
- Contains "Roller Coaster", "Coaster" (without "kiddie" or "junior")
- Water ride keywords: "Log", "Flume", "Rapids", "River"
- Drop tower keywords: "Drop", "Tower", "Freefall"
- Classic attraction names: "Haunted", "Pirates"

### 4.3 Tier 3: Minor Attractions (1x weight)

**Characteristics:**
- Kiddie rides
- Carousels and classic midway rides
- Shows and walk-through attractions
- Playgrounds

**Examples:**
- Carousels
- Teacups
- Kiddie coasters
- Dumbo-style spinners
- Playground areas

**Pattern Matching Rules:**
- Contains "Kiddie", "Kid's", "Junior", "Mini"
- Classic flat rides: "Carousel", "Merry-Go-Round", "Teacups"
- Show keywords: "Theater", "Theatre", "Show", "4D", "3D"
- Playground: "Playground", "Play Area", "Splash Pad"

### 4.4 AI-Assisted Classification (Priority 4)

For rides not matched by Priorities 1-3, the system invokes an AI agent with web search capability to research the ride and make an informed classification decision.

**AI Agent Prompt:**
```
You are a theme park ride classification expert. Classify the following ride into Tier 1, 2, or 3.

**Ride Information:**
- Name: {ride_name}
- Park: {park_name}
- Location: {location}

**Tier Definitions:**
- Tier 1 (3x): Major E-tickets, signature coasters >100ft, advanced dark rides
- Tier 2 (2x): Standard coasters, dark rides, water rides, drop towers
- Tier 3 (1x): Kiddie rides, carousels, shows, minor attractions

**Research using web search:**
1. Ride type and specifications (height, speed, technology)
2. Park significance (flagship attraction?)
3. Guest perception (popularity, wait times)
4. Industry recognition (awards, enthusiast discussion)

**Response Format (JSON):**
{
  "tier": 1|2|3,
  "confidence": 0.0-1.0,
  "reasoning": "Brief explanation citing sources",
  "sources": ["url1", "url2"]
}
```

**AI Integration:**
- **Tool**: `mcp__zen__chat` (Gemini-2.5-pro with web search)
- **Parallel processing**: Uses `concurrent.futures.ThreadPoolExecutor` to process ~650 rides in ~5 minutes (vs 30+ minutes sequential)
- **Error handling**: Retry logic with exponential backoff for transient failures
- **Prompt storage**: Version-controlled in `ai_prompt.md` (not hardcoded)

**Confidence Thresholding:**
- **< 0.5**: Flag for mandatory human review → add to manual_overrides.csv
- **0.5-0.85**: Accept classification but don't cache (may need future review)
- **> 0.85**: Cache in exact_matches.json for reuse

---

## 5. CACHING STRATEGY & COST OPTIMIZATION

### 5.1 Cache Architecture

**exact_matches.json Structure:**
```json
{
  "schema_version": "1.0",
  "last_updated": "2025-11-23T10:30:00Z",
  "classifications": {
    "57:118": {
      "park_id": 57,
      "ride_id": 118,
      "park_name": "California's Great America",
      "ride_name": "Gold Striker",
      "tier": 1,
      "confidence": 0.92,
      "reasoning": "Major wooden coaster, 108.3 ft tall, flagship attraction at CGA",
      "sources": ["https://rcdb.com/11130.htm"],
      "classified_at": "2025-11-22T14:25:10Z",
      "classification_method": "ai_agent"
    }
  }
}
```

**Cache Key Format:**
- **Format**: `"{park_id}:{ride_id}"`
- **Why**: Stable across ride name changes (e.g., "Duelling Dragons" → "Dragon Challenge")
- **Invalidation**: Increment `schema_version` when tier criteria change

### 5.2 Cache Manager Implementation

**Key Responsibilities:**
1. **Load cache** on script startup
2. **Validate schema_version** (reject outdated cache entries)
3. **Lookup by cache key** (`{park_id}:{ride_id}`)
4. **Save high-confidence AI results** (confidence > 0.85)
5. **Atomic writes** to prevent corruption during concurrent runs

**Production Hardening** (from expert analysis):
```python
class CacheManager:
    def get_classification(self, ride):
        """Returns cached classification or None"""
        cache_key = f"{ride.park_id}:{ride.id}"
        entry = self.cache["classifications"].get(cache_key)

        # Validate cache version
        if entry and entry.get("schema_version") != CURRENT_SCHEMA_VERSION:
            return None  # Invalidate outdated entry

        return entry

    def save_classification(self, ride, classification):
        """Save AI classification if confidence > 0.85"""
        if classification.confidence <= 0.85:
            return  # Don't cache low-confidence results

        cache_key = f"{ride.park_id}:{ride.id}"
        self.cache["classifications"][cache_key] = {
            "park_id": ride.park_id,
            "ride_id": ride.id,
            "tier": classification.tier,
            "confidence": classification.confidence,
            # ... other fields
        }
        self._atomic_write()  # Prevent corruption
```

### 5.3 Pattern Matcher Validation

Before relying on pattern matching for 76% of rides, validate accuracy:

**Dry Run Procedure:**
1. Run classification script on all rides
2. Extract pattern-matched results (exclude manual overrides, AI, cache)
3. Random sample 100-200 classifications
4. Manually verify tier assignments
5. Calculate accuracy rate

**Acceptable Thresholds:**
- **> 95% accuracy**: Production-ready, use as-is
- **90-95% accuracy**: Review false positives, tighten pattern rules
- **< 90% accuracy**: Reduce pattern matcher scope, rely more on AI

---

## 6. IMPLEMENTATION

### 6.1 Script Structure

```
ride_classifier/
├── __init__.py
├── config.py                # API URLs, thresholds, schema version
├── api_client.py            # Queue-Times.com API fetcher
├── pattern_matcher.py       # Keyword rules (Priority 3)
├── ai_classifier.py         # MCP zen chat integration (Priority 4)
├── cache_manager.py         # exact_matches.json handler (Priority 2)
├── manual_overrides.py      # CSV reader (Priority 1)
├── main.py                  # Orchestration
└── models.py                # Data structures

data/
├── manual_overrides.csv      # Human corrections
├── exact_matches.json        # Cached AI decisions
├── ai_prompt.md              # Version-controlled AI prompt
└── ride_classifications.csv  # Final output
```

### 6.2 Core Classification Logic

```python
def classify_ride(
    ride: Ride,
    manual_override_handler,
    cache_manager,
    pattern_matcher,
    ai_classifier,
) -> RideClassification:
    """
    Hierarchical 4-tier classification:
    1. Manual Overrides (human authority)
    2. Cached AI Decisions (confidence > 0.85)
    3. Pattern Matching (keyword rules)
    4. AI Agent with Web Search (ambiguous cases)
    """
    # Priority 1: Manual Overrides
    classification = manual_override_handler.get_classification(ride)
    if classification:
        return classification

    # Priority 2: Cached AI Decisions
    classification = cache_manager.get_classification(ride)
    if classification:
        return classification

    # Priority 3: Pattern Matching
    tier, reason, confidence = pattern_matcher.classify(ride.name)
    if confidence > 0.85:  # High-confidence pattern match
        return RideClassification(
            park_id=ride.park_id,
            ride_id=ride.id,
            tier=tier,
            reason=f"Pattern: {reason}",
            confidence=confidence,
        )

    # Priority 4: AI Agent with Web Search
    classification = ai_classifier.classify(ride)
    if classification.confidence > 0.85:
        cache_manager.save_classification(ride, classification)

    return classification
```

### 6.3 Parallel Processing for AI Calls

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def classify_ambiguous_rides(rides, ai_classifier):
    """Process ~650 ambiguous rides in parallel (~5 min vs 30+ min)"""
    results = {}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(ai_classifier.classify, ride): ride
            for ride in rides
        }

        for future in as_completed(futures):
            ride = futures[future]
            try:
                result = future.result(timeout=60)  # 60s per call
                results[ride.id] = result
            except Exception as e:
                logger.error(f"Failed to classify {ride.name}: {e}")
                results[ride.id] = fallback_classification(ride)

    return results
```

### 6.4 Validation & Quality Control

**Human Review Workflow:**
1. Script generates `data/ride_classifications.csv`
2. Filter for `confidence < 0.5` → mandatory review
3. Filter for `tier = 1` → verify all Tier 1 classifications
4. Random sample 5% of Tier 2/3 → spot check
5. Add corrections to `data/manual_overrides.csv`
6. Re-run script to apply overrides

**Success Criteria:**
- 95%+ rides classified with confidence > 0.7
- All Tier 1 classifications human-verified
- Tier distribution roughly 10%/40%/50% (Tier 1/2/3)
- Processing time < 10 minutes (including AI calls)

---

## QUICK START

```bash
# Setup
cd /Users/czei/Projects/ThemeParkHallOfShame
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run classification
python -m ride_classifier.main --fetch-all

# Review output
open data/ride_classifications.csv
```

**END OF SPECIFICATION**
