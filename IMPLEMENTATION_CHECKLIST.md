# Implementation Checklist

## Setup Phase
- [ ] Set up Python virtual environment
- [ ] Install dependencies from requirements.txt
- [ ] Create ride_classifier package directory structure
- [ ] Set up logging configuration

## Core Components to Build

### config.py
- [ ] Define API_BASE_URL = "https://queue-times.com"
- [ ] Set FILTER_CONTINENTS = ["North America"]
- [ ] Configure CONFIDENCE_THRESHOLD = 0.7
- [ ] Set RATE_LIMIT_DELAY = 0.5

### api_client.py
- [ ] Implement QueueTimesClient class
- [ ] Create fetch_all_parks() method
- [ ] Create fetch_park_rides(park_id) method
- [ ] Add retry logic with exponential backoff
- [ ] Implement rate limiting

### known_rides.py
- [ ] Create TIER_1_EXACT_MATCHES dictionary
- [ ] Add Disney E-tickets
- [ ] Add Universal major attractions
- [ ] Add Cedar Fair signature coasters
- [ ] Add Six Flags major coasters

### classifier.py
- [ ] Implement RideClassifier class
- [ ] Create classify_ride() method
- [ ] Implement matches_tier_1_patterns()
- [ ] Implement matches_tier_3_patterns()
- [ ] Create export_csv() method

### main.py
- [ ] Parse command-line arguments
- [ ] Implement main() function
- [ ] Add progress tracking
- [ ] Generate summary statistics

## Classification Rules

### Tier 1 Patterns
- [ ] Space Mountain, Thunder Mountain, Tower of Terror
- [ ] Giga, Hyper, Strata keywords
- [ ] Rise of, Flight of, Forbidden Journey

### Tier 3 Patterns
- [ ] Kiddie, Junior, Mini keywords
- [ ] Carousel, Teacups
- [ ] Show, Theater keywords

## Testing & Validation

- [ ] Test on 5-10 parks first
- [ ] Verify Tier 1 classifications manually
- [ ] Check tier distribution (~10/40/50)
- [ ] Validate confidence scores

## First Run

- [ ] Run: `python -m ride_classifier.main --fetch-all`
- [ ] Review data/ride_classifications.csv
- [ ] Check data/classification_summary.json

## Manual Review

- [ ] Export all Tier 1 classifications
- [ ] Research ambiguous ride names
- [ ] Add corrections to data/manual_overrides.csv
- [ ] Re-run with overrides

## Backend Integration

- [ ] Create rides table with tier column
- [ ] Import classifications
- [ ] Test weighted downtime formula
- [ ] Verify park rankings

## Success Criteria

- [ ] 95%+ rides classified with confidence > 0.7
- [ ] Tier distribution roughly 10/40/50
- [ ] All Tier 1 classifications verified
- [ ] Processing time < 30 minutes

---

**Track your progress as you implement the specification!**
