# Theme Park Hall of Shame - Ride Classification System

## Quick Start

This project automatically classifies theme park rides into three tiers for weighted downtime analysis.

### Project Structure

```
ThemeParkHallOfShame/
├── RIDE_CLASSIFICATION_SPEC.md    # Complete technical specification
├── README.md                       # This file
├── ride_classifier/                # Python package (to be created)
│   ├── __init__.py
│   ├── config.py
│   ├── api_client.py
│   ├── classifier.py
│   ├── known_rides.py
│   └── main.py
├── data/                          # Data directory (to be created)
│   ├── ride_classifications.csv
│   ├── manual_overrides.csv
│   └── classification_summary.json
├── logs/                          # Log files
│   └── classification.log
├── requirements.txt
└── tests/                         # Unit tests
    └── test_classifier.py
```

## Tier System

- **Tier 1 (3x)**: Major attractions - E-tickets, signature coasters, complex dark rides
- **Tier 2 (2x)**: Standard attractions - Regular coasters, dark rides, water rides
- **Tier 3 (1x)**: Minor attractions - Kiddie rides, carousels, shows, flat rides

## Next Steps

1. **Review the specification**: Read `RIDE_CLASSIFICATION_SPEC.md` for complete details
2. **Set up environment**:
   ```bash
   cd /Users/czei/Projects/ThemeParkHallOfShame
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
3. **Run the classifier**:
   ```bash
   python -m ride_classifier.main --fetch-all
   ```
4. **Review output**: Check `data/ride_classifications.csv` for results

## Key Features

- ✅ Fetches all rides from queue-times.com API
- ✅ Applies rule-based classification
- ✅ Generates confidence scores
- ✅ Supports manual overrides
- ✅ Produces reviewable CSV output
- ✅ Handles API errors gracefully

## Classification Examples

| Ride | Park | Tier | Reasoning |
|------|------|------|-----------|
| Millennium Force | Cedar Point | 1 | Major coaster, known attraction |
| Haunted Mansion | Magic Kingdom | 2 | Classic dark ride |
| Carousel | Any Park | 3 | Classic flat ride |

## API Attribution

This project uses the free Queue-Times.com API. Per their terms:
> Display "Powered by Queue-Times.com" with link to https://queue-times.com

## Development Status

- [x] Specification complete
- [ ] Python implementation
- [ ] Initial classification run
- [ ] Manual review process
- [ ] Integration with Hall of Shame webapp

## Contact

For questions about this classification system, refer to the specification document.
