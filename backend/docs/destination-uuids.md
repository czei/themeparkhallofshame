# ThemeParks.wiki Destination UUID Reference

This document provides reference information for ThemeParks.wiki destination UUIDs used in the historical data import system and metadata sync.

## Overview

ThemeParks.wiki organizes entities in a hierarchy:
- **Destinations** (resort complexes) → **Parks** (individual theme parks) → **Attractions** (rides, shows, etc.)

Each entity has a unique UUID that persists across API updates.

## API Endpoint

To fetch all destinations with their UUIDs:
```bash
curl https://api.themeparks.wiki/v1/destinations
```

## Major Destination UUIDs

### Walt Disney World Resort
```
Destination UUID: (fetch from API - UUIDs change)
```
Parks include:
- Magic Kingdom
- EPCOT
- Hollywood Studios
- Animal Kingdom
- Disney Springs (entertainment)
- Typhoon Lagoon (water park)
- Blizzard Beach (water park)

### Disneyland Resort
```
Destination UUID: (fetch from API)
```
Parks include:
- Disneyland Park
- Disney California Adventure

### Universal Orlando Resort
```
Destination UUID: (fetch from API)
```
Parks include:
- Universal Studios Florida
- Islands of Adventure
- Volcano Bay (water park)

### Universal Studios Hollywood
```
Destination UUID: (fetch from API)
```

### SeaWorld Parks
Multiple destinations:
- SeaWorld Orlando
- SeaWorld San Diego
- SeaWorld San Antonio
- Busch Gardens Tampa Bay
- Busch Gardens Williamsburg

### Cedar Fair Parks
Multiple destinations including:
- Cedar Point
- Kings Island
- Knott's Berry Farm
- Carowinds
- Canada's Wonderland

### Six Flags Parks
Multiple destinations including:
- Six Flags Magic Mountain
- Six Flags Great Adventure
- Six Flags Great America
- Six Flags Over Texas
- Six Flags Over Georgia

## Fetching Current UUIDs

Since UUIDs are assigned by ThemeParks.wiki and should be fetched dynamically, use:

```python
from collector.themeparks_wiki_client import get_themeparks_wiki_client

client = get_themeparks_wiki_client()
destinations = client.get_destinations()

for dest in destinations:
    print(f"{dest['name']}: {dest['id']}")
    for park in dest.get('parks', []):
        print(f"  - {park['name']}: {park['id']}")
```

## Usage in Historical Import

When importing historical data, the importer maps external IDs to our internal database:

```python
from importer.id_mapper import IdMapper

mapper = IdMapper(session)

# Map ThemeParks.wiki entity to our ride
ride_id = mapper.get_ride_id_by_wiki_id(wiki_entity_uuid)

# Map park
park_id = mapper.get_park_id_by_wiki_id(wiki_park_uuid)
```

## Entity Relationships

```
destination
├── parks[]
│   ├── id (UUID)
│   ├── name
│   └── timezone
└── metadata
    ├── slug
    └── name
```

## API Response Structure

```json
{
  "destinations": [
    {
      "id": "uuid-here",
      "name": "Walt Disney World Resort",
      "slug": "waltdisneyworldresort",
      "parks": [
        {
          "id": "park-uuid-here",
          "name": "Magic Kingdom Park",
          "entityType": "THEME_PARK",
          "timezone": "America/New_York"
        }
      ]
    }
  ]
}
```

## Metadata Sync

The `sync_metadata` cron job syncs entity metadata from ThemeParks.wiki:

```bash
# Manual sync for a specific park
python -m scripts.sync_metadata --park-uuid <uuid>

# Check coverage statistics
python -m scripts.sync_metadata --coverage

# Dry run (no changes)
python -m scripts.sync_metadata --dry-run
```

## Related Files

- `src/collector/themeparks_wiki_client.py` - API client
- `src/collector/metadata_collector.py` - Metadata sync logic
- `src/importer/id_mapper.py` - UUID to internal ID mapping
- `src/scripts/sync_metadata.py` - CLI for metadata sync
- `src/scripts/import_historical.py` - Historical data importer
