# ThemeParks.wiki Field Gaps (Disney Parks/Rides)

This document summarizes which ThemeParks.wiki fields we currently store vs. drop,
based on the live client in `backend/src/collector/themeparks_wiki_client.py` and
our schema in `backend/src/database/schema/*.py`.

Scope: Disney destinations, parks, and ride live data.

---

## Entity Document (`GET /entity/{entityID}`)

Fields returned by the entity document for parks and rides.

| ThemeParks.wiki field | Stored? | Where |
| --- | --- | --- |
| `id` | Yes | `parks.themeparks_wiki_id` or `rides.themeparks_wiki_id` |
| `name` | Yes | `parks.name` / `rides.name` |
| `entityType` | Partial | `rides.entity_type` (parks do not store entity type) |
| `timezone` | Yes | `parks.timezone` |
| `location.latitude` | Yes (parks only) | `parks.latitude` |
| `location.longitude` | Yes (parks only) | `parks.longitude` |
| `parentId` | No | Not stored |
| `destinationId` | No | Not stored |
| `tags[]` | No | Not stored |

Notes:
- We do not currently fetch the entity document in the collector, so any fields
  listed here are not being refreshed from ThemeParks.wiki today.

---

## Entity Children (`GET /entity/{entityID}/children`)

Fields returned for park children (attractions, shows, restaurants).

| ThemeParks.wiki field | Stored? | Where |
| --- | --- | --- |
| `children[].id` | Yes | `rides.themeparks_wiki_id` |
| `children[].name` | Yes | `rides.name` |
| `children[].entityType` | Partial | `rides.entity_type` (we ignore non-attraction entities) |
| `children[].externalId` | No | Not stored |
| `children[].parentId` | No | Not stored |
| `children[].location.latitude` | No | Not stored |
| `children[].location.longitude` | No | Not stored |

Notes:
- Only ATTRACTION entities are currently ingested for live status; SHOW and
  RESTAURANT entities are ignored by the collector.

---

## Live Data (`GET /entity/{entityID}/live`)

Fields returned per child entity in `liveData[]`.

| ThemeParks.wiki field | Stored? | Where |
| --- | --- | --- |
| `liveData[].id` | Yes | `rides.themeparks_wiki_id` |
| `liveData[].name` | Yes | `rides.name` |
| `liveData[].entityType` | Partial | `rides.entity_type` (only ATTRACTION tracked) |
| `liveData[].status` | Yes | `ride_status_snapshots.status` |
| `liveData[].lastUpdated` | Yes | `ride_status_snapshots.last_updated_api` |
| `liveData[].queue.STANDBY.waitTime` | Yes | `ride_status_snapshots.wait_time` |
| `liveData[].queue.SINGLE_RIDER.waitTime` | No | Not stored |
| `liveData[].queue.RETURN_TIME.*` | No | Not stored |
| `liveData[].queue.PAID_RETURN_TIME.*` | No | Not stored |
| `liveData[].queue.BOARDING_GROUP.*` | No | Not stored |
| `liveData[].queue.PAID_STANDBY.waitTime` | No | Not stored |
| `liveData[].showtimes[]` | No | Not stored |
| `liveData[].operatingHours[]` | No | Not stored |
| `liveData[].diningAvailability[]` | No | Not stored |

---

## Park Schedule (`GET /entity/{entityID}/schedule`)

Fields returned in `schedule[]` entries.

| ThemeParks.wiki field | Stored? | Where |
| --- | --- | --- |
| `schedule[].date` | Yes | `park_schedules.schedule_date` |
| `schedule[].openingTime` | Yes | `park_schedules.opening_time` |
| `schedule[].closingTime` | Yes | `park_schedules.closing_time` |
| `schedule[].type` | Yes | `park_schedules.schedule_type` |
| `schedule[].purchases[]` | No | Not stored |
| `schedule[].purchases[].price.*` | No | Not stored |
| `schedule[].purchases[].available` | No | Not stored |

---

## Additional Observations

- We keep Disney park metadata (name, location, timezone) but do not persist
  destination grouping or entity tags from ThemeParks.wiki.
- We only store standby wait times; all other queue types (return time,
  paid options, boarding groups) are discarded today.
- Live showtimes and dining availability are returned by the API but never
  persisted.

