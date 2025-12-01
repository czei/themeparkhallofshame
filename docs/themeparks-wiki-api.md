# ThemeParks.wiki API Reference

**Base URL:** `https://api.themeparks.wiki/v1`

**Official Docs:** https://api.themeparks.wiki/docs/v1/#/

---

## Endpoints

### 1. Get Destinations
```
GET /destinations
```
Returns list of all supported destinations (resort complexes).

**Response:**
```json
{
  "destinations": [
    {
      "id": "string",
      "name": "string",
      "slug": "string",
      "parks": [
        { "id": "string", "name": "string" }
      ]
    }
  ]
}
```

---

### 2. Get Entity Document
```
GET /entity/{entityID}
```
Get full data for any entity (park, ride, restaurant, etc).

**Parameters:**
- `entityID` (required) - Entity ID or slug

**Response:**
```json
{
  "id": "string",
  "name": "string",
  "entityType": "DESTINATION|PARK|ATTRACTION|RESTAURANT|HOTEL|SHOW",
  "parentId": "string|null",
  "destinationId": "string|null",
  "timezone": "string",
  "location": {
    "latitude": 0.0,
    "longitude": 0.0
  },
  "tags": [
    {
      "tag": "string",
      "tagName": "string",
      "id": "string",
      "value": "string|number|object"
    }
  ]
}
```

---

### 3. Get Entity Children
```
GET /entity/{entityID}/children
```
Get all child entities (e.g., all rides in a park).

**Response:**
```json
{
  "id": "string",
  "name": "string",
  "entityType": "PARK",
  "timezone": "string",
  "children": [
    {
      "id": "string",
      "name": "string",
      "entityType": "ATTRACTION|RESTAURANT|SHOW",
      "externalId": "string",
      "parentId": "string",
      "location": {
        "latitude": 0.0,
        "longitude": 0.0
      }
    }
  ]
}
```

---

### 4. Get Entity Live Data (CURRENTLY USED)
```
GET /entity/{entityID}/live
```
Get real-time status and wait times for entity and all children.

**Response:**
```json
{
  "id": "string",
  "name": "string",
  "entityType": "PARK",
  "timezone": "string",
  "liveData": [
    {
      "id": "string",
      "name": "string",
      "entityType": "ATTRACTION",
      "status": "OPERATING|DOWN|CLOSED|REFURBISHMENT",
      "lastUpdated": "2025-12-01T12:00:00Z",
      "queue": {
        "STANDBY": { "waitTime": 45 },
        "SINGLE_RIDER": { "waitTime": 15 },
        "RETURN_TIME": {
          "state": "AVAILABLE|TEMP_FULL|FINISHED",
          "returnStart": "2025-12-01T14:00:00Z",
          "returnEnd": "2025-12-01T15:00:00Z"
        },
        "PAID_RETURN_TIME": {
          "state": "AVAILABLE",
          "returnStart": "2025-12-01T14:00:00Z",
          "returnEnd": "2025-12-01T15:00:00Z",
          "price": {
            "amount": 15.00,
            "currency": "USD",
            "formatted": "$15.00"
          }
        },
        "BOARDING_GROUP": {
          "allocationStatus": "AVAILABLE|PAUSED|CLOSED",
          "currentGroupStart": 50,
          "currentGroupEnd": 75,
          "nextAllocationTime": "2025-12-01T14:00:00Z",
          "estimatedWait": 30
        },
        "PAID_STANDBY": { "waitTime": 10 }
      },
      "showtimes": [
        {
          "type": "string",
          "startTime": "2025-12-01T14:00:00Z",
          "endTime": "2025-12-01T14:30:00Z"
        }
      ],
      "operatingHours": [
        {
          "type": "string",
          "startTime": "2025-12-01T09:00:00Z",
          "endTime": "2025-12-01T22:00:00Z"
        }
      ],
      "diningAvailability": [
        {
          "partySize": 4,
          "waitTime": 20
        }
      ]
    }
  ]
}
```

**Status Values:**
- `OPERATING` - Ride is running normally
- `DOWN` - Unscheduled breakdown
- `CLOSED` - Scheduled closure (not operating hours)
- `REFURBISHMENT` - Extended maintenance/upgrade

---

### 5. Get Entity Schedule - Next 30 Days (NOT YET USED)
```
GET /entity/{entityID}/schedule
```
**THIS IS THE KEY ENDPOINT WE SHOULD USE FOR PARK HOURS!**

Returns official park operating hours for the next 30 days.

**Response:**
```json
{
  "id": "string",
  "name": "string",
  "entityType": "PARK",
  "timezone": "America/Los_Angeles",
  "schedule": [
    {
      "date": "2025-12-01",
      "openingTime": "2025-12-01T09:00:00-08:00",
      "closingTime": "2025-12-01T22:00:00-08:00",
      "type": "OPERATING|TICKETED_EVENT|PRIVATE_EVENT|EXTRA_HOURS|INFO",
      "purchases": [
        {
          "type": "ADMISSION|PACKAGE|ATTRACTION",
          "id": "string",
          "name": "string",
          "price": {
            "amount": 159.00,
            "currency": "USD",
            "formatted": "$159.00"
          },
          "available": true
        }
      ]
    }
  ]
}
```

**Schedule Types:**
- `OPERATING` - Normal operating hours
- `TICKETED_EVENT` - Special ticketed event (e.g., Halloween party)
- `PRIVATE_EVENT` - Private party/corporate event
- `EXTRA_HOURS` - Early entry, extended hours for hotel guests
- `INFO` - Informational (not operating hours)

---

### 6. Get Entity Schedule - Specific Month
```
GET /entity/{entityID}/schedule/{year}/{month}
```
Get schedule for a specific month.

**Parameters:**
- `year` (required) - 4-digit year (e.g., 2025)
- `month` (required) - Zero-padded month (e.g., 01, 12)

**Response:** Same as endpoint #5

---

## Entity Types

| Type | Description |
|------|-------------|
| `DESTINATION` | Resort complex (e.g., Walt Disney World) |
| `PARK` | Theme park (e.g., Magic Kingdom) |
| `ATTRACTION` | Ride or attraction |
| `RESTAURANT` | Dining location |
| `HOTEL` | Resort hotel |
| `SHOW` | Live show or entertainment |

---

## Current Usage in Our System

We currently use:
- `/entity/{entityID}/live` - Fetched every 5 minutes by the collector

We **should also use**:
- `/entity/{entityID}/schedule` - To get official park hours instead of inferring "park appears open" from ride counts

---

## Improvement Opportunity

**Current approach (hacky):**
- We infer if a park is "open" by checking if any rides are OPERATING
- This is unreliable for parks with few operating rides

**Better approach (using schedule endpoint):**
1. Fetch `/entity/{parkID}/schedule` daily (or cache for 24h)
2. Store opening/closing times in database
3. Check current time against official schedule to determine if park is open
4. Only count downtime during official operating hours

This would:
- Eliminate false positives from seasonal/partial closures
- Give accurate park hours for display
- Allow pre-computing when each park will be open
