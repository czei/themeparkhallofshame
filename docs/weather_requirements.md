# Weather Data Collection System - Requirements Document

## Project Context

This document specifies requirements for adding weather data collection to the Theme Park Hall of Shame project. The goal is to capture comprehensive weather data for all tracked theme parks to enable future correlation analysis between weather conditions and ride operational status.

### Existing System Overview
- **Backend:** Java
- **Database:** MariaDB
- **Data Source:** Queue-Times.com API
- **Current Scope:** ~150+ theme parks, 5,000-7,500 rides
- **Collection Interval:** 10-minute snapshots for ride status
- **Ride Data Retention:** 24-hour rolling window for raw data, then daily aggregation
- **Weather Data Retention:** 2 years hourly (to support user-specific time correlation), daily aggregates indefinitely

### Integration Principle
Weather data collection should run as a parallel process to existing ride data collection. The two datasets will be joined during analysis phase, not during collection.

---

## 1. Weather API Selection

### Primary: Open-Meteo API
- **Base URL:** `https://api.open-meteo.com/v1/forecast`
- **Cost:** Free, no API key required
- **Rate Limits:** No hard limits; requests respectful usage (1 second between requests)
- **Documentation:** https://open-meteo.com/en/docs

**Note:** Open-Meteo also has a Historical API at `https://archive-api.open-meteo.com/v1/archive` but we are not using it. Without historical ride data to correlate against, historical weather data has no value.

### Why Open-Meteo (Alternatives Evaluated)

| Service | Free Tier | Problem at Our Scale |
|---------|-----------|----------------------|
| **Open-Meteo** | **Unlimited, no key** | **None - this is the choice** |
| Visual Crossing | 1,000 records/day | 150 parks × 24 hours = 3,600 calls/day. Exceeds limit. |
| OpenWeatherMap | 1,000 calls/day | Same math, same problem |
| Tomorrow.io | 1,000 calls/month | Completely inadequate |
| WeatherAPI.com | 1M calls/month | Would work, but requires API key management |
| Weatherstack | 1,000 calls/month | Completely inadequate |
| NWS API | Free, unlimited | US only, no international parks, reliability issues |

**Open-Meteo advantages:**
1. Truly free for commercial use at any volume
2. No API key required (zero management overhead)
3. Global coverage (handles international Disney/Universal parks)
4. Includes WMO weather codes for thunderstorm detection
5. Hourly resolution matches our analysis needs
6. **Provides forecasts up to 16 days out** from the same endpoint (we use 7 days; accuracy degrades beyond 3-5 days)

### Update Frequency

**Current Weather:** Every hour, on the hour
**Forecasts:** Every 6 hours

**Rationale:**
- Open-Meteo's underlying weather models update hourly. Polling more frequently returns the same data.
- Weather conditions that cause closures (thunderstorms, high wind) develop over 15-60 minute timescales, not minutes.
- Ride status is collected every 10 minutes, but correlation analysis will aggregate to hourly buckets anyway.
- Collecting more frequently than hourly just creates redundant data points with no analytical value.
- 150 parks at 1 request/second = 2.5 minutes to complete a collection cycle. Hourly gives plenty of margin.

**Collection Schedule:**
- `:00` - Begin hourly weather collection for all parks
- `:03` - Hourly collection complete (with margin)
- Every 6 hours (00:00, 06:00, 12:00, 18:00 UTC) - Forecast collection

### Lightning Data Limitation

**The Problem:** Lightning is the primary driver of ride closures in Florida and other thunderstorm-prone regions. Theme parks typically have lightning policies requiring closure of outdoor attractions when strikes are detected within 5-10 miles.

**Available Lightning APIs (all commercial):**
- Xweather/Vaisala: Industry standard, but standard tier only provides 5 minutes of history
- DTN Lightning: Up to 7 days history, commercial pricing
- Earth Networks: Commercial pricing
- Blitzortung.org: Free community project, but programmatic API access restricted to participants who operate detection stations

**Our Workaround:** Use WMO weather codes as a thunderstorm proxy:
- Code 95: Thunderstorm (slight or moderate)
- Code 96: Thunderstorm with slight hail
- Code 99: Thunderstorm with heavy hail

These codes indicate "thunderstorm conditions present," which is what triggers park lightning policies. While we won't have strike counts or precise proximity data, the ML model can learn the correlation between thunderstorm weather codes and ride closures.

**Future Enhancement:** If correlation analysis shows thunderstorm codes are highly predictive, we could evaluate adding a commercial lightning API. But start with the free proxy first—it may be sufficient.

---

## 2. Data Requirements

### Current Weather Observations
Collect hourly for each park:
- Temperature (actual and feels-like)
- Wind speed and gusts
- Wind direction
- Precipitation amount
- Precipitation probability
- Rain and snowfall amounts
- Cloud cover percentage
- Visibility
- Weather code (WMO standard)
- Humidity
- Barometric pressure

### Forecasts
Collect every 6 hours, storing 7-day hourly forecasts:
- Same variables as current weather
- Track when forecast was issued vs. what time it's forecasting
- This enables later analysis of forecast accuracy at different lead times

### Data Retention
- **Hourly observations:** 2 years (park visitors need hourly granularity to correlate with specific visit times)
- **Forecasts:** 90 days (sufficient for forecast accuracy analysis)
- **Daily aggregates:** Indefinitely (for long-term trend analysis)

### No Historical Backfill
We do not have historical ride/wait time data to correlate against, so historical weather data would be useless. The correlation dataset begins the day weather collection goes live.

---

## 3. Future Considerations (Not In Scope Yet)

1. **Commercial Lightning Data:** If thunderstorm weather codes prove highly predictive, evaluate Xweather, DTN, or Earth Networks for precise strike data. Cost-benefit analysis needed after initial correlation results.

2. **Blitzortung.org Integration:** Free community lightning network. Would require either becoming a participant (running a detection station) or scraping their map interface. Legal/ToS review needed.

3. **Severe Weather Alerts:** NWS API for US parks, similar services for international. Could provide advance warning of incoming weather.

4. **Forecast Ensemble Data:** Open-Meteo offers ensemble forecasts for uncertainty quantification—useful for probabilistic predictions.

5. **Ride Weather Sensitivity Tags:** Database field to mark rides as wind-sensitive, lightning-sensitive, indoor, etc. Enables segmented analysis.

6. **Real-time Weather Display:** Showing current weather on the Hall of Shame website alongside ride status.
