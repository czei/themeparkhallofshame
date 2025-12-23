"""
Theme Park Downtime Tracker - Rides API Routes
===============================================

Endpoints for ride-level downtime rankings, wait times, and live status.

Query File Mapping
------------------
GET /live/status-summary               → database/queries/live/status_summary.py
GET /rides/downtime?period=live        → database/queries/live/live_ride_rankings.py (instantaneous)
GET /rides/downtime?period=today       → database/queries/today/today_ride_rankings.py (cumulative today)
GET /rides/downtime?period=yesterday   → StatsRepository.get_ride_daily_rankings(stat_date=yesterday)
GET /rides/downtime?period=last_week   → database/queries/rankings/ride_downtime_rankings.py
GET /rides/downtime?period=last_month  → database/queries/rankings/ride_downtime_rankings.py
GET /rides/waittimes?period=live       → StatsRepository.get_ride_live_wait_time_rankings()
GET /rides/waittimes?period=today      → database/queries/today/today_ride_wait_times.py (cumulative)
GET /rides/waittimes?period=yesterday  → database/queries/yesterday/yesterday_ride_wait_times.py (full prev day)
GET /rides/waittimes?period=last_week  → database/queries/rankings/ride_wait_time_rankings.py
GET /rides/waittimes?period=last_month → database/queries/rankings/ride_wait_time_rankings.py
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta, timezone
from sqlalchemy import text
import pytz

from database.connection import get_db_connection
from database.repositories.stats_repository import StatsRepository
from database.repositories.ride_repository import RideRepository
from utils.cache import get_query_cache, generate_cache_key
from utils.timezone import get_today_pacific

# New query imports - each file handles one specific data source
from database.queries.live import StatusSummaryQuery
from database.queries.rankings import RideDowntimeRankingsQuery, RideWaitTimeRankingsQuery
from database.queries.today import TodayRideWaitTimesQuery, TodayRideRankingsQuery
from database.queries.yesterday import YesterdayRideWaitTimesQuery, YesterdayRideRankingsQuery

# ORM hourly aggregation
from utils.query_helpers import HourlyAggregationQuery, RideHourlyMetrics
from models.base import SessionLocal

from utils.logger import logger

# Timezone constant
PACIFIC_TZ = pytz.timezone('America/Los_Angeles')

rides_bp = Blueprint('rides', __name__)


def pacific_date_to_utc_range(start_date, end_date):
    """
    Convert Pacific-local calendar dates to a UTC-naive half-open datetime range.

    The returned interval corresponds to:
        [start_date 00:00:00 PT, (end_date + 1 day) 00:00:00 PT)
    converted to UTC, then made naive.

    This matches SQL patterns that convert a UTC timestamp to America/Los_Angeles
    and then compare the DATE() portion against start_date/end_date.

    Args:
        start_date: Pacific date (inclusive).
        end_date: Pacific date (inclusive).

    Returns:
        (start_utc_naive, end_utc_naive)

    Raises:
        ValueError: if end_date < start_date.
    """
    from datetime import date, time

    if end_date < start_date:
        raise ValueError(f"end_date {end_date} cannot be before start_date {start_date}")

    # Local midnights in Pacific time.
    start_local_naive = datetime.combine(start_date, time.min)
    end_local_naive = datetime.combine(end_date + timedelta(days=1), time.min)

    # Localize via pytz to get correct PST/PDT offsets (DST-safe).
    start_local = PACIFIC_TZ.localize(start_local_naive, is_dst=None)
    end_local = PACIFIC_TZ.localize(end_local_naive, is_dst=None)

    # Convert to UTC, then drop tzinfo to return UTC-naive datetimes.
    start_utc = start_local.astimezone(pytz.UTC).replace(tzinfo=None)
    end_utc = end_local.astimezone(pytz.UTC).replace(tzinfo=None)

    return start_utc, end_utc


def _utc_naive_hour_to_pacific_date(hour_utc):
    """
    Convert UTC-naive hour_start_utc to Pacific calendar date.

    Args:
        hour_utc: UTC-naive datetime object

    Returns:
        date: Pacific timezone calendar date
    """
    return (
        hour_utc.replace(tzinfo=pytz.UTC)
        .astimezone(PACIFIC_TZ)
        .date()
    )


# Status computation threshold
OPERATING_THRESHOLD_PCT = 50.0


def _status_from_uptime(uptime):
    """
    Compute ride status from uptime percentage.

    Args:
        uptime: Uptime percentage (0-100) or None

    Returns:
        str: Status code - "OPERATING", "DOWN", or "CLOSED"
    """
    if uptime is None:
        return "CLOSED"
    elif uptime >= OPERATING_THRESHOLD_PCT:
        return "OPERATING"
    elif uptime > 0:
        return "DOWN"
    else:
        return "CLOSED"


def _validate_ride_params(ride_id, start_date, end_date):
    """
    Validate ride query parameters.

    Args:
        ride_id: Integer ride ID
        start_date: date object for range start
        end_date: date object for range end

    Raises:
        ValueError: If parameters are invalid
    """
    from datetime import date

    if not isinstance(ride_id, int) or ride_id <= 0:
        raise ValueError(f"Invalid ride_id: {ride_id}")

    if not isinstance(start_date, date) or not isinstance(end_date, date):
        raise ValueError("start_date and end_date must be date objects")

    if end_date < start_date:
        raise ValueError(f"end_date {end_date} cannot be before start_date {start_date}")

    # Enforce max range to prevent expensive queries
    max_days = 93  # ~3 months
    if (end_date - start_date).days > max_days:
        raise ValueError(f"Date range cannot exceed {max_days} days")


@rides_bp.route('/live/status-summary', methods=['GET'])
def get_live_status_summary():
    """
    Get live status summary for all rides.

    Query File Used:
    ----------------
    database/queries/live/status_summary.py

    Returns counts of rides by status:
    - OPERATING: Rides currently running
    - DOWN: Rides experiencing unscheduled breakdowns
    - CLOSED: Rides on scheduled closure
    - REFURBISHMENT: Rides on extended maintenance
    - PARK_CLOSED: Rides in parks that are currently closed

    Query Parameters:
        filter (str): Park filter - 'disney-universal', 'all-parks' (default: 'all-parks')
        park_id (int): Optional park ID to filter to a single park

    Returns:
        JSON response with status counts

    Performance: ~2.5s uncached, ~8ms cached
    """
    filter_type = request.args.get('filter', 'all-parks')
    park_id = request.args.get('park_id', type=int)

    # Validate filter
    if filter_type not in ['disney-universal', 'all-parks']:
        return jsonify({
            "success": False,
            "error": "Invalid filter. Must be 'disney-universal' or 'all-parks'"
        }), 400

    try:
        # Generate cache key
        cache_key = generate_cache_key(
            "live_status_summary",
            filter=filter_type,
            park_id=str(park_id) if park_id else "none"
        )
        cache = get_query_cache()
        cached_result = cache.get(cache_key)
        if cached_result is not None:
            logger.info(f"Cache HIT for live status summary: filter={filter_type}")
            return jsonify(cached_result), 200

        with get_db_connection() as conn:
            # See: database/queries/live/status_summary.py
            query = StatusSummaryQuery(conn)
            summary = query.get_summary(
                filter_disney_universal=(filter_type == 'disney-universal'),
                park_id=park_id
            )

            response = {
                "success": True,
                "filter": filter_type,
                "status_summary": summary,
                "attribution": {
                    "data_source": "ThemeParks.wiki",
                    "url": "https://themeparks.wiki"
                }
            }

            if park_id:
                response["park_id"] = park_id

            # Cache the result (5-minute TTL)
            cache.set(cache_key, response)
            logger.info(f"Cache STORE for live status summary: filter={filter_type}")

            return jsonify(response), 200

    except Exception as e:
        logger.error(f"Error fetching live status summary: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@rides_bp.route('/rides/downtime', methods=['GET'])
def get_ride_downtime_rankings():
    """
    Get ride downtime rankings for specified time period.

Query Files Used:
-----------------
- period=live: database/queries/live/live_ride_rankings.py
  Uses real-time snapshot data (instantaneous - rides down RIGHT NOW)

- period=today: database/queries/today/today_ride_rankings.py
  Uses ride_hourly_stats (pre-aggregated) for fast cumulative downtime

    - period=last_week/last_month: database/queries/rankings/ride_downtime_rankings.py
      Uses pre-aggregated data from ride_daily_stats (calendar-based periods)

    Query Parameters:
        period (str): Time period - 'live', 'today', 'last_week', 'last_month' (default: 'live')
        filter (str): Park filter - 'disney-universal', 'all-parks' (default: 'all-parks')
        limit (int): Maximum results (default: 100, max: 200)
        sort_by (str): Sort column - 'current_is_open', 'downtime_hours', 'uptime_percentage', 'trend_percentage' (default: 'downtime_hours')

    Returns:
        JSON response with ride rankings including current status and trends

    Performance: <100ms for all periods
    """
    # Parse query parameters
    period = request.args.get('period', 'live')
    filter_type = request.args.get('filter', 'all-parks')
    limit = min(int(request.args.get('limit', 100)), 200)
    sort_by = request.args.get('sort_by', 'downtime_hours')

    # Validate period
    valid_periods = ['live', 'today', 'yesterday', '7days', '30days', 'last_week', 'last_month']
    if period not in valid_periods:
        return jsonify({
            "success": False,
            "error": "Invalid period. Must be one of: live, today, yesterday, 7days, 30days, last_week, last_month"
        }), 400

    # Validate filter
    if filter_type not in ['disney-universal', 'all-parks']:
        return jsonify({
            "success": False,
            "error": "Invalid filter. Must be 'disney-universal' or 'all-parks'"
        }), 400

    # Validate sort_by
    valid_sort_options = ['current_is_open', 'downtime_hours', 'uptime_percentage', 'trend_percentage']
    if sort_by not in valid_sort_options:
        return jsonify({
            "success": False,
            "error": f"Invalid sort_by. Must be one of: {', '.join(valid_sort_options)}"
        }), 400

    try:
        # Generate cache key - all periods are cached (live data only updates every 5 min anyway)
        cache_key = generate_cache_key(
            "rides_downtime",
            period=period,
            filter=filter_type,
            limit=str(limit),
            sort_by=sort_by
        )
        cache = get_query_cache()
        cached_result = cache.get(cache_key)
        if cached_result is not None:
            logger.info(f"Cache HIT for ride downtime: period={period}, filter={filter_type}")
            return jsonify(cached_result), 200

        with get_db_connection() as conn:
            filter_disney_universal = (filter_type == 'disney-universal')
            stats_repo = StatsRepository(conn)

            # Route to appropriate query class based on period
            today_pacific = get_today_pacific()
            if period == 'live':
                # LIVE/TODAY: Try pre-aggregated cache first (instant ~10ms)
                # The ride_live_rankings table contains cumulative today data
                # Falls back to raw query (~7s) if cache is empty/stale
                rankings = stats_repo.get_ride_live_rankings_cached(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit,
                    sort_by=sort_by
                )

                # If cache miss, fall back to computing from raw snapshots
                if not rankings:
                    logger.warning("Ride live rankings cache miss, falling back to raw query")
                    rankings = stats_repo.get_ride_live_downtime_rankings(
                        filter_disney_universal=filter_disney_universal,
                        limit=limit,
                        sort_by=sort_by
                    )
            elif period == 'today':
                # TODAY: Use pre-aggregated hourly stats (live-updating)
                query = TodayRideRankingsQuery(conn)
                rankings = query.get_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit,
                    sort_by=sort_by
                )
            elif period == 'yesterday':
                # YESTERDAY: Full previous Pacific day (immutable, highly cacheable)
                query = YesterdayRideRankingsQuery(conn)
                rankings = query.get_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            elif period == '7days':
                rankings = stats_repo.get_ride_weekly_rankings(
                    year=today_pacific.year,
                    week_number=today_pacific.isocalendar()[1],
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            elif period == '30days':
                rankings = stats_repo.get_ride_monthly_rankings(
                    year=today_pacific.year,
                    month=today_pacific.month,
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            else:
                # Historical data from aggregated stats (calendar-based periods)
                # See: database/queries/rankings/ride_downtime_rankings.py
                query = RideDowntimeRankingsQuery(conn)
                if period == 'last_week':
                    rankings = query.get_weekly(
                        filter_disney_universal=filter_disney_universal,
                        limit=limit,
                        sort_by=sort_by
                    )
                else:  # last_month
                    rankings = query.get_monthly(
                        filter_disney_universal=filter_disney_universal,
                        limit=limit,
                        sort_by=sort_by
                    )

            # Add external URLs and rank to rankings
            rankings_with_urls = []
            for rank_idx, ride in enumerate(rankings, start=1):
                ride_dict = dict(ride) if hasattr(ride, '_mapping') else dict(ride)
                ride_dict['rank'] = rank_idx
                # Generate external URL (legacy queue-times format)
                if 'queue_times_id' in ride_dict and 'park_queue_times_id' in ride_dict:
                    ride_dict['queue_times_url'] = f"https://queue-times.com/parks/{ride_dict['park_queue_times_id']}/rides/{ride_dict['queue_times_id']}"
                else:
                    ride_dict['queue_times_url'] = None
                rankings_with_urls.append(ride_dict)

            # Build response
            response = {
                "success": True,
                "period": period,
                "filter": filter_type,
                "data": rankings_with_urls,
                "attribution": {
                    "data_source": "ThemeParks.wiki",
                    "url": "https://themeparks.wiki"
                }
            }

            logger.info(f"Ride rankings requested: period={period}, filter={filter_type}, results={len(rankings_with_urls)}")

            # Cache the result (5-minute TTL)
            cache.set(cache_key, response)
            logger.info(f"Cache STORE for ride downtime: period={period}, filter={filter_type}")

            return jsonify(response), 200

    except Exception as e:
        logger.error(f"Error fetching ride rankings: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@rides_bp.route('/rides/waittimes', methods=['GET'])
def get_ride_wait_times():
    """
    Get ride wait times for a specified time period.

    Query Files Used:
    -----------------
    - period=live: StatsRepository.get_ride_live_wait_time_rankings()
      Uses real-time snapshot data (instantaneous current wait times)

    - period=today: database/queries/today/today_ride_wait_times.py
      Uses snapshot data aggregated from midnight Pacific to now (cumulative)

    - period=last_week/last_month: database/queries/rankings/ride_wait_time_rankings.py
      Uses pre-aggregated data from ride_daily_stats (calendar-based periods)

    Query Parameters:
        period (str): Time period - 'live', 'today', 'last_week', 'last_month' (default: 'live')
        filter (str): Park filter - 'disney-universal', 'all-parks' (default: 'all-parks')
        limit (int): Maximum results (default: 100, max: 200)

    Returns:
        JSON response with wait times sorted by longest average waits descending

    Performance: <100ms for all periods
    """
    # Parse query parameters
    period = request.args.get('period', 'live')
    mode = request.args.get('mode', period)
    filter_type = request.args.get('filter', 'all-parks')
    limit = min(int(request.args.get('limit', 100)), 200)

    # Validate period
    valid_periods = ['live', 'today', 'yesterday', '7days', '30days', 'last_week', 'last_month']
    if period not in valid_periods:
        return jsonify({
            "success": False,
            "error": "Invalid period. Must be one of: live, today, yesterday, 7days, 30days, last_week, last_month"
        }), 400

    # Validate mode (legacy compatibility: allow same values as period plus new aliases)
    valid_modes = ['live', 'today', 'yesterday', 'last_week', 'last_month', '7day-average', 'peak-times']
    if mode not in valid_modes:
        return jsonify({
            "success": False,
            "error": "Invalid mode. Must be one of: live, today, yesterday, last_week, last_month, 7day-average, peak-times"
        }), 400

    # Validate filter
    if filter_type not in ['disney-universal', 'all-parks']:
        return jsonify({
            "success": False,
            "error": "Invalid filter. Must be 'disney-universal' or 'all-parks'"
        }), 400

    try:
        # Generate cache key - all periods are cached (live data only updates every 5 min anyway)
        cache_key = generate_cache_key(
            "rides_waittimes",
            period=period,
            mode=mode,
            filter=filter_type,
            limit=str(limit)
        )
        cache = get_query_cache()
        cached_result = cache.get(cache_key)
        if cached_result is not None:
            logger.info(f"Cache HIT for ride waittimes: period={period}, filter={filter_type}")
            return jsonify(cached_result), 200

        with get_db_connection() as conn:
            filter_disney_universal = (filter_type == 'disney-universal')

            # Route to appropriate query based on period
            if mode == 'live':
                # LIVE data - instantaneous current wait times
                stats_repo = StatsRepository(conn)
                wait_times = stats_repo.get_ride_live_wait_time_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            elif mode == 'today':
                # TODAY data - cumulative from midnight Pacific to now
                # See: database/queries/today/today_ride_wait_times.py
                query = TodayRideWaitTimesQuery(conn)
                wait_times = query.get_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            elif mode == 'yesterday':
                # YESTERDAY data - full previous Pacific day
                # See: database/queries/yesterday/yesterday_ride_wait_times.py
                query = YesterdayRideWaitTimesQuery(conn)
                wait_times = query.get_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            elif mode == '7day-average':
                stats_repo = StatsRepository(conn)
                wait_times = stats_repo.get_average_wait_times(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            elif mode == 'peak-times':
                stats_repo = StatsRepository(conn)
                wait_times = stats_repo.get_peak_wait_times(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            else:
                # Historical data from aggregated stats (calendar-based periods)
                # See: database/queries/rankings/ride_wait_time_rankings.py
                query = RideWaitTimeRankingsQuery(conn)
                if mode == 'last_week':
                    wait_times = query.get_weekly(
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                else:  # last_month
                    wait_times = query.get_monthly(
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )

            # Add external URLs and rank to wait times
            wait_times_with_urls = []
            for rank_idx, ride in enumerate(wait_times, start=1):
                ride_dict = dict(ride) if hasattr(ride, '_mapping') else dict(ride)
                ride_dict['rank'] = rank_idx
                # Generate external URL (legacy queue-times format)
                if 'queue_times_id' in ride_dict and 'park_queue_times_id' in ride_dict:
                    ride_dict['queue_times_url'] = f"https://queue-times.com/parks/{ride_dict['park_queue_times_id']}/rides/{ride_dict['queue_times_id']}"
                else:
                    ride_dict['queue_times_url'] = None
                if mode == 'live':
                    current_wait = ride_dict.get('avg_wait_minutes')
                    if current_wait is None:
                        ride_dict['current_wait_minutes'] = None
                    else:
                        ride_dict['current_wait_minutes'] = float(current_wait)
                wait_times_with_urls.append(ride_dict)

            # Build response
            response = {
                "success": True,
                "period": period,
                "mode": mode,
                "filter": filter_type,
                "data": wait_times_with_urls,
                "attribution": {
                    "data_source": "ThemeParks.wiki",
                    "url": "https://themeparks.wiki"
                }
            }

            logger.info(f"Wait times requested: period={period}, filter={filter_type}, results={len(wait_times_with_urls)}")

            # Cache the result (5-minute TTL)
            cache.set(cache_key, response)
            logger.info(f"Cache STORE for ride waittimes: period={period}, filter={filter_type}")

            return jsonify(response), 200

    except Exception as e:
        logger.error(f"Error fetching wait times: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@rides_bp.route('/rides/<int:ride_id>/details', methods=['GET'])
def get_ride_details(ride_id: int):
    """
    Get detailed information for a specific ride.

    This endpoint provides comprehensive ride data for the ride detail page:
    - Ride metadata (name, park, tier, etc.)
    - Time-series data (hourly stats with wait times and status)
    - Summary statistics (total downtime, uptime percentage, etc.)
    - Downtime events (start time, end time, duration)

    Path Parameters:
        ride_id (int): Ride ID

    Query Parameters:
        period (str): Time period - 'today', 'yesterday', 'last_week', 'last_month'
            - today: All data from midnight Pacific to now
            - yesterday: Full previous Pacific day (immutable, highly cacheable)
            - last_week: Last 7 complete days
            - last_month: Last 30 complete days

    Returns:
        JSON response with ride details, time-series data, summary stats, and downtime events

    Performance: <200ms for all periods
    """
    period = request.args.get('period', 'today')

    # Validate period
    if period not in ['today', 'yesterday', 'last_week', 'last_month']:
        return jsonify({
            "success": False,
            "error": "Invalid period. Must be 'today', 'yesterday', 'last_week', or 'last_month'"
        }), 400

    try:
        # Generate cache key
        cache_key = generate_cache_key(
            "ride_details",
            ride_id=str(ride_id),
            period=period
        )
        cache = get_query_cache()
        cached_result = cache.get(cache_key)
        if cached_result is not None:
            logger.info(f"Cache HIT for ride details: ride_id={ride_id}, period={period}")
            return jsonify(cached_result), 200

        with get_db_connection() as conn:
            ride_repo = RideRepository(conn)

            # Get ride basic info
            ride = ride_repo.get_by_id(ride_id)
            if not ride:
                return jsonify({
                    "success": False,
                    "error": f"Ride {ride_id} not found"
                }), 404

            # Determine date range based on period
            today_pacific = get_today_pacific()

            if period == 'today':
                # Today: midnight Pacific to now
                start_date = today_pacific
                end_date = today_pacific
                is_today = True
            elif period == 'yesterday':
                # Yesterday: full previous day
                start_date = today_pacific - timedelta(days=1)
                end_date = today_pacific - timedelta(days=1)
                is_today = False
            elif period == 'last_week':
                # Last 7 complete days
                start_date = today_pacific - timedelta(days=7)
                end_date = today_pacific - timedelta(days=1)
                is_today = False
            else:  # last_month
                # Last 30 complete days
                start_date = today_pacific - timedelta(days=30)
                end_date = today_pacific - timedelta(days=1)
                is_today = False

            # Get hourly time-series data
            timeseries_data = _get_ride_timeseries(
                ride_id, start_date, end_date, period
            )

            # Get summary statistics
            summary_stats = _get_ride_summary_stats(
                ride_id, start_date, end_date, period
            )

            # Get downtime events
            downtime_events = _get_ride_downtime_events(
                ride_id, start_date, end_date, period
            )

            # Get hourly breakdown (for table display)
            hourly_breakdown = _get_ride_hourly_breakdown(
                ride_id, start_date, end_date, period
            )

            # Get park name and tier from ride dataclass (populated via ORM join)
            park_name = ride.park_name
            tier = ride.tier

            # Build response
            response = {
                "success": True,
                "period": period,
                "ride": {
                    "ride_id": ride.ride_id,
                    "name": ride.name,
                    "park_id": ride.park_id,
                    "park_name": park_name,
                    "tier": tier,
                    "category": ride.category,
                    "queue_times_url": f"https://queue-times.com/parks/{ride.park_queue_times_id}/rides/{ride.queue_times_id}" if ride.queue_times_id and ride.park_queue_times_id else None
                },
                "timeseries": timeseries_data,
                "summary": summary_stats,
                "downtime_events": downtime_events,
                "hourly_breakdown": hourly_breakdown,
                "attribution": {
                    "data_source": "ThemeParks.wiki",
                    "url": "https://themeparks.wiki"
                }
            }

            logger.info(f"Ride details requested: ride_id={ride_id}, period={period}")

            # Cache the result (5-minute TTL for today, 1-hour TTL for historical)
            cache.set(cache_key, response)
            logger.info(f"Cache STORE for ride details: ride_id={ride_id}, period={period}")

            return jsonify(response), 200

    except Exception as e:
        logger.error(f"Error fetching ride details for ride {ride_id}: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


def _get_ride_timeseries(ride_id, start_date, end_date, period):
    """
    Get time-series data for the ride (for wait time chart with status overlay).

    For TODAY/YESTERDAY: Returns hourly data with hour_start_utc
    FOR LAST_WEEK/LAST_MONTH: Returns daily aggregated data with date field

    Args:
        ride_id: Integer ride ID
        start_date: date object for range start (Pacific timezone)
        end_date: date object for range end (Pacific timezone)
        period: Period identifier ('today', 'yesterday', 'last_week', 'last_month')

    Returns:
        list: Hourly or daily time series data
            - Hourly data: hour_start_utc, avg_wait_time_minutes, status, uptime_percentage
            - Daily data: date, avg_wait_time_minutes, status, uptime_percentage
    """
    from sqlalchemy.exc import SQLAlchemyError
    import logging

    logger = logging.getLogger(__name__)

    # Validate inputs
    _validate_ride_params(ride_id, start_date, end_date)

    # Convert inclusive Pacific date range to a half-open UTC range [start, end)
    start_utc, end_utc = pacific_date_to_utc_range(start_date, end_date)

    # Query hourly metrics with error handling
    try:
        with SessionLocal() as session:
            metrics = HourlyAggregationQuery.ride_hour_range_metrics(
                session=session,
                ride_id=ride_id,
                start_utc=start_utc,
                end_utc=end_utc,
            )
    except SQLAlchemyError as e:
        logger.error(
            "Database error in _get_ride_timeseries",
            extra={
                "ride_id": ride_id,
                "start_utc": start_utc,
                "end_utc": end_utc,
                "period": period,
                "error": str(e)
            }
        )
        raise

    # Daily aggregation for weekly/monthly views (mirrors GROUP BY DATE(CONVERT_TZ(...)))
    if period in ['last_week', 'last_month']:
        daily = {}
        for m in metrics:
            pacific_day = _utc_naive_hour_to_pacific_date(m.hour_start_utc)

            bucket = daily.setdefault(pacific_day, {
                "wait_values": [],
                "uptime_values": [],
                "snapshot_count": 0,
            })

            # AVG(avg_wait_time_minutes) ignores NULLs
            if m.avg_wait_time_minutes is not None:
                bucket["wait_values"].append(m.avg_wait_time_minutes)

            # AVG(uptime_percentage) (defensive null-handling)
            if m.uptime_percentage is not None:
                bucket["uptime_values"].append(m.uptime_percentage)

            # SUM(snapshot_count)
            bucket["snapshot_count"] += int(m.snapshot_count or 0)

        timeseries = []
        for day in sorted(daily.keys()):  # SQL orders ORDER BY date ASC
            b = daily[day]

            avg_wait = (sum(b["wait_values"]) / len(b["wait_values"])) if b["wait_values"] else None
            avg_uptime = (sum(b["uptime_values"]) / len(b["uptime_values"])) if b["uptime_values"] else None

            # Compute status from uptime percentage
            status = _status_from_uptime(avg_uptime)

            timeseries.append({
                "date": day.strftime('%Y-%m-%d'),
                "avg_wait_time_minutes": float(avg_wait) if avg_wait is not None else None,
                "uptime_percentage": float(avg_uptime) if avg_uptime is not None else None,
                "status": status,
                "snapshot_count": int(b["snapshot_count"]),
            })

        return timeseries

    # Hourly series for today/yesterday (ORDER BY hour_start_utc ASC)
    timeseries = []
    for m in sorted(metrics, key=lambda x: x.hour_start_utc):
        status = _status_from_uptime(m.uptime_percentage)

        timeseries.append({
            "hour_start_utc": m.hour_start_utc,
            "avg_wait_time_minutes": float(m.avg_wait_time_minutes) if m.avg_wait_time_minutes is not None else None,
            "uptime_percentage": float(m.uptime_percentage) if m.uptime_percentage is not None else None,
            "status": status,
            "snapshot_count": int(m.snapshot_count or 0),
        })

    return timeseries


def _get_ride_summary_stats(ride_id, start_date, end_date, period):
    """
    Get summary statistics for the ride.

    Args:
        ride_id: Integer ride ID
        start_date: date object for range start (Pacific timezone)
        end_date: date object for range end (Pacific timezone)
        period: Period identifier (unused, kept for API compatibility)

    Returns:
        dict: Summary statistics
            - total_downtime_hours: Total hours the ride was down
            - uptime_percentage: Overall uptime percentage
            - avg_wait_time: Average wait time across the period
            - total_operating_hours: Total hours ride was operating
            - total_hours: Total hours with operated=1
    """
    from sqlalchemy.exc import SQLAlchemyError
    import logging

    logger = logging.getLogger(__name__)

    # Validate inputs
    _validate_ride_params(ride_id, start_date, end_date)

    # Convert inclusive Pacific date range to a half-open UTC range [start, end)
    start_utc, end_utc = pacific_date_to_utc_range(start_date, end_date)

    # Query hourly metrics with error handling
    try:
        with SessionLocal() as session:
            metrics = HourlyAggregationQuery.ride_hour_range_metrics(
                session=session,
                ride_id=ride_id,
                start_utc=start_utc,
                end_utc=end_utc,
            )
    except SQLAlchemyError as e:
        logger.error(
            "Database error in _get_ride_summary_stats",
            extra={
                "ride_id": ride_id,
                "start_utc": start_utc,
                "end_utc": end_utc,
                "period": period,
                "error": str(e)
            }
        )
        raise

    # Match SQL filter: AND ride_operated = 1
    operated_metrics = [m for m in metrics if m.ride_operated]

    total_hours = len(operated_metrics)
    if total_hours == 0:
        return {
            "total_downtime_hours": None,
            "uptime_percentage": None,
            "avg_wait_time": None,
            "total_operating_hours": None,
            "total_hours": 0,
        }

    total_downtime_hours = sum((m.downtime_hours or 0.0) for m in operated_metrics)

    # SQL AVG() ignores NULLs; uptime_percentage is non-null in RideHourlyMetrics (float),
    # but keep defensive behavior.
    uptime_values = [m.uptime_percentage for m in operated_metrics if m.uptime_percentage is not None]
    uptime_percentage = (sum(uptime_values) / len(uptime_values)) if uptime_values else None

    # SQL AVG(avg_wait_time_minutes) ignores NULLs.
    wait_values = [m.avg_wait_time_minutes for m in operated_metrics if m.avg_wait_time_minutes is not None]
    avg_wait_time = (sum(wait_values) / len(wait_values)) if wait_values else None

    total_operating_hours = sum(1 for m in operated_metrics if (m.operating_snapshots or 0) > 0)

    return {
        "total_downtime_hours": float(total_downtime_hours) if total_downtime_hours is not None else None,
        "uptime_percentage": float(uptime_percentage) if uptime_percentage is not None else None,
        "avg_wait_time": float(avg_wait_time) if avg_wait_time is not None else None,
        "total_operating_hours": int(total_operating_hours) if total_operating_hours is not None else None,
        "total_hours": int(total_hours),
    }


def _get_ride_downtime_events(ride_id, start_date, end_date, period):
    """
    Get downtime events for the ride (for downtime events table).

    Note: Currently returns 1-hour buckets for hours with down_snapshots > 0.
    Future enhancement: merge contiguous hours into single events.

    Args:
        ride_id: Integer ride ID
        start_date: date object for range start (Pacific timezone)
        end_date: date object for range end (Pacific timezone)
        period: Period identifier (unused, kept for API compatibility)

    Returns:
        list: Downtime events (1-hour buckets)
            - start_time: Hour start (UTC datetime)
            - end_time: Hour end (UTC datetime)
            - duration_hours: Downtime within this hour
    """
    from sqlalchemy.exc import SQLAlchemyError
    import logging

    logger = logging.getLogger(__name__)

    # Validate inputs
    _validate_ride_params(ride_id, start_date, end_date)

    # Convert inclusive Pacific date range to a half-open UTC range [start, end)
    start_utc, end_utc = pacific_date_to_utc_range(start_date, end_date)

    # Query hourly metrics with error handling
    try:
        with SessionLocal() as session:
            metrics = HourlyAggregationQuery.ride_hour_range_metrics(
                session=session,
                ride_id=ride_id,
                start_utc=start_utc,
                end_utc=end_utc,
            )
    except SQLAlchemyError as e:
        logger.error(
            "Database error in _get_ride_downtime_events",
            extra={
                "ride_id": ride_id,
                "start_utc": start_utc,
                "end_utc": end_utc,
                "period": period,
                "error": str(e)
            }
        )
        raise

    # Match SQL: AND down_snapshots > 0 and ORDER BY hour_start_utc DESC
    events = []
    for m in sorted(metrics, key=lambda x: x.hour_start_utc, reverse=True):
        if (m.down_snapshots or 0) <= 0:
            continue

        events.append({
            "start_time": m.hour_start_utc,
            "end_time": m.hour_start_utc + timedelta(hours=1),
            "duration_hours": float(m.downtime_hours) if m.downtime_hours is not None else None,
        })

    return events


def _get_ride_hourly_breakdown(ride_id, start_date, end_date, period):
    """
    Get breakdown data for the ride (for breakdown table).

    For TODAY/YESTERDAY: Returns hourly breakdown
    For LAST_WEEK/LAST_MONTH: Returns daily breakdown (matches chart granularity)

    Args:
        ride_id: Integer ride ID
        start_date: date object for range start (Pacific timezone)
        end_date: date object for range end (Pacific timezone)
        period: Period identifier ('today', 'yesterday', 'last_week', 'last_month')

    Returns:
        list: Hourly or daily breakdown data
            - Hourly: hour_start_utc, avg_wait_time_minutes, operating_snapshots, down_snapshots, etc.
            - Daily: date, avg_wait_time_minutes, operating_hours, down_hours, etc.
    """
    from sqlalchemy.exc import SQLAlchemyError
    import logging

    logger = logging.getLogger(__name__)

    # Validate inputs
    _validate_ride_params(ride_id, start_date, end_date)

    # Convert inclusive Pacific date range to a half-open UTC range [start, end)
    start_utc, end_utc = pacific_date_to_utc_range(start_date, end_date)

    # Query hourly metrics with error handling
    try:
        with SessionLocal() as session:
            metrics = HourlyAggregationQuery.ride_hour_range_metrics(
                session=session,
                ride_id=ride_id,
                start_utc=start_utc,
                end_utc=end_utc,
            )
    except SQLAlchemyError as e:
        logger.error(
            "Database error in _get_ride_hourly_breakdown",
            extra={
                "ride_id": ride_id,
                "start_utc": start_utc,
                "end_utc": end_utc,
                "period": period,
                "error": str(e)
            }
        )
        raise

    # Path 1: daily aggregation for last_week/last_month
    if period in ['last_week', 'last_month']:
        # Group by Pacific calendar date (mirrors DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')))
        daily = {}
        for m in metrics:
            pacific_day = _utc_naive_hour_to_pacific_date(m.hour_start_utc)

            bucket = daily.setdefault(pacific_day, {
                "wait_values": [],
                "uptime_values": [],
                "operating_hours": 0,
                "down_hours": 0,
                "downtime_hours": 0.0,
                "snapshot_count": 0,
            })

            # AVG(avg_wait_time_minutes) ignores NULLs
            if m.avg_wait_time_minutes is not None:
                bucket["wait_values"].append(m.avg_wait_time_minutes)

            # AVG(uptime_percentage) in SQL would ignore NULLs; ORM provides float, but keep consistent.
            if m.uptime_percentage is not None:
                bucket["uptime_values"].append(m.uptime_percentage)

                # SUM(CASE WHEN uptime_percentage >= OPERATING_THRESHOLD_PCT THEN 1 ELSE 0 END)
                if m.uptime_percentage >= OPERATING_THRESHOLD_PCT:
                    bucket["operating_hours"] += 1

                # SUM(CASE WHEN uptime_percentage < OPERATING_THRESHOLD_PCT AND uptime_percentage > 0 THEN 1 ELSE 0 END)
                if 0 < m.uptime_percentage < OPERATING_THRESHOLD_PCT:
                    bucket["down_hours"] += 1

            # SUM(downtime_hours)
            bucket["downtime_hours"] += float(m.downtime_hours or 0.0)

            # SUM(snapshot_count)
            bucket["snapshot_count"] += int(m.snapshot_count or 0)

        # ORDER BY date DESC (match SQL)
        breakdown = []
        for day in sorted(daily.keys(), reverse=True):
            b = daily[day]

            avg_wait = (sum(b["wait_values"]) / len(b["wait_values"])) if b["wait_values"] else None
            avg_uptime = (sum(b["uptime_values"]) / len(b["uptime_values"])) if b["uptime_values"] else None

            breakdown.append({
                "date": day.strftime('%Y-%m-%d'),
                "avg_wait_time_minutes": float(avg_wait) if avg_wait is not None else None,
                "operating_hours": int(b["operating_hours"]),
                "down_hours": int(b["down_hours"]),
                "downtime_hours": float(b["downtime_hours"]) if b["downtime_hours"] is not None else None,
                "uptime_percentage": float(avg_uptime) if avg_uptime is not None else None,
                "snapshot_count": int(b["snapshot_count"]),
            })

        return breakdown

    # Path 2: hourly breakdown for today/yesterday
    # ORDER BY hour_start_utc DESC (match SQL)
    breakdown = []
    for m in sorted(metrics, key=lambda x: x.hour_start_utc, reverse=True):
        breakdown.append({
            "hour_start_utc": m.hour_start_utc,
            "avg_wait_time_minutes": float(m.avg_wait_time_minutes) if m.avg_wait_time_minutes is not None else None,
            "operating_snapshots": int(m.operating_snapshots or 0),
            "down_snapshots": int(m.down_snapshots or 0),
            "downtime_hours": float(m.downtime_hours) if m.downtime_hours is not None else None,
            "uptime_percentage": float(m.uptime_percentage) if m.uptime_percentage is not None else None,
            "snapshot_count": int(m.snapshot_count or 0),
        })

    return breakdown
