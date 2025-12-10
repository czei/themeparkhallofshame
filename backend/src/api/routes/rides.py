"""
Theme Park Downtime Tracker - Rides API Routes
===============================================

Endpoints for ride-level downtime rankings, wait times, and live status.

Query File Mapping
------------------
GET /live/status-summary               → database/queries/live/status_summary.py
GET /rides/downtime?period=live        → database/queries/live/live_ride_rankings.py (instantaneous)
GET /rides/downtime?period=today       → database/queries/today/today_ride_rankings.py (cumulative)
GET /rides/downtime?period=yesterday   → database/queries/yesterday/yesterday_ride_rankings.py (full prev day)
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

from database.connection import get_db_connection
from database.repositories.stats_repository import StatsRepository
from database.repositories.ride_repository import RideRepository
from utils.cache import get_query_cache, generate_cache_key
from utils.timezone import get_today_pacific

# New query imports - each file handles one specific data source
from database.queries.live import StatusSummaryQuery
from database.queries.rankings import RideDowntimeRankingsQuery, RideWaitTimeRankingsQuery
from database.queries.today import TodayRideRankingsQuery, TodayRideWaitTimesQuery
from database.queries.yesterday import YesterdayRideRankingsQuery, YesterdayRideWaitTimesQuery

from utils.logger import logger

rides_bp = Blueprint('rides', __name__)


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
      Uses snapshot data aggregated from midnight Pacific to now (cumulative)

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
    if period not in ['live', 'today', 'yesterday', 'last_week', 'last_month']:
        return jsonify({
            "success": False,
            "error": "Invalid period. Must be 'live', 'today', 'yesterday', 'last_week', or 'last_month'"
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
            if period in ('live', 'today'):
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
            elif period == 'yesterday':
                # YESTERDAY: Full previous Pacific day (immutable, highly cacheable)
                query = YesterdayRideRankingsQuery(conn)
                rankings = query.get_rankings(
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

            # Add Queue-Times.com URLs and rank to rankings
            rankings_with_urls = []
            for rank_idx, ride in enumerate(rankings, start=1):
                ride_dict = dict(ride) if hasattr(ride, '_mapping') else dict(ride)
                ride_dict['rank'] = rank_idx
                # Generate correct Queue-Times URL: /parks/{park_qt_id}/rides/{ride_qt_id}
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
    filter_type = request.args.get('filter', 'all-parks')
    limit = min(int(request.args.get('limit', 100)), 200)

    # Validate period
    if period not in ['live', 'today', 'yesterday', 'last_week', 'last_month']:
        return jsonify({
            "success": False,
            "error": "Invalid period. Must be 'live', 'today', 'yesterday', 'last_week', or 'last_month'"
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
            if period == 'live':
                # LIVE data - instantaneous current wait times
                stats_repo = StatsRepository(conn)
                wait_times = stats_repo.get_ride_live_wait_time_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            elif period == 'today':
                # TODAY data - cumulative from midnight Pacific to now
                # See: database/queries/today/today_ride_wait_times.py
                query = TodayRideWaitTimesQuery(conn)
                wait_times = query.get_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            elif period == 'yesterday':
                # YESTERDAY data - full previous Pacific day
                # See: database/queries/yesterday/yesterday_ride_wait_times.py
                query = YesterdayRideWaitTimesQuery(conn)
                wait_times = query.get_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            else:
                # Historical data from aggregated stats (calendar-based periods)
                # See: database/queries/rankings/ride_wait_time_rankings.py
                query = RideWaitTimeRankingsQuery(conn)
                if period == 'last_week':
                    wait_times = query.get_weekly(
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                else:  # last_month
                    wait_times = query.get_monthly(
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )

            # Add Queue-Times.com URLs and rank to wait times
            wait_times_with_urls = []
            for rank_idx, ride in enumerate(wait_times, start=1):
                ride_dict = dict(ride) if hasattr(ride, '_mapping') else dict(ride)
                ride_dict['rank'] = rank_idx
                # Generate correct Queue-Times URL: /parks/{park_qt_id}/rides/{ride_qt_id}
                if 'queue_times_id' in ride_dict and 'park_queue_times_id' in ride_dict:
                    ride_dict['queue_times_url'] = f"https://queue-times.com/parks/{ride_dict['park_queue_times_id']}/rides/{ride_dict['queue_times_id']}"
                else:
                    ride_dict['queue_times_url'] = None
                wait_times_with_urls.append(ride_dict)

            # Build response
            response = {
                "success": True,
                "period": period,
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
                conn, ride_id, start_date, end_date, is_today, period
            )

            # Get summary statistics
            summary_stats = _get_ride_summary_stats(
                conn, ride_id, start_date, end_date, is_today, period
            )

            # Get downtime events
            downtime_events = _get_ride_downtime_events(
                conn, ride_id, start_date, end_date, is_today, period
            )

            # Get hourly breakdown (for table display)
            hourly_breakdown = _get_ride_hourly_breakdown(
                conn, ride_id, start_date, end_date, is_today, period
            )

            # Get park name
            park_query = text("SELECT name FROM parks WHERE park_id = :park_id")
            park_result = conn.execute(park_query, {"park_id": ride.park_id})
            park_row = park_result.fetchone()
            park_name = park_row[0] if park_row else None

            # Get tier from ride_classifications (tier data is stored there, not in rides table)
            tier_query = text("SELECT tier FROM ride_classifications WHERE ride_id = :ride_id")
            tier_result = conn.execute(tier_query, {"ride_id": ride_id})
            tier_row = tier_result.fetchone()
            tier = tier_row[0] if tier_row else None

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


def _get_ride_timeseries(conn, ride_id, start_date, end_date, is_today, period):
    """
    Get time-series data for the ride (for wait time chart with status overlay).

    For TODAY/YESTERDAY/LAST_WEEK: Returns hourly data with hour_start_utc
    For LAST_MONTH: Returns daily aggregated data with date field

    Returns:
    - Hourly data: hour_start_utc, avg_wait_time_minutes, status, uptime_percentage
    - Daily data: date, avg_wait_time_minutes, status, uptime_percentage
    """
    # Use daily aggregation only for monthly view (too many hourly points for a month)
    if period in ['last_month']:
        # Daily aggregation query
        query = text("""
            SELECT
                DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')) as date,
                AVG(avg_wait_time_minutes) as avg_wait_time_minutes,
                AVG(uptime_percentage) as uptime_percentage,
                CASE
                    WHEN AVG(uptime_percentage) >= 50 THEN 'OPERATING'
                    WHEN AVG(uptime_percentage) > 0 THEN 'DOWN'
                    ELSE 'CLOSED'
                END as status,
                SUM(snapshot_count) as snapshot_count
            FROM ride_hourly_stats
            WHERE ride_id = :ride_id
                AND DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')) >= :start_date
                AND DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')) <= :end_date
            GROUP BY DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles'))
            ORDER BY date
        """)

        result = conn.execute(query, {
            "ride_id": ride_id,
            "start_date": start_date,
            "end_date": end_date
        })

        # Convert to daily format
        timeseries = []
        for row in result:
            row_dict = dict(row._mapping)
            timeseries.append({
                "date": row_dict["date"].strftime('%Y-%m-%d'),
                "avg_wait_time_minutes": float(row_dict["avg_wait_time_minutes"]) if row_dict["avg_wait_time_minutes"] is not None else None,
                "uptime_percentage": float(row_dict["uptime_percentage"]) if row_dict["uptime_percentage"] is not None else None,
                "status": row_dict["status"],
                "snapshot_count": int(row_dict["snapshot_count"]) if row_dict["snapshot_count"] is not None else 0
            })
        return timeseries

    # Hourly data for TODAY/YESTERDAY
    if is_today:
        query = text("""
            SELECT
                hour_start_utc,
                avg_wait_time_minutes,
                uptime_percentage,
                CASE
                    WHEN uptime_percentage >= 50 THEN 'OPERATING'
                    WHEN uptime_percentage > 0 THEN 'DOWN'
                    ELSE 'CLOSED'
                END as status,
                snapshot_count
            FROM ride_hourly_stats
            WHERE ride_id = :ride_id
                AND DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')) >= :start_date
                AND DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')) <= :end_date
            ORDER BY hour_start_utc
        """)
    else:
        # For historical periods, only include complete hours
        query = text("""
            SELECT
                hour_start_utc,
                avg_wait_time_minutes,
                uptime_percentage,
                CASE
                    WHEN uptime_percentage >= 50 THEN 'OPERATING'
                    WHEN uptime_percentage > 0 THEN 'DOWN'
                    ELSE 'CLOSED'
                END as status,
                snapshot_count
            FROM ride_hourly_stats
            WHERE ride_id = :ride_id
                AND DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')) >= :start_date
                AND DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')) <= :end_date
            ORDER BY hour_start_utc
        """)

    result = conn.execute(query, {
        "ride_id": ride_id,
        "start_date": start_date,
        "end_date": end_date
    })

    # Convert Decimal values to float for JSON serialization
    timeseries = []
    for row in result:
        row_dict = dict(row._mapping)
        timeseries.append({
            "hour_start_utc": row_dict["hour_start_utc"],
            "avg_wait_time_minutes": float(row_dict["avg_wait_time_minutes"]) if row_dict["avg_wait_time_minutes"] is not None else None,
            "uptime_percentage": float(row_dict["uptime_percentage"]) if row_dict["uptime_percentage"] is not None else None,
            "status": row_dict["status"],
            "snapshot_count": int(row_dict["snapshot_count"]) if row_dict["snapshot_count"] is not None else 0
        })
    return timeseries


def _get_ride_summary_stats(conn, ride_id, start_date, end_date, is_today, period):
    """
    Get summary statistics for the ride.

    Returns:
    - total_downtime_hours: Total hours the ride was down
    - uptime_percentage: Overall uptime percentage
    - avg_wait_time: Average wait time across the period
    - total_operating_hours: Total hours ride was operating
    """
    query = text("""
        SELECT
            SUM(downtime_hours) as total_downtime_hours,
            AVG(uptime_percentage) as uptime_percentage,
            AVG(avg_wait_time_minutes) as avg_wait_time,
            SUM(CASE WHEN operating_snapshots > 0 THEN 1 ELSE 0 END) as total_operating_hours,
            COUNT(*) as total_hours
        FROM ride_hourly_stats
        WHERE ride_id = :ride_id
            AND DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')) >= :start_date
            AND DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')) <= :end_date
            AND ride_operated = 1
    """)

    result = conn.execute(query, {
        "ride_id": ride_id,
        "start_date": start_date,
        "end_date": end_date
    })

    row = result.fetchone()
    if row:
        row_dict = dict(row._mapping)
        # Convert Decimal values to float for JSON serialization
        return {
            "total_downtime_hours": float(row_dict["total_downtime_hours"]) if row_dict["total_downtime_hours"] is not None else None,
            "uptime_percentage": float(row_dict["uptime_percentage"]) if row_dict["uptime_percentage"] is not None else None,
            "avg_wait_time": float(row_dict["avg_wait_time"]) if row_dict["avg_wait_time"] is not None else None,
            "total_operating_hours": int(row_dict["total_operating_hours"]) if row_dict["total_operating_hours"] is not None else None,
            "total_hours": int(row_dict["total_hours"])
        }
    return {
        "total_downtime_hours": None,
        "uptime_percentage": None,
        "avg_wait_time": None,
        "total_operating_hours": None,
        "total_hours": 0
    }


def _get_ride_downtime_events(conn, ride_id, start_date, end_date, is_today, period):
    """
    Get downtime events for the ride (for downtime events table).

    A downtime event is a contiguous period where the ride was down.
    Returns:
    - start_time: When the downtime began
    - end_time: When the downtime ended (or NULL if ongoing)
    - duration_hours: How long the downtime lasted
    """
    # For now, we'll identify downtime events as hours with down_snapshots > 0
    # In the future, this could be enhanced to merge contiguous hours into single events
    query = text("""
        SELECT
            hour_start_utc as start_time,
            DATE_ADD(hour_start_utc, INTERVAL 1 HOUR) as end_time,
            downtime_hours as duration_hours
        FROM ride_hourly_stats
        WHERE ride_id = :ride_id
            AND DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')) >= :start_date
            AND DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')) <= :end_date
            AND down_snapshots > 0
        ORDER BY hour_start_utc DESC
    """)

    result = conn.execute(query, {
        "ride_id": ride_id,
        "start_date": start_date,
        "end_date": end_date
    })

    # Convert Decimal values to float for JSON serialization
    events = []
    for row in result:
        row_dict = dict(row._mapping)
        events.append({
            "start_time": row_dict["start_time"],
            "end_time": row_dict["end_time"],
            "duration_hours": float(row_dict["duration_hours"]) if row_dict["duration_hours"] is not None else None
        })
    return events


def _get_ride_hourly_breakdown(conn, ride_id, start_date, end_date, is_today, period):
    """
    Get breakdown data for the ride (for breakdown table).

    For TODAY/YESTERDAY/LAST_WEEK: Returns hourly breakdown
    For LAST_MONTH: Returns daily breakdown (matches chart granularity)

    Returns detailed stats:
    - hour_start_utc or date: Time period
    - avg_wait_time_minutes: Average wait time
    - operating_snapshots/operating_hours: Operating time
    - down_snapshots/down_hours: Down time
    - downtime_hours: Hours of downtime
    - uptime_percentage: Uptime percentage
    """
    # Use daily aggregation only for monthly view (too many hourly rows for a month)
    if period in ['last_month']:
        query = text("""
            SELECT
                DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')) as date,
                AVG(avg_wait_time_minutes) as avg_wait_time_minutes,
                SUM(CASE WHEN uptime_percentage >= 50 THEN 1 ELSE 0 END) as operating_hours,
                SUM(CASE WHEN uptime_percentage < 50 AND uptime_percentage > 0 THEN 1 ELSE 0 END) as down_hours,
                SUM(downtime_hours) as downtime_hours,
                AVG(uptime_percentage) as uptime_percentage,
                SUM(snapshot_count) as snapshot_count
            FROM ride_hourly_stats
            WHERE ride_id = :ride_id
                AND DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')) >= :start_date
                AND DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')) <= :end_date
            GROUP BY DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles'))
            ORDER BY date DESC
        """)

        result = conn.execute(query, {
            "ride_id": ride_id,
            "start_date": start_date,
            "end_date": end_date
        })

        # Convert to daily format
        breakdown = []
        for row in result:
            row_dict = dict(row._mapping)
            breakdown.append({
                "date": row_dict["date"].strftime('%Y-%m-%d'),
                "avg_wait_time_minutes": float(row_dict["avg_wait_time_minutes"]) if row_dict["avg_wait_time_minutes"] is not None else None,
                "operating_hours": int(row_dict["operating_hours"]) if row_dict["operating_hours"] is not None else 0,
                "down_hours": int(row_dict["down_hours"]) if row_dict["down_hours"] is not None else 0,
                "downtime_hours": float(row_dict["downtime_hours"]) if row_dict["downtime_hours"] is not None else None,
                "uptime_percentage": float(row_dict["uptime_percentage"]) if row_dict["uptime_percentage"] is not None else None,
                "snapshot_count": int(row_dict["snapshot_count"]) if row_dict["snapshot_count"] is not None else 0
            })
        return breakdown

    # Hourly breakdown for TODAY/YESTERDAY
    query = text("""
        SELECT
            hour_start_utc,
            avg_wait_time_minutes,
            operating_snapshots,
            down_snapshots,
            downtime_hours,
            uptime_percentage,
            snapshot_count
        FROM ride_hourly_stats
        WHERE ride_id = :ride_id
            AND DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')) >= :start_date
            AND DATE(CONVERT_TZ(hour_start_utc, 'UTC', 'America/Los_Angeles')) <= :end_date
        ORDER BY hour_start_utc DESC
    """)

    result = conn.execute(query, {
        "ride_id": ride_id,
        "start_date": start_date,
        "end_date": end_date
    })

    # Convert Decimal values to float for JSON serialization
    breakdown = []
    for row in result:
        row_dict = dict(row._mapping)
        breakdown.append({
            "hour_start_utc": row_dict["hour_start_utc"],
            "avg_wait_time_minutes": float(row_dict["avg_wait_time_minutes"]) if row_dict["avg_wait_time_minutes"] is not None else None,
            "operating_snapshots": int(row_dict["operating_snapshots"]) if row_dict["operating_snapshots"] is not None else 0,
            "down_snapshots": int(row_dict["down_snapshots"]) if row_dict["down_snapshots"] is not None else 0,
            "downtime_hours": float(row_dict["downtime_hours"]) if row_dict["downtime_hours"] is not None else None,
            "uptime_percentage": float(row_dict["uptime_percentage"]) if row_dict["uptime_percentage"] is not None else None,
            "snapshot_count": int(row_dict["snapshot_count"]) if row_dict["snapshot_count"] is not None else 0
        })
    return breakdown
