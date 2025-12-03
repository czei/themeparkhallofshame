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
from typing import Dict, Any, List
from datetime import date, datetime

from database.connection import get_db_connection
from database.repositories.ride_repository import RideRepository
from database.repositories.stats_repository import StatsRepository
from utils.cache import get_query_cache, generate_cache_key

# New query imports - each file handles one specific data source
from database.queries.live import LiveRideRankingsQuery, StatusSummaryQuery
from database.queries.rankings import RideDowntimeRankingsQuery, RideWaitTimeRankingsQuery
from database.queries.today import TodayRideRankingsQuery, TodayRideWaitTimesQuery
from database.queries.yesterday import YesterdayRideRankingsQuery, YesterdayRideWaitTimesQuery

from utils.logger import logger
from utils.timezone import get_today_pacific

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
            if period == 'live':
                # LIVE: Try pre-aggregated cache first (instant ~10ms)
                # Falls back to raw query (~7s) if cache is empty/stale
                rankings = stats_repo.get_ride_live_rankings_cached(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit,
                    sort_by=sort_by
                )

                # If cache miss, fall back to computing from raw snapshots
                if not rankings:
                    logger.warning(f"Ride live rankings cache miss, falling back to raw query")
                    rankings = stats_repo.get_ride_live_downtime_rankings(
                        filter_disney_universal=filter_disney_universal,
                        limit=limit,
                        sort_by=sort_by
                    )
            elif period == 'today':
                # TODAY: Cumulative data from midnight Pacific to now
                query = TodayRideRankingsQuery(conn)
                rankings = query.get_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
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
