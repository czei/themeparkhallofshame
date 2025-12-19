"""
Theme Park Downtime Tracker - Parks API Routes
===============================================

Endpoints for park-level downtime rankings and statistics.

Query File Mapping
------------------
GET /parks/downtime?period=live       → database/queries/live/fast_live_park_rankings.py (instantaneous via cache table)
GET /parks/downtime?period=today      → database/queries/today/today_park_rankings.py (cumulative today)
GET /parks/downtime?period=yesterday  → StatsRepository.get_park_daily_rankings(stat_date=yesterday)
GET /parks/downtime?period=last_week  → database/queries/rankings/park_downtime_rankings.py
GET /parks/downtime?period=last_month → database/queries/rankings/park_downtime_rankings.py
GET /parks/waittimes?period=live      → StatsRepository.get_park_live_wait_time_rankings()
GET /parks/waittimes?period=today     → database/queries/today/today_park_wait_times.py (cumulative)
GET /parks/waittimes?period=yesterday → database/queries/yesterday/yesterday_park_wait_times.py (full prev day)
GET /parks/waittimes?period=last_week   → database/queries/rankings/park_wait_time_rankings.py
GET /parks/waittimes?period=last_month  → database/queries/rankings/park_wait_time_rankings.py
GET /parks/<id>/details               → (uses multiple repositories)
"""

from flask import Blueprint, request, jsonify
from datetime import timedelta
from decimal import Decimal

from database.connection import get_db_connection
from database.repositories.park_repository import ParkRepository
from database.repositories.stats_repository import StatsRepository
from utils.cache import get_query_cache, generate_cache_key

# New query imports - each file handles one specific data source
from database.queries.rankings import ParkDowntimeRankingsQuery, ParkWaitTimeRankingsQuery
from database.queries.today import TodayParkWaitTimesQuery, TodayParkRankingsQuery
from database.queries.yesterday import YesterdayParkWaitTimesQuery, YesterdayParkRankingsQuery
from database.queries.charts import ParkShameHistoryQuery, ParkRidesComparisonQuery
from database.queries.live.fast_live_park_rankings import FastLiveParkRankingsQuery
from database.calculators.shame_score import ShameScoreCalculator

from utils.logger import logger
from utils.timezone import get_today_pacific

parks_bp = Blueprint('parks', __name__)


@parks_bp.route('/parks/downtime', methods=['GET'])
def get_park_downtime_rankings():
    """
    Get park downtime rankings for specified time period.

    Query Files Used:
    -----------------
    - period=live: database/queries/live/live_park_rankings.py
      Uses real-time snapshot data (instantaneous - rides down RIGHT NOW)

    - period=today: database/queries/today/today_park_rankings.py
      Uses snapshot data aggregated from midnight Pacific to now (cumulative)

    - period=7days/30days: database/queries/rankings/park_downtime_rankings.py
      Uses pre-aggregated data from park_weekly_stats/park_monthly_stats

    Query Parameters:
        period (str): Time period - 'live', 'today', '7days', '30days' (default: 'live')
        filter (str): Park filter - 'disney-universal', 'all-parks' (default: 'all-parks')
        limit (int): Maximum results (default: 50, max: 100)
        weighted (bool): Use weighted scoring by ride tier (default: false)
        sort_by (str): Sort column - shame_score, total_downtime_hours, uptime_percentage,
            rides_down (default: shame_score)

    Returns:
        JSON response with park rankings and aggregate statistics

    Performance: <50ms for daily, <100ms for weekly/monthly
    """
    # Parse query parameters
    period = request.args.get('period', 'live')
    filter_type = request.args.get('filter', 'all-parks')
    limit = min(int(request.args.get('limit', 50)), 100)
    weighted = request.args.get('weighted', 'false').lower() == 'true'
    sort_by = request.args.get('sort_by', 'shame_score')

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
    valid_sort_options = ['shame_score', 'total_downtime_hours', 'uptime_percentage', 'rides_down']
    if sort_by not in valid_sort_options:
        return jsonify({
            "success": False,
            "error": f"Invalid sort_by. Must be one of: {', '.join(valid_sort_options)}"
        }), 400

    try:
        # Generate cache key - all periods are cached (live data only updates every 5 min anyway)
        cache_key = generate_cache_key(
            "parks_downtime",
            period=period,
            filter=filter_type,
            limit=str(limit),
            sort_by=sort_by,
            weighted=str(weighted).lower()
        )
        cache = get_query_cache()
        cached_result = cache.get(cache_key)
        if cached_result is not None:
            logger.info(f"Cache HIT for park downtime: period={period}, filter={filter_type}")
            return jsonify(cached_result), 200

        with get_db_connection() as conn:
            filter_disney_universal = (filter_type == 'disney-universal')
            stats_repo = StatsRepository(conn)

            # Route to appropriate query based on period
            today_pacific = get_today_pacific()
            if period == 'live':
                # LIVE: True instantaneous data - rides down RIGHT NOW
                # Uses pre-aggregated park_live_rankings table for instant performance
                query = FastLiveParkRankingsQuery(conn)
                rankings = query.get_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit,
                    sort_by=sort_by
                )
            elif period == 'today':
                # TODAY: Use pre-aggregated hourly stats (live-updating)
                query = TodayParkRankingsQuery(conn)
                rankings = query.get_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit,
                    sort_by=sort_by
                )
            elif period == 'yesterday':
                # YESTERDAY: Use pre-aggregated hourly stats (full previous day)
                query = YesterdayParkRankingsQuery(conn)
                rankings = query.get_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit,
                    sort_by=sort_by
                )
            elif period == '7days':
                rankings = stats_repo.get_park_weekly_rankings(
                    year=today_pacific.year,
                    week_number=today_pacific.isocalendar()[1],
                    filter_disney_universal=filter_disney_universal,
                    limit=limit,
                    weighted=weighted
                )
            elif period == '30days':
                rankings = stats_repo.get_park_monthly_rankings(
                    year=today_pacific.year,
                    month=today_pacific.month,
                    filter_disney_universal=filter_disney_universal,
                    limit=limit,
                    weighted=weighted
                )
            else:
                # Historical data from aggregated stats (calendar-based periods)
                # See: database/queries/rankings/park_downtime_rankings.py
                query = ParkDowntimeRankingsQuery(conn)
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
            aggregate_period = 'last_week' if period == '7days' else ('last_month' if period == '30days' else period)
            aggregate_stats = stats_repo.get_aggregate_park_stats(
                period=aggregate_period,
                filter_disney_universal=filter_disney_universal
            )

            # Add external URLs to rankings
            rankings_with_urls = []
            for rank_idx, park in enumerate(rankings, start=1):
                park_dict = dict(park) if hasattr(park, '_mapping') else dict(park)
                park_dict['rank'] = rank_idx
                if 'queue_times_id' in park_dict:
                    park_dict['queue_times_url'] = f"https://queue-times.com/parks/{park_dict['queue_times_id']}"

                numeric_fields = {
                    'shame_score': float,
                    'total_downtime_hours': float,
                    'weighted_downtime_hours': float,
                    'rides_operating': int,
                    'rides_down': int,
                    'uptime_percentage': float,
                    'effective_park_weight': float,
                    'snapshot_count': int,
                }
                for field, caster in numeric_fields.items():
                    if field in park_dict and isinstance(park_dict[field], Decimal):
                        park_dict[field] = caster(park_dict[field])

                rankings_with_urls.append(park_dict)

            # Build response
            response = {
                "success": True,
                "period": period,
                "filter": filter_type,
                "weighted": weighted,
                "sort_by": sort_by,
                "aggregate_stats": aggregate_stats,
                "data": rankings_with_urls,
                "attribution": {
                    "data_source": "ThemeParks.wiki",
                    "url": "https://themeparks.wiki"
                }
            }

            logger.info(f"Park rankings requested: period={period}, filter={filter_type}, weighted={weighted}, sort_by={sort_by}, results={len(rankings_with_urls)}")

            # Cache the result (5-minute TTL)
            cache.set(cache_key, response)
            logger.info(f"Cache STORE for park downtime: period={period}, filter={filter_type}")

            return jsonify(response), 200

    except Exception as e:
        logger.error(f"Error fetching park rankings: {e}")
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@parks_bp.route('/parks/waittimes', methods=['GET'])
def get_park_wait_times():
    """
    Get park-level wait time rankings for specified time period.

    Query Files Used:
    -----------------
    - period=live: StatsRepository.get_park_live_wait_time_rankings()
      Uses real-time snapshot data (instantaneous current wait times)

    - period=today: database/queries/today/today_park_wait_times.py
      Uses snapshot data aggregated from midnight Pacific to now (cumulative)

    - period=last_week/last_month: database/queries/rankings/park_wait_time_rankings.py
      Uses pre-aggregated data from park_daily_stats (calendar-based periods)

    Query Parameters:
        period (str): Time period - 'live', 'today', 'last_week', 'last_month' (default: 'live')
        filter (str): Park filter - 'disney-universal', 'all-parks' (default: 'all-parks')
        limit (int): Maximum results (default: 50, max: 100)

    Returns:
        JSON response with park wait time rankings

    Performance: <100ms for all periods
    """
    # Parse query parameters
    period = request.args.get('period', 'live')
    filter_type = request.args.get('filter', 'all-parks')
    limit = min(int(request.args.get('limit', 50)), 100)

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
            "parks_waittimes",
            period=period,
            filter=filter_type,
            limit=str(limit)
        )
        cache = get_query_cache()
        cached_result = cache.get(cache_key)
        if cached_result is not None:
            logger.info(f"Cache HIT for park waittimes: period={period}, filter={filter_type}")
            return jsonify(cached_result), 200

        with get_db_connection() as conn:
            filter_disney_universal = (filter_type == 'disney-universal')

            # Route to appropriate query based on period
            if period == 'live':
                # LIVE data - instantaneous current wait times
                stats_repo = StatsRepository(conn)
                wait_times = stats_repo.get_park_live_wait_time_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            elif period == 'today':
                # TODAY data - cumulative from midnight Pacific to now
                # See: database/queries/today/today_park_wait_times.py
                query = TodayParkWaitTimesQuery(conn)
                wait_times = query.get_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            elif period == 'yesterday':
                # YESTERDAY data - full previous Pacific day
                # See: database/queries/yesterday/yesterday_park_wait_times.py
                query = YesterdayParkWaitTimesQuery(conn)
                wait_times = query.get_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            else:
                # Historical data from aggregated stats (calendar-based periods)
                # See: database/queries/rankings/park_wait_time_rankings.py
                query = ParkWaitTimeRankingsQuery(conn)
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

            # Add external URLs and rank to wait times
            wait_times_with_urls = []
            for rank_idx, park in enumerate(wait_times, start=1):
                park_dict = dict(park) if hasattr(park, '_mapping') else dict(park)
                park_dict['rank'] = rank_idx
                if 'queue_times_id' in park_dict:
                    park_dict['queue_times_url'] = f"https://queue-times.com/parks/{park_dict['queue_times_id']}"
                wait_times_with_urls.append(park_dict)

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

            logger.info(f"Park wait times requested: period={period}, filter={filter_type}, results={len(wait_times_with_urls)}")

            # Cache the result (5-minute TTL)
            cache.set(cache_key, response)
            logger.info(f"Cache STORE for park waittimes: period={period}, filter={filter_type}")

            return jsonify(response), 200

    except Exception as e:
        logger.error(f"Error fetching park wait times: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@parks_bp.route('/parks/<int:park_id>/rides', methods=['GET'])
def get_park_rides(park_id: int):
    """
    Get all rides for a park, separated into active (included in shame score)
    and excluded (not operated in 7+ days).

    This endpoint supports the Excluded Rides UI feature, which helps users
    understand why certain rides are not counted in the park's shame score.

    Path Parameters:
        park_id (int): Park ID

    Returns:
        JSON response with:
        - park_id, park_name
        - effective_park_weight: Sum of tier weights for active rides
        - total_roster_weight: Sum of tier weights for all rides
        - rides.active: List of rides included in shame score
        - rides.excluded: List of rides excluded (7+ days without operation)
    """
    try:
        with get_db_connection() as conn:
            park_repo = ParkRepository(conn)
            stats_repo = StatsRepository(conn)

            # Get park basic info
            park = park_repo.get_by_id(park_id)
            if not park:
                return jsonify({
                    "success": False,
                    "error": f"Park with ID {park_id} not found"
                }), 404

            # Get active and excluded rides
            active_rides = stats_repo.get_active_rides(park_id)
            excluded_rides = stats_repo.get_excluded_rides(park_id)

            # Calculate weights
            effective_weight = sum(r.get('tier_weight', 2) for r in active_rides)
            total_roster_weight = effective_weight + sum(r.get('tier_weight', 2) for r in excluded_rides)

            return jsonify({
                "success": True,
                "park_id": park_id,
                "park_name": park.name,
                "effective_park_weight": effective_weight,
                "total_roster_weight": total_roster_weight,
                "rides": {
                    "active": active_rides,
                    "excluded": excluded_rides
                }
            }), 200

    except Exception as e:
        logger.error(f"Error fetching park rides: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@parks_bp.route('/parks/<int:park_id>/details', methods=['GET'])
def get_park_details(park_id: int):
    """
    Get detailed information for a specific park.

    Note: This endpoint uses repositories rather than query classes
    because it aggregates data from multiple sources.

    Path Parameters:
        park_id (int): Park ID

    Query Parameters:
        period (str): Time period for shame breakdown - 'live', 'today', 'last_week', 'last_month'
            - live: Shows rides currently down (instantaneous)
            - today: Average shame score from snapshots today, all rides with downtime
            - last_week: Average daily shame score for previous calendar week
            - last_month: Average daily shame score for previous calendar month

    Returns:
        JSON response with park details, tier distribution, and operating hours

    Performance: <100ms for live/today, <500ms for historical periods
    """
    # Get period parameter (defaults to 'live' for backwards compatibility)
    period = request.args.get('period', 'live')
    if period not in ('live', 'today', 'yesterday', 'last_week', 'last_month'):
        period = 'live'

    try:
        with get_db_connection() as conn:
            park_repo = ParkRepository(conn)
            stats_repo = StatsRepository(conn)

            # Get park basic info
            park = park_repo.get_by_id(park_id)
            if not park:
                return jsonify({
                    "success": False,
                    "error": f"Park {park_id} not found"
                }), 404

            # Get tier distribution
            tier_distribution = stats_repo.get_park_tier_distribution(park_id)

            # Get recent operating sessions (last 7 days)
            operating_sessions = stats_repo.get_park_operating_sessions(
                park_id=park_id,
                limit=7
            )

            # Get current ride status summary
            current_status = stats_repo.get_park_current_status(park_id)

            # Get shame score breakdown based on period
            # Each period returns data appropriate for that time range
            if period == 'today':
                shame_breakdown = stats_repo.get_park_today_shame_breakdown(park_id)
            elif period == 'yesterday':
                shame_breakdown = stats_repo.get_park_yesterday_shame_breakdown(park_id)
            elif period == 'last_week':
                shame_breakdown = stats_repo.get_park_weekly_shame_breakdown(park_id)
            elif period == 'last_month':
                shame_breakdown = stats_repo.get_park_monthly_shame_breakdown(park_id)
            else:  # live
                shame_breakdown = stats_repo.get_park_shame_breakdown(park_id)

            # Get chart data for all periods
            # - LIVE: 5-minute granularity for last 60 minutes (recent snapshots)
            # - TODAY/YESTERDAY: Hourly averages for full day
            # - LAST_WEEK/LAST_MONTH: Daily averages for period
            chart_data = None
            if period == 'live':
                # LIVE: Get recent 60 minutes of stored shame_score data at 5-minute granularity
                # Use the pre-calculated shame_score from park_activity_snapshots (calculated with 7-day hybrid logic)
                from datetime import datetime, timedelta, timezone
                from sqlalchemy import text

                now_utc = datetime.now(timezone.utc)
                start_utc = now_utc - timedelta(minutes=60)

                query = text("""
                    SELECT
                        DATE_FORMAT(DATE_SUB(recorded_at, INTERVAL 8 HOUR), '%H:%i') AS time_label,
                        shame_score
                    FROM park_activity_snapshots
                    WHERE park_id = :park_id
                        AND recorded_at >= :start_utc
                        AND recorded_at < :end_utc
                    ORDER BY recorded_at
                """)

                result = conn.execute(query, {
                    "park_id": park_id,
                    "start_utc": start_utc,
                    "end_utc": now_utc
                })

                rows = [dict(row._mapping) for row in result]

                # Build chart data
                labels = [row['time_label'] for row in rows]
                data = [float(row['shame_score']) if row['shame_score'] is not None else None for row in rows]

                chart_data = {
                    "labels": labels,
                    "data": data,
                    "granularity": "minutes"
                }

                # For LIVE, set 'current' to the last non-null value from the chart data
                # This ensures the badge matches the rightmost point on the chart
                last_value = None
                for val in reversed(data):
                    if val is not None:
                        last_value = val
                        break
                chart_data['current'] = float(last_value) if last_value is not None else 0.0
            elif period in ('today', 'yesterday'):
                chart_query = ParkShameHistoryQuery(conn)
                today = get_today_pacific()
                if period == 'today':
                    chart_data = chart_query.get_single_park_hourly(park_id, today, is_today=True)
                elif period == 'yesterday':
                    from datetime import timedelta
                    yesterday = today - timedelta(days=1)
                    chart_data = chart_query.get_single_park_hourly(park_id, yesterday, is_today=False)

                # Override chart average with breakdown's shame_score for consistency
                # The hourly chart query uses different filtering logic which can show 0
                # when rides haven't "operated" yet, but the breakdown correctly counts downtime
                if chart_data and shame_breakdown:
                    chart_data['average'] = shame_breakdown.get('shame_score', chart_data.get('average', 0))
            elif period in ('last_week', 'last_month'):
                # WEEKLY/MONTHLY: Daily averages for the period
                from utils.timezone import get_last_week_date_range, get_last_month_date_range
                chart_query = ParkShameHistoryQuery(conn)

                if period == 'last_week':
                    start_date, end_date, _ = get_last_week_date_range()
                else:  # last_month
                    start_date, end_date, _ = get_last_month_date_range()

                chart_data = chart_query.get_single_park_daily(park_id, start_date, end_date)

                # Override chart average with breakdown's shame_score for consistency
                if chart_data and shame_breakdown:
                    chart_data['average'] = shame_breakdown.get('shame_score', chart_data.get('average', 0))

            # Get excluded rides count for display in modal
            excluded_rides = stats_repo.get_excluded_rides(park_id)
            excluded_rides_count = len(excluded_rides)

            # Get active rides (operated in last 7 days) to calculate effective_park_weight
            active_rides = stats_repo.get_active_rides(park_id)
            effective_park_weight = sum(r.get('tier_weight', 2) for r in active_rides)

            # Build response
            response = {
                "success": True,
                "period": period,
                "park": {
                    "park_id": park.park_id,
                    "name": park.name,
                    "location": park.location,
                    "operator": park.operator,
                    "timezone": park.timezone,
                    "queue_times_url": park.queue_times_url
                },
                "tier_distribution": tier_distribution,
                "operating_sessions": operating_sessions,
                "current_status": current_status,
                "shame_breakdown": shame_breakdown,
                "excluded_rides_count": excluded_rides_count,  # Rides not operated in 7+ days
                "effective_park_weight": effective_park_weight,  # Sum of tier weights for active rides (7-day window)
                "chart_data": chart_data,  # Hourly shame scores for TODAY/YESTERDAY periods
                "attribution": {
                    "data_source": "ThemeParks.wiki",
                    "url": "https://themeparks.wiki"
                }
            }

            logger.info(f"Park details requested: park_id={park_id}, period={period}")

            return jsonify(response), 200

    except Exception as e:
        logger.error(f"Error fetching park details for park {park_id}: {e}")
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@parks_bp.route('/parks/<int:park_id>/rides/charts', methods=['GET'])
def get_park_rides_comparison_chart(park_id: int):
    """
    Get ride comparison chart data for a specific park.

    Returns time-series data for all rides in the park, enabling comparison
    of either downtime hours or wait times across rides over time.

    Query Parameters:
        period (str): Time period - 'today', 'yesterday', 'last_week', 'last_month'
        type (str): Chart type - 'downtime' or 'wait_times' (default: 'downtime')

    Returns:
        JSON with Chart.js compatible data:
        {
            "labels": ["9:00", "10:00", ...] or ["Dec 01", "Dec 02", ...],
            "datasets": [
                {"label": "Space Mountain", "ride_id": 123, "tier": 1, "data": [...]},
                ...
            ],
            "chart_type": "downtime" | "wait_times",
            "granularity": "hourly" | "daily"
        }
    """
    from datetime import date, timedelta
    from utils.timezone import (
        get_today_pacific,
        get_last_week_date_range,
        get_last_month_date_range,
    )

    period = request.args.get('period', 'yesterday')
    chart_type = request.args.get('type', 'downtime')

    # Validate period
    if period not in ['today', 'yesterday', 'last_week', 'last_month']:
        return jsonify({
            "success": False,
            "error": "Invalid period. Must be 'today', 'yesterday', 'last_week', or 'last_month'"
        }), 400

    # Validate chart type
    if chart_type not in ['downtime', 'wait_times']:
        return jsonify({
            "success": False,
            "error": "Invalid type. Must be 'downtime' or 'wait_times'"
        }), 400

    try:
        with get_db_connection() as conn:
            query = ParkRidesComparisonQuery(conn)

            if period == 'today':
                target_date = get_today_pacific()
                if chart_type == 'downtime':
                    chart_data = query.get_downtime_hourly(park_id, target_date)
                else:
                    chart_data = query.get_wait_times_hourly(park_id, target_date)

            elif period == 'yesterday':
                target_date = get_today_pacific() - timedelta(days=1)
                if chart_type == 'downtime':
                    chart_data = query.get_downtime_hourly(park_id, target_date)
                else:
                    chart_data = query.get_wait_times_hourly(park_id, target_date)

            elif period == 'last_week':
                start_date, end_date, _ = get_last_week_date_range()
                if chart_type == 'downtime':
                    chart_data = query.get_downtime_daily(park_id, start_date, end_date)
                else:
                    chart_data = query.get_wait_times_daily(park_id, start_date, end_date)

            elif period == 'last_month':
                start_date, end_date, _ = get_last_month_date_range()
                if chart_type == 'downtime':
                    chart_data = query.get_downtime_daily(park_id, start_date, end_date)
                else:
                    chart_data = query.get_wait_times_daily(park_id, start_date, end_date)

            logger.info(
                f"Park rides comparison chart: park_id={park_id}, period={period}, "
                f"type={chart_type}, rides={len(chart_data.get('datasets', []))}"
            )

            return jsonify({
                "success": True,
                "park_id": park_id,
                "period": period,
                **chart_data
            }), 200

    except Exception as e:
        logger.error(f"Error fetching ride comparison chart for park {park_id}: {e}")
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500
