"""
Theme Park Downtime Tracker - Parks API Routes
===============================================

Endpoints for park-level downtime rankings and statistics.

Query File Mapping
------------------
GET /parks/downtime?period=live     → database/queries/live/live_park_rankings.py (instantaneous)
GET /parks/downtime?period=today    → database/queries/today/today_park_rankings.py (cumulative)
GET /parks/downtime?period=7days    → database/queries/rankings/park_downtime_rankings.py
GET /parks/downtime?period=30days   → database/queries/rankings/park_downtime_rankings.py
GET /parks/waittimes?period=live    → StatsRepository.get_park_live_wait_time_rankings()
GET /parks/waittimes?period=today   → database/queries/today/today_park_wait_times.py (cumulative)
GET /parks/waittimes?period=7days   → database/queries/rankings/park_wait_time_rankings.py
GET /parks/waittimes?period=30days  → database/queries/rankings/park_wait_time_rankings.py
GET /parks/<id>/details             → (uses multiple repositories)
"""

from flask import Blueprint, request, jsonify
from typing import Dict, Any, List
from datetime import date, datetime

from database.connection import get_db_connection
from database.repositories.park_repository import ParkRepository
from database.repositories.stats_repository import StatsRepository

# New query imports - each file handles one specific data source
from database.queries.live import LiveParkRankingsQuery
from database.queries.rankings import ParkDowntimeRankingsQuery, ParkWaitTimeRankingsQuery
from database.queries.today import TodayParkRankingsQuery, TodayParkWaitTimesQuery

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
        sort_by (str): Sort column - 'shame_score', 'total_downtime_hours', 'uptime_percentage', 'rides_down' (default: 'shame_score')

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
    if period not in ['live', 'today', '7days', '30days']:
        return jsonify({
            "success": False,
            "error": "Invalid period. Must be 'live', 'today', '7days', or '30days'"
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
        with get_db_connection() as conn:
            filter_disney_universal = (filter_type == 'disney-universal')
            stats_repo = StatsRepository(conn)

            # Route to appropriate query based on period
            if period == 'live':
                # LIVE data - instantaneous snapshot (rides down RIGHT NOW)
                # Uses stats_repo for optimized raw SQL query (not SQLAlchemy ORM)
                rankings = stats_repo.get_park_live_downtime_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit,
                    sort_by=sort_by
                )
            elif period == 'today':
                # TODAY data - cumulative from midnight Pacific to now
                # See: database/queries/today/today_park_rankings.py
                query = TodayParkRankingsQuery(conn)
                rankings = query.get_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit,
                    sort_by=sort_by
                )
            else:
                # Historical data from aggregated stats
                # See: database/queries/rankings/park_downtime_rankings.py
                query = ParkDowntimeRankingsQuery(conn)
                if period == '7days':
                    rankings = query.get_weekly(
                        filter_disney_universal=filter_disney_universal,
                        limit=limit,
                        sort_by=sort_by
                    )
                else:  # 30days
                    rankings = query.get_monthly(
                        filter_disney_universal=filter_disney_universal,
                        limit=limit,
                        sort_by=sort_by
                    )
            aggregate_stats = stats_repo.get_aggregate_park_stats(
                period=period,
                filter_disney_universal=filter_disney_universal
            )

            # Add Queue-Times.com URLs to rankings
            rankings_with_urls = []
            for rank_idx, park in enumerate(rankings, start=1):
                park_dict = dict(park) if hasattr(park, '_mapping') else dict(park)
                park_dict['rank'] = rank_idx
                if 'queue_times_id' in park_dict:
                    park_dict['queue_times_url'] = f"https://queue-times.com/parks/{park_dict['queue_times_id']}"
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

    - period=7days/30days: database/queries/rankings/park_wait_time_rankings.py
      Uses pre-aggregated data from park_daily_stats

    Query Parameters:
        period (str): Time period - 'live', 'today', '7days', '30days' (default: 'live')
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
    if period not in ['live', 'today', '7days', '30days']:
        return jsonify({
            "success": False,
            "error": "Invalid period. Must be 'live', 'today', '7days', or '30days'"
        }), 400

    # Validate filter
    if filter_type not in ['disney-universal', 'all-parks']:
        return jsonify({
            "success": False,
            "error": "Invalid filter. Must be 'disney-universal' or 'all-parks'"
        }), 400

    try:
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
            else:
                # Historical data from aggregated stats
                # See: database/queries/rankings/park_wait_time_rankings.py
                query = ParkWaitTimeRankingsQuery(conn)
                wait_times = query.get_by_period(
                    period=period,
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )

            # Add Queue-Times.com URLs and rank to wait times
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

            return jsonify(response), 200

    except Exception as e:
        logger.error(f"Error fetching park wait times: {e}", exc_info=True)
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

    Returns:
        JSON response with park details, tier distribution, and operating hours

    Performance: <100ms
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

            # Get shame score breakdown (rides currently down with tier weights)
            shame_breakdown = stats_repo.get_park_shame_breakdown(park_id)

            # Build response
            response = {
                "success": True,
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
                "attribution": {
                    "data_source": "ThemeParks.wiki",
                    "url": "https://themeparks.wiki"
                }
            }

            logger.info(f"Park details requested: park_id={park_id}")

            return jsonify(response), 200

    except Exception as e:
        logger.error(f"Error fetching park details for park {park_id}: {e}")
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500
