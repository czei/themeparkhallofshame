"""
Theme Park Downtime Tracker - Rides API Routes
===============================================

Endpoints for ride-level downtime rankings, wait times, and live status.

Query File Mapping
------------------
GET /live/status-summary             → database/queries/live/status_summary.py
GET /rides/downtime?period=today     → database/queries/live/live_ride_rankings.py
GET /rides/downtime?period=7days     → database/queries/rankings/ride_downtime_rankings.py
GET /rides/downtime?period=30days    → database/queries/rankings/ride_downtime_rankings.py
GET /rides/waittimes                 → database/queries/rankings/ride_wait_time_rankings.py
"""

from flask import Blueprint, request, jsonify
from typing import Dict, Any, List
from datetime import date, datetime

from database.connection import get_db_connection
from database.repositories.ride_repository import RideRepository
from database.repositories.stats_repository import StatsRepository

# New query imports - each file handles one specific data source
from database.queries.live import LiveRideRankingsQuery, StatusSummaryQuery
from database.queries.rankings import RideDowntimeRankingsQuery, RideWaitTimeRankingsQuery

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

    Performance: <50ms
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

            logger.info(f"Live status summary requested: filter={filter_type}, park_id={park_id}")

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
    - period=today: database/queries/live/live_ride_rankings.py
      Uses real-time snapshot data from ride_status_snapshots

    - period=7days/30days: database/queries/rankings/ride_downtime_rankings.py
      Uses pre-aggregated data from ride_weekly_stats/ride_monthly_stats

    Query Parameters:
        period (str): Time period - 'today', '7days', '30days' (default: '7days')
        filter (str): Park filter - 'disney-universal', 'all-parks' (default: 'all-parks')
        limit (int): Maximum results (default: 100, max: 200)

    Returns:
        JSON response with ride rankings including current status and trends

    Performance: <100ms for all periods
    """
    # Parse query parameters
    period = request.args.get('period', '7days')
    filter_type = request.args.get('filter', 'all-parks')
    limit = min(int(request.args.get('limit', 100)), 200)

    # Validate period
    if period not in ['today', '7days', '30days']:
        return jsonify({
            "success": False,
            "error": "Invalid period. Must be 'today', '7days', or '30days'"
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

            # Route to appropriate query class based on period
            if period == 'today':
                # LIVE data from snapshots
                # See: database/queries/live/live_ride_rankings.py
                query = LiveRideRankingsQuery(conn)
                rankings = query.get_rankings(
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            else:
                # Historical data from aggregated stats
                # See: database/queries/rankings/ride_downtime_rankings.py
                query = RideDowntimeRankingsQuery(conn)
                if period == '7days':
                    rankings = query.get_weekly(
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                else:  # 30days
                    rankings = query.get_monthly(
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
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

    Query File Used:
    ----------------
    database/queries/rankings/ride_wait_time_rankings.py

    Query Parameters:
        period (str): Time period - 'today', '7days', '30days' (default: 'today')
        filter (str): Park filter - 'disney-universal', 'all-parks' (default: 'all-parks')
        limit (int): Maximum results (default: 100, max: 200)

    Returns:
        JSON response with wait times sorted by longest average waits descending

    Performance: <100ms for all periods
    """
    # Parse query parameters
    period = request.args.get('period', 'today')
    filter_type = request.args.get('filter', 'all-parks')
    limit = min(int(request.args.get('limit', 100)), 200)

    # Validate period
    if period not in ['today', '7days', '30days']:
        return jsonify({
            "success": False,
            "error": "Invalid period. Must be 'today', '7days', or '30days'"
        }), 400

    # Validate filter
    if filter_type not in ['disney-universal', 'all-parks']:
        return jsonify({
            "success": False,
            "error": "Invalid filter. Must be 'disney-universal' or 'all-parks'"
        }), 400

    try:
        with get_db_connection() as conn:
            # See: database/queries/rankings/ride_wait_time_rankings.py
            query = RideWaitTimeRankingsQuery(conn)
            wait_times = query.get_by_period(
                period=period,
                filter_disney_universal=(filter_type == 'disney-universal'),
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

            return jsonify(response), 200

    except Exception as e:
        logger.error(f"Error fetching wait times: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500
