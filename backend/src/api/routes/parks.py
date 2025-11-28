"""
Theme Park Downtime Tracker - Parks API Routes
Endpoints for park-level downtime rankings and statistics.
"""

from flask import Blueprint, request, jsonify
from typing import Dict, Any, List
from datetime import date, datetime

from database.connection import get_db_connection
from database.repositories.park_repository import ParkRepository
from database.repositories.stats_repository import StatsRepository
from utils.logger import logger
from utils.timezone import get_today_pacific

parks_bp = Blueprint('parks', __name__)


@parks_bp.route('/parks/downtime', methods=['GET'])
def get_park_downtime_rankings():
    """
    Get park downtime rankings for specified time period.

    Query Parameters:
        period (str): Time period - 'today', '7days', '30days' (default: 'today')
        filter (str): Park filter - 'disney-universal', 'all-parks' (default: 'all-parks')
        limit (int): Maximum results (default: 50, max: 100)
        weighted (bool): Use weighted scoring by ride tier (default: false)

    Returns:
        JSON response with park rankings and aggregate statistics

    Performance: <50ms for daily, <100ms for weekly/monthly
    """
    # Parse query parameters
    period = request.args.get('period', 'today')
    filter_type = request.args.get('filter', 'all-parks')
    limit = min(int(request.args.get('limit', 50)), 100)
    weighted = request.args.get('weighted', 'false').lower() == 'true'

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
            stats_repo = StatsRepository(conn)

            # Get park rankings based on period
            if period == 'today':
                # Use LIVE snapshot data for "today" - computed up to the minute
                rankings = stats_repo.get_park_live_downtime_rankings(
                    filter_disney_universal=(filter_type == 'disney-universal'),
                    limit=limit
                )
            elif period == '7days':
                rankings = stats_repo.get_park_weekly_rankings(
                    year=datetime.now().year,
                    week_number=datetime.now().isocalendar()[1],
                    filter_disney_universal=(filter_type == 'disney-universal'),
                    limit=limit,
                    weighted=weighted
                )
            else:  # 30days
                rankings = stats_repo.get_park_monthly_rankings(
                    year=datetime.now().year,
                    month=datetime.now().month,
                    filter_disney_universal=(filter_type == 'disney-universal'),
                    limit=limit,
                    weighted=weighted
                )

            # Get aggregate statistics
            aggregate_stats = stats_repo.get_aggregate_park_stats(
                period=period,
                filter_disney_universal=(filter_type == 'disney-universal')
            )

            # Add Queue-Times.com URLs to rankings
            rankings_with_urls = []
            for rank_idx, park in enumerate(rankings, start=1):
                park_dict = dict(park)
                park_dict['rank'] = rank_idx
                park_dict['queue_times_url'] = f"https://queue-times.com/parks/{park_dict['queue_times_id']}"
                rankings_with_urls.append(park_dict)

            # Build response
            response = {
                "success": True,
                "period": period,
                "filter": filter_type,
                "weighted": weighted,
                "aggregate_stats": aggregate_stats,
                "data": rankings_with_urls,
                "attribution": {
                    "data_source": "Queue-Times.com",
                    "url": "https://queue-times.com"
                }
            }

            logger.info(f"Park rankings requested: period={period}, filter={filter_type}, weighted={weighted}, results={len(rankings_with_urls)}")

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

    Query Parameters:
        period (str): Time period - 'today', '7days', '30days' (default: 'today')
        filter (str): Park filter - 'disney-universal', 'all-parks' (default: 'all-parks')
        limit (int): Maximum results (default: 50, max: 100)

    Returns:
        JSON response with park wait time rankings

    Performance: <100ms for all periods
    """
    # Parse query parameters
    period = request.args.get('period', 'today')
    filter_type = request.args.get('filter', 'all-parks')
    limit = min(int(request.args.get('limit', 50)), 100)

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
            stats_repo = StatsRepository(conn)

            # Get park wait time rankings
            wait_times = stats_repo.get_park_wait_times_by_period(
                period=period,
                filter_disney_universal=(filter_type == 'disney-universal'),
                limit=limit
            )

            # Add Queue-Times.com URLs and rank to wait times
            wait_times_with_urls = []
            for rank_idx, park in enumerate(wait_times, start=1):
                park_dict = dict(park)
                park_dict['rank'] = rank_idx
                park_dict['queue_times_url'] = f"https://queue-times.com/parks/{park_dict['queue_times_id']}"
                wait_times_with_urls.append(park_dict)

            # Build response
            response = {
                "success": True,
                "period": period,
                "filter": filter_type,
                "data": wait_times_with_urls,
                "attribution": {
                    "data_source": "Queue-Times.com",
                    "url": "https://queue-times.com"
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
                "attribution": {
                    "data_source": "Queue-Times.com",
                    "url": "https://queue-times.com"
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
