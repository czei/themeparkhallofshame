"""
Theme Park Downtime Tracker - Rides API Routes
Endpoints for ride-level downtime rankings and wait times.
"""

from flask import Blueprint, request, jsonify
from typing import Dict, Any, List
from datetime import date, datetime

from database.connection import get_db_connection
from database.repositories.ride_repository import RideRepository
from database.repositories.stats_repository import StatsRepository
from utils.logger import logger

rides_bp = Blueprint('rides', __name__)


@rides_bp.route('/rides/downtime', methods=['GET'])
def get_ride_downtime_rankings():
    """
    Get ride downtime rankings for specified time period.

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
            stats_repo = StatsRepository(conn)

            # Get ride rankings based on period
            if period == 'today':
                rankings = stats_repo.get_ride_daily_rankings(
                    stat_date=date.today(),
                    filter_disney_universal=(filter_type == 'disney-universal'),
                    limit=limit
                )
            elif period == '7days':
                rankings = stats_repo.get_ride_weekly_rankings(
                    year=datetime.now().year,
                    week_number=datetime.now().isocalendar()[1],
                    filter_disney_universal=(filter_type == 'disney-universal'),
                    limit=limit
                )
            else:  # 30days
                rankings = stats_repo.get_ride_monthly_rankings(
                    year=datetime.now().year,
                    month=datetime.now().month,
                    filter_disney_universal=(filter_type == 'disney-universal'),
                    limit=limit
                )

            # Add Queue-Times.com URLs and rank to rankings
            rankings_with_urls = []
            for rank_idx, ride in enumerate(rankings, start=1):
                ride_dict = dict(ride)
                ride_dict['rank'] = rank_idx
                ride_dict['queue_times_url'] = f"https://queue-times.com/rides/{ride_dict['ride_id']}"
                rankings_with_urls.append(ride_dict)

            # Build response
            response = {
                "success": True,
                "period": period,
                "filter": filter_type,
                "data": rankings_with_urls,
                "attribution": {
                    "data_source": "Queue-Times.com",
                    "url": "https://queue-times.com"
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
