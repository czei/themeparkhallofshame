"""
Trends API Routes
GET /api/trends - Performance trends showing parks/rides with ≥5% uptime changes
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from database.connection import get_db_connection
from database.repositories.stats_repository import StatsRepository
from utils.logger import logger
from utils.timezone import get_today_pacific

# Create Blueprint
trends_bp = Blueprint('trends', __name__)


@trends_bp.route('/trends', methods=['GET'])
def get_trends():
    """
    GET /api/trends

    Returns parks/rides showing ≥5% uptime changes comparing current period to previous period.

    Query Parameters:
        - period: today | 7days | 30days (default: 7days)
        - category: parks-improving | parks-declining | rides-improving | rides-declining (required)
        - filter: disney-universal | all-parks (default: all-parks)
        - limit: max results (default: 50, max: 100)

    Returns:
        JSON response with trend data for the specified category
    """
    try:
        # Parse query parameters
        period = request.args.get('period', '7days')
        category = request.args.get('category')
        park_filter = request.args.get('filter', 'all-parks')
        limit = int(request.args.get('limit', 50))

        # Validate required parameters
        if not category:
            return jsonify({
                "success": False,
                "error": "Missing required parameter: category"
            }), 400

        # Validate parameter values
        valid_periods = ['today', '7days', '30days']
        valid_categories = ['parks-improving', 'parks-declining', 'rides-improving', 'rides-declining']
        valid_filters = ['disney-universal', 'all-parks']

        if period not in valid_periods:
            return jsonify({
                "success": False,
                "error": f"Invalid period. Must be one of: {', '.join(valid_periods)}"
            }), 400

        if category not in valid_categories:
            return jsonify({
                "success": False,
                "error": f"Invalid category. Must be one of: {', '.join(valid_categories)}"
            }), 400

        if park_filter not in valid_filters:
            return jsonify({
                "success": False,
                "error": f"Invalid filter. Must be one of: {', '.join(valid_filters)}"
            }), 400

        if limit < 1 or limit > 100:
            return jsonify({
                "success": False,
                "error": "Limit must be between 1 and 100"
            }), 400

        # Calculate period dates
        period_info = _calculate_period_dates(period)

        # Get database connection using context manager
        with get_db_connection() as conn:
            stats_repo = StatsRepository(conn)

            # Get trends based on category
            if category == 'parks-improving':
                results = stats_repo.get_parks_improving(
                    period=period,
                    park_filter=park_filter,
                    limit=limit
                )
            elif category == 'parks-declining':
                results = stats_repo.get_parks_declining(
                    period=period,
                    park_filter=park_filter,
                    limit=limit
                )
            elif category == 'rides-improving':
                results = stats_repo.get_rides_improving(
                    period=period,
                    park_filter=park_filter,
                    limit=limit
                )
            elif category == 'rides-declining':
                results = stats_repo.get_rides_declining(
                    period=period,
                    park_filter=park_filter,
                    limit=limit
                )

        # Build response
        response = {
            "success": True,
            "period": period,
            "category": category,
            "filter": park_filter,
            "comparison": {
                "current_period": period_info['current_period_label'],
                "previous_period": period_info['previous_period_label']
            },
            "count": len(results),
            "attribution": "Data powered by ThemeParks.wiki - https://themeparks.wiki",
            "timestamp": datetime.utcnow().isoformat() + 'Z'
        }

        # Add results based on category type
        if 'parks-' in category:
            response['parks'] = results
        else:
            response['rides'] = results

        return jsonify(response), 200

    except ValueError as e:
        logger.error(f"Validation error in get_trends: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

    except Exception as e:
        logger.error(f"Error in get_trends: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


def _calculate_period_dates(period: str) -> Dict[str, str]:
    """
    Calculate current and previous period date ranges.

    Args:
        period: 'today', '7days', or '30days'

    Returns:
        Dict with current_period_label and previous_period_label
    """
    today = get_today_pacific()  # Pacific Time for US parks

    if period == 'today':
        current_start = today
        current_end = today
        previous_start = today - timedelta(days=1)
        previous_end = today - timedelta(days=1)

    elif period == '7days':
        current_end = today
        current_start = today - timedelta(days=6)  # Last 7 days including today
        previous_end = current_start - timedelta(days=1)
        previous_start = previous_end - timedelta(days=6)

    elif period == '30days':
        current_end = today
        current_start = today - timedelta(days=29)  # Last 30 days including today
        previous_end = current_start - timedelta(days=1)
        previous_start = previous_end - timedelta(days=29)

    return {
        'current_period_label': f"{current_start} to {current_end}",
        'previous_period_label': f"{previous_start} to {previous_end}",
        'current_start': current_start,
        'current_end': current_end,
        'previous_start': previous_start,
        'previous_end': previous_end
    }
