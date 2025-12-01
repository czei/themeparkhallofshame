"""
Trends API Routes
=================

Performance trends showing parks/rides with ≥5% uptime changes.

Query File Mapping
------------------
GET /trends?category=parks-improving  → database/queries/trends/improving_parks.py
GET /trends?category=parks-declining  → database/queries/trends/declining_parks.py
GET /trends?category=rides-improving  → database/queries/trends/improving_rides.py
GET /trends?category=rides-declining  → database/queries/trends/declining_rides.py
GET /trends/chart-data?type=parks     → database/queries/charts/park_shame_history.py
GET /trends/chart-data?type=rides     → database/queries/charts/ride_downtime_history.py
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from database.connection import get_db_connection
from database.repositories.stats_repository import StatsRepository

# New query imports - each file handles one specific data source
from database.queries.trends import (
    ImprovingParksQuery,
    DecliningParksQuery,
    ImprovingRidesQuery,
    DecliningRidesQuery,
)
from database.queries.charts import (
    ParkShameHistoryQuery,
    ParkWaitTimeHistoryQuery,
    RideDowntimeHistoryQuery,
)

from utils.logger import logger
from utils.timezone import get_today_pacific

# Create Blueprint
trends_bp = Blueprint('trends', __name__)


@trends_bp.route('/trends', methods=['GET'])
def get_trends():
    """
    GET /api/trends

    Returns parks/rides showing ≥5% uptime changes comparing current period to previous period.

    Query Files Used:
    -----------------
    - parks-improving: database/queries/trends/improving_parks.py
    - parks-declining: database/queries/trends/declining_parks.py
    - rides-improving: database/queries/trends/improving_rides.py
    - rides-declining: database/queries/trends/declining_rides.py

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
        # Note: 'live' is intentionally excluded - trends require comparison between periods,
        # which doesn't make sense for instantaneous data. Frontend defaults to 'today' if 'live'.
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
        filter_disney_universal = (park_filter == 'disney-universal')

        # Get database connection using context manager
        with get_db_connection() as conn:
            # Route to appropriate query class based on category
            if category == 'parks-improving':
                # See: database/queries/trends/improving_parks.py
                query = ImprovingParksQuery(conn)
                results = query.get_improving(
                    period=period,
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            elif category == 'parks-declining':
                # See: database/queries/trends/declining_parks.py
                query = DecliningParksQuery(conn)
                results = query.get_declining(
                    period=period,
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            elif category == 'rides-improving':
                # See: database/queries/trends/improving_rides.py
                query = ImprovingRidesQuery(conn)
                results = query.get_improving(
                    period=period,
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
            elif category == 'rides-declining':
                # See: database/queries/trends/declining_rides.py
                query = DecliningRidesQuery(conn)
                results = query.get_declining(
                    period=period,
                    filter_disney_universal=filter_disney_universal,
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


@trends_bp.route('/trends/chart-data', methods=['GET'])
def get_chart_data():
    """
    GET /api/trends/chart-data

    Returns time-series data for charts showing shame score or downtime trends.

    Query Files Used:
    -----------------
    - type=parks: database/queries/charts/park_shame_history.py
    - type=rides: database/queries/charts/ride_downtime_history.py

    Query Parameters:
        - period: today | 7days | 30days (default: 7days)
          - today: Returns hourly breakdown (6am-11pm Pacific)
          - 7days/30days: Returns daily breakdown
        - type: parks | rides (default: parks)
        - filter: disney-universal | all-parks (default: disney-universal)
        - limit: max entities to return (default: 10, max: 20)

    Returns:
        JSON response with chart-ready data structure:
        {
            "success": true,
            "chart_data": {
                "labels": ["6:00", "7:00", ...] or ["Nov 23", "Nov 24", ...],
                "datasets": [
                    {"label": "Park Name", "data": [0.21, 0.18, ...]},
                    ...
                ]
            },
            "mock": false,
            "granularity": "hourly" or "daily"
        }
    """
    try:
        # Parse query parameters
        period = request.args.get('period', '7days')
        data_type = request.args.get('type', 'parks')
        park_filter = request.args.get('filter', 'disney-universal')
        limit = int(request.args.get('limit', 10))

        # Validate parameters
        # Note: 'live' is intentionally excluded - chart trends require time series data,
        # which doesn't make sense for instantaneous data. Frontend defaults to 'today' if 'live'.
        valid_periods = ['today', '7days', '30days']
        valid_types = ['parks', 'rides', 'waittimes']
        valid_filters = ['disney-universal', 'all-parks']

        if period not in valid_periods:
            return jsonify({
                "success": False,
                "error": f"Invalid period. Must be one of: {', '.join(valid_periods)}"
            }), 400

        if data_type not in valid_types:
            return jsonify({
                "success": False,
                "error": f"Invalid type. Must be one of: {', '.join(valid_types)}"
            }), 400

        if park_filter not in valid_filters:
            return jsonify({
                "success": False,
                "error": f"Invalid filter. Must be one of: {', '.join(valid_filters)}"
            }), 400

        if limit < 1 or limit > 20:
            limit = min(max(limit, 1), 20)

        today = get_today_pacific()
        is_mock = False
        granularity = 'daily'
        filter_disney_universal = (park_filter == 'disney-universal')

        # Get database connection
        with get_db_connection() as conn:
            if period == 'today':
                # Hourly data for today
                granularity = 'hourly'
                if data_type == 'parks':
                    # See: database/queries/charts/park_shame_history.py
                    query = ParkShameHistoryQuery(conn)
                    chart_data = query.get_hourly(
                        target_date=today,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                elif data_type == 'waittimes':
                    # See: database/queries/charts/park_waittime_history.py
                    query = ParkWaitTimeHistoryQuery(conn)
                    chart_data = query.get_hourly(
                        target_date=today,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                else:
                    # See: database/queries/charts/ride_downtime_history.py
                    query = RideDowntimeHistoryQuery(conn)
                    chart_data = query.get_hourly(
                        target_date=today,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )

                # Generate mock hourly data if empty
                if not chart_data or not chart_data.get('datasets') or len(chart_data.get('datasets', [])) == 0:
                    is_mock = True
                    chart_data = _generate_mock_hourly_chart_data(data_type, limit)
            else:
                # Daily data for 7days/30days
                days = 7 if period == '7days' else 30

                if data_type == 'parks':
                    # See: database/queries/charts/park_shame_history.py
                    query = ParkShameHistoryQuery(conn)
                    chart_data = query.get_daily(
                        days=days,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                elif data_type == 'waittimes':
                    # See: database/queries/charts/park_waittime_history.py
                    query = ParkWaitTimeHistoryQuery(conn)
                    chart_data = query.get_daily(
                        days=days,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                else:
                    # See: database/queries/charts/ride_downtime_history.py
                    query = RideDowntimeHistoryQuery(conn)
                    chart_data = query.get_daily(
                        days=days,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )

                # Generate mock daily data if empty
                if not chart_data or not chart_data.get('datasets') or len(chart_data.get('datasets', [])) == 0:
                    is_mock = True
                    chart_data = _generate_mock_chart_data(data_type, days, limit)

        return jsonify({
            "success": True,
            "period": period,
            "type": data_type,
            "filter": park_filter,
            "chart_data": chart_data,
            "mock": is_mock,
            "granularity": granularity,
            "attribution": "Data powered by ThemeParks.wiki - https://themeparks.wiki",
            "timestamp": datetime.utcnow().isoformat() + 'Z'
        }), 200

    except ValueError as e:
        logger.error(f"Validation error in get_chart_data: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

    except Exception as e:
        logger.error(f"Error in get_chart_data: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


def _generate_mock_chart_data(data_type: str, days: int, limit: int) -> Dict[str, Any]:
    """
    Generate mock chart data for demo/development when real data is empty.

    Args:
        data_type: 'parks', 'rides', or 'waittimes'
        days: Number of days
        limit: Number of entities

    Returns:
        Chart data structure with mock values
    """
    import random
    from datetime import date, timedelta

    today = get_today_pacific()
    labels = [(today - timedelta(days=i)).strftime('%b %d') for i in range(days - 1, -1, -1)]

    if data_type == 'parks':
        park_names = [
            "Disney Magic Kingdom", "Universal Studios Florida", "Disney Hollywood Studios",
            "Universal Islands of Adventure", "Disney EPCOT", "Disney Animal Kingdom",
            "SeaWorld Orlando", "Busch Gardens Tampa", "Universal Volcano Bay", "LEGOLAND Florida"
        ]
        datasets = []
        for i, name in enumerate(park_names[:limit]):
            # Generate realistic-looking shame scores (0.05 to 0.5 range)
            base = random.uniform(0.1, 0.3)
            data = [round(base + random.uniform(-0.1, 0.15), 2) for _ in range(days)]
            datasets.append({"label": name, "data": data})
    elif data_type == 'waittimes':
        park_names = [
            "Disney Magic Kingdom", "Universal Studios Florida", "Disney Hollywood Studios",
            "Universal Islands of Adventure", "Disney EPCOT", "Disney Animal Kingdom",
            "SeaWorld Orlando", "Busch Gardens Tampa"
        ]
        datasets = []
        for i, name in enumerate(park_names[:limit]):
            # Generate realistic average wait times (20-70 minutes range)
            base = random.uniform(30, 50)
            data = [round(max(10, base + random.uniform(-15, 20)), 0) for _ in range(days)]
            datasets.append({"label": name, "data": data})
    else:
        ride_names = [
            ("Hagrid's Motorbike Adventure", "Universal Islands of Adventure"),
            ("Tron Lightcycle Run", "Disney Magic Kingdom"),
            ("Guardians of the Galaxy", "Disney EPCOT"),
            ("VelociCoaster", "Universal Islands of Adventure"),
            ("Rise of the Resistance", "Disney Hollywood Studios"),
            ("Flight of Passage", "Disney Animal Kingdom"),
            ("Expedition Everest", "Disney Animal Kingdom"),
            ("Space Mountain", "Disney Magic Kingdom"),
            ("Test Track", "Disney EPCOT"),
            ("Mako", "SeaWorld Orlando")
        ]
        datasets = []
        for i, (ride, park) in enumerate(ride_names[:limit]):
            # Generate realistic downtime percentages (0% to 15% range)
            base = random.uniform(2, 8)
            data = [round(max(0, base + random.uniform(-3, 5)), 1) for _ in range(days)]
            datasets.append({"label": f"{ride}", "park": park, "data": data})

    return {
        "labels": labels,
        "datasets": datasets
    }


def _generate_mock_hourly_chart_data(data_type: str, limit: int) -> Dict[str, Any]:
    """
    Generate mock hourly chart data for TODAY when real data is empty.

    Args:
        data_type: 'parks', 'rides', or 'waittimes'
        limit: Number of entities

    Returns:
        Chart data structure with mock hourly values (6am-11pm)
    """
    import random

    # Hourly labels from 6am to 11pm
    labels = [f"{h}:00" for h in range(6, 24)]
    num_hours = 18  # 6am to 11pm

    if data_type == 'parks':
        park_names = [
            "Disney Magic Kingdom", "Universal Studios Florida", "Disney Hollywood Studios",
            "Universal Islands of Adventure", "Disney EPCOT", "Disney Animal Kingdom",
            "SeaWorld Orlando", "Busch Gardens Tampa", "Universal Volcano Bay", "LEGOLAND Florida"
        ]
        datasets = []
        for i, name in enumerate(park_names[:limit]):
            # Generate realistic hourly shame scores (higher mid-day due to crowds)
            data = []
            for h in range(num_hours):
                # Simulate higher issues mid-day (hours 4-10 = 10am-4pm)
                if 4 <= h <= 10:
                    base = random.uniform(15, 35)
                else:
                    base = random.uniform(5, 20)
                data.append(round(base, 1))
            datasets.append({"label": name, "data": data})
    elif data_type == 'waittimes':
        park_names = [
            "Disney Magic Kingdom", "Universal Studios Florida", "Disney Hollywood Studios",
            "Universal Islands of Adventure", "Disney EPCOT", "Disney Animal Kingdom",
            "SeaWorld Orlando", "Busch Gardens Tampa"
        ]
        datasets = []
        for i, name in enumerate(park_names[:limit]):
            # Generate realistic hourly wait times (higher mid-day due to crowds)
            data = []
            for h in range(num_hours):
                # Simulate higher wait times mid-day (hours 4-10 = 10am-4pm)
                if 4 <= h <= 10:
                    base = random.uniform(40, 70)
                else:
                    base = random.uniform(20, 45)
                data.append(round(base, 0))
            datasets.append({"label": name, "data": data})
    else:
        ride_names = [
            ("Hagrid's Motorbike Adventure", "Universal Islands of Adventure"),
            ("Tron Lightcycle Run", "Disney Magic Kingdom"),
            ("Guardians of the Galaxy", "Disney EPCOT"),
            ("VelociCoaster", "Universal Islands of Adventure"),
            ("Rise of the Resistance", "Disney Hollywood Studios"),
            ("Flight of Passage", "Disney Animal Kingdom"),
            ("Expedition Everest", "Disney Animal Kingdom"),
            ("Space Mountain", "Disney Magic Kingdom"),
            ("Test Track", "Disney EPCOT"),
            ("Mako", "SeaWorld Orlando")
        ]
        datasets = []
        for i, (ride, park) in enumerate(ride_names[:limit]):
            # Generate realistic hourly downtime percentages
            data = []
            for h in range(num_hours):
                # Random chance of downtime each hour
                if random.random() < 0.3:  # 30% chance of some downtime
                    data.append(round(random.uniform(10, 80), 1))
                else:
                    data.append(0)
            datasets.append({"label": ride, "park": park, "data": data})

    return {
        "labels": labels,
        "datasets": datasets
    }


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
