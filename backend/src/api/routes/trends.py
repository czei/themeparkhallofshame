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
GET /trends/heatmap-data?type=parks   → Reuses park_waittime_history.py + transforms to matrix
GET /trends/heatmap-data?type=rides-* → Reuses ride_*_history.py + transforms to matrix
GET /trends/longest-wait-times        → database/queries/trends/longest_wait_times.py
GET /trends/least-reliable            → database/queries/trends/least_reliable_rides.py
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
from typing import Dict, Any, List

from database.connection import get_db_connection, get_db_session

# New query imports - each file handles one specific data source
from database.queries.trends import (
    ImprovingParksQuery,
    DecliningParksQuery,
    ImprovingRidesQuery,
    DecliningRidesQuery,
    LongestWaitTimesQuery,
    LeastReliableRidesQuery,
)
from database.queries.charts import (
    ParkShameHistoryQuery,
    ParkWaitTimeHistoryQuery,
    RideDowntimeHistoryQuery,
    RideWaitTimeHistoryQuery,
)

from utils.logger import logger
from utils.timezone import get_today_pacific, get_now_pacific, get_last_week_date_range, get_last_month_date_range, PERIOD_ALIASES
from utils.cache import get_query_cache, generate_cache_key
from utils.heatmap_helpers import transform_chart_to_heatmap, validate_heatmap_period

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
        - period: today | last_week | last_month (default: last_week)
        - category: parks-improving | parks-declining | rides-improving | rides-declining (required)
        - filter: disney-universal | all-parks (default: all-parks)
        - limit: max results (default: 50, max: 100)

    Returns:
        JSON response with trend data for the specified category
    """
    try:
        # Parse query parameters
        period = request.args.get('period', 'last_week')
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
        valid_periods = ['today', 'yesterday', 'last_week', 'last_month', '7days', '30days']
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

        normalized_period = PERIOD_ALIASES.get(period, period)

        # Calculate period dates
        period_info = _calculate_period_dates(normalized_period)
        filter_disney_universal = (park_filter == 'disney-universal')

        # Get database connection using context manager
        # Note: ImprovingParksQuery uses ORM (Session), others still use Core (Connection)
        if category == 'parks-improving':
            # See: database/queries/trends/improving_parks.py (ORM)
            with get_db_session() as session:
                query = ImprovingParksQuery(session)
                results = query.get_improving(
                    period=normalized_period,
                    filter_disney_universal=filter_disney_universal,
                    limit=limit
                )
        else:
            with get_db_connection() as conn:
                # Route to appropriate query class based on category
                if category == 'parks-declining':
                    # See: database/queries/trends/declining_parks.py
                    query = DecliningParksQuery(conn)
                    results = query.get_declining(
                        period=normalized_period,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                elif category == 'rides-improving':
                    # See: database/queries/trends/improving_rides.py
                    query = ImprovingRidesQuery(conn)
                    results = query.get_improving(
                        period=normalized_period,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                elif category == 'rides-declining':
                    # See: database/queries/trends/declining_rides.py
                    query = DecliningRidesQuery(conn)
                    results = query.get_declining(
                        period=normalized_period,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )

        results = _attach_queue_times_urls(category, results)

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
        - period: today | last_week | last_month (default: last_week)
          - today: Returns hourly breakdown (6am-11pm Pacific)
          - last_week/last_month: Returns daily breakdown
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
        period = request.args.get('period', 'last_week')
        data_type = request.args.get('type', 'parks')
        park_filter = request.args.get('filter', 'disney-universal')
        limit = int(request.args.get('limit', 10))

        # Validate parameters
        # Note: 'live' is mapped to 'today' since charts need time series data
        valid_periods = ['live', 'today', 'yesterday', 'last_week', 'last_month']
        valid_types = ['parks', 'rides', 'waittimes', 'ridewaittimes']
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
            if period == 'live':
                # LIVE: 5-minute granularity for recent data (last 60 minutes)
                granularity = 'minutes'
                if data_type == 'parks':
                    # See: database/queries/charts/park_shame_history.py
                    query = ParkShameHistoryQuery(conn)
                    chart_data = query.get_live(
                        filter_disney_universal=filter_disney_universal,
                        limit=limit,
                        minutes=60
                    )
                elif data_type == 'waittimes':
                    query = ParkWaitTimeHistoryQuery(conn)
                    chart_data = query.get_live(
                        filter_disney_universal=filter_disney_universal,
                        limit=limit,
                        minutes=60
                    )
                elif data_type == 'rides':
                    query = RideDowntimeHistoryQuery(conn)
                    chart_data = query.get_live(
                        filter_disney_universal=filter_disney_universal,
                        limit=limit,
                        minutes=60
                    )
                else:  # ridewaittimes
                    query = RideWaitTimeHistoryQuery(conn)
                    chart_data = query.get_live(
                        filter_disney_universal=filter_disney_universal,
                        limit=limit,
                        minutes=60
                    )

                # Generate mock data if empty for LIVE
                if not chart_data or not chart_data.get('datasets') or len(chart_data.get('datasets', [])) == 0:
                    is_mock = True
                    chart_data = _generate_mock_live_chart_data(data_type, limit)

            elif period == 'today':
                # TODAY: Hourly data for the full day
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
                elif data_type == 'rides':
                    # See: database/queries/charts/ride_downtime_history.py
                    query = RideDowntimeHistoryQuery(conn)
                    chart_data = query.get_hourly(
                        target_date=today,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                else:  # ridewaittimes
                    # See: database/queries/charts/ride_waittime_history.py
                    query = RideWaitTimeHistoryQuery(conn)
                    chart_data = query.get_hourly(
                        target_date=today,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )

                # Generate mock hourly data if empty (for TODAY, limit to current hour)
                if not chart_data or not chart_data.get('datasets') or len(chart_data.get('datasets', [])) == 0:
                    is_mock = True
                    chart_data = _generate_mock_hourly_chart_data(data_type, limit, for_today=True)

            elif period == 'yesterday':
                # Hourly data for yesterday (similar to today, but for previous day)
                granularity = 'hourly'
                yesterday = today - timedelta(days=1)
                if data_type == 'parks':
                    query = ParkShameHistoryQuery(conn)
                    chart_data = query.get_hourly(
                        target_date=yesterday,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                elif data_type == 'waittimes':
                    query = ParkWaitTimeHistoryQuery(conn)
                    chart_data = query.get_hourly(
                        target_date=yesterday,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                elif data_type == 'rides':
                    query = RideDowntimeHistoryQuery(conn)
                    chart_data = query.get_hourly(
                        target_date=yesterday,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                else:  # ridewaittimes
                    query = RideWaitTimeHistoryQuery(conn)
                    chart_data = query.get_hourly(
                        target_date=yesterday,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )

                # Generate mock hourly data if empty (for YESTERDAY, show full day)
                if not chart_data or not chart_data.get('datasets') or len(chart_data.get('datasets', [])) == 0:
                    is_mock = True
                    chart_data = _generate_mock_hourly_chart_data(data_type, limit, for_today=False)

            else:
                # Daily data for last_week/last_month (calendar-based periods)
                if period == 'last_week':
                    start_date, end_date, _ = get_last_week_date_range()
                    days = (end_date - start_date).days + 1  # Include both start and end
                else:  # last_month
                    start_date, end_date, _ = get_last_month_date_range()
                    days = (end_date - start_date).days + 1

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
                elif data_type == 'rides':
                    # See: database/queries/charts/ride_downtime_history.py
                    query = RideDowntimeHistoryQuery(conn)
                    chart_data = query.get_daily(
                        days=days,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                else:  # ridewaittimes
                    # See: database/queries/charts/ride_waittime_history.py
                    query = RideWaitTimeHistoryQuery(conn)
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


@trends_bp.route('/trends/longest-wait-times', methods=['GET'])
def get_longest_wait_times():
    """
    GET /api/trends/longest-wait-times

    Returns top 10 parks or rides ranked by average wait time for the Awards section.

    IMPORTANT: Rankings use avg_wait_time to match the Wait Times table.
    This ensures consistency between Awards and the main rankings.

    Query Files Used:
    -----------------
    - database/queries/trends/longest_wait_times.py

    Query Parameters:
        - period: today | last_week | last_month (default: today)
        - filter: disney-universal | all-parks (default: all-parks)
        - entity: parks | rides (default: rides)
        - limit: max results (default: 10, max: 20)

    Returns:
        JSON response with top parks/rides by average wait time
    """
    try:
        # Parse query parameters
        period = request.args.get('period', 'today')
        park_filter = request.args.get('filter', 'all-parks')
        entity = request.args.get('entity', 'rides')
        limit = min(int(request.args.get('limit', 10)), 20)

        # Validate period (LIVE not supported - frontend should convert to TODAY)
        valid_periods = ['today', 'yesterday', 'last_week', 'last_month']
        if period not in valid_periods:
            return jsonify({
                "success": False,
                "error": f"Invalid period. Must be one of: {', '.join(valid_periods)}"
            }), 400

        # Validate filter
        valid_filters = ['disney-universal', 'all-parks']
        if park_filter not in valid_filters:
            return jsonify({
                "success": False,
                "error": f"Invalid filter. Must be one of: {', '.join(valid_filters)}"
            }), 400

        # Validate entity
        valid_entities = ['parks', 'rides']
        if entity not in valid_entities:
            return jsonify({
                "success": False,
                "error": f"Invalid entity. Must be one of: {', '.join(valid_entities)}"
            }), 400

        filter_disney_universal = (park_filter == 'disney-universal')

        # PERFORMANCE: Use 5-minute cache for expensive aggregation queries
        cache = get_query_cache()
        cache_key = generate_cache_key(
            "longest_wait_times",
            period=period,
            filter=park_filter,
            entity=entity,
            limit=str(limit)
        )

        def compute_results():
            with get_db_connection() as conn:
                query = LongestWaitTimesQuery(conn)
                if entity == 'parks':
                    return query.get_park_rankings(
                        period=period,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                else:
                    return query.get_rankings(
                        period=period,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )

        results = cache.get_or_compute(key=cache_key, compute_fn=compute_results)

        # Add rank to results
        ranked_results = []
        for idx, item in enumerate(results, start=1):
            item['rank'] = idx
            ranked_results.append(item)

        return jsonify({
            "success": True,
            "period": period,
            "filter": park_filter,
            "entity": entity,
            "count": len(ranked_results),
            "data": ranked_results,
            "cached": True,
            "attribution": "Data powered by ThemeParks.wiki - https://themeparks.wiki",
            "timestamp": datetime.utcnow().isoformat() + 'Z'
        }), 200

    except ValueError as e:
        logger.error(f"Validation error in get_longest_wait_times: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

    except Exception as e:
        logger.error(f"Error in get_longest_wait_times: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@trends_bp.route('/trends/least-reliable', methods=['GET'])
def get_least_reliable():
    """
    GET /api/trends/least-reliable

    Returns top 10 parks or rides ranked by total downtime hours for the Awards section.

    Query Files Used:
    -----------------
    - database/queries/trends/least_reliable_rides.py

    Query Parameters:
        - period: today | last_week | last_month (default: today)
        - filter: disney-universal | all-parks (default: all-parks)
        - entity: parks | rides (default: rides)
        - limit: max results (default: 10, max: 20)

    Returns:
        JSON response with top parks/rides by downtime hours
    """
    try:
        # Parse query parameters
        period = request.args.get('period', 'today')
        park_filter = request.args.get('filter', 'all-parks')
        entity = request.args.get('entity', 'rides')
        limit = min(int(request.args.get('limit', 10)), 20)

        # Validate period (LIVE not supported - frontend should convert to TODAY)
        valid_periods = ['today', 'yesterday', 'last_week', 'last_month']
        if period not in valid_periods:
            return jsonify({
                "success": False,
                "error": f"Invalid period. Must be one of: {', '.join(valid_periods)}"
            }), 400

        # Validate filter
        valid_filters = ['disney-universal', 'all-parks']
        if park_filter not in valid_filters:
            return jsonify({
                "success": False,
                "error": f"Invalid filter. Must be one of: {', '.join(valid_filters)}"
            }), 400

        # Validate entity
        valid_entities = ['parks', 'rides']
        if entity not in valid_entities:
            return jsonify({
                "success": False,
                "error": f"Invalid entity. Must be one of: {', '.join(valid_entities)}"
            }), 400

        filter_disney_universal = (park_filter == 'disney-universal')

        # PERFORMANCE: Use 5-minute cache for expensive aggregation queries
        cache = get_query_cache()
        cache_key = generate_cache_key(
            "least_reliable",
            period=period,
            filter=park_filter,
            entity=entity,
            limit=str(limit)
        )

        def compute_results():
            with get_db_session() as session:
                query = LeastReliableRidesQuery(session)
                if entity == 'parks':
                    return query.get_park_rankings(
                        period=period,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                else:
                    return query.get_rankings(
                        period=period,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )

        results = cache.get_or_compute(key=cache_key, compute_fn=compute_results)

        # Add rank to results
        ranked_results = []
        for idx, item in enumerate(results, start=1):
            item['rank'] = idx
            ranked_results.append(item)

        return jsonify({
            "success": True,
            "period": period,
            "filter": park_filter,
            "entity": entity,
            "count": len(ranked_results),
            "data": ranked_results,
            "cached": True,
            "attribution": "Data powered by ThemeParks.wiki - https://themeparks.wiki",
            "timestamp": datetime.utcnow().isoformat() + 'Z'
        }), 200

    except ValueError as e:
        logger.error(f"Validation error in get_least_reliable: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

    except Exception as e:
        logger.error(f"Error in get_least_reliable: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@trends_bp.route('/trends/heatmap-data', methods=['GET'])
def get_heatmap_data():
    """
    GET /api/trends/heatmap-data

    Returns heatmap matrix data by reusing existing chart queries and transforming the response.

    ARCHITECTURE: This endpoint calls existing chart query classes (ParkWaitTimeHistoryQuery,
    RideDowntimeHistoryQuery, RideWaitTimeHistoryQuery) and transforms their Chart.js format
    into heatmap matrix format using transform_chart_to_heatmap().

    Query Parameters:
        - period: today | yesterday | last_week | last_month (LIVE NOT SUPPORTED)
        - type: parks | rides-downtime | rides-waittimes (required)
        - filter: disney-universal | all-parks (default: all-parks)
        - limit: max entities to return (default: 10, max: 20)

    Returns:
        JSON response with heatmap matrix format:
        {
            "success": true,
            "period": "last_week",
            "granularity": "daily",
            "title": "Top 10 Parks by Average Wait Time (Last Week)",
            "metric": "avg_wait_time_minutes",
            "metric_unit": "minutes",
            "timezone": "America/Los_Angeles",
            "entities": [
                {"entity_id": 1, "entity_name": "Magic Kingdom", "rank": 1, "total_value": 55.0}
            ],
            "time_labels": ["Dec 09", "Dec 10", ...],
            "matrix": [[45, 52, 68, ...], [38, 41, 55, ...]]
        }
    """
    try:
        # Parse query parameters
        period = request.args.get('period')
        heatmap_type = request.args.get('type')
        park_filter = request.args.get('filter', 'all-parks')
        limit = int(request.args.get('limit', 10))

        # Validate required parameters
        if not period:
            return jsonify({
                "success": False,
                "error": "Missing required parameter: period"
            }), 400

        if not heatmap_type:
            return jsonify({
                "success": False,
                "error": "Missing required parameter: type"
            }), 400

        # Validate period (LIVE NOT SUPPORTED for heatmaps)
        if not validate_heatmap_period(period):
            return jsonify({
                "success": False,
                "error": "Invalid period for heatmaps. LIVE period not supported. Use: today, yesterday, last_week, last_month"
            }), 400

        # Validate type
        valid_types = ['parks', 'parks-shame', 'rides-downtime', 'rides-waittimes']
        if heatmap_type not in valid_types:
            return jsonify({
                "success": False,
                "error": f"Invalid type. Must be one of: {', '.join(valid_types)}"
            }), 400

        # Validate filter
        valid_filters = ['disney-universal', 'all-parks']
        if park_filter not in valid_filters:
            return jsonify({
                "success": False,
                "error": f"Invalid filter. Must be one of: {', '.join(valid_filters)}"
            }), 400

        # Validate limit
        if limit < 1 or limit > 20:
            limit = min(max(limit, 1), 20)

        today = get_today_pacific()
        filter_disney_universal = (park_filter == 'disney-universal')

        # Get database connection
        with get_db_connection() as conn:
            # Determine granularity and call appropriate method
            if period in ['today', 'yesterday']:
                # Hourly granularity
                granularity = 'hourly'
                target_date = today if period == 'today' else today - timedelta(days=1)

                if heatmap_type == 'parks':
                    query = ParkWaitTimeHistoryQuery(conn)
                    chart_data = query.get_hourly(
                        target_date=target_date,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                    metric = 'avg_wait_time_minutes'
                    metric_unit = 'minutes'
                elif heatmap_type == 'parks-shame':
                    query = ParkShameHistoryQuery(conn)
                    chart_data = query.get_hourly(
                        target_date=target_date,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                    metric = 'shame_score'
                    metric_unit = 'points'
                elif heatmap_type == 'rides-downtime':
                    query = RideDowntimeHistoryQuery(conn)
                    chart_data = query.get_hourly(
                        target_date=target_date,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                    metric = 'downtime_hours'
                    metric_unit = 'hours'
                else:  # rides-waittimes
                    query = RideWaitTimeHistoryQuery(conn)
                    chart_data = query.get_hourly(
                        target_date=target_date,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                    metric = 'avg_wait_time_minutes'
                    metric_unit = 'minutes'

            else:  # last_week or last_month
                # Daily granularity
                granularity = 'daily'
                if period == 'last_week':
                    start_date, end_date, _ = get_last_week_date_range()
                    days = (end_date - start_date).days + 1
                else:  # last_month
                    start_date, end_date, _ = get_last_month_date_range()
                    days = (end_date - start_date).days + 1

                if heatmap_type == 'parks':
                    query = ParkWaitTimeHistoryQuery(conn)
                    chart_data = query.get_daily(
                        days=days,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                    metric = 'avg_wait_time_minutes'
                    metric_unit = 'minutes'
                elif heatmap_type == 'parks-shame':
                    query = ParkShameHistoryQuery(conn)
                    chart_data = query.get_daily(
                        days=days,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                    metric = 'shame_score'
                    metric_unit = 'points'
                elif heatmap_type == 'rides-downtime':
                    query = RideDowntimeHistoryQuery(conn)
                    chart_data = query.get_daily(
                        days=days,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                    metric = 'downtime_hours'
                    metric_unit = 'hours'
                else:  # rides-waittimes
                    query = RideWaitTimeHistoryQuery(conn)
                    chart_data = query.get_daily(
                        days=days,
                        filter_disney_universal=filter_disney_universal,
                        limit=limit
                    )
                    metric = 'avg_wait_time_minutes'
                    metric_unit = 'minutes'

            # Add granularity to chart_data for transformation
            chart_data['granularity'] = granularity

            # Transform Chart.js format to Heatmap matrix format
            heatmap_data = transform_chart_to_heatmap(
                chart_data=chart_data,
                period=period,
                metric=metric,
                metric_unit=metric_unit
            )

        return jsonify(heatmap_data), 200

    except ValueError as e:
        logger.error(f"Validation error in get_heatmap_data: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

    except Exception as e:
        logger.error(f"Error in get_heatmap_data: {e}", exc_info=True)
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
    from datetime import timedelta

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


def _generate_mock_hourly_chart_data(data_type: str, limit: int, for_today: bool = True) -> Dict[str, Any]:
    """
    Generate mock hourly chart data when real data is empty.

    Args:
        data_type: 'parks', 'rides', or 'waittimes'
        limit: Number of entities
        for_today: If True, only generate data up to current hour (for TODAY/LIVE).
                   If False, generate full day data (for YESTERDAY).

    Returns:
        Chart data structure with mock hourly values (6am-11pm or current hour)
    """
    import random

    # Get current Pacific hour to limit data for TODAY
    current_hour = get_now_pacific().hour

    # For TODAY/LIVE: only show hours up to current time
    # For YESTERDAY: show all hours (6am-11pm = hours 6-23)
    if for_today:
        # Start at 6am, end at current hour (or 6am minimum)
        start_hour = 6
        end_hour = max(start_hour, min(current_hour, 23))  # Cap at 11pm
        # If before 6am, show no data
        if current_hour < 6:
            return {"labels": [], "datasets": []}
    else:
        # Full day for yesterday
        start_hour = 6
        end_hour = 23

    # Generate labels for valid hours only
    labels = [f"{h}:00" for h in range(start_hour, end_hour + 1)]
    num_hours = len(labels)

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


def _generate_mock_live_chart_data(data_type: str, limit: int) -> Dict[str, Any]:
    """
    Generate mock 5-minute granularity chart data for LIVE period when real data is empty.

    Args:
        data_type: 'parks', 'rides', or 'waittimes'
        limit: Number of entities

    Returns:
        Chart data structure with mock values at 5-minute intervals
    """
    import random

    # Generate 5-minute interval labels for the last 60 minutes
    # Example: ['10:05', '10:10', '10:15', ..., '11:00']
    now = get_now_pacific()
    labels = []
    for i in range(12, 0, -1):  # 12 intervals of 5 minutes = 60 minutes
        minutes_ago = i * 5
        t = now - timedelta(minutes=minutes_ago)
        labels.append(t.strftime("%H:%M"))

    num_intervals = len(labels)

    if data_type == 'parks':
        park_names = [
            "Disney Magic Kingdom", "Universal Studios Florida", "Disney Hollywood Studios",
            "Universal Islands of Adventure", "Disney EPCOT", "Disney Animal Kingdom",
            "SeaWorld Orlando", "Busch Gardens Tampa", "Universal Volcano Bay", "LEGOLAND Florida"
        ]
        datasets = []
        for i, name in enumerate(park_names[:limit]):
            # Generate realistic shame scores (0.5 to 3.0 range)
            base = random.uniform(0.8, 2.0)
            data = [round(base + random.uniform(-0.3, 0.5), 1) for _ in range(num_intervals)]
            datasets.append({"label": name, "data": data})
    elif data_type == 'waittimes':
        park_names = [
            "Disney Magic Kingdom", "Universal Studios Florida", "Disney Hollywood Studios",
            "Universal Islands of Adventure", "Disney EPCOT", "Disney Animal Kingdom",
            "SeaWorld Orlando", "Busch Gardens Tampa"
        ]
        datasets = []
        for i, name in enumerate(park_names[:limit]):
            # Generate realistic wait times (20-60 minutes range)
            base = random.uniform(30, 45)
            data = [round(max(10, base + random.uniform(-10, 15)), 0) for _ in range(num_intervals)]
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
            # Generate realistic downtime percentages
            data = []
            for _ in range(num_intervals):
                if random.random() < 0.2:  # 20% chance of downtime
                    data.append(round(random.uniform(5, 50), 1))
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
        period: 'today', 'yesterday', 'last_week', or 'last_month' (aliases like '7days' map to last_week)

    Returns:
        Dict with current_period_label and previous_period_label
    """
    today = get_today_pacific()  # Pacific Time for US parks
    normalized_period = PERIOD_ALIASES.get(period, period)

    if normalized_period == 'today':
        current_start = today
        current_end = today
        previous_start = today - timedelta(days=1)
        previous_end = today - timedelta(days=1)

    elif normalized_period == 'yesterday':
        yesterday = today - timedelta(days=1)
        current_start = yesterday
        current_end = yesterday
        previous_start = today - timedelta(days=2)
        previous_end = today - timedelta(days=2)

    elif normalized_period == 'last_week':
        # Calendar-based: previous complete week (Sunday-Saturday)
        current_start, current_end, _ = get_last_week_date_range()
        # Previous week is the week before that
        previous_end = current_start - timedelta(days=1)
        previous_start = previous_end - timedelta(days=6)

    elif normalized_period == 'last_month':
        # Calendar-based: previous complete calendar month
        current_start, current_end, _ = get_last_month_date_range()
        # Previous month is the month before that
        previous_end = current_start - timedelta(days=1)
        # Get first day of that previous month
        previous_start = previous_end.replace(day=1)

    return {
        'current_period_label': f"{current_start} to {current_end}",
        'previous_period_label': f"{previous_start} to {previous_end}",
        'current_start': current_start,
        'current_end': current_end,
        'previous_start': previous_start,
        'previous_end': previous_end
    }
def _attach_queue_times_urls(category: str, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Ensure each result includes a Queue-Times URL so the API contract stays stable.
    """
    enriched: List[Dict[str, Any]] = []
    for row in results:
        entry = dict(row) if hasattr(row, "_mapping") else dict(row)
        if category.startswith("rides-"):
            park_qt_id = entry.get("park_queue_times_id")
            ride_qt_id = entry.get("queue_times_id")
            if park_qt_id and ride_qt_id:
                entry["queue_times_url"] = f"https://queue-times.com/parks/{park_qt_id}/rides/{ride_qt_id}"
            else:
                entry["queue_times_url"] = None
        else:
            park_qt_id = entry.get("queue_times_id")
            entry["queue_times_url"] = f"https://queue-times.com/parks/{park_qt_id}" if park_qt_id else None
        enriched.append(entry)
    return enriched
