"""
Theme Park Downtime Tracker - Health Check Endpoint
Provides API health status, database connectivity, and data freshness.
"""

from flask import Blueprint, jsonify
from datetime import datetime
from sqlalchemy import text

from database.connection import get_db_connection
from utils.logger import logger

health_bp = Blueprint('health', __name__)


@health_bp.route('/health', methods=['GET'])
def health_check():
    """
    Health check endpoint.

    Returns:
        JSON with API health status, database connectivity, and metrics

    Response:
        200 OK: All systems operational
        503 Service Unavailable: Database connection failed
    """
    health_data = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + 'Z',
        "api_version": "1.0.0",
        "checks": {}
    }

    # Check database connectivity
    try:
        with get_db_connection() as conn:
            # Simple connectivity check
            result = conn.execute(text("SELECT 1"))
            result.fetchone()

            health_data["checks"]["database"] = {
                "status": "healthy",
                "message": "Database connection successful"
            }

            # Get last data collection timestamp
            last_collection_query = text("""
                SELECT MAX(recorded_at) AS last_collection
                FROM ride_status_snapshots
            """)

            result = conn.execute(last_collection_query)
            row = result.fetchone()

            if row.last_collection:
                last_collection = row.last_collection
                age_seconds = (datetime.utcnow() - last_collection.replace(tzinfo=None)).total_seconds()
                age_minutes = int(age_seconds / 60)

                health_data["checks"]["data_collection"] = {
                    "status": "healthy" if age_minutes < 30 else "stale",
                    "last_collection": last_collection.isoformat() + 'Z',
                    "age_minutes": age_minutes,
                    "message": f"Last collection {age_minutes} minutes ago"
                }
            else:
                health_data["checks"]["data_collection"] = {
                    "status": "no_data",
                    "message": "No data collected yet"
                }

            # Get last daily aggregation status
            last_daily_aggregation_query = text("""
                SELECT
                    aggregation_type,
                    aggregation_date,
                    status,
                    completed_at
                FROM aggregation_log
                WHERE aggregation_type = 'daily'
                ORDER BY aggregation_date DESC, completed_at DESC
                LIMIT 1
            """)

            result = conn.execute(last_daily_aggregation_query)
            row = result.fetchone()

            if row:
                health_data["checks"]["daily_aggregation"] = {
                    "status": row.status,
                    "aggregation_date": str(row.aggregation_date),
                    "completed_at": row.completed_at.isoformat() + 'Z' if row.completed_at else None
                }
            else:
                health_data["checks"]["daily_aggregation"] = {
                    "status": "no_data",
                    "message": "No daily aggregation runs yet"
                }

            # Get hourly aggregation health status
            hourly_health_query = text("""
                SELECT
                    COUNT(*) as total_runs,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count,
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error_count,
                    MAX(aggregated_until_ts) as last_aggregated_until,
                    MAX(completed_at) as last_completed_at
                FROM aggregation_log
                WHERE aggregation_type = 'hourly'
                    AND started_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            """)

            result = conn.execute(hourly_health_query)
            hourly_row = result.fetchone()

            if hourly_row and hourly_row.total_runs > 0:
                # Calculate lag (how far behind are we?)
                lag_minutes = None
                status = "healthy"

                if hourly_row.last_aggregated_until:
                    lag_seconds = (datetime.utcnow() - hourly_row.last_aggregated_until.replace(tzinfo=None)).total_seconds()
                    lag_minutes = int(lag_seconds / 60)

                    # Hourly job runs at :05 past the hour, so we expect ~65 min lag max (1 hour + 5 min)
                    if lag_minutes > 125:  # More than 2 hours behind
                        status = "stale"
                    elif lag_minutes > 185:  # More than 3 hours behind
                        status = "unhealthy"

                # Check error rate
                error_rate = (hourly_row.error_count / hourly_row.total_runs * 100) if hourly_row.total_runs > 0 else 0
                if error_rate > 25:  # More than 25% errors
                    status = "degraded" if status == "healthy" else status

                health_data["checks"]["hourly_aggregation"] = {
                    "status": status,
                    "last_aggregated_until": hourly_row.last_aggregated_until.isoformat() + 'Z' if hourly_row.last_aggregated_until else None,
                    "last_completed_at": hourly_row.last_completed_at.isoformat() + 'Z' if hourly_row.last_completed_at else None,
                    "lag_minutes": lag_minutes,
                    "runs_last_24h": hourly_row.total_runs,
                    "success_count": hourly_row.success_count,
                    "error_count": hourly_row.error_count,
                    "error_rate_percent": round(error_rate, 1),
                    "message": f"Last aggregation {lag_minutes} minutes behind" if lag_minutes else "Hourly aggregations running"
                }
            else:
                health_data["checks"]["hourly_aggregation"] = {
                    "status": "no_data",
                    "message": "No hourly aggregation runs in last 24 hours"
                }

    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)

        health_data["status"] = "unhealthy"
        health_data["checks"]["database"] = {
            "status": "unhealthy",
            "message": f"Database connection failed: {str(e)}"
        }

        return jsonify(health_data), 503

    # Determine overall status
    check_statuses = [check.get("status") for check in health_data["checks"].values()]

    if "unhealthy" in check_statuses:
        health_data["status"] = "unhealthy"
        return jsonify(health_data), 503
    elif "stale" in check_statuses or "no_data" in check_statuses:
        health_data["status"] = "degraded"

    return jsonify(health_data), 200
