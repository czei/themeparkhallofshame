"""
Theme Park Downtime Tracker - Health Check Endpoint
Provides API health status, database connectivity, and data freshness.
"""

from flask import Blueprint, jsonify
from datetime import datetime
from sqlalchemy import text

from ...database.connection import get_db_connection
from ...utils.logger import logger

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

            # Get last aggregation status
            last_aggregation_query = text("""
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

            result = conn.execute(last_aggregation_query)
            row = result.fetchone()

            if row:
                health_data["checks"]["aggregation"] = {
                    "status": row.status,
                    "aggregation_date": str(row.aggregation_date),
                    "completed_at": row.completed_at.isoformat() + 'Z' if row.completed_at else None
                }
            else:
                health_data["checks"]["aggregation"] = {
                    "status": "no_data",
                    "message": "No aggregation runs yet"
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
