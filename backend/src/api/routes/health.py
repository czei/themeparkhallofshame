"""
Theme Park Downtime Tracker - Health Check Endpoint
Provides API health status, database connectivity, and data freshness.
"""

import os
import shutil
from flask import Blueprint, jsonify
from datetime import datetime, timedelta
from sqlalchemy import select, func, case, and_

from database.connection import get_db_session
from models import RideStatusSnapshot, AggregationLog
from models.orm_aggregation import AggregationType, AggregationStatus
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
        with get_db_session() as session:
            # Simple connectivity check (ORM session validates connection on first query)
            last_collection_stmt = select(
                func.max(RideStatusSnapshot.recorded_at).label('last_collection')
            )
            result = session.execute(last_collection_stmt)
            row = result.fetchone()

            health_data["checks"]["database"] = {
                "status": "healthy",
                "message": "Database connection successful"
            }

            if row and row.last_collection:
                last_collection = row.last_collection
                age_seconds = (datetime.utcnow() - last_collection.replace(tzinfo=None)).total_seconds()
                age_minutes = int(age_seconds / 60)

                # Feature 004: Data freshness threshold for alerting
                # 30 min = stale (monitoring alert), 60 min = critical
                DATA_FRESHNESS_STALE_MINUTES = 30
                DATA_FRESHNESS_CRITICAL_MINUTES = 60

                if age_minutes >= DATA_FRESHNESS_CRITICAL_MINUTES:
                    freshness_status = "critical"
                    freshness_message = f"CRITICAL: Data is {age_minutes} minutes stale - check collection pipeline"
                elif age_minutes >= DATA_FRESHNESS_STALE_MINUTES:
                    freshness_status = "stale"
                    freshness_message = f"Warning: Data is {age_minutes} minutes stale"
                else:
                    freshness_status = "healthy"
                    freshness_message = f"Data fresh ({age_minutes} min old)"

                health_data["checks"]["data_collection"] = {
                    "status": freshness_status,
                    "last_collection": last_collection.isoformat() + 'Z',
                    "age_minutes": age_minutes,
                    "stale_threshold_minutes": DATA_FRESHNESS_STALE_MINUTES,
                    "critical_threshold_minutes": DATA_FRESHNESS_CRITICAL_MINUTES,
                    "message": freshness_message
                }
            else:
                health_data["checks"]["data_collection"] = {
                    "status": "no_data",
                    "message": "No data collected yet"
                }

            # Get last daily aggregation status
            last_daily_aggregation_stmt = (
                select(
                    AggregationLog.aggregation_type,
                    AggregationLog.aggregation_date,
                    AggregationLog.status,
                    AggregationLog.completed_at
                )
                .where(AggregationLog.aggregation_type == AggregationType.DAILY)
                .order_by(AggregationLog.aggregation_date.desc(), AggregationLog.completed_at.desc())
                .limit(1)
            )

            result = session.execute(last_daily_aggregation_stmt)
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
            hourly_health_stmt = (
                select(
                    func.count().label('total_runs'),
                    func.sum(case((AggregationLog.status == AggregationStatus.SUCCESS, 1), else_=0)).label('success_count'),
                    func.sum(case((AggregationLog.status == AggregationStatus.FAILED, 1), else_=0)).label('error_count'),
                    func.max(AggregationLog.aggregated_until_ts).label('last_aggregated_until'),
                    func.max(AggregationLog.completed_at).label('last_completed_at')
                )
                .where(AggregationLog.aggregation_type == AggregationType.HOURLY)
                .where(AggregationLog.started_at >= func.now() - timedelta(hours=24))
            )

            result = session.execute(hourly_health_stmt)
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

            # Check disk space for logs directory
            try:
                logs_dir = "/opt/themeparkhallofshame/logs"
                if os.path.exists(logs_dir):
                    disk_usage = shutil.disk_usage(logs_dir)
                    usage_percent = (disk_usage.used / disk_usage.total) * 100

                    status = "healthy"
                    if usage_percent > 90:
                        status = "critical"
                    elif usage_percent > 80:
                        status = "warning"

                    health_data["checks"]["disk_space"] = {
                        "status": status,
                        "usage_percent": round(usage_percent, 1),
                        "total_gb": round(disk_usage.total / (1024**3), 2),
                        "used_gb": round(disk_usage.used / (1024**3), 2),
                        "free_gb": round(disk_usage.free / (1024**3), 2),
                        "message": f"Disk {usage_percent:.1f}% full"
                    }
                else:
                    health_data["checks"]["disk_space"] = {
                        "status": "unknown",
                        "message": "Logs directory not found (running locally?)"
                    }
            except Exception as disk_err:
                logger.warning(f"Could not check disk space: {disk_err}")
                health_data["checks"]["disk_space"] = {
                    "status": "unknown",
                    "message": f"Disk check failed: {str(disk_err)}"
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
    elif "critical" in check_statuses:
        # Feature 004: Critical status for data pipeline failures
        health_data["status"] = "critical"
        return jsonify(health_data), 503
    elif "stale" in check_statuses or "no_data" in check_statuses:
        health_data["status"] = "degraded"

    return jsonify(health_data), 200
