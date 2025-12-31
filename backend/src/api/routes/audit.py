"""
Theme Park Hall of Shame - Audit API Routes
============================================

Endpoints for data accuracy verification and auditing.

Endpoints:
    POST /audit/verify           - User-triggered audit for any displayed number
    GET  /audit/status           - Current validation status for a date
    GET  /audit/anomalies        - Detected anomalies for a date
    GET  /audit/flagged-entities - List of parks/rides flagged for review

These endpoints support the user-triggered audit feature:
When a user clicks any statistic, they can verify the calculation
and see the complete trace from raw data to final number.
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta

from database.connection import get_db_connection, get_db_session
from database.audit import AnomalyDetector, ComputationTracer
from database.audit.validation_checks import run_hourly_audit
from database.repositories.data_quality_repository import DataQualityRepository
from utils.logger import logger
from utils.timezone import get_today_pacific

audit_bp = Blueprint("audit", __name__)


@audit_bp.route("/audit/verify", methods=["POST"])
def verify_statistic():
    """
    User-triggered audit for any displayed number.

    Users can click any statistic to verify the calculation and see
    the complete computation trace from raw snapshots to final number.

    Request Body:
        {
            "type": "park_shame_score" | "ride_downtime",
            "entity_id": 123,
            "period": "today" | "7days" | "30days",
            "displayed_value": 2.45
        }

    Returns:
        {
            "success": true,
            "verified": true/false,
            "displayed_value": 2.45,
            "computed_value": 2.45,
            "computation_trace": [...],
            "data_quality": {...},
            "methodology_url": "/about#methodology"
        }

    Status Codes:
        200: Audit completed (even if values don't match)
        400: Invalid request body
        500: Internal server error
    """
    data = request.get_json()

    # Validate required fields
    required_fields = ["type", "entity_id", "period", "displayed_value"]
    missing = [f for f in required_fields if f not in data]
    if missing:
        return jsonify({
            "success": False,
            "error": f"Missing required fields: {', '.join(missing)}"
        }), 400

    audit_type = data["type"]
    entity_id = data["entity_id"]
    period = data["period"]
    displayed_value = float(data["displayed_value"])

    # Validate type
    valid_types = ["park_shame_score", "ride_downtime", "wait_time"]
    if audit_type not in valid_types:
        return jsonify({
            "success": False,
            "error": f"Invalid type. Must be one of: {', '.join(valid_types)}"
        }), 400

    # Validate period
    valid_periods = ["today", "7days", "30days"]
    if period not in valid_periods:
        return jsonify({
            "success": False,
            "error": f"Invalid period. Must be one of: {', '.join(valid_periods)}"
        }), 400

    try:
        with get_db_connection() as conn:
            tracer = ComputationTracer(conn)

            if audit_type == "park_shame_score":
                trace = tracer.trace_park_shame_score(
                    park_id=entity_id,
                    period=period,
                    displayed_value=displayed_value,
                )
            elif audit_type == "ride_downtime":
                trace = tracer.trace_ride_downtime(
                    ride_id=entity_id,
                    period=period,
                    displayed_value=displayed_value,
                )
            else:
                # Wait time tracing not implemented yet
                return jsonify({
                    "success": False,
                    "error": "Wait time tracing not yet implemented"
                }), 400

            response = tracer.to_dict(trace)
            response["success"] = True

            # Add anomaly info if this entity was flagged
            detector = AnomalyDetector(conn)
            target_date = get_today_pacific() if period == "today" else get_today_pacific() - timedelta(days=1)
            anomalies = detector.detect_anomalies(target_date)

            entity_anomalies = [
                a for a in anomalies
                if a.entity_type == trace.entity_type and a.entity_id == entity_id
            ]
            if entity_anomalies:
                response["anomalies_detected"] = detector.to_dict(entity_anomalies)
            else:
                response["anomalies_detected"] = []

            logger.info(
                f"Audit verify: type={audit_type}, entity={entity_id}, "
                f"verified={trace.verified}, displayed={displayed_value}, computed={trace.computed_value}"
            )

            return jsonify(response), 200

    except Exception as e:
        logger.error(f"Error in audit verify: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error during audit"
        }), 500


@audit_bp.route("/audit/status", methods=["GET"])
def get_audit_status():
    """
    Get current validation status for a date.

    Shows results of all validation checks (impossible values,
    cross-table consistency, data completeness).

    Query Parameters:
        date (str): Date to check (YYYY-MM-DD, default: yesterday)

    Returns:
        {
            "success": true,
            "target_date": "2024-11-29",
            "status": "PASS" | "WARN" | "FAIL",
            "total_checks": 12,
            "passed": 11,
            "failed": 1,
            "critical_failures": 0,
            "failures": [...]
        }
    """
    date_str = request.args.get("date")

    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({
                "success": False,
                "error": "Invalid date format. Use YYYY-MM-DD"
            }), 400
    else:
        target_date = get_today_pacific() - timedelta(days=1)

    try:
        with get_db_session() as session:
            summary = run_hourly_audit(session, target_date)
            summary["success"] = True

            return jsonify(summary), 200

    except Exception as e:
        logger.error(f"Error fetching audit status: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@audit_bp.route("/audit/anomalies", methods=["GET"])
def get_anomalies():
    """
    Get detected anomalies for a date.

    Shows statistical anomalies (Z-score, sudden changes) and
    data quality issues.

    Query Parameters:
        date (str): Date to check (YYYY-MM-DD, default: yesterday)
        severity (str): Filter by severity ('CRITICAL', 'WARNING', 'INFO')

    Returns:
        {
            "success": true,
            "target_date": "2024-11-29",
            "total_anomalies": 3,
            "anomalies": [...]
        }
    """
    date_str = request.args.get("date")
    severity_filter = request.args.get("severity")

    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({
                "success": False,
                "error": "Invalid date format. Use YYYY-MM-DD"
            }), 400
    else:
        target_date = get_today_pacific() - timedelta(days=1)

    # Validate severity filter
    if severity_filter and severity_filter not in ["CRITICAL", "WARNING", "INFO"]:
        return jsonify({
            "success": False,
            "error": "Invalid severity. Must be CRITICAL, WARNING, or INFO"
        }), 400

    try:
        with get_db_connection() as conn:
            detector = AnomalyDetector(conn)
            anomalies = detector.detect_anomalies(target_date)

            if severity_filter:
                anomalies = [a for a in anomalies if a.severity == severity_filter]

            response = {
                "success": True,
                "target_date": target_date.isoformat(),
                "total_anomalies": len(anomalies),
                "critical_count": len([a for a in anomalies if a.severity == "CRITICAL"]),
                "warning_count": len([a for a in anomalies if a.severity == "WARNING"]),
                "anomalies": detector.to_dict(anomalies),
            }

            return jsonify(response), 200

    except Exception as e:
        logger.error(f"Error fetching anomalies: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@audit_bp.route("/audit/flagged-entities", methods=["GET"])
def get_flagged_entities():
    """
    Get list of parks and rides flagged for review.

    Use this to add warning badges to displayed statistics.
    Flagged entities have anomalies detected that warrant user attention.

    Query Parameters:
        date (str): Date to check (YYYY-MM-DD, default: yesterday)
        severity (str): Minimum severity to include ('CRITICAL', 'WARNING')

    Returns:
        {
            "success": true,
            "target_date": "2024-11-29",
            "flagged_parks": [1, 5, 12],
            "flagged_rides": [42, 103]
        }
    """
    date_str = request.args.get("date")
    severity_filter = request.args.get("severity", "WARNING")

    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({
                "success": False,
                "error": "Invalid date format. Use YYYY-MM-DD"
            }), 400
    else:
        target_date = get_today_pacific() - timedelta(days=1)

    try:
        with get_db_connection() as conn:
            detector = AnomalyDetector(conn)
            anomalies = detector.detect_anomalies(target_date)

            # Filter by severity
            if severity_filter == "CRITICAL":
                anomalies = [a for a in anomalies if a.severity == "CRITICAL"]
            else:  # WARNING includes both WARNING and CRITICAL
                anomalies = [a for a in anomalies if a.severity in ["WARNING", "CRITICAL"]]

            # Extract unique IDs
            flagged_parks = list(set(
                a.entity_id for a in anomalies if a.entity_type == "park"
            ))
            flagged_rides = list(set(
                a.entity_id for a in anomalies if a.entity_type == "ride"
            ))

            response = {
                "success": True,
                "target_date": target_date.isoformat(),
                "flagged_parks": flagged_parks,
                "flagged_rides": flagged_rides,
                "methodology_url": "/about#methodology",
            }

            return jsonify(response), 200

    except Exception as e:
        logger.error(f"Error fetching flagged entities: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@audit_bp.route("/audit/run", methods=["POST"])
def trigger_audit():
    """
    Manually trigger an audit run.

    This is typically called automatically after data updates,
    but can be triggered manually for testing or re-runs.

    Request Body (optional):
        {
            "date": "2024-11-29"  // Default: yesterday
        }

    Returns:
        Audit summary with validation results
    """
    data = request.get_json() or {}
    date_str = data.get("date")

    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({
                "success": False,
                "error": "Invalid date format. Use YYYY-MM-DD"
            }), 400
    else:
        target_date = get_today_pacific() - timedelta(days=1)

    try:
        with get_db_session() as session:
            # Run validation checks
            summary = run_hourly_audit(session, target_date)

            # Also run anomaly detection (still uses Connection)
            with get_db_connection() as conn:
                detector = AnomalyDetector(conn)
                anomalies = detector.detect_anomalies(target_date)

            summary["success"] = True
            summary["anomalies_detected"] = len(anomalies)
            summary["anomalies"] = detector.to_dict(anomalies)

            logger.info(
                f"Manual audit triggered for {target_date}: "
                f"status={summary['status']}, anomalies={len(anomalies)}"
            )

            return jsonify(summary), 200

    except Exception as e:
        logger.error(f"Error triggering audit: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


# =============================================================================
# DATA QUALITY ENDPOINTS (for reporting to ThemeParks.wiki)
# =============================================================================

@audit_bp.route("/audit/data-quality", methods=["GET"])
def get_data_quality_issues():
    """
    Get recent data quality issues for reporting to upstream APIs.

    This endpoint returns stale/invalid data issues detected from ThemeParks.wiki.
    Useful for reporting issues to ThemeParks.wiki maintainers.

    Query Parameters:
        hours (int): How far back to look (default: 24)
        source (str): Filter by source ('themeparks_wiki' or 'queue_times')
        unresolved (bool): Only show unresolved issues (default: true)

    Returns:
        {
            "success": true,
            "total_issues": 5,
            "issues": [
                {
                    "issue_id": 1,
                    "data_source": "themeparks_wiki",
                    "issue_type": "STALE_DATA",
                    "entity_name": "Buzz Lightyear Astro Blasters",
                    "themeparks_wiki_id": "88197808-3c56-4198-a5a4-6066541251cf",
                    "data_age_minutes": 259200,
                    "reported_status": "CLOSED",
                    "detected_at": "2025-12-01T10:15:00"
                }
            ]
        }
    """
    hours = request.args.get("hours", 24, type=int)
    data_source = request.args.get("source")
    unresolved_only = request.args.get("unresolved", "true").lower() == "true"

    try:
        with get_db_session() as session:
            repo = DataQualityRepository(session)
            issues = repo.get_recent_issues(
                hours=hours,
                data_source=data_source,
                unresolved_only=unresolved_only,
            )

            # Convert datetime objects to ISO strings
            for issue in issues:
                if issue.get("detected_at"):
                    issue["detected_at"] = issue["detected_at"].isoformat()
                if issue.get("last_updated_api"):
                    issue["last_updated_api"] = issue["last_updated_api"].isoformat()

            return jsonify({
                "success": True,
                "total_issues": len(issues),
                "issues": issues,
            }), 200

    except Exception as e:
        logger.error(f"Error fetching data quality issues: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@audit_bp.route("/audit/data-quality/summary", methods=["GET"])
def get_data_quality_summary():
    """
    Get aggregated summary of data quality issues for upstream reporting.

    Groups issues by entity for easy reporting to ThemeParks.wiki.

    Query Parameters:
        days (int): How far back to look (default: 7)
        source (str): Filter by source (default: 'themeparks_wiki')

    Returns:
        {
            "success": true,
            "summary": [
                {
                    "themeparks_wiki_id": "88197808-3c56-4198-a5a4-6066541251cf",
                    "entity_name": "Buzz Lightyear Astro Blasters",
                    "issue_count": 15,
                    "max_staleness_minutes": 259200,
                    "avg_staleness_minutes": 180000,
                    "first_detected": "2025-12-01T08:00:00",
                    "last_detected": "2025-12-01T14:00:00"
                }
            ]
        }
    """
    days = request.args.get("days", 7, type=int)
    data_source = request.args.get("source", "themeparks_wiki")

    try:
        with get_db_session() as session:
            repo = DataQualityRepository(session)
            summary = repo.get_summary_for_reporting(
                days=days,
                data_source=data_source,
            )

            # Convert datetime objects to ISO strings
            for item in summary:
                if item.get("first_detected"):
                    item["first_detected"] = item["first_detected"].isoformat()
                if item.get("last_detected"):
                    item["last_detected"] = item["last_detected"].isoformat()

            return jsonify({
                "success": True,
                "days": days,
                "data_source": data_source,
                "entities_with_issues": len(summary),
                "summary": summary,
            }), 200

    except Exception as e:
        logger.error(f"Error fetching data quality summary: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500
