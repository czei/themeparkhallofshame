"""
Theme Park Downtime Tracker - Admin API Endpoints
Import management and administrative operations.
Feature: 004-themeparks-data-collection
Task: T028
"""

from flask import Blueprint, jsonify, request
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from database.connection import get_db_session
from importer import ArchiveImporter, ImportProgress
from database.repositories.import_repository import ImportRepository
from database.repositories.quality_log_repository import QualityLogRepository
from database.repositories.storage_repository import StorageRepository
from utils.logger import logger

admin_bp = Blueprint('admin', __name__)


def _parse_date(date_str: Optional[str]) -> Optional[date]:
    """Parse date string in YYYY-MM-DD format."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        return None


@admin_bp.route('/admin/import/start', methods=['POST'])
def start_import():
    """
    Start a new historical data import.

    Request Body:
        destination_uuid: str (required) - ThemeParks.wiki destination UUID
        start_date: str (optional) - Start date in YYYY-MM-DD format
        end_date: str (optional) - End date in YYYY-MM-DD format
        batch_size: int (optional) - Records per batch (default: 10000)
        auto_create: bool (optional) - Auto-create new entities (default: false)

    Returns:
        201 Created: Import started successfully
        400 Bad Request: Invalid parameters
        409 Conflict: Import already in progress for this destination
    """
    data = request.get_json() or {}

    destination_uuid = data.get('destination_uuid')
    if not destination_uuid:
        return jsonify({
            "error": "destination_uuid is required"
        }), 400

    start_date = _parse_date(data.get('start_date'))
    end_date = _parse_date(data.get('end_date'))
    batch_size = data.get('batch_size', 10000)
    auto_create = data.get('auto_create', False)

    try:
        with get_db_session() as session:
            # Check for existing import
            repo = ImportRepository(session)
            existing = repo.get_resumable_import(destination_uuid)
            if existing and existing.status == 'IN_PROGRESS':
                return jsonify({
                    "error": "Import already in progress",
                    "import_id": existing.import_id,
                    "status": existing.status
                }), 409

            # Create importer and start import
            importer = ArchiveImporter(
                session=session,
                batch_size=batch_size,
                auto_create_entities=auto_create
            )

            # Create checkpoint (don't run the full import synchronously)
            checkpoint = repo.create(destination_uuid)
            session.commit()

            logger.info(f"Started import {checkpoint.import_id} for {destination_uuid}")

            return jsonify({
                "import_id": checkpoint.import_id,
                "destination_uuid": destination_uuid,
                "status": checkpoint.status,
                "message": "Import created. Use resume endpoint to start processing.",
                "start_date": str(start_date) if start_date else None,
                "end_date": str(end_date) if end_date else None
            }), 201

    except Exception as e:
        logger.exception(f"Failed to start import: {e}")
        return jsonify({
            "error": f"Failed to start import: {str(e)}"
        }), 500


@admin_bp.route('/admin/import/status/<import_id>', methods=['GET'])
def get_import_status(import_id: str):
    """
    Get status of an import.

    Path Parameters:
        import_id: Import ID to check

    Returns:
        200 OK: Import status
        404 Not Found: Import not found
    """
    try:
        with get_db_session() as session:
            repo = ImportRepository(session)
            checkpoint = repo.get_by_import_id(import_id)

            if not checkpoint:
                return jsonify({
                    "error": "Import not found",
                    "import_id": import_id
                }), 404

            return jsonify({
                "import_id": checkpoint.import_id,
                "destination_uuid": checkpoint.destination_uuid,
                "status": checkpoint.status,
                "records_imported": checkpoint.records_imported,
                "errors_encountered": checkpoint.errors_encountered,
                "last_processed_date": str(checkpoint.last_processed_date) if checkpoint.last_processed_date else None,
                "last_processed_file": checkpoint.last_processed_file,
                "started_at": checkpoint.started_at.isoformat() + 'Z' if checkpoint.started_at else None,
                "completed_at": checkpoint.completed_at.isoformat() + 'Z' if checkpoint.completed_at else None,
                "can_resume": checkpoint.can_resume
            })

    except Exception as e:
        logger.exception(f"Failed to get import status: {e}")
        return jsonify({
            "error": f"Failed to get status: {str(e)}"
        }), 500


@admin_bp.route('/admin/import/resume/<import_id>', methods=['POST'])
def resume_import(import_id: str):
    """
    Resume a paused or pending import.

    Path Parameters:
        import_id: Import ID to resume

    Returns:
        200 OK: Resume initiated
        400 Bad Request: Import cannot be resumed
        404 Not Found: Import not found
    """
    try:
        with get_db_session() as session:
            repo = ImportRepository(session)
            checkpoint = repo.get_by_import_id(import_id)

            if not checkpoint:
                return jsonify({
                    "error": "Import not found",
                    "import_id": import_id
                }), 404

            if not checkpoint.can_resume:
                return jsonify({
                    "error": "Import cannot be resumed",
                    "import_id": import_id,
                    "status": checkpoint.status
                }), 400

            # Mark as in progress
            repo.start_import(checkpoint)
            session.commit()

            return jsonify({
                "import_id": checkpoint.import_id,
                "destination_uuid": checkpoint.destination_uuid,
                "status": checkpoint.status,
                "message": "Import resumed. Run CLI for actual processing."
            })

    except Exception as e:
        logger.exception(f"Failed to resume import: {e}")
        return jsonify({
            "error": f"Failed to resume: {str(e)}"
        }), 500


@admin_bp.route('/admin/import/pause/<import_id>', methods=['POST'])
def pause_import(import_id: str):
    """
    Pause a running import.

    Path Parameters:
        import_id: Import ID to pause

    Returns:
        200 OK: Import paused
        400 Bad Request: Import not in progress
        404 Not Found: Import not found
    """
    try:
        with get_db_session() as session:
            repo = ImportRepository(session)
            checkpoint = repo.get_by_import_id(import_id)

            if not checkpoint:
                return jsonify({
                    "error": "Import not found",
                    "import_id": import_id
                }), 404

            if checkpoint.status != 'IN_PROGRESS':
                return jsonify({
                    "error": "Import not in progress",
                    "import_id": import_id,
                    "status": checkpoint.status
                }), 400

            repo.pause_import(checkpoint)
            session.commit()

            return jsonify({
                "import_id": checkpoint.import_id,
                "destination_uuid": checkpoint.destination_uuid,
                "status": checkpoint.status,
                "message": "Import paused"
            })

    except Exception as e:
        logger.exception(f"Failed to pause import: {e}")
        return jsonify({
            "error": f"Failed to pause: {str(e)}"
        }), 500


@admin_bp.route('/admin/import/cancel/<import_id>', methods=['DELETE'])
def cancel_import(import_id: str):
    """
    Cancel an import.

    Path Parameters:
        import_id: Import ID to cancel

    Returns:
        200 OK: Import cancelled
        404 Not Found: Import not found
    """
    try:
        with get_db_session() as session:
            repo = ImportRepository(session)
            success = repo.cancel_import(import_id)
            session.commit()

            if not success:
                return jsonify({
                    "error": "Import not found or already completed",
                    "import_id": import_id
                }), 404

            return jsonify({
                "import_id": import_id,
                "status": "CANCELLED",
                "message": "Import cancelled"
            })

    except Exception as e:
        logger.exception(f"Failed to cancel import: {e}")
        return jsonify({
            "error": f"Failed to cancel: {str(e)}"
        }), 500


@admin_bp.route('/admin/import/list', methods=['GET'])
def list_imports():
    """
    List all imports with pagination.

    Query Parameters:
        status: str (optional) - Filter by status (PENDING, IN_PROGRESS, PAUSED, COMPLETED, FAILED, CANCELLED)
        limit: int (optional) - Max results (default: 50, max: 200)
        offset: int (optional) - Skip first N results (default: 0)

    Returns:
        200 OK: List of imports
    """
    status_filter = request.args.get('status')
    limit = min(int(request.args.get('limit', 50)), 200)
    offset = int(request.args.get('offset', 0))

    try:
        with get_db_session() as session:
            repo = ImportRepository(session)

            if status_filter:
                imports = repo.get_by_status(status_filter, limit=limit, offset=offset)
            else:
                imports = repo.get_all(limit=limit, offset=offset)

            return jsonify({
                "imports": [
                    {
                        "import_id": cp.import_id,
                        "destination_uuid": cp.destination_uuid,
                        "status": cp.status,
                        "records_imported": cp.records_imported,
                        "errors_encountered": cp.errors_encountered,
                        "started_at": cp.started_at.isoformat() + 'Z' if cp.started_at else None,
                        "completed_at": cp.completed_at.isoformat() + 'Z' if cp.completed_at else None
                    }
                    for cp in imports
                ],
                "limit": limit,
                "offset": offset,
                "count": len(imports)
            })

    except Exception as e:
        logger.exception(f"Failed to list imports: {e}")
        return jsonify({
            "error": f"Failed to list imports: {str(e)}"
        }), 500


@admin_bp.route('/admin/import/quality/<import_id>', methods=['GET'])
def get_quality_report(import_id: str):
    """
    Get data quality report for an import.

    Path Parameters:
        import_id: Import ID

    Query Parameters:
        issue_type: str (optional) - Filter by issue type
        limit: int (optional) - Max results (default: 100, max: 500)

    Returns:
        200 OK: Quality report with issues
        404 Not Found: Import not found
    """
    issue_type = request.args.get('issue_type')
    limit = min(int(request.args.get('limit', 100)), 500)

    try:
        with get_db_session() as session:
            import_repo = ImportRepository(session)
            checkpoint = import_repo.get_by_import_id(import_id)

            if not checkpoint:
                return jsonify({
                    "error": "Import not found",
                    "import_id": import_id
                }), 404

            quality_repo = QualityLogRepository(session)

            # Get issue counts by type
            counts = quality_repo.count_by_type(import_id)

            # Get recent issues
            if issue_type:
                issues = quality_repo.get_by_import_and_type(import_id, issue_type, limit=limit)
            else:
                issues = quality_repo.get_by_import(import_id, limit=limit)

            return jsonify({
                "import_id": import_id,
                "destination_uuid": checkpoint.destination_uuid,
                "status": checkpoint.status,
                "records_imported": checkpoint.records_imported,
                "errors_encountered": checkpoint.errors_encountered,
                "issue_counts": counts,
                "issues": [
                    {
                        "issue_type": issue.issue_type,
                        "entity_type": issue.entity_type,
                        "external_id": issue.external_id,
                        "description": issue.description,
                        "timestamp_start": issue.timestamp_start.isoformat() + 'Z' if issue.timestamp_start else None,
                        "created_at": issue.created_at.isoformat() + 'Z' if issue.created_at else None
                    }
                    for issue in issues
                ],
                "issues_returned": len(issues),
                "issues_total": sum(counts.values())
            })

    except Exception as e:
        logger.exception(f"Failed to get quality report: {e}")
        return jsonify({
            "error": f"Failed to get quality report: {str(e)}"
        }), 500


@admin_bp.route('/admin/import/destinations', methods=['GET'])
def list_destinations():
    """
    List available destinations from archive.themeparks.wiki.

    Returns:
        200 OK: List of destination UUIDs
        500 Error: Failed to list destinations
    """
    try:
        with get_db_session() as session:
            importer = ArchiveImporter(session=session)
            destinations = importer.list_destinations()

            return jsonify({
                "destinations": destinations,
                "count": len(destinations)
            })

    except Exception as e:
        logger.exception(f"Failed to list destinations: {e}")
        return jsonify({
            "error": f"Failed to list destinations: {str(e)}"
        }), 500


# =============================================================================
# STORAGE MONITORING ENDPOINTS (Feature 004, Task T048)
# =============================================================================

def _decimal_to_float(obj):
    """Convert Decimal values to float for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: _decimal_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_decimal_to_float(item) for item in obj]
    return obj


@admin_bp.route('/admin/storage/summary', methods=['GET'])
def get_storage_summary():
    """
    Get current storage summary across all tables.

    Returns:
        200 OK: Storage summary with totals and breakdown
    """
    try:
        with get_db_session() as session:
            repo = StorageRepository(session)
            total = repo.get_total_storage()
            latest = repo.get_all_tables_latest()

            return jsonify(_decimal_to_float({
                "summary": {
                    "total_data_mb": total['total_data_mb'],
                    "total_index_mb": total['total_index_mb'],
                    "total_size_mb": total['total_size_mb'],
                    "total_size_gb": total['total_size_mb'] / Decimal('1024'),
                    "total_rows": total['total_rows'],
                    "total_growth_mb_per_day": total['total_growth_mb_per_day'],
                    "table_count": total['table_count']
                },
                "tables": [
                    {
                        "table_name": m.table_name,
                        "row_count": m.row_count,
                        "data_size_mb": m.data_size_mb,
                        "index_size_mb": m.index_size_mb,
                        "total_size_mb": m.total_size_mb,
                        "growth_rate_mb_per_day": m.growth_rate_mb_per_day,
                        "partition_count": m.partition_count,
                        "measurement_date": m.measurement_date.isoformat()
                    }
                    for m in latest
                ]
            }))

    except Exception as e:
        logger.exception(f"Failed to get storage summary: {e}")
        return jsonify({
            "error": f"Failed to get storage summary: {str(e)}"
        }), 500


@admin_bp.route('/admin/storage/growth', methods=['GET'])
def get_storage_growth():
    """
    Get storage growth analysis and projections.

    Query Parameters:
        days: int (optional) - Days to project (default: 365)
        table: str (optional) - Specific table to analyze

    Returns:
        200 OK: Growth analysis and projections
    """
    days = int(request.args.get('days', 365))
    table_name = request.args.get('table')

    try:
        with get_db_session() as session:
            repo = StorageRepository(session)

            if table_name:
                # Analyze specific table
                analysis = repo.get_growth_analysis(table_name, days=min(days, 90))
                return jsonify(_decimal_to_float({
                    "table_analysis": analysis
                }))
            else:
                # Overall projection
                projection = repo.project_storage(days)
                return jsonify(_decimal_to_float({
                    "projection": {
                        "current_size_mb": projection['current_size_mb'],
                        "current_size_gb": projection['current_size_gb'],
                        "growth_rate_mb_per_day": projection['growth_rate_mb_per_day'],
                        "growth_rate_gb_per_month": projection['growth_rate_gb_per_month'],
                        "projected_days": projection['projected_days'],
                        "projected_size_mb": projection['projected_size_mb'],
                        "projected_size_gb": projection['projected_size_gb']
                    }
                }))

    except Exception as e:
        logger.exception(f"Failed to get storage growth: {e}")
        return jsonify({
            "error": f"Failed to get storage growth: {str(e)}"
        }), 500


@admin_bp.route('/admin/storage/partitions', methods=['GET'])
def get_storage_partitions():
    """
    Get partition-level storage details for partitioned tables.

    Returns:
        200 OK: Partition details for each partitioned table
    """
    try:
        with get_db_session() as session:
            from sqlalchemy import text

            # Query partition details from information_schema
            result = session.execute(text("""
                SELECT
                    table_name,
                    partition_name,
                    partition_ordinal_position,
                    partition_method,
                    partition_expression,
                    partition_description,
                    table_rows,
                    data_length,
                    index_length
                FROM information_schema.partitions
                WHERE table_schema = DATABASE()
                AND partition_name IS NOT NULL
                ORDER BY table_name, partition_ordinal_position
            """))

            partitions = []
            for row in result:
                partitions.append({
                    "table_name": row[0],
                    "partition_name": row[1],
                    "position": row[2],
                    "method": row[3],
                    "expression": row[4],
                    "description": row[5],
                    "row_count": row[6] or 0,
                    "data_size_mb": float((row[7] or 0) / 1048576),
                    "index_size_mb": float((row[8] or 0) / 1048576),
                    "total_size_mb": float(((row[7] or 0) + (row[8] or 0)) / 1048576)
                })

            # Group by table
            tables = {}
            for p in partitions:
                table_name = p['table_name']
                if table_name not in tables:
                    tables[table_name] = {
                        "table_name": table_name,
                        "partition_method": p['method'],
                        "partition_expression": p['expression'],
                        "partition_count": 0,
                        "total_rows": 0,
                        "total_size_mb": 0,
                        "partitions": []
                    }
                tables[table_name]['partition_count'] += 1
                tables[table_name]['total_rows'] += p['row_count']
                tables[table_name]['total_size_mb'] += p['total_size_mb']
                tables[table_name]['partitions'].append({
                    "name": p['partition_name'],
                    "position": p['position'],
                    "description": p['description'],
                    "row_count": p['row_count'],
                    "size_mb": p['total_size_mb']
                })

            return jsonify({
                "partitioned_tables": list(tables.values())
            })

    except Exception as e:
        logger.exception(f"Failed to get partition details: {e}")
        return jsonify({
            "error": f"Failed to get partition details: {str(e)}"
        }), 500


@admin_bp.route('/admin/storage/retention-comparison', methods=['GET'])
def get_retention_comparison():
    """
    Compare storage requirements under different retention strategies.

    This endpoint provides projections for storage costs under various
    retention policies to aid capacity planning decisions.

    Returns:
        200 OK: Retention strategy comparisons
    """
    try:
        with get_db_session() as session:
            repo = StorageRepository(session)
            current = repo.get_total_storage()

            # Get current growth rate
            growth_rate = current['total_growth_mb_per_day']
            current_size = current['total_size_mb']

            # Project different scenarios (365 days)
            days = 365

            strategies = [
                {
                    "name": "Permanent Retention",
                    "description": "Keep all data forever (current policy)",
                    "projected_size_gb": float((current_size + growth_rate * days) / Decimal('1024')),
                    "annual_growth_gb": float((growth_rate * days) / Decimal('1024')),
                    "notes": "Required for year-over-year analysis"
                },
                {
                    "name": "Rolling 90-Day",
                    "description": "Keep only last 90 days of snapshots",
                    "projected_size_gb": float((growth_rate * 90) / Decimal('1024')),
                    "annual_growth_gb": 0,
                    "notes": "Steady state after 90 days"
                },
                {
                    "name": "Rolling 365-Day",
                    "description": "Keep only last year of snapshots",
                    "projected_size_gb": float((growth_rate * 365) / Decimal('1024')),
                    "annual_growth_gb": 0,
                    "notes": "Supports year-over-year, steady state after 1 year"
                },
                {
                    "name": "Tiered Retention",
                    "description": "Full for 30d, daily aggregates for 1y, monthly thereafter",
                    "projected_size_gb": float((growth_rate * 30 + growth_rate * 335 * Decimal('0.05')) / Decimal('1024')),
                    "annual_growth_gb": float((growth_rate * Decimal('0.01') * 365) / Decimal('1024')),
                    "notes": "Estimated 95% reduction for older data"
                }
            ]

            return jsonify({
                "current": _decimal_to_float({
                    "size_gb": current_size / Decimal('1024'),
                    "growth_mb_per_day": growth_rate,
                    "growth_gb_per_month": growth_rate * 30 / Decimal('1024')
                }),
                "strategies": strategies,
                "projection_days": days
            })

    except Exception as e:
        logger.exception(f"Failed to get retention comparison: {e}")
        return jsonify({
            "error": f"Failed to get retention comparison: {str(e)}"
        }), 500


@admin_bp.route('/admin/storage/alerts', methods=['GET'])
def get_storage_alerts():
    """
    Get active storage alerts.

    Query Parameters:
        warning_gb: float (optional) - Warning threshold in GB (default: 50)
        critical_gb: float (optional) - Critical threshold in GB (default: 80)

    Returns:
        200 OK: List of active alerts
    """
    warning_gb = Decimal(str(request.args.get('warning_gb', 50)))
    critical_gb = Decimal(str(request.args.get('critical_gb', 80)))

    try:
        with get_db_session() as session:
            repo = StorageRepository(session)
            alerts = repo.check_alerts(
                warning_threshold_gb=warning_gb,
                critical_threshold_gb=critical_gb
            )

            # Get current totals for context
            total = repo.get_total_storage()

            return jsonify({
                "alerts": alerts,
                "alert_count": len(alerts),
                "status": "CRITICAL" if any(a['level'] == 'CRITICAL' for a in alerts)
                         else "WARNING" if alerts
                         else "HEALTHY",
                "current_size_gb": float(total['total_size_mb'] / Decimal('1024')),
                "thresholds": {
                    "warning_gb": float(warning_gb),
                    "critical_gb": float(critical_gb)
                }
            })

    except Exception as e:
        logger.exception(f"Failed to check storage alerts: {e}")
        return jsonify({
            "error": f"Failed to check storage alerts: {str(e)}"
        }), 500


@admin_bp.route('/admin/storage/measure', methods=['POST'])
def trigger_storage_measure():
    """
    Trigger an immediate storage measurement.

    This creates new storage_metrics entries for all tables.
    Typically runs daily via cron, but can be triggered manually.

    Returns:
        200 OK: Measurement completed
        409 Conflict: Already measured today
    """
    try:
        with get_db_session() as session:
            repo = StorageRepository(session)
            measurements = repo.measure_from_database()
            session.commit()

            if not measurements:
                return jsonify({
                    "message": "Already measured today",
                    "tables_measured": 0
                }), 409

            return jsonify({
                "message": "Storage measurement completed",
                "tables_measured": len(measurements),
                "measurements": [
                    {
                        "table_name": m.table_name,
                        "total_size_mb": float(m.total_size_mb),
                        "row_count": m.row_count
                    }
                    for m in measurements
                ]
            })

    except Exception as e:
        logger.exception(f"Failed to measure storage: {e}")
        return jsonify({
            "error": f"Failed to measure storage: {str(e)}"
        }), 500


# =============================================================================
# DATA QUALITY ENDPOINTS (Feature 004)
# =============================================================================


@admin_bp.route('/admin/quality/summary', methods=['GET'])
def get_quality_summary():
    """
    Get overall data quality health summary.

    Query Parameters:
        import_id: Optional import ID to filter to

    Returns:
        200 OK: Quality summary statistics
    """
    try:
        import_id = request.args.get('import_id')

        with get_db_session() as session:
            repo = QualityLogRepository(session)
            summary = repo.get_summary(import_id)

            return jsonify({
                "total_issues": summary['total_issues'],
                "by_type": summary['by_type'],
                "by_status": summary['by_status'],
                "import_id": import_id,
                "health": "HEALTHY" if summary['by_status'].get('OPEN', 0) == 0 else "DEGRADED"
            })

    except Exception as e:
        logger.exception(f"Failed to get quality summary: {e}")
        return jsonify({
            "error": f"Failed to get quality summary: {str(e)}"
        }), 500


@admin_bp.route('/admin/quality/gaps', methods=['GET'])
def get_quality_gaps():
    """
    Get data gaps detected during import/collection.

    Query Parameters:
        import_id: Optional import ID to filter to
        limit: Maximum results (default: 100)

    Returns:
        200 OK: List of gap issues
    """
    try:
        import_id = request.args.get('import_id')
        limit = int(request.args.get('limit', 100))

        with get_db_session() as session:
            repo = QualityLogRepository(session)

            if import_id:
                gaps = repo.get_by_import_and_type(import_id, 'GAP', limit)
            else:
                gaps = repo.list_all(issue_type='GAP', limit=limit)

            return jsonify({
                "total": len(gaps),
                "gaps": [
                    {
                        "log_id": g.log_id,
                        "entity_type": g.entity_type,
                        "entity_id": g.entity_id,
                        "external_id": g.external_id,
                        "timestamp_start": g.timestamp_start.isoformat(),
                        "timestamp_end": g.timestamp_end.isoformat() if g.timestamp_end else None,
                        "duration_minutes": g.duration,
                        "description": g.description,
                        "resolution_status": g.resolution_status,
                        "created_at": g.created_at.isoformat()
                    }
                    for g in gaps
                ]
            })

    except Exception as e:
        logger.exception(f"Failed to get quality gaps: {e}")
        return jsonify({
            "error": f"Failed to get quality gaps: {str(e)}"
        }), 500


@admin_bp.route('/admin/quality/issues', methods=['GET'])
def get_quality_issues():
    """
    Get open quality issues.

    Query Parameters:
        status: Filter by status (OPEN, INVESTIGATING, RESOLVED, WONTFIX)
        type: Filter by issue type
        limit: Maximum results (default: 100)
        offset: Pagination offset (default: 0)

    Returns:
        200 OK: List of quality issues
    """
    try:
        status = request.args.get('status')
        issue_type = request.args.get('type')
        limit = int(request.args.get('limit', 100))
        offset = int(request.args.get('offset', 0))

        with get_db_session() as session:
            repo = QualityLogRepository(session)
            issues = repo.list_all(
                issue_type=issue_type,
                resolution_status=status,
                limit=limit,
                offset=offset
            )

            return jsonify({
                "total": len(issues),
                "offset": offset,
                "issues": [
                    {
                        "log_id": i.log_id,
                        "import_id": i.import_id,
                        "issue_type": i.issue_type,
                        "entity_type": i.entity_type,
                        "entity_id": i.entity_id,
                        "external_id": i.external_id,
                        "timestamp_start": i.timestamp_start.isoformat(),
                        "timestamp_end": i.timestamp_end.isoformat() if i.timestamp_end else None,
                        "description": i.description,
                        "resolution_status": i.resolution_status,
                        "resolution_notes": i.resolution_notes,
                        "created_at": i.created_at.isoformat(),
                        "updated_at": i.updated_at.isoformat()
                    }
                    for i in issues
                ]
            })

    except Exception as e:
        logger.exception(f"Failed to get quality issues: {e}")
        return jsonify({
            "error": f"Failed to get quality issues: {str(e)}"
        }), 500


@admin_bp.route('/admin/quality/issues/<int:log_id>', methods=['PATCH'])
def update_quality_issue(log_id: int):
    """
    Update a quality issue status.

    Path Parameters:
        log_id: Issue log ID

    Request Body:
        status: New status (INVESTIGATING, RESOLVED, WONTFIX)
        notes: Resolution notes (required for RESOLVED/WONTFIX)

    Returns:
        200 OK: Issue updated
        400 Bad Request: Invalid parameters
        404 Not Found: Issue not found
    """
    try:
        data = request.get_json() or {}
        new_status = data.get('status')
        notes = data.get('notes', '')

        if not new_status:
            return jsonify({"error": "status is required"}), 400

        if new_status not in ('INVESTIGATING', 'RESOLVED', 'WONTFIX'):
            return jsonify({
                "error": f"Invalid status: {new_status}. Must be INVESTIGATING, RESOLVED, or WONTFIX"
            }), 400

        if new_status in ('RESOLVED', 'WONTFIX') and not notes:
            return jsonify({
                "error": f"notes are required when setting status to {new_status}"
            }), 400

        with get_db_session() as session:
            repo = QualityLogRepository(session)
            issue = repo.get_by_id(log_id)

            if not issue:
                return jsonify({"error": "Issue not found"}), 404

            if new_status == 'INVESTIGATING':
                repo.investigate(log_id)
            elif new_status == 'RESOLVED':
                repo.resolve(log_id, notes)
            elif new_status == 'WONTFIX':
                repo.wontfix(log_id, notes)

            session.commit()

            return jsonify({
                "log_id": log_id,
                "status": new_status,
                "message": f"Issue updated to {new_status}"
            })

    except Exception as e:
        logger.exception(f"Failed to update quality issue: {e}")
        return jsonify({
            "error": f"Failed to update quality issue: {str(e)}"
        }), 500


@admin_bp.route('/admin/quality/freshness', methods=['GET'])
def get_data_freshness():
    """
    Get data freshness by park.

    Shows when data was last collected for each park.

    Returns:
        200 OK: Freshness data by park
    """
    try:
        from sqlalchemy import func, select
        from models.orm_park import Park
        from models.orm_snapshots import RideStatusSnapshot

        with get_db_session() as session:
            # Get latest snapshot per park
            stmt = select(
                Park.park_id,
                Park.name,
                func.max(RideStatusSnapshot.recorded_at).label('last_snapshot')
            ).join(
                RideStatusSnapshot.ride
            ).join(
                Park
            ).group_by(
                Park.park_id,
                Park.name
            ).order_by(
                func.max(RideStatusSnapshot.recorded_at).desc()
            )

            results = session.execute(stmt).all()

            now = datetime.utcnow()
            freshness = []
            for park_id, park_name, last_snapshot in results:
                if last_snapshot:
                    age_minutes = int((now - last_snapshot).total_seconds() / 60)
                    status = "FRESH" if age_minutes < 30 else ("STALE" if age_minutes < 120 else "VERY_STALE")
                else:
                    age_minutes = None
                    status = "NO_DATA"

                freshness.append({
                    "park_id": park_id,
                    "park_name": park_name,
                    "last_snapshot": last_snapshot.isoformat() if last_snapshot else None,
                    "age_minutes": age_minutes,
                    "status": status
                })

            return jsonify({
                "timestamp": now.isoformat(),
                "parks": freshness,
                "summary": {
                    "fresh": sum(1 for f in freshness if f['status'] == 'FRESH'),
                    "stale": sum(1 for f in freshness if f['status'] == 'STALE'),
                    "very_stale": sum(1 for f in freshness if f['status'] == 'VERY_STALE'),
                    "no_data": sum(1 for f in freshness if f['status'] == 'NO_DATA')
                }
            })

    except Exception as e:
        logger.exception(f"Failed to get data freshness: {e}")
        return jsonify({
            "error": f"Failed to get data freshness: {str(e)}"
        }), 500


@admin_bp.route('/admin/quality/coverage', methods=['GET'])
def get_metadata_coverage():
    """
    Get metadata coverage statistics.

    Shows how many rides have complete metadata.

    Returns:
        200 OK: Coverage statistics
    """
    try:
        from collector.metadata_collector import MetadataCollector

        with get_db_session() as session:
            collector = MetadataCollector(session)
            stats = collector.get_coverage_stats()

            return jsonify({
                "total_rides": stats['total_rides'],
                "with_metadata": stats['with_metadata'],
                "with_coordinates": stats['with_coordinates'],
                "with_height_requirement": stats['with_height_requirement'],
                "with_indoor_outdoor": stats['with_indoor_outdoor'],
                "coverage": {
                    "metadata_pct": stats['metadata_coverage_pct'],
                    "coordinate_pct": stats['coordinate_coverage_pct']
                }
            })

    except Exception as e:
        logger.exception(f"Failed to get metadata coverage: {e}")
        return jsonify({
            "error": f"Failed to get metadata coverage: {str(e)}"
        }), 500
