"""
Theme Park Downtime Tracker - Data Quality Repository
Tracks and queries data quality issues from external APIs for reporting.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.engine import Connection

try:
    from ...utils.logger import logger, log_database_error
except ImportError:
    from utils.logger import logger, log_database_error


class DataQualityRepository:
    """
    Repository for data quality issue tracking.

    Implements:
    - Logging stale/invalid data from ThemeParks.wiki and Queue-Times
    - Querying recent issues for reporting
    - Deduplication to avoid flooding with repeated issues
    """

    # Minimum time between logging duplicate issues for same ride (minutes)
    DEDUP_WINDOW_MINUTES = 60

    def __init__(self, connection: Connection):
        """
        Initialize repository with database connection.

        Args:
            connection: SQLAlchemy connection object
        """
        self.conn = connection

    def log_stale_data(
        self,
        data_source: str,
        park_id: Optional[int],
        ride_id: Optional[int],
        themeparks_wiki_id: Optional[str],
        queue_times_id: Optional[int],
        entity_name: str,
        last_updated_api: Optional[datetime],
        data_age_minutes: int,
        reported_status: str,
        details: Optional[str] = None,
    ) -> Optional[int]:
        """
        Log a stale data issue (data with old lastUpdated timestamp).

        Implements deduplication - won't log if same issue logged recently.

        Args:
            data_source: 'themeparks_wiki' or 'queue_times'
            park_id: Internal park ID (if known)
            ride_id: Internal ride ID (if known)
            themeparks_wiki_id: ThemeParks.wiki entity UUID
            queue_times_id: Queue-Times ID
            entity_name: Name of the ride/park from API
            last_updated_api: The stale timestamp from API
            data_age_minutes: How old the data is
            reported_status: What status the API reported
            details: Additional JSON context

        Returns:
            issue_id if logged, None if deduplicated
        """
        # Check for recent duplicate (same ride, same issue type)
        dedup_check = text("""
            SELECT issue_id FROM data_quality_issues
            WHERE data_source = :data_source
              AND issue_type = 'STALE_DATA'
              AND (
                  (themeparks_wiki_id IS NOT NULL AND themeparks_wiki_id = :themeparks_wiki_id)
                  OR (ride_id IS NOT NULL AND ride_id = :ride_id)
              )
              AND detected_at > :dedup_cutoff
            LIMIT 1
        """)

        dedup_cutoff = datetime.now() - timedelta(minutes=self.DEDUP_WINDOW_MINUTES)

        try:
            result = self.conn.execute(dedup_check, {
                "data_source": data_source,
                "themeparks_wiki_id": themeparks_wiki_id,
                "ride_id": ride_id,
                "dedup_cutoff": dedup_cutoff,
            })
            if result.fetchone():
                # Already logged recently
                return None

            # Insert the issue
            insert_query = text("""
                INSERT INTO data_quality_issues (
                    data_source, issue_type, detected_at,
                    park_id, ride_id, themeparks_wiki_id, queue_times_id,
                    entity_name, last_updated_api, data_age_minutes,
                    reported_status, details
                )
                VALUES (
                    :data_source, 'STALE_DATA', :detected_at,
                    :park_id, :ride_id, :themeparks_wiki_id, :queue_times_id,
                    :entity_name, :last_updated_api, :data_age_minutes,
                    :reported_status, :details
                )
            """)

            result = self.conn.execute(insert_query, {
                "data_source": data_source,
                "detected_at": datetime.now(),
                "park_id": park_id,
                "ride_id": ride_id,
                "themeparks_wiki_id": themeparks_wiki_id,
                "queue_times_id": queue_times_id,
                "entity_name": entity_name,
                "last_updated_api": last_updated_api,
                "data_age_minutes": data_age_minutes,
                "reported_status": reported_status,
                "details": details,
            })

            issue_id = result.lastrowid
            logger.warning(
                f"Data quality issue logged: {entity_name} has stale data "
                f"({data_age_minutes} minutes old, status={reported_status})"
            )
            return issue_id

        except Exception as e:
            log_database_error(e, "Failed to log data quality issue")
            return None

    def get_recent_issues(
        self,
        hours: int = 24,
        data_source: Optional[str] = None,
        issue_type: Optional[str] = None,
        unresolved_only: bool = False,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get recent data quality issues for review/reporting.

        Args:
            hours: How far back to look
            data_source: Filter by source ('themeparks_wiki' or 'queue_times')
            issue_type: Filter by type ('STALE_DATA', etc.)
            unresolved_only: Only show unresolved issues
            limit: Maximum results

        Returns:
            List of issue dictionaries with park/ride names
        """
        cutoff = datetime.now() - timedelta(hours=hours)

        filters = ["dqi.detected_at >= :cutoff"]
        params = {"cutoff": cutoff, "limit": limit}

        if data_source:
            filters.append("dqi.data_source = :data_source")
            params["data_source"] = data_source

        if issue_type:
            filters.append("dqi.issue_type = :issue_type")
            params["issue_type"] = issue_type

        if unresolved_only:
            filters.append("dqi.is_resolved = FALSE")

        where_clause = " AND ".join(filters)

        query = text(f"""
            SELECT
                dqi.issue_id,
                dqi.data_source,
                dqi.issue_type,
                dqi.detected_at,
                dqi.themeparks_wiki_id,
                dqi.queue_times_id,
                dqi.entity_name,
                dqi.last_updated_api,
                dqi.data_age_minutes,
                dqi.reported_status,
                dqi.is_resolved,
                p.name as park_name,
                r.name as ride_name
            FROM data_quality_issues dqi
            LEFT JOIN parks p ON dqi.park_id = p.park_id
            LEFT JOIN rides r ON dqi.ride_id = r.ride_id
            WHERE {where_clause}
            ORDER BY dqi.detected_at DESC
            LIMIT :limit
        """)

        result = self.conn.execute(query, params)
        return [dict(row._mapping) for row in result]

    def get_summary_for_reporting(
        self,
        days: int = 7,
        data_source: str = "themeparks_wiki",
    ) -> List[Dict[str, Any]]:
        """
        Get aggregated summary of issues grouped by entity for upstream reporting.

        Args:
            days: How far back to look
            data_source: Which data source to summarize

        Returns:
            List of entities with issue counts and details
        """
        cutoff = datetime.now() - timedelta(days=days)

        query = text("""
            SELECT
                dqi.themeparks_wiki_id,
                dqi.entity_name,
                p.name as park_name,
                COUNT(*) as issue_count,
                MAX(dqi.data_age_minutes) as max_staleness_minutes,
                AVG(dqi.data_age_minutes) as avg_staleness_minutes,
                MIN(dqi.detected_at) as first_detected,
                MAX(dqi.detected_at) as last_detected,
                GROUP_CONCAT(DISTINCT dqi.reported_status) as statuses_seen
            FROM data_quality_issues dqi
            LEFT JOIN parks p ON dqi.park_id = p.park_id
            WHERE dqi.data_source = :data_source
              AND dqi.issue_type = 'STALE_DATA'
              AND dqi.detected_at >= :cutoff
            GROUP BY dqi.themeparks_wiki_id, dqi.entity_name, p.name
            ORDER BY issue_count DESC, max_staleness_minutes DESC
        """)

        result = self.conn.execute(query, {
            "data_source": data_source,
            "cutoff": cutoff,
        })
        return [dict(row._mapping) for row in result]

    def mark_resolved(self, issue_ids: List[int]) -> int:
        """
        Mark issues as resolved.

        Args:
            issue_ids: List of issue IDs to resolve

        Returns:
            Number of issues updated
        """
        if not issue_ids:
            return 0

        query = text("""
            UPDATE data_quality_issues
            SET is_resolved = TRUE, resolved_at = :resolved_at
            WHERE issue_id IN :issue_ids
        """)

        result = self.conn.execute(query, {
            "resolved_at": datetime.now(),
            "issue_ids": tuple(issue_ids),
        })
        return result.rowcount
