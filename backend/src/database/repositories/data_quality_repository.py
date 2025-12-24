"""
Theme Park Downtime Tracker - Data Quality Repository
Tracks and queries data quality issues from external APIs for reporting.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import select, func, update, and_, or_

from src.models import DataQualityIssue, Park, Ride
from src.utils.logger import logger, log_database_error


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

    def __init__(self, session: Session):
        """
        Initialize repository with database session.

        Args:
            session: SQLAlchemy ORM session
        """
        self.session = session

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
        dedup_cutoff = datetime.now() - timedelta(minutes=self.DEDUP_WINDOW_MINUTES)

        try:
            # Build dedup check conditions
            dedup_conditions = [
                DataQualityIssue.data_source == data_source,
                DataQualityIssue.issue_type == "STALE_DATA",
                DataQualityIssue.detected_at > dedup_cutoff
            ]

            # Check either by themeparks_wiki_id or ride_id
            id_conditions = []
            if themeparks_wiki_id:
                id_conditions.append(
                    and_(
                        DataQualityIssue.themeparks_wiki_id.isnot(None),
                        DataQualityIssue.themeparks_wiki_id == themeparks_wiki_id
                    )
                )
            if ride_id:
                id_conditions.append(
                    and_(
                        DataQualityIssue.ride_id.isnot(None),
                        DataQualityIssue.ride_id == ride_id
                    )
                )

            if id_conditions:
                dedup_conditions.append(or_(*id_conditions))

            stmt = select(DataQualityIssue.issue_id).where(and_(*dedup_conditions)).limit(1)
            result = self.session.execute(stmt).first()

            if result:
                # Already logged recently
                return None

            # Insert the issue
            new_issue = DataQualityIssue(
                data_source=data_source,
                issue_type="STALE_DATA",
                detected_at=datetime.now(),
                park_id=park_id,
                ride_id=ride_id,
                themeparks_wiki_id=themeparks_wiki_id,
                queue_times_id=queue_times_id,
                entity_name=entity_name,
                last_updated_api=last_updated_api,
                data_age_minutes=data_age_minutes,
                reported_status=reported_status,
                details=details,
            )

            self.session.add(new_issue)
            self.session.flush()  # Flush to get the issue_id

            issue_id = new_issue.issue_id
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

        # Build dynamic WHERE conditions
        conditions = [DataQualityIssue.detected_at >= cutoff]

        if data_source:
            conditions.append(DataQualityIssue.data_source == data_source)

        if issue_type:
            conditions.append(DataQualityIssue.issue_type == issue_type)

        if unresolved_only:
            conditions.append(DataQualityIssue.is_resolved == False)

        # Build query with LEFT JOINs
        stmt = (
            select(
                DataQualityIssue.issue_id,
                DataQualityIssue.data_source,
                DataQualityIssue.issue_type,
                DataQualityIssue.detected_at,
                DataQualityIssue.themeparks_wiki_id,
                DataQualityIssue.queue_times_id,
                DataQualityIssue.entity_name,
                DataQualityIssue.last_updated_api,
                DataQualityIssue.data_age_minutes,
                DataQualityIssue.reported_status,
                DataQualityIssue.is_resolved,
                Park.name.label('park_name'),
                Ride.name.label('ride_name')
            )
            .outerjoin(Park, DataQualityIssue.park_id == Park.park_id)
            .outerjoin(Ride, DataQualityIssue.ride_id == Ride.ride_id)
            .where(and_(*conditions))
            .order_by(DataQualityIssue.detected_at.desc())
            .limit(limit)
        )

        result = self.session.execute(stmt)
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

        # Build aggregation query with GROUP BY
        stmt = (
            select(
                DataQualityIssue.themeparks_wiki_id,
                DataQualityIssue.entity_name,
                Park.name.label('park_name'),
                func.count().label('issue_count'),
                func.max(DataQualityIssue.data_age_minutes).label('max_staleness_minutes'),
                func.avg(DataQualityIssue.data_age_minutes).label('avg_staleness_minutes'),
                func.min(DataQualityIssue.detected_at).label('first_detected'),
                func.max(DataQualityIssue.detected_at).label('last_detected'),
                func.group_concat(func.distinct(DataQualityIssue.reported_status)).label('statuses_seen')
            )
            .outerjoin(Park, DataQualityIssue.park_id == Park.park_id)
            .where(
                and_(
                    DataQualityIssue.data_source == data_source,
                    DataQualityIssue.issue_type == "STALE_DATA",
                    DataQualityIssue.detected_at >= cutoff
                )
            )
            .group_by(DataQualityIssue.themeparks_wiki_id, DataQualityIssue.entity_name, Park.name)
            .order_by(func.count().desc(), func.max(DataQualityIssue.data_age_minutes).desc())
        )

        result = self.session.execute(stmt)
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

        stmt = (
            update(DataQualityIssue)
            .where(DataQualityIssue.issue_id.in_(issue_ids))
            .values(is_resolved=True, resolved_at=datetime.now())
        )

        result = self.session.execute(stmt)
        return result.rowcount
