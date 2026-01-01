"""
Repository: Data Quality Log
CRUD operations for DataQualityLog model.
Feature: 004-themeparks-data-collection
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import select, func, and_

from models.orm_quality_log import DataQualityLog


class QualityLogRepository:
    """Repository for DataQualityLog CRUD operations."""

    def __init__(self, session: Session):
        self.session = session

    def create(
        self,
        issue_type: str,
        entity_type: str,
        timestamp_start: datetime,
        description: str,
        entity_id: Optional[int] = None,
        external_id: Optional[str] = None,
        timestamp_end: Optional[datetime] = None,
        import_id: Optional[str] = None,
        raw_data: Optional[Dict[str, Any]] = None
    ) -> DataQualityLog:
        """
        Create a new quality log entry.

        Args:
            issue_type: Type of issue (GAP, DUPLICATE, INVALID, etc.)
            entity_type: Entity type (ride, park, snapshot, etc.)
            timestamp_start: Start of affected time range
            description: Human-readable description
            entity_id: Internal entity ID (optional)
            external_id: External UUID (optional)
            timestamp_end: End of affected time range (optional)
            import_id: Associated import ID (optional)
            raw_data: Original data for debugging (optional)

        Returns:
            Created DataQualityLog instance
        """
        log = DataQualityLog(
            issue_type=issue_type,
            entity_type=entity_type,
            timestamp_start=timestamp_start,
            description=description,
            entity_id=entity_id,
            external_id=external_id,
            timestamp_end=timestamp_end,
            import_id=import_id,
            raw_data=raw_data
        )
        self.session.add(log)
        self.session.flush()
        return log

    def get_by_id(self, log_id: int) -> Optional[DataQualityLog]:
        """Get log entry by ID."""
        return self.session.get(DataQualityLog, log_id)

    def get_by_import(
        self,
        import_id: str,
        limit: int = 100
    ) -> List[DataQualityLog]:
        """Get all log entries for an import."""
        stmt = select(DataQualityLog).where(
            DataQualityLog.import_id == import_id
        ).order_by(DataQualityLog.created_at.desc()).limit(limit)
        return self.session.execute(stmt).scalars().all()

    def get_by_import_and_type(
        self,
        import_id: str,
        issue_type: str,
        limit: int = 100
    ) -> List[DataQualityLog]:
        """Get log entries for an import filtered by issue type."""
        stmt = select(DataQualityLog).where(
            and_(
                DataQualityLog.import_id == import_id,
                DataQualityLog.issue_type == issue_type
            )
        ).order_by(DataQualityLog.created_at.desc()).limit(limit)
        return self.session.execute(stmt).scalars().all()

    def get_open_issues(self, limit: int = 100) -> List[DataQualityLog]:
        """Get all open (unresolved) issues."""
        stmt = select(DataQualityLog).where(
            DataQualityLog.resolution_status == 'OPEN'
        ).order_by(DataQualityLog.created_at.desc()).limit(limit)
        return self.session.execute(stmt).scalars().all()

    def get_by_entity(
        self,
        entity_type: str,
        entity_id: Optional[int] = None,
        external_id: Optional[str] = None
    ) -> List[DataQualityLog]:
        """Get log entries for a specific entity."""
        conditions = [DataQualityLog.entity_type == entity_type]
        if entity_id:
            conditions.append(DataQualityLog.entity_id == entity_id)
        if external_id:
            conditions.append(DataQualityLog.external_id == external_id)

        stmt = select(DataQualityLog).where(
            and_(*conditions)
        ).order_by(DataQualityLog.timestamp_start.desc())
        return self.session.execute(stmt).scalars().all()

    def list_all(
        self,
        issue_type: Optional[str] = None,
        resolution_status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[DataQualityLog]:
        """
        List log entries with optional filtering.

        Args:
            issue_type: Filter by issue type
            resolution_status: Filter by resolution status
            limit: Maximum results
            offset: Pagination offset

        Returns:
            List of DataQualityLog instances
        """
        stmt = select(DataQualityLog)
        conditions = []
        if issue_type:
            conditions.append(DataQualityLog.issue_type == issue_type)
        if resolution_status:
            conditions.append(DataQualityLog.resolution_status == resolution_status)

        if conditions:
            stmt = stmt.where(and_(*conditions))

        stmt = stmt.order_by(DataQualityLog.created_at.desc()).limit(limit).offset(offset)
        return self.session.execute(stmt).scalars().all()

    def count_by_type(self, import_id: Optional[str] = None) -> Dict[str, int]:
        """
        Get count of issues by type.

        Args:
            import_id: Filter to specific import (optional)

        Returns:
            Dict mapping issue_type to count
        """
        stmt = select(
            DataQualityLog.issue_type,
            func.count(DataQualityLog.log_id)
        ).group_by(DataQualityLog.issue_type)

        if import_id:
            stmt = stmt.where(DataQualityLog.import_id == import_id)

        result = self.session.execute(stmt).all()
        return {row[0]: row[1] for row in result}

    def count_by_status(self) -> Dict[str, int]:
        """Get count of issues by resolution status."""
        stmt = select(
            DataQualityLog.resolution_status,
            func.count(DataQualityLog.log_id)
        ).group_by(DataQualityLog.resolution_status)
        result = self.session.execute(stmt).all()
        return {row[0]: row[1] for row in result}

    def get_summary(self, import_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get quality summary statistics.

        Args:
            import_id: Filter to specific import (optional)

        Returns:
            Dict with summary statistics
        """
        base_filter = DataQualityLog.import_id == import_id if import_id else True

        # Total count
        total_stmt = select(func.count(DataQualityLog.log_id)).where(base_filter)
        total = self.session.execute(total_stmt).scalar() or 0

        # By type
        type_counts = self.count_by_type(import_id)

        # By status (global)
        status_counts = self.count_by_status()

        return {
            'total_issues': total,
            'by_type': type_counts,
            'by_status': status_counts,
            'import_id': import_id
        }

    def investigate(self, log_id: int) -> bool:
        """Mark issue as investigating."""
        log = self.get_by_id(log_id)
        if not log:
            return False
        log.investigate()
        self.session.flush()
        return True

    def resolve(self, log_id: int, notes: str) -> bool:
        """Mark issue as resolved."""
        log = self.get_by_id(log_id)
        if not log:
            return False
        log.resolve(notes)
        self.session.flush()
        return True

    def wontfix(self, log_id: int, notes: str) -> bool:
        """Mark issue as won't fix."""
        log = self.get_by_id(log_id)
        if not log:
            return False
        log.wontfix(notes)
        self.session.flush()
        return True

    def log_gap(
        self,
        entity_type: str,
        entity_id: Optional[int],
        timestamp_start: datetime,
        timestamp_end: datetime,
        description: str,
        import_id: Optional[str] = None
    ) -> DataQualityLog:
        """Convenience method to log a GAP issue."""
        return self.create(
            issue_type='GAP',
            entity_type=entity_type,
            entity_id=entity_id,
            timestamp_start=timestamp_start,
            timestamp_end=timestamp_end,
            description=description,
            import_id=import_id
        )

    def log_parse_error(
        self,
        entity_type: str,
        external_id: str,
        timestamp: datetime,
        description: str,
        raw_data: Dict[str, Any],
        import_id: Optional[str] = None
    ) -> DataQualityLog:
        """Convenience method to log a PARSE_ERROR issue."""
        return self.create(
            issue_type='PARSE_ERROR',
            entity_type=entity_type,
            external_id=external_id,
            timestamp_start=timestamp,
            description=description,
            raw_data=raw_data,
            import_id=import_id
        )

    def log_mapping_failed(
        self,
        entity_type: str,
        external_id: str,
        timestamp: datetime,
        description: str,
        import_id: Optional[str] = None
    ) -> DataQualityLog:
        """Convenience method to log a MAPPING_FAILED issue."""
        return self.create(
            issue_type='MAPPING_FAILED',
            entity_type=entity_type,
            external_id=external_id,
            timestamp_start=timestamp,
            description=description,
            import_id=import_id
        )
