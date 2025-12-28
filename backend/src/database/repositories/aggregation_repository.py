"""
Theme Park Downtime Tracker - Aggregation Log Repository
Provides data access layer for aggregation job tracking and verification.
"""

from typing import List, Optional, Dict, Any
from datetime import date, datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import select, func

from models import AggregationLog, AggregationType, AggregationStatus
from utils.logger import logger, log_database_error


class AggregationLogRepository:
    """
    Repository for aggregation log operations.

    Implements:
    - CRUD operations for aggregation_log table
    - Aggregation job status tracking
    - Completion verification for safe cleanup
    """

    def __init__(self, session: Session):
        """
        Initialize repository with database session.

        Args:
            session: SQLAlchemy session object
        """
        self.session = session

    def insert(self, log_data: Dict[str, Any]) -> int:
        """
        Create a new aggregation log entry.

        Args:
            log_data: Dictionary with log fields

        Returns:
            log_id of inserted record

        Raises:
            DatabaseError: If insertion fails
        """
        try:
            # Convert string type to enum if needed
            aggregation_type = log_data.get('aggregation_type')
            if isinstance(aggregation_type, str):
                aggregation_type = AggregationType(aggregation_type)

            status = log_data.get('status', 'running')
            if isinstance(status, str):
                status = AggregationStatus(status)

            log_entry = AggregationLog(
                aggregation_date=log_data['aggregation_date'],
                aggregation_type=aggregation_type,
                started_at=log_data['started_at'],
                status=status,
                parks_processed=log_data.get('parks_processed', 0),
                rides_processed=log_data.get('rides_processed', 0)
            )

            self.session.add(log_entry)
            self.session.flush()

            log_id = log_entry.log_id
            logger.info(f"Created aggregation log entry: {log_id}")
            return log_id

        except Exception as e:
            log_database_error(e, "Failed to insert aggregation log")
            raise

    def update(self, log_data: Dict[str, Any]) -> bool:
        """
        Update an existing aggregation log entry.

        Args:
            log_data: Dictionary with fields to update (must include log_id)

        Returns:
            True if update succeeded, False if record not found

        Raises:
            DatabaseError: If update fails
        """
        if 'log_id' not in log_data:
            raise ValueError("log_id is required for update")

        try:
            log_entry = self.session.get(AggregationLog, log_data['log_id'])

            if log_entry is None:
                return False

            # Update fields dynamically
            for field, value in log_data.items():
                if field != 'log_id' and hasattr(log_entry, field):
                    # Convert string enums to enum types
                    if field == 'aggregation_type' and isinstance(value, str):
                        value = AggregationType(value)
                    elif field == 'status' and isinstance(value, str):
                        value = AggregationStatus(value)

                    setattr(log_entry, field, value)

            self.session.flush()
            return True

        except Exception as e:
            log_database_error(e, f"Failed to update aggregation log {log_data['log_id']}")
            raise

    def get_by_id(self, log_id: int) -> Optional[Dict[str, Any]]:
        """
        Get aggregation log by ID.

        Args:
            log_id: Log ID

        Returns:
            Dictionary with log data or None if not found
        """
        log_entry = self.session.get(AggregationLog, log_id)

        if log_entry is None:
            return None

        return {
            'log_id': log_entry.log_id,
            'aggregation_date': log_entry.aggregation_date,
            'aggregation_type': log_entry.aggregation_type.value,
            'started_at': log_entry.started_at,
            'completed_at': log_entry.completed_at,
            'status': log_entry.status.value,
            'parks_processed': log_entry.parks_processed,
            'rides_processed': log_entry.rides_processed,
            'error_message': log_entry.error_message
        }

    def get_by_date_and_type(
        self,
        aggregation_date: date,
        aggregation_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get aggregation log by date and type.

        Args:
            aggregation_date: Date of aggregation
            aggregation_type: Type (daily, weekly, monthly, yearly)

        Returns:
            Dictionary with log data or None if not found
        """
        # Convert string to enum
        agg_type = AggregationType(aggregation_type)

        stmt = (
            select(AggregationLog)
            .where(
                AggregationLog.aggregation_date == aggregation_date,
                AggregationLog.aggregation_type == agg_type
            )
            .order_by(AggregationLog.started_at.desc())
            .limit(1)
        )

        log_entry = self.session.execute(stmt).scalar_one_or_none()

        if log_entry is None:
            return None

        return {
            'log_id': log_entry.log_id,
            'aggregation_date': log_entry.aggregation_date,
            'aggregation_type': log_entry.aggregation_type.value,
            'started_at': log_entry.started_at,
            'completed_at': log_entry.completed_at,
            'status': log_entry.status.value,
            'parks_processed': log_entry.parks_processed,
            'rides_processed': log_entry.rides_processed,
            'error_message': log_entry.error_message
        }

    def get_recent_logs(
        self,
        aggregation_type: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get recent aggregation logs.

        Args:
            aggregation_type: Optional type filter (daily, weekly, monthly, yearly)
            limit: Maximum number of results

        Returns:
            List of dictionaries with log data
        """
        stmt = select(AggregationLog)

        if aggregation_type:
            agg_type = AggregationType(aggregation_type)
            stmt = stmt.where(AggregationLog.aggregation_type == agg_type)

        stmt = stmt.order_by(AggregationLog.started_at.desc()).limit(limit)

        results = self.session.execute(stmt).scalars().all()

        return [
            {
                'log_id': log.log_id,
                'aggregation_date': log.aggregation_date,
                'aggregation_type': log.aggregation_type.value,
                'started_at': log.started_at,
                'completed_at': log.completed_at,
                'status': log.status.value,
                'parks_processed': log.parks_processed,
                'rides_processed': log.rides_processed,
                'error_message': log.error_message
            }
            for log in results
        ]

    def get_failed_aggregations(
        self,
        days: int = 7
    ) -> List[Dict[str, Any]]:
        """
        Get failed aggregation attempts from the last N days.

        Args:
            days: Number of days to look back

        Returns:
            List of dictionaries with failed log data
        """
        start_date = datetime.now() - timedelta(days=days)

        stmt = (
            select(AggregationLog)
            .where(
                AggregationLog.status == AggregationStatus.FAILED,
                AggregationLog.started_at >= start_date
            )
            .order_by(AggregationLog.started_at.desc())
        )

        results = self.session.execute(stmt).scalars().all()

        return [
            {
                'log_id': log.log_id,
                'aggregation_date': log.aggregation_date,
                'aggregation_type': log.aggregation_type.value,
                'started_at': log.started_at,
                'completed_at': log.completed_at,
                'status': log.status.value,
                'parks_processed': log.parks_processed,
                'rides_processed': log.rides_processed,
                'error_message': log.error_message
            }
            for log in results
        ]

    def mark_complete(
        self,
        log_id: int,
        parks_processed: int = 0,
        rides_processed: int = 0
    ) -> bool:
        """
        Mark an aggregation as successfully completed.

        Args:
            log_id: Log ID to update
            parks_processed: Number of parks processed
            rides_processed: Number of rides processed

        Returns:
            True if update succeeded
        """
        try:
            log_entry = self.session.get(AggregationLog, log_id)

            if log_entry is None:
                return False

            log_entry.status = AggregationStatus.SUCCESS
            log_entry.completed_at = datetime.now()
            log_entry.parks_processed = parks_processed
            log_entry.rides_processed = rides_processed

            self.session.flush()
            logger.info(f"Marked aggregation {log_id} as complete")
            return True

        except Exception as e:
            log_database_error(e, f"Failed to mark aggregation {log_id} as complete")
            raise

    def mark_failed(
        self,
        log_id: int,
        error_message: str
    ) -> bool:
        """
        Mark an aggregation as failed.

        Args:
            log_id: Log ID to update
            error_message: Error details

        Returns:
            True if update succeeded
        """
        try:
            log_entry = self.session.get(AggregationLog, log_id)

            if log_entry is None:
                return False

            log_entry.status = AggregationStatus.FAILED
            log_entry.completed_at = datetime.now()
            log_entry.error_message = error_message

            self.session.flush()
            logger.error(f"Marked aggregation {log_id} as failed: {error_message}")
            return True

        except Exception as e:
            log_database_error(e, f"Failed to mark aggregation {log_id} as failed")
            raise

    def is_date_aggregated(
        self,
        aggregation_date: date,
        aggregation_type: str = 'daily'
    ) -> bool:
        """
        Check if a specific date has been successfully aggregated.

        Used for safe cleanup - only delete raw data after aggregation verified.

        Args:
            aggregation_date: Date to check
            aggregation_type: Type (daily, weekly, monthly, yearly)

        Returns:
            True if date has successful aggregation
        """
        agg_type = AggregationType(aggregation_type)

        stmt = (
            select(func.count(AggregationLog.log_id))
            .where(
                AggregationLog.aggregation_date == aggregation_date,
                AggregationLog.aggregation_type == agg_type,
                AggregationLog.status == AggregationStatus.SUCCESS
            )
        )

        count = self.session.execute(stmt).scalar()
        return count > 0 if count else False

    def get_aggregation_status(
        self,
        aggregation_date: date,
        aggregation_type: str = 'daily'
    ) -> Optional[str]:
        """
        Get the status of an aggregation for a specific date.

        Args:
            aggregation_date: Date to check
            aggregation_type: Type (daily, weekly, monthly, yearly)

        Returns:
            Status string ('success', 'failed', 'running') or None if not found
        """
        log = self.get_by_date_and_type(aggregation_date, aggregation_type)
        return log['status'] if log else None

    def get_last_successful_aggregation(
        self,
        aggregation_type: str = 'daily'
    ) -> Optional[Dict[str, Any]]:
        """
        Get the most recent successful aggregation of specified type.

        Args:
            aggregation_type: Type (daily, weekly, monthly, yearly)

        Returns:
            Dictionary with log data or None if not found
        """
        agg_type = AggregationType(aggregation_type)

        stmt = (
            select(AggregationLog)
            .where(
                AggregationLog.aggregation_type == agg_type,
                AggregationLog.status == AggregationStatus.SUCCESS
            )
            .order_by(AggregationLog.aggregation_date.desc())
            .limit(1)
        )

        log_entry = self.session.execute(stmt).scalar_one_or_none()

        if log_entry is None:
            return None

        return {
            'log_id': log_entry.log_id,
            'aggregation_date': log_entry.aggregation_date,
            'aggregation_type': log_entry.aggregation_type.value,
            'started_at': log_entry.started_at,
            'completed_at': log_entry.completed_at,
            'status': log_entry.status.value,
            'parks_processed': log_entry.parks_processed,
            'rides_processed': log_entry.rides_processed,
            'error_message': log_entry.error_message
        }
