"""
Theme Park Downtime Tracker - Aggregation Log Repository
Provides data access layer for aggregation job tracking and verification.
"""

from typing import List, Optional, Dict, Any
from datetime import date, datetime, timedelta
from sqlalchemy import text
from sqlalchemy.engine import Connection

try:
    from ...utils.logger import logger, log_database_error
except ImportError:
    from utils.logger import logger, log_database_error


class AggregationLogRepository:
    """
    Repository for aggregation log operations.

    Implements:
    - CRUD operations for aggregation_log table
    - Aggregation job status tracking
    - Completion verification for safe cleanup
    """

    def __init__(self, connection: Connection):
        """
        Initialize repository with database connection.

        Args:
            connection: SQLAlchemy connection object
        """
        self.conn = connection

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
        query = text("""
            INSERT INTO aggregation_log (
                aggregation_date, aggregation_type, started_at,
                status, parks_processed, rides_processed
            )
            VALUES (
                :aggregation_date, :aggregation_type, :started_at,
                :status, :parks_processed, :rides_processed
            )
        """)

        try:
            result = self.conn.execute(query, log_data)
            log_id = result.lastrowid
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

        # Build dynamic SET clause from provided fields
        set_clauses = []
        params = {"log_id": log_data['log_id']}

        for field, value in log_data.items():
            if field != 'log_id':
                set_clauses.append(f"{field} = :{field}")
                params[field] = value

        if not set_clauses:
            return True  # Nothing to update

        query = text(f"""
            UPDATE aggregation_log
            SET {', '.join(set_clauses)}
            WHERE log_id = :log_id
        """)

        try:
            result = self.conn.execute(query, params)
            return result.rowcount > 0

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
        query = text("""
            SELECT log_id, aggregation_date, aggregation_type,
                   started_at, completed_at, status, parks_processed,
                   rides_processed, error_message
            FROM aggregation_log
            WHERE log_id = :log_id
        """)

        result = self.conn.execute(query, {"log_id": log_id})
        row = result.fetchone()

        if row is None:
            return None

        return dict(row._mapping)

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
        query = text("""
            SELECT log_id, aggregation_date, aggregation_type,
                   started_at, completed_at, status, parks_processed,
                   rides_processed, error_message
            FROM aggregation_log
            WHERE aggregation_date = :aggregation_date
                AND aggregation_type = :aggregation_type
            ORDER BY started_at DESC
            LIMIT 1
        """)

        result = self.conn.execute(query, {
            "aggregation_date": aggregation_date,
            "aggregation_type": aggregation_type
        })
        row = result.fetchone()

        if row is None:
            return None

        return dict(row._mapping)

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
        if aggregation_type:
            query = text("""
                SELECT log_id, aggregation_date, aggregation_type,
                       started_at, completed_at, status, parks_processed,
                       rides_processed, error_message
                FROM aggregation_log
                WHERE aggregation_type = :aggregation_type
                ORDER BY started_at DESC
                LIMIT :limit
            """)
            result = self.conn.execute(query, {
                "aggregation_type": aggregation_type,
                "limit": limit
            })
        else:
            query = text("""
                SELECT log_id, aggregation_date, aggregation_type,
                       started_at, completed_at, status, parks_processed,
                       rides_processed, error_message
                FROM aggregation_log
                ORDER BY started_at DESC
                LIMIT :limit
            """)
            result = self.conn.execute(query, {"limit": limit})

        return [dict(row._mapping) for row in result]

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
        query = text("""
            SELECT log_id, aggregation_date, aggregation_type,
                   started_at, completed_at, status, parks_processed,
                   rides_processed, error_message
            FROM aggregation_log
            WHERE status = 'failed'
                AND started_at >= :start_date
            ORDER BY started_at DESC
        """)

        start_date = datetime.now() - timedelta(days=days)
        result = self.conn.execute(query, {"start_date": start_date})
        return [dict(row._mapping) for row in result]

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
        query = text("""
            UPDATE aggregation_log
            SET status = 'success',
                completed_at = CURRENT_TIMESTAMP,
                parks_processed = :parks_processed,
                rides_processed = :rides_processed
            WHERE log_id = :log_id
        """)

        try:
            result = self.conn.execute(query, {
                "log_id": log_id,
                "parks_processed": parks_processed,
                "rides_processed": rides_processed
            })
            logger.info(f"Marked aggregation {log_id} as complete")
            return result.rowcount > 0

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
        query = text("""
            UPDATE aggregation_log
            SET status = 'failed',
                completed_at = CURRENT_TIMESTAMP,
                error_message = :error_message
            WHERE log_id = :log_id
        """)

        try:
            result = self.conn.execute(query, {
                "log_id": log_id,
                "error_message": error_message
            })
            logger.error(f"Marked aggregation {log_id} as failed: {error_message}")
            return result.rowcount > 0

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
        query = text("""
            SELECT COUNT(*) as count
            FROM aggregation_log
            WHERE aggregation_date = :aggregation_date
                AND aggregation_type = :aggregation_type
                AND status = 'success'
        """)

        result = self.conn.execute(query, {
            "aggregation_date": aggregation_date,
            "aggregation_type": aggregation_type
        })
        row = result.fetchone()

        return row.count > 0 if row else False

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
        query = text("""
            SELECT log_id, aggregation_date, aggregation_type,
                   started_at, completed_at, status, parks_processed,
                   rides_processed, error_message
            FROM aggregation_log
            WHERE aggregation_type = :aggregation_type
                AND status = 'success'
            ORDER BY aggregation_date DESC
            LIMIT 1
        """)

        result = self.conn.execute(query, {"aggregation_type": aggregation_type})
        row = result.fetchone()

        if row is None:
            return None

        return dict(row._mapping)
