"""
Queue Data Collector
Saves extended queue information (Lightning Lane, Virtual Queue, Single Rider, etc.)
to the queue_data table.
Feature: 004-themeparks-data-collection
"""

import logging
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from sqlalchemy.orm import Session

from collector.themeparks_wiki_client import LiveRideData, QueueData as ClientQueueData
from models.orm_queue import QueueData, QueueType

logger = logging.getLogger(__name__)


class QueueDataCollector:
    """
    Collects and stores extended queue data from ThemeParks.wiki API.

    Captures all queue types beyond STANDBY:
    - SINGLE_RIDER: Single rider queue wait times
    - RETURN_TIME: Virtual queue return windows
    - PAID_RETURN_TIME: Lightning Lane/Genie+ with pricing
    - BOARDING_GROUP: Virtual queue boarding groups
    """

    def __init__(self, session: Session):
        """
        Initialize queue data collector.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session
        self._stats = {
            'queues_processed': 0,
            'queues_saved': 0,
            'errors': 0
        }

    @property
    def stats(self) -> dict:
        """Get collection statistics."""
        return self._stats.copy()

    def reset_stats(self) -> None:
        """Reset collection statistics."""
        self._stats = {
            'queues_processed': 0,
            'queues_saved': 0,
            'errors': 0
        }

    def save_queue_data(
        self,
        snapshot_id: int,
        ride_data: LiveRideData,
        recorded_at: datetime
    ) -> List[QueueData]:
        """
        Save all queue data for a ride snapshot.

        Args:
            snapshot_id: ID of the parent ride_status_snapshot
            ride_data: LiveRideData from ThemeParks.wiki client
            recorded_at: Timestamp when data was collected

        Returns:
            List of created QueueData records
        """
        if not ride_data.queues:
            return []

        created_records = []

        for queue_info in ride_data.queues:
            try:
                self._stats['queues_processed'] += 1

                # Skip STANDBY - it's already stored in ride_status_snapshots.wait_time
                if queue_info.queue_type == 'STANDBY':
                    continue

                # Validate queue type
                if queue_info.queue_type not in [qt.value for qt in QueueType]:
                    logger.warning(f"Unknown queue type: {queue_info.queue_type}")
                    continue

                # Create queue data record
                queue_record = self._create_queue_record(
                    snapshot_id=snapshot_id,
                    queue_info=queue_info,
                    recorded_at=recorded_at
                )

                self.session.add(queue_record)
                created_records.append(queue_record)
                self._stats['queues_saved'] += 1

            except Exception as e:
                logger.error(f"Error saving queue data: {e}")
                self._stats['errors'] += 1

        return created_records

    def _create_queue_record(
        self,
        snapshot_id: int,
        queue_info: ClientQueueData,
        recorded_at: datetime
    ) -> QueueData:
        """
        Create a QueueData record from client queue info.

        Args:
            snapshot_id: ID of parent snapshot
            queue_info: QueueData from client
            recorded_at: Collection timestamp

        Returns:
            QueueData ORM instance
        """
        # Parse return time windows
        return_start = None
        return_end = None

        if queue_info.return_start:
            return_start = self._parse_datetime(queue_info.return_start)
        if queue_info.return_end:
            return_end = self._parse_datetime(queue_info.return_end)

        # Parse price
        price_amount = None
        if queue_info.price_amount is not None:
            price_amount = Decimal(str(queue_info.price_amount))

        return QueueData(
            snapshot_id=snapshot_id,
            queue_type=queue_info.queue_type,
            wait_time_minutes=queue_info.wait_time,
            return_time_start=return_start,
            return_time_end=return_end,
            price_amount=price_amount,
            price_currency=queue_info.price_currency,
            boarding_group_status=queue_info.state,
            boarding_group_current=queue_info.current_group,
            recorded_at=recorded_at
        )

    def _parse_datetime(self, value: Optional[str]) -> Optional[datetime]:
        """
        Parse datetime from ISO format string.

        Args:
            value: ISO format datetime string or None

        Returns:
            Parsed datetime or None
        """
        if not value:
            return None

        try:
            # Handle Z suffix
            value = value.replace('Z', '+00:00')
            dt = datetime.fromisoformat(value)
            # Convert to naive datetime (remove timezone) for MySQL
            if dt.tzinfo:
                dt = dt.replace(tzinfo=None)
            return dt
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse datetime '{value}': {e}")
            return None

    def save_batch(
        self,
        snapshot_ride_pairs: List[tuple],
        recorded_at: datetime
    ) -> int:
        """
        Save queue data for multiple rides in a batch.

        Args:
            snapshot_ride_pairs: List of (snapshot_id, LiveRideData) tuples
            recorded_at: Collection timestamp

        Returns:
            Number of queue records saved
        """
        total_saved = 0

        for snapshot_id, ride_data in snapshot_ride_pairs:
            records = self.save_queue_data(snapshot_id, ride_data, recorded_at)
            total_saved += len(records)

        return total_saved
