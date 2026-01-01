"""
SQLAlchemy ORM Models: Queue Data
Extended queue information beyond standby wait times.
Feature: 004-themeparks-data-collection
"""

from sqlalchemy import Integer, BigInteger, String, DateTime, Enum, Index, Numeric
from sqlalchemy.orm import Mapped, mapped_column
from models.base import Base
from datetime import datetime
from typing import Optional
from decimal import Decimal
import enum


class QueueType(enum.Enum):
    """Queue type enum matching database ENUM."""
    STANDBY = "STANDBY"
    SINGLE_RIDER = "SINGLE_RIDER"
    RETURN_TIME = "RETURN_TIME"
    PAID_RETURN_TIME = "PAID_RETURN_TIME"
    BOARDING_GROUP = "BOARDING_GROUP"


class QueueData(Base):
    """
    Extended queue information beyond standby wait times.

    Captures Lightning Lane, Virtual Queue, Single Rider, and other
    queue types with pricing and return time information.

    NOTE: No FK constraint to ride_status_snapshots because MySQL does not
    support FK references to partitioned tables. Application-level integrity
    is enforced via import validation.

    ORPHAN CLEANUP STRATEGY:
    Since FK constraints are not possible, orphaned records must be cleaned
    periodically. Use the following query to identify and remove orphans:

        DELETE qd FROM queue_data qd
        LEFT JOIN ride_status_snapshots rss ON qd.snapshot_id = rss.snapshot_id
        WHERE rss.snapshot_id IS NULL;

    This should be run as a scheduled maintenance task (e.g., weekly cron job).
    See: scripts/cleanup_orphaned_queue_data.py
    """
    __tablename__ = "queue_data"

    # Primary Key
    queue_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Reference to snapshot (no FK due to partitioning)
    snapshot_id: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        index=True,
        comment="Reference to ride_status_snapshots.snapshot_id (no FK due to partitioning)"
    )

    # Queue type
    queue_type: Mapped[str] = mapped_column(
        Enum('STANDBY', 'SINGLE_RIDER', 'RETURN_TIME', 'PAID_RETURN_TIME', 'BOARDING_GROUP',
             name='queue_type_enum'),
        nullable=False,
        comment="Type of queue: STANDBY, SINGLE_RIDER, RETURN_TIME, etc."
    )

    # Wait time
    wait_time_minutes: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        comment="Wait time in minutes (NULL if not applicable)"
    )

    # Return time window (for RETURN_TIME, PAID_RETURN_TIME)
    return_time_start: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        comment="Start of return time window"
    )
    return_time_end: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        comment="End of return time window"
    )

    # Pricing (for PAID_RETURN_TIME)
    price_amount: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 2),
        nullable=True,
        comment="Price for paid queue types"
    )
    price_currency: Mapped[Optional[str]] = mapped_column(
        String(3),
        nullable=True,
        comment="ISO 4217 currency code (USD, EUR, etc.)"
    )

    # Boarding group info (for BOARDING_GROUP)
    boarding_group_status: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
        comment="Status: OPEN, CLOSED, DISTRIBUTING"
    )
    boarding_group_current: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
        comment="Current boarding group being called"
    )

    # Denormalized timestamp for efficient queries
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        index=True,
        comment="Timestamp when queue data was recorded"
    )

    # Indexes
    __table_args__ = (
        Index('idx_queue_type_time', 'queue_type', 'recorded_at'),
        {'extend_existing': True}
    )

    @property
    def is_paid_queue(self) -> bool:
        """Check if this is a paid queue type."""
        return self.queue_type == 'PAID_RETURN_TIME'

    @property
    def has_return_window(self) -> bool:
        """Check if return time window is set."""
        return self.return_time_start is not None

    @property
    def price_display(self) -> Optional[str]:
        """Return formatted price string."""
        if self.price_amount is None:
            return None
        currency_symbols = {'USD': '$', 'EUR': '€', 'GBP': '£', 'JPY': '¥'}
        symbol = currency_symbols.get(self.price_currency or 'USD', '$')
        return f"{symbol}{self.price_amount:.2f}"

    @classmethod
    def from_api_data(
        cls,
        snapshot_id: int,
        queue_type: str,
        queue_data: dict,
        recorded_at: datetime
    ) -> "QueueData":
        """
        Create QueueData from ThemeParks.wiki API response.

        Args:
            snapshot_id: Related snapshot ID
            queue_type: Queue type string (STANDBY, SINGLE_RIDER, etc.)
            queue_data: Dict containing queue data from API
            recorded_at: Timestamp of the data

        Returns:
            QueueData instance
        """
        return cls(
            snapshot_id=snapshot_id,
            queue_type=queue_type,
            wait_time_minutes=queue_data.get('waitTime'),
            return_time_start=queue_data.get('returnStart'),
            return_time_end=queue_data.get('returnEnd'),
            price_amount=Decimal(str(queue_data['price']['amount'])) if queue_data.get('price', {}).get('amount') else None,
            price_currency=queue_data.get('price', {}).get('currency'),
            boarding_group_status=queue_data.get('state'),
            boarding_group_current=queue_data.get('currentGroup'),
            recorded_at=recorded_at
        )

    def __repr__(self) -> str:
        return f"<QueueData(queue_id={self.queue_id}, snapshot_id={self.snapshot_id}, type='{self.queue_type}')>"
