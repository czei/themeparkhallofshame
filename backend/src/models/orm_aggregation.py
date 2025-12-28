"""
SQLAlchemy ORM Model: AggregationLog
Tracks aggregation job execution status for safe cleanup operations.
"""

from sqlalchemy import Integer, Date, Enum, DateTime, Text, Index
from sqlalchemy.orm import Mapped, mapped_column
from models.base import Base
from datetime import date, datetime
from typing import Optional
import enum


class AggregationType(str, enum.Enum):
    """Aggregation period types"""
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"


class AggregationStatus(str, enum.Enum):
    """Aggregation job status"""
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class AggregationLog(Base):
    """
    Tracks aggregation job execution status.

    Purpose:
    - Prevents raw data deletion before successful aggregation
    - Enables retry logic (run at 12:10 AM, 1:10 AM, 2:10 AM)
    - Provides audit trail for aggregation jobs
    """
    __tablename__ = "aggregation_log"

    # Primary Key
    log_id: Mapped[int] = mapped_column(primary_key=True)

    # Aggregation Identification
    aggregation_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="Date for which aggregation was performed (local date)"
    )
    aggregation_type: Mapped[AggregationType] = mapped_column(
        Enum(
            AggregationType,
            values_callable=lambda x: [e.value for e in x]
        ),
        nullable=False
    )

    # Job Execution Tracking
    started_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        comment="When aggregation job started"
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        comment="When aggregation job completed successfully"
    )
    status: Mapped[AggregationStatus] = mapped_column(
        Enum(
            AggregationStatus,
            values_callable=lambda x: [e.value for e in x]
        ),
        nullable=False,
        default=AggregationStatus.RUNNING
    )

    # Aggregation Metadata
    aggregated_until_ts: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        comment="Maximum recorded_at timestamp that was aggregated"
    )
    error_message: Mapped[Optional[str]] = mapped_column(
        Text,
        comment="Error details if status = failed"
    )
    parks_processed: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Number of parks successfully aggregated"
    )
    rides_processed: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Number of rides successfully aggregated"
    )

    # Composite Indexes for Performance
    __table_args__ = (
        Index('unique_aggregation', 'aggregation_date', 'aggregation_type', unique=True),
        Index('idx_status', 'status', 'aggregation_date'),
        Index('idx_completed', 'completed_at'),
        {'extend_existing': True}
    )

    def __repr__(self) -> str:
        return f"<AggregationLog(log_id={self.log_id}, date={self.aggregation_date}, type={self.aggregation_type.value}, status={self.status.value})>"
