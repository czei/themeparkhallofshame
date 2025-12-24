"""
SQLAlchemy ORM Models: Data Quality Issues
Tracks data quality issues from external APIs for reporting.
"""

from sqlalchemy import Integer, String, ForeignKey, DateTime, Enum, Boolean, Text, Index, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from src.models.base import Base
from datetime import datetime
from typing import Optional
import enum


class DataSource(enum.Enum):
    """Data source enum matching database ENUM."""
    THEMEPARKS_WIKI = "themeparks_wiki"
    QUEUE_TIMES = "queue_times"


class IssueType(enum.Enum):
    """Issue type enum matching database ENUM."""
    STALE_DATA = "STALE_DATA"
    MISSING_DATA = "MISSING_DATA"
    INVALID_STATUS = "INVALID_STATUS"
    INCONSISTENT_DATA = "INCONSISTENT_DATA"


class DataQualityIssue(Base):
    """
    Data quality issues detected from external APIs.
    Tracks stale/invalid data from ThemeParks.wiki and Queue-Times for reporting.
    """
    __tablename__ = "data_quality_issues"

    # Primary Key
    issue_id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Data Source and Issue Type
    data_source: Mapped[str] = mapped_column(
        Enum("themeparks_wiki", "queue_times", name="data_source_enum"),
        nullable=False,
        comment="Which data source had the issue"
    )
    issue_type: Mapped[str] = mapped_column(
        Enum("STALE_DATA", "MISSING_DATA", "INVALID_STATUS", "INCONSISTENT_DATA", name="issue_type_enum"),
        nullable=False,
        comment="Type of data quality issue"
    )

    # When Detected
    detected_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment="When we detected this issue"
    )

    # Park/Ride Context (optional - data might not be in our DB)
    park_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("parks.park_id", ondelete="SET NULL")
    )
    ride_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("rides.ride_id", ondelete="SET NULL")
    )

    # External IDs for reporting upstream
    themeparks_wiki_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        comment="UUID from ThemeParks.wiki API"
    )
    queue_times_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="ID from Queue-Times API"
    )

    # Issue Details
    entity_name: Mapped[Optional[str]] = mapped_column(
        String(255),
        comment="Name of ride/park from API"
    )
    last_updated_api: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        comment="lastUpdated timestamp from API"
    )
    data_age_minutes: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="How stale the data was in minutes"
    )
    reported_status: Mapped[Optional[str]] = mapped_column(
        String(50),
        comment="Status the API returned"
    )

    # Additional Context
    details: Mapped[Optional[str]] = mapped_column(
        Text,
        comment="JSON with additional context"
    )

    # Resolution Tracking
    is_resolved: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="0"
    )
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Composite Indexes for Performance
    __table_args__ = (
        Index('idx_dqi_detected', 'detected_at'),
        Index('idx_dqi_source', 'data_source', 'issue_type'),
        Index('idx_dqi_park', 'park_id', 'detected_at'),
        Index('idx_dqi_ride', 'ride_id', 'detected_at'),
        Index('idx_dqi_unresolved', 'is_resolved', 'detected_at'),
        Index('idx_dqi_wiki_id', 'themeparks_wiki_id'),
        {'extend_existing': True}
    )

    # Note: Relationships to Park and Ride are not defined here because:
    # 1. Foreign keys are nullable (data might not be in our DB)
    # 2. The repository queries join these tables directly when needed
    # 3. Avoids circular import issues with optional relationships

    def __repr__(self) -> str:
        return f"<DataQualityIssue(issue_id={self.issue_id}, source={self.data_source}, type={self.issue_type}, entity={self.entity_name})>"
