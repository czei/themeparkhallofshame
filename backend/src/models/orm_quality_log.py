"""
SQLAlchemy ORM Models: Data Quality Log
Data quality issues and gaps tracking for import/collection.
Feature: 004-themeparks-data-collection
"""

from sqlalchemy import Integer, String, DateTime, Enum, Index, Text, JSON, func
from sqlalchemy.orm import Mapped, mapped_column
from models.base import Base
from datetime import datetime
from typing import Optional, Dict, Any
import enum


class LogIssueType(enum.Enum):
    """Issue type enum for data quality log."""
    GAP = "GAP"
    DUPLICATE = "DUPLICATE"
    INVALID = "INVALID"
    MISSING_FIELD = "MISSING_FIELD"
    PARSE_ERROR = "PARSE_ERROR"
    MAPPING_FAILED = "MAPPING_FAILED"


class ResolutionStatus(enum.Enum):
    """Resolution status enum."""
    OPEN = "OPEN"
    INVESTIGATING = "INVESTIGATING"
    RESOLVED = "RESOLVED"
    WONTFIX = "WONTFIX"


class DataQualityLog(Base):
    """
    Data quality issues and gaps tracking for import/collection.

    Logs quality issues encountered during historical import or live collection
    with severity, affected time ranges, and resolution tracking.
    """
    __tablename__ = "data_quality_log"

    # Primary Key
    log_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Import association (NULL for live collection issues)
    import_id: Mapped[Optional[str]] = mapped_column(
        String(20),
        nullable=True,
        index=True,
        comment="Associated import (NULL for live collection issues)"
    )

    # Issue classification
    issue_type: Mapped[str] = mapped_column(
        Enum('GAP', 'DUPLICATE', 'INVALID', 'MISSING_FIELD', 'PARSE_ERROR', 'MAPPING_FAILED',
             name='log_issue_type_enum'),
        nullable=False,
        comment="Type of quality issue"
    )

    # Affected entity
    entity_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="Entity type: ride, park, snapshot, etc."
    )
    entity_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        comment="Internal entity ID (if applicable)"
    )
    external_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        nullable=True,
        comment="External ID (ThemeParks.wiki UUID)"
    )

    # Time range affected
    timestamp_start: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        comment="Start of affected time range"
    )
    timestamp_end: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        comment="End of affected time range (NULL if point-in-time)"
    )

    # Description
    description: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="Human-readable description of the issue"
    )

    # Raw data for debugging
    raw_data: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSON,
        nullable=True,
        comment="Original data that caused the issue"
    )

    # Resolution tracking
    resolution_status: Mapped[str] = mapped_column(
        Enum('OPEN', 'INVESTIGATING', 'RESOLVED', 'WONTFIX', name='resolution_status_enum'),
        nullable=False,
        default='OPEN',
        server_default='OPEN'
    )
    resolution_notes: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Notes about how the issue was resolved"
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now()
    )

    # Indexes
    __table_args__ = (
        Index('idx_quality_log_type', 'issue_type'),
        Index('idx_quality_log_status', 'resolution_status'),
        Index('idx_quality_log_time', 'timestamp_start', 'timestamp_end'),
        {'extend_existing': True}
    )

    @property
    def is_resolved(self) -> bool:
        """Check if issue is resolved."""
        return self.resolution_status in ('RESOLVED', 'WONTFIX')

    @property
    def is_open(self) -> bool:
        """Check if issue is still open."""
        return self.resolution_status == 'OPEN'

    @property
    def duration(self) -> Optional[int]:
        """Return duration of issue in minutes (if time range)."""
        if self.timestamp_end is None:
            return None
        delta = self.timestamp_end - self.timestamp_start
        return int(delta.total_seconds() / 60)

    def investigate(self) -> None:
        """Mark issue as being investigated."""
        self.resolution_status = 'INVESTIGATING'

    def resolve(self, notes: str) -> None:
        """Mark issue as resolved with notes."""
        self.resolution_status = 'RESOLVED'
        self.resolution_notes = notes

    def wontfix(self, notes: str) -> None:
        """Mark issue as won't fix with justification."""
        self.resolution_status = 'WONTFIX'
        self.resolution_notes = notes

    @classmethod
    def log_gap(
        cls,
        entity_type: str,
        entity_id: Optional[int],
        timestamp_start: datetime,
        timestamp_end: datetime,
        description: str,
        import_id: Optional[str] = None
    ) -> "DataQualityLog":
        """Create a GAP issue log entry."""
        return cls(
            import_id=import_id,
            issue_type='GAP',
            entity_type=entity_type,
            entity_id=entity_id,
            timestamp_start=timestamp_start,
            timestamp_end=timestamp_end,
            description=description
        )

    @classmethod
    def log_parse_error(
        cls,
        entity_type: str,
        external_id: str,
        timestamp: datetime,
        description: str,
        raw_data: Dict[str, Any],
        import_id: Optional[str] = None
    ) -> "DataQualityLog":
        """Create a PARSE_ERROR issue log entry."""
        return cls(
            import_id=import_id,
            issue_type='PARSE_ERROR',
            entity_type=entity_type,
            external_id=external_id,
            timestamp_start=timestamp,
            description=description,
            raw_data=raw_data
        )

    @classmethod
    def log_mapping_failed(
        cls,
        entity_type: str,
        external_id: str,
        timestamp: datetime,
        description: str,
        import_id: Optional[str] = None
    ) -> "DataQualityLog":
        """Create a MAPPING_FAILED issue log entry."""
        return cls(
            import_id=import_id,
            issue_type='MAPPING_FAILED',
            entity_type=entity_type,
            external_id=external_id,
            timestamp_start=timestamp,
            description=description
        )

    def __repr__(self) -> str:
        return f"<DataQualityLog(log_id={self.log_id}, type='{self.issue_type}', status='{self.resolution_status}')>"
