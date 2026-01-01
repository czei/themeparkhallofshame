"""
SQLAlchemy ORM Models: Import Checkpoint
Tracks historical import progress for resumable imports from archive.themeparks.wiki.
Feature: 004-themeparks-data-collection
"""

from sqlalchemy import Integer, String, ForeignKey, DateTime, Date, Enum, Index, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from models.base import Base
from datetime import datetime, date, timezone
from typing import Optional, TYPE_CHECKING
import enum
import secrets

if TYPE_CHECKING:
    from models.orm_park import Park


class ImportStatus(enum.Enum):
    """Import status enum matching database ENUM."""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PAUSED = "PAUSED"
    CANCELLED = "CANCELLED"


class ImportCheckpoint(Base):
    """
    Tracks historical import progress for resumable imports.

    Each import checkpoint represents a single destination import operation.
    The import_id is a public-facing identifier for API references.
    """
    __tablename__ = "import_checkpoints"

    # Primary Key
    checkpoint_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Public-facing identifier (e.g., "imp_abc123")
    import_id: Mapped[str] = mapped_column(
        String(20),
        unique=True,
        nullable=False,
        comment="Public-facing import identifier"
    )

    # ThemeParks.wiki destination UUID
    destination_uuid: Mapped[str] = mapped_column(
        String(36),
        nullable=False,
        index=True,
        comment="ThemeParks.wiki destination UUID being imported"
    )

    # Optional internal park reference (if importing specific park)
    park_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("parks.park_id"),
        nullable=True,
        comment="Internal park_id if mapped, NULL if destination-level import"
    )

    # Progress tracking
    last_processed_date: Mapped[Optional[date]] = mapped_column(
        Date,
        nullable=True,
        comment="Last successfully processed date"
    )
    last_processed_file: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        comment="Last processed S3 file path"
    )
    records_imported: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default='0',
        comment="Total records successfully imported"
    )
    errors_encountered: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default='0',
        comment="Total errors encountered during import"
    )

    # Status
    status: Mapped[str] = mapped_column(
        Enum('PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'PAUSED', 'CANCELLED', name='import_status_enum'),
        nullable=False,
        default='PENDING',
        server_default='PENDING'
    )

    # Timestamps
    started_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        comment="When import was started"
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        comment="When import completed (success or failure)"
    )
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
        Index('idx_import_status', 'status'),
        Index('idx_import_destination', 'destination_uuid'),
        {'extend_existing': True}
    )

    # Relationships
    park: Mapped[Optional["Park"]] = relationship(
        "Park",
        back_populates="import_checkpoints"
    )

    @classmethod
    def generate_import_id(cls) -> str:
        """Generate a unique import ID in the format 'imp_abc123def456'."""
        return f"imp_{secrets.token_hex(8)}"

    def start(self) -> None:
        """Mark import as started."""
        self.status = 'IN_PROGRESS'
        self.started_at = datetime.now(timezone.utc)

    def pause(self) -> None:
        """Pause the import."""
        self.status = 'PAUSED'

    def resume(self) -> None:
        """Resume a paused import."""
        self.status = 'IN_PROGRESS'

    def complete(self) -> None:
        """Mark import as completed successfully."""
        self.status = 'COMPLETED'
        self.completed_at = datetime.now(timezone.utc)

    def fail(self) -> None:
        """Mark import as failed."""
        self.status = 'FAILED'
        self.completed_at = datetime.now(timezone.utc)

    def cancel(self) -> None:
        """Cancel the import."""
        self.status = 'CANCELLED'
        self.completed_at = datetime.now(timezone.utc)

    def update_progress(self, processed_date: date, file_path: str, records: int) -> None:
        """Update progress with latest processed file."""
        self.last_processed_date = processed_date
        self.last_processed_file = file_path
        self.records_imported += records

    def record_error(self) -> None:
        """Increment error counter."""
        self.errors_encountered += 1

    @property
    def is_active(self) -> bool:
        """Check if import is currently active."""
        return self.status in ('PENDING', 'IN_PROGRESS', 'PAUSED')

    @property
    def can_resume(self) -> bool:
        """Check if import can be resumed."""
        return self.status in ('PAUSED', 'FAILED')

    def __repr__(self) -> str:
        return f"<ImportCheckpoint(import_id='{self.import_id}', destination='{self.destination_uuid}', status='{self.status}')>"
