"""
Repository: Import Checkpoints
CRUD operations for ImportCheckpoint model.
Feature: 004-themeparks-data-collection
"""

from typing import Optional, List
from datetime import date, datetime
from sqlalchemy.orm import Session
from sqlalchemy import select, update, and_

from models.orm_import import ImportCheckpoint


class ImportRepository:
    """Repository for ImportCheckpoint CRUD operations."""

    def __init__(self, session: Session):
        self.session = session

    def create(
        self,
        destination_uuid: str,
        park_id: Optional[int] = None
    ) -> ImportCheckpoint:
        """
        Create a new import checkpoint.

        Args:
            destination_uuid: ThemeParks.wiki destination UUID
            park_id: Optional internal park ID

        Returns:
            Created ImportCheckpoint instance
        """
        checkpoint = ImportCheckpoint(
            import_id=ImportCheckpoint.generate_import_id(),
            destination_uuid=destination_uuid,
            park_id=park_id,
            status='PENDING'
        )
        self.session.add(checkpoint)
        self.session.flush()
        return checkpoint

    def get_by_id(self, checkpoint_id: int) -> Optional[ImportCheckpoint]:
        """Get checkpoint by internal ID."""
        return self.session.get(ImportCheckpoint, checkpoint_id)

    def get_by_import_id(self, import_id: str) -> Optional[ImportCheckpoint]:
        """Get checkpoint by public import ID."""
        stmt = select(ImportCheckpoint).where(
            ImportCheckpoint.import_id == import_id
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_by_destination(self, destination_uuid: str) -> List[ImportCheckpoint]:
        """Get all checkpoints for a destination."""
        stmt = select(ImportCheckpoint).where(
            ImportCheckpoint.destination_uuid == destination_uuid
        ).order_by(ImportCheckpoint.created_at.desc())
        return self.session.execute(stmt).scalars().all()

    def get_active_imports(self) -> List[ImportCheckpoint]:
        """Get all active (non-completed) imports."""
        stmt = select(ImportCheckpoint).where(
            ImportCheckpoint.status.in_(['PENDING', 'IN_PROGRESS', 'PAUSED'])
        ).order_by(ImportCheckpoint.created_at)
        return self.session.execute(stmt).scalars().all()

    def get_resumable_import(self, destination_uuid: str) -> Optional[ImportCheckpoint]:
        """
        Get a resumable import for destination (PAUSED or FAILED).

        Args:
            destination_uuid: ThemeParks.wiki destination UUID

        Returns:
            Most recent resumable checkpoint or None
        """
        stmt = select(ImportCheckpoint).where(
            and_(
                ImportCheckpoint.destination_uuid == destination_uuid,
                ImportCheckpoint.status.in_(['PAUSED', 'FAILED'])
            )
        ).order_by(ImportCheckpoint.updated_at.desc()).limit(1)
        return self.session.execute(stmt).scalar_one_or_none()

    def list_all(
        self,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[ImportCheckpoint]:
        """
        List imports with optional filtering.

        Args:
            status: Filter by status (optional)
            limit: Maximum results
            offset: Pagination offset

        Returns:
            List of ImportCheckpoint instances
        """
        stmt = select(ImportCheckpoint)
        if status:
            stmt = stmt.where(ImportCheckpoint.status == status)
        stmt = stmt.order_by(ImportCheckpoint.created_at.desc()).limit(limit).offset(offset)
        return self.session.execute(stmt).scalars().all()

    def count(self, status: Optional[str] = None) -> int:
        """Count imports with optional status filter."""
        from sqlalchemy import func
        stmt = select(func.count(ImportCheckpoint.checkpoint_id))
        if status:
            stmt = stmt.where(ImportCheckpoint.status == status)
        return self.session.execute(stmt).scalar() or 0

    def start_import(self, checkpoint: ImportCheckpoint) -> None:
        """Mark import as started."""
        checkpoint.start()
        self.session.flush()

    def pause_import(self, checkpoint: ImportCheckpoint) -> None:
        """Pause the import."""
        checkpoint.pause()
        self.session.flush()

    def resume_import(self, checkpoint: ImportCheckpoint) -> None:
        """Resume a paused import."""
        checkpoint.resume()
        self.session.flush()

    def complete_import(self, checkpoint: ImportCheckpoint) -> None:
        """Mark import as completed successfully."""
        checkpoint.complete()
        self.session.flush()

    def fail_import(self, checkpoint: ImportCheckpoint) -> None:
        """Mark import as failed."""
        checkpoint.fail()
        self.session.flush()

    def update_progress(
        self,
        checkpoint: ImportCheckpoint,
        processed_date: date,
        file_path: str,
        records: int
    ) -> None:
        """
        Update progress with latest processed file.

        Args:
            checkpoint: The checkpoint to update
            processed_date: Date that was processed
            file_path: S3 file path that was processed
            records: Number of records imported from this file
        """
        checkpoint.update_progress(processed_date, file_path, records)
        self.session.flush()

    def record_error(self, checkpoint: ImportCheckpoint) -> None:
        """Increment error counter."""
        checkpoint.record_error()
        self.session.flush()

    def delete(self, checkpoint: ImportCheckpoint) -> None:
        """Delete a checkpoint (use with caution)."""
        self.session.delete(checkpoint)
        self.session.flush()

    def cancel_import(self, import_id: str) -> bool:
        """
        Cancel an import by setting status to CANCELLED.

        Args:
            import_id: Public import ID

        Returns:
            True if import was cancelled, False if not found or already completed
        """
        checkpoint = self.get_by_import_id(import_id)
        if not checkpoint or not checkpoint.is_active:
            return False
        checkpoint.cancel()
        self.session.flush()
        return True

    def get_by_status(
        self,
        status: str,
        limit: int = 100,
        offset: int = 0
    ) -> List[ImportCheckpoint]:
        """
        Get imports filtered by status.

        Args:
            status: Status to filter by
            limit: Maximum results
            offset: Pagination offset

        Returns:
            List of ImportCheckpoint instances
        """
        stmt = select(ImportCheckpoint).where(
            ImportCheckpoint.status == status
        ).order_by(ImportCheckpoint.created_at.desc()).limit(limit).offset(offset)
        return self.session.execute(stmt).scalars().all()

    def get_all(
        self,
        limit: int = 100,
        offset: int = 0
    ) -> List[ImportCheckpoint]:
        """
        Get all imports with pagination.

        Args:
            limit: Maximum results
            offset: Pagination offset

        Returns:
            List of ImportCheckpoint instances
        """
        stmt = select(ImportCheckpoint).order_by(
            ImportCheckpoint.created_at.desc()
        ).limit(limit).offset(offset)
        return self.session.execute(stmt).scalars().all()
