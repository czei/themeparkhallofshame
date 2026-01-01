"""
Archive Importer for ThemeParks.wiki Historical Data
Orchestrates S3 file listing, parsing, ID mapping, and database insertion.
Feature: 004-themeparks-data-collection
"""

import os
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Iterator, Callable
from dataclasses import dataclass, field

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from sqlalchemy.orm import Session

from importer.archive_parser import ArchiveParser, ArchiveEvent, ArchiveParseError, DecompressionError
from importer.id_mapper import IDMapper, MappingResult
from database.repositories.import_repository import ImportRepository
from database.repositories.quality_log_repository import QualityLogRepository
from models.orm_snapshots import RideStatusSnapshot
from models.orm_import import ImportCheckpoint
from models.orm_quality_log import DataQualityLog

logger = logging.getLogger(__name__)


@dataclass
class ImportProgress:
    """Progress information for an import."""
    import_id: str
    destination_uuid: str
    status: str
    records_imported: int
    errors_encountered: int
    files_processed: int
    files_total: int
    current_date: Optional[date] = None
    started_at: Optional[datetime] = None
    eta_minutes: Optional[int] = None

    @property
    def percent_complete(self) -> float:
        """Calculate completion percentage."""
        if self.files_total == 0:
            return 0.0
        return (self.files_processed / self.files_total) * 100


@dataclass
class ImportResult:
    """Result of a completed import."""
    import_id: str
    destination_uuid: str
    records_imported: int
    errors_encountered: int
    files_processed: int
    duration_seconds: float
    status: str
    quality_summary: Dict[str, int] = field(default_factory=dict)


class ArchiveImporter:
    """
    Imports historical data from archive.themeparks.wiki S3 bucket.

    Features:
    - Resumable checkpoints for interrupted imports
    - Batch processing with configurable size
    - Error logging to data_quality_log
    - ID reconciliation with fuzzy matching
    - Progress callbacks for monitoring
    """

    # Default S3 bucket settings
    DEFAULT_BUCKET = "archive.themeparks.wiki"
    DEFAULT_REGION = "eu-west-2"

    def __init__(
        self,
        session: Session,
        bucket: Optional[str] = None,
        region: Optional[str] = None,
        batch_size: int = 10000,
        checkpoint_interval: int = 10,
        auto_create_entities: bool = False,
        progress_callback: Optional[Callable[[ImportProgress], None]] = None
    ):
        """
        Initialize archive importer.

        Args:
            session: SQLAlchemy session
            bucket: S3 bucket name (default: archive.themeparks.wiki)
            region: S3 region (default: eu-west-2)
            batch_size: Number of records per batch
            checkpoint_interval: Number of batches between checkpoints
            auto_create_entities: Whether to auto-create unknown rides
            progress_callback: Optional callback for progress updates
        """
        self.session = session
        self.bucket = bucket or os.getenv('ARCHIVE_S3_BUCKET', self.DEFAULT_BUCKET)
        self.region = region or os.getenv('ARCHIVE_S3_REGION', self.DEFAULT_REGION)
        self.batch_size = int(os.getenv('IMPORT_BATCH_SIZE', str(batch_size)))
        self.checkpoint_interval = int(os.getenv('IMPORT_CHECKPOINT_INTERVAL', str(checkpoint_interval)))
        self.auto_create_entities = auto_create_entities
        self.progress_callback = progress_callback

        # Initialize components
        self.parser = ArchiveParser()
        self.id_mapper = IDMapper(session, auto_create=auto_create_entities)
        self.import_repo = ImportRepository(session)
        self.quality_repo = QualityLogRepository(session)

        # S3 client with anonymous access (bucket is public)
        self.s3_client = boto3.client(
            's3',
            region_name=self.region,
            config=Config(signature_version=UNSIGNED)
        )

    def list_destinations(self) -> List[str]:
        """
        List all destination UUIDs in the archive.

        Returns:
            List of destination UUID strings
        """
        destinations = []
        paginator = self.s3_client.get_paginator('list_objects_v2')

        for page in paginator.paginate(Bucket=self.bucket, Delimiter='/'):
            for prefix in page.get('CommonPrefixes', []):
                # Extract UUID from prefix like "abc123-def456-.../"
                dest_uuid = prefix['Prefix'].rstrip('/')
                destinations.append(dest_uuid)

        return destinations

    def list_files_for_destination(
        self,
        destination_uuid: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[str]:
        """
        List all archive files for a destination.

        Args:
            destination_uuid: ThemeParks.wiki destination UUID
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            List of S3 object keys
        """
        files = []
        prefix = f"{destination_uuid}/"
        paginator = self.s3_client.get_paginator('list_objects_v2')

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                # Parse date from key: destination/YYYY/MM/DD.json.gz
                if self._file_in_date_range(key, start_date, end_date):
                    files.append(key)

        return sorted(files)

    def _file_in_date_range(
        self,
        key: str,
        start_date: Optional[date],
        end_date: Optional[date]
    ) -> bool:
        """Check if file falls within date range."""
        try:
            # Parse key: destination/YYYY/MM/DD.json.gz
            parts = key.split('/')
            if len(parts) < 4:
                return True  # Can't determine, include it

            year = int(parts[1])
            month = int(parts[2])
            day = int(parts[3].split('.')[0])
            file_date = date(year, month, day)

            if start_date and file_date < start_date:
                return False
            if end_date and file_date > end_date:
                return False
            return True
        except (ValueError, IndexError):
            return True  # Can't parse, include it

    def _parse_date_from_key(self, key: str) -> Optional[date]:
        """Parse date from S3 key."""
        try:
            parts = key.split('/')
            if len(parts) >= 4:
                year = int(parts[1])
                month = int(parts[2])
                day = int(parts[3].split('.')[0])
                return date(year, month, day)
        except (ValueError, IndexError):
            pass
        return None

    def import_destination(
        self,
        destination_uuid: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        resume: bool = True
    ) -> ImportResult:
        """
        Import all data for a destination.

        Args:
            destination_uuid: ThemeParks.wiki destination UUID
            start_date: Optional start date
            end_date: Optional end date
            resume: Whether to resume from checkpoint

        Returns:
            ImportResult with summary statistics
        """
        start_time = datetime.now(timezone.utc)

        # Check for existing checkpoint
        checkpoint = None
        if resume:
            checkpoint = self.import_repo.get_resumable_import(destination_uuid)

        if checkpoint:
            logger.info(f"Resuming import {checkpoint.import_id} from {checkpoint.last_processed_date}")
            # Adjust start date to resume point
            if checkpoint.last_processed_date:
                start_date = checkpoint.last_processed_date + timedelta(days=1)
        else:
            # Create new checkpoint
            checkpoint = self.import_repo.create(destination_uuid)
            logger.info(f"Starting new import {checkpoint.import_id} for {destination_uuid}")

        # Get file list
        files = self.list_files_for_destination(destination_uuid, start_date, end_date)
        files_total = len(files)
        logger.info(f"Found {files_total} files to process")

        if files_total == 0:
            checkpoint.complete()
            self.session.commit()
            return ImportResult(
                import_id=checkpoint.import_id,
                destination_uuid=destination_uuid,
                records_imported=checkpoint.records_imported,
                errors_encountered=checkpoint.errors_encountered,
                files_processed=0,
                duration_seconds=(datetime.now(timezone.utc) - start_time).total_seconds(),
                status='COMPLETED'
            )

        # Start import
        self.import_repo.start_import(checkpoint)
        self.session.commit()

        files_processed = 0
        batch = []
        batch_count = 0

        try:
            for file_key in files:
                file_date = self._parse_date_from_key(file_key)

                try:
                    # Download and parse file
                    events = self._download_and_parse(file_key)
                    logger.debug(f"Parsed {len(events)} events from {file_key}")

                    # Process events
                    for event in events:
                        snapshot = self._event_to_snapshot(event, checkpoint.import_id)
                        if snapshot:
                            batch.append(snapshot)

                        # Process batch when full
                        if len(batch) >= self.batch_size:
                            self._save_batch(batch)
                            batch = []
                            batch_count += 1

                            # Checkpoint periodically
                            if batch_count % self.checkpoint_interval == 0:
                                self.import_repo.update_progress(
                                    checkpoint, file_date or date.today(), file_key,
                                    self.batch_size * self.checkpoint_interval
                                )
                                self.session.commit()

                    files_processed += 1

                    # Report progress
                    if self.progress_callback:
                        progress = ImportProgress(
                            import_id=checkpoint.import_id,
                            destination_uuid=destination_uuid,
                            status=checkpoint.status,
                            records_imported=checkpoint.records_imported + len(batch),
                            errors_encountered=checkpoint.errors_encountered,
                            files_processed=files_processed,
                            files_total=files_total,
                            current_date=file_date,
                            started_at=checkpoint.started_at
                        )
                        self.progress_callback(progress)

                except (ArchiveParseError, DecompressionError) as e:
                    logger.error(f"Error processing {file_key}: {e}")
                    self.import_repo.record_error(checkpoint)
                    self._log_quality_issue(
                        'PARSE_ERROR',
                        file_key,
                        str(e),
                        checkpoint.import_id
                    )

            # Save remaining batch
            if batch:
                self._save_batch(batch)

            # Complete import
            self.import_repo.complete_import(checkpoint)
            self.session.commit()

        except Exception as e:
            logger.exception(f"Import failed: {e}")
            self.import_repo.fail_import(checkpoint)
            self.session.commit()
            raise

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        quality_summary = self.quality_repo.count_by_type(checkpoint.import_id)

        return ImportResult(
            import_id=checkpoint.import_id,
            destination_uuid=destination_uuid,
            records_imported=checkpoint.records_imported,
            errors_encountered=checkpoint.errors_encountered,
            files_processed=files_processed,
            duration_seconds=duration,
            status=checkpoint.status,
            quality_summary=quality_summary
        )

    def _download_and_parse(self, file_key: str) -> List[ArchiveEvent]:
        """Download and parse a single archive file."""
        response = self.s3_client.get_object(Bucket=self.bucket, Key=file_key)
        content = response['Body'].read()
        return self.parser.parse_s3_content(content)

    def _event_to_snapshot(
        self,
        event: ArchiveEvent,
        import_id: str
    ) -> Optional[RideStatusSnapshot]:
        """
        Convert archive event to ride status snapshot.

        Args:
            event: Parsed archive event
            import_id: Import ID for quality logging

        Returns:
            RideStatusSnapshot or None if mapping fails
        """
        # Map entity to internal IDs
        mapping = self.id_mapper.map_entity_from_event(
            entity_id=event.entity_id,
            name=event.name,
            park_id=event.park_id,
            park_slug=event.park_slug
        )

        if not mapping.ride_id:
            self._log_quality_issue(
                'MAPPING_FAILED',
                event.entity_id,
                f"Could not map entity: {event.name}",
                import_id
            )
            return None

        # Create snapshot
        return RideStatusSnapshot(
            ride_id=mapping.ride_id,
            recorded_at=event.event_time,
            status=event.status if event.status in ('OPERATING', 'DOWN', 'CLOSED', 'REFURBISHMENT') else None,
            wait_time=event.wait_time,
            is_open=event.is_operating,
            computed_is_open=event.is_operating,
            data_source='ARCHIVE',
            last_updated_api=event.event_time
        )

    def _save_batch(self, batch: List[RideStatusSnapshot]) -> None:
        """Save a batch of snapshots to database."""
        for snapshot in batch:
            self.session.add(snapshot)
        self.session.flush()
        logger.debug(f"Saved batch of {len(batch)} snapshots")

    def _log_quality_issue(
        self,
        issue_type: str,
        entity_id: str,
        description: str,
        import_id: str
    ) -> None:
        """Log a quality issue."""
        try:
            self.quality_repo.create(
                issue_type=issue_type,
                entity_type='archive_file' if '/' in entity_id else 'ride',
                external_id=entity_id,
                timestamp_start=datetime.now(timezone.utc),
                description=description,
                import_id=import_id
            )
        except Exception as e:
            logger.warning(f"Failed to log quality issue: {e}")

    def get_checkpoint(self, destination_uuid: str) -> Optional[ImportCheckpoint]:
        """Get existing checkpoint for destination."""
        return self.import_repo.get_resumable_import(destination_uuid)

    def get_import_status(self, import_id: str) -> Optional[ImportProgress]:
        """Get current status of an import."""
        checkpoint = self.import_repo.get_by_import_id(import_id)
        if not checkpoint:
            return None

        # Estimate files processed from records
        avg_events_per_file = 1000  # Rough estimate
        files_processed = checkpoint.records_imported // avg_events_per_file

        return ImportProgress(
            import_id=checkpoint.import_id,
            destination_uuid=checkpoint.destination_uuid,
            status=checkpoint.status,
            records_imported=checkpoint.records_imported,
            errors_encountered=checkpoint.errors_encountered,
            files_processed=files_processed,
            files_total=0,  # Would need to list files to know
            current_date=checkpoint.last_processed_date,
            started_at=checkpoint.started_at
        )

    def pause_import(self, import_id: str) -> bool:
        """Pause a running import."""
        checkpoint = self.import_repo.get_by_import_id(import_id)
        if not checkpoint or checkpoint.status != 'IN_PROGRESS':
            return False
        self.import_repo.pause_import(checkpoint)
        self.session.commit()
        return True

    def resume_import(self, import_id: str) -> Optional[ImportResult]:
        """Resume a paused import."""
        checkpoint = self.import_repo.get_by_import_id(import_id)
        if not checkpoint or not checkpoint.can_resume:
            return None

        return self.import_destination(
            destination_uuid=checkpoint.destination_uuid,
            resume=True
        )

    def cancel_import(self, import_id: str) -> bool:
        """Cancel an import."""
        return self.import_repo.cancel_import(import_id)
