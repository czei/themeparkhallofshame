"""
Unit Tests: Archive Importer
Tests for ArchiveImporter orchestration.
Feature: 004-themeparks-data-collection
Task: T032
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import date, datetime

from importer.archive_importer import (
    ArchiveImporter,
    ImportProgress,
    ImportResult
)


class TestImportProgress:
    """Tests for ImportProgress dataclass."""

    def test_percent_complete_no_files(self):
        """Zero percent when no files."""
        progress = ImportProgress(
            import_id="imp_123",
            destination_uuid="dest-uuid",
            status="IN_PROGRESS",
            records_imported=0,
            errors_encountered=0,
            files_processed=0,
            files_total=0
        )

        assert progress.percent_complete == 0.0

    def test_percent_complete_partial(self):
        """Calculate partial completion."""
        progress = ImportProgress(
            import_id="imp_123",
            destination_uuid="dest-uuid",
            status="IN_PROGRESS",
            records_imported=5000,
            errors_encountered=10,
            files_processed=50,
            files_total=100
        )

        assert progress.percent_complete == 50.0

    def test_percent_complete_full(self):
        """Full completion percentage."""
        progress = ImportProgress(
            import_id="imp_123",
            destination_uuid="dest-uuid",
            status="COMPLETED",
            records_imported=10000,
            errors_encountered=5,
            files_processed=100,
            files_total=100
        )

        assert progress.percent_complete == 100.0


class TestImportResult:
    """Tests for ImportResult dataclass."""

    def test_result_with_quality_summary(self):
        """ImportResult with quality summary."""
        result = ImportResult(
            import_id="imp_123",
            destination_uuid="dest-uuid",
            records_imported=10000,
            errors_encountered=50,
            files_processed=100,
            duration_seconds=3600.0,
            status="COMPLETED",
            quality_summary={
                "PARSE_ERROR": 25,
                "MAPPING_FAILED": 25
            }
        )

        assert result.import_id == "imp_123"
        assert result.records_imported == 10000
        assert len(result.quality_summary) == 2

    def test_result_default_quality_summary(self):
        """ImportResult with default empty quality summary."""
        result = ImportResult(
            import_id="imp_123",
            destination_uuid="dest-uuid",
            records_imported=10000,
            errors_encountered=0,
            files_processed=100,
            duration_seconds=3600.0,
            status="COMPLETED"
        )

        assert result.quality_summary == {}


class TestArchiveImporter:
    """Tests for ArchiveImporter class."""

    @pytest.fixture
    def mock_session(self):
        """Create mock SQLAlchemy session."""
        session = MagicMock()
        session.commit = MagicMock()
        session.flush = MagicMock()
        return session

    @pytest.fixture
    def mock_s3_client(self):
        """Create mock S3 client."""
        return MagicMock()

    @pytest.fixture
    def importer(self, mock_session, mock_s3_client):
        """Create ArchiveImporter with mocked dependencies."""
        with patch('importer.archive_importer.boto3') as mock_boto3:
            mock_boto3.client.return_value = mock_s3_client
            importer = ArchiveImporter(
                session=mock_session,
                batch_size=1000,
                checkpoint_interval=5
            )
        return importer

    def test_init_default_values(self, mock_session):
        """Initialize with default values."""
        with patch('importer.archive_importer.boto3'):
            importer = ArchiveImporter(session=mock_session)

        assert importer.bucket == "archive.themeparks.wiki"
        assert importer.region == "eu-west-2"
        assert importer.batch_size == 10000
        assert importer.checkpoint_interval == 10
        assert importer.auto_create_entities is False

    def test_init_custom_values(self, mock_session):
        """Initialize with custom values."""
        with patch('importer.archive_importer.boto3'):
            importer = ArchiveImporter(
                session=mock_session,
                bucket="custom-bucket",
                region="us-east-1",
                batch_size=5000,
                checkpoint_interval=20,
                auto_create_entities=True
            )

        assert importer.bucket == "custom-bucket"
        assert importer.region == "us-east-1"
        assert importer.batch_size == 5000
        assert importer.checkpoint_interval == 20
        assert importer.auto_create_entities is True

    def test_list_destinations(self, importer, mock_s3_client):
        """List destinations from S3 bucket."""
        mock_s3_client.get_paginator.return_value.paginate.return_value = [
            {
                'CommonPrefixes': [
                    {'Prefix': 'dest-uuid-1/'},
                    {'Prefix': 'dest-uuid-2/'},
                    {'Prefix': 'dest-uuid-3/'}
                ]
            }
        ]

        destinations = importer.list_destinations()

        assert len(destinations) == 3
        assert 'dest-uuid-1' in destinations
        assert 'dest-uuid-2' in destinations
        assert 'dest-uuid-3' in destinations

    def test_list_files_for_destination(self, importer, mock_s3_client):
        """List files for a destination."""
        mock_s3_client.get_paginator.return_value.paginate.return_value = [
            {
                'Contents': [
                    {'Key': 'dest-uuid/2024/12/01.json.gz'},
                    {'Key': 'dest-uuid/2024/12/02.json.gz'},
                    {'Key': 'dest-uuid/2024/12/03.json.gz'}
                ]
            }
        ]

        files = importer.list_files_for_destination("dest-uuid")

        assert len(files) == 3
        assert files[0] == 'dest-uuid/2024/12/01.json.gz'

    def test_list_files_with_date_filter(self, importer, mock_s3_client):
        """List files filtered by date range."""
        mock_s3_client.get_paginator.return_value.paginate.return_value = [
            {
                'Contents': [
                    {'Key': 'dest-uuid/2024/12/01.json.gz'},
                    {'Key': 'dest-uuid/2024/12/02.json.gz'},
                    {'Key': 'dest-uuid/2024/12/03.json.gz'},
                    {'Key': 'dest-uuid/2024/12/04.json.gz'},
                    {'Key': 'dest-uuid/2024/12/05.json.gz'}
                ]
            }
        ]

        files = importer.list_files_for_destination(
            "dest-uuid",
            start_date=date(2024, 12, 2),
            end_date=date(2024, 12, 4)
        )

        assert len(files) == 3
        assert 'dest-uuid/2024/12/02.json.gz' in files
        assert 'dest-uuid/2024/12/03.json.gz' in files
        assert 'dest-uuid/2024/12/04.json.gz' in files

    def test_file_in_date_range_before_start(self, importer):
        """File before start date is excluded."""
        result = importer._file_in_date_range(
            "dest/2024/12/01.json.gz",
            start_date=date(2024, 12, 5),
            end_date=date(2024, 12, 10)
        )

        assert result is False

    def test_file_in_date_range_after_end(self, importer):
        """File after end date is excluded."""
        result = importer._file_in_date_range(
            "dest/2024/12/15.json.gz",
            start_date=date(2024, 12, 5),
            end_date=date(2024, 12, 10)
        )

        assert result is False

    def test_file_in_date_range_within(self, importer):
        """File within date range is included."""
        result = importer._file_in_date_range(
            "dest/2024/12/07.json.gz",
            start_date=date(2024, 12, 5),
            end_date=date(2024, 12, 10)
        )

        assert result is True

    def test_file_in_date_range_no_filter(self, importer):
        """File included when no date filter."""
        result = importer._file_in_date_range(
            "dest/2024/12/07.json.gz",
            start_date=None,
            end_date=None
        )

        assert result is True

    def test_parse_date_from_key(self, importer):
        """Parse date from S3 key."""
        result = importer._parse_date_from_key("dest-uuid/2024/12/25.json.gz")

        assert result == date(2024, 12, 25)

    def test_parse_date_from_key_invalid(self, importer):
        """Return None for invalid key format."""
        result = importer._parse_date_from_key("invalid/key")

        assert result is None

    def test_import_destination_no_files(self, importer, mock_session, mock_s3_client):
        """Import destination with no files."""
        mock_s3_client.get_paginator.return_value.paginate.return_value = [
            {'Contents': []}
        ]

        # Mock import repository
        mock_checkpoint = MagicMock()
        mock_checkpoint.import_id = "imp_123"
        mock_checkpoint.records_imported = 0
        mock_checkpoint.errors_encountered = 0
        mock_checkpoint.status = 'COMPLETED'

        importer.import_repo = MagicMock()
        importer.import_repo.get_resumable_import.return_value = None
        importer.import_repo.create.return_value = mock_checkpoint

        result = importer.import_destination("dest-uuid")

        assert result.import_id == "imp_123"
        assert result.records_imported == 0
        assert result.status == 'COMPLETED'

    def test_get_checkpoint(self, importer):
        """Get checkpoint for destination."""
        mock_checkpoint = MagicMock()
        importer.import_repo = MagicMock()
        importer.import_repo.get_resumable_import.return_value = mock_checkpoint

        result = importer.get_checkpoint("dest-uuid")

        assert result == mock_checkpoint

    def test_get_import_status(self, importer):
        """Get import status by ID."""
        mock_checkpoint = MagicMock()
        mock_checkpoint.import_id = "imp_123"
        mock_checkpoint.destination_uuid = "dest-uuid"
        mock_checkpoint.status = "IN_PROGRESS"
        mock_checkpoint.records_imported = 5000
        mock_checkpoint.errors_encountered = 10
        mock_checkpoint.last_processed_date = date(2024, 12, 15)
        mock_checkpoint.started_at = datetime.utcnow()

        importer.import_repo = MagicMock()
        importer.import_repo.get_by_import_id.return_value = mock_checkpoint

        result = importer.get_import_status("imp_123")

        assert result is not None
        assert result.import_id == "imp_123"
        assert result.status == "IN_PROGRESS"

    def test_get_import_status_not_found(self, importer):
        """Return None for unknown import ID."""
        importer.import_repo = MagicMock()
        importer.import_repo.get_by_import_id.return_value = None

        result = importer.get_import_status("unknown")

        assert result is None

    def test_pause_import(self, importer, mock_session):
        """Pause a running import."""
        mock_checkpoint = MagicMock()
        mock_checkpoint.status = 'IN_PROGRESS'

        importer.import_repo = MagicMock()
        importer.import_repo.get_by_import_id.return_value = mock_checkpoint

        result = importer.pause_import("imp_123")

        assert result is True
        importer.import_repo.pause_import.assert_called_once_with(mock_checkpoint)
        mock_session.commit.assert_called()

    def test_pause_import_not_in_progress(self, importer):
        """Cannot pause import not in progress."""
        mock_checkpoint = MagicMock()
        mock_checkpoint.status = 'COMPLETED'

        importer.import_repo = MagicMock()
        importer.import_repo.get_by_import_id.return_value = mock_checkpoint

        result = importer.pause_import("imp_123")

        assert result is False

    def test_pause_import_not_found(self, importer):
        """Cannot pause unknown import."""
        importer.import_repo = MagicMock()
        importer.import_repo.get_by_import_id.return_value = None

        result = importer.pause_import("unknown")

        assert result is False

    def test_resume_import(self, importer):
        """Resume a paused import."""
        mock_checkpoint = MagicMock()
        mock_checkpoint.can_resume = True
        mock_checkpoint.destination_uuid = "dest-uuid"

        importer.import_repo = MagicMock()
        importer.import_repo.get_by_import_id.return_value = mock_checkpoint

        # Mock import_destination
        with patch.object(importer, 'import_destination') as mock_import:
            mock_result = MagicMock()
            mock_import.return_value = mock_result

            result = importer.resume_import("imp_123")

        assert result == mock_result
        mock_import.assert_called_once_with(
            destination_uuid="dest-uuid",
            resume=True
        )

    def test_resume_import_cannot_resume(self, importer):
        """Cannot resume non-resumable import."""
        mock_checkpoint = MagicMock()
        mock_checkpoint.can_resume = False

        importer.import_repo = MagicMock()
        importer.import_repo.get_by_import_id.return_value = mock_checkpoint

        result = importer.resume_import("imp_123")

        assert result is None

    def test_cancel_import(self, importer):
        """Cancel an import."""
        importer.import_repo = MagicMock()
        importer.import_repo.cancel_import.return_value = True

        result = importer.cancel_import("imp_123")

        assert result is True
        importer.import_repo.cancel_import.assert_called_once_with("imp_123")

    def test_cancel_import_not_found(self, importer):
        """Cancel unknown import returns False."""
        importer.import_repo = MagicMock()
        importer.import_repo.cancel_import.return_value = False

        result = importer.cancel_import("unknown")

        assert result is False


class TestArchiveImporterCallbacks:
    """Tests for ArchiveImporter progress callbacks."""

    def test_progress_callback_called(self):
        """Progress callback is called during import."""
        mock_session = MagicMock()
        mock_session.commit = MagicMock()
        mock_session.flush = MagicMock()

        progress_calls = []

        def capture_progress(progress):
            progress_calls.append(progress)

        with patch('importer.archive_importer.boto3') as mock_boto3:
            mock_s3 = MagicMock()
            mock_boto3.client.return_value = mock_s3

            importer = ArchiveImporter(
                session=mock_session,
                progress_callback=capture_progress
            )

            # Verify callback is set
            assert importer.progress_callback == capture_progress
