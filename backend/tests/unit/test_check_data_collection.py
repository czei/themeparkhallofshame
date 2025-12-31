"""
Tests for Data Collection Health Check Script

Tests the monitoring script that checks if data collection is running properly.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Mock sendgrid before importing the script (sendgrid not installed in test env)
sys.modules['sendgrid'] = MagicMock()
sys.modules['sendgrid.helpers'] = MagicMock()
sys.modules['sendgrid.helpers.mail'] = MagicMock()

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from scripts.check_data_collection import (
    check_recent_snapshots,
    get_collection_process_status,
    format_alert_email,
    MAX_DATA_AGE_MINUTES,
    MIN_PARKS_EXPECTED,
    MIN_RIDES_EXPECTED,
)


class TestCheckRecentSnapshots:
    """Tests for check_recent_snapshots function."""

    def test_check_recent_snapshots_with_healthy_data(self):
        """Healthy data should report recent snapshots for both rides and parks."""
        now = datetime.now()
        recent_time = now - timedelta(minutes=10)

        mock_conn = Mock()
        mock_cursor = Mock()

        # Mock ride snapshot query
        mock_cursor.fetchone.side_effect = [
            (recent_time, 100, 500),  # ride_status_snapshots recent
            (recent_time, 10, 50),    # park_activity_snapshots recent
            (recent_time,),           # overall most recent ride
            (recent_time,),           # overall most recent park
        ]

        mock_conn.execute.return_value = mock_cursor

        with patch('scripts.check_data_collection.get_db_session') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_conn

            result = check_recent_snapshots()

            assert result["has_recent_ride_data"] is True
            assert result["has_recent_park_data"] is True
            assert result["rides_with_recent_data"] == 100
            assert result["parks_with_recent_data"] == 10
            assert result["ride_snapshot_count"] == 500
            assert result["park_snapshot_count"] == 50
            assert result["minutes_since_last_ride_snapshot"] is not None
            assert result["minutes_since_last_park_snapshot"] is not None

    def test_check_recent_snapshots_with_stale_data(self):
        """Stale data should report no recent snapshots."""
        now = datetime.now()
        old_time = now - timedelta(hours=2)

        mock_conn = Mock()
        mock_cursor = Mock()

        # No recent data within cutoff window
        mock_cursor.fetchone.side_effect = [
            (None, 0, 0),      # ride_status_snapshots recent (none)
            (None, 0, 0),      # park_activity_snapshots recent (none)
            (old_time,),       # overall most recent ride (2 hours ago)
            (old_time,),       # overall most recent park (2 hours ago)
        ]

        mock_conn.execute.return_value = mock_cursor

        with patch('scripts.check_data_collection.get_db_session') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_conn

            result = check_recent_snapshots()

            assert result["has_recent_ride_data"] is False
            assert result["has_recent_park_data"] is False
            assert result["rides_with_recent_data"] == 0
            assert result["parks_with_recent_data"] == 0
            assert result["minutes_since_last_ride_snapshot"] == 120
            assert result["minutes_since_last_park_snapshot"] == 120

    def test_check_recent_snapshots_with_insufficient_rides(self):
        """Too few rides with recent data should fail the check."""
        now = datetime.now()
        recent_time = now - timedelta(minutes=10)

        mock_conn = Mock()
        mock_cursor = Mock()

        # Recent data but too few rides (need MIN_RIDES_EXPECTED)
        mock_cursor.fetchone.side_effect = [
            (recent_time, 10, 50),    # ride_status_snapshots - only 10 rides (need 50+)
            (recent_time, 10, 50),    # park_activity_snapshots
            (recent_time,),           # overall most recent ride
            (recent_time,),           # overall most recent park
        ]

        mock_conn.execute.return_value = mock_cursor

        with patch('scripts.check_data_collection.get_db_session') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_conn

            result = check_recent_snapshots()

            # Should fail because rides_with_recent_data < MIN_RIDES_EXPECTED
            assert result["has_recent_ride_data"] is False
            assert result["has_recent_park_data"] is True

    def test_check_recent_snapshots_with_insufficient_parks(self):
        """Too few parks with recent data should fail the check."""
        now = datetime.now()
        recent_time = now - timedelta(minutes=10)

        mock_conn = Mock()
        mock_cursor = Mock()

        # Recent data but too few parks (need MIN_PARKS_EXPECTED)
        mock_cursor.fetchone.side_effect = [
            (recent_time, 100, 500),  # ride_status_snapshots
            (recent_time, 2, 10),     # park_activity_snapshots - only 2 parks (need 5+)
            (recent_time,),           # overall most recent ride
            (recent_time,),           # overall most recent park
        ]

        mock_conn.execute.return_value = mock_cursor

        with patch('scripts.check_data_collection.get_db_session') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_conn

            result = check_recent_snapshots()

            # Should fail because parks_with_recent_data < MIN_PARKS_EXPECTED
            assert result["has_recent_ride_data"] is True
            assert result["has_recent_park_data"] is False

    def test_check_recent_snapshots_with_no_data_ever(self):
        """Empty database should report no data."""
        mock_conn = Mock()
        mock_cursor = Mock()

        # No data at all
        mock_cursor.fetchone.side_effect = [
            (None, 0, 0),  # ride_status_snapshots recent
            (None, 0, 0),  # park_activity_snapshots recent
            (None,),       # overall most recent ride (none)
            (None,),       # overall most recent park (none)
        ]

        mock_conn.execute.return_value = mock_cursor

        with patch('scripts.check_data_collection.get_db_session') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_conn

            result = check_recent_snapshots()

            assert result["has_recent_ride_data"] is False
            assert result["has_recent_park_data"] is False
            assert result["overall_most_recent_ride_snapshot"] is None
            assert result["overall_most_recent_park_snapshot"] is None
            assert result["minutes_since_last_ride_snapshot"] is None
            assert result["minutes_since_last_park_snapshot"] is None


class TestGetCollectionProcessStatus:
    """Tests for get_collection_process_status function."""

    @patch('subprocess.run')
    def test_process_running(self, mock_run):
        """Should detect running collect_snapshots process."""
        mock_run.return_value = Mock(
            stdout="12345\n67890\n",
            stderr=""
        )

        result = get_collection_process_status()

        assert result["is_running"] is True
        assert result["process_count"] == 2
        mock_run.assert_called_once_with(
            ["pgrep", "-f", "collect_snapshots"],
            capture_output=True,
            text=True
        )

    @patch('subprocess.run')
    def test_process_not_running(self, mock_run):
        """Should detect when collect_snapshots is not running."""
        mock_run.return_value = Mock(
            stdout="",
            stderr=""
        )

        result = get_collection_process_status()

        assert result["is_running"] is False
        assert result["process_count"] == 0

    @patch('subprocess.run')
    def test_subprocess_error(self, mock_run):
        """Should handle subprocess errors gracefully."""
        mock_run.side_effect = Exception("pgrep not found")

        result = get_collection_process_status()

        assert result["is_running"] is None
        assert result["process_count"] is None


class TestFormatAlertEmail:
    """Tests for format_alert_email function."""

    def test_format_alert_email_with_stale_data(self):
        """Alert email should contain diagnostic information."""
        now = datetime.now()
        old_time = now - timedelta(hours=2)

        diagnostics = {
            "has_recent_ride_data": False,
            "has_recent_park_data": False,
            "most_recent_ride_snapshot": None,
            "most_recent_park_snapshot": None,
            "overall_most_recent_ride_snapshot": old_time,
            "overall_most_recent_park_snapshot": old_time,
            "parks_with_recent_data": 0,
            "rides_with_recent_data": 0,
            "ride_snapshot_count": 0,
            "park_snapshot_count": 0,
            "minutes_since_last_ride_snapshot": 120,
            "minutes_since_last_park_snapshot": 120,
        }

        process_status = {
            "is_running": False,
            "process_count": 0,
        }

        with patch('scripts.check_data_collection.get_cron_log_tail') as mock_log:
            mock_log.return_value = "Sample log content\nError: Connection failed"

            html = format_alert_email(diagnostics, process_status)

            # Check HTML contains key information
            assert "CRITICAL" in html
            assert "Data collection appears to have stopped" in html
            assert "120 minutes ago" in html
            assert "Sample log content" in html
            assert str(MIN_PARKS_EXPECTED) in html
            assert str(MIN_RIDES_EXPECTED) in html
            assert "No" in html  # Process not running

    def test_format_alert_email_with_healthy_data(self):
        """Alert email should show healthy status when data is recent."""
        now = datetime.now()
        recent_time = now - timedelta(minutes=5)

        diagnostics = {
            "has_recent_ride_data": True,
            "has_recent_park_data": True,
            "most_recent_ride_snapshot": recent_time,
            "most_recent_park_snapshot": recent_time,
            "overall_most_recent_ride_snapshot": recent_time,
            "overall_most_recent_park_snapshot": recent_time,
            "parks_with_recent_data": 10,
            "rides_with_recent_data": 100,
            "ride_snapshot_count": 500,
            "park_snapshot_count": 50,
            "minutes_since_last_ride_snapshot": 5,
            "minutes_since_last_park_snapshot": 5,
        }

        process_status = {
            "is_running": True,
            "process_count": 1,
        }

        with patch('scripts.check_data_collection.get_cron_log_tail') as mock_log:
            mock_log.return_value = None

            html = format_alert_email(diagnostics, process_status)

            # Even with healthy data, email is for alerting
            assert "5 minutes ago" in html
            assert "Yes" in html  # Process running

    def test_format_alert_email_without_log(self):
        """Alert email should handle missing log gracefully."""
        diagnostics = {
            "has_recent_ride_data": False,
            "has_recent_park_data": False,
            "overall_most_recent_ride_snapshot": None,
            "overall_most_recent_park_snapshot": None,
            "parks_with_recent_data": 0,
            "rides_with_recent_data": 0,
            "ride_snapshot_count": 0,
            "park_snapshot_count": 0,
            "minutes_since_last_ride_snapshot": None,
            "minutes_since_last_park_snapshot": None,
        }

        process_status = {
            "is_running": None,
            "process_count": None,
        }

        with patch('scripts.check_data_collection.get_cron_log_tail') as mock_log:
            mock_log.return_value = None

            html = format_alert_email(diagnostics, process_status)

            # Should not contain log section
            assert "Recent Collection Log" not in html
            assert "Never" in html  # For timestamps that are None
