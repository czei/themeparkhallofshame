"""
Unit Tests: Archive Parser
Tests for ArchiveParser, ArchiveEvent, and QueueInfo.
Feature: 004-themeparks-data-collection
Task: T030
"""

import pytest
import json
import zlib
from datetime import datetime
from unittest.mock import patch, MagicMock

from importer.archive_parser import (
    ArchiveParser,
    ArchiveEvent,
    QueueInfo,
    ArchiveParseError,
    DecompressionError,
    _parse_datetime
)


class TestParseDatetime:
    """Tests for _parse_datetime helper function."""

    def test_parse_iso_format_with_milliseconds(self):
        """Parse ISO format with milliseconds and Z suffix."""
        result = _parse_datetime("2024-12-25T00:05:10.644Z")
        assert result is not None
        assert result.year == 2024
        assert result.month == 12
        assert result.day == 25
        assert result.hour == 0
        assert result.minute == 5
        assert result.second == 10

    def test_parse_iso_format_without_milliseconds(self):
        """Parse ISO format without milliseconds."""
        result = _parse_datetime("2024-12-25T00:05:10Z")
        assert result is not None
        assert result.year == 2024
        assert result.month == 12
        assert result.day == 25

    def test_parse_iso_format_with_timezone_offset(self):
        """Parse ISO format with timezone offset."""
        result = _parse_datetime("2024-12-24T19:05:10-05:00")
        assert result is not None
        assert result.year == 2024
        assert result.month == 12

    def test_parse_none_returns_none(self):
        """Return None for None input."""
        assert _parse_datetime(None) is None

    def test_parse_empty_string_returns_none(self):
        """Return None for empty string."""
        assert _parse_datetime("") is None

    def test_parse_invalid_format_returns_none(self):
        """Return None for invalid format."""
        assert _parse_datetime("not a date") is None


class TestQueueInfo:
    """Tests for QueueInfo dataclass."""

    def test_from_dict_standby_queue(self):
        """Parse standby queue with wait time."""
        data = {"waitTime": 45}
        queue = QueueInfo.from_dict("STANDBY", data)

        assert queue.queue_type == "STANDBY"
        assert queue.wait_time == 45
        assert queue.return_start is None
        assert queue.return_end is None

    def test_from_dict_return_time_queue(self):
        """Parse return time queue with return window."""
        data = {
            "returnStart": "2024-12-25T14:00:00Z",
            "returnEnd": "2024-12-25T15:00:00Z"
        }
        queue = QueueInfo.from_dict("RETURN_TIME", data)

        assert queue.queue_type == "RETURN_TIME"
        assert queue.wait_time is None
        assert queue.return_start is not None
        assert queue.return_end is not None

    def test_from_dict_paid_return_time_with_price(self):
        """Parse paid return time queue with price."""
        data = {
            "returnStart": "2024-12-25T14:00:00Z",
            "returnEnd": "2024-12-25T15:00:00Z",
            "price": {"amount": 25.00, "currency": "USD"}
        }
        queue = QueueInfo.from_dict("PAID_RETURN_TIME", data)

        assert queue.queue_type == "PAID_RETURN_TIME"
        assert queue.price_amount == 25.00
        assert queue.price_currency == "USD"

    def test_from_dict_boarding_group(self):
        """Parse boarding group queue."""
        data = {
            "state": "AVAILABLE",
            "currentGroup": "45"
        }
        queue = QueueInfo.from_dict("BOARDING_GROUP", data)

        assert queue.queue_type == "BOARDING_GROUP"
        assert queue.state == "AVAILABLE"
        assert queue.current_group == "45"


class TestArchiveEvent:
    """Tests for ArchiveEvent dataclass."""

    def test_from_dict_minimal(self):
        """Parse event with minimal required fields."""
        data = {
            "entityId": "abc-123",
            "name": "Test Ride",
            "eventTime": "2024-12-25T12:00:00Z",
            "data": {"status": "OPERATING"}
        }
        event = ArchiveEvent.from_dict(data)

        assert event.entity_id == "abc-123"
        assert event.name == "Test Ride"
        assert event.status == "OPERATING"
        assert event.event_time is not None

    def test_from_dict_full_event(self):
        """Parse event with all fields."""
        data = {
            "entityId": "abc-123",
            "internalId": "ride-456",
            "name": "Test Ride",
            "eventTime": "2024-12-25T12:00:00Z",
            "localTime": "2024-12-25T07:00:00-05:00",
            "timezone": "America/New_York",
            "parkId": "park-789",
            "parkSlug": "test-park",
            "data": {
                "status": "OPERATING",
                "queue": {
                    "STANDBY": {"waitTime": 30},
                    "SINGLE_RIDER": {"waitTime": 10}
                },
                "destinationId": "dest-111"
            }
        }
        event = ArchiveEvent.from_dict(data)

        assert event.entity_id == "abc-123"
        assert event.internal_id == "ride-456"
        assert event.name == "Test Ride"
        assert event.status == "OPERATING"
        assert event.timezone == "America/New_York"
        assert event.park_id == "park-789"
        assert event.park_slug == "test-park"
        assert event.destination_id == "dest-111"
        assert len(event.queues) == 2

    def test_from_dict_with_showtimes(self):
        """Parse event with showtimes (for shows)."""
        data = {
            "entityId": "show-123",
            "name": "Test Show",
            "eventTime": "2024-12-25T12:00:00Z",
            "data": {
                "status": "OPERATING",
                "showtimes": [
                    "2024-12-25T14:00:00Z",
                    "2024-12-25T16:00:00Z",
                    "2024-12-25T18:00:00Z"
                ]
            }
        }
        event = ArchiveEvent.from_dict(data)

        assert event.is_show
        assert len(event.showtimes) == 3

    def test_from_dict_missing_entity_id_raises(self):
        """Raise error for missing entityId."""
        data = {
            "name": "Test Ride",
            "eventTime": "2024-12-25T12:00:00Z",
            "data": {"status": "OPERATING"}
        }
        with pytest.raises(ArchiveParseError, match="Missing entityId"):
            ArchiveEvent.from_dict(data)

    def test_from_dict_missing_event_time_raises(self):
        """Raise error for missing eventTime."""
        data = {
            "entityId": "abc-123",
            "name": "Test Ride",
            "data": {"status": "OPERATING"}
        }
        with pytest.raises(ArchiveParseError, match="Missing eventTime"):
            ArchiveEvent.from_dict(data)

    def test_from_dict_include_raw(self):
        """Include raw data when requested."""
        data = {
            "entityId": "abc-123",
            "name": "Test Ride",
            "eventTime": "2024-12-25T12:00:00Z",
            "data": {"status": "OPERATING"}
        }
        event = ArchiveEvent.from_dict(data, include_raw=True)

        assert event.raw_data is not None
        assert event.raw_data["entityId"] == "abc-123"

    def test_wait_time_property(self):
        """Get standby wait time from queues."""
        data = {
            "entityId": "abc-123",
            "name": "Test Ride",
            "eventTime": "2024-12-25T12:00:00Z",
            "data": {
                "status": "OPERATING",
                "queue": {
                    "STANDBY": {"waitTime": 45},
                    "SINGLE_RIDER": {"waitTime": 15}
                }
            }
        }
        event = ArchiveEvent.from_dict(data)

        assert event.wait_time == 45

    def test_wait_time_none_when_no_standby(self):
        """Return None when no standby queue."""
        data = {
            "entityId": "abc-123",
            "name": "Test Ride",
            "eventTime": "2024-12-25T12:00:00Z",
            "data": {
                "status": "OPERATING",
                "queue": {
                    "SINGLE_RIDER": {"waitTime": 15}
                }
            }
        }
        event = ArchiveEvent.from_dict(data)

        assert event.wait_time is None

    def test_is_operating_property(self):
        """Check is_operating property."""
        data = {
            "entityId": "abc-123",
            "name": "Test Ride",
            "eventTime": "2024-12-25T12:00:00Z",
            "data": {"status": "OPERATING"}
        }
        event = ArchiveEvent.from_dict(data)

        assert event.is_operating is True
        assert event.is_down is False

    def test_is_down_property(self):
        """Check is_down property."""
        data = {
            "entityId": "abc-123",
            "name": "Test Ride",
            "eventTime": "2024-12-25T12:00:00Z",
            "data": {"status": "DOWN"}
        }
        event = ArchiveEvent.from_dict(data)

        assert event.is_operating is False
        assert event.is_down is True


class TestArchiveParser:
    """Tests for ArchiveParser class."""

    def test_decompress_data_success(self):
        """Decompress valid zlib data."""
        parser = ArchiveParser()
        original = b'{"test": "data"}'
        compressed = zlib.compress(original)

        result = parser.decompress_data(compressed)

        assert result == original

    def test_decompress_data_invalid_raises(self):
        """Raise error for invalid compressed data."""
        parser = ArchiveParser()

        with pytest.raises(DecompressionError):
            parser.decompress_data(b"not compressed data")

    def test_parse_events_single_event(self):
        """Parse single event from JSON."""
        parser = ArchiveParser()
        data = json.dumps({
            "events": [{
                "entityId": "abc-123",
                "name": "Test Ride",
                "eventTime": "2024-12-25T12:00:00Z",
                "data": {"status": "OPERATING"}
            }]
        }).encode()

        events = parser.parse_events(data)

        assert len(events) == 1
        assert events[0].entity_id == "abc-123"
        assert parser.stats['events_parsed'] == 1

    def test_parse_events_multiple_events(self):
        """Parse multiple events from JSON."""
        parser = ArchiveParser()
        data = json.dumps({
            "events": [
                {
                    "entityId": "abc-123",
                    "name": "Ride 1",
                    "eventTime": "2024-12-25T12:00:00Z",
                    "data": {"status": "OPERATING"}
                },
                {
                    "entityId": "def-456",
                    "name": "Ride 2",
                    "eventTime": "2024-12-25T12:00:00Z",
                    "data": {"status": "DOWN"}
                }
            ]
        }).encode()

        events = parser.parse_events(data)

        assert len(events) == 2
        assert parser.stats['events_parsed'] == 2

    def test_parse_events_list_format(self):
        """Parse events from list format (no wrapper object)."""
        parser = ArchiveParser()
        data = json.dumps([
            {
                "entityId": "abc-123",
                "name": "Ride 1",
                "eventTime": "2024-12-25T12:00:00Z",
                "data": {"status": "OPERATING"}
            }
        ]).encode()

        events = parser.parse_events(data)

        assert len(events) == 1

    def test_parse_events_invalid_json_raises(self):
        """Raise error for invalid JSON."""
        parser = ArchiveParser()

        with pytest.raises(ArchiveParseError, match="Invalid JSON"):
            parser.parse_events(b"not json")

    def test_parse_events_skips_invalid_event(self):
        """Skip events with invalid format and continue."""
        parser = ArchiveParser()
        data = json.dumps({
            "events": [
                {
                    "entityId": "abc-123",
                    "name": "Valid Ride",
                    "eventTime": "2024-12-25T12:00:00Z",
                    "data": {"status": "OPERATING"}
                },
                {
                    # Missing entityId - will be skipped
                    "name": "Invalid Ride",
                    "eventTime": "2024-12-25T12:00:00Z",
                    "data": {"status": "OPERATING"}
                },
                {
                    "entityId": "ghi-789",
                    "name": "Another Valid Ride",
                    "eventTime": "2024-12-25T12:00:00Z",
                    "data": {"status": "DOWN"}
                }
            ]
        }).encode()

        events = parser.parse_events(data)

        assert len(events) == 2
        assert parser.stats['errors'] == 1

    def test_parse_s3_content(self):
        """Parse compressed S3 content."""
        parser = ArchiveParser()
        json_data = json.dumps({
            "events": [{
                "entityId": "abc-123",
                "name": "Test Ride",
                "eventTime": "2024-12-25T12:00:00Z",
                "data": {"status": "OPERATING"}
            }]
        }).encode()
        compressed = zlib.compress(json_data)

        events = parser.parse_s3_content(compressed)

        assert len(events) == 1
        assert events[0].entity_id == "abc-123"

    def test_reset_stats(self):
        """Reset statistics counters."""
        parser = ArchiveParser()
        data = json.dumps({
            "events": [{
                "entityId": "abc-123",
                "name": "Test Ride",
                "eventTime": "2024-12-25T12:00:00Z",
                "data": {"status": "OPERATING"}
            }]
        }).encode()
        parser.parse_events(data)

        parser.reset_stats()

        assert parser.stats['events_parsed'] == 0
        assert parser.stats['errors'] == 0

    def test_include_raw_flag(self):
        """Test include_raw flag in parser."""
        parser = ArchiveParser(include_raw=True)
        data = json.dumps({
            "events": [{
                "entityId": "abc-123",
                "name": "Test Ride",
                "eventTime": "2024-12-25T12:00:00Z",
                "data": {"status": "OPERATING"}
            }]
        }).encode()

        events = parser.parse_events(data)

        assert events[0].raw_data is not None
