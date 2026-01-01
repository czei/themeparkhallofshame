"""
Archive Parser for ThemeParks.wiki Historical Data
Handles zlib decompression and JSON parsing of archive files.
Feature: 004-themeparks-data-collection
"""

import json
import zlib
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, List, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class ArchiveParseError(Exception):
    """Raised when parsing archive data fails."""
    pass


class DecompressionError(Exception):
    """Raised when decompression fails."""
    pass


@dataclass
class QueueInfo:
    """Queue information from archive event."""
    queue_type: str  # STANDBY, SINGLE_RIDER, RETURN_TIME, PAID_RETURN_TIME, BOARDING_GROUP
    wait_time: Optional[int] = None
    return_start: Optional[datetime] = None
    return_end: Optional[datetime] = None
    price_amount: Optional[float] = None
    price_currency: Optional[str] = None
    state: Optional[str] = None  # For BOARDING_GROUP
    current_group: Optional[str] = None  # For BOARDING_GROUP

    @classmethod
    def from_dict(cls, queue_type: str, data: Dict[str, Any]) -> "QueueInfo":
        """
        Parse queue info from API data.

        Args:
            queue_type: Type of queue (STANDBY, SINGLE_RIDER, etc.)
            data: Queue data dict from API

        Returns:
            QueueInfo instance
        """
        return cls(
            queue_type=queue_type,
            wait_time=data.get('waitTime'),
            return_start=_parse_datetime(data.get('returnStart')),
            return_end=_parse_datetime(data.get('returnEnd')),
            price_amount=data.get('price', {}).get('amount') if data.get('price') else None,
            price_currency=data.get('price', {}).get('currency') if data.get('price') else None,
            state=data.get('state'),
            current_group=data.get('currentGroup')
        )


@dataclass
class ArchiveEvent:
    """
    Represents a single event from the ThemeParks.wiki archive.

    Each event is a status change/update for an attraction.
    """
    # Required fields (no defaults) must come first
    entity_id: str  # ThemeParks.wiki UUID
    name: str
    status: str  # OPERATING, CLOSED, DOWN, REFURBISHMENT
    event_time: datetime  # UTC timestamp

    # Optional fields with defaults
    internal_id: Optional[str] = None  # ThemeParks internal ID
    queues: List[QueueInfo] = field(default_factory=list)
    showtimes: Optional[List[datetime]] = None
    local_time: Optional[datetime] = None
    timezone: str = "UTC"
    park_id: Optional[str] = None  # ThemeParks.wiki park UUID
    park_slug: Optional[str] = None  # e.g., "universalstudiosflorida"
    destination_id: Optional[str] = None  # ThemeParks.wiki destination UUID
    raw_data: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any], include_raw: bool = False) -> "ArchiveEvent":
        """
        Parse archive event from API data dict.

        Args:
            data: Event dict from archive JSON
            include_raw: Whether to include raw_data for debugging

        Returns:
            ArchiveEvent instance

        Raises:
            ArchiveParseError: If required fields are missing
        """
        try:
            # Parse required fields
            entity_id = data.get('entityId')
            if not entity_id:
                raise ArchiveParseError("Missing entityId in event")

            name = data.get('name', '')
            event_time_str = data.get('eventTime')
            if not event_time_str:
                raise ArchiveParseError(f"Missing eventTime for entity {entity_id}")

            event_time = _parse_datetime(event_time_str)
            if not event_time:
                raise ArchiveParseError(f"Invalid eventTime format: {event_time_str}")

            # Parse nested data
            event_data = data.get('data', {})
            status = event_data.get('status', 'UNKNOWN')

            # Parse queues
            queues = []
            queue_data = event_data.get('queue', {})
            for queue_type, queue_info in queue_data.items():
                if isinstance(queue_info, dict):
                    queues.append(QueueInfo.from_dict(queue_type, queue_info))

            # Parse showtimes for shows
            showtimes = None
            if 'showtimes' in event_data:
                showtimes = []
                for showtime_str in event_data['showtimes']:
                    st = _parse_datetime(showtime_str)
                    if st:
                        showtimes.append(st)

            # Parse local time
            local_time = _parse_datetime(data.get('localTime'))

            return cls(
                entity_id=entity_id,
                name=name,
                status=status,
                event_time=event_time,
                internal_id=data.get('internalId'),
                queues=queues,
                showtimes=showtimes,
                local_time=local_time,
                timezone=data.get('timezone', 'UTC'),
                park_id=data.get('parkId'),
                park_slug=data.get('parkSlug'),
                destination_id=event_data.get('destinationId'),
                raw_data=data if include_raw else None
            )
        except KeyError as e:
            raise ArchiveParseError(f"Missing required field: {e}")
        except Exception as e:
            if isinstance(e, ArchiveParseError):
                raise
            raise ArchiveParseError(f"Failed to parse event: {e}")

    @property
    def wait_time(self) -> Optional[int]:
        """Get standby wait time if available."""
        for queue in self.queues:
            if queue.queue_type == 'STANDBY' and queue.wait_time is not None:
                return queue.wait_time
        return None

    @property
    def is_operating(self) -> bool:
        """Check if entity is operating."""
        return self.status == 'OPERATING'

    @property
    def is_down(self) -> bool:
        """Check if entity is down (unexpected closure)."""
        return self.status == 'DOWN'

    @property
    def is_show(self) -> bool:
        """Check if this is a show (has showtimes instead of queue)."""
        return self.showtimes is not None


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    """
    Parse datetime from various formats used in archive.

    Args:
        value: Datetime string or None

    Returns:
        Parsed datetime or None
    """
    if not value:
        return None

    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",  # 2024-12-25T00:05:10.644Z
        "%Y-%m-%dT%H:%M:%SZ",     # 2024-12-25T00:05:10Z
        "%Y-%m-%dT%H:%M:%S%z",    # 2024-12-24T19:05:10-05:00
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    # Try fromisoformat for more flexibility
    try:
        # Handle timezone offset like -05:00
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        pass

    logger.warning(f"Could not parse datetime: {value}")
    return None


class ArchiveParser:
    """
    Parser for ThemeParks.wiki archive files.

    Handles zlib decompression and JSON parsing of archived
    wait time data from archive.themeparks.wiki.
    """

    def __init__(self, include_raw: bool = False):
        """
        Initialize archive parser.

        Args:
            include_raw: Whether to include raw data in parsed events for debugging
        """
        self.include_raw = include_raw
        self._stats = {
            'files_parsed': 0,
            'events_parsed': 0,
            'errors': 0
        }

    @property
    def stats(self) -> Dict[str, int]:
        """Get parsing statistics."""
        return self._stats.copy()

    def decompress_data(self, compressed: bytes) -> bytes:
        """
        Decompress zlib-compressed data.

        Args:
            compressed: zlib-compressed bytes

        Returns:
            Decompressed bytes

        Raises:
            DecompressionError: If decompression fails
        """
        try:
            return zlib.decompress(compressed)
        except zlib.error as e:
            raise DecompressionError(f"Failed to decompress data: {e}")

    def decompress_file(self, file_path: str) -> bytes:
        """
        Read and decompress a zlib-compressed archive file.

        Args:
            file_path: Path to compressed file

        Returns:
            Decompressed bytes

        Raises:
            DecompressionError: If decompression fails
            FileNotFoundError: If file doesn't exist
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Archive file not found: {file_path}")

        try:
            with open(path, 'rb') as f:
                compressed = f.read()
            return self.decompress_data(compressed)
        except IOError as e:
            raise DecompressionError(f"Failed to read file {file_path}: {e}")

    def parse_events(self, data: bytes) -> List[ArchiveEvent]:
        """
        Parse decompressed JSON data into ArchiveEvent objects.

        Args:
            data: Decompressed JSON bytes

        Returns:
            List of ArchiveEvent objects

        Raises:
            ArchiveParseError: If JSON parsing fails
        """
        try:
            content = json.loads(data)
        except json.JSONDecodeError as e:
            self._stats['errors'] += 1
            raise ArchiveParseError(f"Invalid JSON: {e}")

        events = []

        # Handle both {"events": [...]} and [...] formats
        if isinstance(content, dict):
            event_list = content.get('events', [])
        elif isinstance(content, list):
            event_list = content
        else:
            raise ArchiveParseError(f"Unexpected content type: {type(content)}")

        for event_data in event_list:
            try:
                event = ArchiveEvent.from_dict(event_data, include_raw=self.include_raw)
                events.append(event)
                self._stats['events_parsed'] += 1
            except ArchiveParseError as e:
                self._stats['errors'] += 1
                logger.warning(f"Failed to parse event: {e}")
                continue

        return events

    def parse_file(self, file_path: str) -> List[ArchiveEvent]:
        """
        Decompress and parse an archive file.

        Args:
            file_path: Path to compressed archive file

        Returns:
            List of ArchiveEvent objects

        Raises:
            DecompressionError: If decompression fails
            ArchiveParseError: If parsing fails
        """
        decompressed = self.decompress_file(file_path)
        events = self.parse_events(decompressed)
        self._stats['files_parsed'] += 1
        return events

    def parse_s3_content(self, content: bytes) -> List[ArchiveEvent]:
        """
        Parse content retrieved from S3.

        Args:
            content: Raw bytes from S3 object

        Returns:
            List of ArchiveEvent objects
        """
        decompressed = self.decompress_data(content)
        return self.parse_events(decompressed)

    def reset_stats(self) -> None:
        """Reset parsing statistics."""
        self._stats = {
            'files_parsed': 0,
            'events_parsed': 0,
            'errors': 0
        }
