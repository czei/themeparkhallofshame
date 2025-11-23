"""
Theme Park Downtime Tracker - Park Entity Model
Represents theme park master data from the parks table.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Park:
    """
    Theme park entity model.

    Attributes match the parks table schema from data-model.md.
    """
    park_id: int
    queue_times_id: int
    name: str
    city: str
    state_province: Optional[str]
    country: str
    latitude: Optional[float]
    longitude: Optional[float]
    timezone: str
    operator: Optional[str]
    is_disney: bool
    is_universal: bool
    is_active: bool
    created_at: datetime
    updated_at: datetime

    @property
    def location(self) -> str:
        """
        Get formatted location string.

        Returns:
            "City, State/Province" or "City, Country" if no state
        """
        if self.state_province:
            return f"{self.city}, {self.state_province}"
        return f"{self.city}, {self.country}"

    @property
    def is_disney_or_universal(self) -> bool:
        """Check if park is Disney or Universal (for filtering)."""
        return self.is_disney or self.is_universal

    @property
    def queue_times_url(self) -> str:
        """
        Get Queue-Times.com URL for this park.

        Returns:
            URL to park page on Queue-Times.com (FR-036)
        """
        return f"https://queue-times.com/parks/{self.queue_times_id}"

    def to_dict(self) -> dict:
        """
        Convert park to dictionary for API responses.

        Returns:
            Dictionary representation of park
        """
        return {
            "park_id": self.park_id,
            "queue_times_id": self.queue_times_id,
            "name": self.name,
            "city": self.city,
            "state_province": self.state_province,
            "country": self.country,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "timezone": self.timezone,
            "operator": self.operator,
            "is_disney": self.is_disney,
            "is_universal": self.is_universal,
            "is_active": self.is_active,
            "location": self.location,
            "queue_times_url": self.queue_times_url
        }

    @classmethod
    def from_row(cls, row) -> 'Park':
        """
        Create Park instance from database row.

        Args:
            row: SQLAlchemy Row object or dict-like object

        Returns:
            Park instance
        """
        return cls(
            park_id=row['park_id'] if isinstance(row, dict) else row.park_id,
            queue_times_id=row['queue_times_id'] if isinstance(row, dict) else row.queue_times_id,
            name=row['name'] if isinstance(row, dict) else row.name,
            city=row['city'] if isinstance(row, dict) else row.city,
            state_province=row['state_province'] if isinstance(row, dict) else row.state_province,
            country=row['country'] if isinstance(row, dict) else row.country,
            latitude=row['latitude'] if isinstance(row, dict) else row.latitude,
            longitude=row['longitude'] if isinstance(row, dict) else row.longitude,
            timezone=row['timezone'] if isinstance(row, dict) else row.timezone,
            operator=row['operator'] if isinstance(row, dict) else row.operator,
            is_disney=row['is_disney'] if isinstance(row, dict) else row.is_disney,
            is_universal=row['is_universal'] if isinstance(row, dict) else row.is_universal,
            is_active=row['is_active'] if isinstance(row, dict) else row.is_active,
            created_at=row['created_at'] if isinstance(row, dict) else row.created_at,
            updated_at=row['updated_at'] if isinstance(row, dict) else row.updated_at
        )
