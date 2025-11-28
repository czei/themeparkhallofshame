"""
Theme Park Downtime Tracker - Ride Entity Model
Represents individual ride/attraction master data from the rides table.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Ride:
    """
    Ride/attraction entity model.

    Attributes match the rides table schema from data-model.md.
    """
    ride_id: int
    queue_times_id: int
    park_id: int
    name: str
    land_area: Optional[str]
    tier: Optional[int]  # 1 (major, 3x weight), 2 (standard, 2x weight), 3 (minor, 1x weight)
    category: Optional[str]  # 'ATTRACTION', 'MEET_AND_GREET', 'SHOW', 'EXPERIENCE'
    is_active: bool
    created_at: datetime
    updated_at: datetime

    @property
    def tier_weight(self) -> int:
        """
        Get tier weight for weighted downtime calculations (FR-024).

        Returns:
            3 for Tier 1, 2 for Tier 2, 1 for Tier 3, 1 for unclassified
        """
        tier_weights = {1: 3, 2: 2, 3: 1}
        return tier_weights.get(self.tier, 1)

    @property
    def tier_label(self) -> str:
        """
        Get human-readable tier label.

        Returns:
            "Tier 1 (Major)", "Tier 2 (Standard)", "Tier 3 (Minor)", or "Unclassified"
        """
        tier_labels = {
            1: "Tier 1 (Major)",
            2: "Tier 2 (Standard)",
            3: "Tier 3 (Minor)"
        }
        return tier_labels.get(self.tier, "Unclassified")

    @property
    def queue_times_url(self) -> str:
        """
        Get Queue-Times.com URL for this ride.

        Returns:
            URL to ride page on Queue-Times.com (FR-036)
        """
        return f"https://queue-times.com/ride/{self.queue_times_id}"

    def to_dict(self) -> dict:
        """
        Convert ride to dictionary for API responses.

        Returns:
            Dictionary representation of ride
        """
        return {
            "ride_id": self.ride_id,
            "queue_times_id": self.queue_times_id,
            "park_id": self.park_id,
            "name": self.name,
            "land_area": self.land_area,
            "tier": self.tier,
            "tier_weight": self.tier_weight,
            "tier_label": self.tier_label,
            "category": self.category,
            "is_active": self.is_active,
            "queue_times_url": self.queue_times_url
        }

    @classmethod
    def from_row(cls, row) -> 'Ride':
        """
        Create Ride instance from database row.

        Args:
            row: SQLAlchemy Row object or dict-like object

        Returns:
            Ride instance
        """
        return cls(
            ride_id=row['ride_id'] if isinstance(row, dict) else row.ride_id,
            queue_times_id=row['queue_times_id'] if isinstance(row, dict) else row.queue_times_id,
            park_id=row['park_id'] if isinstance(row, dict) else row.park_id,
            name=row['name'] if isinstance(row, dict) else row.name,
            land_area=row['land_area'] if isinstance(row, dict) else row.land_area,
            tier=row['tier'] if isinstance(row, dict) else row.tier,
            category=row['category'] if isinstance(row, dict) else getattr(row, 'category', None),
            is_active=row['is_active'] if isinstance(row, dict) else row.is_active,
            created_at=row['created_at'] if isinstance(row, dict) else row.created_at,
            updated_at=row['updated_at'] if isinstance(row, dict) else row.updated_at
        )
