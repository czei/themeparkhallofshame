"""
SQLAlchemy ORM Models: Entity Metadata
Comprehensive attraction metadata from ThemeParks.wiki.
Feature: 004-themeparks-data-collection
"""

from sqlalchemy import Integer, String, ForeignKey, DateTime, Enum, Index, Numeric, JSON, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from models.base import Base
from datetime import datetime, timezone
from typing import Optional, TYPE_CHECKING, List, Dict, Any
from decimal import Decimal
import enum

if TYPE_CHECKING:
    from models.orm_ride import Ride


class IndoorOutdoor(enum.Enum):
    """Indoor/outdoor classification enum."""
    INDOOR = "INDOOR"
    OUTDOOR = "OUTDOOR"
    HYBRID = "HYBRID"


class EntityMetadata(Base):
    """
    Comprehensive attraction metadata from ThemeParks.wiki.

    Stores coordinates, indoor/outdoor classification, height requirements,
    and other attributes for location-based optimization and analytics.
    """
    __tablename__ = "entity_metadata"

    # Primary Key
    metadata_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Foreign Key to rides
    ride_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("rides.ride_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Internal ride ID"
    )

    # ThemeParks.wiki UUID
    themeparks_wiki_id: Mapped[str] = mapped_column(
        String(36),
        unique=True,
        nullable=False,
        comment="ThemeParks.wiki entity UUID"
    )

    # Entity identification
    entity_name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Entity name from ThemeParks.wiki"
    )
    entity_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="Entity type: ATTRACTION, SHOW, RESTAURANT, etc."
    )

    # Geographic coordinates
    latitude: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 7),
        nullable=True,
        comment="Latitude coordinate"
    )
    longitude: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 7),
        nullable=True,
        comment="Longitude coordinate"
    )

    # Classification
    indoor_outdoor: Mapped[Optional[str]] = mapped_column(
        Enum('INDOOR', 'OUTDOOR', 'HYBRID', name='indoor_outdoor_enum'),
        nullable=True,
        comment="Indoor/outdoor classification"
    )

    # Height requirements
    height_min_cm: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        comment="Minimum height requirement in centimeters"
    )
    height_max_cm: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        comment="Maximum height requirement in centimeters"
    )

    # Additional metadata
    tags: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSON,
        nullable=True,
        comment="Additional tags from ThemeParks.wiki"
    )

    # Sync tracking
    last_synced: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment="When metadata was last synced from ThemeParks.wiki"
    )
    version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        server_default='1',
        comment="Version counter for change tracking"
    )

    # Indexes
    __table_args__ = (
        Index('idx_metadata_type', 'entity_type'),
        Index('idx_metadata_coords', 'latitude', 'longitude'),
        {'extend_existing': True}
    )

    # Relationships
    ride: Mapped["Ride"] = relationship(
        "Ride",
        back_populates="entity_metadata"
    )

    @property
    def has_coordinates(self) -> bool:
        """Check if entity has valid coordinates."""
        return self.latitude is not None and self.longitude is not None

    @property
    def height_requirement_text(self) -> Optional[str]:
        """Return human-readable height requirement."""
        if self.height_min_cm is None:
            return None
        if self.height_max_cm:
            return f"{self.height_min_cm}-{self.height_max_cm} cm"
        return f"{self.height_min_cm}+ cm"

    def update_from_api(self, api_data: Dict[str, Any]) -> bool:
        """
        Update metadata from ThemeParks.wiki API response.

        Args:
            api_data: Dict containing entity data from API

        Returns:
            True if any field was updated
        """
        changed = False

        # Update basic fields
        if api_data.get('name') and api_data['name'] != self.entity_name:
            self.entity_name = api_data['name']
            changed = True

        if api_data.get('entityType') and api_data['entityType'] != self.entity_type:
            self.entity_type = api_data['entityType']
            changed = True

        # Update coordinates
        location = api_data.get('location', {})
        if location.get('latitude') is not None:
            new_lat = Decimal(str(location['latitude']))
            if new_lat != self.latitude:
                self.latitude = new_lat
                changed = True
        if location.get('longitude') is not None:
            new_lon = Decimal(str(location['longitude']))
            if new_lon != self.longitude:
                self.longitude = new_lon
                changed = True

        # Update tags
        if api_data.get('tags'):
            if api_data['tags'] != self.tags:
                self.tags = api_data['tags']
                changed = True

        if changed:
            self.last_synced = datetime.now(timezone.utc)
            self.version += 1

        return changed

    def __repr__(self) -> str:
        return f"<EntityMetadata(metadata_id={self.metadata_id}, ride_id={self.ride_id}, name='{self.entity_name}')>"
