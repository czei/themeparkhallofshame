"""
Metadata Collector: Entity Metadata from ThemeParks.wiki

Fetches comprehensive attraction metadata including coordinates,
indoor/outdoor classification, height requirements, and tags.

Feature: 004-themeparks-data-collection
Task: T051
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from requests.exceptions import RequestException

from collector.themeparks_wiki_client import ThemeParksWikiClient, get_themeparks_wiki_client
from database.repositories.quality_log_repository import QualityLogRepository
from models.orm_metadata import EntityMetadata
from models.orm_ride import Ride
from utils.logger import setup_logger

logger = setup_logger(__name__)


# Tags that indicate indoor attractions
INDOOR_TAGS = {
    'indoor',
    'dark ride',
    'dark-ride',
    'indoor ride',
    'indoor-ride',
    'enclosed',
    'air conditioned',
    'covered',
}

# Tags that indicate outdoor attractions
OUTDOOR_TAGS = {
    'outdoor',
    'outdoor ride',
    'outdoor-ride',
    'coaster',
    'roller coaster',
    'water ride',
    'log flume',
    'rapids',
    'boat ride',
}


class MetadataCollector:
    """
    Collects rich entity metadata from ThemeParks.wiki API.

    Extracts:
    - Geographic coordinates (latitude, longitude)
    - Indoor/outdoor classification
    - Height requirements
    - Tags and entity type
    """

    def __init__(
        self,
        session: Session,
        client: Optional[ThemeParksWikiClient] = None
    ):
        """
        Initialize the metadata collector.

        Args:
            session: SQLAlchemy session for database operations
            client: ThemeParks.wiki client (defaults to singleton)
        """
        self.session = session
        self.client = client or get_themeparks_wiki_client()
        self.quality_log_repo = QualityLogRepository(session)

    def classify_indoor_outdoor(self, tags: List[str]) -> Optional[str]:
        """
        Classify an entity as indoor/outdoor/hybrid based on tags.

        Args:
            tags: List of tag strings from the API

        Returns:
            'INDOOR', 'OUTDOOR', 'HYBRID', or None if unknown
        """
        if not tags:
            return None

        tags_lower = {t.lower() for t in tags}

        has_indoor = bool(tags_lower & INDOOR_TAGS)
        has_outdoor = bool(tags_lower & OUTDOOR_TAGS)

        if has_indoor and has_outdoor:
            return 'HYBRID'
        elif has_indoor:
            return 'INDOOR'
        elif has_outdoor:
            return 'OUTDOOR'

        return None

    def parse_height_requirement(
        self,
        restrictions: Dict[str, Any]
    ) -> Tuple[Optional[int], Optional[int]]:
        """
        Parse height requirements from API restrictions object.

        Args:
            restrictions: Restrictions dict from the API

        Returns:
            Tuple of (min_height_cm, max_height_cm)
        """
        if not restrictions:
            return None, None

        min_height = None
        max_height = None

        # Try to get height restriction
        height = restrictions.get('height', {})
        if height:
            # Height may be in various units; API typically uses cm
            min_height = height.get('min')
            max_height = height.get('max')

            # Convert inches to cm if needed (API sometimes uses inches)
            unit = height.get('unit', 'cm')
            if unit.lower() in ('in', 'inch', 'inches'):
                if min_height:
                    min_height = int(min_height * 2.54)
                if max_height:
                    max_height = int(max_height * 2.54)

        return min_height, max_height

    def extract_coordinates(
        self,
        entity_data: Dict[str, Any]
    ) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """
        Extract coordinates from entity data.

        Args:
            entity_data: Entity document from API

        Returns:
            Tuple of (latitude, longitude) as Decimal
        """
        location = entity_data.get('location', {})
        if not location:
            return None, None

        lat = location.get('latitude')
        lon = location.get('longitude')

        if lat is not None and lon is not None:
            try:
                return Decimal(str(lat)), Decimal(str(lon))
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid coordinates: lat={lat}, lon={lon}: {e}")
                return None, None

        return None, None

    def fetch_entity_metadata(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch metadata for a single entity from the API.

        Args:
            entity_id: ThemeParks.wiki entity UUID

        Returns:
            Entity data dict or None if failed
        """
        try:
            return self.client.get_entity(entity_id)
        except Exception as e:
            logger.error(f"Failed to fetch entity {entity_id}: {e}")
            return None

    def sync_ride_metadata(self, ride: Ride) -> Optional[EntityMetadata]:
        """
        Sync metadata for a single ride from ThemeParks.wiki.

        Args:
            ride: Ride ORM object with themeparks_wiki_id

        Returns:
            EntityMetadata object if successful, None otherwise
        """
        if not ride.themeparks_wiki_id:
            logger.warning(f"Ride {ride.ride_id} has no themeparks_wiki_id")
            return None

        # Fetch from API
        entity_data = self.fetch_entity_metadata(ride.themeparks_wiki_id)
        if not entity_data:
            self.quality_log_repo.log_mapping_failed(
                entity_type='ride',
                external_id=ride.themeparks_wiki_id,
                timestamp=datetime.now(timezone.utc),
                description=f"Failed to fetch metadata for ride {ride.name}"
            )
            return None

        # Extract fields
        lat, lon = self.extract_coordinates(entity_data)
        tags = entity_data.get('tags', [])
        indoor_outdoor = self.classify_indoor_outdoor(tags)
        min_height, max_height = self.parse_height_requirement(
            entity_data.get('restrictions', {})
        )

        # Find or create metadata record
        metadata = self.session.query(EntityMetadata).filter(
            EntityMetadata.ride_id == ride.ride_id
        ).first()

        if metadata:
            # Update existing record
            changed = False

            if entity_data.get('name') and entity_data['name'] != metadata.entity_name:
                metadata.entity_name = entity_data['name']
                changed = True

            if entity_data.get('entityType') and entity_data['entityType'] != metadata.entity_type:
                metadata.entity_type = entity_data['entityType']
                changed = True

            if lat is not None and lat != metadata.latitude:
                metadata.latitude = lat
                changed = True

            if lon is not None and lon != metadata.longitude:
                metadata.longitude = lon
                changed = True

            if indoor_outdoor and indoor_outdoor != metadata.indoor_outdoor:
                metadata.indoor_outdoor = indoor_outdoor
                changed = True

            if min_height is not None and min_height != metadata.height_min_cm:
                metadata.height_min_cm = min_height
                changed = True

            if max_height is not None and max_height != metadata.height_max_cm:
                metadata.height_max_cm = max_height
                changed = True

            if tags and tags != metadata.tags:
                metadata.tags = tags
                changed = True

            if changed:
                metadata.last_synced = datetime.now(timezone.utc)
                metadata.version += 1
                logger.info(f"Updated metadata for ride {ride.name}")
        else:
            # Create new record
            metadata = EntityMetadata(
                ride_id=ride.ride_id,
                themeparks_wiki_id=ride.themeparks_wiki_id,
                entity_name=entity_data.get('name', ride.name),
                entity_type=entity_data.get('entityType', 'ATTRACTION'),
                latitude=lat,
                longitude=lon,
                indoor_outdoor=indoor_outdoor,
                height_min_cm=min_height,
                height_max_cm=max_height,
                tags=tags if tags else None,
                last_synced=datetime.now(timezone.utc)
            )
            self.session.add(metadata)
            logger.info(f"Created metadata for ride {ride.name}")

        return metadata

    def sync_park_metadata(self, park_themeparks_wiki_id: str) -> Dict[str, int]:
        """
        Sync metadata for all rides in a park.

        Args:
            park_themeparks_wiki_id: Park's ThemeParks.wiki UUID

        Returns:
            Dict with counts: {'synced': N, 'skipped': N, 'failed': N}
        """
        stats = {'synced': 0, 'skipped': 0, 'failed': 0}

        # Get all rides for this park
        rides = self.session.query(Ride).filter(
            Ride.themeparks_wiki_id.isnot(None)
        ).join(Ride.park).filter(
            Ride.park.has(themeparks_wiki_id=park_themeparks_wiki_id)
        ).all()

        logger.info(f"Syncing metadata for {len(rides)} rides in park {park_themeparks_wiki_id}")

        for ride in rides:
            try:
                metadata = self.sync_ride_metadata(ride)
                if metadata:
                    stats['synced'] += 1
                else:
                    stats['skipped'] += 1
            except (RequestException, SQLAlchemyError, ValueError, TypeError) as e:
                logger.error(f"Failed to sync metadata for ride {ride.name}: {e}")
                stats['failed'] += 1
                self.quality_log_repo.log_parse_error(
                    entity_type='ride',
                    external_id=ride.themeparks_wiki_id or '',
                    timestamp=datetime.now(timezone.utc),
                    description=f"Metadata sync failed: {e}",
                    raw_data={'ride_id': ride.ride_id, 'error': str(e)}
                )

        return stats

    def sync_all_metadata(self) -> Dict[str, int]:
        """
        Sync metadata for all rides with ThemeParks.wiki IDs.

        Returns:
            Dict with total counts: {'synced': N, 'skipped': N, 'failed': N}
        """
        stats = {'synced': 0, 'skipped': 0, 'failed': 0}

        # Get all rides with themeparks_wiki_id
        rides = self.session.query(Ride).filter(
            Ride.themeparks_wiki_id.isnot(None)
        ).all()

        logger.info(f"Syncing metadata for {len(rides)} total rides")

        for ride in rides:
            try:
                metadata = self.sync_ride_metadata(ride)
                if metadata:
                    stats['synced'] += 1
                else:
                    stats['skipped'] += 1
            except (RequestException, SQLAlchemyError, ValueError, TypeError) as e:
                logger.error(f"Failed to sync metadata for ride {ride.name}: {e}")
                stats['failed'] += 1

        return stats

    def get_coverage_stats(self) -> Dict[str, Any]:
        """
        Get metadata coverage statistics.

        Returns:
            Dict with coverage stats
        """
        from sqlalchemy import func

        # Total rides with themeparks_wiki_id
        total_rides = self.session.query(func.count(Ride.ride_id)).filter(
            Ride.themeparks_wiki_id.isnot(None)
        ).scalar()

        # Rides with metadata
        with_metadata = self.session.query(func.count(EntityMetadata.metadata_id)).scalar()

        # Rides with coordinates
        with_coords = self.session.query(func.count(EntityMetadata.metadata_id)).filter(
            EntityMetadata.latitude.isnot(None),
            EntityMetadata.longitude.isnot(None)
        ).scalar()

        # Rides with height requirements
        with_height = self.session.query(func.count(EntityMetadata.metadata_id)).filter(
            EntityMetadata.height_min_cm.isnot(None)
        ).scalar()

        # Rides with indoor/outdoor
        with_indoor_outdoor = self.session.query(func.count(EntityMetadata.metadata_id)).filter(
            EntityMetadata.indoor_outdoor.isnot(None)
        ).scalar()

        return {
            'total_rides': total_rides,
            'with_metadata': with_metadata,
            'with_coordinates': with_coords,
            'with_height_requirement': with_height,
            'with_indoor_outdoor': with_indoor_outdoor,
            'metadata_coverage_pct': round(with_metadata / total_rides * 100, 1) if total_rides else 0,
            'coordinate_coverage_pct': round(with_coords / total_rides * 100, 1) if total_rides else 0,
        }


def get_metadata_collector(session: Session) -> MetadataCollector:
    """
    Factory function to create a MetadataCollector.

    Args:
        session: SQLAlchemy session

    Returns:
        MetadataCollector instance
    """
    return MetadataCollector(session)
