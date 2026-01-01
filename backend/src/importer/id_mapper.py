"""
ID Mapper for ThemeParks.wiki UUID Reconciliation
Maps ThemeParks.wiki UUIDs to internal integer IDs with fuzzy name matching.
Feature: 004-themeparks-data-collection
"""

import logging
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import select, func

try:
    from Levenshtein import distance as levenshtein_distance
except ImportError:
    # Fallback to simple implementation if python-Levenshtein not installed
    import warnings
    warnings.warn(
        "The 'python-Levenshtein' library is not installed. "
        "Falling back to a significantly slower pure Python implementation. "
        "Install it with: pip install python-Levenshtein",
        ImportWarning,
        stacklevel=2
    )

    def levenshtein_distance(s1: str, s2: str) -> int:
        """Simple Levenshtein distance implementation."""
        if len(s1) < len(s2):
            return levenshtein_distance(s2, s1)
        if len(s2) == 0:
            return len(s1)
        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        return previous_row[-1]

from models.orm_ride import Ride
from models.orm_park import Park

logger = logging.getLogger(__name__)


@dataclass
class MappingResult:
    """Result of an ID mapping attempt."""
    ride_id: Optional[int]
    park_id: Optional[int]
    match_type: str  # 'exact_uuid', 'exact_name', 'fuzzy_name', 'created', 'not_found'
    confidence: float  # 0.0 to 1.0
    matched_name: Optional[str] = None
    distance: Optional[int] = None


class IDMapper:
    """
    Maps ThemeParks.wiki UUIDs and names to internal integer IDs.

    Uses a multi-step matching algorithm:
    1. Exact match on themeparks_wiki_id (UUID)
    2. Exact match on (park_id, ride_name)
    3. Fuzzy match on (park_id, ride_name) with Levenshtein distance
    4. Optionally create new records for unmatched entities
    """

    # Maximum Levenshtein distance for fuzzy matching
    MAX_FUZZY_DISTANCE = 3

    # Minimum confidence threshold for automatic matching
    MIN_CONFIDENCE = 0.8

    def __init__(
        self,
        session: Session,
        auto_create: bool = False,
        max_distance: int = MAX_FUZZY_DISTANCE
    ):
        """
        Initialize ID mapper.

        Args:
            session: SQLAlchemy session
            auto_create: Whether to automatically create unmatched entities
            max_distance: Maximum Levenshtein distance for fuzzy matching
        """
        self.session = session
        self.auto_create = auto_create
        self.max_distance = max_distance

        # Caches for performance
        self._ride_uuid_cache: Dict[str, int] = {}
        self._ride_name_cache: Dict[Tuple[int, str], int] = {}
        self._park_uuid_cache: Dict[str, int] = {}
        self._park_slug_cache: Dict[str, int] = {}
        self._rides_by_park: Dict[int, List[Tuple[int, str]]] = {}

        # Statistics
        self._stats = {
            'exact_uuid': 0,
            'exact_name': 0,
            'fuzzy_name': 0,
            'created': 0,
            'not_found': 0,
            'cache_hits': 0
        }

    @property
    def stats(self) -> Dict[str, int]:
        """Get mapping statistics."""
        return self._stats.copy()

    def _normalize_name(self, name: str) -> str:
        """Normalize ride name for comparison."""
        # Remove special characters, lowercase, collapse whitespace
        import re
        normalized = name.lower()
        normalized = re.sub(r'[^\w\s]', '', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return normalized

    def _load_rides_for_park(self, park_id: int) -> None:
        """Load all rides for a park into cache."""
        if park_id in self._rides_by_park:
            return

        stmt = select(Ride).where(Ride.park_id == park_id)
        rides = self.session.execute(stmt).scalars().all()

        self._rides_by_park[park_id] = [
            (ride.ride_id, self._normalize_name(ride.ride_name))
            for ride in rides
        ]

        # Also populate other caches
        for ride in rides:
            if ride.themeparks_wiki_id:
                self._ride_uuid_cache[ride.themeparks_wiki_id] = ride.ride_id
            key = (park_id, self._normalize_name(ride.ride_name))
            self._ride_name_cache[key] = ride.ride_id

    def map_park(
        self,
        park_uuid: Optional[str] = None,
        park_slug: Optional[str] = None
    ) -> Optional[int]:
        """
        Map a ThemeParks.wiki park to internal park_id.

        Args:
            park_uuid: ThemeParks.wiki park UUID
            park_slug: Park slug (e.g., 'universalstudiosflorida')

        Returns:
            Internal park_id or None
        """
        # Try UUID cache first
        if park_uuid and park_uuid in self._park_uuid_cache:
            self._stats['cache_hits'] += 1
            return self._park_uuid_cache[park_uuid]

        # Try slug cache
        if park_slug and park_slug in self._park_slug_cache:
            self._stats['cache_hits'] += 1
            return self._park_slug_cache[park_slug]

        # Query database by UUID
        if park_uuid:
            stmt = select(Park).where(Park.themeparks_wiki_id == park_uuid)
            park = self.session.execute(stmt).scalar_one_or_none()
            if park:
                self._park_uuid_cache[park_uuid] = park.park_id
                return park.park_id

        # Query database by slug (using park_slug or similar field)
        if park_slug:
            # Try to match by name containing the slug
            slug_pattern = park_slug.replace('_', ' ').replace('-', ' ')
            stmt = select(Park).where(
                func.lower(Park.park_name).contains(slug_pattern.lower())
            )
            park = self.session.execute(stmt).scalar_one_or_none()
            if park:
                self._park_slug_cache[park_slug] = park.park_id
                return park.park_id

        return None

    def map_ride(
        self,
        themeparks_id: str,
        name: str,
        park_id: int
    ) -> MappingResult:
        """
        Map a ThemeParks.wiki entity to internal ride_id.

        Uses multi-step matching algorithm:
        1. Exact match on themeparks_wiki_id
        2. Exact match on normalized name
        3. Fuzzy match on normalized name
        4. Create new record (if auto_create enabled)

        Args:
            themeparks_id: ThemeParks.wiki entity UUID
            name: Entity name
            park_id: Internal park ID

        Returns:
            MappingResult with ride_id and match details
        """
        normalized_name = self._normalize_name(name)

        # Step 1: Check UUID cache
        if themeparks_id in self._ride_uuid_cache:
            self._stats['cache_hits'] += 1
            return MappingResult(
                ride_id=self._ride_uuid_cache[themeparks_id],
                park_id=park_id,
                match_type='exact_uuid',
                confidence=1.0,
                matched_name=name
            )

        # Query for UUID match in database
        stmt = select(Ride).where(Ride.themeparks_wiki_id == themeparks_id)
        ride = self.session.execute(stmt).scalar_one_or_none()
        if ride:
            self._ride_uuid_cache[themeparks_id] = ride.ride_id
            self._stats['exact_uuid'] += 1
            return MappingResult(
                ride_id=ride.ride_id,
                park_id=ride.park_id,
                match_type='exact_uuid',
                confidence=1.0,
                matched_name=ride.ride_name
            )

        # Step 2: Check exact name cache
        name_key = (park_id, normalized_name)
        if name_key in self._ride_name_cache:
            ride_id = self._ride_name_cache[name_key]
            self._stats['cache_hits'] += 1
            return MappingResult(
                ride_id=ride_id,
                park_id=park_id,
                match_type='exact_name',
                confidence=1.0,
                matched_name=name
            )

        # Load all rides for park if not cached
        self._load_rides_for_park(park_id)

        # Step 2b: Exact name match from loaded rides
        if name_key in self._ride_name_cache:
            ride_id = self._ride_name_cache[name_key]
            self._stats['exact_name'] += 1
            return MappingResult(
                ride_id=ride_id,
                park_id=park_id,
                match_type='exact_name',
                confidence=1.0,
                matched_name=name
            )

        # Step 3: Fuzzy name matching
        best_match: Optional[Tuple[int, str, int]] = None  # (ride_id, name, distance)
        for ride_id, ride_name in self._rides_by_park.get(park_id, []):
            dist = levenshtein_distance(normalized_name, ride_name)
            if dist <= self.max_distance:
                if best_match is None or dist < best_match[2]:
                    best_match = (ride_id, ride_name, dist)

        if best_match:
            ride_id, matched_name, distance = best_match
            # Calculate confidence based on distance and name length
            max_len = max(len(normalized_name), len(matched_name))
            confidence = 1.0 - (distance / max_len) if max_len > 0 else 0.0

            if confidence >= self.MIN_CONFIDENCE:
                # Update the UUID mapping for future lookups
                self._update_ride_uuid(ride_id, themeparks_id)
                self._stats['fuzzy_name'] += 1

                return MappingResult(
                    ride_id=ride_id,
                    park_id=park_id,
                    match_type='fuzzy_name',
                    confidence=confidence,
                    matched_name=matched_name,
                    distance=distance
                )

        # Step 4: Create new record if enabled
        if self.auto_create:
            # Generate a unique queue_times_id using hash of themeparks_id
            # Use negative values to distinguish from real queue-times IDs
            generated_queue_id = -abs(hash(themeparks_id)) % (10**9)

            ride = Ride(
                park_id=park_id,
                name=name,
                themeparks_wiki_id=themeparks_id,
                queue_times_id=generated_queue_id,
                tier=2  # Default tier
            )
            self.session.add(ride)
            self.session.flush()

            # Update caches
            self._ride_uuid_cache[themeparks_id] = ride.ride_id
            self._ride_name_cache[name_key] = ride.ride_id
            self._rides_by_park.setdefault(park_id, []).append(
                (ride.ride_id, normalized_name)
            )
            self._stats['created'] += 1

            return MappingResult(
                ride_id=ride.ride_id,
                park_id=park_id,
                match_type='created',
                confidence=1.0,
                matched_name=name
            )

        # No match found
        self._stats['not_found'] += 1
        logger.warning(
            f"Could not map ride: uuid={themeparks_id}, name={name}, park_id={park_id}"
        )
        return MappingResult(
            ride_id=None,
            park_id=park_id,
            match_type='not_found',
            confidence=0.0
        )

    def _update_ride_uuid(self, ride_id: int, themeparks_id: str) -> None:
        """Update ride's themeparks_wiki_id after fuzzy match."""
        ride = self.session.get(Ride, ride_id)
        if ride and not ride.themeparks_wiki_id:
            ride.themeparks_wiki_id = themeparks_id
            self._ride_uuid_cache[themeparks_id] = ride_id
            logger.info(f"Updated ride {ride_id} with UUID {themeparks_id}")

    def map_entity_from_event(
        self,
        entity_id: str,
        name: str,
        park_id: Optional[str] = None,
        park_slug: Optional[str] = None
    ) -> MappingResult:
        """
        Map an entity from an archive event.

        Convenience method that handles park resolution and ride mapping.

        Args:
            entity_id: ThemeParks.wiki entity UUID
            name: Entity name
            park_id: ThemeParks.wiki park UUID
            park_slug: Park slug

        Returns:
            MappingResult
        """
        # First resolve the park
        internal_park_id = self.map_park(park_uuid=park_id, park_slug=park_slug)
        if not internal_park_id:
            logger.warning(
                f"Could not resolve park: uuid={park_id}, slug={park_slug}"
            )
            return MappingResult(
                ride_id=None,
                park_id=None,
                match_type='not_found',
                confidence=0.0
            )

        # Then map the ride
        return self.map_ride(entity_id, name, internal_park_id)

    def bulk_map(
        self,
        entities: List[Tuple[str, str, int]]  # (uuid, name, park_id)
    ) -> Dict[str, MappingResult]:
        """
        Map multiple entities in bulk.

        Args:
            entities: List of (themeparks_id, name, park_id) tuples

        Returns:
            Dict mapping themeparks_id to MappingResult
        """
        results = {}
        for themeparks_id, name, park_id in entities:
            results[themeparks_id] = self.map_ride(themeparks_id, name, park_id)
        return results

    def clear_caches(self) -> None:
        """Clear all internal caches."""
        self._ride_uuid_cache.clear()
        self._ride_name_cache.clear()
        self._park_uuid_cache.clear()
        self._park_slug_cache.clear()
        self._rides_by_park.clear()

    def reset_stats(self) -> None:
        """Reset mapping statistics."""
        self._stats = {
            'exact_uuid': 0,
            'exact_name': 0,
            'fuzzy_name': 0,
            'created': 0,
            'not_found': 0,
            'cache_hits': 0
        }
