#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - ThemeParks.wiki Entity Mapper

Maps our existing parks and rides to ThemeParks.wiki entity UUIDs using fuzzy matching.
Run after migration 008_themeparks_wiki.sql to populate themeparks_wiki_id columns.

Usage:
    python scripts/map_themeparks_wiki_entities.py [--dry-run] [--parks-only] [--threshold 0.7]

Options:
    --dry-run       Show matches without updating database
    --parks-only    Only map parks, skip rides
    --threshold     Minimum fuzzy match ratio (default: 0.7)
"""

import sys
import re
import argparse
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from difflib import SequenceMatcher

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.logger import logger
from database.connection import get_db_connection
from collector.themeparks_wiki_client import get_themeparks_wiki_client
from sqlalchemy import text


class ThemeParksWikiMapper:
    """Maps our database entities to ThemeParks.wiki UUIDs."""

    def __init__(self, dry_run: bool = False, match_threshold: float = 0.7):
        self.dry_run = dry_run
        self.match_threshold = match_threshold
        self.client = get_themeparks_wiki_client()
        self.stats = {
            "parks_total": 0,
            "parks_matched": 0,
            "parks_unmatched": 0,
            "rides_total": 0,
            "rides_matched": 0,
            "rides_unmatched": 0,
        }

    def normalize_name(self, name: str) -> str:
        """
        Normalize name for fuzzy matching.

        Removes:
        - Apostrophes and special characters
        - Leading "The", "Disney's", etc.
        - Trademark symbols
        """
        name = name.lower()
        name = re.sub(r"[''`'\\u2019\\u2018]", "", name)  # Apostrophes
        name = re.sub(r"[\\u00ae\\u2122\\u00a9™®©]", "", name)  # Trademark/copyright
        name = re.sub(r"^the\\s+", "", name)  # Leading "The"
        name = re.sub(r"^disney'?s?\\s+", "", name)  # "Disney's"
        name = re.sub(r"\\s*[-–—]\\s*", " ", name)  # Dashes to spaces
        name = re.sub(r"\\s+", " ", name).strip()
        return name

    def match_score(self, name1: str, name2: str) -> float:
        """Calculate fuzzy match score between two names."""
        norm1 = self.normalize_name(name1)
        norm2 = self.normalize_name(name2)
        return SequenceMatcher(None, norm1, norm2).ratio()

    def find_best_match(
        self,
        our_name: str,
        candidates: List[Dict],
        name_key: str = "name"
    ) -> Tuple[Optional[Dict], float]:
        """Find best matching candidate for our entity name."""
        best_match = None
        best_score = 0

        for candidate in candidates:
            score = self.match_score(our_name, candidate.get(name_key, ""))
            if score > best_score:
                best_score = score
                best_match = candidate

        if best_score >= self.match_threshold:
            return best_match, best_score
        return None, best_score

    def get_our_parks(self, conn) -> List[Dict]:
        """Get all active parks from our database."""
        # Check if themeparks_wiki_id column exists
        try:
            check = text("SELECT themeparks_wiki_id FROM parks LIMIT 1")
            conn.execute(check)
            has_wiki_col = True
        except Exception:
            has_wiki_col = False

        if has_wiki_col:
            query = text("""
                SELECT park_id, queue_times_id, name, city, country, operator,
                       is_disney, is_universal, themeparks_wiki_id
                FROM parks
                WHERE is_active = TRUE
                ORDER BY name
            """)
        else:
            query = text("""
                SELECT park_id, queue_times_id, name, city, country, operator,
                       is_disney, is_universal, NULL as themeparks_wiki_id
                FROM parks
                WHERE is_active = TRUE
                ORDER BY name
            """)

        result = conn.execute(query)
        return [dict(row._mapping) for row in result]

    def get_our_rides(self, conn, park_id: int) -> List[Dict]:
        """Get all active rides for a park."""
        # Check if themeparks_wiki_id column exists
        try:
            check = text("SELECT themeparks_wiki_id FROM rides LIMIT 1")
            conn.execute(check)
            has_wiki_col = True
        except Exception:
            has_wiki_col = False

        if has_wiki_col:
            query = text("""
                SELECT ride_id, queue_times_id, name, category, themeparks_wiki_id
                FROM rides
                WHERE park_id = :park_id AND is_active = TRUE
                ORDER BY name
            """)
        else:
            query = text("""
                SELECT ride_id, queue_times_id, name, category, NULL as themeparks_wiki_id
                FROM rides
                WHERE park_id = :park_id AND is_active = TRUE
                ORDER BY name
            """)

        result = conn.execute(query, {"park_id": park_id})
        return [dict(row._mapping) for row in result]

    def update_park_wiki_id(self, conn, park_id: int, wiki_id: str):
        """Update park's themeparks_wiki_id."""
        if self.dry_run:
            return
        query = text("""
            UPDATE parks SET themeparks_wiki_id = :wiki_id WHERE park_id = :park_id
        """)
        conn.execute(query, {"park_id": park_id, "wiki_id": wiki_id})

    def update_ride_wiki_id(self, conn, ride_id: int, wiki_id: str, entity_type: str):
        """Update ride's themeparks_wiki_id and entity_type."""
        if self.dry_run:
            return
        query = text("""
            UPDATE rides
            SET themeparks_wiki_id = :wiki_id, entity_type = :entity_type
            WHERE ride_id = :ride_id
        """)
        conn.execute(query, {
            "ride_id": ride_id,
            "wiki_id": wiki_id,
            "entity_type": entity_type
        })

    def map_parks(self, conn) -> Dict[int, str]:
        """
        Map our parks to ThemeParks.wiki parks.

        Returns:
            Dict mapping our park_id to ThemeParks.wiki entity_id
        """
        logger.info("=" * 60)
        logger.info("MAPPING PARKS")
        logger.info("=" * 60)

        # Get all parks from ThemeParks.wiki
        wiki_parks = self.client.get_all_parks()
        logger.info(f"ThemeParks.wiki has {len(wiki_parks)} parks")

        # Get our parks
        our_parks = self.get_our_parks(conn)
        logger.info(f"Our database has {len(our_parks)} active parks")

        park_mapping = {}
        unmatched = []

        for our_park in our_parks:
            self.stats["parks_total"] += 1

            # Skip if already mapped
            if our_park.get("themeparks_wiki_id"):
                park_mapping[our_park["park_id"]] = our_park["themeparks_wiki_id"]
                self.stats["parks_matched"] += 1
                logger.debug(f"  {our_park['name']} - already mapped")
                continue

            # Find best match
            match, score = self.find_best_match(our_park["name"], wiki_parks)

            if match:
                wiki_id = match["id"]
                park_mapping[our_park["park_id"]] = wiki_id
                self.stats["parks_matched"] += 1

                logger.info(
                    f"  MATCH ({score:.0%}): {our_park['name']} -> "
                    f"{match['name']} [{wiki_id[:8]}...]"
                )

                self.update_park_wiki_id(conn, our_park["park_id"], wiki_id)
            else:
                self.stats["parks_unmatched"] += 1
                unmatched.append(our_park)
                logger.warning(f"  NO MATCH: {our_park['name']} (best: {score:.0%})")

        if unmatched:
            logger.info(f"\n{len(unmatched)} unmatched parks:")
            for p in unmatched[:10]:
                logger.info(f"  - {p['name']} ({p['city']}, {p['country']})")

        return park_mapping

    def map_rides_for_park(
        self,
        conn,
        our_park_id: int,
        wiki_park_id: str,
        our_park_name: str
    ):
        """Map rides for a single park."""
        # Get attractions from ThemeParks.wiki
        try:
            wiki_children = self.client.get_entity_children(wiki_park_id)
        except Exception as e:
            logger.warning(f"  Failed to fetch attractions for {our_park_name}: {e}")
            return

        # Filter to attractions only
        wiki_attractions = [
            c for c in wiki_children
            if c.get("entityType") == "ATTRACTION"
        ]

        if not wiki_attractions:
            logger.debug(f"  No attractions found for {our_park_name}")
            return

        # Get our rides for this park
        our_rides = self.get_our_rides(conn, our_park_id)

        if not our_rides:
            logger.debug(f"  No rides in our DB for {our_park_name}")
            return

        logger.info(f"\n  {our_park_name}: {len(our_rides)} rides vs {len(wiki_attractions)} wiki attractions")

        matched = 0
        for our_ride in our_rides:
            self.stats["rides_total"] += 1

            # Skip if already mapped
            if our_ride.get("themeparks_wiki_id"):
                self.stats["rides_matched"] += 1
                matched += 1
                continue

            # Find best match
            match, score = self.find_best_match(our_ride["name"], wiki_attractions)

            if match:
                wiki_id = match["id"]
                entity_type = match.get("entityType", "ATTRACTION")

                self.stats["rides_matched"] += 1
                matched += 1

                logger.debug(
                    f"    MATCH ({score:.0%}): {our_ride['name']} -> {match['name']}"
                )

                self.update_ride_wiki_id(conn, our_ride["ride_id"], wiki_id, entity_type)
            else:
                self.stats["rides_unmatched"] += 1
                logger.debug(f"    NO MATCH: {our_ride['name']}")

        logger.info(f"    Matched {matched}/{len(our_rides)} rides")

    def map_all_rides(self, conn, park_mapping: Dict[int, str]):
        """Map rides for all mapped parks."""
        logger.info("\n" + "=" * 60)
        logger.info("MAPPING RIDES")
        logger.info("=" * 60)

        our_parks = self.get_our_parks(conn)

        for our_park in our_parks:
            park_id = our_park["park_id"]
            wiki_id = park_mapping.get(park_id)

            if not wiki_id:
                continue

            self.map_rides_for_park(conn, park_id, wiki_id, our_park["name"])

    def run(self, parks_only: bool = False):
        """Main execution."""
        logger.info("=" * 60)
        logger.info("THEMEPARKS.WIKI ENTITY MAPPER")
        logger.info("=" * 60)

        if self.dry_run:
            logger.info("DRY RUN MODE - No database changes will be made")

        logger.info(f"Match threshold: {self.match_threshold:.0%}")

        with get_db_connection() as conn:
            # Map parks first
            park_mapping = self.map_parks(conn)

            # Map rides if not parks-only
            if not parks_only:
                self.map_all_rides(conn, park_mapping)

            # Print summary
            self._print_summary()

    def _print_summary(self):
        """Print mapping summary."""
        logger.info("\n" + "=" * 60)
        logger.info("MAPPING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Parks total:     {self.stats['parks_total']}")
        logger.info(f"Parks matched:   {self.stats['parks_matched']}")
        logger.info(f"Parks unmatched: {self.stats['parks_unmatched']}")
        logger.info(f"Rides total:     {self.stats['rides_total']}")
        logger.info(f"Rides matched:   {self.stats['rides_matched']}")
        logger.info(f"Rides unmatched: {self.stats['rides_unmatched']}")

        if self.stats["parks_total"] > 0:
            park_rate = self.stats["parks_matched"] / self.stats["parks_total"] * 100
            logger.info(f"\nPark match rate:  {park_rate:.1f}%")

        if self.stats["rides_total"] > 0:
            ride_rate = self.stats["rides_matched"] / self.stats["rides_total"] * 100
            logger.info(f"Ride match rate:  {ride_rate:.1f}%")


def main():
    parser = argparse.ArgumentParser(
        description="Map database entities to ThemeParks.wiki UUIDs"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show matches without updating database"
    )
    parser.add_argument(
        "--parks-only",
        action="store_true",
        help="Only map parks, skip rides"
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.7,
        help="Minimum fuzzy match ratio (default: 0.7)"
    )

    args = parser.parse_args()

    mapper = ThemeParksWikiMapper(
        dry_run=args.dry_run,
        match_threshold=args.threshold
    )
    mapper.run(parks_only=args.parks_only)


if __name__ == "__main__":
    main()
