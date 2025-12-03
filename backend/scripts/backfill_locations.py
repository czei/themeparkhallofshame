#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Backfill Park Locations

One-time script to populate city/state/country for existing parks using
reverse geocoding from their latitude/longitude coordinates.

Uses OpenStreetMap Nominatim (free, no API key required).
Rate limited to 1 request/second per Nominatim ToS.

Usage:
    python scripts/backfill_locations.py [--dry-run]

Options:
    --dry-run    Show what would be updated without making changes
"""

import sys
import argparse
from pathlib import Path

# Add src to path
backend_src = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))

from utils.logger import logger
from collector.geocoding_client import GeocodingClient
from database.connection import get_db_connection
from sqlalchemy import text


def backfill_locations(dry_run: bool = False):
    """
    Backfill missing park locations using reverse geocoding.

    Args:
        dry_run: If True, show what would be updated without making changes
    """
    logger.info("=" * 60)
    logger.info("BACKFILL PARK LOCATIONS")
    logger.info("=" * 60)

    if dry_run:
        logger.info("DRY RUN MODE - No changes will be made")

    geocoder = GeocodingClient()

    stats = {
        'total': 0,
        'updated': 0,
        'skipped': 0,
        'failed': 0
    }

    try:
        with get_db_connection() as conn:
            # Find parks with Unknown city but valid coordinates
            result = conn.execute(text("""
                SELECT park_id, name, latitude, longitude, city, state_province, country
                FROM parks
                WHERE (city = 'Unknown' OR city IS NULL)
                  AND latitude IS NOT NULL
                  AND longitude IS NOT NULL
                ORDER BY name
            """))

            parks = result.fetchall()
            stats['total'] = len(parks)

            logger.info(f"Found {stats['total']} parks needing location data")
            logger.info("")

            for park in parks:
                park_id = park[0]
                name = park[1]
                lat = park[2]
                lng = park[3]

                logger.info(f"Processing: {name}")
                logger.info(f"  Coordinates: ({lat}, {lng})")

                # Call geocoding API
                location = geocoder.reverse_geocode(lat, lng)

                if location and (location['city'] or location['state']):
                    city = location['city'] or 'Unknown'
                    state = location['state'] or ''
                    country = location['country'] or ''

                    logger.info(f"  Result: {city}, {state}, {country}")

                    if not dry_run:
                        conn.execute(text("""
                            UPDATE parks
                            SET city = :city, state_province = :state, country = :country
                            WHERE park_id = :park_id
                        """), {
                            'city': city,
                            'state': state,
                            'country': country,
                            'park_id': park_id
                        })

                    stats['updated'] += 1
                    logger.info(f"  ✓ {'Would update' if dry_run else 'Updated'}")
                else:
                    logger.warning("  ✗ No location found")
                    stats['failed'] += 1

                logger.info("")

            # Print summary
            logger.info("=" * 60)
            logger.info("SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Total parks processed: {stats['total']}")
            logger.info(f"Successfully updated:  {stats['updated']}")
            logger.info(f"Failed to geocode:     {stats['failed']}")
            logger.info(f"Skipped:               {stats['skipped']}")

            if dry_run:
                logger.info("")
                logger.info("This was a dry run. Run without --dry-run to apply changes.")

    except Exception as e:
        logger.error(f"Backfill failed: {e}", exc_info=True)
        sys.exit(1)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Backfill missing park locations using reverse geocoding'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be updated without making changes'
    )

    args = parser.parse_args()
    backfill_locations(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
