#!/usr/bin/env python3
"""
Theme Park Downtime Tracker - Map ThemeParks.wiki IDs to Rides
Maps ThemeParks.wiki entity UUIDs to existing rides by matching names.

This script:
1. Fetches parks with themeparks_wiki_id
2. For each park, fetches children from ThemeParks.wiki API
3. Matches attractions by name to existing rides in the database
4. Updates the themeparks_wiki_id column for matched rides

Usage:
    python -m scripts.map_themeparks_wiki_ids [--dry-run]

Options:
    --dry-run   Preview changes without updating database
"""

import sys
import argparse
from pathlib import Path
from difflib import SequenceMatcher

# Add src to path
backend_src = Path(__file__).parent.parent
sys.path.insert(0, str(backend_src.absolute()))

from utils.logger import logger
from collector.themeparks_wiki_client import get_themeparks_wiki_client
from database.connection import get_db_session
from sqlalchemy import select, update
from models import Park, Ride


def normalize_name(name: str) -> str:
    """
    Normalize ride name for matching.

    Args:
        name: Original ride name

    Returns:
        Normalized name (lowercase, no special chars)
    """
    # Convert to lowercase
    name = name.lower()

    # Remove common suffixes that differ between APIs
    suffixes_to_remove = [
        ' - single rider',
        ' single rider',
        ' holiday',
        ' - holiday',
    ]
    for suffix in suffixes_to_remove:
        if name.endswith(suffix):
            name = name[:-len(suffix)]

    # Remove special characters but keep spaces
    result = ''
    for c in name:
        if c.isalnum() or c == ' ':
            result += c

    # Collapse multiple spaces
    return ' '.join(result.split())


def similarity_score(name1: str, name2: str) -> float:
    """
    Calculate similarity between two ride names.

    Args:
        name1: First ride name
        name2: Second ride name

    Returns:
        Similarity score between 0 and 1
    """
    norm1 = normalize_name(name1)
    norm2 = normalize_name(name2)

    # Exact match after normalization
    if norm1 == norm2:
        return 1.0

    # Use SequenceMatcher for fuzzy matching
    return SequenceMatcher(None, norm1, norm2).ratio()


def map_themeparks_wiki_ids(dry_run: bool = False):
    """
    Map ThemeParks.wiki entity IDs to existing rides.

    Args:
        dry_run: If True, preview changes without updating
    """
    logger.info("=" * 60)
    logger.info("THEMEPARKS.WIKI ID MAPPING - Starting")
    logger.info("=" * 60)

    if dry_run:
        logger.info("DRY RUN MODE - No changes will be made")

    client = get_themeparks_wiki_client()

    stats = {
        'parks_processed': 0,
        'rides_matched': 0,
        'rides_already_mapped': 0,
        'rides_not_found': 0,
        'api_attractions': 0,
        'errors': 0
    }

    try:
        with get_db_session() as session:
            # Step 1: Get all parks with themeparks_wiki_id
            parks_stmt = (
                select(Park.park_id, Park.name, Park.themeparks_wiki_id)
                .where(Park.themeparks_wiki_id.isnot(None))
                .where(Park.is_active == True)
                .order_by(Park.name)
            )

            parks = session.execute(parks_stmt).fetchall()
            logger.info(f"Found {len(parks)} parks with ThemeParks.wiki IDs")

            for park in parks:
                park_id = park.park_id
                park_name = park.name
                wiki_id = park.themeparks_wiki_id

                logger.info(f"\nProcessing: {park_name}")
                stats['parks_processed'] += 1

                try:
                    # Step 2: Fetch children from ThemeParks.wiki API
                    children = client.get_entity_children(wiki_id)

                    # Filter to attractions only
                    attractions = [c for c in children if c.get('entityType') == 'ATTRACTION']
                    stats['api_attractions'] += len(attractions)
                    logger.info(f"  Found {len(attractions)} attractions from API")

                    # Step 3: Get existing rides for this park
                    rides_stmt = (
                        select(Ride.ride_id, Ride.name, Ride.themeparks_wiki_id, Ride.category)
                        .where(Ride.park_id == park_id)
                        .where(Ride.is_active == True)
                    )

                    db_rides = session.execute(rides_stmt).fetchall()

                    # Build a lookup by normalized name
                    db_rides_by_name = {}
                    for ride in db_rides:
                        norm_name = normalize_name(ride.name)
                        db_rides_by_name[norm_name] = ride

                    # Step 4: Match API attractions to database rides
                    for attraction in attractions:
                        api_id = attraction.get('id')
                        api_name = attraction.get('name', '')

                        # Check if already mapped
                        existing_stmt = (
                            select(Ride.ride_id)
                            .where(Ride.themeparks_wiki_id == api_id)
                        )
                        existing = session.execute(existing_stmt).fetchone()

                        if existing:
                            stats['rides_already_mapped'] += 1
                            continue

                        # Try exact match first
                        norm_api_name = normalize_name(api_name)
                        matched_ride = db_rides_by_name.get(norm_api_name)

                        # If no exact match, try fuzzy matching
                        if not matched_ride:
                            best_score = 0
                            best_ride = None
                            for db_ride in db_rides:
                                score = similarity_score(api_name, db_ride.name)
                                if score > best_score and score >= 0.8:  # 80% threshold
                                    best_score = score
                                    best_ride = db_ride

                            if best_ride:
                                matched_ride = best_ride
                                logger.info(f"    Fuzzy match: '{api_name}' -> '{best_ride.name}' ({best_score:.0%})")

                        if matched_ride:
                            # If ride already has a different themeparks_wiki_id, update it
                            # (API IDs can change when ThemeParks.wiki refreshes their data)
                            if matched_ride.themeparks_wiki_id and matched_ride.themeparks_wiki_id != api_id:
                                logger.info(f"    Updating stale ID: {api_name} ({matched_ride.themeparks_wiki_id[:8]}... -> {api_id[:8]}...)")
                                stats['rides_updated'] = stats.get('rides_updated', 0) + 1
                            elif matched_ride.themeparks_wiki_id == api_id:
                                # Already correctly mapped
                                stats['rides_already_mapped'] += 1
                                continue

                            # Update the ride with themeparks_wiki_id
                            if not dry_run:
                                # Use COALESCE logic: only set category if it's currently NULL
                                current_category = matched_ride.category
                                new_category = current_category if current_category else 'ATTRACTION'

                                update_stmt = (
                                    update(Ride)
                                    .where(Ride.ride_id == matched_ride.ride_id)
                                    .values(
                                        themeparks_wiki_id=api_id,
                                        category=new_category
                                    )
                                )
                                session.execute(update_stmt)

                            stats['rides_matched'] += 1
                            logger.info(f"    Mapped: {api_name}")
                        else:
                            stats['rides_not_found'] += 1
                            logger.debug(f"    No match: {api_name}")

                except Exception as e:
                    logger.error(f"  Error processing {park_name}: {e}")
                    stats['errors'] += 1

            if not dry_run:
                session.commit()

            # Print summary
            logger.info("")
            logger.info("=" * 60)
            logger.info("MAPPING SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Parks processed:      {stats['parks_processed']}")
            logger.info(f"API attractions:      {stats['api_attractions']}")
            logger.info(f"Rides matched:        {stats['rides_matched']}")
            logger.info(f"Stale IDs updated:    {stats.get('rides_updated', 0)}")
            logger.info(f"Already mapped:       {stats['rides_already_mapped']}")
            logger.info(f"No match found:       {stats['rides_not_found']}")
            logger.info(f"Errors:               {stats['errors']}")
            logger.info("=" * 60)

            if dry_run:
                logger.info("DRY RUN - No changes were made. Run without --dry-run to apply.")
            else:
                logger.info("MAPPING COMPLETE - IDs have been updated")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='Map ThemeParks.wiki entity IDs to existing rides'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without updating database'
    )

    args = parser.parse_args()
    map_themeparks_wiki_ids(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
