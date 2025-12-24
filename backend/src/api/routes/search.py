"""
Theme Park Downtime Tracker - Search API Routes
================================================

Endpoints for search index data used by client-side fuzzy search (Fuse.js).

Query File Mapping
------------------
GET /search/index → Returns all parks and rides for client-side search index
"""

from flask import Blueprint, jsonify
from datetime import datetime, timezone
from sqlalchemy import select

from database.connection import get_db_session
from models import Park, Ride
from utils.cache import get_query_cache, generate_cache_key
from utils.logger import logger

search_bp = Blueprint('search', __name__)


@search_bp.route('/search/index', methods=['GET'])
def get_search_index():
    """
    Get search index containing all parks and rides for client-side fuzzy search.

    This endpoint returns a lightweight index optimized for Fuse.js client-side
    search. Data is cached for 5 minutes since park/ride names rarely change.

    Returns:
        JSON response with:
        - parks: List of park objects with id, name, location, type, url
        - rides: List of ride objects with id, name, park_name, park_id, type, url
        - meta: Index metadata (counts, last_updated timestamp)

    Performance: <100ms (cached for 5 minutes)
    """
    # Check cache first
    cache = get_query_cache()
    cache_key = generate_cache_key("search_index")

    cached_result = cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)

    try:
        with get_db_session() as session:
            # Fetch all active parks using ORM
            parks_stmt = (
                select(
                    Park.park_id,
                    Park.name,
                    Park.city,
                    Park.state_province
                )
                .where(Park.is_active == True)
                .order_by(Park.name)
            )
            parks_result = session.execute(parks_stmt)
            parks_rows = parks_result.fetchall()

            # Fetch all rides with their park names using ORM
            rides_stmt = (
                select(
                    Ride.ride_id,
                    Ride.name.label('ride_name'),
                    Park.name.label('park_name'),
                    Ride.park_id
                )
                .select_from(Ride)
                .join(Park, Ride.park_id == Park.park_id)
                .where(Ride.is_active == True)
                .where(Park.is_active == True)
                .order_by(Ride.name)
            )
            rides_result = session.execute(rides_stmt)
            rides_rows = rides_result.fetchall()

        # Format parks for search index
        parks = []
        for row in parks_rows:
            # Build location string (city, state)
            location_parts = []
            if row[2]:  # city
                location_parts.append(row[2])
            if row[3]:  # state_province
                location_parts.append(row[3])
            location = ", ".join(location_parts) if location_parts else ""

            parks.append({
                "id": row[0],
                "name": row[1] or "",
                "location": location,
                "type": "park",
                "url": f"/park-detail.html?id={row[0]}"
            })

        # Format rides for search index
        rides = []
        for row in rides_rows:
            rides.append({
                "id": row[0],
                "name": row[1] or "",
                "park_name": row[2] or "",
                "park_id": row[3],
                "type": "ride",
                "url": f"/ride-detail.html?id={row[0]}"
            })

        # Build response
        response = {
            "success": True,
            "parks": parks,
            "rides": rides,
            "meta": {
                "park_count": len(parks),
                "ride_count": len(rides),
                "last_updated": datetime.now(timezone.utc).isoformat()
            }
        }

        # Cache result (uses default TTL from QueryCache)
        cache.set(cache_key, response)

        return jsonify(response)

    except Exception as e:
        logger.error(f"Error fetching search index: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
            "parks": [],
            "rides": [],
            "meta": {
                "park_count": 0,
                "ride_count": 0,
                "last_updated": datetime.now(timezone.utc).isoformat()
            }
        }), 500
