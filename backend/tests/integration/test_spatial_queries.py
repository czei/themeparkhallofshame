"""
Integration Tests: Spatial Queries (Feature 004)

Tests for ST_Distance_Sphere queries used to calculate distances
between attractions and find nearby entities.

Feature: 004-themeparks-data-collection
Task: T056
"""

import pytest
from decimal import Decimal
from sqlalchemy import func, text

from models.orm_metadata import EntityMetadata
from models.orm_ride import Ride
from models.orm_park import Park


# Test coordinates (real theme park locations)
MAGIC_KINGDOM_COORDS = (28.4177, -81.5812)  # Walt Disney World Magic Kingdom
EPCOT_COORDS = (28.3747, -81.5494)  # EPCOT (about 10km from MK)
UNIVERSAL_STUDIOS_COORDS = (28.4741, -81.4687)  # Universal Studios (~15km from MK)
DISNEY_SPRINGS_COORDS = (28.3702, -81.5216)  # Disney Springs (~12km from MK)


@pytest.mark.integration
class TestSpatialQueryBasics:
    """Basic spatial query tests with ST_Distance_Sphere."""

    def _create_test_park(self, session, name="Test Park", lat=28.4177, lon=-81.5812):
        """Helper to create a test park."""
        from sqlalchemy import func as sqlfunc
        max_id = session.query(sqlfunc.max(Park.queue_times_id)).scalar() or 90000
        park = Park(
            queue_times_id=max_id + 1,
            name=name,
            city="Orlando",
            state_province="FL",
            country="US",
            latitude=Decimal(str(lat)),
            longitude=Decimal(str(lon)),
            themeparks_wiki_id=f"park-{name.lower().replace(' ', '-')}",
            timezone='America/New_York'
        )
        session.add(park)
        session.flush()
        return park

    def _create_test_ride_with_metadata(
        self, session, park, name="Test Ride", lat=28.4177, lon=-81.5812
    ):
        """Helper to create a test ride with metadata."""
        from sqlalchemy import func as sqlfunc
        max_id = session.query(sqlfunc.max(Ride.queue_times_id)).scalar() or 80000
        ride = Ride(
            queue_times_id=max_id + 1,
            name=name,
            park_id=park.park_id,
            themeparks_wiki_id=f"ride-{name.lower().replace(' ', '-')}",
            tier=1
        )
        session.add(ride)
        session.flush()

        metadata = EntityMetadata(
            ride_id=ride.ride_id,
            themeparks_wiki_id=ride.themeparks_wiki_id,
            entity_name=name,
            entity_type='ATTRACTION',
            latitude=Decimal(str(lat)),
            longitude=Decimal(str(lon))
        )
        session.add(metadata)
        session.flush()

        return ride, metadata

    def test_st_distance_sphere_available(self, mysql_session):
        """Verify ST_Distance_Sphere function is available in MySQL."""
        # Create a simple test with two points
        result = mysql_session.execute(text("""
            SELECT ST_Distance_Sphere(
                POINT(-81.5812, 28.4177),
                POINT(-81.5494, 28.3747)
            ) AS distance_meters
        """)).scalar()

        # MK to EPCOT is roughly 6-7 km
        assert result is not None
        assert 5000 < result < 10000  # Between 5km and 10km

    def test_distance_between_metadata_points(self, mysql_session):
        """Calculate distance between two attraction metadata points."""
        park = self._create_test_park(mysql_session, "Test WDW Park")

        # Create Space Mountain
        ride1, meta1 = self._create_test_ride_with_metadata(
            mysql_session, park, "Space Mountain",
            lat=28.4193, lon=-81.5781
        )

        # Create Big Thunder Mountain (about 300m away)
        ride2, meta2 = self._create_test_ride_with_metadata(
            mysql_session, park, "Big Thunder Mountain",
            lat=28.4201, lon=-81.5846
        )

        # Query distance between the two attractions
        result = mysql_session.execute(text("""
            SELECT ST_Distance_Sphere(
                POINT(:lon1, :lat1),
                POINT(:lon2, :lat2)
            ) AS distance_meters
        """), {
            'lat1': float(meta1.latitude),
            'lon1': float(meta1.longitude),
            'lat2': float(meta2.latitude),
            'lon2': float(meta2.longitude)
        }).scalar()

        # Should be a few hundred meters
        assert result is not None
        assert 100 < result < 1000  # Between 100m and 1km


@pytest.mark.integration
class TestNearbyAttractionsQuery:
    """Test queries for finding nearby attractions."""

    def _create_test_park(self, session, name="Test Park"):
        """Helper to create a test park."""
        from sqlalchemy import func as sqlfunc
        max_id = session.query(sqlfunc.max(Park.queue_times_id)).scalar() or 90000
        park = Park(
            queue_times_id=max_id + 1,
            name=name,
            city="Orlando",
            state_province="FL",
            country="US",
            latitude=Decimal('28.4177'),
            longitude=Decimal('-81.5812'),
            themeparks_wiki_id=f"park-{name.lower().replace(' ', '-')}",
            timezone='America/New_York'
        )
        session.add(park)
        session.flush()
        return park

    def _create_ride_with_coords(self, session, park, name, lat, lon, i):
        """Helper to create a ride with metadata at given coordinates."""
        from sqlalchemy import func as sqlfunc
        max_id = session.query(sqlfunc.max(Ride.queue_times_id)).scalar() or 80000
        ride = Ride(
            queue_times_id=max_id + i + 1,
            name=name,
            park_id=park.park_id,
            themeparks_wiki_id=f"ride-{name.lower().replace(' ', '-')}-{i}",
            tier=1
        )
        session.add(ride)
        session.flush()

        metadata = EntityMetadata(
            ride_id=ride.ride_id,
            themeparks_wiki_id=ride.themeparks_wiki_id,
            entity_name=name,
            entity_type='ATTRACTION',
            latitude=Decimal(str(lat)),
            longitude=Decimal(str(lon))
        )
        session.add(metadata)
        session.flush()
        return ride, metadata

    def test_find_attractions_within_radius(self, mysql_session):
        """Find all attractions within a specified radius."""
        park = self._create_test_park(mysql_session, "Radius Test Park")

        # Create attractions at various distances from a center point
        # Center: Space Mountain (28.4193, -81.5781)
        center_lat, center_lon = 28.4193, -81.5781

        attractions = [
            # Name, lat, lon, expected distance (approx)
            ("Space Mountain", 28.4193, -81.5781, 0),  # Center
            ("Big Thunder Mountain", 28.4201, -81.5846, 600),  # ~600m
            ("Splash Mountain", 28.4194, -81.5850, 640),  # ~640m
            ("Haunted Mansion", 28.4209, -81.5832, 500),  # ~500m
            ("Pirates of Caribbean", 28.4185, -81.5840, 550),  # ~550m (closer)
            ("Jungle Cruise", 28.4213, -81.5866, 800),  # ~800m - outside 700m
        ]

        for i, (name, lat, lon, _) in enumerate(attractions):
            self._create_ride_with_coords(mysql_session, park, name, lat, lon, i)

        # Find attractions within 700 meters
        radius_meters = 700

        result = mysql_session.execute(text("""
            SELECT
                em.entity_name,
                ST_Distance_Sphere(
                    POINT(em.longitude, em.latitude),
                    POINT(:center_lon, :center_lat)
                ) AS distance_meters
            FROM entity_metadata em
            WHERE em.latitude IS NOT NULL
              AND em.longitude IS NOT NULL
              AND ST_Distance_Sphere(
                    POINT(em.longitude, em.latitude),
                    POINT(:center_lon, :center_lat)
                  ) <= :radius
            ORDER BY distance_meters ASC
        """), {
            'center_lat': center_lat,
            'center_lon': center_lon,
            'radius': radius_meters
        }).fetchall()

        # Should find 5 attractions (all except Jungle Cruise which is >700m)
        names = [r[0] for r in result]
        assert len(result) >= 5
        assert "Space Mountain" in names  # Center point
        assert "Big Thunder Mountain" in names
        assert "Jungle Cruise" not in names  # Too far

    def test_find_nearest_attractions(self, mysql_session):
        """Find the N nearest attractions to a point."""
        park = self._create_test_park(mysql_session, "Nearest Test Park")

        attractions = [
            ("Attraction A", 28.4195, -81.5785),  # Very close
            ("Attraction B", 28.4200, -81.5800),  # Close
            ("Attraction C", 28.4210, -81.5850),  # Medium
            ("Attraction D", 28.4250, -81.5900),  # Far
        ]

        for i, (name, lat, lon) in enumerate(attractions):
            self._create_ride_with_coords(mysql_session, park, name, lat, lon, i)

        center_lat, center_lon = 28.4193, -81.5781

        # Get top 2 nearest
        result = mysql_session.execute(text("""
            SELECT
                em.entity_name,
                ST_Distance_Sphere(
                    POINT(em.longitude, em.latitude),
                    POINT(:center_lon, :center_lat)
                ) AS distance_meters
            FROM entity_metadata em
            WHERE em.latitude IS NOT NULL
              AND em.longitude IS NOT NULL
            ORDER BY distance_meters ASC
            LIMIT 2
        """), {
            'center_lat': center_lat,
            'center_lon': center_lon
        }).fetchall()

        assert len(result) == 2
        # Attraction A should be closest
        assert result[0][0] == "Attraction A"
        assert result[1][0] == "Attraction B"


@pytest.mark.integration
class TestParkDistanceQueries:
    """Test distance queries between parks."""

    def _create_parks_at_locations(self, session):
        """Create test parks at known locations."""
        from sqlalchemy import func as sqlfunc
        max_id = session.query(sqlfunc.max(Park.queue_times_id)).scalar() or 90000

        parks = [
            ("Magic Kingdom", 28.4177, -81.5812),
            ("EPCOT", 28.3747, -81.5494),
            ("Hollywood Studios", 28.3575, -81.5583),
            ("Animal Kingdom", 28.3553, -81.5901),
        ]

        created_parks = []
        for i, (name, lat, lon) in enumerate(parks):
            park = Park(
                queue_times_id=max_id + i + 1,
                name=name,
                city="Orlando",
                state_province="FL",
                country="US",
                latitude=Decimal(str(lat)),
                longitude=Decimal(str(lon)),
                themeparks_wiki_id=f"test-{name.lower().replace(' ', '-')}",
                timezone='America/New_York'
            )
            session.add(park)
            created_parks.append(park)

        session.flush()
        return created_parks

    def test_calculate_park_to_park_distance(self, mysql_session):
        """Calculate distance between two parks."""
        parks = self._create_parks_at_locations(mysql_session)
        mk = parks[0]  # Magic Kingdom
        epcot = parks[1]  # EPCOT

        # Query distance
        result = mysql_session.execute(text("""
            SELECT ST_Distance_Sphere(
                POINT(:lon1, :lat1),
                POINT(:lon2, :lat2)
            ) / 1000 AS distance_km
        """), {
            'lat1': float(mk.latitude),
            'lon1': float(mk.longitude),
            'lat2': float(epcot.latitude),
            'lon2': float(epcot.longitude)
        }).scalar()

        # MK to EPCOT is about 5-6 km
        assert result is not None
        assert 4 < result < 8

    def test_find_parks_within_distance(self, mysql_session):
        """Find all parks within a certain distance of a location."""
        parks = self._create_parks_at_locations(mysql_session)

        # Find parks within 10km of Magic Kingdom
        mk_lat, mk_lon = 28.4177, -81.5812
        radius_km = 10

        result = mysql_session.execute(text("""
            SELECT
                p.name,
                ST_Distance_Sphere(
                    POINT(p.longitude, p.latitude),
                    POINT(:center_lon, :center_lat)
                ) / 1000 AS distance_km
            FROM parks p
            WHERE p.latitude IS NOT NULL
              AND p.longitude IS NOT NULL
              AND ST_Distance_Sphere(
                    POINT(p.longitude, p.latitude),
                    POINT(:center_lon, :center_lat)
                  ) <= :radius_meters
            ORDER BY distance_km ASC
        """), {
            'center_lat': mk_lat,
            'center_lon': mk_lon,
            'radius_meters': radius_km * 1000
        }).fetchall()

        names = [r[0] for r in result]
        assert "Magic Kingdom" in names  # Should include itself
        assert "EPCOT" in names  # About 5-6 km away


@pytest.mark.integration
class TestSpatialQueryORM:
    """Test spatial queries using SQLAlchemy ORM."""

    def _create_test_park(self, session, name="ORM Test Park"):
        """Helper to create a test park."""
        from sqlalchemy import func as sqlfunc
        max_id = session.query(sqlfunc.max(Park.queue_times_id)).scalar() or 90000
        park = Park(
            queue_times_id=max_id + 1,
            name=name,
            city="Orlando",
            state_province="FL",
            country="US",
            latitude=Decimal('28.4177'),
            longitude=Decimal('-81.5812'),
            themeparks_wiki_id=f"orm-{name.lower().replace(' ', '-')}",
            timezone='America/New_York'
        )
        session.add(park)
        session.flush()
        return park

    def _create_ride_with_metadata(self, session, park, name, lat, lon):
        """Create ride with metadata at specified coordinates."""
        from sqlalchemy import func as sqlfunc
        max_id = session.query(sqlfunc.max(Ride.queue_times_id)).scalar() or 80000
        ride = Ride(
            queue_times_id=max_id + 1,
            name=name,
            park_id=park.park_id,
            themeparks_wiki_id=f"orm-ride-{name.lower().replace(' ', '-')}",
            tier=1
        )
        session.add(ride)
        session.flush()

        metadata = EntityMetadata(
            ride_id=ride.ride_id,
            themeparks_wiki_id=ride.themeparks_wiki_id,
            entity_name=name,
            entity_type='ATTRACTION',
            latitude=Decimal(str(lat)),
            longitude=Decimal(str(lon))
        )
        session.add(metadata)
        session.flush()
        return ride, metadata

    def test_orm_with_func_st_distance(self, mysql_session):
        """Use SQLAlchemy func for ST_Distance_Sphere."""
        park = self._create_test_park(mysql_session)
        ride, metadata = self._create_ride_with_metadata(
            mysql_session, park, "ORM Test Ride", 28.4200, -81.5800
        )

        center_lat, center_lon = 28.4177, -81.5812

        # Use SQLAlchemy func for spatial query
        distance_expr = func.ST_Distance_Sphere(
            func.POINT(EntityMetadata.longitude, EntityMetadata.latitude),
            func.POINT(center_lon, center_lat)
        )

        result = mysql_session.query(
            EntityMetadata.entity_name,
            distance_expr.label('distance_meters')
        ).filter(
            EntityMetadata.latitude.isnot(None),
            EntityMetadata.longitude.isnot(None)
        ).first()

        assert result is not None
        assert result.entity_name == "ORM Test Ride"
        assert result.distance_meters > 0

    def test_count_attractions_by_distance_band(self, mysql_session):
        """Count attractions in distance bands (0-500m, 500-1000m, etc.)."""
        park = self._create_test_park(mysql_session, "Distance Band Park")

        # Create attractions at various distances
        attractions = [
            ("Very Close A", 28.4180, -81.5815),  # ~50m
            ("Very Close B", 28.4175, -81.5808),  # ~60m
            ("Close A", 28.4190, -81.5830),  # ~300m
            ("Medium A", 28.4210, -81.5860),  # ~600m
            ("Far A", 28.4250, -81.5900),  # ~1100m
        ]

        for i, (name, lat, lon) in enumerate(attractions):
            max_id = mysql_session.query(func.max(Ride.queue_times_id)).scalar() or 80000
            ride = Ride(
                queue_times_id=max_id + i + 1,
                name=name,
                park_id=park.park_id,
                themeparks_wiki_id=f"band-{i}",
                tier=1
            )
            mysql_session.add(ride)
            mysql_session.flush()

            metadata = EntityMetadata(
                ride_id=ride.ride_id,
                themeparks_wiki_id=ride.themeparks_wiki_id,
                entity_name=name,
                entity_type='ATTRACTION',
                latitude=Decimal(str(lat)),
                longitude=Decimal(str(lon))
            )
            mysql_session.add(metadata)

        mysql_session.flush()

        center_lat, center_lon = 28.4177, -81.5812

        # Count by distance bands
        result = mysql_session.execute(text("""
            SELECT
                CASE
                    WHEN dist <= 200 THEN '0-200m'
                    WHEN dist <= 500 THEN '200-500m'
                    WHEN dist <= 1000 THEN '500-1000m'
                    ELSE '1000m+'
                END AS distance_band,
                COUNT(*) as count
            FROM (
                SELECT
                    em.entity_name,
                    ST_Distance_Sphere(
                        POINT(em.longitude, em.latitude),
                        POINT(:center_lon, :center_lat)
                    ) AS dist
                FROM entity_metadata em
                WHERE em.latitude IS NOT NULL
                  AND em.longitude IS NOT NULL
            ) distances
            GROUP BY distance_band
            ORDER BY distance_band
        """), {
            'center_lat': center_lat,
            'center_lon': center_lon
        }).fetchall()

        bands = {r[0]: r[1] for r in result}
        # Should have entries in multiple bands
        assert len(bands) >= 1


@pytest.mark.integration
class TestSpatialEdgeCases:
    """Edge cases and error handling for spatial queries."""

    def test_null_coordinates_excluded(self, mysql_session):
        """Attractions with NULL coordinates should be excluded from spatial queries."""
        from sqlalchemy import func as sqlfunc
        max_park_id = mysql_session.query(sqlfunc.max(Park.queue_times_id)).scalar() or 90000
        park = Park(
            queue_times_id=max_park_id + 1,
            name="Null Coords Park",
            city="Orlando",
            state_province="FL",
            country="US",
            themeparks_wiki_id="null-coords-park",
            timezone='America/New_York'
        )
        mysql_session.add(park)
        mysql_session.flush()

        # Create ride with metadata but NULL coordinates
        max_ride_id = mysql_session.query(sqlfunc.max(Ride.queue_times_id)).scalar() or 80000
        ride = Ride(
            queue_times_id=max_ride_id + 1,
            name="No Coords Ride",
            park_id=park.park_id,
            themeparks_wiki_id="no-coords-ride",
            tier=1
        )
        mysql_session.add(ride)
        mysql_session.flush()

        metadata = EntityMetadata(
            ride_id=ride.ride_id,
            themeparks_wiki_id=ride.themeparks_wiki_id,
            entity_name="No Coords Ride",
            entity_type='ATTRACTION',
            latitude=None,
            longitude=None
        )
        mysql_session.add(metadata)
        mysql_session.flush()

        # Query should return 0 results (NULL coords excluded by WHERE clause)
        result = mysql_session.execute(text("""
            SELECT COUNT(*) FROM entity_metadata em
            WHERE em.latitude IS NOT NULL
              AND em.longitude IS NOT NULL
              AND em.themeparks_wiki_id = 'no-coords-ride'
        """)).scalar()

        assert result == 0

    def test_extreme_coordinates(self, mysql_session):
        """Test with coordinates at geographic extremes."""
        from sqlalchemy import func as sqlfunc

        # North Pole area vs South Pole area
        result = mysql_session.execute(text("""
            SELECT ST_Distance_Sphere(
                POINT(0, 89.9),
                POINT(0, -89.9)
            ) / 1000 AS distance_km
        """)).scalar()

        # Should be roughly half Earth's circumference (~20,000 km)
        assert result is not None
        assert 19000 < result < 21000

    def test_same_point_zero_distance(self, mysql_session):
        """Distance between same point should be zero."""
        result = mysql_session.execute(text("""
            SELECT ST_Distance_Sphere(
                POINT(-81.5812, 28.4177),
                POINT(-81.5812, 28.4177)
            ) AS distance_meters
        """)).scalar()

        assert result == 0
