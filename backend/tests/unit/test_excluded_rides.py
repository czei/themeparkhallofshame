"""
Excluded Rides API Tests
========================

TDD Tests for Phase 2: Frontend Excluded Rides Display.

CONTEXT: Parks use a 7-day hybrid denominator for shame score calculations.
Rides that haven't operated in 7+ days are EXCLUDED from the denominator.
Users need visibility into which rides are excluded and why.

API Endpoint: GET /api/parks/<id>/rides
Returns:
{
    "park_id": 139,
    "park_name": "Knott's Berry Farm",
    "effective_park_weight": 45.0,
    "total_roster_weight": 66.7,
    "rides": {
        "active": [...],    # Rides included in shame score
        "excluded": [...]   # Rides excluded (7+ days without operation)
    }
}
"""

import pytest
from pathlib import Path


class TestExcludedRidesQuery:
    """Tests for the excluded rides database query."""

    def test_stats_repository_has_get_excluded_rides_method(self):
        """
        FAILING TEST: StatsRepository must have get_excluded_rides() method.

        This method returns rides that haven't operated in 7 days
        (excluded from shame score denominator).
        """
        from database.repositories.stats_repository import StatsRepository

        assert hasattr(StatsRepository, 'get_excluded_rides'), \
            "StatsRepository must have get_excluded_rides() method"

    def test_get_excluded_rides_method_signature(self):
        """
        get_excluded_rides should accept park_id parameter.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository

        if not hasattr(StatsRepository, 'get_excluded_rides'):
            pytest.skip("get_excluded_rides not yet implemented")

        sig = inspect.signature(StatsRepository.get_excluded_rides)
        params = list(sig.parameters.keys())

        assert 'park_id' in params, \
            "get_excluded_rides should accept park_id parameter"

    def test_get_excluded_rides_uses_7_day_window(self):
        """
        get_excluded_rides should use 7-day window for exclusion criteria.

        A ride is excluded if:
        - last_operated_at IS NULL, OR
        - last_operated_at < UTC_TIMESTAMP() - INTERVAL 7 DAY
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository

        if not hasattr(StatsRepository, 'get_excluded_rides'):
            pytest.skip("get_excluded_rides not yet implemented")

        source = inspect.getsource(StatsRepository.get_excluded_rides)

        # Should use 7-day window for exclusion (raw SQL or ORM timedelta)
        uses_7_day = (
            '7 DAY' in source or
            'INTERVAL 7' in source or
            'timedelta(days=7)' in source or
            'seven_days_ago' in source or
            'last_operated_at' in source
        )

        assert uses_7_day, \
            "get_excluded_rides should use 7-day window for exclusion criteria"


class TestExcludedRidesApiEndpoint:
    """Tests for the /parks/<id>/rides API endpoint."""

    def test_parks_routes_has_rides_endpoint(self):
        """
        FAILING TEST: parks.py must have /parks/<id>/rides endpoint.
        """
        parks_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "parks.py"
        source = parks_path.read_text()

        # Check for route definition - matches @parks_bp.route('/parks/<int:park_id>/rides', ...)
        has_rides_endpoint = (
            "/rides'" in source or
            '/rides"' in source or
            "/rides'," in source
        )

        assert has_rides_endpoint, \
            "parks.py should have /parks/<id>/rides endpoint for excluded rides data"


class TestExcludedRidesResponseFormat:
    """Tests for the response format of excluded rides data."""

    def test_get_excluded_rides_returns_required_fields(self):
        """
        get_excluded_rides should return rides with required fields.

        Required fields per ride:
        - ride_id
        - ride_name
        - tier
        - tier_weight
        - last_operated_at (nullable)
        - days_since_operation (nullable)
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository

        if not hasattr(StatsRepository, 'get_excluded_rides'):
            pytest.skip("get_excluded_rides not yet implemented")

        source = inspect.getsource(StatsRepository.get_excluded_rides)

        # Check for required field selections (raw SQL or ORM patterns)
        has_ride_name = 'ride_name' in source or 'r.name' in source or 'Ride.name' in source or '.name' in source
        has_tier = 'tier' in source or 'Ride.tier' in source or '.tier' in source
        has_last_operated = 'last_operated_at' in source
        has_days_since = 'days_since' in source or 'DATEDIFF' in source or 'timedelta' in source or 'days_since_operation' in source

        assert has_ride_name, "Query should select ride_name"
        assert has_tier, "Query should select tier"
        assert has_last_operated, "Query should select last_operated_at"
        assert has_days_since, "Query should calculate days_since_operation"


class TestActiveRidesQuery:
    """Tests for the active rides query (included in shame score)."""

    def test_stats_repository_has_get_active_rides_method(self):
        """
        StatsRepository should have get_active_rides() for rides included in shame score.
        """
        from database.repositories.stats_repository import StatsRepository

        assert hasattr(StatsRepository, 'get_active_rides'), \
            "StatsRepository should have get_active_rides() method"

    def test_get_active_rides_uses_7_day_window(self):
        """
        get_active_rides should only return rides that operated in last 7 days.
        """
        import inspect
        from database.repositories.stats_repository import StatsRepository

        if not hasattr(StatsRepository, 'get_active_rides'):
            pytest.skip("get_active_rides not yet implemented")

        source = inspect.getsource(StatsRepository.get_active_rides)

        # Should filter to rides that operated in last 7 days (raw SQL or ORM timedelta)
        uses_7_day = (
            '7 DAY' in source or
            'INTERVAL 7' in source or
            'timedelta(days=7)' in source or
            'seven_days_ago' in source or
            'last_operated_at >=' in source
        )

        assert uses_7_day, \
            "get_active_rides should filter to rides that operated in last 7 days"


class TestParkWeightCalculations:
    """Tests for park weight calculations in rides response."""

    def test_response_includes_effective_park_weight(self):
        """
        API response should include effective_park_weight (7-day filtered weight).
        """
        parks_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "parks.py"
        source = parks_path.read_text()

        # Check for effective weight in response
        has_effective_weight = (
            'effective_park_weight' in source or
            'effective_weight' in source
        )

        # This test may initially fail if endpoint doesn't exist
        if "/rides'" not in source and '/rides"' not in source:
            pytest.skip("Rides endpoint not yet implemented")

        assert has_effective_weight, \
            "Rides endpoint should return effective_park_weight"

    def test_response_includes_total_roster_weight(self):
        """
        API response should include total_roster_weight (all rides weight).
        """
        parks_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "parks.py"
        source = parks_path.read_text()

        # Check for total roster weight in response
        has_total_weight = (
            'total_roster_weight' in source or
            'total_weight' in source
        )

        # This test may initially fail if endpoint doesn't exist
        if "/rides'" not in source and '/rides"' not in source:
            pytest.skip("Rides endpoint not yet implemented")

        assert has_total_weight, \
            "Rides endpoint should return total_roster_weight"


class TestDetailsModalExcludedRides:
    """Tests for excluded rides in the park details modal."""

    def test_details_endpoint_includes_excluded_rides_count(self):
        """
        The /parks/<id>/details endpoint should include excluded_rides_count
        so the modal can show how many rides are excluded.
        """
        parks_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "parks.py"
        source = parks_path.read_text()

        # Look for excluded_rides in details response
        has_excluded = (
            'excluded_rides' in source or
            'excluded_count' in source
        )

        # Note: This test documents expected behavior for the details modal
        # The actual implementation may add this to shame_breakdown
        assert has_excluded, \
            "Details endpoint should include excluded_rides info for the modal"

    def test_details_endpoint_includes_effective_park_weight(self):
        """
        The /parks/<id>/details endpoint should include effective_park_weight
        at the top level so the modal can show the 7-day filtered park weight.

        effective_park_weight = sum of tier weights for rides that operated
        in the last 7 days (the denominator for shame score calculation).
        """
        parks_path = Path(__file__).parent.parent.parent / "src" / "api" / "routes" / "parks.py"
        source = parks_path.read_text()

        # Look for effective_park_weight in response building section
        has_effective_weight = 'effective_park_weight' in source

        assert has_effective_weight, \
            "Details endpoint should include effective_park_weight for the modal"
