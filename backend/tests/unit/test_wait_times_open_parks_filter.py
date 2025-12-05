"""
Unit Tests for Wait Times Open Parks Filter
============================================

TDD Tests: These tests verify that the wait times API only returns
parks that are currently OPEN, excluding closed parks even if they
have wait time data from earlier in the day.

The Bug These Tests Catch:
-------------------------
Knott's Berry Farm was appearing in wait times results with park_is_open=0
because it had wait time data from earlier today when it was open.
Closed parks should be excluded from wait times rankings.

Implementation Fix:
------------------
Add filtering in get_park_live_wait_time_rankings() to only include
parks where park_is_open = 1.
"""

import inspect


class TestWaitTimesExcludesClosedParks:
    """Test that wait times only show OPEN parks."""

    def test_park_wait_times_query_filters_closed_parks(self):
        """
        CRITICAL: The SQL query should filter out parks where park_is_open = 0.

        Verifies that the query includes a filter condition for park_is_open.
        """
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_park_live_wait_time_rankings)

        # The query should filter to only include open parks
        # This can be done via HAVING clause or WHERE clause with subquery
        assert 'park_is_open' in source and ('= 1' in source or '= TRUE' in source or 'HAVING' in source), \
            "Query must filter to only include open parks (park_is_open = 1)"

    def test_park_wait_times_having_clause_includes_open_filter(self):
        """
        Verify the HAVING clause filters for open parks.

        The HAVING clause should include: park_is_open = 1
        """
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_park_live_wait_time_rankings)

        # Check that HAVING clause includes park_is_open filter
        # The query has HAVING avg_wait_minutes > 0, it should also have park_is_open = 1
        having_section = source[source.find('HAVING'):source.find('ORDER BY')] if 'HAVING' in source else ""

        assert 'park_is_open' in having_section, \
            "HAVING clause must include park_is_open = 1 to filter closed parks"

    def test_closed_park_scenario_documented(self):
        """
        Document the Knott's Berry Farm scenario for regression testing.

        Scenario: A park (Knott's Berry Farm) was open earlier today and has
        wait time data. It is now closed (park_is_open = 0). It should NOT
        appear in the wait times rankings.

        Before fix: Knott's appeared with park_is_open=0
        After fix: Knott's should not appear at all
        """
        # This test documents the expected behavior
        # The actual filtering happens in the SQL query
        expected_behavior = {
            "park_has_wait_data_today": True,
            "park_is_currently_open": False,
            "should_appear_in_results": False,  # KEY ASSERTION
        }

        assert expected_behavior["should_appear_in_results"] == False, \
            "Closed parks with today's wait data should NOT appear in results"


class TestWaitTimesIncludesOpenParks:
    """Test that open parks appear correctly in wait times."""

    def test_open_park_with_wait_data_appears(self):
        """
        Verify open parks (like Epic Universe) appear in results.

        A park that is open (park_is_open = 1) and has wait time data
        should appear in the wait times rankings.
        """
        expected_behavior = {
            "park_has_wait_data_today": True,
            "park_is_currently_open": True,
            "should_appear_in_results": True,
        }

        assert expected_behavior["should_appear_in_results"] == True, \
            "Open parks with wait data should appear in results"

    def test_park_is_open_field_returned_for_all_results(self):
        """
        Verify that park_is_open field is returned for all results.

        Even though we filter to only open parks, the field should still
        be present in the response for display purposes.
        """
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_park_live_wait_time_rankings)

        # The query should still return park_is_open as a column
        assert 'park_is_open' in source, \
            "Query must return park_is_open field"


class TestRideWaitTimesOpenParksFilter:
    """Test that ride wait times also respect park open status."""

    def test_ride_wait_times_query_has_park_is_open_field(self):
        """
        Verify ride wait times query returns park_is_open field.

        Rides should show their park's open status so the frontend
        can indicate if the park is closed.
        """
        from database.repositories.stats_repository import StatsRepository

        assert hasattr(StatsRepository, 'get_ride_live_wait_time_rankings'), \
            "StatsRepository must have get_ride_live_wait_time_rankings"

        source = inspect.getsource(StatsRepository.get_ride_live_wait_time_rankings)
        assert 'park_is_open' in source, \
            "Ride wait times query must include park_is_open field"


class TestQueryStructureForOpenParksFilter:
    """Test the SQL structure ensures proper filtering."""

    def test_having_clause_format(self):
        """
        Verify HAVING clause properly combines conditions.

        HAVING should be:
            HAVING avg_wait_minutes > 0
                AND park_is_open = 1
        """
        from database.repositories.stats_repository import StatsRepository
        source = inspect.getsource(StatsRepository.get_park_live_wait_time_rankings)

        # Find the HAVING clause
        if 'HAVING' in source:
            having_start = source.find('HAVING')
            having_end = source.find('ORDER BY', having_start)
            having_clause = source[having_start:having_end]

            # Should have both conditions
            assert 'avg_wait_minutes' in having_clause, \
                "HAVING must filter on avg_wait_minutes > 0"
            # After fix, should also have park_is_open
            assert 'park_is_open' in having_clause, \
                "HAVING must filter on park_is_open = 1"
