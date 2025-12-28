"""
Ride Status From Hourly Metrics Tests
======================================

TDD tests for determining ride status from hourly aggregated metrics.

Bug Context:
- Alice in Wonderland at Disneyland was DOWN for 13+ hours on 2025-12-26
- The ride detail page showed "CLOSED" for every hour instead of "DOWN"
- Root cause: _status_from_uptime() only looked at uptime_percentage
- When uptime=0%, it returned "CLOSED" even when down_snapshots > closed_snapshots

Fix:
- Use predominant status logic: compare down_snapshots vs closed_snapshots
- If down_snapshots > closed_snapshots, status should be "DOWN"
- If closed_snapshots > down_snapshots, status should be "CLOSED"

Related Files:
- src/api/routes/rides.py: _status_from_uptime() and _get_ride_timeseries()
"""

import pytest


class TestStatusFromHourlyMetrics:
    """
    Test that ride status is determined by predominant snapshot status,
    not just derived from uptime percentage.
    """

    def test_status_is_down_when_down_snapshots_predominate(self):
        """
        When down_snapshots > closed_snapshots, status should be DOWN.

        Example: Hour with 6 snapshots, all showing DOWN
        - operating_snapshots: 0
        - down_snapshots: 6
        - closed_snapshots: 0 (computed as total - operating - down)

        Expected: "DOWN" (not "CLOSED")
        """
        from api.routes.rides import _status_from_hourly_metrics

        status = _status_from_hourly_metrics(
            operating_snapshots=0,
            down_snapshots=6,
            snapshot_count=6
        )

        assert status == "DOWN", (
            f"Expected 'DOWN' when down_snapshots=6, closed_snapshots=0, "
            f"but got '{status}'"
        )

    def test_status_is_closed_when_closed_snapshots_predominate(self):
        """
        When closed_snapshots > down_snapshots, status should be CLOSED.

        Example: Hour during park closure with 6 snapshots, all showing CLOSED
        - operating_snapshots: 0
        - down_snapshots: 0
        - closed_snapshots: 6

        Expected: "CLOSED"
        """
        from api.routes.rides import _status_from_hourly_metrics

        status = _status_from_hourly_metrics(
            operating_snapshots=0,
            down_snapshots=0,
            snapshot_count=6
        )

        assert status == "CLOSED", (
            f"Expected 'CLOSED' when down_snapshots=0, closed_snapshots=6, "
            f"but got '{status}'"
        )

    def test_status_is_operating_when_operating_snapshots_predominate(self):
        """
        When operating_snapshots > down + closed, status should be OPERATING.

        Example: Hour with 6 snapshots, 4 operating, 1 down, 1 closed
        - operating_snapshots: 4
        - down_snapshots: 1
        - closed_snapshots: 1

        Expected: "OPERATING"
        """
        from api.routes.rides import _status_from_hourly_metrics

        status = _status_from_hourly_metrics(
            operating_snapshots=4,
            down_snapshots=1,
            snapshot_count=6
        )

        assert status == "OPERATING", (
            f"Expected 'OPERATING' when operating_snapshots=4, "
            f"but got '{status}'"
        )

    def test_status_down_beats_closed_when_equal_operating(self):
        """
        When down_snapshots == closed_snapshots and both > operating,
        DOWN should win (more concerning status).

        Example: Hour with 6 snapshots, 0 operating, 3 down, 3 closed

        Expected: "DOWN" (tie-breaker: DOWN is more severe)
        """
        from api.routes.rides import _status_from_hourly_metrics

        status = _status_from_hourly_metrics(
            operating_snapshots=0,
            down_snapshots=3,
            snapshot_count=6
        )

        assert status == "DOWN", (
            f"Expected 'DOWN' as tie-breaker when down=closed=3, "
            f"but got '{status}'"
        )

    def test_status_with_partial_down(self):
        """
        When there's a mix of statuses but DOWN predominates among non-operating.

        Example: Hour with 6 snapshots
        - operating_snapshots: 1
        - down_snapshots: 4
        - closed_snapshots: 1

        Expected: "DOWN" (4 > 1 operating, 4 > 1 closed)
        """
        from api.routes.rides import _status_from_hourly_metrics

        status = _status_from_hourly_metrics(
            operating_snapshots=1,
            down_snapshots=4,
            snapshot_count=6
        )

        assert status == "DOWN", (
            f"Expected 'DOWN' when down_snapshots=4 predominates, "
            f"but got '{status}'"
        )

    def test_status_handles_none_values(self):
        """
        Function should handle None values gracefully (treat as 0).
        """
        from api.routes.rides import _status_from_hourly_metrics

        # All None except total
        status = _status_from_hourly_metrics(
            operating_snapshots=None,
            down_snapshots=None,
            snapshot_count=6
        )

        assert status == "CLOSED", (
            f"Expected 'CLOSED' when no data, but got '{status}'"
        )

    def test_status_handles_zero_snapshots(self):
        """
        When snapshot_count is 0, status should be CLOSED (no data).
        """
        from api.routes.rides import _status_from_hourly_metrics

        status = _status_from_hourly_metrics(
            operating_snapshots=0,
            down_snapshots=0,
            snapshot_count=0
        )

        assert status == "CLOSED", (
            f"Expected 'CLOSED' when no snapshots, but got '{status}'"
        )


class TestStatusFromUptimeBackwardsCompatibility:
    """
    Ensure the old _status_from_uptime function still works for cases
    where we don't have snapshot counts (e.g., legacy data).
    """

    def test_uptime_100_returns_operating(self):
        """100% uptime should return OPERATING."""
        from api.routes.rides import _status_from_uptime

        assert _status_from_uptime(100.0) == "OPERATING"

    def test_uptime_50_returns_operating(self):
        """50% uptime (threshold) should return OPERATING."""
        from api.routes.rides import _status_from_uptime

        assert _status_from_uptime(50.0) == "OPERATING"

    def test_uptime_49_returns_down(self):
        """49% uptime (below threshold) should return DOWN."""
        from api.routes.rides import _status_from_uptime

        assert _status_from_uptime(49.0) == "DOWN"

    def test_uptime_1_returns_down(self):
        """1% uptime should return DOWN (ride operated briefly)."""
        from api.routes.rides import _status_from_uptime

        assert _status_from_uptime(1.0) == "DOWN"

    def test_uptime_none_returns_closed(self):
        """None uptime (no data) should return CLOSED."""
        from api.routes.rides import _status_from_uptime

        assert _status_from_uptime(None) == "CLOSED"
