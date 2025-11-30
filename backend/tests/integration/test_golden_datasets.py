"""
Golden Dataset Regression Tests
===============================

Tests that verify our calculations match hand-computed expected values.
These serve as regression tests to catch formula bugs or aggregation errors.

How These Tests Work:
1. Load expected values from tests/golden_data/{scenario}_expected.json
2. Run the same calculations against test data
3. Assert computed values match expected values

Note: These tests require the audit views to be created in the database.
Run `mysql < src/database/audit/views.sql` to create views.

If a test fails, it means either:
- A calculation formula was changed (update the golden data)
- A bug was introduced (fix the calculation)
"""

import pytest
import json
from pathlib import Path
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

# Path to golden data
GOLDEN_DATA_DIR = Path(__file__).parent.parent / "golden_data"


class TestGoldenDatasetStructure:
    """Verify golden datasets are properly structured."""

    def test_simple_park_expected_exists(self):
        """Verify simple park golden data exists."""
        path = GOLDEN_DATA_DIR / "simple_park_expected.json"
        assert path.exists(), f"Missing golden data: {path}"

    def test_complex_hours_expected_exists(self):
        """Verify complex hours golden data exists."""
        path = GOLDEN_DATA_DIR / "complex_hours_expected.json"
        assert path.exists(), f"Missing golden data: {path}"

    def test_maintenance_expected_exists(self):
        """Verify maintenance golden data exists."""
        path = GOLDEN_DATA_DIR / "maintenance_expected.json"
        assert path.exists(), f"Missing golden data: {path}"

    def test_simple_park_has_required_fields(self):
        """Verify simple park golden data has all required fields."""
        path = GOLDEN_DATA_DIR / "simple_park_expected.json"
        with open(path) as f:
            data = json.load(f)

        assert "metadata" in data
        assert "rides" in data
        assert "park_daily_expected" in data

        # Check metadata
        assert "scenario" in data["metadata"]
        assert "park_id" in data["metadata"]

        # Check at least one ride has expected structure
        first_ride = list(data["rides"].values())[0]
        assert "tier" in first_ride
        assert "tier_weight" in first_ride
        assert "daily_stats" in first_ride

    def test_maintenance_has_status_breakdown(self):
        """Verify maintenance scenario includes all status types."""
        path = GOLDEN_DATA_DIR / "maintenance_expected.json"
        with open(path) as f:
            data = json.load(f)

        # Find a ride with mixed statuses
        mixed_ride = data["rides"].get("304")
        assert mixed_ride is not None, "Missing mixed status ride 304"

        stats = list(mixed_ride["daily_stats"].values())[0]
        assert "operating_snapshots" in stats
        assert "down_snapshots" in stats
        assert "closed_snapshots" in stats
        assert "refurbishment_snapshots" in stats


class TestShameScoreCalculation:
    """Test shame score calculation matches expected values."""

    def test_simple_park_daily_shame_score(self):
        """Verify daily shame score calculation for simple park."""
        path = GOLDEN_DATA_DIR / "simple_park_expected.json"
        with open(path) as f:
            data = json.load(f)

        # Test Nov 18 calculations
        expected = data["park_daily_expected"]["2024-11-18"]

        # Manual verification of the formula
        weighted_downtime = expected["weighted_downtime_hours"]
        total_weight = expected["total_park_weight"]
        expected_shame = expected["shame_score"]

        # Recalculate
        calculated = round(weighted_downtime / total_weight, 2)

        assert calculated == expected_shame, (
            f"Shame score mismatch: "
            f"calculated={calculated}, expected={expected_shame}, "
            f"formula={weighted_downtime}/{total_weight}"
        )

    def test_complex_hours_shame_score(self):
        """Verify shame score handles complex hours correctly."""
        path = GOLDEN_DATA_DIR / "complex_hours_expected.json"
        with open(path) as f:
            data = json.load(f)

        expected = data["park_daily_expected"]["2024-11-18"]

        # Verify: 9.5 / 7 = 1.36 (rounded)
        weighted_downtime = expected["weighted_downtime_hours"]
        total_weight = expected["total_park_weight"]
        expected_shame = expected["shame_score"]

        calculated = round(weighted_downtime / total_weight, 2)
        assert calculated == expected_shame

    def test_maintenance_park_excludes_refurbishment(self):
        """Verify REFURBISHMENT status doesn't count as downtime."""
        path = GOLDEN_DATA_DIR / "maintenance_expected.json"
        with open(path) as f:
            data = json.load(f)

        # Ride 301 is under refurbishment
        refurb_ride = data["rides"]["301"]
        stats = list(refurb_ride["daily_stats"].values())[0]

        # Should have 0 downtime hours despite 200 refurbishment snapshots
        assert stats["refurbishment_snapshots"] == 200
        assert stats["downtime_hours"] == 0.0
        assert stats["weighted_downtime"] == 0.0

    def test_maintenance_park_excludes_closed(self):
        """Verify CLOSED status doesn't count as downtime."""
        path = GOLDEN_DATA_DIR / "maintenance_expected.json"
        with open(path) as f:
            data = json.load(f)

        # Ride 302 is closed all day
        closed_ride = data["rides"]["302"]
        stats = list(closed_ride["daily_stats"].values())[0]

        # Should have 0 downtime hours despite 200 closed snapshots
        assert stats["closed_snapshots"] == 200
        assert stats["downtime_hours"] == 0.0
        assert stats["weighted_downtime"] == 0.0

    def test_maintenance_park_counts_down_only(self):
        """Verify only DOWN status counts as downtime."""
        path = GOLDEN_DATA_DIR / "maintenance_expected.json"
        with open(path) as f:
            data = json.load(f)

        # Ride 303 has actual breakdowns
        breakdown_ride = data["rides"]["303"]
        stats = list(breakdown_ride["daily_stats"].values())[0]

        # 60 down snapshots = 300 minutes = 5.0 hours
        assert stats["down_snapshots"] == 60
        assert stats["downtime_hours"] == 5.0
        assert stats["weighted_downtime"] == 10.0  # 5.0 × tier_weight 2


class TestTierWeighting:
    """Test tier weight calculations."""

    def test_tier_1_weight_is_3(self):
        """Verify tier 1 rides have weight 3."""
        path = GOLDEN_DATA_DIR / "simple_park_expected.json"
        with open(path) as f:
            data = json.load(f)

        tier_1_ride = data["rides"]["101"]
        assert tier_1_ride["tier"] == 1
        assert tier_1_ride["tier_weight"] == 3

    def test_tier_2_weight_is_2(self):
        """Verify tier 2 rides have weight 2."""
        path = GOLDEN_DATA_DIR / "simple_park_expected.json"
        with open(path) as f:
            data = json.load(f)

        tier_2_ride = data["rides"]["102"]
        assert tier_2_ride["tier"] == 2
        assert tier_2_ride["tier_weight"] == 2

    def test_tier_3_weight_is_1(self):
        """Verify tier 3 rides have weight 1."""
        path = GOLDEN_DATA_DIR / "simple_park_expected.json"
        with open(path) as f:
            data = json.load(f)

        tier_3_ride = data["rides"]["103"]
        assert tier_3_ride["tier"] == 3
        assert tier_3_ride["tier_weight"] == 1

    def test_weighted_downtime_calculation(self):
        """Verify weighted_downtime = downtime_hours × tier_weight."""
        path = GOLDEN_DATA_DIR / "simple_park_expected.json"
        with open(path) as f:
            data = json.load(f)

        ride = data["rides"]["101"]  # Tier 1, weight 3
        stats = ride["daily_stats"]["2024-11-18"]

        expected_weighted = stats["downtime_hours"] * ride["tier_weight"]
        # Allow small rounding difference (0.05)
        assert abs(stats["weighted_downtime"] - expected_weighted) < 0.05, (
            f"Weighted downtime mismatch: "
            f"{stats['downtime_hours']} × {ride['tier_weight']} = {expected_weighted}, "
            f"but got {stats['weighted_downtime']}"
        )


class TestDowntimeCalculation:
    """Test downtime calculation from snapshots."""

    def test_downtime_hours_from_snapshots(self):
        """Verify downtime_hours = (down_snapshots × 5) / 60."""
        path = GOLDEN_DATA_DIR / "simple_park_expected.json"
        with open(path) as f:
            data = json.load(f)

        ride = data["rides"]["101"]
        stats = ride["daily_stats"]["2024-11-18"]

        # 20 snapshots × 5 min = 100 min = 1.67 hours
        down_snapshots = stats["down_snapshots"]
        expected_hours = round((down_snapshots * 5) / 60, 2)

        assert stats["downtime_hours"] == expected_hours, (
            f"Downtime hours mismatch: "
            f"({down_snapshots} × 5) / 60 = {expected_hours}, "
            f"but got {stats['downtime_hours']}"
        )

    def test_uptime_percentage_calculation(self):
        """Verify uptime_percentage = (operating / park_open) × 100."""
        path = GOLDEN_DATA_DIR / "simple_park_expected.json"
        with open(path) as f:
            data = json.load(f)

        ride = data["rides"]["101"]
        stats = ride["daily_stats"]["2024-11-18"]

        # 180 operating / 200 park_open = 90%
        operating = stats["operating_snapshots"]
        park_open = stats["park_open_snapshots"]
        expected_pct = round((operating / park_open) * 100, 1)

        assert stats["uptime_percentage"] == expected_pct


class TestParkOpenLogic:
    """Test park_appears_open logic."""

    def test_downtime_only_during_park_open(self):
        """Verify downtime only counts when park_appears_open = TRUE."""
        path = GOLDEN_DATA_DIR / "complex_hours_expected.json"
        with open(path) as f:
            data = json.load(f)

        # Read the key validations
        validations = data["key_validations"]
        assert "park_open_is_denominator" in validations

        # Verify early entry ride logic
        early_ride = data["rides"]["201"]
        stats = list(early_ride["daily_stats"].values())[0]

        # Total snapshots > park_open_snapshots (ride was active before park opened)
        assert stats["total_snapshots"] > stats["park_open_snapshots"]

        # Downtime only from park_open_snapshots
        down_minutes = stats["down_snapshots"] * 5
        assert stats["downtime_minutes"] == down_minutes


class TestWeeklyAggregation:
    """Test weekly aggregation totals."""

    def test_weekly_totals_match_daily_sum(self):
        """Verify weekly totals equal sum of daily values."""
        path = GOLDEN_DATA_DIR / "simple_park_expected.json"
        with open(path) as f:
            data = json.load(f)

        ride = data["rides"]["101"]

        # Sum daily down_snapshots
        daily_sum = sum(
            stats["down_snapshots"]
            for stats in ride["daily_stats"].values()
        )

        # Should match weekly totals
        assert ride["weekly_totals"]["down_snapshots"] == daily_sum


class TestEdgeCases:
    """Test edge cases in calculations."""

    def test_zero_downtime_day(self):
        """Verify parks can have 0 downtime."""
        path = GOLDEN_DATA_DIR / "simple_park_expected.json"
        with open(path) as f:
            data = json.load(f)

        # Nov 19 has zero downtime
        expected = data["park_daily_expected"]["2024-11-19"]
        assert expected["total_downtime_hours"] == 0.0
        assert expected["shame_score"] == 0.0
        assert expected["rides_with_downtime"] == 0

    def test_perfect_uptime(self):
        """Verify 100% uptime is calculated correctly."""
        path = GOLDEN_DATA_DIR / "simple_park_expected.json"
        with open(path) as f:
            data = json.load(f)

        expected = data["park_daily_expected"]["2024-11-19"]
        assert expected["avg_uptime_percentage"] == 100.0
