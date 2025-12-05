"""
Tests for chart_data numeric type validation.

Problem: MariaDB ROUND() returns Decimal which Flask jsonify converts to strings.
This breaks Chart.js rendering when it receives "0.9" instead of 0.9.

These tests ensure chart_data values are proper floats before JSON serialization.
"""
import pytest
from decimal import Decimal
from unittest.mock import MagicMock, patch
from datetime import date, datetime, timezone, timedelta


class TestChartDataNumericTypes:
    """Tests ensuring chart_data returns proper numeric types (float), not strings or Decimals."""

    def test_get_recent_snapshots_returns_floats_not_decimals(self):
        """
        get_recent_snapshots() should return float values, not Decimal.

        Decimal types serialize to strings in JSON, breaking Chart.js.
        This test verifies the conversion is done at the source.
        """
        from database.calculators.shame_score import ShameScoreCalculator

        # Mock a database result that returns Decimals (as MariaDB does)
        mock_db = MagicMock()
        mock_row1 = MagicMock()
        mock_row1._mapping = {
            'recorded_at': datetime.now(timezone.utc),
            'time_label': '10:05',
            'shame_score': Decimal('1.5')  # MariaDB returns Decimal
        }
        mock_row2 = MagicMock()
        mock_row2._mapping = {
            'recorded_at': datetime.now(timezone.utc),
            'time_label': '10:10',
            'shame_score': None  # Null values should stay None
        }
        mock_db.execute.return_value = [mock_row1, mock_row2]

        calc = ShameScoreCalculator(mock_db)
        result = calc.get_recent_snapshots(park_id=139, minutes=60)

        # Verify data array contains floats or None, never Decimal
        assert result is not None, "Should return a result"
        assert 'data' in result, "Should have data array"

        for value in result['data']:
            if value is not None:
                assert isinstance(value, float), (
                    f"Chart data values must be float, not {type(value).__name__}. "
                    "Decimal types serialize to strings in JSON, breaking Chart.js."
                )

    def test_get_single_park_hourly_returns_floats_not_decimals(self):
        """
        FAILING TEST: get_single_park_hourly() should return float values.

        Currently, it returns raw Decimal values from get_hourly_breakdown(),
        which serialize to strings like "0.9" instead of 0.9.
        """
        from database.queries.charts.park_shame_history import ParkShameHistoryQuery
        from database.calculators.shame_score import ShameScoreCalculator

        # Mock the connection and ShameScoreCalculator
        mock_conn = MagicMock()

        # Create mock hourly breakdown data with Decimals (as returned from DB)
        mock_hourly_data = [
            {'hour': 10, 'shame_score': Decimal('1.5'), 'total_rides': 5, 'down_minutes': 30},
            {'hour': 11, 'shame_score': Decimal('2.3'), 'total_rides': 5, 'down_minutes': 45},
            {'hour': 12, 'shame_score': None, 'total_rides': 0, 'down_minutes': 0},  # No data
        ]

        with patch.object(ShameScoreCalculator, 'get_hourly_breakdown', return_value=mock_hourly_data):
            with patch.object(ShameScoreCalculator, 'get_average', return_value=Decimal('1.9')):
                query = ParkShameHistoryQuery(mock_conn)
                result = query.get_single_park_hourly(park_id=139, target_date=date.today())

        # Verify data array contains floats or None, never Decimal
        assert result is not None, "Should return a result"
        assert 'data' in result, "Should have data array"

        for value in result['data']:
            if value is not None:
                assert isinstance(value, float), (
                    f"Chart data values must be float, not {type(value).__name__}. "
                    "Decimal types serialize to strings in JSON, breaking Chart.js."
                )

        # Also verify average is float
        assert isinstance(result['average'], float), (
            f"Average must be float, not {type(result['average']).__name__}"
        )

    def test_chart_data_handles_null_values_gracefully(self):
        """
        Chart data should handle NULL database values as None (not 0 or error).

        NULL indicates no data for that time period (park closed, no snapshots).
        """
        from database.queries.charts.park_shame_history import ParkShameHistoryQuery
        from database.calculators.shame_score import ShameScoreCalculator

        mock_conn = MagicMock()

        # All null values (park was closed all day)
        mock_hourly_data = [
            {'hour': 10, 'shame_score': None, 'total_rides': 0, 'down_minutes': 0},
            {'hour': 11, 'shame_score': None, 'total_rides': 0, 'down_minutes': 0},
        ]

        with patch.object(ShameScoreCalculator, 'get_hourly_breakdown', return_value=mock_hourly_data):
            with patch.object(ShameScoreCalculator, 'get_average', return_value=None):
                query = ParkShameHistoryQuery(mock_conn)
                result = query.get_single_park_hourly(park_id=139, target_date=date.today())

        # Verify nulls are preserved as None (not converted to 0)
        assert result is not None, "Should return a result even with null data"
        assert result['data'][4] is None, "Hour 10 (index 4) should be None"
        assert result['data'][5] is None, "Hour 11 (index 5) should be None"

        # Average should be 0.0 when no data (safe fallback)
        assert result['average'] == 0.0, "Average should default to 0.0 when null"


class TestChartDataAPIResponse:
    """Integration-style tests for chart_data in API responses."""

    def test_json_serialized_chart_data_contains_numbers(self):
        """
        Verify that after JSON serialization, chart_data values are numbers not strings.

        This documents the bug where Decimal("0.9") becomes '"0.9"' (string) in JSON.
        The fix is to always convert Decimal to float before JSON serialization.
        """
        import json
        from decimal import Decimal

        # Simulate what Flask jsonify does with different types
        good_data = {"data": [0.9, 1.5, None, 2.3]}
        bad_data = {"data": [Decimal('0.9'), Decimal('1.5'), None, Decimal('2.3')]}

        # Good data stays as numbers
        good_json = json.dumps(good_data)
        good_parsed = json.loads(good_json)
        for val in good_parsed['data']:
            if val is not None:
                assert isinstance(val, (int, float)), f"Should be number, got {type(val)}"

        # Bad data (Decimal) - this shows what happens without conversion
        # Note: json.dumps converts Decimal to string!
        # We need to use a custom encoder or convert beforehand
        class DecimalEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, Decimal):
                    return str(obj)  # Flask's default behavior
                return super().default(obj)

        bad_json = json.dumps(bad_data, cls=DecimalEncoder)
        bad_parsed = json.loads(bad_json)

        # Document the bug - Decimals become strings!
        # This test verifies the problem exists so we know why we need float() conversion
        for val in bad_parsed['data']:
            if val is not None:
                # This documents the behavior - Decimals serialize as strings
                assert isinstance(val, str), (
                    "Expected Decimal to serialize as string (the bug we're preventing). "
                    "Our fix in get_single_park_hourly() converts to float before JSON."
                )


class TestSingleSourceOfChartDataConversion:
    """Tests ensuring all chart data sources convert Decimals consistently."""

    def test_shame_score_calculator_converts_decimals(self):
        """ShameScoreCalculator.get_recent_snapshots() must convert Decimals to floats."""
        from database.calculators.shame_score import ShameScoreCalculator
        import inspect

        # Get the source code of get_recent_snapshots
        source = inspect.getsource(ShameScoreCalculator.get_recent_snapshots)

        # Should have float conversion
        assert 'float(' in source, (
            "get_recent_snapshots() must convert Decimal to float. "
            "Add: data.append(float(score) if score is not None else None)"
        )

    def test_park_shame_history_converts_decimals(self):
        """ParkShameHistoryQuery.get_single_park_hourly() must convert Decimals to floats."""
        from database.queries.charts.park_shame_history import ParkShameHistoryQuery
        import inspect

        # Get the source code of get_single_park_hourly
        source = inspect.getsource(ParkShameHistoryQuery.get_single_park_hourly)

        # Should have float conversion for data array
        assert 'float(' in source or 'aligned_data' not in source, (
            "get_single_park_hourly() must convert Decimal to float. "
            "The aligned_data contains raw Decimals from get_hourly_breakdown(). "
            "Add conversion: [float(v) if v is not None else None for v in aligned_data]"
        )
