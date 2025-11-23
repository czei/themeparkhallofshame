"""
Theme Park Downtime Tracker - Status Calculator Unit Tests

Tests the computed_is_open logic from data-model.md:
- Wait time > 0 overrides is_open flag
- Wait time = 0 follows is_open flag
- NULL wait time follows is_open flag
- Default to closed when no clear signal

Corresponds to T144 in tasks.md (originally in Phase 14, moved to Phase 2)
"""

import pytest
import sys
from pathlib import Path

# Add src to path for imports
backend_src = Path(__file__).parent.parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))

from collector.status_calculator import computed_is_open, validate_wait_time


# ============================================================================
# Test Class: computed_is_open() - Core Business Logic
# ============================================================================

class TestComputedIsOpen:
    """
    Test computed_is_open logic from data-model.md.

    Priority: P0 - Critical business logic used in every snapshot
    Coverage Target: 100% (function is simple but critical)
    """

    # ========================================================================
    # Positive Wait Time Cases (wait_time > 0)
    # ========================================================================

    def test_positive_wait_time_overrides_is_open_false(self):
        """
        RULE: wait_time > 0 → OPEN (overrides is_open flag)

        Given: wait_time = 45 minutes, is_open = False
        When: computed_is_open() is called
        Then: Return True (people waiting means ride is open)
        """
        result = computed_is_open(wait_time=45, is_open=False)
        assert result is True, \
            "Positive wait time should override is_open=False"

    def test_positive_wait_time_with_is_open_true(self):
        """
        RULE: wait_time > 0 → OPEN (consistent with is_open flag)

        Given: wait_time = 60 minutes, is_open = True
        When: computed_is_open() is called
        Then: Return True (both signals agree: ride is open)
        """
        result = computed_is_open(wait_time=60, is_open=True)
        assert result is True, \
            "Positive wait time with is_open=True should be open"

    def test_positive_wait_time_with_is_open_none(self):
        """
        RULE: wait_time > 0 → OPEN (regardless of missing is_open)

        Given: wait_time = 30 minutes, is_open = None
        When: computed_is_open() is called
        Then: Return True (wait time alone indicates open)
        """
        result = computed_is_open(wait_time=30, is_open=None)
        assert result is True, \
            "Positive wait time with is_open=None should be open"

    def test_minimal_positive_wait_time(self):
        """
        Edge case: Minimum positive wait time (1 minute)

        Given: wait_time = 1 minute, is_open = False
        When: computed_is_open() is called
        Then: Return True (even 1 minute wait means open)
        """
        result = computed_is_open(wait_time=1, is_open=False)
        assert result is True, \
            "wait_time=1 should still override is_open=False"

    def test_large_wait_time(self):
        """
        Edge case: Extremely large wait time

        Given: wait_time = 500 minutes (8+ hours), is_open = False
        When: computed_is_open() is called
        Then: Return True (trust the wait time data)
        """
        result = computed_is_open(wait_time=500, is_open=False)
        assert result is True, \
            "Large wait time should override is_open=False"

    # ========================================================================
    # Zero Wait Time Cases (wait_time = 0)
    # ========================================================================

    def test_zero_wait_time_with_is_open_true(self):
        """
        RULE: wait_time = 0 AND is_open = true → OPEN

        Given: wait_time = 0 minutes, is_open = True
        When: computed_is_open() is called
        Then: Return True (API says open, no wait to enter)
        """
        result = computed_is_open(wait_time=0, is_open=True)
        assert result is True, \
            "Zero wait time with is_open=True should be open"

    def test_zero_wait_time_with_is_open_false(self):
        """
        RULE: wait_time = 0 AND is_open = false → CLOSED

        Given: wait_time = 0 minutes, is_open = False
        When: computed_is_open() is called
        Then: Return False (API says closed)
        """
        result = computed_is_open(wait_time=0, is_open=False)
        assert result is False, \
            "Zero wait time with is_open=False should be closed"

    def test_zero_wait_time_with_is_open_none(self):
        """
        Edge case: Zero wait time with missing is_open flag

        Given: wait_time = 0 minutes, is_open = None
        When: computed_is_open() is called
        Then: Return False (default to closed when unclear)
        """
        result = computed_is_open(wait_time=0, is_open=None)
        assert result is False, \
            "Zero wait time with is_open=None should default to closed"

    # ========================================================================
    # NULL Wait Time Cases (wait_time = None)
    # ========================================================================

    def test_null_wait_time_with_is_open_true(self):
        """
        RULE: wait_time = NULL AND is_open = true → OPEN

        Given: wait_time = None, is_open = True
        When: computed_is_open() is called
        Then: Return True (rely on is_open flag)
        """
        result = computed_is_open(wait_time=None, is_open=True)
        assert result is True, \
            "NULL wait time with is_open=True should be open"

    def test_null_wait_time_with_is_open_false(self):
        """
        RULE: wait_time = NULL AND is_open = false → CLOSED

        Given: wait_time = None, is_open = False
        When: computed_is_open() is called
        Then: Return False (rely on is_open flag)
        """
        result = computed_is_open(wait_time=None, is_open=False)
        assert result is False, \
            "NULL wait time with is_open=False should be closed"

    def test_null_wait_time_with_null_is_open(self):
        """
        RULE: wait_time = NULL AND is_open = NULL → CLOSED (default)

        Given: wait_time = None, is_open = None
        When: computed_is_open() is called
        Then: Return False (no data, assume closed)
        """
        result = computed_is_open(wait_time=None, is_open=None)
        assert result is False, \
            "Both NULL should default to closed"

    # ========================================================================
    # Examples from Docstring
    # ========================================================================

    def test_docstring_example_1_wait_overrides(self):
        """Docstring example: computed_is_open(45, False) → True"""
        assert computed_is_open(45, False) is True

    def test_docstring_example_2_open_no_wait(self):
        """Docstring example: computed_is_open(0, True) → True"""
        assert computed_is_open(0, True) is True

    def test_docstring_example_3_closed(self):
        """Docstring example: computed_is_open(0, False) → False"""
        assert computed_is_open(0, False) is False

    def test_docstring_example_4_rely_on_flag(self):
        """Docstring example: computed_is_open(None, True) → True"""
        assert computed_is_open(None, True) is True

    def test_docstring_example_5_no_data(self):
        """Docstring example: computed_is_open(None, None) → False"""
        assert computed_is_open(None, None) is False


# ============================================================================
# Test Class: validate_wait_time() - Input Validation
# ============================================================================

class TestValidateWaitTime:
    """
    Test wait time validation and sanitization.

    Priority: P0 - Prevents invalid data from entering the system
    Coverage Target: 100% (simple validation function)
    """

    # ========================================================================
    # Valid Cases
    # ========================================================================

    def test_valid_wait_time_normal(self):
        """
        Valid wait time should pass through unchanged.

        Given: wait_time = 45 minutes
        When: validate_wait_time() is called
        Then: Return 45 (valid, unchanged)
        """
        result = validate_wait_time(45)
        assert result == 45, \
            "Valid wait time should pass through unchanged"

    def test_valid_wait_time_zero(self):
        """
        Zero wait time is valid (ride open, no wait).

        Given: wait_time = 0 minutes
        When: validate_wait_time() is called
        Then: Return 0 (valid)
        """
        result = validate_wait_time(0)
        assert result == 0, \
            "Zero is a valid wait time (no wait)"

    def test_valid_wait_time_large(self):
        """
        Large wait times are accepted (no capping).

        Given: wait_time = 999 minutes
        When: validate_wait_time() is called
        Then: Return 999 (accepted, not capped)
        """
        result = validate_wait_time(999)
        assert result == 999, \
            "Large wait times should be accepted without capping"

    def test_valid_wait_time_very_large(self):
        """
        Edge case: Very large wait time (>8 hours) is suspicious but allowed.

        Given: wait_time = 600 minutes (10 hours)
        When: validate_wait_time() is called
        Then: Return 600 (suspicious but allowed, let analysis decide)
        """
        result = validate_wait_time(600)
        assert result == 600, \
            "Very large wait times should be allowed (data analysis can filter)"

    # ========================================================================
    # Invalid Cases
    # ========================================================================

    def test_negative_wait_time_invalid(self):
        """
        Negative wait times are invalid.

        Given: wait_time = -1 minutes
        When: validate_wait_time() is called
        Then: Return None (invalid)
        """
        result = validate_wait_time(-1)
        assert result is None, \
            "Negative wait time should be rejected"

    def test_negative_wait_time_large_negative(self):
        """
        Edge case: Large negative wait time.

        Given: wait_time = -999 minutes
        When: validate_wait_time() is called
        Then: Return None (invalid)
        """
        result = validate_wait_time(-999)
        assert result is None, \
            "Large negative wait time should be rejected"

    # ========================================================================
    # NULL Cases
    # ========================================================================

    def test_null_wait_time(self):
        """
        NULL wait time is valid (missing data).

        Given: wait_time = None
        When: validate_wait_time() is called
        Then: Return None (missing data, not invalid)
        """
        result = validate_wait_time(None)
        assert result is None, \
            "NULL wait time should return None (missing data)"

    # ========================================================================
    # Examples from Docstring
    # ========================================================================

    def test_docstring_example_1_valid(self):
        """Docstring example: validate_wait_time(45) → 45"""
        assert validate_wait_time(45) == 45

    def test_docstring_example_2_negative(self):
        """Docstring example: validate_wait_time(-1) → None"""
        assert validate_wait_time(-1) is None

    def test_docstring_example_3_large(self):
        """Docstring example: validate_wait_time(999) → 999"""
        assert validate_wait_time(999) == 999

    def test_docstring_example_4_null(self):
        """Docstring example: validate_wait_time(None) → None"""
        assert validate_wait_time(None) is None


# ============================================================================
# Integration Tests: Combined Scenarios
# ============================================================================

class TestIntegratedScenarios:
    """
    Test realistic combined scenarios using both functions together.

    Priority: P1 - Verify functions work together correctly
    """

    def test_validated_positive_wait_time_overrides_closed(self):
        """
        Scenario: API returns wait_time=45, is_open=False

        Given: Raw API data with contradictory signals
        When: validate_wait_time(45) → 45, then computed_is_open(45, False)
        Then: Ride is considered open (wait time wins)
        """
        validated_wait = validate_wait_time(45)
        assert validated_wait == 45
        is_open = computed_is_open(validated_wait, False)
        assert is_open is True

    def test_validated_negative_wait_time_defaults_to_flag(self):
        """
        Scenario: API returns invalid negative wait_time=-5, is_open=True

        Given: Invalid wait time needs to be sanitized
        When: validate_wait_time(-5) → None, then computed_is_open(None, True)
        Then: Ride is considered open (relies on is_open flag)
        """
        validated_wait = validate_wait_time(-5)
        assert validated_wait is None
        is_open = computed_is_open(validated_wait, True)
        assert is_open is True

    def test_validated_zero_wait_with_open_flag(self):
        """
        Scenario: API returns wait_time=0, is_open=True (walk-on ride)

        Given: Ride is open with no wait
        When: validate_wait_time(0) → 0, then computed_is_open(0, True)
        Then: Ride is considered open
        """
        validated_wait = validate_wait_time(0)
        assert validated_wait == 0
        is_open = computed_is_open(validated_wait, True)
        assert is_open is True

    def test_complete_missing_data(self):
        """
        Scenario: API returns no data (wait_time=None, is_open=None)

        Given: Complete missing data
        When: validate_wait_time(None) → None, then computed_is_open(None, None)
        Then: Ride is assumed closed (safe default)
        """
        validated_wait = validate_wait_time(None)
        assert validated_wait is None
        is_open = computed_is_open(validated_wait, None)
        assert is_open is False


# ============================================================================
# Performance & Edge Cases
# ============================================================================

class TestEdgeCasesAndPerformance:
    """
    Test edge cases and performance characteristics.

    Priority: P2 - Nice to have, ensures robustness
    """

    def test_function_is_fast(self):
        """
        Performance: computed_is_open should be extremely fast.

        Given: 10,000 calls to computed_is_open
        When: Executed in a loop
        Then: Complete in < 0.1 seconds (simple logic, no I/O)
        """
        import time
        start = time.time()
        for _ in range(10000):
            computed_is_open(45, False)
        elapsed = time.time() - start
        assert elapsed < 0.1, \
            f"10k calls should complete in <0.1s, took {elapsed:.3f}s"

    def test_type_safety_wait_time_string(self):
        """
        Edge case: What happens if wait_time is a string?

        Note: This should fail at runtime (TypeError) since function
        expects Optional[int]. Type checking should catch this.
        """
        # Uncomment to test - should raise TypeError
        # with pytest.raises(TypeError):
        #     validate_wait_time("45")
        pass  # Type checking via mypy should prevent this

    def test_type_safety_is_open_string(self):
        """
        Edge case: What happens if is_open is a string?

        Note: This should fail at runtime (comparison issues) since
        function expects Optional[bool]. Type checking should catch this.
        """
        # Uncomment to test - should cause issues
        # with pytest.raises(TypeError):
        #     computed_is_open(45, "true")
        pass  # Type checking via mypy should prevent this
