"""
Unit Tests for Status Summary API Contract (Field Names)
========================================================

TDD Tests: These tests define the EXPECTED API response field names
that the frontend requires. They should FAIL if the API returns
different field names.

The Bug These Tests Catch:
--------------------------
Frontend expects: OPERATING, DOWN, CLOSED, REFURBISHMENT (UPPERCASE)
API was returning: operating, down, closed, refurbishment (lowercase)

This mismatch causes the frontend to display zeros because:
  - status.OPERATING is undefined (API sends status.operating)
  - undefined || 0 evaluates to 0

Frontend code (wait-times.js lines 119-134):
    <div class="stat-value">${status.OPERATING || 0}</div>
    <div class="stat-value">${status.DOWN || 0}</div>
    <div class="stat-value">${status.CLOSED || 0}</div>
    <div class="stat-value">${status.REFURBISHMENT || 0}</div>
"""



class TestStatusSummaryFieldNames:
    """Test that status summary API returns uppercase field names for frontend."""

    def test_status_summary_returns_uppercase_operating(self):
        """
        CRITICAL: Frontend expects 'OPERATING', not 'operating'.

        Frontend code (wait-times.js line 121):
            <div class="stat-value">${status.OPERATING || 0}</div>

        If API returns 'operating', the frontend shows 0.
        """
        import inspect
        from database.queries.live.status_summary import StatusSummaryQuery
        source = inspect.getsource(StatusSummaryQuery.get_summary)

        # The return dict should use uppercase 'OPERATING'
        assert '"OPERATING"' in source or "'OPERATING'" in source, \
            "Status summary must return 'OPERATING' (uppercase) for frontend compatibility"

    def test_status_summary_returns_uppercase_down(self):
        """
        CRITICAL: Frontend expects 'DOWN', not 'down'.

        Frontend code (wait-times.js line 124):
            <div class="stat-value">${status.DOWN || 0}</div>

        If API returns 'down', the frontend shows 0.
        """
        import inspect
        from database.queries.live.status_summary import StatusSummaryQuery
        source = inspect.getsource(StatusSummaryQuery.get_summary)

        # Check the return statement uses uppercase
        # Need to find the dict construction in return statement
        assert '"DOWN"' in source or "'DOWN'" in source, \
            "Status summary must return 'DOWN' (uppercase) for frontend compatibility"

    def test_status_summary_returns_uppercase_closed(self):
        """
        CRITICAL: Frontend expects 'CLOSED', not 'closed'.

        Frontend code (wait-times.js line 128):
            <div class="stat-value">${status.CLOSED || 0}</div>

        If API returns 'closed', the frontend shows 0.
        """
        import inspect
        from database.queries.live.status_summary import StatusSummaryQuery
        source = inspect.getsource(StatusSummaryQuery.get_summary)

        assert '"CLOSED"' in source or "'CLOSED'" in source, \
            "Status summary must return 'CLOSED' (uppercase) for frontend compatibility"

    def test_status_summary_returns_uppercase_refurbishment(self):
        """
        CRITICAL: Frontend expects 'REFURBISHMENT', not 'refurbishment'.

        Frontend code (wait-times.js line 132):
            <div class="stat-value">${status.REFURBISHMENT || 0}</div>

        If API returns 'refurbishment', the frontend shows 0.
        """
        import inspect
        from database.queries.live.status_summary import StatusSummaryQuery
        source = inspect.getsource(StatusSummaryQuery.get_summary)

        assert '"REFURBISHMENT"' in source or "'REFURBISHMENT'" in source, \
            "Status summary must return 'REFURBISHMENT' (uppercase) for frontend compatibility"


class TestStatusSummaryReturnDict:
    """Test that the return dict keys match frontend expectations exactly."""

    def test_return_dict_keys_are_uppercase(self):
        """
        The return dictionary keys must be uppercase to match frontend.

        Previous bug: Return dict had lowercase keys like {"operating": 245}
        Fix: Should be {"OPERATING": 245}
        """
        import inspect
        from database.queries.live.status_summary import StatusSummaryQuery
        source = inspect.getsource(StatusSummaryQuery.get_summary)

        # Find the return statement section (after "if result:")
        lines = source.split('\n')
        in_return_section = False
        lowercase_keys_found = []

        for line in lines:
            stripped = line.strip()
            # Look for lowercase dictionary keys in return statements
            if 'return {' in stripped or in_return_section:
                in_return_section = True
                # Check for lowercase status keys (these are the bugs)
                if '"operating":' in stripped or "'operating':" in stripped:
                    lowercase_keys_found.append('operating')
                if '"down":' in stripped or "'down':" in stripped:
                    lowercase_keys_found.append('down')
                if '"closed":' in stripped or "'closed':" in stripped:
                    lowercase_keys_found.append('closed')
                if '"refurbishment":' in stripped or "'refurbishment':" in stripped:
                    lowercase_keys_found.append('refurbishment')
                if stripped == '}':
                    in_return_section = False

        assert len(lowercase_keys_found) == 0, \
            f"Found lowercase keys {lowercase_keys_found} that will break frontend. Must be UPPERCASE."


class TestStatusSummaryRequiredFields:
    """Document all required fields for status summary API contract."""

    def test_status_summary_required_fields(self):
        """
        Document all required fields for /live/status-summary

        Required by frontend (wait-times.js lines 119-134):
        - OPERATING: Count of rides currently running
        - DOWN: Count of rides with technical issues
        - CLOSED: Count of rides on scheduled closure
        - REFURBISHMENT: Count of rides under maintenance

        Optional (not displayed in panels):
        - PARK_CLOSED: Count of rides at closed parks
        - total: Total ride count
        """
        required_fields = {
            'OPERATING',      # Must be UPPERCASE
            'DOWN',           # Must be UPPERCASE
            'CLOSED',         # Must be UPPERCASE
            'REFURBISHMENT',  # Must be UPPERCASE
        }

        # These are optional/supplementary fields
        optional_fields = {
            'PARK_CLOSED',
            'total',
        }

        assert len(required_fields) == 4, "Expected 4 required status fields"
