# Plan to Complete Test Fixes

## Current Status

**✅ Completed:**
- Fixed test isolation in `test_today_api_contract.py` - Flask client now created AFTER database setup (all 10 tests pass)
- Fixed 5/6 tests in `test_collect_snapshots_integration.py`:
  - Added ride_classifications fixture data
  - Mocked `_aggregate_live_rankings()` to prevent hanging
  - Added park schedule data with time windows that include "now"
  - Added explicit commit between collector runs

**🔄 In Progress:**
- Implemented fixes for remaining integration tests (status change detection, weather collection mocks). Needs DB-backed rerun to confirm.

---

## Remaining Work

### Task 1: Fix `test_detects_ride_going_down`

**Status:** Updated collector to compare against the previous persisted snapshot BEFORE inserting the new one, so OPEN → CLOSED transitions are detected across runs.

**Next Step:** Re-run the test with a MySQL test DB to confirm `ride_status_changes` is written.

---

### Task 2: Fix `test_weather_collection.py` Failures

**Status:** Mock API client now returns observations with the current park_id (side effect) to satisfy FK constraints during inserts.

**Next Step:** Re-run `tests/integration/test_weather_collection.py` against MySQL to verify no FK errors and failure-handling assertions still hold.

---

### Task 3: Update Todo List

Refresh after DB reruns; if all green, collapse Tasks 1–2 and mark file as resolved.

---

### Task 4: Run Full Integration Test Suite

Once all tests pass individually:
1. Run `pytest tests/integration/ -v --no-cov`
2. Verify all tests pass (expect ~230-240 passing)
3. Check for any new failures introduced by fixes

**Expected Result:** All integration tests pass

---

### Task 5: Commit All Fixes

**Files to commit:**
1. `test_today_api_contract.py` - Flask client fixture fix
2. `test_collect_snapshots_integration.py` - Multiple fixture fixes
3. `test_weather_collection.py` - Mock data fixes
4. `scripts/collect_snapshots.py` - Status change detection ordering

**Commit Message:**
```
test: fix integration test isolation and fixture issues

- Fix test_today_api_contract.py: Create Flask client after DB setup
- Fix test_collect_snapshots_integration.py: Add missing fixtures
  - Add ride_classifications entries
  - Mock _aggregate_live_rankings to prevent hangs
  - Add park schedule data for park open detection
  - Explicit commit between collector runs
- Fix test_weather_collection.py: Use valid park IDs in mocks
- Update snapshot collector to detect status changes before inserting new snapshot

Resolves test isolation issues that caused failures when running
full test suite. All fixtures now properly set up and clean up data.
```

---

## Estimated Time to Complete

- **Task 1** (status change detection): 20-30 minutes (debugging + fix)
- **Task 2** (weather collection): 15-20 minutes
- **Task 3** (todo update): 2 minutes
- **Task 4** (full test run): 5 minutes
- **Task 5** (commit): 5 minutes

**Total: ~45-60 minutes**

---

## Success Criteria

✅ All integration tests pass when run individually
✅ All integration tests pass when run as full suite
✅ No test isolation issues remaining
✅ Changes committed with clear description

---

## Commands Reference

```bash
# Run specific failing tests
source ~/.zshrc && PYTHONPATH=src pytest tests/integration/test_collect_snapshots_integration.py::TestStatusChangeDetection::test_detects_ride_going_down -v --no-cov

source ~/.zshrc && PYTHONPATH=src pytest tests/integration/test_weather_collection.py -v --no-cov

# Run full integration suite
source ~/.zshrc && PYTHONPATH=src pytest tests/integration/ -v --no-cov

# Count passing/failing tests
source ~/.zshrc && PYTHONPATH=src pytest tests/integration/ --no-cov -q | tail -5
```

---

## Notes

- All database fixtures use transactions that auto-rollback except where explicitly committed
- Tests that use `collector.run()` need mocked `_aggregate_live_rankings` or they hang
- Schedule data must use naive datetime (not timezone-aware) to match `schedule_repository.is_park_open_now()` logic
- Flask test client isolation fixed by creating app AFTER database setup in fixture
