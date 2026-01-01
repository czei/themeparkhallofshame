"""
Locust Load Test Configuration for Theme Park API

Simulates realistic user traffic patterns for performance validation.

Usage:
    # Interactive mode (opens web UI at http://localhost:8089)
    locust -f tests/performance/locustfile.py --host=http://localhost:5001

    # Headless mode (for CI/automation)
    locust -f tests/performance/locustfile.py --host=http://localhost:5001 \
        --users=20 --spawn-rate=2 --run-time=60s --headless

    # Quick validation (User Story 6 target: <500ms p95)
    locust -f tests/performance/locustfile.py --host=http://localhost:5001 \
        --users=20 --spawn-rate=5 --run-time=30s --headless \
        --csv=performance_results

Performance Targets (from spec FR-010):
    - p95 response time < 500ms for all API endpoints
    - p99 response time < 1000ms
    - 0% error rate under normal load (20 concurrent users)

Feature 003-orm-refactoring, Task T038
"""

from locust import HttpUser, task, between, events
import json
import time


class ThemeParkAPIUser(HttpUser):
    """
    Simulates a user browsing the Theme Park Hall of Shame dashboard.

    Traffic patterns based on observed usage:
    - 40% parks/downtime (main dashboard view)
    - 30% rides/downtime (drill-down from park)
    - 15% parks/waittimes (secondary tab)
    - 15% rides/waittimes (wait time rankings)
    """

    # User think time between requests (1-3 seconds simulates real browsing)
    wait_time = between(1, 3)

    # Periods users commonly view
    periods = ['today', 'yesterday', 'last_week', 'last_month']

    def on_start(self):
        """Called when a simulated user starts."""
        self.request_count = 0

    # =========================================================================
    # PRIMARY DASHBOARD - Parks Downtime (40% of traffic)
    # =========================================================================

    @task(4)
    def get_parks_downtime_today(self):
        """
        Main dashboard: TODAY park shame rankings.

        This is the most frequently accessed endpoint (default view).
        Target: <500ms p95
        """
        with self.client.get(
            "/api/parks/downtime?period=today&limit=50",
            name="/api/parks/downtime [today]",
            catch_response=True
        ) as response:
            self._validate_response(response, min_results=5)

    @task(2)
    def get_parks_downtime_yesterday(self):
        """Parks downtime for yesterday - common historical view."""
        with self.client.get(
            "/api/parks/downtime?period=yesterday&limit=50",
            name="/api/parks/downtime [yesterday]",
            catch_response=True
        ) as response:
            self._validate_response(response, min_results=5)

    @task(1)
    def get_parks_downtime_weekly(self):
        """Parks downtime for last week."""
        with self.client.get(
            "/api/parks/downtime?period=last_week&limit=50",
            name="/api/parks/downtime [last_week]",
            catch_response=True
        ) as response:
            self._validate_response(response, min_results=5)

    # =========================================================================
    # RIDES DOWNTIME - Drill-down view (30% of traffic)
    # =========================================================================

    @task(3)
    def get_rides_downtime_today(self):
        """
        Rides downtime rankings for today.

        Users click on a park to see individual ride breakdowns.
        Target: <500ms p95
        """
        with self.client.get(
            "/api/rides/downtime?period=today&filter=all-parks&limit=50",
            name="/api/rides/downtime [today]",
            catch_response=True
        ) as response:
            self._validate_response(response, min_results=1)

    @task(1)
    def get_rides_downtime_yesterday(self):
        """Rides downtime for yesterday."""
        with self.client.get(
            "/api/rides/downtime?period=yesterday&filter=all-parks&limit=50",
            name="/api/rides/downtime [yesterday]",
            catch_response=True
        ) as response:
            self._validate_response(response, min_results=1)

    # =========================================================================
    # WAIT TIMES - Secondary tabs (30% of traffic combined)
    # =========================================================================

    @task(2)
    def get_parks_waittimes_today(self):
        """Park wait time rankings."""
        with self.client.get(
            "/api/parks/waittimes?period=today&limit=50",
            name="/api/parks/waittimes [today]",
            catch_response=True
        ) as response:
            self._validate_response(response, min_results=1)

    @task(2)
    def get_rides_waittimes_today(self):
        """Ride wait time rankings."""
        with self.client.get(
            "/api/rides/waittimes?period=today&limit=50",
            name="/api/rides/waittimes [today]",
            catch_response=True
        ) as response:
            self._validate_response(response, min_results=1)

    # =========================================================================
    # HEALTH CHECK - Lightweight endpoint for baseline
    # =========================================================================

    @task(1)
    def get_health(self):
        """Health check endpoint - should be very fast."""
        with self.client.get(
            "/api/health",
            name="/api/health",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Health check failed: {response.status_code}")

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _validate_response(self, response, min_results=0):
        """
        Validate API response.

        Marks response as failed if:
        - Status code is not 200
        - Response time exceeds 2 seconds (hard limit)
        - Response doesn't contain expected data structure
        """
        if response.status_code != 200:
            response.failure(f"Status {response.status_code}")
            return

        # Hard limit: 2 seconds is unacceptable
        if response.elapsed.total_seconds() > 2.0:
            response.failure(f"Too slow: {response.elapsed.total_seconds():.2f}s > 2s limit")
            return

        try:
            data = response.json()

            # Check for expected structure
            if isinstance(data, dict):
                # API returns {data: [...], meta: {...}}
                results = data.get('data', data.get('parks', data.get('rides', [])))
                if len(results) < min_results:
                    response.failure(f"Too few results: {len(results)} < {min_results}")
                    return
            elif isinstance(data, list):
                if len(data) < min_results:
                    response.failure(f"Too few results: {len(data)} < {min_results}")
                    return

            response.success()

        except json.JSONDecodeError:
            response.failure("Invalid JSON response")


# =============================================================================
# PERFORMANCE VALIDATION HOOK
# =============================================================================

@events.quitting.add_listener
def validate_performance(environment, **kwargs):
    """
    Validate performance targets when test completes.

    User Story 6 targets:
    - p95 < 500ms
    - p99 < 1000ms
    - 0% error rate
    """
    if environment.stats.total.num_requests == 0:
        print("\nNo requests completed - cannot validate performance")
        return

    stats = environment.stats.total

    # Calculate percentiles
    p50 = stats.get_response_time_percentile(0.5)
    p95 = stats.get_response_time_percentile(0.95)
    p99 = stats.get_response_time_percentile(0.99)

    # Error rate
    error_rate = (stats.num_failures / stats.num_requests) * 100 if stats.num_requests > 0 else 0

    print("\n" + "=" * 60)
    print("PERFORMANCE VALIDATION RESULTS")
    print("=" * 60)
    print(f"  Total Requests:  {stats.num_requests}")
    print(f"  Failed Requests: {stats.num_failures}")
    print(f"  Error Rate:      {error_rate:.2f}%")
    print(f"  p50 (median):    {p50:.0f}ms")
    print(f"  p95:             {p95:.0f}ms")
    print(f"  p99:             {p99:.0f}ms")
    print("=" * 60)

    # Validate targets
    passed = True

    if p95 > 500:
        print(f"  FAIL: p95 ({p95:.0f}ms) exceeds 500ms target")
        passed = False
    else:
        print(f"  PASS: p95 ({p95:.0f}ms) within 500ms target")

    if p99 > 1000:
        print(f"  FAIL: p99 ({p99:.0f}ms) exceeds 1000ms target")
        passed = False
    else:
        print(f"  PASS: p99 ({p99:.0f}ms) within 1000ms target")

    if error_rate > 0:
        print(f"  FAIL: Error rate ({error_rate:.2f}%) should be 0%")
        passed = False
    else:
        print("  PASS: Error rate is 0%")

    print("=" * 60)
    print(f"  OVERALL: {'PASSED' if passed else 'FAILED'}")
    print("=" * 60)

    # Exit with non-zero code if validation failed (for CI)
    if not passed:
        environment.process_exit_code = 1
