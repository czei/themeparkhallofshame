"""
Performance Baseline Tests for API Queries
==========================================

These tests measure query performance to:
1. Establish baselines BEFORE optimization
2. Verify improvements AFTER optimization
3. Prevent performance regressions

Run with: pytest tests/performance/ -v -s --durations=0

Note: These tests require a database connection and real data.
Mark as @pytest.mark.slow to exclude from fast test runs.
"""

import pytest
import time
from typing import Callable, Tuple


def measure_query_time(query_fn: Callable, iterations: int = 3) -> Tuple[float, float, float]:
    """
    Measure query execution time over multiple iterations.

    Returns:
        Tuple of (min_time, avg_time, max_time) in seconds
    """
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        query_fn()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    return min(times), sum(times) / len(times), max(times)


@pytest.mark.slow
@pytest.mark.integration
class TestTodayQueryPerformance:
    """
    Performance tests for TODAY period queries.

    These queries aggregate ride_status_snapshots from midnight to now,
    which is the primary performance bottleneck.

    Target: < 2 seconds after optimization
    Baseline: 20-30 seconds before optimization
    """

    def test_today_ride_wait_times_performance(self, mysql_connection):
        """
        Measure TODAY ride wait times query performance.

        This query:
        - Aggregates all wait times from midnight Pacific to now
        - Groups by ride with AVG, MAX calculations
        - Joins ride_status_snapshots, rides, parks, ride_classifications
        - Has 3 correlated subqueries for current status (performance bottleneck)
        """
        from database.queries.today.today_ride_wait_times import TodayRideWaitTimesQuery

        def run_query():
            query = TodayRideWaitTimesQuery(mysql_connection)
            return query.get_rankings(limit=50)

        min_time, avg_time, max_time = measure_query_time(run_query)
        results = run_query()

        print(f"\n{'='*60}")
        print("TODAY Ride Wait Times Query Performance")
        print(f"{'='*60}")
        print(f"  Results:    {len(results)} rides")
        print(f"  Min time:   {min_time:.3f}s")
        print(f"  Avg time:   {avg_time:.3f}s")
        print(f"  Max time:   {max_time:.3f}s")
        print(f"{'='*60}")

        # Baseline assertion - will be tightened after optimization
        # Current expectation: < 30s (will reduce to < 2s after optimization)
        assert avg_time < 30.0, \
            f"TODAY ride wait times took {avg_time:.1f}s avg, expected < 30s"

    def test_today_park_wait_times_performance(self, mysql_session):
        """
        Measure TODAY park wait times query performance.

        This query:
        - Aggregates wait times at park level
        - Groups by park with AVG, MAX, COUNT calculations
        - Joins ride_status_snapshots, rides, parks, park_activity_snapshots

        NOTE: Uses mysql_session (not mysql_connection) because TodayParkWaitTimesQuery
        uses StatsRepository which requires Session.query() API.
        """
        from database.queries.today.today_park_wait_times import TodayParkWaitTimesQuery

        def run_query():
            query = TodayParkWaitTimesQuery(mysql_session)
            return query.get_rankings(limit=50)

        min_time, avg_time, max_time = measure_query_time(run_query)
        results = run_query()

        print(f"\n{'='*60}")
        print("TODAY Park Wait Times Query Performance")
        print(f"{'='*60}")
        print(f"  Results:    {len(results)} parks")
        print(f"  Min time:   {min_time:.3f}s")
        print(f"  Avg time:   {avg_time:.3f}s")
        print(f"  Max time:   {max_time:.3f}s")
        print(f"{'='*60}")

        assert avg_time < 30.0, \
            f"TODAY park wait times took {avg_time:.1f}s avg, expected < 30s"

    def test_today_ride_downtime_performance(self, mysql_connection):
        """
        Measure TODAY ride downtime ranking query performance.

        This query:
        - Calculates downtime for rides from midnight to now
        - Uses weighted tier-based scoring
        - Similar pattern to wait times but different aggregation
        """
        from database.queries.today.today_ride_rankings import TodayRideRankingsQuery

        def run_query():
            query = TodayRideRankingsQuery(mysql_connection)
            return query.get_rankings(limit=50)

        min_time, avg_time, max_time = measure_query_time(run_query)
        results = run_query()

        print(f"\n{'='*60}")
        print("TODAY Ride Downtime Query Performance")
        print(f"{'='*60}")
        print(f"  Results:    {len(results)} rides")
        print(f"  Min time:   {min_time:.3f}s")
        print(f"  Avg time:   {avg_time:.3f}s")
        print(f"  Max time:   {max_time:.3f}s")
        print(f"{'='*60}")

        assert avg_time < 30.0, \
            f"TODAY ride downtime took {avg_time:.1f}s avg, expected < 30s"

    def test_today_park_downtime_performance(self, mysql_connection):
        """
        Measure TODAY park downtime (shame score) query performance.

        This query:
        - Calculates weighted shame score for parks
        - Complex aggregation with tier weights
        """
        from database.queries.today.today_park_rankings import TodayParkRankingsQuery

        def run_query():
            query = TodayParkRankingsQuery(mysql_connection)
            return query.get_rankings(limit=50)

        min_time, avg_time, max_time = measure_query_time(run_query)
        results = run_query()

        print(f"\n{'='*60}")
        print("TODAY Park Downtime (Shame) Query Performance")
        print(f"{'='*60}")
        print(f"  Results:    {len(results)} parks")
        print(f"  Min time:   {min_time:.3f}s")
        print(f"  Avg time:   {avg_time:.3f}s")
        print(f"  Max time:   {max_time:.3f}s")
        print(f"{'='*60}")

        assert avg_time < 30.0, \
            f"TODAY park downtime took {avg_time:.1f}s avg, expected < 30s"


@pytest.mark.slow
@pytest.mark.integration
class TestAwardsQueryPerformance:
    """
    Performance tests for Awards (Trends) queries.

    These are the slowest queries, currently taking 1.5-2 minutes.
    Target: < 10 seconds after optimization
    """

    def test_longest_wait_times_today_performance(self, mysql_connection):
        """
        Measure longest wait times query for TODAY period.
        """
        from database.queries.trends.longest_wait_times import LongestWaitTimesQuery

        def run_query():
            query = LongestWaitTimesQuery(mysql_connection)
            return query.get_rankings(period='today', limit=10)

        min_time, avg_time, max_time = measure_query_time(run_query, iterations=2)
        results = run_query()

        print(f"\n{'='*60}")
        print("Awards: Longest Wait Times (TODAY) Performance")
        print(f"{'='*60}")
        print(f"  Results:    {len(results)} rides")
        print(f"  Min time:   {min_time:.3f}s")
        print(f"  Avg time:   {avg_time:.3f}s")
        print(f"  Max time:   {max_time:.3f}s")
        print(f"{'='*60}")

        # Baseline: 90-120 seconds - will reduce to < 10s
        assert avg_time < 120.0, \
            f"Longest wait times took {avg_time:.1f}s avg, expected < 120s"

    def test_least_reliable_today_performance(self, mysql_connection):
        """
        Measure least reliable rides query for TODAY period.
        """
        from database.queries.trends.least_reliable_rides import LeastReliableRidesQuery

        def run_query():
            query = LeastReliableRidesQuery(mysql_connection)
            return query.get_rankings(period='today', limit=10)

        min_time, avg_time, max_time = measure_query_time(run_query, iterations=2)
        results = run_query()

        print(f"\n{'='*60}")
        print("Awards: Least Reliable Rides (TODAY) Performance")
        print(f"{'='*60}")
        print(f"  Results:    {len(results)} rides")
        print(f"  Min time:   {min_time:.3f}s")
        print(f"  Avg time:   {avg_time:.3f}s")
        print(f"  Max time:   {max_time:.3f}s")
        print(f"{'='*60}")

        assert avg_time < 120.0, \
            f"Least reliable took {avg_time:.1f}s avg, expected < 120s"


@pytest.mark.slow
@pytest.mark.integration
class TestDatabaseHealth:
    """
    Tests for database configuration and health metrics.

    These help diagnose performance issues at the database level.
    """

    def test_buffer_pool_configuration(self, mysql_connection):
        """
        Check InnoDB buffer pool size and utilization.

        Buffer pool should be >= 512MB to hold all indexes in memory.
        Hit rate should be > 99% for good performance.
        """
        from sqlalchemy import text

        # Get buffer pool size
        result = mysql_connection.execute(text(
            "SELECT @@innodb_buffer_pool_size / 1024 / 1024 AS size_mb"
        ))
        size_mb = result.scalar()

        # Get buffer pool stats
        stats_query = text("""
            SELECT
                variable_name,
                variable_value
            FROM performance_schema.global_status
            WHERE variable_name IN (
                'Innodb_buffer_pool_reads',
                'Innodb_buffer_pool_read_requests',
                'Innodb_buffer_pool_pages_data',
                'Innodb_buffer_pool_pages_total'
            )
        """)

        try:
            stats = {row[0]: int(row[1]) for row in mysql_connection.execute(stats_query)}
            reads = stats.get('Innodb_buffer_pool_reads', 0)
            requests = stats.get('Innodb_buffer_pool_read_requests', 1)
            hit_rate = (1 - reads / requests) * 100 if requests > 0 else 0
            pages_data = stats.get('Innodb_buffer_pool_pages_data', 0)
            pages_total = stats.get('Innodb_buffer_pool_pages_total', 1)
            utilization = (pages_data / pages_total) * 100
        except Exception:
            hit_rate = "N/A"
            utilization = "N/A"

        print(f"\n{'='*60}")
        print("InnoDB Buffer Pool Configuration")
        print(f"{'='*60}")
        print(f"  Buffer pool size:    {size_mb:.0f} MB")
        print(f"  Hit rate:            {hit_rate}%")
        print(f"  Utilization:         {utilization}%")
        print(f"{'='*60}")

        # After optimization, this should pass
        # For now, just document the current state
        if size_mb < 512:
            print(f"  WARNING: Buffer pool ({size_mb}MB) < 512MB recommended")

    def test_table_statistics(self, mysql_connection):
        """
        Get row counts and data sizes for key tables.
        """
        from sqlalchemy import text

        tables = ['ride_status_snapshots', 'park_activity_snapshots', 'rides', 'parks']

        print(f"\n{'='*60}")
        print("Table Statistics")
        print(f"{'='*60}")

        for table in tables:
            try:
                count_result = mysql_connection.execute(
                    text(f"SELECT COUNT(*) FROM {table}")
                )
                row_count = count_result.scalar()

                size_result = mysql_connection.execute(text("""
                    SELECT
                        ROUND((data_length + index_length) / 1024 / 1024, 2) AS size_mb
                    FROM information_schema.tables
                    WHERE table_schema = DATABASE()
                    AND table_name = :table
                """), {"table": table})
                size_mb = size_result.scalar() or 0

                print(f"  {table}: {row_count:,} rows, {size_mb} MB")
            except Exception as e:
                print(f"  {table}: Error - {e}")

        print(f"{'='*60}")

    def test_index_usage(self, mysql_connection):
        """
        Check which indexes exist on key tables.
        """
        from sqlalchemy import text

        print(f"\n{'='*60}")
        print("Index Inventory - ride_status_snapshots")
        print(f"{'='*60}")

        result = mysql_connection.execute(text("""
            SHOW INDEX FROM ride_status_snapshots
        """))

        indexes = {}
        for row in result:
            key_name = row[2]  # Key_name
            column_name = row[4]  # Column_name
            if key_name not in indexes:
                indexes[key_name] = []
            indexes[key_name].append(column_name)

        for name, columns in indexes.items():
            print(f"  {name}: ({', '.join(columns)})")

        print(f"{'='*60}")
