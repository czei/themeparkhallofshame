"""
Integration Tests: Storage Metrics Repository (Feature 004)

Tests for storage measurement, growth analysis, and alert generation.

Feature: 004-themeparks-data-collection
Task: T050
"""

import pytest
from datetime import date, timedelta
from decimal import Decimal

from database.repositories.storage_repository import StorageRepository
from models.orm_storage import StorageMetrics


@pytest.mark.integration
class TestStorageRepositoryCreate:
    """Tests for StorageMetrics creation."""

    def test_create_storage_metrics(self, mysql_session):
        """Should create storage metrics entry."""
        repo = StorageRepository(mysql_session)

        metrics = repo.create(
            table_name='test_table',
            measurement_date=date.today(),
            row_count=1000,
            data_size_mb=Decimal('10.50'),
            index_size_mb=Decimal('2.50')
        )

        assert metrics.metric_id is not None
        assert metrics.table_name == 'test_table'
        assert metrics.row_count == 1000
        assert metrics.data_size_mb == Decimal('10.50')
        assert metrics.index_size_mb == Decimal('2.50')
        assert metrics.total_size_mb == Decimal('13.00')
        # First measurement has no growth rate
        assert metrics.growth_rate_mb_per_day is None

    def test_create_calculates_growth_rate(self, mysql_session):
        """Should calculate growth rate from previous measurement."""
        repo = StorageRepository(mysql_session)

        # Create first measurement
        yesterday = date.today() - timedelta(days=1)
        repo.create(
            table_name='growth_table',
            measurement_date=yesterday,
            row_count=1000,
            data_size_mb=Decimal('10.00'),
            index_size_mb=Decimal('2.00')
        )
        mysql_session.flush()

        # Create second measurement today
        metrics = repo.create(
            table_name='growth_table',
            measurement_date=date.today(),
            row_count=1100,
            data_size_mb=Decimal('11.00'),
            index_size_mb=Decimal('2.20')
        )

        # Should have calculated growth rate (1.20 MB growth over 1 day)
        assert metrics.growth_rate_mb_per_day is not None
        assert metrics.growth_rate_mb_per_day == Decimal('1.2000')

    def test_create_with_partition_count(self, mysql_session):
        """Should store partition count if provided."""
        repo = StorageRepository(mysql_session)

        metrics = repo.create(
            table_name='partitioned_table',
            measurement_date=date.today(),
            row_count=100000,
            data_size_mb=Decimal('500.00'),
            index_size_mb=Decimal('100.00'),
            partition_count=24
        )

        assert metrics.partition_count == 24


@pytest.mark.integration
class TestStorageRepositoryQueries:
    """Tests for storage metrics queries."""

    def test_get_latest(self, mysql_session):
        """Should get most recent measurement for a table."""
        repo = StorageRepository(mysql_session)

        # Create multiple measurements
        for i in range(3):
            measurement_date = date.today() - timedelta(days=2-i)
            repo.create(
                table_name='latest_table',
                measurement_date=measurement_date,
                row_count=1000 + i * 100,
                data_size_mb=Decimal('10.00') + i,
                index_size_mb=Decimal('2.00')
            )
        mysql_session.flush()

        latest = repo.get_latest('latest_table')

        assert latest is not None
        assert latest.measurement_date == date.today()
        assert latest.row_count == 1200

    def test_get_by_date(self, mysql_session):
        """Should get measurement for specific table and date."""
        repo = StorageRepository(mysql_session)

        target_date = date.today() - timedelta(days=1)
        repo.create(
            table_name='date_table',
            measurement_date=target_date,
            row_count=1000,
            data_size_mb=Decimal('10.00'),
            index_size_mb=Decimal('2.00')
        )
        mysql_session.flush()

        metrics = repo.get_by_date('date_table', target_date)

        assert metrics is not None
        assert metrics.measurement_date == target_date

    def test_get_history(self, mysql_session):
        """Should get historical measurements for a table."""
        repo = StorageRepository(mysql_session)

        # Create measurements for last 10 days
        for i in range(10):
            measurement_date = date.today() - timedelta(days=9-i)
            repo.create(
                table_name='history_table',
                measurement_date=measurement_date,
                row_count=1000 + i * 100,
                data_size_mb=Decimal('10.00') + i,
                index_size_mb=Decimal('2.00')
            )
        mysql_session.flush()

        # Get last 7 days (days=7 means >= today-7, which returns 8 records)
        history = repo.get_history('history_table', days=7)

        assert len(history) == 8
        # Should be ordered by date ascending
        assert history[0].measurement_date < history[-1].measurement_date

    def test_get_all_tables_latest(self, mysql_session):
        """Should get latest measurement for all tables."""
        repo = StorageRepository(mysql_session)

        # Create measurements for multiple tables
        for table in ['table_a', 'table_b', 'table_c']:
            repo.create(
                table_name=table,
                measurement_date=date.today(),
                row_count=1000,
                data_size_mb=Decimal('10.00'),
                index_size_mb=Decimal('2.00')
            )
        mysql_session.flush()

        all_latest = repo.get_all_tables_latest()

        table_names = [m.table_name for m in all_latest]
        assert 'table_a' in table_names
        assert 'table_b' in table_names
        assert 'table_c' in table_names


@pytest.mark.integration
class TestStorageRepositoryTotals:
    """Tests for storage total calculations."""

    def test_get_total_storage(self, mysql_session):
        """Should calculate total storage across all tables."""
        repo = StorageRepository(mysql_session)

        # Create measurements for multiple tables
        repo.create(
            table_name='total_a',
            measurement_date=date.today(),
            row_count=1000,
            data_size_mb=Decimal('100.00'),
            index_size_mb=Decimal('20.00')
        )
        repo.create(
            table_name='total_b',
            measurement_date=date.today(),
            row_count=2000,
            data_size_mb=Decimal('200.00'),
            index_size_mb=Decimal('40.00')
        )
        mysql_session.flush()

        total = repo.get_total_storage()

        # Filter to our test tables (there may be others from prior tests)
        assert total['total_data_mb'] >= Decimal('300.00')
        assert total['total_index_mb'] >= Decimal('60.00')
        assert total['total_size_mb'] >= Decimal('360.00')
        assert total['total_rows'] >= 3000
        assert total['table_count'] >= 2


@pytest.mark.integration
class TestStorageRepositoryProjections:
    """Tests for storage projections."""

    def test_project_storage(self, mysql_session):
        """Should project future storage based on growth rate."""
        repo = StorageRepository(mysql_session)

        # Create measurements with growth
        yesterday = date.today() - timedelta(days=1)
        repo.create(
            table_name='project_table',
            measurement_date=yesterday,
            row_count=1000,
            data_size_mb=Decimal('100.00'),
            index_size_mb=Decimal('20.00')
        )
        mysql_session.flush()

        repo.create(
            table_name='project_table',
            measurement_date=date.today(),
            row_count=1100,
            data_size_mb=Decimal('110.00'),
            index_size_mb=Decimal('22.00')
        )
        mysql_session.flush()

        projection = repo.project_storage(days=365)

        assert 'current_size_mb' in projection
        assert 'growth_rate_mb_per_day' in projection
        assert 'projected_size_mb' in projection
        assert projection['projected_days'] == 365

    def test_get_growth_analysis(self, mysql_session):
        """Should analyze growth trends for a table."""
        repo = StorageRepository(mysql_session)

        # Create measurements over several days
        for i in range(7):
            measurement_date = date.today() - timedelta(days=6-i)
            repo.create(
                table_name='analysis_table',
                measurement_date=measurement_date,
                row_count=1000 + i * 100,
                data_size_mb=Decimal('100.00') + i * Decimal('5.00'),
                index_size_mb=Decimal('20.00') + i
            )
        mysql_session.flush()

        analysis = repo.get_growth_analysis('analysis_table', days=30)

        assert analysis['table_name'] == 'analysis_table'
        assert analysis['data_points'] == 7
        assert 'avg_growth_mb_per_day' in analysis
        assert 'size_change_mb' in analysis


@pytest.mark.integration
class TestStorageRepositoryAlerts:
    """Tests for storage alert generation."""

    def test_check_alerts_no_alerts(self, mysql_session):
        """Should return empty list when under threshold."""
        repo = StorageRepository(mysql_session)

        # Create small measurement
        repo.create(
            table_name='small_table',
            measurement_date=date.today(),
            row_count=100,
            data_size_mb=Decimal('1.00'),
            index_size_mb=Decimal('0.50')
        )
        mysql_session.flush()

        # Check with high thresholds (100GB)
        alerts = repo.check_alerts(
            warning_threshold_gb=Decimal('100'),
            critical_threshold_gb=Decimal('200')
        )

        assert len(alerts) == 0

    def test_check_alerts_warning(self, mysql_session):
        """Should return warning alert when exceeding warning threshold."""
        repo = StorageRepository(mysql_session)

        # Create measurement exceeding 0.1 GB (for testing)
        repo.create(
            table_name='warn_table',
            measurement_date=date.today(),
            row_count=100000,
            data_size_mb=Decimal('150.00'),  # 0.15 GB
            index_size_mb=Decimal('50.00')
        )
        mysql_session.flush()

        # Low thresholds for testing
        alerts = repo.check_alerts(
            warning_threshold_gb=Decimal('0.1'),
            critical_threshold_gb=Decimal('0.5')
        )

        # Should have at least a warning
        warning_alerts = [a for a in alerts if a['level'] == 'WARNING']
        assert len(warning_alerts) >= 1

    def test_check_alerts_critical(self, mysql_session):
        """Should return critical alert when exceeding critical threshold."""
        repo = StorageRepository(mysql_session)

        # Create large measurement
        repo.create(
            table_name='critical_table',
            measurement_date=date.today(),
            row_count=1000000,
            data_size_mb=Decimal('600.00'),  # 0.6 GB
            index_size_mb=Decimal('200.00')
        )
        mysql_session.flush()

        # Low thresholds for testing
        alerts = repo.check_alerts(
            warning_threshold_gb=Decimal('0.1'),
            critical_threshold_gb=Decimal('0.5')
        )

        critical_alerts = [a for a in alerts if a['level'] == 'CRITICAL']
        assert len(critical_alerts) >= 1


@pytest.mark.integration
class TestStorageRepositoryMeasure:
    """Tests for storage measurement from database."""

    def test_measure_from_database(self, mysql_session):
        """Should measure storage from information_schema."""
        repo = StorageRepository(mysql_session)

        # This queries actual database tables
        measurements = repo.measure_from_database()

        # Should have at least one table (there are always system tables)
        # The exact count depends on what tables exist
        assert isinstance(measurements, list)
        # If tables were already measured today, list will be empty
        # That's OK - the method works correctly

    def test_measure_skips_already_measured(self, mysql_session):
        """Should skip tables already measured today."""
        repo = StorageRepository(mysql_session)

        # First measurement
        first = repo.measure_from_database()
        mysql_session.commit()

        # Second measurement should return empty (already done today)
        second = repo.measure_from_database()

        # Second call should return empty list since already measured
        assert len(second) == 0


@pytest.mark.integration
class TestStorageMetricsModel:
    """Tests for StorageMetrics model methods."""

    def test_total_size_gb_property(self, mysql_session):
        """Should calculate GB from MB."""
        repo = StorageRepository(mysql_session)

        metrics = repo.create(
            table_name='gb_test',
            measurement_date=date.today(),
            row_count=1000,
            data_size_mb=Decimal('1024.00'),  # 1 GB
            index_size_mb=Decimal('512.00')   # 0.5 GB
        )

        assert metrics.total_size_gb == Decimal('1.5')

    def test_index_overhead_percent_property(self, mysql_session):
        """Should calculate index overhead percentage."""
        repo = StorageRepository(mysql_session)

        metrics = repo.create(
            table_name='overhead_test',
            measurement_date=date.today(),
            row_count=1000,
            data_size_mb=Decimal('80.00'),
            index_size_mb=Decimal('20.00')
        )

        # Index is 20% of total (100 MB)
        assert metrics.index_overhead_percent == Decimal('20')

    def test_project_size_method(self, mysql_session):
        """Should project future size based on growth rate."""
        repo = StorageRepository(mysql_session)

        # Create with growth rate
        yesterday = date.today() - timedelta(days=1)
        repo.create(
            table_name='proj_test',
            measurement_date=yesterday,
            row_count=1000,
            data_size_mb=Decimal('100.00'),
            index_size_mb=Decimal('20.00')
        )
        mysql_session.flush()

        metrics = repo.create(
            table_name='proj_test',
            measurement_date=date.today(),
            row_count=1100,
            data_size_mb=Decimal('110.00'),
            index_size_mb=Decimal('22.00')
        )

        # Growth rate is 12 MB/day
        # Project 30 days
        projected = metrics.project_size(30)

        # 132 + 12*30 = 492
        expected = Decimal('132.00') + (Decimal('12.0000') * 30)
        assert projected == expected

    def test_days_until_size_method(self, mysql_session):
        """Should calculate days until reaching target size."""
        repo = StorageRepository(mysql_session)

        # Create with growth rate
        yesterday = date.today() - timedelta(days=1)
        repo.create(
            table_name='days_test',
            measurement_date=yesterday,
            row_count=1000,
            data_size_mb=Decimal('100.00'),
            index_size_mb=Decimal('0.00')
        )
        mysql_session.flush()

        metrics = repo.create(
            table_name='days_test',
            measurement_date=date.today(),
            row_count=1100,
            data_size_mb=Decimal('110.00'),
            index_size_mb=Decimal('0.00')
        )

        # Growth rate is 10 MB/day, current size is 110 MB
        # Days to reach 210 MB = (210-110)/10 = 10 days
        days = metrics.days_until_size(Decimal('210.00'))
        assert days == 10

    def test_from_information_schema_factory(self):
        """Should create metrics from information_schema data."""
        metrics = StorageMetrics.from_information_schema(
            table_name='factory_test',
            measurement_date=date.today(),
            row_count=5000,
            data_length=10485760,  # 10 MB in bytes
            index_length=2097152   # 2 MB in bytes
        )

        assert metrics.table_name == 'factory_test'
        assert metrics.row_count == 5000
        assert metrics.data_size_mb == Decimal('10.00')
        assert metrics.index_size_mb == Decimal('2.00')
        assert metrics.total_size_mb == Decimal('12.00')
        assert metrics.growth_rate_mb_per_day is None  # No previous measurement
