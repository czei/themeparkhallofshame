"""
Repository: Storage Metrics
CRUD operations for StorageMetrics model.
Feature: 004-themeparks-data-collection
"""

from typing import Optional, List, Dict, Any
from datetime import date, timedelta
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy import select, func, and_, text

from models.orm_storage import StorageMetrics


class StorageRepository:
    """Repository for StorageMetrics CRUD operations."""

    def __init__(self, session: Session):
        self.session = session

    def create(
        self,
        table_name: str,
        measurement_date: date,
        row_count: int,
        data_size_mb: Decimal,
        index_size_mb: Decimal,
        partition_count: Optional[int] = None
    ) -> StorageMetrics:
        """
        Create a new storage metrics entry.

        Args:
            table_name: Name of the table
            measurement_date: Date of measurement
            row_count: Number of rows
            data_size_mb: Data size in MB
            index_size_mb: Index size in MB
            partition_count: Number of partitions (optional)

        Returns:
            Created StorageMetrics instance
        """
        # Get previous measurement for growth rate calculation
        previous = self.get_latest(table_name)
        total_size_mb = data_size_mb + index_size_mb

        growth_rate = None
        if previous:
            days_diff = (measurement_date - previous.measurement_date).days
            if days_diff > 0:
                growth_rate = (total_size_mb - previous.total_size_mb) / Decimal(str(days_diff))

        metrics = StorageMetrics(
            table_name=table_name,
            measurement_date=measurement_date,
            row_count=row_count,
            data_size_mb=data_size_mb,
            index_size_mb=index_size_mb,
            total_size_mb=total_size_mb,
            growth_rate_mb_per_day=growth_rate.quantize(Decimal('0.0001')) if growth_rate else None,
            partition_count=partition_count
        )
        self.session.add(metrics)
        self.session.flush()
        return metrics

    def get_by_id(self, metric_id: int) -> Optional[StorageMetrics]:
        """Get metrics by ID."""
        return self.session.get(StorageMetrics, metric_id)

    def get_latest(self, table_name: str) -> Optional[StorageMetrics]:
        """Get most recent metrics for a table."""
        stmt = select(StorageMetrics).where(
            StorageMetrics.table_name == table_name
        ).order_by(StorageMetrics.measurement_date.desc()).limit(1)
        return self.session.execute(stmt).scalar_one_or_none()

    def get_by_date(self, table_name: str, measurement_date: date) -> Optional[StorageMetrics]:
        """Get metrics for a specific table and date."""
        stmt = select(StorageMetrics).where(
            and_(
                StorageMetrics.table_name == table_name,
                StorageMetrics.measurement_date == measurement_date
            )
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_history(
        self,
        table_name: str,
        days: int = 30
    ) -> List[StorageMetrics]:
        """Get historical metrics for a table."""
        cutoff = date.today() - timedelta(days=days)
        stmt = select(StorageMetrics).where(
            and_(
                StorageMetrics.table_name == table_name,
                StorageMetrics.measurement_date >= cutoff
            )
        ).order_by(StorageMetrics.measurement_date)
        return self.session.execute(stmt).scalars().all()

    def get_all_tables_latest(self) -> List[StorageMetrics]:
        """Get most recent metrics for all tables."""
        # Subquery to find max date per table
        subquery = select(
            StorageMetrics.table_name,
            func.max(StorageMetrics.measurement_date).label('max_date')
        ).group_by(StorageMetrics.table_name).subquery()

        stmt = select(StorageMetrics).join(
            subquery,
            and_(
                StorageMetrics.table_name == subquery.c.table_name,
                StorageMetrics.measurement_date == subquery.c.max_date
            )
        ).order_by(StorageMetrics.total_size_mb.desc())

        return self.session.execute(stmt).scalars().all()

    def get_total_storage(self) -> Dict[str, Any]:
        """Get total storage across all tables."""
        latest = self.get_all_tables_latest()
        total_data = sum(m.data_size_mb for m in latest)
        total_index = sum(m.index_size_mb for m in latest)
        total_rows = sum(m.row_count for m in latest)
        total_growth = sum(m.growth_rate_mb_per_day or Decimal('0') for m in latest)

        return {
            'total_data_mb': total_data,
            'total_index_mb': total_index,
            'total_size_mb': total_data + total_index,
            'total_rows': total_rows,
            'total_growth_mb_per_day': total_growth,
            'table_count': len(latest)
        }

    def project_storage(self, days: int = 365) -> Dict[str, Any]:
        """
        Project future storage requirements.

        Args:
            days: Number of days to project

        Returns:
            Dict with projections
        """
        current = self.get_total_storage()
        total_size = current['total_size_mb']
        growth_rate = current['total_growth_mb_per_day']

        # Project forward
        projected_size = total_size + (growth_rate * Decimal(str(days)))

        return {
            'current_size_mb': total_size,
            'current_size_gb': total_size / Decimal('1024'),
            'growth_rate_mb_per_day': growth_rate,
            'growth_rate_gb_per_month': (growth_rate * Decimal('30')) / Decimal('1024'),
            'projected_days': days,
            'projected_size_mb': projected_size,
            'projected_size_gb': projected_size / Decimal('1024')
        }

    def get_growth_analysis(self, table_name: str, days: int = 30) -> Dict[str, Any]:
        """
        Analyze growth trends for a table.

        Args:
            table_name: Table to analyze
            days: Number of days to analyze

        Returns:
            Dict with growth analysis
        """
        history = self.get_history(table_name, days)
        if len(history) < 2:
            return {
                'table_name': table_name,
                'data_points': len(history),
                'analysis': 'Insufficient data for analysis'
            }

        first = history[0]
        last = history[-1]
        days_span = (last.measurement_date - first.measurement_date).days

        if days_span == 0:
            return {
                'table_name': table_name,
                'data_points': len(history),
                'analysis': 'Insufficient time span for analysis'
            }

        # Calculate growth
        size_change = last.total_size_mb - first.total_size_mb
        row_change = last.row_count - first.row_count
        avg_growth_mb = size_change / Decimal(str(days_span))
        avg_growth_rows = row_change / days_span

        return {
            'table_name': table_name,
            'data_points': len(history),
            'period_days': days_span,
            'size_change_mb': size_change,
            'row_change': row_change,
            'avg_growth_mb_per_day': avg_growth_mb,
            'avg_growth_rows_per_day': avg_growth_rows,
            'current_size_mb': last.total_size_mb,
            'current_rows': last.row_count,
            'partition_count': last.partition_count
        }

    def check_alerts(
        self,
        warning_threshold_gb: Decimal = Decimal('50'),
        critical_threshold_gb: Decimal = Decimal('80')
    ) -> List[Dict[str, Any]]:
        """
        Check for storage alerts.

        Args:
            warning_threshold_gb: Warning threshold in GB
            critical_threshold_gb: Critical threshold in GB

        Returns:
            List of alert dicts
        """
        alerts = []
        total = self.get_total_storage()
        total_gb = total['total_size_mb'] / Decimal('1024')

        if total_gb >= critical_threshold_gb:
            alerts.append({
                'level': 'CRITICAL',
                'message': f'Total storage ({total_gb:.2f} GB) exceeds critical threshold ({critical_threshold_gb} GB)',
                'current_gb': float(total_gb),
                'threshold_gb': float(critical_threshold_gb)
            })
        elif total_gb >= warning_threshold_gb:
            alerts.append({
                'level': 'WARNING',
                'message': f'Total storage ({total_gb:.2f} GB) exceeds warning threshold ({warning_threshold_gb} GB)',
                'current_gb': float(total_gb),
                'threshold_gb': float(warning_threshold_gb)
            })

        # Check growth rate
        growth_rate = total['total_growth_mb_per_day']
        if growth_rate > Decimal('100'):  # >100 MB/day
            alerts.append({
                'level': 'WARNING',
                'message': f'High growth rate: {growth_rate:.2f} MB/day',
                'growth_rate_mb_per_day': float(growth_rate)
            })

        return alerts

    def measure_from_database(self) -> List[StorageMetrics]:
        """
        Measure current storage from MySQL information_schema.

        Returns:
            List of created StorageMetrics instances
        """
        today = date.today()
        created = []

        # Query information_schema for table sizes
        result = self.session.execute(text("""
            SELECT
                table_name,
                table_rows,
                data_length,
                index_length
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_type = 'BASE TABLE'
            ORDER BY (data_length + index_length) DESC
        """))
        tables = result.fetchall()

        # Batch query: Get all partition counts in a single query (fixes N+1 pattern)
        partition_result = self.session.execute(text("""
            SELECT table_name, COUNT(*) as partition_count
            FROM information_schema.partitions
            WHERE table_schema = DATABASE()
            AND partition_name IS NOT NULL
            GROUP BY table_name
        """))
        partition_counts = {row[0]: row[1] for row in partition_result.fetchall()}

        for row in tables:
            table_name = row[0]
            row_count = row[1] or 0
            data_length = row[2] or 0
            index_length = row[3] or 0

            # Get partition count from batch query result
            partition_count = partition_counts.get(table_name)

            # Check if already measured today
            existing = self.get_by_date(table_name, today)
            if existing:
                continue

            metrics = self.create(
                table_name=table_name,
                measurement_date=today,
                row_count=row_count,
                data_size_mb=Decimal(str(data_length)) / Decimal('1048576'),
                index_size_mb=Decimal(str(index_length)) / Decimal('1048576'),
                partition_count=partition_count
            )
            created.append(metrics)

        return created
