"""
SQLAlchemy ORM Models: Storage Metrics
Database storage tracking for capacity planning.
Feature: 004-themeparks-data-collection
"""

from sqlalchemy import Integer, BigInteger, String, DateTime, Date, Numeric, UniqueConstraint, Index, func
from sqlalchemy.orm import Mapped, mapped_column
from models.base import Base
from datetime import datetime, date
from typing import Optional
from decimal import Decimal


class StorageMetrics(Base):
    """
    Database storage tracking for capacity planning.

    Stores daily measurements of table sizes, row counts, and growth rates
    to enable storage projections and capacity alerts.
    """
    __tablename__ = "storage_metrics"

    # Primary Key
    metric_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Table identification
    table_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="Name of the table being measured"
    )

    # Measurement date
    measurement_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        comment="Date of measurement"
    )

    # Size metrics
    row_count: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        comment="Number of rows in table"
    )
    data_size_mb: Mapped[Decimal] = mapped_column(
        Numeric(12, 2),
        nullable=False,
        comment="Data size in megabytes"
    )
    index_size_mb: Mapped[Decimal] = mapped_column(
        Numeric(12, 2),
        nullable=False,
        comment="Index size in megabytes"
    )
    total_size_mb: Mapped[Decimal] = mapped_column(
        Numeric(12, 2),
        nullable=False,
        comment="Total size (data + index) in megabytes"
    )

    # Growth rate
    growth_rate_mb_per_day: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 4),
        nullable=True,
        comment="Calculated growth rate in MB per day"
    )

    # Partition info
    partition_count: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        comment="Number of partitions (if table is partitioned)"
    )

    # Timestamp
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now()
    )

    # Constraints and indexes
    __table_args__ = (
        UniqueConstraint('table_name', 'measurement_date', name='unique_table_date'),
        Index('idx_storage_date', 'measurement_date'),
        Index('idx_storage_table', 'table_name', 'measurement_date'),
        {'extend_existing': True}
    )

    @property
    def total_size_gb(self) -> Decimal:
        """Return total size in gigabytes."""
        return self.total_size_mb / Decimal('1024')

    @property
    def index_overhead_percent(self) -> Decimal:
        """Return index size as percentage of total."""
        if self.total_size_mb == 0:
            return Decimal('0')
        return (self.index_size_mb / self.total_size_mb) * Decimal('100')

    def project_size(self, days: int) -> Decimal:
        """
        Project future size based on growth rate.

        Args:
            days: Number of days into the future

        Returns:
            Projected total size in MB
        """
        if self.growth_rate_mb_per_day is None:
            return self.total_size_mb
        return self.total_size_mb + (self.growth_rate_mb_per_day * days)

    def days_until_size(self, target_mb: Decimal) -> Optional[int]:
        """
        Calculate days until reaching target size.

        Args:
            target_mb: Target size in megabytes

        Returns:
            Number of days until target, or None if not growing
        """
        if self.growth_rate_mb_per_day is None or self.growth_rate_mb_per_day <= 0:
            return None
        remaining = target_mb - self.total_size_mb
        if remaining <= 0:
            return 0
        return int(remaining / self.growth_rate_mb_per_day)

    @classmethod
    def from_information_schema(
        cls,
        table_name: str,
        measurement_date: date,
        row_count: int,
        data_length: int,
        index_length: int,
        previous_measurement: Optional["StorageMetrics"] = None
    ) -> "StorageMetrics":
        """
        Create StorageMetrics from MySQL information_schema data.

        Args:
            table_name: Name of the table
            measurement_date: Date of measurement
            row_count: Number of rows
            data_length: Data size in bytes
            index_length: Index size in bytes
            previous_measurement: Previous day's measurement for growth rate

        Returns:
            StorageMetrics instance
        """
        data_mb = Decimal(str(data_length)) / Decimal('1048576')  # bytes to MB
        index_mb = Decimal(str(index_length)) / Decimal('1048576')
        total_mb = data_mb + index_mb

        growth_rate = None
        if previous_measurement:
            days_diff = (measurement_date - previous_measurement.measurement_date).days
            if days_diff > 0:
                growth_rate = (total_mb - previous_measurement.total_size_mb) / Decimal(str(days_diff))

        return cls(
            table_name=table_name,
            measurement_date=measurement_date,
            row_count=row_count,
            data_size_mb=data_mb.quantize(Decimal('0.01')),
            index_size_mb=index_mb.quantize(Decimal('0.01')),
            total_size_mb=total_mb.quantize(Decimal('0.01')),
            growth_rate_mb_per_day=growth_rate.quantize(Decimal('0.0001')) if growth_rate else None
        )

    def __repr__(self) -> str:
        return f"<StorageMetrics(table='{self.table_name}', date={self.measurement_date}, size={self.total_size_mb}MB)>"
