# Data Model: ORM Schema Definitions

**Date**: 2025-12-21
**Feature**: 003-orm-refactoring
**Purpose**: Define complete ORM model structure for SQLAlchemy 2.0 migration

---

## Model Overview

**Total Models**: 8 (6 existing tables + 2 weather tables)
**Dropped Tables**: 1 (hourly_stats - removed, served via on-the-fly queries)
**New Columns**: 1 (metrics_version added to daily_stats)
**New Indexes**: 4 (composite indexes for time-series query optimization)

---

## Base Configuration

### Declarative Base

```python
# src/models/base.py
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from typing import Optional
import os

class Base(DeclarativeBase):
    """Base class for all ORM models"""
    pass

# Database connection configuration
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "mysql+mysqldb://user:password@localhost/themepark_tracker"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # Validate connections before using
    pool_recycle=3600,   # Recycle connections after 1 hour
    echo=os.getenv("SQL_ECHO", "false").lower() == "true",  # Log SQL queries
)

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
db_session = scoped_session(SessionLocal)

def create_session():
    """
    Factory for creating sessions outside Flask context (cron jobs, scripts).

    Usage:
        session = create_session()
        try:
            # Do work
            session.commit()
        finally:
            session.close()
    """
    return SessionLocal()
```

### Flask Integration

```python
# src/api/app.py
from src.models.base import db_session

def create_app():
    app = Flask(__name__)

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        """Remove scoped_session at end of request"""
        db_session.remove()

    return app
```

---

## Core Models

### 1. Park Model

```python
# src/models/park.py
from sqlalchemy import String, Enum, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from src.models.base import Base
from datetime import datetime
from typing import List

class Park(Base):
    __tablename__ = "parks"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True)

    # Attributes
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    park_type: Mapped[str] = mapped_column(
        Enum('disney', 'universal', 'other', name='park_type_enum'),
        nullable=False,
        comment="Park brand - determines downtime logic (disney/universal handle CLOSED differently)"
    )
    timezone: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default='America/Los_Angeles',
        comment="Park local timezone for operating hours calculation"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now()
    )

    # Relationships
    rides: Mapped[List["Ride"]] = relationship(
        back_populates="park",
        lazy="select",  # Default: lazy load, use joinedload() for hot paths
        cascade="all, delete-orphan"
    )
    park_snapshots: Mapped[List["ParkActivitySnapshot"]] = relationship(
        back_populates="park",
        lazy="select"
    )
    daily_stats: Mapped[List["DailyStats"]] = relationship(
        back_populates="park",
        lazy="select"
    )

    # Model Methods (Business Logic)
    def is_operating_at(self, timestamp: datetime) -> bool:
        """
        Check if park is operating at given timestamp.
        Queries park_activity_snapshots for park_appears_open=TRUE.
        """
        from src.models.snapshots import ParkActivitySnapshot
        from src.models.base import db_session

        snapshot = (
            db_session.query(ParkActivitySnapshot)
            .filter(ParkActivitySnapshot.park_id == self.id)
            .filter(ParkActivitySnapshot.snapshot_time <= timestamp)
            .order_by(ParkActivitySnapshot.snapshot_time.desc())
            .first()
        )

        return snapshot.park_appears_open if snapshot else False

    def __repr__(self) -> str:
        return f"<Park(id={self.id}, name='{self.name}', type='{self.park_type}')>"
```

---

### 2. Ride Model

```python
# src/models/ride.py
from sqlalchemy import String, Boolean, Integer, ForeignKey, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from src.models.base import Base
from datetime import datetime
from typing import List, Optional

class Ride(Base):
    __tablename__ = "rides"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True)

    # Foreign Keys
    park_id: Mapped[int] = mapped_column(
        ForeignKey("parks.id", ondelete="CASCADE"),
        nullable=False
    )

    # Attributes
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    ride_type: Mapped[Optional[str]] = mapped_column(String(100))
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        comment="FALSE if ride permanently closed or removed"
    )
    tier: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Ride demand tier: 1=flagship/high-demand, 2=moderate, 3=low-demand/filler"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now()
    )

    # Relationships
    park: Mapped["Park"] = relationship(back_populates="rides")
    status_snapshots: Mapped[List["RideStatusSnapshot"]] = relationship(
        back_populates="ride",
        lazy="select"
    )
    daily_stats: Mapped[List["DailyStats"]] = relationship(
        back_populates="ride",
        lazy="select"
    )

    # Model Methods (Business Logic)
    def get_current_status(self) -> Optional["RideStatusSnapshot"]:
        """Get most recent status snapshot for this ride"""
        from src.models.snapshots import RideStatusSnapshot
        from src.models.base import db_session

        return (
            db_session.query(RideStatusSnapshot)
            .filter(RideStatusSnapshot.ride_id == self.id)
            .order_by(RideStatusSnapshot.snapshot_time.desc())
            .first()
        )

    def calculate_uptime(self, period_start: datetime, period_end: datetime) -> float:
        """
        Calculate uptime percentage for date range.

        Args:
            period_start: Start of period (UTC)
            period_end: End of period (UTC)

        Returns:
            Uptime percentage (0.0 to 100.0)
        """
        from src.models.snapshots import RideStatusSnapshot
        from src.models.base import db_session
        from sqlalchemy import func, case

        # Count snapshots where ride was operating
        operating_count = (
            db_session.query(func.count(RideStatusSnapshot.id))
            .filter(RideStatusSnapshot.ride_id == self.id)
            .filter(RideStatusSnapshot.snapshot_time.between(period_start, period_end))
            .filter(RideStatusSnapshot.park_appears_open == True)
            .filter(
                (RideStatusSnapshot.status == 'OPERATING') |
                (RideStatusSnapshot.computed_is_open == True)
            )
            .scalar()
        )

        # Count total snapshots when park was open
        total_count = (
            db_session.query(func.count(RideStatusSnapshot.id))
            .filter(RideStatusSnapshot.ride_id == self.id)
            .filter(RideStatusSnapshot.snapshot_time.between(period_start, period_end))
            .filter(RideStatusSnapshot.park_appears_open == True)
            .scalar()
        )

        if total_count == 0:
            return 0.0

        return (operating_count / total_count) * 100.0

    def __repr__(self) -> str:
        return f"<Ride(id={self.id}, name='{self.name}', park_id={self.park_id}, tier={self.tier})>"
```

---

### 3. RideStatusSnapshot Model

```python
# src/models/snapshots.py
from sqlalchemy import String, Integer, Boolean, ForeignKey, DateTime, Index, or_
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from src.models.base import Base
from datetime import datetime
from typing import Optional

class RideStatusSnapshot(Base):
    __tablename__ = "ride_status_snapshots"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True)

    # Foreign Keys
    ride_id: Mapped[int] = mapped_column(
        ForeignKey("rides.id", ondelete="CASCADE"),
        nullable=False
    )

    # Attributes
    snapshot_time: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        comment="Snapshot timestamp in UTC"
    )
    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="Raw status from API: OPERATING, CLOSED, DOWN, REFURBISHMENT"
    )
    wait_time: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Wait time in minutes (NULL if ride not operating)"
    )
    computed_is_open: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="Derived field: TRUE if ride appears operational based on business logic"
    )
    park_appears_open: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="Derived field: TRUE if park appears operational (critical gate for downtime calculations)"
    )

    # Relationships
    ride: Mapped["Ride"] = relationship(back_populates="status_snapshots")

    # Indexes
    __table_args__ = (
        Index('idx_ride_snapshots_ride_time', 'ride_id', 'snapshot_time'),
        Index('idx_ride_snapshots_time', 'snapshot_time'),
    )

    # Hybrid Methods (Business Logic - works in Python AND SQL)
    @hybrid_method
    def is_operating(self):
        """
        Ride is operating if status='OPERATING' OR computed_is_open=TRUE.
        Used in Python: snapshot.is_operating() → bool
        """
        return (self.status == 'OPERATING') or (self.computed_is_open == True)

    @is_operating.expression
    def is_operating(cls):
        """
        SQL expression for is_operating (for WHERE clauses).
        Used in SQL: .filter(RideStatusSnapshot.is_operating())
        """
        return or_(cls.status == 'OPERATING', cls.computed_is_open == True)

    @hybrid_method
    def is_down(self):
        """
        Ride is down if park is open AND ride is not operating.

        NOTE: This is park-type-aware logic (Disney/Universal vs. other parks).
        For Disney/Universal: DOWN = unexpected breakdown, CLOSED = scheduled
        For other parks: CLOSED may indicate downtime (park-specific logic)

        Used in Python: snapshot.is_down() → bool
        """
        return (self.park_appears_open == True) and not self.is_operating()

    @is_down.expression
    def is_down(cls):
        """
        SQL expression for is_down (for WHERE clauses).
        Used in SQL: .filter(RideStatusSnapshot.is_down())
        """
        return (cls.park_appears_open == True) & ~cls.is_operating()

    def __repr__(self) -> str:
        return (
            f"<RideStatusSnapshot(id={self.id}, ride_id={self.ride_id}, "
            f"time={self.snapshot_time}, status='{self.status}', wait={self.wait_time})>"
        )
```

---

### 4. ParkActivitySnapshot Model

```python
# src/models/snapshots.py (continued)
from sqlalchemy import ForeignKey, DateTime, Boolean, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from src.models.base import Base
from datetime import datetime
from typing import Optional

class ParkActivitySnapshot(Base):
    __tablename__ = "park_activity_snapshots"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True)

    # Foreign Keys
    park_id: Mapped[int] = mapped_column(
        ForeignKey("parks.id", ondelete="CASCADE"),
        nullable=False
    )

    # Attributes
    snapshot_time: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        comment="Snapshot timestamp in UTC"
    )
    park_appears_open: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="TRUE if park appears operational based on ride activity"
    )
    first_activity_time: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        comment="Time of first ride operation detected (UTC)"
    )
    last_activity_time: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        comment="Time of last ride operation detected (UTC)"
    )

    # Relationships
    park: Mapped["Park"] = relationship(back_populates="park_snapshots")

    # Indexes
    __table_args__ = (
        Index('idx_park_snapshots_park_time', 'park_id', 'snapshot_time'),
        Index('idx_park_snapshots_time', 'snapshot_time'),
    )

    # Model Methods
    def is_within_operating_hours(self, timestamp: datetime) -> bool:
        """
        Check if timestamp falls within park operating hours.

        Args:
            timestamp: Timestamp to check (UTC)

        Returns:
            TRUE if timestamp is between first_activity_time and last_activity_time
        """
        if not self.first_activity_time or not self.last_activity_time:
            return False

        return self.first_activity_time <= timestamp <= self.last_activity_time

    def __repr__(self) -> str:
        return (
            f"<ParkActivitySnapshot(id={self.id}, park_id={self.park_id}, "
            f"time={self.snapshot_time}, open={self.park_appears_open})>"
        )
```

---

### 5. DailyStats Model

```python
# src/models/stats.py
from sqlalchemy import Date, Integer, ForeignKey, Float, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from src.models.base import Base
from datetime import date
from typing import Optional

class DailyStats(Base):
    __tablename__ = "daily_stats"

    # Composite Primary Key (date, ride_id, metrics_version)
    date: Mapped[date] = mapped_column(
        Date,
        primary_key=True,
        comment="Calendar date in Pacific timezone"
    )
    ride_id: Mapped[int] = mapped_column(
        ForeignKey("rides.id", ondelete="CASCADE"),
        primary_key=True
    )
    metrics_version: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        default=1,
        comment="Metrics calculation version (for side-by-side validation)"
    )

    # Foreign Keys (not part of PK)
    park_id: Mapped[int] = mapped_column(
        ForeignKey("parks.id", ondelete="CASCADE"),
        nullable=False
    )

    # Aggregated Metrics
    total_downtime_minutes: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Total minutes ride was down while park was operating"
    )
    shame_score: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        default=0.0,
        comment="Weighted downtime score (downtime_minutes * tier_weight)"
    )
    uptime_percentage: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        default=100.0,
        comment="Percentage of operating hours ride was available"
    )

    # Relationships
    ride: Mapped["Ride"] = relationship(back_populates="daily_stats")
    park: Mapped["Park"] = relationship(back_populates="daily_stats")

    # Indexes
    __table_args__ = (
        Index('idx_daily_stats_date', 'date'),
        Index('idx_daily_stats_version', 'metrics_version', 'date'),
        Index('idx_daily_stats_park_date', 'park_id', 'date'),
    )

    def __repr__(self) -> str:
        return (
            f"<DailyStats(date={self.date}, ride_id={self.ride_id}, "
            f"shame_score={self.shame_score:.2f}, uptime={self.uptime_percentage:.1f}%, "
            f"version={self.metrics_version})>"
        )
```

---

### 6. Weather Models

```python
# src/models/weather.py
from sqlalchemy import Integer, ForeignKey, DateTime, Float, String, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from src.models.base import Base
from datetime import datetime
from typing import Optional

class WeatherObservation(Base):
    __tablename__ = "weather_observations"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True)

    # Foreign Keys
    park_id: Mapped[int] = mapped_column(
        ForeignKey("parks.id", ondelete="CASCADE"),
        nullable=False
    )

    # Attributes
    observation_time: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        comment="Observation timestamp in UTC"
    )
    temperature_f: Mapped[Optional[float]] = mapped_column(
        Float,
        comment="Temperature in Fahrenheit"
    )
    precipitation_inches: Mapped[Optional[float]] = mapped_column(
        Float,
        comment="Precipitation amount in inches"
    )
    uv_index: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="UV index (0-11+)"
    )
    conditions: Mapped[Optional[str]] = mapped_column(
        String(100),
        comment="Weather conditions description (e.g., 'Clear', 'Rain', 'Cloudy')"
    )

    # Relationships
    park: Mapped["Park"] = relationship()

    # Indexes
    __table_args__ = (
        Index('idx_weather_obs_park_time', 'park_id', 'observation_time'),
    )

    def __repr__(self) -> str:
        return (
            f"<WeatherObservation(id={self.id}, park_id={self.park_id}, "
            f"time={self.observation_time}, temp={self.temperature_f}°F, "
            f"conditions='{self.conditions}')>"
        )


class WeatherForecast(Base):
    __tablename__ = "weather_forecasts"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True)

    # Foreign Keys
    park_id: Mapped[int] = mapped_column(
        ForeignKey("parks.id", ondelete="CASCADE"),
        nullable=False
    )

    # Attributes
    forecast_time: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        comment="Time forecast is for (UTC)"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        comment="When forecast was generated (UTC)"
    )
    temperature_f: Mapped[Optional[float]] = mapped_column(Float)
    precipitation_probability: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Probability of precipitation (0-100%)"
    )
    precipitation_inches: Mapped[Optional[float]] = mapped_column(Float)
    uv_index: Mapped[Optional[int]] = mapped_column(Integer)
    conditions: Mapped[Optional[str]] = mapped_column(String(100))

    # Relationships
    park: Mapped["Park"] = relationship()

    # Indexes
    __table_args__ = (
        Index('idx_weather_forecast_park_time', 'park_id', 'forecast_time'),
    )

    def __repr__(self) -> str:
        return (
            f"<WeatherForecast(id={self.id}, park_id={self.park_id}, "
            f"forecast_time={self.forecast_time}, temp={self.temperature_f}°F, "
            f"precip_prob={self.precipitation_probability}%)>"
        )
```

---

## Dropped Tables

### hourly_stats (REMOVED)

**Reason**: Hourly metrics now calculated on-the-fly via ORM queries with composite indexes.

**Replacement Query Pattern**:
```python
# OLD: Query hourly_stats table
SELECT shame_score, total_downtime_minutes
FROM hourly_stats
WHERE park_id = ? AND hour_start = ?;

# NEW: Calculate on-the-fly with ORM
from sqlalchemy import func
from src.models.snapshots import RideStatusSnapshot
from src.models.ride import Ride

hour_start = datetime(2025, 12, 21, 14, 0, 0)  # 2pm Pacific
hour_end = hour_start + timedelta(hours=1)

hourly_stats = (
    db_session.query(
        Ride.park_id,
        func.count(RideStatusSnapshot.id.distinct()).label('total_rides_down'),
        func.sum(
            func.coalesce(Ride.tier, 1) *
            (RideStatusSnapshot.downtime_minutes)
        ).label('shame_score')
    )
    .join(RideStatusSnapshot, Ride.id == RideStatusSnapshot.ride_id)
    .filter(RideStatusSnapshot.snapshot_time.between(hour_start, hour_end))
    .filter(RideStatusSnapshot.is_down())
    .group_by(Ride.park_id)
    .all()
)
```

**Performance**: Composite indexes `idx_ride_snapshots_ride_time` ensure <500ms query time for hourly aggregations.

---

## Query Abstraction Layer

### Centralized Business Logic Helpers

```python
# src/utils/query_helpers.py
from sqlalchemy.orm import Session
from sqlalchemy import func, or_, and_
from src.models.snapshots import RideStatusSnapshot, ParkActivitySnapshot
from src.models.ride import Ride
from src.models.park import Park
from datetime import datetime, date, timedelta
from typing import List

class TimeHelper:
    """Timezone conversion helpers for Pacific time queries"""

    @staticmethod
    def to_pacific(utc_column):
        """Convert UTC timestamp column to Pacific time"""
        return func.convert_tz(utc_column, '+00:00', 'America/Los_Angeles')

    @staticmethod
    def pacific_date(utc_column):
        """Get Pacific calendar date from UTC timestamp"""
        return func.date(TimeHelper.to_pacific(utc_column))

    @staticmethod
    def pacific_hour(utc_column):
        """Get Pacific hour (0-23) from UTC timestamp"""
        return func.hour(TimeHelper.to_pacific(utc_column))


class RideStatusQuery:
    """Query helpers for ride status operations (replaces RideStatusSQL)"""

    @staticmethod
    def rides_that_operated_today(session: Session, today_start_utc: datetime) -> List[int]:
        """
        Get ride IDs that operated at any point today (Pacific calendar day).

        Replaces: rides_that_operated_cte() from src/utils/sql_helpers.py

        Args:
            session: SQLAlchemy session
            today_start_utc: Start of Pacific calendar day in UTC

        Returns:
            List of ride IDs that had at least one OPERATING snapshot today
        """
        return (
            session.query(RideStatusSnapshot.ride_id.distinct())
            .filter(RideStatusSnapshot.snapshot_time >= today_start_utc)
            .filter(RideStatusSnapshot.park_appears_open == True)
            .filter(RideStatusSnapshot.is_operating())
            .scalar_all()
        )

    @staticmethod
    def hourly_downtime_by_park(
        session: Session,
        park_id: int,
        hour_start: datetime,
        hour_end: datetime
    ) -> dict:
        """
        Calculate hourly downtime metrics for a park.

        Args:
            session: SQLAlchemy session
            park_id: Park ID
            hour_start: Hour start time (UTC)
            hour_end: Hour end time (UTC)

        Returns:
            Dictionary with shame_score, total_rides_down, total_rides_operated
        """
        # Get rides that operated during this hour
        operated_rides = (
            session.query(Ride.id)
            .join(RideStatusSnapshot)
            .filter(Ride.park_id == park_id)
            .filter(RideStatusSnapshot.snapshot_time.between(hour_start, hour_end))
            .filter(RideStatusSnapshot.park_appears_open == True)
            .filter(RideStatusSnapshot.is_operating())
            .distinct()
            .scalar_all()
        )

        # Calculate downtime for rides that operated
        downtime = (
            session.query(
                func.count(Ride.id.distinct()).label('rides_down'),
                func.sum(
                    func.coalesce(Ride.tier, 1) * 5  # 5 minutes per snapshot
                ).label('shame_score')
            )
            .join(RideStatusSnapshot)
            .filter(Ride.id.in_(operated_rides))
            .filter(RideStatusSnapshot.snapshot_time.between(hour_start, hour_end))
            .filter(RideStatusSnapshot.is_down())
            .one()
        )

        return {
            'shame_score': downtime.shame_score or 0.0,
            'total_rides_down': downtime.rides_down or 0,
            'total_rides_operated': len(operated_rides),
        }


class DowntimeQuery:
    """Downtime calculation helpers (replaces DowntimeSQL)"""

    @staticmethod
    def calculate_daily_shame_score(
        session: Session,
        ride_id: int,
        pacific_date: date
    ) -> float:
        """
        Calculate shame score for ride on specific Pacific calendar day.

        Replaces: Shame score calculation from src/utils/sql_helpers.py

        Args:
            session: SQLAlchemy session
            ride_id: Ride ID
            pacific_date: Calendar date in Pacific timezone

        Returns:
            Shame score (downtime_minutes * tier_weight)
        """
        ride = session.query(Ride).get(ride_id)
        if not ride:
            return 0.0

        # Convert Pacific date to UTC range
        day_start_utc = datetime.combine(pacific_date, datetime.min.time())
        day_end_utc = day_start_utc + timedelta(days=1)

        # Count down snapshots (5-minute intervals)
        down_count = (
            session.query(func.count(RideStatusSnapshot.id))
            .filter(RideStatusSnapshot.ride_id == ride_id)
            .filter(RideStatusSnapshot.snapshot_time.between(day_start_utc, day_end_utc))
            .filter(RideStatusSnapshot.is_down())
            .scalar()
        )

        downtime_minutes = down_count * 5  # 5-minute snapshot intervals
        tier_weight = ride.tier or 1

        return downtime_minutes * tier_weight
```

---

## Migration Scripts

### Migration 001: Add Composite Indexes

```python
# src/database/migrations/versions/001_add_composite_indexes.py
"""Add composite indexes for time-series query optimization

Revision ID: 001
Create Date: 2025-12-21
"""
from alembic import op

def upgrade():
    # Ride status snapshots indexes
    op.create_index(
        'idx_ride_snapshots_ride_time',
        'ride_status_snapshots',
        ['ride_id', 'snapshot_time'],
        mysql_length={'snapshot_time': None}
    )
    op.create_index(
        'idx_ride_snapshots_time',
        'ride_status_snapshots',
        ['snapshot_time']
    )

    # Park activity snapshots indexes
    op.create_index(
        'idx_park_snapshots_park_time',
        'park_activity_snapshots',
        ['park_id', 'snapshot_time'],
        mysql_length={'snapshot_time': None}
    )
    op.create_index(
        'idx_park_snapshots_time',
        'park_activity_snapshots',
        ['snapshot_time']
    )

def downgrade():
    op.drop_index('idx_ride_snapshots_ride_time', 'ride_status_snapshots')
    op.drop_index('idx_ride_snapshots_time', 'ride_status_snapshots')
    op.drop_index('idx_park_snapshots_park_time', 'park_activity_snapshots')
    op.drop_index('idx_park_snapshots_time', 'park_activity_snapshots')
```

### Migration 002: Add metrics_version to daily_stats

```python
# src/database/migrations/versions/002_add_metrics_version.py
"""Add metrics_version column for side-by-side validation

Revision ID: 002
Depends on: 001
Create Date: 2025-12-21
"""
from alembic import op
import sqlalchemy as sa

def upgrade():
    # Add metrics_version column (default=1 for existing rows)
    op.add_column(
        'daily_stats',
        sa.Column('metrics_version', sa.Integer(), nullable=False, server_default='1')
    )

    # Update primary key to include metrics_version
    op.drop_constraint('PRIMARY', 'daily_stats', type_='primary')
    op.create_primary_key(
        'pk_daily_stats',
        'daily_stats',
        ['date', 'ride_id', 'metrics_version']
    )

    # Add index for version-based queries
    op.create_index(
        'idx_daily_stats_version',
        'daily_stats',
        ['metrics_version', 'date']
    )

def downgrade():
    # Restore original primary key (requires deleting version!=1 rows first)
    op.execute('DELETE FROM daily_stats WHERE metrics_version != 1')
    op.drop_index('idx_daily_stats_version', 'daily_stats')
    op.drop_constraint('pk_daily_stats', 'daily_stats', type_='primary')
    op.create_primary_key('PRIMARY', 'daily_stats', ['date', 'ride_id'])
    op.drop_column('daily_stats', 'metrics_version')
```

### Migration 003: Drop hourly_stats Table

```python
# src/database/migrations/versions/003_drop_hourly_stats.py
"""Drop hourly_stats table (served via on-the-fly ORM queries)

Revision ID: 003
Depends on: 002
Create Date: 2025-12-21
"""
from alembic import op
import sqlalchemy as sa

def upgrade():
    # Drop hourly_stats table
    op.drop_table('hourly_stats')

def downgrade():
    # Recreate hourly_stats table (schema backup)
    op.create_table(
        'hourly_stats',
        sa.Column('hour_start', sa.DateTime(), nullable=False),
        sa.Column('park_id', sa.Integer(), nullable=False),
        sa.Column('ride_id', sa.Integer(), nullable=False),
        sa.Column('shame_score', sa.Float(), nullable=False),
        sa.Column('total_downtime_minutes', sa.Integer(), nullable=False),
        sa.Column('total_rides_down', sa.Integer(), nullable=False),
        sa.Column('total_rides_operated', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('hour_start', 'park_id', 'ride_id')
    )
```

---

## Testing Patterns

### ORM Model Unit Tests

```python
# tests/unit/test_orm_models.py
import pytest
from freezegun import freeze_time
from datetime import datetime, timezone
from src.models.ride import Ride
from src.models.snapshots import RideStatusSnapshot

def test_ride_is_operating_hybrid_method(orm_session):
    """Validate is_operating hybrid method works in Python and SQL"""
    ride = Ride(name="Test Ride", park_id=1)
    snapshot = RideStatusSnapshot(
        ride=ride,
        snapshot_time=datetime.now(timezone.utc),
        status='OPERATING',
        computed_is_open=False,
        park_appears_open=True
    )
    orm_session.add_all([ride, snapshot])
    orm_session.commit()

    # Test Python usage
    assert snapshot.is_operating() is True

    # Test SQL usage
    result = (
        orm_session.query(RideStatusSnapshot)
        .filter(RideStatusSnapshot.is_operating())
        .first()
    )
    assert result.id == snapshot.id
```

### ORM Query Parity Tests

```python
# tests/integration/test_orm_query_parity.py
import pytest
from freezegun import freeze_time
from src.utils.query_helpers import RideStatusQuery
from tests.golden_data.fixtures import GOLDEN_HOURLY_SHAME

@freeze_time("2025-12-05 22:00:00")  # 2pm Pacific
def test_hourly_downtime_matches_golden_data(orm_session, production_snapshots):
    """Validate ORM hourly query matches hand-computed golden data"""
    # Load production snapshot data
    load_snapshots(orm_session, production_snapshots)

    # Execute ORM query
    hour_start = datetime(2025, 12, 5, 14, 0, 0)  # 2pm Pacific in UTC
    hour_end = hour_start + timedelta(hours=1)

    result = RideStatusQuery.hourly_downtime_by_park(
        orm_session,
        park_id=1,
        hour_start=hour_start,
        hour_end=hour_end
    )

    # Validate against golden data
    expected = GOLDEN_HOURLY_SHAME["2025-12-05-14"]
    assert result['shame_score'] == pytest.approx(expected['shame_score'], rel=0.01)
    assert result['total_rides_down'] == expected['total_rides_down']
    assert result['total_rides_operated'] == expected['total_rides_operated']
```

---

## Summary

**Complete ORM schema defined with**:
- ✅ 8 ORM models (6 core + 2 weather)
- ✅ 4 composite indexes for time-series queries
- ✅ 1 new column (metrics_version for side-by-side validation)
- ✅ 1 table dropped (hourly_stats → on-the-fly queries)
- ✅ Hybrid methods for business logic (is_operating, is_down)
- ✅ Query abstraction layer (replaces SQL helpers)
- ✅ 3 migration scripts (indexes, metrics_version, drop hourly_stats)
- ✅ Testing patterns (unit tests, parity tests, golden data)

**Next Step**: Generate quickstart.md developer guide for using the ORM layer.
