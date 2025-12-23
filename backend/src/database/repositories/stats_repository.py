"""
Theme Park Downtime Tracker - Statistics Repository
Provides data access layer for aggregate statistics tables using SQLAlchemy ORM.

MIGRATION NOTE
==============
This is a minimal ORM migration. Most query methods have been migrated to
modular query classes in database/queries/ and are NOT included here.

For new development, use the query classes in database/queries/:
    - Rankings: queries/rankings/
    - Trends: queries/trends/
    - Charts: queries/charts/
    - Live data: queries/live/
    - Today data: queries/today/
    - Yesterday data: queries/yesterday/

Methods Implemented (still in use):
- get_aggregate_park_stats()
- get_park_tier_distribution()
- get_park_operating_sessions()
- get_park_current_status()
- get_last_aggregation_status()
- check_aggregation_health()

All other methods have been migrated to modular query classes or are deprecated.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, text, distinct

from src.models.orm_park import Park
from src.models.orm_ride import Ride
from src.models.orm_stats import ParkDailyStats
from src.models.orm_snapshots import RideStatusSnapshot, ParkActivitySnapshot


class StatsRepository:
    """
    Repository for statistics queries using SQLAlchemy ORM.

    Note: This is a minimal implementation. Most query methods have been
    migrated to modular query classes in database/queries/.
    """

    def __init__(self, session: Session):
        """
        Initialize repository with SQLAlchemy session.

        Args:
            session: SQLAlchemy session object
        """
        self.session = session

    def get_aggregate_park_stats(
        self,
        park_id: int,
        period: str = "daily",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get aggregated statistics for a park over a period.

        Args:
            park_id: Park ID
            period: "daily", "weekly", or "monthly"
            start_date: Start date (YYYY-MM-DD) or None
            end_date: End date (YYYY-MM-DD) or None

        Returns:
            Dictionary with aggregated statistics
        """
        park = self.session.query(Park).filter(Park.park_id == park_id).first()
        if not park:
            return {}

        # Determine which stats table to query
        if period == "daily":
            stats_model = ParkDailyStats
            date_field = ParkDailyStats.stat_date
        else:
            # Weekly/monthly aggregation not implemented yet
            raise NotImplementedError(f"Period '{period}' aggregation not yet implemented in ORM")

        # Build query with optional date filtering
        query = self.session.query(
            func.sum(stats_model.total_downtime_hours).label('total_downtime'),
            func.avg(stats_model.avg_uptime_percentage).label('avg_uptime'),
            func.sum(stats_model.rides_with_downtime).label('total_affected_rides'),
            func.count(distinct(date_field)).label('days_tracked')
        ).filter(stats_model.park_id == park_id)

        if start_date:
            query = query.filter(date_field >= start_date)
        if end_date:
            query = query.filter(date_field <= end_date)

        result = query.first()

        if not result:
            return {}

        return {
            'park_id': park_id,
            'park_name': park.name,
            'period': period,
            'total_downtime_hours': float(result.total_downtime or 0),
            'avg_uptime_percentage': float(result.avg_uptime or 0),
            'total_affected_rides': int(result.total_affected_rides or 0),
            'days_tracked': int(result.days_tracked or 0)
        }

    def get_park_tier_distribution(self, park_id: int) -> Dict[str, Any]:
        """
        Get distribution of rides by tier for a park.

        Args:
            park_id: Park ID

        Returns:
            Dictionary with tier distribution
        """
        park = self.session.query(Park).filter(Park.park_id == park_id).first()
        if not park:
            return {}

        # Count rides by tier
        tier_counts = (
            self.session.query(
                Ride.tier,
                func.count(Ride.ride_id).label('count')
            )
            .filter(
                and_(
                    Ride.park_id == park_id,
                    Ride.is_active.is_(True)
                )
            )
            .group_by(Ride.tier)
            .all()
        )

        distribution = {
            'park_id': park_id,
            'park_name': park.name,
            'tier_1_count': 0,
            'tier_2_count': 0,
            'tier_3_count': 0,
            'unclassified_count': 0,
            'total_rides': 0
        }

        for tier, count in tier_counts:
            if tier == 1:
                distribution['tier_1_count'] = count
            elif tier == 2:
                distribution['tier_2_count'] = count
            elif tier == 3:
                distribution['tier_3_count'] = count
            else:
                distribution['unclassified_count'] = count

            distribution['total_rides'] += count

        return distribution

    def get_park_operating_sessions(
        self,
        park_id: int,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get park operating sessions (when park was open).

        Args:
            park_id: Park ID
            start_date: Start date (YYYY-MM-DD) or None for 30 days ago
            end_date: End date (YYYY-MM-DD) or None for today

        Returns:
            List of operating session dictionaries
        """
        # Query park_activity_snapshots to find when park was open
        query = (
            self.session.query(
                func.date(ParkActivitySnapshot.recorded_at).label('date'),
                func.min(ParkActivitySnapshot.recorded_at).label('first_open'),
                func.max(ParkActivitySnapshot.recorded_at).label('last_open'),
                func.count(ParkActivitySnapshot.snapshot_id).label('snapshots')
            )
            .filter(
                and_(
                    ParkActivitySnapshot.park_id == park_id,
                    ParkActivitySnapshot.park_appears_open.is_(True)
                )
            )
            .group_by(func.date(ParkActivitySnapshot.recorded_at))
            .order_by(func.date(ParkActivitySnapshot.recorded_at).desc())
        )

        # Apply date filters
        if start_date:
            query = query.filter(func.date(ParkActivitySnapshot.recorded_at) >= start_date)
        else:
            # Default to 30 days ago
            query = query.filter(
                func.date(ParkActivitySnapshot.recorded_at) >= func.date_sub(func.curdate(), text('INTERVAL 30 DAY'))
            )

        if end_date:
            query = query.filter(func.date(ParkActivitySnapshot.recorded_at) <= end_date)

        query = query.limit(100)

        results = query.all()

        return [
            {
                'park_id': park_id,
                'date': str(row.date),
                'first_open_at': row.first_open.isoformat() if row.first_open else None,
                'last_open_at': row.last_open.isoformat() if row.last_open else None,
                'operating_hours': (row.last_open - row.first_open).total_seconds() / 3600.0 if row.first_open and row.last_open else 0,
                'snapshot_count': row.snapshots
            }
            for row in results
        ]

    def get_park_current_status(self, park_id: int) -> Dict[str, Any]:
        """
        Get current operating status for a park.

        Args:
            park_id: Park ID

        Returns:
            Dictionary with current status
        """
        park = self.session.query(Park).filter(Park.park_id == park_id).first()
        if not park:
            return {}

        # Get most recent park activity snapshot
        latest_park_snapshot = (
            self.session.query(ParkActivitySnapshot)
            .filter(ParkActivitySnapshot.park_id == park_id)
            .order_by(ParkActivitySnapshot.recorded_at.desc())
            .first()
        )

        if not latest_park_snapshot:
            return {
                'park_id': park_id,
                'park_name': park.name,
                'is_open': False,
                'last_updated': None,
                'rides_open': 0,
                'rides_closed': 0,
                'total_rides': 0
            }

        # Count active rides
        total_rides = (
            self.session.query(func.count(Ride.ride_id))
            .filter(
                and_(
                    Ride.park_id == park_id,
                    Ride.is_active.is_(True)
                )
            )
            .scalar() or 0
        )

        return {
            'park_id': park_id,
            'park_name': park.name,
            'is_open': latest_park_snapshot.park_appears_open,
            'last_updated': latest_park_snapshot.recorded_at.isoformat() if latest_park_snapshot.recorded_at else None,
            'rides_open': latest_park_snapshot.rides_open,
            'rides_closed': latest_park_snapshot.rides_closed,
            'total_rides': total_rides,
            'avg_wait_time': float(latest_park_snapshot.avg_wait_time) if latest_park_snapshot.avg_wait_time else None,
            'max_wait_time': latest_park_snapshot.max_wait_time
        }

    def get_last_aggregation_status(self) -> Dict[str, Any]:
        """
        Get status of last hourly and daily aggregation runs.

        Note: Hourly aggregation status requires ParkHourlyStats ORM model (not yet implemented).
        Currently only returns daily aggregation status.

        Returns:
            Dictionary with aggregation status
        """
        # Get latest daily aggregation
        latest_daily = (
            self.session.query(func.max(ParkDailyStats.stat_date))
            .scalar()
        )

        # Calculate daily lag
        now = datetime.utcnow()
        daily_lag_hours = None
        if latest_daily:
            daily_lag = now - datetime.combine(latest_daily, datetime.min.time())
            daily_lag_hours = daily_lag.total_seconds() / 3600.0

        # TODO: Add hourly aggregation status when ParkHourlyStats ORM model is created
        return {
            'last_hourly_aggregation': None,  # Not implemented yet
            'last_daily_aggregation': str(latest_daily) if latest_daily else None,
            'hourly_lag_hours': None,  # Not implemented yet
            'daily_lag_hours': round(daily_lag_hours, 2) if daily_lag_hours is not None else None,
            'is_healthy': False  # Can't determine without hourly stats
        }

    def check_aggregation_health(self) -> Dict[str, Any]:
        """
        Comprehensive health check of aggregation system.

        Note: Hourly stats count requires ParkHourlyStats ORM model (not yet implemented).

        Returns:
            Dictionary with health check results
        """
        status = self.get_last_aggregation_status()

        # Count total records in daily stats table
        daily_count = self.session.query(func.count(ParkDailyStats.stat_date)).scalar() or 0

        # Check snapshot freshness
        latest_snapshot = (
            self.session.query(func.max(RideStatusSnapshot.recorded_at))
            .scalar()
        )

        snapshot_lag_minutes = None
        if latest_snapshot:
            snapshot_lag = datetime.utcnow() - latest_snapshot
            snapshot_lag_minutes = snapshot_lag.total_seconds() / 60.0

        return {
            **status,
            'hourly_stats_count': None,  # Not implemented yet
            'daily_stats_count': daily_count,
            'latest_snapshot': latest_snapshot.isoformat() if latest_snapshot else None,
            'snapshot_lag_minutes': round(snapshot_lag_minutes, 2) if snapshot_lag_minutes is not None else None,
            'snapshot_is_fresh': (snapshot_lag_minutes is not None and snapshot_lag_minutes < 15) if snapshot_lag_minutes is not None else False,
            'overall_healthy': (snapshot_lag_minutes is not None and snapshot_lag_minutes < 15) if snapshot_lag_minutes is not None else False
        }

    # === DEPRECATED METHODS ===
    # The following methods have been migrated to modular query classes.
    # Raise NotImplementedError to guide developers to the new classes.

    def get_park_daily_rankings(self, *args, **kwargs):
        """DEPRECATED: Use queries/rankings/park_downtime_rankings.py instead."""
        raise NotImplementedError(
            "get_park_daily_rankings() is deprecated. "
            "Use ParkDowntimeRankingsQuery from queries/rankings/park_downtime_rankings.py"
        )

    def get_ride_daily_rankings(self, *args, **kwargs):
        """DEPRECATED: Use queries/rankings/ride_downtime_rankings.py instead."""
        raise NotImplementedError(
            "get_ride_daily_rankings() is deprecated. "
            "Use RideDowntimeRankingsQuery from queries/rankings/ride_downtime_rankings.py"
        )

    def get_park_shame_breakdown(self, *args, **kwargs):
        """DEPRECATED: Use modular query classes in queries/ instead."""
        raise NotImplementedError(
            "get_park_shame_breakdown() is deprecated. "
            "Use appropriate query class from queries/today/, queries/yesterday/, or queries/charts/"
        )

    def get_live_wait_times(self, *args, **kwargs):
        """DEPRECATED: Use queries/live/ query classes instead."""
        raise NotImplementedError(
            "get_live_wait_times() is deprecated. "
            "Use query classes from queries/live/"
        )

    def get_park_hourly_shame_scores(self, *args, **kwargs):
        """DEPRECATED: Use queries/charts/park_shame_history.py instead."""
        raise NotImplementedError(
            "get_park_hourly_shame_scores() is deprecated. "
            "Use ParkShameHistoryQuery from queries/charts/park_shame_history.py"
        )
