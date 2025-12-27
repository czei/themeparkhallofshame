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
from datetime import datetime, date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, text, distinct

from src.models.orm_park import Park
from src.models.orm_ride import Ride
from src.models.orm_stats import ParkDailyStats, ParkHourlyStats, RideDailyStats, ParkLiveRankings
from src.models.orm_snapshots import RideStatusSnapshot, ParkActivitySnapshot
from src.models.orm_classification import RideClassification


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
        park_id: Optional[int] = None,
        period: str = "daily",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        filter_disney_universal: bool = False
    ) -> Dict[str, Any]:
        """
        Get aggregated statistics for parks over a period.

        Can return stats for a single park (if park_id provided) or summary
        stats across all parks.

        Args:
            park_id: Park ID (optional - if None, returns summary stats)
            period: "daily", "weekly", "monthly", "today", "yesterday", etc.
            start_date: Start date (YYYY-MM-DD) or None
            end_date: End date (YYYY-MM-DD) or None
            filter_disney_universal: If True, filter to Disney/Universal parks only

        Returns:
            Dictionary with aggregated statistics
        """
        # If no park_id, return summary stats across all parks
        if park_id is None:
            return self._get_summary_stats(period, filter_disney_universal)

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

    def _get_summary_stats(
        self,
        period: str,
        filter_disney_universal: bool
    ) -> Dict[str, Any]:
        """
        Get summary stats from pre-aggregated tables based on period.

        Uses pure ORM queries - no raw SQL.

        Each period uses a single efficient query on already-aggregated tables:
        - LIVE: ParkLiveRankings
        - TODAY: ParkHourlyStats (last 24 hours)
        - YESTERDAY: ParkDailyStats (yesterday)
        - LAST_WEEK: ParkDailyStats (last 7 days)
        - LAST_MONTH: ParkDailyStats (last 30 days)
        """
        if period == 'live':
            return self._get_live_summary_stats(filter_disney_universal)
        elif period == 'today':
            return self._get_today_summary_stats(filter_disney_universal)
        elif period == 'yesterday':
            return self._get_daily_summary_stats(1, filter_disney_universal, 'yesterday')
        elif period == 'last_week':
            return self._get_daily_summary_stats(7, filter_disney_universal, 'last_week')
        elif period == 'last_month':
            return self._get_daily_summary_stats(30, filter_disney_universal, 'last_month')
        else:
            # Default to live
            return self._get_live_summary_stats(filter_disney_universal)

    def _get_live_summary_stats(self, filter_disney_universal: bool) -> Dict[str, Any]:
        """Get summary stats from ParkLiveRankings using ORM."""
        query = self.session.query(
            func.count(ParkLiveRankings.park_id).label('total_parks'),
            func.coalesce(func.sum(ParkLiveRankings.total_rides), 0).label('total_rides'),
            func.coalesce(func.sum(ParkLiveRankings.rides_down), 0).label('rides_down'),
            func.coalesce(func.sum(ParkLiveRankings.total_downtime_hours), 0).label('total_downtime_hours'),
            func.coalesce(
                func.avg(100 - (ParkLiveRankings.rides_down * 100.0 /
                         func.nullif(ParkLiveRankings.total_rides, 0))),
                100
            ).label('avg_uptime')
        )

        if filter_disney_universal:
            query = query.filter(
                (ParkLiveRankings.is_disney == True) | (ParkLiveRankings.is_universal == True)
            )

        result = query.first()
        return self._format_summary_result(result, 'live', filter_disney_universal)

    def _get_today_summary_stats(self, filter_disney_universal: bool) -> Dict[str, Any]:
        """Get summary stats from ParkHourlyStats for last 24 hours using ORM."""
        cutoff = datetime.utcnow() - timedelta(hours=24)

        query = self.session.query(
            func.count(distinct(ParkHourlyStats.park_id)).label('total_parks'),
            func.coalesce(
                func.sum(ParkHourlyStats.rides_operating + ParkHourlyStats.rides_down), 0
            ).label('total_rides'),
            func.coalesce(func.sum(ParkHourlyStats.rides_down), 0).label('rides_down'),
            func.coalesce(func.sum(ParkHourlyStats.total_downtime_hours), 0).label('total_downtime_hours'),
            func.coalesce(
                func.avg(100 - (ParkHourlyStats.rides_down * 100.0 /
                         func.nullif(ParkHourlyStats.rides_operating + ParkHourlyStats.rides_down, 0))),
                100
            ).label('avg_uptime')
        ).filter(
            ParkHourlyStats.hour_start_utc >= cutoff,
            ParkHourlyStats.park_was_open == True
        )

        if filter_disney_universal:
            query = query.join(Park, ParkHourlyStats.park_id == Park.park_id).filter(
                (Park.is_disney == True) | (Park.is_universal == True)
            )

        result = query.first()
        return self._format_summary_result(result, 'today', filter_disney_universal)

    def _get_daily_summary_stats(
        self,
        days: int,
        filter_disney_universal: bool,
        period_name: str
    ) -> Dict[str, Any]:
        """Get summary stats from ParkDailyStats for specified number of days using ORM."""
        today = date.today()

        if days == 1:
            # Yesterday only
            date_filter = ParkDailyStats.stat_date == today - timedelta(days=1)
        else:
            # Last N days
            date_filter = ParkDailyStats.stat_date >= today - timedelta(days=days)

        query = self.session.query(
            func.count(distinct(ParkDailyStats.park_id)).label('total_parks'),
            func.coalesce(func.sum(ParkDailyStats.total_rides_tracked), 0).label('total_rides'),
            func.coalesce(func.sum(ParkDailyStats.rides_with_downtime), 0).label('rides_down'),
            func.coalesce(func.sum(ParkDailyStats.total_downtime_hours), 0).label('total_downtime_hours'),
            func.coalesce(func.avg(ParkDailyStats.avg_uptime_percentage), 100).label('avg_uptime')
        ).filter(date_filter)

        if filter_disney_universal:
            query = query.join(Park, ParkDailyStats.park_id == Park.park_id).filter(
                (Park.is_disney == True) | (Park.is_universal == True)
            )

        result = query.first()
        return self._format_summary_result(result, period_name, filter_disney_universal)

    def _format_summary_result(
        self,
        result,
        period: str,
        filter_disney_universal: bool
    ) -> Dict[str, Any]:
        """Format ORM query result into standard summary stats dict."""
        if not result or result.total_parks is None:
            return {
                'period': period,
                'filter_disney_universal': filter_disney_universal,
                'total_parks': 0,
                'rides_operating': 0,
                'rides_down': 0,
                'rides_closed': 0,
                'rides_refurbishment': 0,
                'total_downtime_hours': 0.0,
                'avg_uptime_percentage': 100.0
            }

        total_rides = int(result.total_rides or 0)
        rides_down = int(result.rides_down or 0)

        return {
            'period': period,
            'filter_disney_universal': filter_disney_universal,
            'total_parks': int(result.total_parks or 0),
            'rides_operating': total_rides - rides_down,
            'rides_down': rides_down,
            'rides_closed': 0,
            'rides_refurbishment': 0,
            'total_downtime_hours': float(result.total_downtime_hours or 0),
            'avg_uptime_percentage': round(float(result.avg_uptime or 100), 1)
        }

    def get_hourly_stats(
        self,
        start_hour: datetime,
        end_hour: datetime,
        park_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get hourly statistics from park_hourly_stats table.

        Args:
            start_hour: Start datetime (UTC)
            end_hour: End datetime (UTC)
            park_id: Optional park ID to filter by

        Returns:
            List of hourly stats dictionaries with:
            - park_id
            - hour_start_utc
            - avg_wait_time_minutes
            - snapshot_count
            - shame_score
        """
        query = (
            self.session.query(ParkHourlyStats)
            .filter(
                and_(
                    ParkHourlyStats.hour_start_utc >= start_hour,
                    ParkHourlyStats.hour_start_utc < end_hour,
                    ParkHourlyStats.park_was_open.is_(True)
                )
            )
        )

        if park_id:
            query = query.filter(ParkHourlyStats.park_id == park_id)

        results = query.all()

        return [
            {
                'park_id': row.park_id,
                'hour_start_utc': row.hour_start_utc,
                'avg_wait_time_minutes': float(row.avg_wait_time_minutes) if row.avg_wait_time_minutes else None,
                'snapshot_count': row.snapshot_count,
                'shame_score': float(row.shame_score) if row.shame_score else None,
                'rides_down': row.rides_down,
                'rides_operating': row.rides_operating,
                'total_downtime_hours': float(row.total_downtime_hours) if row.total_downtime_hours else None,
                'weighted_downtime_hours': float(row.weighted_downtime_hours) if row.weighted_downtime_hours else None,
            }
            for row in results
        ]

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
        end_date: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get park operating sessions (when park was open).

        Args:
            park_id: Park ID
            start_date: Start date (YYYY-MM-DD) or None for 30 days ago
            end_date: End date (YYYY-MM-DD) or None for today
            limit: Maximum number of sessions to return (optional)

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
            # Default to 30 days ago - use timedelta for database-agnostic date math
            query = query.filter(
                func.date(ParkActivitySnapshot.recorded_at) >= (date.today() - timedelta(days=30))
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

    def get_live_shame_chart_data(
        self,
        park_id: int,
        minutes: int = 60
    ) -> Dict[str, Any]:
        """
        Get recent shame score data for live chart display.

        Fetches the last N minutes of stored shame_score data at 5-minute granularity
        from park_activity_snapshots.

        Args:
            park_id: Park ID
            minutes: Number of minutes to look back (default 60)

        Returns:
            Dictionary with chart data:
            - labels: List of time labels (HH:MM format in Pacific)
            - data: List of shame scores
            - granularity: "minutes"
            - current: Most recent non-null value
        """
        from datetime import timedelta, timezone

        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - timedelta(minutes=minutes)

        # Query park_activity_snapshots for recent shame scores
        results = (
            self.session.query(
                ParkActivitySnapshot.recorded_at,
                ParkActivitySnapshot.shame_score
            )
            .filter(
                and_(
                    ParkActivitySnapshot.park_id == park_id,
                    ParkActivitySnapshot.recorded_at >= start_utc,
                    ParkActivitySnapshot.recorded_at < now_utc
                )
            )
            .order_by(ParkActivitySnapshot.recorded_at)
            .all()
        )

        # Convert to Pacific time labels (UTC-8)
        labels = []
        data = []
        for row in results:
            # Convert UTC to Pacific (subtract 8 hours)
            pacific_time = row.recorded_at - timedelta(hours=8)
            labels.append(pacific_time.strftime('%H:%M'))
            data.append(float(row.shame_score) if row.shame_score is not None else None)

        # Find most recent non-null value for 'current'
        current = 0.0
        for val in reversed(data):
            if val is not None:
                current = val
                break

        return {
            "labels": labels,
            "data": data,
            "granularity": "minutes",
            "current": current
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

    # === SHAME BREAKDOWN METHODS ===
    # These ORM methods return shame score breakdown for park details modal

    def get_park_today_shame_breakdown(self, park_id: int) -> Dict[str, Any]:
        """
        Get shame breakdown for today using hourly stats.

        Args:
            park_id: Park ID

        Returns:
            Dictionary with shame score and breakdown metrics
        """
        from utils.timezone import get_today_range_to_now_utc

        start_utc, end_utc = get_today_range_to_now_utc()

        # Get today's hourly stats
        hourly_stats = (
            self.session.query(ParkHourlyStats)
            .filter(
                and_(
                    ParkHourlyStats.park_id == park_id,
                    ParkHourlyStats.hour_start_utc >= start_utc,
                    ParkHourlyStats.hour_start_utc < end_utc,
                    ParkHourlyStats.park_was_open.is_(True)
                )
            )
            .all()
        )

        if not hourly_stats:
            return {'shame_score': 0, 'total_downtime_hours': 0, 'weighted_downtime_hours': 0}

        # Calculate averages
        total_shame = sum(float(h.shame_score or 0) for h in hourly_stats)
        total_downtime = sum(float(h.total_downtime_hours or 0) for h in hourly_stats)
        weighted_downtime = sum(float(h.weighted_downtime_hours or 0) for h in hourly_stats)

        return {
            'shame_score': round(total_shame / len(hourly_stats), 2) if hourly_stats else 0,
            'total_downtime_hours': round(total_downtime, 2),
            'weighted_downtime_hours': round(weighted_downtime, 2),
            'hours_tracked': len(hourly_stats)
        }

    def _get_rides_with_downtime_for_date(
        self,
        park_id: int,
        stat_date: date
    ) -> List[Dict[str, Any]]:
        """
        Get rides with downtime for a park on a specific date.

        Args:
            park_id: Park ID
            stat_date: The date to query

        Returns:
            List of ride dictionaries with downtime details, sorted by weighted downtime desc
        """
        # Tier weights for calculating weighted contribution
        TIER_WEIGHTS = {1: 10, 2: 5, 3: 2}

        # Query ride daily stats joined with ride info and classification
        # Sort by tier-weighted downtime (tier 1 = 10x, tier 2 = 5x, tier 3 = 2x)
        results = (
            self.session.query(
                RideDailyStats.ride_id,
                Ride.name.label('ride_name'),
                RideClassification.tier,
                RideClassification.tier_weight,
                RideDailyStats.downtime_minutes
            )
            .join(Ride, RideDailyStats.ride_id == Ride.ride_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .filter(
                and_(
                    Ride.park_id == park_id,
                    RideDailyStats.stat_date == stat_date,
                    RideDailyStats.downtime_minutes > 0
                )
            )
            .order_by(RideDailyStats.downtime_minutes.desc())
            .all()
        )

        rides = []
        for r in results:
            tier = r.tier or 3  # Default to tier 3 if unclassified
            tier_weight = r.tier_weight or TIER_WEIGHTS.get(tier, 2)
            downtime_hours = float(r.downtime_minutes) / 60
            weighted_contribution = downtime_hours * tier_weight

            rides.append({
                'ride_id': r.ride_id,
                'ride_name': r.ride_name,
                'tier': tier,
                'tier_weight': tier_weight,
                'downtime_hours': round(downtime_hours, 2),
                'weighted_contribution': round(weighted_contribution, 2),
                'status': 'DOWN'
            })

        # Sort by weighted contribution (descending)
        rides.sort(key=lambda x: x['weighted_contribution'], reverse=True)
        return rides

    def _get_rides_with_downtime_for_date_range(
        self,
        park_id: int,
        start_date: date,
        end_date: date
    ) -> List[Dict[str, Any]]:
        """
        Get aggregated rides with downtime for a park across a date range.

        Args:
            park_id: Park ID
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of ride dictionaries with total downtime across the period
        """
        from sqlalchemy import func

        TIER_WEIGHTS = {1: 10, 2: 5, 3: 2}

        # Aggregate downtime across all days in range, joining with classification for tier
        results = (
            self.session.query(
                RideDailyStats.ride_id,
                Ride.name.label('ride_name'),
                RideClassification.tier,
                RideClassification.tier_weight,
                func.sum(RideDailyStats.downtime_minutes).label('total_downtime_minutes')
            )
            .join(Ride, RideDailyStats.ride_id == Ride.ride_id)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .filter(
                and_(
                    Ride.park_id == park_id,
                    RideDailyStats.stat_date >= start_date,
                    RideDailyStats.stat_date <= end_date,
                    RideDailyStats.downtime_minutes > 0
                )
            )
            .group_by(RideDailyStats.ride_id, Ride.name, RideClassification.tier, RideClassification.tier_weight)
            .order_by(func.sum(RideDailyStats.downtime_minutes).desc())
            .all()
        )

        rides = []
        for r in results:
            tier = r.tier or 3
            tier_weight = r.tier_weight or TIER_WEIGHTS.get(tier, 2)
            downtime_hours = float(r.total_downtime_minutes) / 60
            weighted_contribution = downtime_hours * tier_weight

            rides.append({
                'ride_id': r.ride_id,
                'ride_name': r.ride_name,
                'tier': tier,
                'tier_weight': tier_weight,
                'downtime_hours': round(downtime_hours, 2),
                'weighted_contribution': round(weighted_contribution, 2),
                'status': 'DOWN'
            })

        rides.sort(key=lambda x: x['weighted_contribution'], reverse=True)
        return rides

    def get_park_yesterday_shame_breakdown(self, park_id: int) -> Dict[str, Any]:
        """
        Get shame breakdown for yesterday using daily stats.

        Args:
            park_id: Park ID

        Returns:
            Dictionary with shame score and breakdown metrics
        """
        from utils.timezone import get_yesterday_date_range
        from utils.metrics import calculate_shame_score

        start_date, end_date, _ = get_yesterday_date_range()

        # Get yesterday's daily stats
        daily_stat = (
            self.session.query(ParkDailyStats)
            .filter(
                and_(
                    ParkDailyStats.park_id == park_id,
                    ParkDailyStats.stat_date == start_date
                )
            )
            .first()
        )

        if not daily_stat:
            return {'shame_score': 0, 'total_downtime_hours': 0, 'rides': [],
                    'weighted_downtime_hours': 0, 'total_park_weight': 0, 'rides_affected_count': 0}

        # Get detailed ride-level downtime data for frontend
        rides = self._get_rides_with_downtime_for_date(park_id, start_date)

        # Calculate weighted downtime and total park weight from rides data
        weighted_downtime_hours = sum(
            float(r.get('downtime_hours', 0)) * float(r.get('tier_weight', 2))
            for r in rides
        )

        # Calculate total_park_weight from rides with tier info
        # Use sum of tier weights for rides we have data for
        total_park_weight = sum(float(r.get('tier_weight', 2)) for r in rides) if rides else 0

        # Determine shame_score: use stored value if available, otherwise calculate fallback
        if daily_stat.shame_score is not None:
            shame_score = float(daily_stat.shame_score)
        elif rides and total_park_weight > 0:
            # Fallback: calculate from ride data when aggregation failed
            # Uses same formula as metrics.py: (weighted_downtime / total_weight) * 10
            calculated = calculate_shame_score(weighted_downtime_hours, total_park_weight)
            shame_score = float(calculated) if calculated is not None else 0.0
        else:
            shame_score = 0.0

        return {
            'shame_score': shame_score,
            'total_downtime_hours': float(daily_stat.total_downtime_hours or 0),
            'avg_uptime_percentage': float(daily_stat.avg_uptime_percentage or 0),
            'rides_with_downtime': daily_stat.rides_with_downtime or 0,  # Keep original count for backwards compatibility
            'rides': rides,  # Array of rides with downtime details for frontend
            'weighted_downtime_hours': round(weighted_downtime_hours, 2),
            'total_park_weight': total_park_weight,
            'rides_affected_count': len(rides)
        }

    def get_park_weekly_shame_breakdown(self, park_id: int) -> Dict[str, Any]:
        """
        Get shame breakdown for last week using daily stats.

        Args:
            park_id: Park ID

        Returns:
            Dictionary with average shame score and breakdown metrics
        """
        from utils.timezone import get_last_week_date_range
        from utils.metrics import calculate_shame_score

        start_date, end_date, _ = get_last_week_date_range()

        # Get daily stats for the week
        daily_stats = (
            self.session.query(ParkDailyStats)
            .filter(
                and_(
                    ParkDailyStats.park_id == park_id,
                    ParkDailyStats.stat_date >= start_date,
                    ParkDailyStats.stat_date <= end_date
                )
            )
            .all()
        )

        if not daily_stats:
            return {'shame_score': 0, 'total_downtime_hours': 0, 'days_tracked': 0,
                    'rides': [], 'weighted_downtime_hours': 0, 'total_park_weight': 0,
                    'rides_affected_count': 0, 'period_label': 'Last 7 Days'}

        # Get aggregated ride-level downtime data for frontend
        rides = self._get_rides_with_downtime_for_date_range(park_id, start_date, end_date)

        # Calculate weighted downtime from rides
        weighted_downtime_hours = sum(r.get('weighted_contribution', 0) for r in rides)
        total_park_weight = sum(float(r.get('tier_weight', 2)) for r in rides) if rides else 0

        # Calculate shame_score: prefer stored values, fallback to ride data
        valid_shame_scores = [float(s.shame_score) for s in daily_stats if s.shame_score is not None]
        if valid_shame_scores:
            # Use average of valid stored values
            shame_score = round(sum(valid_shame_scores) / len(valid_shame_scores), 2)
        elif rides and total_park_weight > 0:
            # Fallback: calculate from ride data when all aggregations failed
            calculated = calculate_shame_score(weighted_downtime_hours, total_park_weight)
            shame_score = float(calculated) if calculated is not None else 0.0
        else:
            shame_score = 0.0

        total_downtime = sum(float(s.total_downtime_hours or 0) for s in daily_stats)

        return {
            'shame_score': shame_score,
            'total_downtime_hours': round(total_downtime, 2),
            'avg_daily_downtime': round(total_downtime / len(daily_stats), 2),
            'days_tracked': len(daily_stats),
            'days_in_period': len(daily_stats),
            'rides': rides,
            'weighted_downtime_hours': round(weighted_downtime_hours, 2),
            'total_park_weight': round(total_park_weight, 2),
            'rides_affected_count': len(rides),
            'period_label': f'{start_date.strftime("%b %d")} - {end_date.strftime("%b %d, %Y")}'
        }

    def get_park_monthly_shame_breakdown(self, park_id: int) -> Dict[str, Any]:
        """
        Get shame breakdown for last month using daily stats.

        Args:
            park_id: Park ID

        Returns:
            Dictionary with average shame score and breakdown metrics
        """
        from utils.timezone import get_last_month_date_range
        from utils.metrics import calculate_shame_score

        start_date, end_date, _ = get_last_month_date_range()

        # Get daily stats for the month
        daily_stats = (
            self.session.query(ParkDailyStats)
            .filter(
                and_(
                    ParkDailyStats.park_id == park_id,
                    ParkDailyStats.stat_date >= start_date,
                    ParkDailyStats.stat_date <= end_date
                )
            )
            .all()
        )

        if not daily_stats:
            return {'shame_score': 0, 'total_downtime_hours': 0, 'days_tracked': 0,
                    'rides': [], 'weighted_downtime_hours': 0, 'total_park_weight': 0,
                    'rides_affected_count': 0, 'period_label': 'Last 30 Days'}

        # Get aggregated ride-level downtime data for frontend
        rides = self._get_rides_with_downtime_for_date_range(park_id, start_date, end_date)

        # Calculate weighted downtime from rides
        weighted_downtime_hours = sum(r.get('weighted_contribution', 0) for r in rides)
        total_park_weight = sum(float(r.get('tier_weight', 2)) for r in rides) if rides else 0

        # Calculate shame_score: prefer stored values, fallback to ride data
        valid_shame_scores = [float(s.shame_score) for s in daily_stats if s.shame_score is not None]
        if valid_shame_scores:
            # Use average of valid stored values
            shame_score = round(sum(valid_shame_scores) / len(valid_shame_scores), 2)
        elif rides and total_park_weight > 0:
            # Fallback: calculate from ride data when all aggregations failed
            calculated = calculate_shame_score(weighted_downtime_hours, total_park_weight)
            shame_score = float(calculated) if calculated is not None else 0.0
        else:
            shame_score = 0.0

        total_downtime = sum(float(s.total_downtime_hours or 0) for s in daily_stats)

        return {
            'shame_score': shame_score,
            'total_downtime_hours': round(total_downtime, 2),
            'avg_daily_downtime': round(total_downtime / len(daily_stats), 2),
            'days_tracked': len(daily_stats),
            'days_in_period': len(daily_stats),
            'rides': rides,
            'weighted_downtime_hours': round(weighted_downtime_hours, 2),
            'total_park_weight': round(total_park_weight, 2),
            'rides_affected_count': len(rides),
            'period_label': f'{start_date.strftime("%b %d")} - {end_date.strftime("%b %d, %Y")}'
        }

    def get_excluded_rides(self, park_id: int) -> List[Dict[str, Any]]:
        """
        Get list of rides excluded from shame calculations for a park.

        Rides are excluded if they haven't operated in the last 7 days.

        Args:
            park_id: Park ID

        Returns:
            List of excluded ride dictionaries with tier, last_operated_at, days_since_operation
        """
        from datetime import timedelta

        # Get rides that haven't operated in last 7 days
        now = datetime.utcnow()
        seven_days_ago = now - timedelta(days=7)

        # Subquery for rides that have operated recently
        # (status='OPERATING' or computed_is_open=TRUE)
        operated_rides = (
            self.session.query(distinct(RideStatusSnapshot.ride_id))
            .filter(
                and_(
                    RideStatusSnapshot.recorded_at >= seven_days_ago,
                    RideStatusSnapshot.status == 'OPERATING'
                )
            )
            .subquery()
        )

        # Get rides in park that are NOT in operated list
        excluded = (
            self.session.query(Ride)
            .filter(
                and_(
                    Ride.park_id == park_id,
                    Ride.is_active.is_(True),
                    ~Ride.ride_id.in_(operated_rides)
                )
            )
            .all()
        )

        result = []
        for r in excluded:
            # Calculate days_since_operation using last_operated_at
            days_since = None
            if r.last_operated_at:
                days_since = (now - r.last_operated_at).days

            result.append({
                'ride_id': r.ride_id,
                'ride_name': r.name,
                'tier': r.tier,
                'tier_weight': {1: 3, 2: 2, 3: 1}.get(r.tier, 2),
                'last_operated_at': r.last_operated_at,
                'days_since_operation': days_since,
                'reason': 'No operation in last 7 days'
            })

        return result

    def get_active_rides(self, park_id: int) -> List[Dict[str, Any]]:
        """
        Get list of active rides that have operated in the last 7 days.

        Args:
            park_id: Park ID

        Returns:
            List of active ride dictionaries with tier info
        """
        from datetime import timedelta

        seven_days_ago = datetime.utcnow() - timedelta(days=7)

        # Subquery for rides that have operated recently
        # (status='OPERATING' means ride was running)
        operated_rides = (
            self.session.query(distinct(RideStatusSnapshot.ride_id))
            .filter(
                and_(
                    RideStatusSnapshot.recorded_at >= seven_days_ago,
                    RideStatusSnapshot.status == 'OPERATING'
                )
            )
            .subquery()
        )

        # Get active rides
        active = (
            self.session.query(Ride)
            .filter(
                and_(
                    Ride.park_id == park_id,
                    Ride.is_active.is_(True),
                    Ride.ride_id.in_(operated_rides)
                )
            )
            .all()
        )

        # Define tier weights (same as used in shame calculations)
        tier_weights = {1: 10, 2: 5, 3: 2, None: 1}

        return [
            {
                'ride_id': r.ride_id,
                'name': r.name,
                'tier': r.tier,
                'weight': tier_weights.get(r.tier, 1)
            }
            for r in active
        ]

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

    def get_park_shame_breakdown(self, park_id: int) -> Dict[str, Any]:
        """
        Get current live shame breakdown using recent snapshots.

        Args:
            park_id: Park ID

        Returns:
            Dictionary with current shame score
        """
        # Get most recent park activity snapshot for live data
        latest = (
            self.session.query(ParkActivitySnapshot)
            .filter(ParkActivitySnapshot.park_id == park_id)
            .order_by(ParkActivitySnapshot.recorded_at.desc())
            .first()
        )

        if not latest:
            return {'shame_score': 0, 'is_live': True}

        return {
            'shame_score': float(latest.shame_score or 0),
            'is_live': True,
            'last_updated': latest.recorded_at.isoformat() if latest.recorded_at else None
        }

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
