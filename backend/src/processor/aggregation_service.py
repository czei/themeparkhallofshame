"""
Theme Park Downtime Tracker - Aggregation Service
Calculates daily/weekly/monthly/yearly statistics from raw snapshots with timezone awareness.

Converted to SQLAlchemy 2.0 ORM (Phase 20).
"""

from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
from zoneinfo import ZoneInfo
from sqlalchemy import select, func, case, and_, or_
from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert as mysql_insert

from utils.logger import logger
from utils.metrics import (
    calculate_shame_score,
    calculate_weighted_downtime_hours,
    DEFAULT_TIER_WEIGHT
)
from processor.operating_hours_detector import OperatingHoursDetector
from processor.status_change_detector import StatusChangeDetector

from models import (
    Park, Ride, RideClassification,
    RideStatusSnapshot, ParkActivitySnapshot,
    RideDailyStats, ParkDailyStats,
    RideWeeklyStats, ParkWeeklyStats,
    RideMonthlyStats, ParkMonthlyStats,
    AggregationLog, AggregationType, AggregationStatus
)


class AggregationService:
    """
    Aggregates raw snapshot data into permanent statistics tables.

    Features:
    - Timezone-aware aggregation (iterates through distinct park timezones)
    - Daily/weekly/monthly/yearly aggregation
    - Aggregation log tracking for safe cleanup
    - Retry logic support (3 attempts)
    - Operating hours detection
    - Status change calculation
    """

    def __init__(self, session: Session):
        """
        Initialize aggregation service.

        Args:
            session: SQLAlchemy session object
        """
        self.session = session
        self.hours_detector = OperatingHoursDetector(session)
        self.change_detector = StatusChangeDetector(session)

    def aggregate_daily(
        self,
        aggregation_date: date,
        park_timezone: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Run daily aggregation for a specific date.

        Args:
            aggregation_date: Date to aggregate (in park's local timezone)
            park_timezone: Optional specific timezone (or None for all timezones)

        Returns:
            Dictionary with aggregation results
        """
        logger.info(f"Starting daily aggregation for {aggregation_date} (timezone: {park_timezone or 'all'})")

        # Start aggregation log
        log_id = self._create_aggregation_log(aggregation_date, AggregationType.DAILY)

        try:
            # Get distinct timezones to aggregate
            if park_timezone:
                timezones = [park_timezone]
            else:
                timezones = self._get_distinct_timezones()

            parks_processed = 0
            rides_processed = 0

            # Aggregate each timezone separately
            for tz in timezones:
                tz_results = self._aggregate_daily_for_timezone(aggregation_date, tz)
                parks_processed += tz_results['parks_count']
                rides_processed += tz_results['rides_count']

            # Calculate aggregated_until_ts (end of the day in UTC for the last timezone)
            last_tz = ZoneInfo(timezones[-1])
            local_end = datetime.combine(aggregation_date, datetime.max.time(), tzinfo=last_tz)
            aggregated_until_ts = local_end.astimezone(ZoneInfo('UTC'))

            # Update aggregation log with success
            self._complete_aggregation_log(
                log_id=log_id,
                status=AggregationStatus.SUCCESS,
                aggregated_until_ts=aggregated_until_ts,
                parks_processed=parks_processed,
                rides_processed=rides_processed
            )

            logger.info(f"Daily aggregation complete: {parks_processed} parks, {rides_processed} rides")

            return {
                "log_id": log_id,
                "status": "success",
                "parks_processed": parks_processed,
                "rides_processed": rides_processed,
                "aggregated_until_ts": aggregated_until_ts
            }

        except Exception as e:
            # Update aggregation log with failure
            self._complete_aggregation_log(
                log_id=log_id,
                status=AggregationStatus.FAILED,
                error_message=str(e)
            )

            logger.error(f"Daily aggregation failed: {e}", exc_info=True)
            raise

    def aggregate_weekly(
        self,
        year: int,
        week_number: int
    ) -> Dict[str, Any]:
        """
        Run weekly aggregation for a specific ISO week.

        Aggregates FROM daily stats (not raw snapshots).
        Calculates week-over-week trends.

        Args:
            year: Year (e.g., 2025)
            week_number: ISO week number (1-53)

        Returns:
            Dictionary with aggregation results
        """
        logger.info(f"Starting weekly aggregation for {year}-W{week_number:02d}")

        # Calculate week_start_date from ISO week
        # ISO week starts on Monday (use %G-W%V-%u for ISO 8601 format)
        week_start_date = datetime.strptime(f'{year}-W{week_number:02d}-1', '%G-W%V-%u').date()

        try:
            # Aggregate ride-level weekly stats
            rides_processed = self._aggregate_rides_weekly_stats(year, week_number, week_start_date)

            # Aggregate park-level weekly stats
            parks_processed = self._aggregate_parks_weekly_stats(year, week_number, week_start_date)

            logger.info(f"Weekly aggregation complete: {parks_processed} parks, {rides_processed} rides")

            return {
                "status": "success",
                "year": year,
                "week_number": week_number,
                "week_start_date": week_start_date,
                "parks_processed": parks_processed,
                "rides_processed": rides_processed
            }

        except Exception as e:
            logger.error(f"Weekly aggregation failed for {year}-W{week_number:02d}: {e}", exc_info=True)
            raise

    def aggregate_monthly(
        self,
        year: int,
        month: int
    ) -> Dict[str, Any]:
        """
        Run monthly aggregation for a specific month.

        Aggregates FROM daily stats (not raw snapshots).
        Calculates month-over-month trends.

        Args:
            year: Year (e.g., 2025)
            month: Month number (1-12)

        Returns:
            Dictionary with aggregation results
        """
        logger.info(f"Starting monthly aggregation for {year}-{month:02d}")

        try:
            # Aggregate ride-level monthly stats
            rides_processed = self._aggregate_rides_monthly_stats(year, month)

            # Aggregate park-level monthly stats
            parks_processed = self._aggregate_parks_monthly_stats(year, month)

            logger.info(f"Monthly aggregation complete: {parks_processed} parks, {rides_processed} rides")

            return {
                "status": "success",
                "year": year,
                "month": month,
                "parks_processed": parks_processed,
                "rides_processed": rides_processed
            }

        except Exception as e:
            logger.error(f"Monthly aggregation failed for {year}-{month:02d}: {e}", exc_info=True)
            raise

    def _aggregate_daily_for_timezone(
        self,
        aggregation_date: date,
        timezone: str
    ) -> Dict[str, int]:
        """
        Aggregate daily stats for all parks in a specific timezone.

        Args:
            aggregation_date: Local date to aggregate
            timezone: IANA timezone string

        Returns:
            Dictionary with counts
        """
        logger.info(f"Aggregating {aggregation_date} for timezone {timezone}")

        # Get parks in this timezone
        stmt = (
            select(Park.park_id, Park.name)
            .where(Park.timezone == timezone)
            .where(Park.is_active == True)
            .order_by(Park.park_id)
        )

        result = self.session.execute(stmt)
        parks = [{"park_id": row.park_id, "name": row.name} for row in result]

        parks_count = 0
        rides_count = 0

        for park in parks:
            # Detect operating session
            session = self.hours_detector.detect_operating_session(
                park_id=park['park_id'],
                operating_date=aggregation_date,
                park_timezone=timezone
            )

            if session:
                self.hours_detector.save_operating_session(session)

                # Aggregate park stats
                self._aggregate_park_daily_stats(park['park_id'], aggregation_date, timezone, session)
                parks_count += 1

                # Aggregate ride stats for this park
                ride_count = self._aggregate_rides_daily_stats(
                    park['park_id'],
                    aggregation_date,
                    timezone,
                    session
                )
                rides_count += ride_count

        return {
            "parks_count": parks_count,
            "rides_count": rides_count
        }

    def _aggregate_park_daily_stats(
        self,
        park_id: int,
        stat_date: date,
        timezone: str,
        operating_session: Dict[str, Any]
    ):
        """
        Calculate and save daily park statistics.

        Only counts downtime during actual operating hours (when park_appears_open = TRUE).
        This prevents false downtime readings when rides are simply closed for the night.

        Args:
            park_id: Park ID
            stat_date: Local date
            timezone: Park timezone
            operating_session: Operating session data (includes session_start_utc, session_end_utc)
        """
        # Use operating session boundaries instead of midnight-midnight
        # This ensures we only count downtime during actual operating hours
        utc_start = operating_session['session_start_utc']
        utc_end = operating_session['session_end_utc']

        # Calculate park-wide statistics
        stmt = (
            select(
                func.count(func.distinct(Ride.ride_id)).label('total_rides_tracked'),
                func.count(func.distinct(
                    case((RideStatusSnapshot.computed_is_open == False, Ride.ride_id))
                )).label('rides_with_downtime'),
                func.sum(case((RideStatusSnapshot.computed_is_open == True, 1), else_=0)).label('total_uptime_snapshots'),
                func.sum(case((RideStatusSnapshot.computed_is_open == False, 1), else_=0)).label('total_downtime_snapshots'),
                func.count(RideStatusSnapshot.snapshot_id).label('total_snapshots')
            )
            .select_from(Ride)
            .outerjoin(
                RideStatusSnapshot,
                and_(
                    Ride.ride_id == RideStatusSnapshot.ride_id,
                    RideStatusSnapshot.recorded_at >= utc_start,
                    RideStatusSnapshot.recorded_at <= utc_end
                )
            )
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
        )

        result = self.session.execute(stmt)
        row = result.one()

        # Calculate downtime hours and uptime percentage
        total_snapshots = row.total_snapshots or 0
        downtime_snapshots = row.total_downtime_snapshots or 0
        operating_minutes = float(operating_session['operating_minutes'])

        if total_snapshots > 0:
            downtime_ratio = float(downtime_snapshots) / float(total_snapshots)
            operating_hours = operating_minutes / 60.0
            total_downtime_hours = operating_hours * downtime_ratio
            avg_uptime_percentage = ((float(total_snapshots) - float(downtime_snapshots)) / float(total_snapshots)) * 100.0
        else:
            total_downtime_hours = 0.0
            avg_uptime_percentage = 0.0
            operating_hours = operating_minutes / 60.0

        # Calculate shame_score using tier weights
        # Formula: weighted_downtime_hours / total_park_weight
        shame_score = self._calculate_park_shame_score(
            park_id=park_id,
            utc_start=utc_start,
            utc_end=utc_end,
            operating_minutes=operating_minutes
        )

        # Insert/update park daily stats using MySQL upsert
        stmt = mysql_insert(ParkDailyStats).values(
            park_id=park_id,
            stat_date=stat_date,
            total_downtime_hours=round(total_downtime_hours, 2),
            avg_uptime_percentage=round(avg_uptime_percentage, 2),
            shame_score=round(shame_score, 3) if shame_score else None,
            rides_with_downtime=row.rides_with_downtime or 0,
            total_rides_tracked=row.total_rides_tracked or 0,
            operating_hours_minutes=int(operating_minutes)
        )

        stmt = stmt.on_duplicate_key_update(
            total_downtime_hours=stmt.inserted.total_downtime_hours,
            avg_uptime_percentage=stmt.inserted.avg_uptime_percentage,
            shame_score=stmt.inserted.shame_score,
            rides_with_downtime=stmt.inserted.rides_with_downtime,
            total_rides_tracked=stmt.inserted.total_rides_tracked,
            operating_hours_minutes=stmt.inserted.operating_hours_minutes
        )

        self.session.execute(stmt)

        logger.debug(f"Aggregated park {park_id} daily stats for {stat_date}")

    def _calculate_park_shame_score(
        self,
        park_id: int,
        utc_start: datetime,
        utc_end: datetime,
        operating_minutes: float
    ) -> Optional[float]:
        """
        Calculate shame score for a park based on weighted ride downtime.

        Shame Score = total_weighted_downtime_hours / total_park_weight

        Where:
        - total_weighted_downtime_hours = SUM(ride_downtime_hours * tier_weight)
        - total_park_weight = SUM(tier_weight) for all rides in the park

        Args:
            park_id: Park ID
            utc_start: Operating session start (UTC)
            utc_end: Operating session end (UTC)
            operating_minutes: Total operating minutes for the day

        Returns:
            Shame score (float) or None if no data
        """
        if operating_minutes <= 0:
            return None

        operating_hours = operating_minutes / 60.0

        # Calculate per-ride downtime and aggregate weighted downtime
        stmt = (
            select(
                Ride.ride_id,
                func.coalesce(RideClassification.tier_weight, DEFAULT_TIER_WEIGHT).label('tier_weight'),
                func.count(RideStatusSnapshot.snapshot_id).label('total_snapshots'),
                func.sum(case((RideStatusSnapshot.computed_is_open == False, 1), else_=0)).label('downtime_snapshots')
            )
            .select_from(Ride)
            .outerjoin(RideClassification, Ride.ride_id == RideClassification.ride_id)
            .outerjoin(
                RideStatusSnapshot,
                and_(
                    Ride.ride_id == RideStatusSnapshot.ride_id,
                    RideStatusSnapshot.recorded_at >= utc_start,
                    RideStatusSnapshot.recorded_at <= utc_end
                )
            )
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
            .where(Ride.category == 'ATTRACTION')
            .group_by(Ride.ride_id, RideClassification.tier_weight)
        )

        result = self.session.execute(stmt)

        total_park_weight = 0.0
        total_weighted_downtime_hours = 0.0

        for ride in result:
            tier_weight = float(ride.tier_weight or DEFAULT_TIER_WEIGHT)
            total_park_weight += tier_weight

            total_snapshots = ride.total_snapshots or 0
            downtime_snapshots = ride.downtime_snapshots or 0

            if total_snapshots > 0:
                downtime_ratio = float(downtime_snapshots) / float(total_snapshots)
                ride_downtime_hours = operating_hours * downtime_ratio
                # Use centralized weighted downtime calculation
                total_weighted_downtime_hours += calculate_weighted_downtime_hours(
                    ride_downtime_hours,
                    int(tier_weight)
                )

        # Use centralized shame score calculation
        return calculate_shame_score(total_weighted_downtime_hours, total_park_weight)

    def _aggregate_rides_daily_stats(
        self,
        park_id: int,
        stat_date: date,
        timezone: str,
        operating_session: Dict[str, Any]
    ) -> int:
        """
        Calculate and save daily ride statistics for all rides in a park.

        Only counts downtime during actual operating hours (when park_appears_open = TRUE).
        This prevents false downtime readings when rides are simply closed for the night.

        Args:
            park_id: Park ID
            stat_date: Local date
            timezone: Park timezone
            operating_session: Operating session data (includes session_start_utc, session_end_utc)

        Returns:
            Number of rides processed
        """
        # Use operating session boundaries instead of midnight-midnight
        # This ensures we only count downtime during actual operating hours
        utc_start = operating_session['session_start_utc']
        utc_end = operating_session['session_end_utc']
        operating_minutes = operating_session['operating_minutes']

        # Get all active rides for this park
        stmt = (
            select(Ride.ride_id)
            .where(Ride.park_id == park_id)
            .where(Ride.is_active == True)
        )

        result = self.session.execute(stmt)
        ride_ids = [row.ride_id for row in result]

        for ride_id in ride_ids:
            # Calculate ride statistics
            self._aggregate_single_ride_daily_stats(
                ride_id=ride_id,
                stat_date=stat_date,
                utc_start=utc_start,
                utc_end=utc_end,
                operating_minutes=operating_minutes
            )

        return len(ride_ids)

    def _aggregate_single_ride_daily_stats(
        self,
        ride_id: int,
        stat_date: date,
        utc_start: datetime,
        utc_end: datetime,
        operating_minutes: float
    ):
        """Calculate and save daily statistics for a single ride."""
        # Get ride snapshots for the day
        stmt = (
            select(
                func.count().label('total_snapshots'),
                func.sum(case((RideStatusSnapshot.computed_is_open == True, 1), else_=0)).label('uptime_snapshots'),
                func.sum(case((RideStatusSnapshot.computed_is_open == False, 1), else_=0)).label('downtime_snapshots'),
                func.avg(case((RideStatusSnapshot.wait_time > 0, RideStatusSnapshot.wait_time))).label('avg_wait_time'),
                func.min(case((RideStatusSnapshot.wait_time > 0, RideStatusSnapshot.wait_time))).label('min_wait_time'),
                func.max(RideStatusSnapshot.wait_time).label('max_wait_time')
            )
            .where(RideStatusSnapshot.ride_id == ride_id)
            .where(RideStatusSnapshot.recorded_at >= utc_start)
            .where(RideStatusSnapshot.recorded_at <= utc_end)
        )

        result = self.session.execute(stmt)
        row = result.one()
        total_snapshots = row.total_snapshots or 0

        # Always create a record, even with zero snapshots (data consistency)
        if total_snapshots == 0:
            # No snapshots for this ride - create record with zeros
            zero_stmt = mysql_insert(RideDailyStats).values(
                ride_id=ride_id,
                stat_date=stat_date,
                uptime_minutes=0,
                downtime_minutes=0,
                uptime_percentage=0.0,
                operating_hours_minutes=0,
                avg_wait_time=None,
                min_wait_time=None,
                max_wait_time=None,
                peak_wait_time=None,
                status_changes=0,
                longest_downtime_minutes=None
            )

            zero_stmt = zero_stmt.on_duplicate_key_update(
                uptime_minutes=zero_stmt.inserted.uptime_minutes,
                downtime_minutes=zero_stmt.inserted.downtime_minutes,
                uptime_percentage=zero_stmt.inserted.uptime_percentage,
                operating_hours_minutes=zero_stmt.inserted.operating_hours_minutes,
                avg_wait_time=zero_stmt.inserted.avg_wait_time,
                min_wait_time=zero_stmt.inserted.min_wait_time,
                max_wait_time=zero_stmt.inserted.max_wait_time,
                peak_wait_time=zero_stmt.inserted.peak_wait_time,
                status_changes=zero_stmt.inserted.status_changes,
                longest_downtime_minutes=zero_stmt.inserted.longest_downtime_minutes
            )

            self.session.execute(zero_stmt)
            logger.debug(f"Created zero-snapshot record for ride {ride_id} on {stat_date}")
            return

        uptime_snapshots = row.uptime_snapshots or 0
        downtime_snapshots = row.downtime_snapshots or 0

        # Calculate uptime/downtime minutes
        uptime_ratio = float(uptime_snapshots) / float(total_snapshots) if total_snapshots > 0 else 0.0
        uptime_minutes = float(operating_minutes) * uptime_ratio
        downtime_minutes = float(operating_minutes) - uptime_minutes
        uptime_percentage = uptime_ratio * 100.0

        # Get status changes count
        changes = self.change_detector.detect_status_changes(ride_id, utc_start, utc_end)
        status_changes = len([c for c in changes if c['new_status'] is False])

        # Find longest downtime
        longest_downtime = max(
            (c.get('downtime_duration_minutes', 0) for c in changes if c.get('downtime_duration_minutes')),
            default=None
        )

        # Handle NULL wait times safely
        avg_wait = None
        if row.avg_wait_time is not None:
            avg_wait = round(float(row.avg_wait_time), 2)

        # Insert/update ride daily stats
        upsert_stmt = mysql_insert(RideDailyStats).values(
            ride_id=ride_id,
            stat_date=stat_date,
            uptime_minutes=int(uptime_minutes),
            downtime_minutes=int(downtime_minutes),
            uptime_percentage=round(uptime_percentage, 2),
            operating_hours_minutes=int(operating_minutes),
            avg_wait_time=avg_wait,
            min_wait_time=row.min_wait_time,
            max_wait_time=row.max_wait_time,
            peak_wait_time=row.max_wait_time,
            status_changes=status_changes,
            longest_downtime_minutes=longest_downtime
        )

        upsert_stmt = upsert_stmt.on_duplicate_key_update(
            uptime_minutes=upsert_stmt.inserted.uptime_minutes,
            downtime_minutes=upsert_stmt.inserted.downtime_minutes,
            uptime_percentage=upsert_stmt.inserted.uptime_percentage,
            operating_hours_minutes=upsert_stmt.inserted.operating_hours_minutes,
            avg_wait_time=upsert_stmt.inserted.avg_wait_time,
            min_wait_time=upsert_stmt.inserted.min_wait_time,
            max_wait_time=upsert_stmt.inserted.max_wait_time,
            peak_wait_time=upsert_stmt.inserted.peak_wait_time,
            status_changes=upsert_stmt.inserted.status_changes,
            longest_downtime_minutes=upsert_stmt.inserted.longest_downtime_minutes
        )

        self.session.execute(upsert_stmt)

    def _aggregate_rides_weekly_stats(
        self,
        year: int,
        week_number: int,
        week_start_date: date
    ) -> int:
        """
        Aggregate ride-level weekly stats from daily stats.

        Args:
            year: Year
            week_number: ISO week number
            week_start_date: Monday of the ISO week

        Returns:
            Number of rides processed
        """
        # Calculate date range for the week (Monday to Sunday)
        week_end_date = week_start_date + timedelta(days=6)

        # Get all rides that have daily stats in this week
        stmt = (
            select(RideDailyStats.ride_id)
            .distinct()
            .where(RideDailyStats.stat_date >= week_start_date)
            .where(RideDailyStats.stat_date <= week_end_date)
            .order_by(RideDailyStats.ride_id)
        )

        result = self.session.execute(stmt)
        ride_ids = [row.ride_id for row in result]

        # Aggregate each ride
        for ride_id in ride_ids:
            self._aggregate_single_ride_weekly_stats(
                ride_id=ride_id,
                year=year,
                week_number=week_number,
                week_start_date=week_start_date,
                week_end_date=week_end_date
            )

        logger.info(f"Processed {len(ride_ids)} rides for weekly aggregation")
        return len(ride_ids)

    def _aggregate_single_ride_weekly_stats(
        self,
        ride_id: int,
        year: int,
        week_number: int,
        week_start_date: date,
        week_end_date: date
    ) -> None:
        """
        Aggregate a single ride's weekly stats from daily stats.

        Args:
            ride_id: Ride ID
            year: Year
            week_number: ISO week number
            week_start_date: Monday of the ISO week
            week_end_date: Sunday of the ISO week
        """
        # Sum daily stats for the week
        stmt = (
            select(
                func.sum(RideDailyStats.uptime_minutes).label('uptime_minutes'),
                func.sum(RideDailyStats.downtime_minutes).label('downtime_minutes'),
                func.sum(RideDailyStats.operating_hours_minutes).label('operating_hours_minutes'),
                func.sum(RideDailyStats.status_changes).label('status_changes'),
                func.max(RideDailyStats.peak_wait_time).label('peak_wait_time'),
                func.sum(RideDailyStats.avg_wait_time * RideDailyStats.operating_hours_minutes).label('weighted_wait_sum'),
                func.sum(RideDailyStats.operating_hours_minutes).label('total_operating_minutes')
            )
            .where(RideDailyStats.ride_id == ride_id)
            .where(RideDailyStats.stat_date >= week_start_date)
            .where(RideDailyStats.stat_date <= week_end_date)
        )

        result = self.session.execute(stmt)
        row = result.one()

        if not row or row.uptime_minutes is None:
            logger.debug(f"No daily stats found for ride {ride_id} in week {year}-W{week_number:02d}")
            return

        # Calculate uptime percentage
        total_operating = int(row.operating_hours_minutes) if row.operating_hours_minutes else 0
        uptime_percentage = 0.0
        if total_operating > 0:
            uptime_percentage = (float(row.uptime_minutes) / float(total_operating)) * 100.0

        # Calculate weighted average wait time
        avg_wait_time = None
        if row.total_operating_minutes and row.total_operating_minutes > 0 and row.weighted_wait_sum:
            avg_wait_time = round(float(row.weighted_wait_sum) / float(row.total_operating_minutes), 2)

        # Calculate trend vs previous week
        trend_vs_previous_week = self._calculate_weekly_trend(
            ride_id=ride_id,
            current_year=year,
            current_week=week_number,
            current_downtime=row.downtime_minutes
        )

        # Insert/update ride weekly stats
        upsert_stmt = mysql_insert(RideWeeklyStats).values(
            ride_id=ride_id,
            year=year,
            week_number=week_number,
            week_start_date=week_start_date,
            uptime_minutes=row.uptime_minutes,
            downtime_minutes=row.downtime_minutes,
            uptime_percentage=round(uptime_percentage, 2),
            operating_hours_minutes=row.operating_hours_minutes,
            avg_wait_time=avg_wait_time,
            peak_wait_time=row.peak_wait_time,
            status_changes=row.status_changes,
            trend_vs_previous_week=trend_vs_previous_week
        )

        upsert_stmt = upsert_stmt.on_duplicate_key_update(
            week_start_date=upsert_stmt.inserted.week_start_date,
            uptime_minutes=upsert_stmt.inserted.uptime_minutes,
            downtime_minutes=upsert_stmt.inserted.downtime_minutes,
            uptime_percentage=upsert_stmt.inserted.uptime_percentage,
            operating_hours_minutes=upsert_stmt.inserted.operating_hours_minutes,
            avg_wait_time=upsert_stmt.inserted.avg_wait_time,
            peak_wait_time=upsert_stmt.inserted.peak_wait_time,
            status_changes=upsert_stmt.inserted.status_changes,
            trend_vs_previous_week=upsert_stmt.inserted.trend_vs_previous_week
        )

        self.session.execute(upsert_stmt)

        logger.debug(f"Aggregated weekly stats for ride {ride_id}, week {year}-W{week_number:02d}")

    def _calculate_weekly_trend(
        self,
        ride_id: int,
        current_year: int,
        current_week: int,
        current_downtime: int
    ) -> Optional[float]:
        """
        Calculate week-over-week downtime trend.

        Args:
            ride_id: Ride ID
            current_year: Current year
            current_week: Current ISO week number
            current_downtime: Current week's downtime minutes

        Returns:
            Percentage change (e.g., 20.75 for +20.75%) or None if no previous data
        """
        # Calculate previous week
        # Handle year boundary (week 1 -> previous year's last week)
        if current_week == 1:
            previous_year = current_year - 1
            # Get last week of previous year (usually 52, sometimes 53)
            previous_week_date = datetime.strptime(f'{previous_year}-12-28', '%Y-%m-%d').date()
            previous_week = previous_week_date.isocalendar()[1]
        else:
            previous_year = current_year
            previous_week = current_week - 1

        # Query previous week's downtime
        stmt = (
            select(RideWeeklyStats.downtime_minutes)
            .where(RideWeeklyStats.ride_id == ride_id)
            .where(RideWeeklyStats.year == previous_year)
            .where(RideWeeklyStats.week_number == previous_week)
        )

        result = self.session.execute(stmt)
        previous_row = result.first()

        if not previous_row or previous_row.downtime_minutes is None or previous_row.downtime_minutes == 0:
            return None

        # Calculate percentage change: ((current - previous) / previous) * 100
        prev_downtime = float(previous_row.downtime_minutes)
        trend = ((float(current_downtime) - prev_downtime) / prev_downtime) * 100.0
        return round(trend, 2)

    def _aggregate_parks_weekly_stats(
        self,
        year: int,
        week_number: int,
        week_start_date: date
    ) -> int:
        """
        Aggregate park-level weekly stats from ride weekly stats.

        Args:
            year: Year
            week_number: ISO week number
            week_start_date: Monday of the ISO week

        Returns:
            Number of parks processed
        """
        # Get all parks that have ride weekly stats
        stmt = (
            select(Ride.park_id)
            .distinct()
            .join(RideWeeklyStats, Ride.ride_id == RideWeeklyStats.ride_id)
            .where(RideWeeklyStats.year == year)
            .where(RideWeeklyStats.week_number == week_number)
            .order_by(Ride.park_id)
        )

        result = self.session.execute(stmt)
        park_ids = [row.park_id for row in result]

        # Aggregate each park
        for park_id in park_ids:
            self._aggregate_single_park_weekly_stats(
                park_id=park_id,
                year=year,
                week_number=week_number,
                week_start_date=week_start_date
            )

        logger.info(f"Processed {len(park_ids)} parks for weekly aggregation")
        return len(park_ids)

    def _aggregate_single_park_weekly_stats(
        self,
        park_id: int,
        year: int,
        week_number: int,
        week_start_date: date
    ) -> None:
        """
        Aggregate a single park's weekly stats from ride weekly stats.

        Args:
            park_id: Park ID
            year: Year
            week_number: ISO week number
            week_start_date: Monday of the ISO week
        """
        # Aggregate ride weekly stats for this park
        stmt = (
            select(
                func.count(func.distinct(RideWeeklyStats.ride_id)).label('total_rides_tracked'),
                func.avg(RideWeeklyStats.uptime_percentage).label('avg_uptime_percentage'),
                (func.sum(RideWeeklyStats.downtime_minutes) / 60.0).label('total_downtime_hours'),
                func.sum(case((RideWeeklyStats.downtime_minutes > 0, 1), else_=0)).label('rides_with_downtime'),
                func.sum(RideWeeklyStats.avg_wait_time * RideWeeklyStats.operating_hours_minutes).label('weighted_wait_sum'),
                func.sum(RideWeeklyStats.operating_hours_minutes).label('total_operating_minutes'),
                func.max(RideWeeklyStats.peak_wait_time).label('peak_wait_time')
            )
            .select_from(RideWeeklyStats)
            .join(Ride, RideWeeklyStats.ride_id == Ride.ride_id)
            .where(Ride.park_id == park_id)
            .where(RideWeeklyStats.year == year)
            .where(RideWeeklyStats.week_number == week_number)
            .where(Ride.is_active == True)
        )

        result = self.session.execute(stmt)
        row = result.one()

        if not row or row.total_rides_tracked == 0:
            logger.debug(f"No ride weekly stats found for park {park_id} in week {year}-W{week_number:02d}")
            return

        # Calculate weighted average wait time
        avg_wait_time = None
        if row.total_operating_minutes and row.total_operating_minutes > 0 and row.weighted_wait_sum:
            avg_wait_time = round(float(row.weighted_wait_sum) / float(row.total_operating_minutes), 2)

        # Calculate park-level trend
        trend_vs_previous_week = self._calculate_park_weekly_trend(
            park_id=park_id,
            current_year=year,
            current_week=week_number,
            current_downtime_hours=row.total_downtime_hours
        )

        # Insert/update park weekly stats
        upsert_stmt = mysql_insert(ParkWeeklyStats).values(
            park_id=park_id,
            year=year,
            week_number=week_number,
            week_start_date=week_start_date,
            total_rides_tracked=row.total_rides_tracked,
            avg_uptime_percentage=round(float(row.avg_uptime_percentage), 2) if row.avg_uptime_percentage else None,
            total_downtime_hours=round(float(row.total_downtime_hours), 2),
            rides_with_downtime=row.rides_with_downtime,
            avg_wait_time=avg_wait_time,
            peak_wait_time=row.peak_wait_time,
            trend_vs_previous_week=trend_vs_previous_week
        )

        upsert_stmt = upsert_stmt.on_duplicate_key_update(
            week_start_date=upsert_stmt.inserted.week_start_date,
            total_rides_tracked=upsert_stmt.inserted.total_rides_tracked,
            avg_uptime_percentage=upsert_stmt.inserted.avg_uptime_percentage,
            total_downtime_hours=upsert_stmt.inserted.total_downtime_hours,
            rides_with_downtime=upsert_stmt.inserted.rides_with_downtime,
            avg_wait_time=upsert_stmt.inserted.avg_wait_time,
            peak_wait_time=upsert_stmt.inserted.peak_wait_time,
            trend_vs_previous_week=upsert_stmt.inserted.trend_vs_previous_week
        )

        self.session.execute(upsert_stmt)

        logger.debug(f"Aggregated weekly stats for park {park_id}, week {year}-W{week_number:02d}")

    def _calculate_park_weekly_trend(
        self,
        park_id: int,
        current_year: int,
        current_week: int,
        current_downtime_hours: float
    ) -> Optional[float]:
        """
        Calculate week-over-week park downtime trend.

        Args:
            park_id: Park ID
            current_year: Current year
            current_week: Current ISO week number
            current_downtime_hours: Current week's downtime hours

        Returns:
            Percentage change or None if no previous data
        """
        # Calculate previous week
        if current_week == 1:
            previous_year = current_year - 1
            previous_week_date = datetime.strptime(f'{previous_year}-12-28', '%Y-%m-%d').date()
            previous_week = previous_week_date.isocalendar()[1]
        else:
            previous_year = current_year
            previous_week = current_week - 1

        # Query previous week's downtime
        stmt = (
            select(ParkWeeklyStats.total_downtime_hours)
            .where(ParkWeeklyStats.park_id == park_id)
            .where(ParkWeeklyStats.year == previous_year)
            .where(ParkWeeklyStats.week_number == previous_week)
        )

        result = self.session.execute(stmt)
        previous_row = result.first()

        if not previous_row or previous_row.total_downtime_hours is None or float(previous_row.total_downtime_hours) == 0:
            return None

        # Calculate percentage change
        previous_downtime = float(previous_row.total_downtime_hours)
        trend = ((float(current_downtime_hours) - previous_downtime) / previous_downtime) * 100.0
        return round(trend, 2)

    def _aggregate_rides_monthly_stats(
        self,
        year: int,
        month: int
    ) -> int:
        """
        Aggregate ride-level monthly stats from daily stats.

        Args:
            year: Year
            month: Month number (1-12)

        Returns:
            Number of rides processed
        """
        # Calculate date range for the month
        import calendar
        first_day = date(year, month, 1)
        last_day_num = calendar.monthrange(year, month)[1]
        last_day = date(year, month, last_day_num)

        # Get all rides that have daily stats in this month
        stmt = (
            select(RideDailyStats.ride_id)
            .distinct()
            .where(RideDailyStats.stat_date >= first_day)
            .where(RideDailyStats.stat_date <= last_day)
            .order_by(RideDailyStats.ride_id)
        )

        result = self.session.execute(stmt)
        ride_ids = [row.ride_id for row in result]

        # Aggregate each ride
        for ride_id in ride_ids:
            self._aggregate_single_ride_monthly_stats(
                ride_id=ride_id,
                year=year,
                month=month,
                month_start=first_day,
                month_end=last_day
            )

        logger.info(f"Processed {len(ride_ids)} rides for monthly aggregation")
        return len(ride_ids)

    def _aggregate_single_ride_monthly_stats(
        self,
        ride_id: int,
        year: int,
        month: int,
        month_start: date,
        month_end: date
    ) -> None:
        """
        Aggregate a single ride's monthly stats from daily stats.

        Args:
            ride_id: Ride ID
            year: Year
            month: Month number (1-12)
            month_start: First day of the month
            month_end: Last day of the month
        """
        # Sum daily stats for the month
        stmt = (
            select(
                func.sum(RideDailyStats.uptime_minutes).label('uptime_minutes'),
                func.sum(RideDailyStats.downtime_minutes).label('downtime_minutes'),
                func.sum(RideDailyStats.operating_hours_minutes).label('operating_hours_minutes'),
                func.sum(RideDailyStats.status_changes).label('status_changes'),
                func.max(RideDailyStats.peak_wait_time).label('peak_wait_time'),
                func.sum(RideDailyStats.avg_wait_time * RideDailyStats.operating_hours_minutes).label('weighted_wait_sum'),
                func.sum(RideDailyStats.operating_hours_minutes).label('total_operating_minutes')
            )
            .where(RideDailyStats.ride_id == ride_id)
            .where(RideDailyStats.stat_date >= month_start)
            .where(RideDailyStats.stat_date <= month_end)
        )

        result = self.session.execute(stmt)
        row = result.one()

        if not row or row.uptime_minutes is None:
            logger.debug(f"No daily stats found for ride {ride_id} in month {year}-{month:02d}")
            return

        # Calculate uptime percentage
        total_operating = int(row.operating_hours_minutes) if row.operating_hours_minutes else 0
        uptime_percentage = 0.0
        if total_operating > 0:
            uptime_percentage = (float(row.uptime_minutes) / float(total_operating)) * 100.0

        # Calculate weighted average wait time
        avg_wait_time = None
        if row.total_operating_minutes and row.total_operating_minutes > 0 and row.weighted_wait_sum:
            avg_wait_time = round(float(row.weighted_wait_sum) / float(row.total_operating_minutes), 2)

        # Calculate trend vs previous month
        trend_vs_previous_month = self._calculate_monthly_trend(
            ride_id=ride_id,
            current_year=year,
            current_month=month,
            current_downtime=row.downtime_minutes
        )

        # Insert/update ride monthly stats
        upsert_stmt = mysql_insert(RideMonthlyStats).values(
            ride_id=ride_id,
            year=year,
            month=month,
            uptime_minutes=row.uptime_minutes,
            downtime_minutes=row.downtime_minutes,
            uptime_percentage=round(uptime_percentage, 2),
            operating_hours_minutes=row.operating_hours_minutes,
            avg_wait_time=avg_wait_time,
            peak_wait_time=row.peak_wait_time,
            status_changes=row.status_changes,
            trend_vs_previous_month=trend_vs_previous_month
        )

        upsert_stmt = upsert_stmt.on_duplicate_key_update(
            uptime_minutes=upsert_stmt.inserted.uptime_minutes,
            downtime_minutes=upsert_stmt.inserted.downtime_minutes,
            uptime_percentage=upsert_stmt.inserted.uptime_percentage,
            operating_hours_minutes=upsert_stmt.inserted.operating_hours_minutes,
            avg_wait_time=upsert_stmt.inserted.avg_wait_time,
            peak_wait_time=upsert_stmt.inserted.peak_wait_time,
            status_changes=upsert_stmt.inserted.status_changes,
            trend_vs_previous_month=upsert_stmt.inserted.trend_vs_previous_month
        )

        self.session.execute(upsert_stmt)

        logger.debug(f"Aggregated monthly stats for ride {ride_id}, month {year}-{month:02d}")

    def _calculate_monthly_trend(
        self,
        ride_id: int,
        current_year: int,
        current_month: int,
        current_downtime: int
    ) -> Optional[float]:
        """
        Calculate month-over-month downtime trend.

        Args:
            ride_id: Ride ID
            current_year: Current year
            current_month: Current month (1-12)
            current_downtime: Current month's downtime minutes

        Returns:
            Percentage change (e.g., 20.75 for +20.75%) or None if no previous data
        """
        # Calculate previous month (handle year boundary)
        if current_month == 1:
            previous_year = current_year - 1
            previous_month = 12
        else:
            previous_year = current_year
            previous_month = current_month - 1

        # Query previous month's downtime
        stmt = (
            select(RideMonthlyStats.downtime_minutes)
            .where(RideMonthlyStats.ride_id == ride_id)
            .where(RideMonthlyStats.year == previous_year)
            .where(RideMonthlyStats.month == previous_month)
        )

        result = self.session.execute(stmt)
        previous_row = result.first()

        if not previous_row or previous_row.downtime_minutes is None or previous_row.downtime_minutes == 0:
            return None

        # Calculate percentage change: ((current - previous) / previous) * 100
        prev_downtime = float(previous_row.downtime_minutes)
        trend = ((float(current_downtime) - prev_downtime) / prev_downtime) * 100.0
        return round(trend, 2)

    def _aggregate_parks_monthly_stats(
        self,
        year: int,
        month: int
    ) -> int:
        """
        Aggregate park-level monthly stats from ride monthly stats.

        Args:
            year: Year
            month: Month number (1-12)

        Returns:
            Number of parks processed
        """
        # Get all parks that have ride monthly stats
        stmt = (
            select(Ride.park_id)
            .distinct()
            .join(RideMonthlyStats, Ride.ride_id == RideMonthlyStats.ride_id)
            .where(RideMonthlyStats.year == year)
            .where(RideMonthlyStats.month == month)
            .order_by(Ride.park_id)
        )

        result = self.session.execute(stmt)
        park_ids = [row.park_id for row in result]

        # Aggregate each park
        for park_id in park_ids:
            self._aggregate_single_park_monthly_stats(
                park_id=park_id,
                year=year,
                month=month
            )

        logger.info(f"Processed {len(park_ids)} parks for monthly aggregation")
        return len(park_ids)

    def _aggregate_single_park_monthly_stats(
        self,
        park_id: int,
        year: int,
        month: int
    ) -> None:
        """
        Aggregate a single park's monthly stats from ride monthly stats.

        Args:
            park_id: Park ID
            year: Year
            month: Month number (1-12)
        """
        # Aggregate ride monthly stats for this park
        stmt = (
            select(
                func.count(func.distinct(RideMonthlyStats.ride_id)).label('total_rides_tracked'),
                func.avg(RideMonthlyStats.uptime_percentage).label('avg_uptime_percentage'),
                (func.sum(RideMonthlyStats.downtime_minutes) / 60.0).label('total_downtime_hours'),
                func.sum(case((RideMonthlyStats.downtime_minutes > 0, 1), else_=0)).label('rides_with_downtime'),
                func.sum(RideMonthlyStats.avg_wait_time * RideMonthlyStats.operating_hours_minutes).label('weighted_wait_sum'),
                func.sum(RideMonthlyStats.operating_hours_minutes).label('total_operating_minutes'),
                func.max(RideMonthlyStats.peak_wait_time).label('peak_wait_time')
            )
            .select_from(RideMonthlyStats)
            .join(Ride, RideMonthlyStats.ride_id == Ride.ride_id)
            .where(Ride.park_id == park_id)
            .where(RideMonthlyStats.year == year)
            .where(RideMonthlyStats.month == month)
            .where(Ride.is_active == True)
        )

        result = self.session.execute(stmt)
        row = result.one()

        if not row or row.total_rides_tracked == 0:
            logger.debug(f"No ride monthly stats found for park {park_id} in month {year}-{month:02d}")
            return

        # Calculate weighted average wait time
        avg_wait_time = None
        if row.total_operating_minutes and row.total_operating_minutes > 0 and row.weighted_wait_sum:
            avg_wait_time = round(float(row.weighted_wait_sum) / float(row.total_operating_minutes), 2)

        # Calculate park-level trend
        trend_vs_previous_month = self._calculate_park_monthly_trend(
            park_id=park_id,
            current_year=year,
            current_month=month,
            current_downtime_hours=row.total_downtime_hours
        )

        # Insert/update park monthly stats
        upsert_stmt = mysql_insert(ParkMonthlyStats).values(
            park_id=park_id,
            year=year,
            month=month,
            total_rides_tracked=row.total_rides_tracked,
            avg_uptime_percentage=round(float(row.avg_uptime_percentage), 2) if row.avg_uptime_percentage else None,
            total_downtime_hours=round(float(row.total_downtime_hours), 2),
            rides_with_downtime=row.rides_with_downtime,
            avg_wait_time=avg_wait_time,
            peak_wait_time=row.peak_wait_time,
            trend_vs_previous_month=trend_vs_previous_month
        )

        upsert_stmt = upsert_stmt.on_duplicate_key_update(
            total_rides_tracked=upsert_stmt.inserted.total_rides_tracked,
            avg_uptime_percentage=upsert_stmt.inserted.avg_uptime_percentage,
            total_downtime_hours=upsert_stmt.inserted.total_downtime_hours,
            rides_with_downtime=upsert_stmt.inserted.rides_with_downtime,
            avg_wait_time=upsert_stmt.inserted.avg_wait_time,
            peak_wait_time=upsert_stmt.inserted.peak_wait_time,
            trend_vs_previous_month=upsert_stmt.inserted.trend_vs_previous_month
        )

        self.session.execute(upsert_stmt)

        logger.debug(f"Aggregated monthly stats for park {park_id}, month {year}-{month:02d}")

    def _calculate_park_monthly_trend(
        self,
        park_id: int,
        current_year: int,
        current_month: int,
        current_downtime_hours: float
    ) -> Optional[float]:
        """
        Calculate month-over-month park downtime trend.

        Args:
            park_id: Park ID
            current_year: Current year
            current_month: Current month (1-12)
            current_downtime_hours: Current month's downtime hours

        Returns:
            Percentage change or None if no previous data
        """
        # Calculate previous month (handle year boundary)
        if current_month == 1:
            previous_year = current_year - 1
            previous_month = 12
        else:
            previous_year = current_year
            previous_month = current_month - 1

        # Query previous month's downtime
        stmt = (
            select(ParkMonthlyStats.total_downtime_hours)
            .where(ParkMonthlyStats.park_id == park_id)
            .where(ParkMonthlyStats.year == previous_year)
            .where(ParkMonthlyStats.month == previous_month)
        )

        result = self.session.execute(stmt)
        previous_row = result.first()

        if not previous_row or previous_row.total_downtime_hours is None or previous_row.total_downtime_hours == 0:
            return None

        # Calculate percentage change: ((current - previous) / previous) * 100
        prev_downtime = float(previous_row.total_downtime_hours)
        trend = ((float(current_downtime_hours) - prev_downtime) / prev_downtime) * 100.0
        return round(trend, 2)

    def _get_distinct_timezones(self) -> List[str]:
        """Get list of distinct timezones from active parks."""
        stmt = (
            select(Park.timezone)
            .distinct()
            .where(Park.is_active == True)
            .order_by(Park.timezone)
        )

        result = self.session.execute(stmt)
        return [row.timezone for row in result]

    def _create_aggregation_log(
        self,
        aggregation_date: date,
        aggregation_type: AggregationType
    ) -> int:
        """
        Create or restart aggregation log entry.

        Returns:
            log_id
        """
        # Use MySQL upsert for aggregation log
        stmt = mysql_insert(AggregationLog).values(
            aggregation_date=aggregation_date,
            aggregation_type=aggregation_type,
            status=AggregationStatus.RUNNING,
            started_at=func.now(),
            parks_processed=0,
            rides_processed=0
        )

        stmt = stmt.on_duplicate_key_update(
            status=AggregationStatus.RUNNING,
            started_at=func.now(),
            completed_at=None,
            error_message=None,
            parks_processed=0,
            rides_processed=0
        )

        result = self.session.execute(stmt)

        # For ON DUPLICATE KEY UPDATE, lastrowid may not be the actual ID
        # We need to query for it
        if result.lastrowid == 0:
            # Update case - query for the log_id
            select_stmt = (
                select(AggregationLog.log_id)
                .where(AggregationLog.aggregation_date == aggregation_date)
                .where(AggregationLog.aggregation_type == aggregation_type)
            )
            log_id = self.session.execute(select_stmt).scalar()
            return log_id
        else:
            return result.lastrowid

    def _complete_aggregation_log(
        self,
        log_id: int,
        status: AggregationStatus,
        aggregated_until_ts: Optional[datetime] = None,
        parks_processed: int = 0,
        rides_processed: int = 0,
        error_message: Optional[str] = None
    ):
        """Update aggregation log with completion status."""
        from sqlalchemy import update

        stmt = (
            update(AggregationLog)
            .where(AggregationLog.log_id == log_id)
            .values(
                status=status,
                completed_at=func.now(),
                aggregated_until_ts=aggregated_until_ts,
                parks_processed=parks_processed,
                rides_processed=rides_processed,
                error_message=error_message
            )
        )

        self.session.execute(stmt)

    def get_last_successful_aggregation(
        self,
        aggregation_type: AggregationType = AggregationType.DAILY
    ) -> Optional[Dict[str, Any]]:
        """
        Get most recent successful aggregation.

        Args:
            aggregation_type: AggregationType enum value

        Returns:
            Dictionary with aggregation log data or None
        """
        stmt = (
            select(AggregationLog)
            .where(AggregationLog.aggregation_type == aggregation_type)
            .where(AggregationLog.status == AggregationStatus.SUCCESS)
            .order_by(AggregationLog.aggregated_until_ts.desc())
            .limit(1)
        )

        result = self.session.execute(stmt)
        row = result.first()

        if not row:
            return None

        log = row[0]
        return {
            "log_id": log.log_id,
            "aggregation_date": log.aggregation_date,
            "aggregation_type": log.aggregation_type.value,
            "status": log.status.value,
            "started_at": log.started_at,
            "completed_at": log.completed_at,
            "aggregated_until_ts": log.aggregated_until_ts,
            "parks_processed": log.parks_processed,
            "rides_processed": log.rides_processed,
            "error_message": log.error_message
        }
