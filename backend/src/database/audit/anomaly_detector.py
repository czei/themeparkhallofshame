"""
Anomaly Detection (Lightweight Version)
========================================

Statistical anomaly detection for theme park data.
Catches unusual patterns that hard validation rules might miss.

Uses pre-aggregated daily stats tables for fast queries:
- park_daily_stats: Park-level metrics
- ride_daily_stats: Ride-level metrics

Detection Methods:
1. Z-Score: Values > 3 standard deviations from 30-day mean
2. Sudden Change: > 200% day-over-day change in metrics
3. Data Quality: Missing data (parks with no ride stats)

Anomaly Severity:
- CRITICAL: Likely data error, requires review before publishing
- WARNING: Unusual pattern, flag for user (link to methodology page)
- INFO: Notable but not concerning, log only

Usage:
    from database.audit import AnomalyDetector

    detector = AnomalyDetector(conn)
    anomalies = detector.detect_anomalies(target_date)

    for anomaly in anomalies:
        if anomaly.severity == 'WARNING':
            flag_for_review(anomaly)

Created: 2024-11 (Data Accuracy Audit Framework)
Updated: 2024-11 (Rewritten to use pre-aggregated tables)
"""

from datetime import date, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from sqlalchemy import func, and_, case
from sqlalchemy.orm import Session

from src.models.orm_park import Park
from src.models.orm_ride import Ride
from src.models.orm_stats import ParkDailyStats, RideDailyStats
from utils.logger import logger


@dataclass
class Anomaly:
    """Detected anomaly in the data."""

    anomaly_type: str  # zscore, sudden_change, data_quality
    severity: str  # CRITICAL, WARNING, INFO
    entity_type: str  # park, ride
    entity_id: int
    entity_name: str
    stat_date: date
    metric: str  # shame_score, downtime_hours, etc.
    current_value: float
    expected_value: Optional[float]  # Mean or previous value
    threshold: Optional[float]  # Z-score or % change
    message: str
    methodology_url: str = "/about#methodology"


class AnomalyDetector:
    """
    Detects statistical anomalies in aggregated data.

    Thresholds are configurable but default to:
    - Z-score: 3.0 (3 standard deviations)
    - Sudden change: 200% (3x day-over-day)
    - Missing data: 20% (>20% missing snapshots)
    """

    DEFAULT_THRESHOLDS = {
        "zscore": 3.0,  # 3 standard deviations
        "sudden_change_pct": 200,  # 200% change
        "missing_data_pct": 20,  # >20% missing
        "baseline_days": 30,  # Days for baseline calculation
    }

    def __init__(self, session: Session, thresholds: Optional[Dict[str, float]] = None):
        """
        Initialize with database session.

        Args:
            session: SQLAlchemy ORM session
            thresholds: Optional custom thresholds (defaults to DEFAULT_THRESHOLDS)
        """
        self.session = session
        self.thresholds = {**self.DEFAULT_THRESHOLDS, **(thresholds or {})}

    def detect_anomalies(self, target_date: date) -> List[Anomaly]:
        """
        Run all anomaly detection for a target date.

        Args:
            target_date: Date to analyze

        Returns:
            List of detected anomalies
        """
        anomalies = []

        # Z-score anomalies for park shame scores
        anomalies.extend(self._detect_park_zscore_anomalies(target_date))

        # Z-score anomalies for ride downtime
        anomalies.extend(self._detect_ride_zscore_anomalies(target_date))

        # Sudden change detection
        anomalies.extend(self._detect_sudden_changes(target_date))

        # Data quality issues
        anomalies.extend(self._detect_data_quality_issues(target_date))

        # Log summary
        if anomalies:
            critical = [a for a in anomalies if a.severity == "CRITICAL"]
            warning = [a for a in anomalies if a.severity == "WARNING"]
            logger.warning(
                f"Anomaly detection for {target_date}: "
                f"{len(critical)} critical, {len(warning)} warnings, "
                f"{len(anomalies)} total"
            )
        else:
            logger.info(f"No anomalies detected for {target_date}")

        return anomalies

    def _detect_park_zscore_anomalies(self, target_date: date) -> List[Anomaly]:
        """
        Detect parks with shame scores > 3σ from their 30-day mean.

        A sudden spike in shame score could indicate:
        - Real operational issues (valid)
        - Data collection error (needs investigation)

        Uses: park_daily_stats (pre-aggregated)
        """
        baseline_start = target_date - timedelta(days=self.thresholds["baseline_days"])
        baseline_end = target_date - timedelta(days=1)

        try:
            # Build baseline statistics subquery
            baseline = (
                self.session.query(
                    ParkDailyStats.park_id,
                    func.avg(ParkDailyStats.shame_score).label('mean_score'),
                    func.stddev(ParkDailyStats.shame_score).label('std_score'),
                    func.count().label('count')
                )
                .filter(
                    and_(
                        ParkDailyStats.stat_date >= baseline_start,
                        ParkDailyStats.stat_date <= baseline_end,
                        ParkDailyStats.shame_score.isnot(None)
                    )
                )
                .group_by(ParkDailyStats.park_id)
                .having(func.count() >= 7)  # Need at least a week of data
                .subquery()
            )

            # Calculate z-scores for target date
            zscore_expr = case(
                (baseline.c.std_score > 0,
                 (ParkDailyStats.shame_score - baseline.c.mean_score) / baseline.c.std_score),
                else_=0
            )

            results = (
                self.session.query(
                    ParkDailyStats.park_id,
                    Park.name.label('park_name'),
                    ParkDailyStats.stat_date,
                    ParkDailyStats.shame_score.label('current_value'),
                    baseline.c.mean_score,
                    baseline.c.std_score,
                    zscore_expr.label('zscore')
                )
                .join(Park, ParkDailyStats.park_id == Park.park_id)
                .join(baseline, ParkDailyStats.park_id == baseline.c.park_id)
                .filter(
                    and_(
                        ParkDailyStats.stat_date == target_date,
                        baseline.c.std_score > 0,
                        func.abs(zscore_expr) > self.thresholds["zscore"]
                    )
                )
                .all()
            )

            anomalies = []
            for row in results:
                zscore = float(row.zscore)
                severity = "CRITICAL" if abs(zscore) > 4.0 else "WARNING"

                anomalies.append(
                    Anomaly(
                        anomaly_type="zscore",
                        severity=severity,
                        entity_type="park",
                        entity_id=row.park_id,
                        entity_name=row.park_name,
                        stat_date=row.stat_date,
                        metric="shame_score",
                        current_value=float(row.current_value),
                        expected_value=float(row.mean_score),
                        threshold=zscore,
                        message=(
                            f"Shame score {row.current_value:.2f} is {abs(zscore):.1f} "
                            f"standard deviations from 30-day mean ({row.mean_score:.2f})"
                        ),
                    )
                )

            return anomalies

        except Exception as e:
            logger.error(f"Park Z-score detection failed: {e}")
            return []

    def _detect_ride_zscore_anomalies(self, target_date: date) -> List[Anomaly]:
        """
        Detect rides with downtime > 3σ from their 30-day mean.

        Uses: ride_daily_stats (pre-aggregated)
        Note: downtime_minutes is converted to hours for display
        """
        baseline_start = target_date - timedelta(days=self.thresholds["baseline_days"])
        baseline_end = target_date - timedelta(days=1)

        try:
            # Build baseline statistics subquery (downtime in hours)
            baseline = (
                self.session.query(
                    RideDailyStats.ride_id,
                    func.avg(RideDailyStats.downtime_minutes / 60.0).label('mean_downtime'),
                    func.stddev(RideDailyStats.downtime_minutes / 60.0).label('std_downtime'),
                    func.count().label('count')
                )
                .filter(
                    and_(
                        RideDailyStats.stat_date >= baseline_start,
                        RideDailyStats.stat_date <= baseline_end
                    )
                )
                .group_by(RideDailyStats.ride_id)
                .having(
                    and_(
                        func.count() >= 7,
                        func.stddev(RideDailyStats.downtime_minutes / 60.0) > 0.1
                    )
                )
                .subquery()
            )

            # Calculate current downtime in hours
            current_downtime_hours = RideDailyStats.downtime_minutes / 60.0

            # Calculate z-scores for target date
            zscore_expr = case(
                (baseline.c.std_downtime > 0,
                 (current_downtime_hours - baseline.c.mean_downtime) / baseline.c.std_downtime),
                else_=0
            )

            results = (
                self.session.query(
                    RideDailyStats.ride_id,
                    Ride.name.label('ride_name'),
                    Park.name.label('park_name'),
                    RideDailyStats.stat_date,
                    func.round(current_downtime_hours, 2).label('current_value'),
                    baseline.c.mean_downtime,
                    baseline.c.std_downtime,
                    zscore_expr.label('zscore')
                )
                .join(Ride, RideDailyStats.ride_id == Ride.ride_id)
                .join(Park, Ride.park_id == Park.park_id)
                .join(baseline, RideDailyStats.ride_id == baseline.c.ride_id)
                .filter(
                    and_(
                        RideDailyStats.stat_date == target_date,
                        RideDailyStats.downtime_minutes > 0,
                        zscore_expr > self.thresholds["zscore"]
                    )
                )
                .all()
            )

            anomalies = []
            for row in results:
                zscore = float(row.zscore)
                current_value = float(row.current_value)
                # Only flag as critical if >4σ and >2 hours downtime
                severity = (
                    "CRITICAL"
                    if abs(zscore) > 4.0 and current_value > 2
                    else "WARNING"
                )

                anomalies.append(
                    Anomaly(
                        anomaly_type="zscore",
                        severity=severity,
                        entity_type="ride",
                        entity_id=row.ride_id,
                        entity_name=f"{row.ride_name} ({row.park_name})",
                        stat_date=row.stat_date,
                        metric="downtime_hours",
                        current_value=current_value,
                        expected_value=float(row.mean_downtime),
                        threshold=zscore,
                        message=(
                            f"Downtime {current_value:.2f}h is {abs(zscore):.1f}σ "
                            f"from 30-day mean ({row.mean_downtime:.2f}h)"
                        ),
                    )
                )

            return anomalies

        except Exception as e:
            logger.error(f"Ride Z-score detection failed: {e}")
            return []

    def _detect_sudden_changes(self, target_date: date) -> List[Anomaly]:
        """
        Detect day-over-day changes > 200% in park shame scores.

        Large sudden changes might indicate:
        - Major incident (valid)
        - Data collection issue (needs investigation)

        Uses: park_daily_stats (pre-aggregated)
        """
        previous_date = target_date - timedelta(days=1)

        try:
            # Alias for current and previous stats
            from sqlalchemy.orm import aliased
            curr = aliased(ParkDailyStats)
            prev = aliased(ParkDailyStats)

            # Calculate percentage change
            pct_change_expr = case(
                (prev.shame_score > 0,
                 ((curr.shame_score - prev.shame_score) / prev.shame_score) * 100),
                else_=None
            )

            results = (
                self.session.query(
                    curr.park_id,
                    Park.name.label('park_name'),
                    curr.stat_date,
                    curr.shame_score.label('current_value'),
                    prev.shame_score.label('previous_value'),
                    pct_change_expr.label('pct_change')
                )
                .join(Park, curr.park_id == Park.park_id)
                .join(
                    prev,
                    and_(
                        curr.park_id == prev.park_id,
                        prev.stat_date == previous_date
                    )
                )
                .filter(
                    and_(
                        curr.stat_date == target_date,
                        prev.shame_score > 0.1,  # Avoid division by tiny numbers
                        curr.shame_score > 0.1,
                        func.abs(pct_change_expr) > self.thresholds["sudden_change_pct"]
                    )
                )
                .all()
            )

            anomalies = []
            for row in results:
                pct_change = float(row.pct_change)
                direction = "increased" if pct_change > 0 else "decreased"
                severity = "WARNING"  # Sudden changes flagged, not critical

                anomalies.append(
                    Anomaly(
                        anomaly_type="sudden_change",
                        severity=severity,
                        entity_type="park",
                        entity_id=row.park_id,
                        entity_name=row.park_name,
                        stat_date=row.stat_date,
                        metric="shame_score",
                        current_value=float(row.current_value),
                        expected_value=float(row.previous_value),
                        threshold=pct_change,
                        message=(
                            f"Shame score {direction} {abs(pct_change):.0f}% "
                            f"({row.previous_value:.2f} → {row.current_value:.2f})"
                        ),
                    )
                )

            return anomalies

        except Exception as e:
            logger.error(f"Sudden change detection failed: {e}")
            return []

    def _detect_data_quality_issues(self, target_date: date) -> List[Anomaly]:
        """
        Detect parks with missing or incomplete data.

        Uses: park_daily_stats (pre-aggregated)

        Checks for:
        1. Active parks with no stats for the target date
        2. Parks with very few rides tracked (potential data collection issue)
        """
        anomalies = []

        # Check 1: Active parks with no daily stats
        try:
            # Subquery for parks with stats on target date
            parks_with_stats = (
                self.session.query(ParkDailyStats.park_id)
                .filter(ParkDailyStats.stat_date == target_date)
                .subquery()
            )

            # Find active parks WITHOUT stats
            missing_parks = (
                self.session.query(
                    Park.park_id,
                    Park.name.label('park_name')
                )
                .filter(
                    and_(
                        Park.is_active.is_(True),
                        ~Park.park_id.in_(parks_with_stats)
                    )
                )
                .all()
            )

            for row in missing_parks:
                anomalies.append(
                    Anomaly(
                        anomaly_type="data_quality",
                        severity="WARNING",
                        entity_type="park",
                        entity_id=row.park_id,
                        entity_name=row.park_name,
                        stat_date=target_date,
                        metric="missing_daily_stats",
                        current_value=0,
                        expected_value=1.0,  # Expected to have stats
                        threshold=0,
                        message="No daily stats recorded (park may be closed)",
                    )
                )

        except Exception as e:
            logger.error(f"Missing data detection failed: {e}")

        # Check 2: Parks with abnormally low ride counts
        try:
            start_date = target_date - timedelta(days=14)
            end_date = target_date - timedelta(days=1)

            # Build average rides subquery
            park_avg_rides = (
                self.session.query(
                    ParkDailyStats.park_id,
                    func.avg(ParkDailyStats.total_rides_tracked).label('avg_rides')
                )
                .filter(
                    and_(
                        ParkDailyStats.stat_date >= start_date,
                        ParkDailyStats.stat_date <= end_date
                    )
                )
                .group_by(ParkDailyStats.park_id)
                .having(func.avg(ParkDailyStats.total_rides_tracked) > 5)
                .subquery()
            )

            # Calculate percentage of normal
            pct_of_normal = func.round(
                100.0 * ParkDailyStats.total_rides_tracked / park_avg_rides.c.avg_rides,
                1
            )

            results = (
                self.session.query(
                    ParkDailyStats.park_id,
                    Park.name.label('park_name'),
                    ParkDailyStats.stat_date,
                    ParkDailyStats.total_rides_tracked.label('current_rides'),
                    park_avg_rides.c.avg_rides,
                    pct_of_normal.label('pct_of_normal')
                )
                .join(Park, ParkDailyStats.park_id == Park.park_id)
                .join(park_avg_rides, ParkDailyStats.park_id == park_avg_rides.c.park_id)
                .filter(
                    and_(
                        ParkDailyStats.stat_date == target_date,
                        ParkDailyStats.total_rides_tracked < park_avg_rides.c.avg_rides * 0.5
                    )
                )
                .all()
            )

            for row in results:
                pct = float(row.pct_of_normal) if row.pct_of_normal else 0

                severity = "CRITICAL" if pct < 25 else "WARNING"

                anomalies.append(
                    Anomaly(
                        anomaly_type="data_quality",
                        severity=severity,
                        entity_type="park",
                        entity_id=row.park_id,
                        entity_name=row.park_name,
                        stat_date=row.stat_date,
                        metric="rides_tracked",
                        current_value=float(row.current_rides),
                        expected_value=float(row.avg_rides),
                        threshold=50.0,  # <50% of normal
                        message=(
                            f"Only {row.current_rides} rides tracked "
                            f"({pct:.0f}% of normal {row.avg_rides:.0f})"
                        ),
                    )
                )

        except Exception as e:
            logger.error(f"Low ride count detection failed: {e}")

        return anomalies

    def get_flagged_entities(
        self, target_date: date, severity_filter: Optional[str] = None
    ) -> Dict[str, List[int]]:
        """
        Get entity IDs that should be flagged in the UI.

        Returns:
            Dict with 'parks' and 'rides' lists of IDs to flag
        """
        anomalies = self.detect_anomalies(target_date)

        if severity_filter:
            anomalies = [a for a in anomalies if a.severity == severity_filter]

        flagged = {"parks": [], "rides": []}

        for anomaly in anomalies:
            if anomaly.entity_type == "park":
                flagged["parks"].append(anomaly.entity_id)
            else:
                flagged["rides"].append(anomaly.entity_id)

        return flagged

    def to_dict(self, anomalies: List[Anomaly]) -> List[Dict[str, Any]]:
        """Convert anomalies to JSON-serializable dicts."""
        return [
            {
                "anomaly_type": a.anomaly_type,
                "severity": a.severity,
                "entity_type": a.entity_type,
                "entity_id": a.entity_id,
                "entity_name": a.entity_name,
                "stat_date": a.stat_date.isoformat(),
                "metric": a.metric,
                "current_value": a.current_value,
                "expected_value": a.expected_value,
                "threshold": a.threshold,
                "message": a.message,
                "methodology_url": a.methodology_url,
            }
            for a in anomalies
        ]
