"""
Anomaly Detection
=================

Statistical anomaly detection for theme park data.
Catches unusual patterns that hard validation rules might miss.

Detection Methods:
1. Z-Score: Values > 3 standard deviations from 30-day mean
2. Sudden Change: > 200% day-over-day change in metrics
3. Data Quality: Missing snapshots, coverage gaps

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
"""

from datetime import date, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from sqlalchemy import text
from sqlalchemy.engine import Connection

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

    def __init__(self, conn: Connection, thresholds: Optional[Dict[str, float]] = None):
        """
        Initialize with database connection.

        Args:
            conn: SQLAlchemy connection
            thresholds: Optional custom thresholds (defaults to DEFAULT_THRESHOLDS)
        """
        self.conn = conn
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
        """
        baseline_start = target_date - timedelta(days=self.thresholds["baseline_days"])

        query = text("""
            WITH park_baseline AS (
                SELECT
                    park_id,
                    AVG(shame_score) AS mean_score,
                    STDDEV(shame_score) AS std_score
                FROM v_audit_park_daily
                WHERE stat_date BETWEEN :start_date AND :end_date
                AND shame_score IS NOT NULL
                GROUP BY park_id
                HAVING COUNT(*) >= 7  -- Need at least a week of data
            )
            SELECT
                vpd.park_id,
                vpd.park_name,
                vpd.stat_date,
                vpd.shame_score AS current_value,
                pb.mean_score,
                pb.std_score,
                CASE
                    WHEN pb.std_score > 0 THEN
                        (vpd.shame_score - pb.mean_score) / pb.std_score
                    ELSE 0
                END AS zscore
            FROM v_audit_park_daily vpd
            JOIN park_baseline pb ON vpd.park_id = pb.park_id
            WHERE vpd.stat_date = :target_date
            AND pb.std_score > 0
            AND ABS((vpd.shame_score - pb.mean_score) / pb.std_score) > :threshold
        """)

        try:
            result = self.conn.execute(
                query,
                {
                    "start_date": baseline_start,
                    "end_date": target_date - timedelta(days=1),
                    "target_date": target_date,
                    "threshold": self.thresholds["zscore"],
                },
            )
            rows = result.fetchall()

            anomalies = []
            for row in rows:
                row_dict = dict(row._mapping)
                zscore = row_dict["zscore"]
                severity = "CRITICAL" if abs(zscore) > 4.0 else "WARNING"

                anomalies.append(
                    Anomaly(
                        anomaly_type="zscore",
                        severity=severity,
                        entity_type="park",
                        entity_id=row_dict["park_id"],
                        entity_name=row_dict["park_name"],
                        stat_date=row_dict["stat_date"],
                        metric="shame_score",
                        current_value=float(row_dict["current_value"]),
                        expected_value=float(row_dict["mean_score"]),
                        threshold=float(zscore),
                        message=(
                            f"Shame score {row_dict['current_value']:.2f} is {abs(zscore):.1f} "
                            f"standard deviations from 30-day mean ({row_dict['mean_score']:.2f})"
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
        """
        baseline_start = target_date - timedelta(days=self.thresholds["baseline_days"])

        query = text("""
            WITH ride_baseline AS (
                SELECT
                    ride_id,
                    AVG(downtime_hours) AS mean_downtime,
                    STDDEV(downtime_hours) AS std_downtime
                FROM v_audit_ride_daily
                WHERE stat_date BETWEEN :start_date AND :end_date
                GROUP BY ride_id
                HAVING COUNT(*) >= 7 AND STDDEV(downtime_hours) > 0.1
            )
            SELECT
                vrd.ride_id,
                vrd.ride_name,
                vrd.park_name,
                vrd.stat_date,
                vrd.downtime_hours AS current_value,
                rb.mean_downtime,
                rb.std_downtime,
                CASE
                    WHEN rb.std_downtime > 0 THEN
                        (vrd.downtime_hours - rb.mean_downtime) / rb.std_downtime
                    ELSE 0
                END AS zscore
            FROM v_audit_ride_daily vrd
            JOIN ride_baseline rb ON vrd.ride_id = rb.ride_id
            WHERE vrd.stat_date = :target_date
            AND vrd.downtime_hours > 0
            AND (vrd.downtime_hours - rb.mean_downtime) / rb.std_downtime > :threshold
        """)

        try:
            result = self.conn.execute(
                query,
                {
                    "start_date": baseline_start,
                    "end_date": target_date - timedelta(days=1),
                    "target_date": target_date,
                    "threshold": self.thresholds["zscore"],
                },
            )
            rows = result.fetchall()

            anomalies = []
            for row in rows:
                row_dict = dict(row._mapping)
                zscore = row_dict["zscore"]
                # Only flag as critical if >4σ and >2 hours downtime
                severity = (
                    "CRITICAL"
                    if abs(zscore) > 4.0 and row_dict["current_value"] > 2
                    else "WARNING"
                )

                anomalies.append(
                    Anomaly(
                        anomaly_type="zscore",
                        severity=severity,
                        entity_type="ride",
                        entity_id=row_dict["ride_id"],
                        entity_name=f"{row_dict['ride_name']} ({row_dict['park_name']})",
                        stat_date=row_dict["stat_date"],
                        metric="downtime_hours",
                        current_value=float(row_dict["current_value"]),
                        expected_value=float(row_dict["mean_downtime"]),
                        threshold=float(zscore),
                        message=(
                            f"Downtime {row_dict['current_value']:.2f}h is {abs(zscore):.1f}σ "
                            f"from 30-day mean ({row_dict['mean_downtime']:.2f}h)"
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
        """
        previous_date = target_date - timedelta(days=1)

        query = text("""
            SELECT
                curr.park_id,
                curr.park_name,
                curr.stat_date,
                curr.shame_score AS current_value,
                prev.shame_score AS previous_value,
                CASE
                    WHEN prev.shame_score > 0 THEN
                        ((curr.shame_score - prev.shame_score) / prev.shame_score) * 100
                    ELSE NULL
                END AS pct_change
            FROM v_audit_park_daily curr
            JOIN v_audit_park_daily prev
                ON curr.park_id = prev.park_id
                AND prev.stat_date = :previous_date
            WHERE curr.stat_date = :target_date
            AND prev.shame_score > 0.1  -- Avoid division by tiny numbers
            AND curr.shame_score > 0.1
            AND ABS((curr.shame_score - prev.shame_score) / prev.shame_score) * 100 > :threshold
        """)

        try:
            result = self.conn.execute(
                query,
                {
                    "target_date": target_date,
                    "previous_date": previous_date,
                    "threshold": self.thresholds["sudden_change_pct"],
                },
            )
            rows = result.fetchall()

            anomalies = []
            for row in rows:
                row_dict = dict(row._mapping)
                pct_change = row_dict["pct_change"]
                direction = "increased" if pct_change > 0 else "decreased"
                severity = "WARNING"  # Sudden changes flagged, not critical

                anomalies.append(
                    Anomaly(
                        anomaly_type="sudden_change",
                        severity=severity,
                        entity_type="park",
                        entity_id=row_dict["park_id"],
                        entity_name=row_dict["park_name"],
                        stat_date=row_dict["stat_date"],
                        metric="shame_score",
                        current_value=float(row_dict["current_value"]),
                        expected_value=float(row_dict["previous_value"]),
                        threshold=float(pct_change),
                        message=(
                            f"Shame score {direction} {abs(pct_change):.0f}% "
                            f"({row_dict['previous_value']:.2f} → {row_dict['current_value']:.2f})"
                        ),
                    )
                )

            return anomalies

        except Exception as e:
            logger.error(f"Sudden change detection failed: {e}")
            return []

    def _detect_data_quality_issues(self, target_date: date) -> List[Anomaly]:
        """
        Detect parks with significant missing data.
        """
        query = text("""
            SELECT
                park_id,
                park_name,
                stat_date,
                total_rides,
                total_park_open_snapshots,
                ROUND(100.0 * total_park_open_snapshots / NULLIF(total_rides * 288, 0), 1) AS coverage_pct
            FROM v_audit_park_daily
            WHERE stat_date = :target_date
            AND total_rides > 0
            AND (total_park_open_snapshots * 100.0 / (total_rides * 288)) < :threshold
        """)

        try:
            result = self.conn.execute(
                query,
                {
                    "target_date": target_date,
                    "threshold": 100 - self.thresholds["missing_data_pct"],  # <80% coverage
                },
            )
            rows = result.fetchall()

            anomalies = []
            for row in rows:
                row_dict = dict(row._mapping)
                coverage = row_dict["coverage_pct"] or 0

                # Very low coverage is critical
                severity = "CRITICAL" if coverage < 50 else "WARNING"

                anomalies.append(
                    Anomaly(
                        anomaly_type="data_quality",
                        severity=severity,
                        entity_type="park",
                        entity_id=row_dict["park_id"],
                        entity_name=row_dict["park_name"],
                        stat_date=row_dict["stat_date"],
                        metric="snapshot_coverage",
                        current_value=float(coverage),
                        expected_value=80.0,  # Target coverage
                        threshold=self.thresholds["missing_data_pct"],
                        message=(
                            f"Only {coverage:.1f}% snapshot coverage "
                            f"({row_dict['total_park_open_snapshots']} of expected {row_dict['total_rides'] * 288})"
                        ),
                    )
                )

            return anomalies

        except Exception as e:
            logger.error(f"Data quality detection failed: {e}")
            return []

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
