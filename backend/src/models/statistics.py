"""
Theme Park Downtime Tracker - Statistics Entity Models
Represents aggregate statistics for rides and parks.
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


@dataclass
class RideDailyStats:
    """Daily ride performance statistics."""
    stat_id: int
    ride_id: int
    stat_date: date
    uptime_minutes: int
    downtime_minutes: int
    uptime_percentage: float
    operating_hours_minutes: int
    avg_wait_time: Optional[float]
    min_wait_time: Optional[int]
    max_wait_time: Optional[int]
    peak_wait_time: Optional[int]
    status_changes: int
    longest_downtime_minutes: Optional[int]
    created_at: datetime

    @property
    def downtime_hours(self) -> float:
        """Convert downtime minutes to hours."""
        return round(self.downtime_minutes / 60.0, 2)

    def to_dict(self) -> dict:
        """Convert to dictionary for API responses."""
        return {
            "stat_id": self.stat_id,
            "ride_id": self.ride_id,
            "stat_date": self.stat_date.isoformat(),
            "uptime_minutes": self.uptime_minutes,
            "downtime_minutes": self.downtime_minutes,
            "downtime_hours": self.downtime_hours,
            "uptime_percentage": self.uptime_percentage,
            "operating_hours_minutes": self.operating_hours_minutes,
            "avg_wait_time": self.avg_wait_time,
            "peak_wait_time": self.peak_wait_time,
            "status_changes": self.status_changes,
            "longest_downtime_minutes": self.longest_downtime_minutes
        }


@dataclass
class ParkDailyStats:
    """Daily park-wide performance statistics."""
    stat_id: int
    park_id: int
    stat_date: date
    total_rides_tracked: int
    avg_uptime_percentage: Optional[float]
    total_downtime_hours: float
    rides_with_downtime: int
    avg_wait_time: Optional[float]
    peak_wait_time: Optional[int]
    operating_hours_minutes: int
    created_at: datetime

    def to_dict(self) -> dict:
        """Convert to dictionary for API responses."""
        return {
            "stat_id": self.stat_id,
            "park_id": self.park_id,
            "stat_date": self.stat_date.isoformat(),
            "total_rides_tracked": self.total_rides_tracked,
            "avg_uptime_percentage": self.avg_uptime_percentage,
            "total_downtime_hours": self.total_downtime_hours,
            "rides_with_downtime": self.rides_with_downtime,
            "avg_wait_time": self.avg_wait_time,
            "peak_wait_time": self.peak_wait_time,
            "operating_hours_minutes": self.operating_hours_minutes
        }


@dataclass
class RideWeeklyStats:
    """Weekly ride performance statistics."""
    stat_id: int
    ride_id: int
    year: int
    week_number: int
    week_start_date: date
    uptime_minutes: int
    downtime_minutes: int
    uptime_percentage: float
    operating_hours_minutes: int
    avg_wait_time: Optional[float]
    peak_wait_time: Optional[int]
    status_changes: int
    trend_vs_previous_week: Optional[float]
    created_at: datetime

    @property
    def downtime_hours(self) -> float:
        """Convert downtime minutes to hours."""
        return round(self.downtime_minutes / 60.0, 2)

    def to_dict(self) -> dict:
        """Convert to dictionary for API responses."""
        return {
            "stat_id": self.stat_id,
            "ride_id": self.ride_id,
            "year": self.year,
            "week_number": self.week_number,
            "week_start_date": self.week_start_date.isoformat(),
            "uptime_minutes": self.uptime_minutes,
            "downtime_minutes": self.downtime_minutes,
            "downtime_hours": self.downtime_hours,
            "uptime_percentage": self.uptime_percentage,
            "operating_hours_minutes": self.operating_hours_minutes,
            "avg_wait_time": self.avg_wait_time,
            "peak_wait_time": self.peak_wait_time,
            "status_changes": self.status_changes,
            "trend_vs_previous_week": self.trend_vs_previous_week
        }


@dataclass
class ParkWeeklyStats:
    """Weekly park-wide performance statistics."""
    stat_id: int
    park_id: int
    year: int
    week_number: int
    week_start_date: date
    total_rides_tracked: int
    avg_uptime_percentage: Optional[float]
    total_downtime_hours: float
    rides_with_downtime: int
    avg_wait_time: Optional[float]
    peak_wait_time: Optional[int]
    trend_vs_previous_week: Optional[float]
    created_at: datetime

    def to_dict(self) -> dict:
        """Convert to dictionary for API responses."""
        return {
            "stat_id": self.stat_id,
            "park_id": self.park_id,
            "year": self.year,
            "week_number": self.week_number,
            "week_start_date": self.week_start_date.isoformat(),
            "total_rides_tracked": self.total_rides_tracked,
            "avg_uptime_percentage": self.avg_uptime_percentage,
            "total_downtime_hours": self.total_downtime_hours,
            "rides_with_downtime": self.rides_with_downtime,
            "avg_wait_time": self.avg_wait_time,
            "peak_wait_time": self.peak_wait_time,
            "trend_vs_previous_week": self.trend_vs_previous_week
        }
