# Theme Park Downtime Tracker - Models Package

# Import all ORM models to register them with SQLAlchemy's declarative base
# This ensures string-based relationship() forward references can be resolved
# IMPORTANT: Use relative imports to avoid duplicate module loading issues
from .base import Base, SessionLocal, db_session, create_session
from .orm_park import Park
from .orm_ride import Ride
from .orm_classification import RideClassification
from .orm_schedule import ParkSchedule
from .orm_snapshots import RideStatusSnapshot, ParkActivitySnapshot
from .orm_status_change import RideStatusChange
from .orm_stats import (
    RideDailyStats, ParkDailyStats, RideWeeklyStats, ParkWeeklyStats,
    RideMonthlyStats, ParkMonthlyStats,
    RideHourlyStats, ParkHourlyStats, ParkLiveRankings, ParkLiveRankingsStaging,
    RideLiveRankings, RideLiveRankingsStaging
)
from .orm_weather import WeatherObservation, WeatherForecast
from .orm_aggregation import AggregationLog, AggregationType, AggregationStatus
from .orm_data_quality import DataQualityIssue
from .orm_operating_session import ParkOperatingSession
# Feature 004: Data Warehouse models
from .orm_import import ImportCheckpoint, ImportStatus
from .orm_metadata import EntityMetadata, IndoorOutdoor
from .orm_queue import QueueData, QueueType
from .orm_storage import StorageMetrics
from .orm_quality_log import DataQualityLog, LogIssueType, ResolutionStatus

__all__ = [
    'Base',
    'SessionLocal',
    'db_session',
    'create_session',
    'Park',
    'ParkSchedule',
    'Ride',
    'RideClassification',
    'RideStatusSnapshot',
    'RideStatusChange',
    'ParkActivitySnapshot',
    'RideDailyStats',
    'ParkDailyStats',
    'RideWeeklyStats',
    'ParkWeeklyStats',
    'RideMonthlyStats',
    'ParkMonthlyStats',
    'RideHourlyStats',
    'ParkHourlyStats',
    'ParkLiveRankings',
    'ParkLiveRankingsStaging',
    'RideLiveRankings',
    'RideLiveRankingsStaging',
    'WeatherObservation',
    'WeatherForecast',
    'AggregationLog',
    'AggregationType',
    'AggregationStatus',
    'DataQualityIssue',
    'ParkOperatingSession',
    # Feature 004: Data Warehouse exports
    'ImportCheckpoint',
    'ImportStatus',
    'EntityMetadata',
    'IndoorOutdoor',
    'QueueData',
    'QueueType',
    'StorageMetrics',
    'DataQualityLog',
    'LogIssueType',
    'ResolutionStatus',
]
