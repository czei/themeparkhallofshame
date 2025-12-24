# Theme Park Downtime Tracker - Models Package

# Import all ORM models to register them with SQLAlchemy's declarative base
# This ensures string-based relationship() forward references can be resolved
from src.models.base import Base, SessionLocal, db_session, create_session
from src.models.orm_park import Park
from src.models.orm_ride import Ride
from src.models.orm_classification import RideClassification
from src.models.orm_schedule import ParkSchedule
from src.models.orm_snapshots import RideStatusSnapshot, ParkActivitySnapshot
from src.models.orm_status_change import RideStatusChange
from src.models.orm_stats import (
    RideDailyStats, ParkDailyStats, RideWeeklyStats, ParkWeeklyStats,
    RideHourlyStats, ParkHourlyStats
)
from src.models.orm_weather import WeatherObservation, WeatherForecast
from src.models.orm_aggregation import AggregationLog, AggregationType, AggregationStatus
from src.models.orm_data_quality import DataQualityIssue
from src.models.orm_operating_session import ParkOperatingSession

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
    'RideHourlyStats',
    'ParkHourlyStats',
    'WeatherObservation',
    'WeatherForecast',
    'AggregationLog',
    'AggregationType',
    'AggregationStatus',
    'DataQualityIssue',
    'ParkOperatingSession',
]
