# Theme Park Downtime Tracker - Models Package

# Import all ORM models to register them with SQLAlchemy's declarative base
# This ensures string-based relationship() forward references can be resolved
from src.models.base import Base, SessionLocal, db_session, create_session
from src.models.orm_park import Park
from src.models.orm_ride import Ride
from src.models.orm_snapshots import RideStatusSnapshot, ParkActivitySnapshot
from src.models.orm_stats import RideDailyStats, ParkDailyStats
from src.models.orm_weather import WeatherObservation, WeatherForecast

__all__ = [
    'Base',
    'SessionLocal',
    'db_session',
    'create_session',
    'Park',
    'Ride',
    'RideStatusSnapshot',
    'ParkActivitySnapshot',
    'RideDailyStats',
    'ParkDailyStats',
    'WeatherObservation',
    'WeatherForecast',
]
