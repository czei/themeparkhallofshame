"""
SQLAlchemy ORM Models: Weather Tables
WeatherObservation and WeatherForecast for weather data integration.
"""

from sqlalchemy import Integer, ForeignKey, DateTime, Numeric, SmallInteger, Index, BigInteger, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from src.models.base import Base
from datetime import datetime
from typing import Optional
from decimal import Decimal


class WeatherObservation(Base):
    """
    Historical weather observation data for theme parks.
    Collected from weather APIs and stored for correlation analysis.
    """
    __tablename__ = "weather_observations"

    # Primary Key
    observation_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # Foreign Keys
    park_id: Mapped[int] = mapped_column(
        ForeignKey("parks.park_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    # Observation Metadata
    observation_time: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        index=True,
        server_default=func.now(),
        comment="UTC timestamp of weather observation"
    )
    collected_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment="UTC timestamp when data was collected from API"
    )

    # Temperature
    temperature_c: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Temperature in Celsius"
    )
    temperature_f: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Temperature in Fahrenheit"
    )
    apparent_temperature_c: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Feels-like temperature in Celsius"
    )
    apparent_temperature_f: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Feels-like temperature in Fahrenheit"
    )

    # Wind
    wind_speed_kmh: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Wind speed in km/h"
    )
    wind_speed_mph: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Wind speed in mph"
    )
    wind_gusts_kmh: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Wind gusts in km/h"
    )
    wind_gusts_mph: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Wind gusts in mph"
    )
    wind_direction_degrees: Mapped[Optional[int]] = mapped_column(
        SmallInteger,
        comment="Wind direction in degrees (0-360)"
    )

    # Precipitation
    precipitation_mm: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(6, 2),
        comment="Total precipitation in mm"
    )
    precipitation_probability: Mapped[Optional[int]] = mapped_column(
        SmallInteger,
        comment="Probability of precipitation (0-100%)"
    )
    rain_mm: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(6, 2),
        comment="Rainfall in mm"
    )
    snowfall_mm: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(6, 2),
        comment="Snowfall in mm"
    )

    # Atmospheric Conditions
    cloud_cover_percent: Mapped[Optional[int]] = mapped_column(
        SmallInteger,
        comment="Cloud cover percentage (0-100)"
    )
    visibility_meters: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Visibility in meters"
    )
    humidity_percent: Mapped[Optional[int]] = mapped_column(
        SmallInteger,
        comment="Relative humidity percentage (0-100)"
    )
    pressure_hpa: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(6, 2),
        comment="Atmospheric pressure in hPa"
    )

    # Weather Code
    weather_code: Mapped[Optional[int]] = mapped_column(
        SmallInteger,
        index=True,
        comment="WMO weather code"
    )

    # Composite Indexes for Performance
    __table_args__ = (
        Index('idx_weather_obs_park_time', 'park_id', 'observation_time'),
        {'extend_existing': True}
    )

    # Relationships
    park: Mapped["Park"] = relationship("Park")

    def __repr__(self) -> str:
        return f"<WeatherObservation(observation_id={self.observation_id}, park_id={self.park_id}, temp={self.temperature_f}°F, time={self.observation_time})>"


class WeatherForecast(Base):
    """
    Weather forecast data for theme parks.
    Used for predictive wait time analysis and visit planning optimization.
    """
    __tablename__ = "weather_forecasts"

    # Primary Key
    forecast_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # Foreign Keys
    park_id: Mapped[int] = mapped_column(
        ForeignKey("parks.park_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    # Forecast Metadata
    issued_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        index=True,
        server_default=func.now(),
        comment="UTC timestamp when forecast was issued"
    )
    forecast_time: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment="UTC timestamp this forecast applies to"
    )

    # Temperature
    temperature_c: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Predicted temperature in Celsius"
    )
    temperature_f: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Predicted temperature in Fahrenheit"
    )
    apparent_temperature_c: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Predicted feels-like temperature in Celsius"
    )
    apparent_temperature_f: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Predicted feels-like temperature in Fahrenheit"
    )

    # Wind
    wind_speed_kmh: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Predicted wind speed in km/h"
    )
    wind_speed_mph: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Predicted wind speed in mph"
    )
    wind_gusts_kmh: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Predicted wind gusts in km/h"
    )
    wind_gusts_mph: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 2),
        comment="Predicted wind gusts in mph"
    )
    wind_direction_degrees: Mapped[Optional[int]] = mapped_column(
        SmallInteger,
        comment="Predicted wind direction in degrees (0-360)"
    )

    # Precipitation
    precipitation_mm: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(6, 2),
        comment="Predicted precipitation in mm"
    )
    precipitation_probability: Mapped[Optional[int]] = mapped_column(
        SmallInteger,
        comment="Probability of precipitation (0-100%)"
    )
    rain_mm: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(6, 2),
        comment="Predicted rainfall in mm"
    )
    snowfall_mm: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(6, 2),
        comment="Predicted snowfall in mm"
    )

    # Atmospheric Conditions
    cloud_cover_percent: Mapped[Optional[int]] = mapped_column(
        SmallInteger,
        comment="Predicted cloud cover percentage (0-100)"
    )
    visibility_meters: Mapped[Optional[int]] = mapped_column(
        Integer,
        comment="Predicted visibility in meters"
    )
    humidity_percent: Mapped[Optional[int]] = mapped_column(
        SmallInteger,
        comment="Predicted relative humidity percentage (0-100)"
    )
    pressure_hpa: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(6, 2),
        comment="Predicted atmospheric pressure in hPa"
    )

    # Weather Code
    weather_code: Mapped[Optional[int]] = mapped_column(
        SmallInteger,
        index=True,
        comment="WMO weather code"
    )

    # Composite Indexes for Performance
    __table_args__ = (
        Index('idx_weather_forecast_park_issued', 'park_id', 'issued_at'),
        {'extend_existing': True}
    )

    # Relationships
    park: Mapped["Park"] = relationship("Park")

    def __repr__(self) -> str:
        return f"<WeatherForecast(forecast_id={self.forecast_id}, park_id={self.park_id}, for={self.forecast_time}, temp={self.temperature_f}°F)>"
