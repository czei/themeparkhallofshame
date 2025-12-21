"""
SQLAlchemy ORM Base Configuration
Provides declarative base, engine, and session management for ORM models.
"""

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, scoped_session, sessionmaker
from sqlalchemy import create_engine
from typing import Optional
import os


class Base(DeclarativeBase):
    """Base class for all ORM models"""
    pass


# Database connection configuration
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"mysql+pymysql://{os.getenv('DB_USER', 'root')}:{os.getenv('DB_PASSWORD', '')}@"
    f"{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '3306')}/"
    f"{os.getenv('DB_NAME', 'themepark_tracker_dev')}"
)

# Create engine with connection pooling and validation
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # Validate connections before using (catch stale connections)
    pool_recycle=3600,   # Recycle connections after 1 hour (prevent MySQL timeout)
    echo=os.getenv("SQL_ECHO", "false").lower() == "true",  # Log SQL queries if enabled
)

# Session factory for manual session creation (cron jobs, scripts)
SessionLocal = sessionmaker(
    bind=engine,
    expire_on_commit=False,  # Allow access to objects after commit
    autoflush=True,
    autocommit=False
)

# Scoped session for Flask request context (thread-local session management)
db_session = scoped_session(SessionLocal)


def create_session():
    """
    Factory for creating sessions outside Flask context (cron jobs, scripts).

    Usage:
        session = create_session()
        try:
            # Do work
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    Returns:
        SQLAlchemy Session instance
    """
    return SessionLocal()
