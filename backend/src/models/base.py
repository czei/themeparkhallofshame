"""
SQLAlchemy ORM Base Configuration
Provides declarative base and session management for ORM models.

IMPORTANT: Engine is imported from database.connection to ensure single source of truth.
"""

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, scoped_session, sessionmaker
from typing import Optional


class Base(DeclarativeBase):
    """Base class for all ORM models"""
    pass


def _get_engine():
    """
    Get the SQLAlchemy engine from database.connection.

    Lazy import to avoid circular dependencies during module initialization.
    """
    from database.connection import db
    return db.get_engine()


# Session factory for manual session creation (cron jobs, scripts)
# Uses engine from database.connection for consistency
SessionLocal = sessionmaker(
    bind=_get_engine(),
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
