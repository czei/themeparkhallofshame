"""
Theme Park Downtime Tracker - Database Connection Management
Provides SQLAlchemy Core connection pooling and ORM session management for MySQL.

Migration Note:
- get_db_connection() returns raw SQLAlchemy Core connections (for legacy code)
- get_db_session() returns ORM sessions (for new ORM-based code)
- Both use the same underlying engine for connection pooling
"""

from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.engine import Engine, Connection, URL
from sqlalchemy.orm import Session
from typing import Generator

try:
    from ..utils.config import (
        DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
        DB_POOL_SIZE, DB_POOL_MAX_OVERFLOW, DB_POOL_RECYCLE, DB_POOL_PRE_PING,
        config
    )
    from ..utils.logger import logger, log_database_error
except ImportError:
    from utils.config import (
        DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
        DB_POOL_SIZE, DB_POOL_MAX_OVERFLOW, DB_POOL_RECYCLE, DB_POOL_PRE_PING,
        config
    )
    from utils.logger import logger, log_database_error


class DatabaseConnection:
    """
    Manages MySQL database connections with connection pooling.

    Features:
    - Connection pooling (10 connections + 20 overflow)
    - Automatic connection recycling (every hour)
    - Health checks before connection use (pool_pre_ping)
    - Optimized for production reliability
    """

    def __init__(self):
        self._engine: Engine = None

    def get_engine(self) -> Engine:
        """
        Get or create the SQLAlchemy engine with connection pooling.

        Returns:
            SQLAlchemy Engine instance

        Raises:
            DatabaseConnectionError: If connection fails
        """
        if self._engine is None:
            try:
                # Build MySQL connection URL using URL.create() to prevent password exposure
                # This method properly handles credentials and prevents them from appearing in logs
                # Using pymysql (pure Python driver) for better compatibility
                connection_url = URL.create(
                    drivername="mysql+pymysql",
                    username=DB_USER,
                    password=DB_PASSWORD,
                    host=DB_HOST,
                    port=DB_PORT,
                    database=DB_NAME,
                    query={
                        "charset": "utf8mb4",
                        "init_command": "SET time_zone='+00:00'",  # Force UTC for all connections
                    },
                )

                # Create engine with connection pooling
                self._engine = create_engine(
                    connection_url,
                    poolclass=QueuePool,
                    pool_size=DB_POOL_SIZE,  # 10 connections
                    max_overflow=DB_POOL_MAX_OVERFLOW,  # +20 overflow
                    pool_recycle=DB_POOL_RECYCLE,  # Recycle after 1 hour
                    pool_pre_ping=DB_POOL_PRE_PING,  # Health check before use
                    echo=False,  # Set to True for SQL logging in development
                    hide_parameters=True,  # Prevent password from appearing in logs
                )

                logger.info("Database connection pool initialized", extra={
                    "host": DB_HOST,
                    "database": DB_NAME,
                    "pool_size": DB_POOL_SIZE,
                    "max_overflow": DB_POOL_MAX_OVERFLOW,
                    "environment": config.environment
                })

            except Exception as e:
                log_database_error(e, "Failed to create database engine")
                raise DatabaseConnectionError(f"Failed to create database engine: {e}")

        return self._engine

    @contextmanager
    def get_connection(self) -> Generator[Connection, None, None]:
        """
        Context manager for database connections.

        Yields:
            SQLAlchemy Connection object

        Example:
            >>> with db.get_connection() as conn:
            ...     result = conn.execute(text("SELECT * FROM parks"))
            ...     for row in result:
            ...         print(row)
        """
        engine = self.get_engine()
        connection = engine.connect()
        try:
            yield connection
            connection.commit()
        except Exception as e:
            connection.rollback()
            log_database_error(e, "Transaction failed, rolled back")
            raise
        finally:
            connection.close()

    def test_connection(self) -> bool:
        """
        Test database connectivity.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error("Database connection test failed", extra={
                "error": str(e)
            })
            return False

    def close(self):
        """Close all connections in the pool."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            logger.info("Database connection pool closed")


class DatabaseConnectionError(Exception):
    """Raised when database connection fails."""
    pass


# Global database connection instance
db = DatabaseConnection()


def get_db_connection():
    """
    Get database connection context manager.

    Returns:
        Context manager for database connections

    Example:
        >>> with get_db_connection() as conn:
        ...     result = conn.execute(text("SELECT * FROM parks"))
    """
    return db.get_connection()


def test_database_connection() -> bool:
    """
    Test database connectivity.

    Returns:
        True if connection successful, False otherwise
    """
    return db.test_connection()


# === ORM Session Management ===
# The following functions provide ORM session management using the session factory from base.py.
# This allows ORM-based repositories to coexist with raw SQL code during the migration.

@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """
    Context manager for ORM database sessions.

    Creates a new Session from the scoped session factory, ensuring proper lifecycle management.
    Sessions are automatically committed on success or rolled back on error.

    Yields:
        SQLAlchemy Session object

    Example:
        >>> from database.repositories.ride_repository_orm import RideRepository
        >>> with get_db_session() as session:
        ...     repo = RideRepository(session)
        ...     ride = repo.get_by_id(1)
        ...     print(ride.name)
    """
    from src.models.base import db_session

    # Get a real Session from the scoped_session factory
    session = db_session()

    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        log_database_error(e, "ORM transaction failed, rolled back")
        raise
    finally:
        session.close()
        db_session.remove()  # Remove scoped session to prevent connection leaks


def create_db_session() -> Session:
    """
    Create a new ORM session (for scripts and cron jobs).

    Unlike get_db_session(), this returns a session object that must be manually
    managed (commit/rollback/close). Use this for long-running scripts or when
    you need explicit session control.

    Returns:
        SQLAlchemy Session object (must be manually closed)

    Example:
        >>> from database.repositories.park_repository_orm import ParkRepository
        >>> session = create_db_session()
        >>> try:
        ...     repo = ParkRepository(session)
        ...     park = repo.get_by_id(1)
        ...     session.commit()
        ... except Exception as e:
        ...     session.rollback()
        ...     raise
        ... finally:
        ...     session.close()
    """
    from src.models.base import create_session
    return create_session()
