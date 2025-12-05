"""
Theme Park Downtime Tracker - Database Connection Unit Tests

Tests DatabaseConnection with SQLAlchemy mocking:
- Engine creation with connection pooling configuration
- get_engine() - lazy initialization, singleton pattern
- get_connection() - context manager with commit/rollback
- test_connection() - connectivity validation
- close() - connection pool disposal
- DatabaseConnectionError exception
- Global db instance and helper functions

Priority: P2 - Infrastructure testing (40% → 80% coverage)
"""

import pytest
from unittest.mock import Mock, patch
from sqlalchemy.engine import Engine, Connection
from contextlib import contextmanager
from database.connection import (
    DatabaseConnection,
    DatabaseConnectionError,
    db,
    get_db_connection,
    test_database_connection
)


class TestDatabaseConnectionInit:
    """Test DatabaseConnection initialization."""

    def test_init_creates_instance(self):
        """DatabaseConnection should initialize with _engine as None."""
        db_conn = DatabaseConnection()

        assert db_conn._engine is None


class TestGetEngine:
    """Test get_engine() method."""

    @patch('database.connection.create_engine')
    @patch('database.connection.URL')
    def test_get_engine_creates_engine_first_time(self, mock_url, mock_create_engine):
        """get_engine() should create engine on first call."""
        db_conn = DatabaseConnection()
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        # Mock URL.create()
        mock_url.create.return_value = "mysql+pymysql://user:pass@host:3306/db"

        engine = db_conn.get_engine()

        # Should create engine
        assert mock_create_engine.called
        assert engine == mock_engine
        assert db_conn._engine == mock_engine

    @patch('database.connection.create_engine')
    @patch('database.connection.URL')
    def test_get_engine_returns_cached_engine(self, mock_url, mock_create_engine):
        """get_engine() should return cached engine on subsequent calls."""
        db_conn = DatabaseConnection()
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        mock_url.create.return_value = "mysql+pymysql://user:pass@host:3306/db"

        # First call
        engine1 = db_conn.get_engine()
        # Second call
        engine2 = db_conn.get_engine()

        # Should only create engine once
        assert mock_create_engine.call_count == 1
        assert engine1 == engine2
        assert engine1 == mock_engine

    @patch('database.connection.create_engine')
    @patch('database.connection.URL')
    def test_get_engine_configures_connection_pooling(self, mock_url, mock_create_engine):
        """get_engine() should configure connection pool with correct parameters."""
        db_conn = DatabaseConnection()
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        mock_url.create.return_value = "mysql+pymysql://user:pass@host:3306/db"

        db_conn.get_engine()

        # Verify pool configuration
        call_kwargs = mock_create_engine.call_args[1]
        assert 'pool_size' in call_kwargs
        assert 'max_overflow' in call_kwargs
        assert 'pool_recycle' in call_kwargs
        assert 'pool_pre_ping' in call_kwargs
        assert call_kwargs['hide_parameters'] is True
        assert call_kwargs['echo'] is False

    @patch('database.connection.create_engine')
    @patch('database.connection.URL')
    def test_get_engine_uses_mysql_pymysql_driver(self, mock_url, mock_create_engine):
        """get_engine() should use mysql+pymysql driver."""
        db_conn = DatabaseConnection()
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        mock_url.create.return_value = "mysql+pymysql://user:pass@host:3306/db"

        db_conn.get_engine()

        # Verify URL.create was called with correct drivername
        call_kwargs = mock_url.create.call_args[1]
        assert call_kwargs['drivername'] == "mysql+pymysql"

    @patch('database.connection.create_engine')
    @patch('database.connection.URL')
    def test_get_engine_raises_error_on_failure(self, mock_url, mock_create_engine):
        """get_engine() should raise DatabaseConnectionError on engine creation failure."""
        db_conn = DatabaseConnection()
        mock_create_engine.side_effect = Exception("Connection failed")
        mock_url.create.return_value = "mysql+pymysql://user:pass@host:3306/db"

        with pytest.raises(DatabaseConnectionError) as exc_info:
            db_conn.get_engine()

        assert "Failed to create database engine" in str(exc_info.value)


class TestGetConnection:
    """Test get_connection() context manager."""

    @patch('database.connection.DatabaseConnection.get_engine')
    def test_get_connection_yields_connection(self, mock_get_engine):
        """get_connection() should yield a connection object."""
        db_conn = DatabaseConnection()

        # Mock engine and connection
        mock_connection = Mock(spec=Connection)
        mock_engine = Mock(spec=Engine)
        mock_engine.connect.return_value = mock_connection
        mock_get_engine.return_value = mock_engine

        with db_conn.get_connection() as conn:
            assert conn == mock_connection

        # Verify connection lifecycle
        mock_engine.connect.assert_called_once()
        mock_connection.commit.assert_called_once()
        mock_connection.close.assert_called_once()

    @patch('database.connection.DatabaseConnection.get_engine')
    def test_get_connection_commits_on_success(self, mock_get_engine):
        """get_connection() should commit transaction on successful exit."""
        db_conn = DatabaseConnection()

        mock_connection = Mock(spec=Connection)
        mock_engine = Mock(spec=Engine)
        mock_engine.connect.return_value = mock_connection
        mock_get_engine.return_value = mock_engine

        with db_conn.get_connection() as conn:
            # Simulate successful operation
            pass

        mock_connection.commit.assert_called_once()
        mock_connection.rollback.assert_not_called()

    @patch('database.connection.DatabaseConnection.get_engine')
    def test_get_connection_rolls_back_on_error(self, mock_get_engine):
        """get_connection() should rollback transaction on exception."""
        db_conn = DatabaseConnection()

        mock_connection = Mock(spec=Connection)
        mock_engine = Mock(spec=Engine)
        mock_engine.connect.return_value = mock_connection
        mock_get_engine.return_value = mock_engine

        with pytest.raises(ValueError):
            with db_conn.get_connection() as conn:
                raise ValueError("Test error")

        mock_connection.rollback.assert_called_once()
        mock_connection.commit.assert_not_called()
        mock_connection.close.assert_called_once()

    @patch('database.connection.DatabaseConnection.get_engine')
    def test_get_connection_closes_on_success(self, mock_get_engine):
        """get_connection() should close connection in finally block."""
        db_conn = DatabaseConnection()

        mock_connection = Mock(spec=Connection)
        mock_engine = Mock(spec=Engine)
        mock_engine.connect.return_value = mock_connection
        mock_get_engine.return_value = mock_engine

        with db_conn.get_connection() as conn:
            pass

        mock_connection.close.assert_called_once()

    @patch('database.connection.DatabaseConnection.get_engine')
    def test_get_connection_closes_on_error(self, mock_get_engine):
        """get_connection() should close connection even on exception."""
        db_conn = DatabaseConnection()

        mock_connection = Mock(spec=Connection)
        mock_engine = Mock(spec=Engine)
        mock_engine.connect.return_value = mock_connection
        mock_get_engine.return_value = mock_engine

        with pytest.raises(ValueError):
            with db_conn.get_connection() as conn:
                raise ValueError("Test error")

        mock_connection.close.assert_called_once()


class TestTestConnection:
    """Test test_connection() method."""

    @patch('database.connection.DatabaseConnection.get_connection')
    def test_test_connection_success(self, mock_get_connection):
        """test_connection() should return True on successful connection."""
        db_conn = DatabaseConnection()

        # Mock successful connection
        mock_conn = Mock(spec=Connection)
        mock_result = Mock()
        mock_result.fetchone.return_value = (1,)
        mock_conn.execute.return_value = mock_result

        # Make get_connection return a context manager
        @contextmanager
        def mock_context():
            yield mock_conn

        mock_get_connection.return_value = mock_context()

        result = db_conn.test_connection()

        assert result is True
        mock_conn.execute.assert_called_once()

    @patch('database.connection.DatabaseConnection.get_connection')
    def test_test_connection_failure(self, mock_get_connection):
        """test_connection() should return False on connection failure."""
        db_conn = DatabaseConnection()

        # Mock failed connection
        mock_get_connection.side_effect = Exception("Connection refused")

        result = db_conn.test_connection()

        assert result is False


class TestClose:
    """Test close() method."""

    def test_close_disposes_engine(self):
        """close() should dispose of the engine."""
        db_conn = DatabaseConnection()

        # Set up a mock engine
        mock_engine = Mock(spec=Engine)
        db_conn._engine = mock_engine

        db_conn.close()

        mock_engine.dispose.assert_called_once()
        assert db_conn._engine is None

    def test_close_when_no_engine(self):
        """close() should handle case where engine is None."""
        db_conn = DatabaseConnection()

        # No engine created yet
        assert db_conn._engine is None

        # Should not raise an error
        db_conn.close()

        assert db_conn._engine is None


class TestGlobalInstance:
    """Test global db instance and helper functions."""

    def test_global_db_instance_exists(self):
        """Global db instance should be initialized."""
        assert db is not None
        assert isinstance(db, DatabaseConnection)

    @patch('database.connection.db')
    def test_get_db_connection_returns_connection(self, mock_db):
        """get_db_connection() should return db.get_connection()."""
        mock_connection_manager = Mock()
        mock_db.get_connection.return_value = mock_connection_manager

        result = get_db_connection()

        assert result == mock_connection_manager
        mock_db.get_connection.assert_called_once()

    @patch('database.connection.db')
    def test_test_database_connection_calls_db_method(self, mock_db):
        """test_database_connection() should call db.test_connection()."""
        mock_db.test_connection.return_value = True

        result = test_database_connection()

        assert result is True
        mock_db.test_connection.assert_called_once()


class TestDatabaseConnectionError:
    """Test DatabaseConnectionError exception."""

    def test_database_connection_error_is_exception(self):
        """DatabaseConnectionError should be an Exception subclass."""
        error = DatabaseConnectionError("Test error")

        assert isinstance(error, Exception)
        assert str(error) == "Test error"

    def test_database_connection_error_can_be_raised(self):
        """DatabaseConnectionError should be raisable."""
        with pytest.raises(DatabaseConnectionError) as exc_info:
            raise DatabaseConnectionError("Connection failed")

        assert "Connection failed" in str(exc_info.value)


class TestEdgeCases:
    """Test edge cases for database connection."""

    @patch('database.connection.create_engine')
    @patch('database.connection.URL')
    def test_multiple_get_engine_calls_singleton(self, mock_url, mock_create_engine):
        """Multiple get_engine() calls should return same engine instance."""
        db_conn = DatabaseConnection()
        mock_engine = Mock(spec=Engine)
        mock_create_engine.return_value = mock_engine
        mock_url.create.return_value = "mysql+pymysql://user:pass@host:3306/db"

        # Multiple calls
        engine1 = db_conn.get_engine()
        engine2 = db_conn.get_engine()
        engine3 = db_conn.get_engine()

        # All should be the same instance
        assert engine1 is engine2
        assert engine2 is engine3
        # Engine created only once
        assert mock_create_engine.call_count == 1

    @patch('database.connection.DatabaseConnection.get_connection')
    def test_nested_get_connection_calls(self, mock_get_connection):
        """Nested get_connection() calls should work correctly."""
        db_conn = DatabaseConnection()

        mock_conn1 = Mock(spec=Connection)
        mock_conn2 = Mock(spec=Connection)

        @contextmanager
        def mock_context1():
            yield mock_conn1

        @contextmanager
        def mock_context2():
            yield mock_conn2

        mock_get_connection.side_effect = [mock_context1(), mock_context2()]

        # Nested connections
        with db_conn.get_connection() as conn1:
            assert conn1 == mock_conn1
            with db_conn.get_connection() as conn2:
                assert conn2 == mock_conn2

    def test_close_then_get_engine_recreates(self):
        """Calling get_engine() after close() should recreate engine."""
        db_conn = DatabaseConnection()

        with patch('database.connection.create_engine') as mock_create:
            with patch('database.connection.URL'):
                mock_engine1 = Mock(spec=Engine)
                mock_engine2 = Mock(spec=Engine)
                mock_create.side_effect = [mock_engine1, mock_engine2]

                # Get engine, close, get again
                engine1 = db_conn.get_engine()
                db_conn.close()
                engine2 = db_conn.get_engine()

                # Should create two different engines
                assert mock_create.call_count == 2
                assert engine1 == mock_engine1
                assert engine2 == mock_engine2
