"""
Test fixtures for SQLAlchemy integration tests.
"""
import pytest
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from database.options import DatabaseOptions


@pytest.fixture
def postgres_options():
    """Fixture that returns DatabaseOptions for PostgreSQL."""
    return DatabaseOptions(
        drivername='postgresql',
        hostname='localhost',
        port=5432,
        database='testdb',
        username='testuser',
        password='testpass'
    )


@pytest.fixture
def sqlserver_options():
    """Fixture that returns DatabaseOptions for SQL Server."""
    return DatabaseOptions(
        drivername='mssql',
        hostname='localhost',
        port=1433,
        database='testdb',
        username='testuser',
        password='testpass',
        driver='ODBC Driver 18 for SQL Server'
    )


@pytest.fixture
def sqlite_options():
    """Fixture that returns DatabaseOptions for SQLite."""
    return DatabaseOptions(
        drivername='sqlite',
        database=':memory:'
    )


@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine."""
    engine = MagicMock(spec=Engine)
    engine.dialect.name = 'postgresql'
    engine.pool = MagicMock(spec=NullPool)
    return engine


@pytest.fixture
def mock_sqlalchemy_connection(mock_engine):
    """Create a mock SQLAlchemy connection."""
    conn = MagicMock()
    conn.engine = mock_engine
    conn.dialect = mock_engine.dialect
    conn.closed = False
    
    # Create a mock for the underlying DBAPI connection
    raw_conn = MagicMock()
    conn.driver_connection = raw_conn
    
    # Setup commonly used methods
    raw_conn.cursor.return_value = MagicMock()
    
    return conn


@pytest.fixture
def mock_postgres_conn(mock_sqlalchemy_connection):
    """Create a mock PostgreSQL connection."""
    mock_sqlalchemy_connection.engine.dialect.name = 'postgresql'
    return mock_sqlalchemy_connection


@pytest.fixture
def mock_sqlserver_conn(mock_sqlalchemy_connection):
    """Create a mock SQL Server connection."""
    mock_sqlalchemy_connection.engine.dialect.name = 'mssql'
    return mock_sqlalchemy_connection


@pytest.fixture
def mock_sqlite_conn(mock_sqlalchemy_connection):
    """Create a mock SQLite connection."""
    mock_sqlalchemy_connection.engine.dialect.name = 'sqlite'
    return mock_sqlalchemy_connection


@pytest.fixture(autouse=True)
def patch_engine_registry():
    """
    Patch the engine registry to avoid creating real engines during tests.
    
    This fixture is automatically used in all tests due to autouse=True.
    """
    with patch('database.utils.connection_utils._engine_registry', {}):
        yield


@pytest.fixture
def mock_engine_factory():
    """
    Fixture that patches get_engine_for_options to return a mock engine.
    """
    with patch('database.utils.connection_utils.get_engine_for_options') as mock_factory:
        # Setup to return a mock engine
        engine = create_engine('sqlite:///:memory:', poolclass=NullPool)
        mock_factory.return_value = engine
        yield mock_factory
