"""
Unit tests for database utility functions.
"""

import logging

import pytest
from database.sql import quote_identifier, standardize_placeholders
from database.utils.connection_utils import get_dialect_name
from database.utils.sql_generation import build_insert_sql

logger = logging.getLogger(__name__)


def _create_mock_connection(mocker, db_type):
    """Helper to create a mock database connection of specified type"""
    # For supported database types, create with full MagicMock capabilities
    mock_conn = mocker.MagicMock()

    # Configure connection class to appear as the specified database type
    if db_type == 'postgresql':
        # Set PostgreSQL-specific attributes that the detection function checks
        mock_conn.__class__.__module__ = 'psycopg'
        mock_conn.__class__.__name__ = 'Connection'
        mock_conn.info = {'dbname': 'test_db', 'user': 'test_user'}
        mock_conn.pgconn = mocker.MagicMock()
        # Add dialect for SQLAlchemy compatibility - must be a PropertyMock to return string
        mock_dialect = mocker.MagicMock()
        mock_dialect.name = 'postgresql'
        type(mock_conn).dialect = mocker.PropertyMock(return_value=mock_dialect)
    elif db_type == 'sqlite':
        # Set SQLite-specific attributes that the detection function checks
        mock_conn.__class__.__module__ = 'sqlite3'
        mock_conn.__class__.__name__ = 'Connection'
        mock_conn.execute = mocker.MagicMock()
        # Add dialect for SQLAlchemy compatibility - must be a PropertyMock to return string
        mock_dialect = mocker.MagicMock()
        mock_dialect.name = 'sqlite'
        type(mock_conn).dialect = mocker.PropertyMock(return_value=mock_dialect)

    # Set up common connection properties
    mock_conn.in_transaction = False
    mock_conn.driver_connection = mock_conn  # Self-reference for unwrapping checks

    return mock_conn


def _create_mock_sqlalchemy_connection(mocker, db_type):
    """Helper to create a mock SQLAlchemy connection of specified type"""
    mock_sa_conn = mocker.MagicMock()
    mock_dbapi_conn = _create_mock_connection(mocker, db_type)

    # Set up SQLAlchemy connection attributes
    mock_sa_conn.connection = mock_dbapi_conn  # Point to the DBAPI connection
    mock_sa_conn.driver_connection = mock_dbapi_conn  # For SQLAlchemy 2.0

    # Set up engine and dialect attributes
    mock_engine = mocker.MagicMock()
    mock_engine.dialect = mocker.MagicMock()
    mock_engine.dialect.name = db_type

    mock_sa_conn.engine = mock_engine
    mock_sa_conn.dialect = mock_engine.dialect

    # Set closed attribute for connection validity checks
    mock_sa_conn.closed = False

    return mock_sa_conn


def _create_mock_transaction(mocker, connection, db_type):
    """Helper to create a mock transaction that wraps a connection"""
    mock_tx = mocker.MagicMock()

    # Set the connection as an attribute
    mock_tx.connection = connection

    # Add connection property that returns the underlying connection
    type(mock_tx).connection = mocker.PropertyMock(return_value=connection)

    # Set driver_connection for unwrapping logic
    mock_tx.driver_connection = connection.driver_connection

    # Set up dialect for SQLAlchemy compatibility
    mock_dialect = mocker.MagicMock()
    mock_dialect.name = db_type
    type(mock_tx).dialect = mocker.PropertyMock(return_value=mock_dialect)

    return mock_tx


class TestDatabaseUtilities:
    """Tests for general database utility functions"""

    @pytest.mark.parametrize(('identifier', 'dialect', 'expected'), [
        # PostgreSQL
        ('table_name', 'postgresql', '"table_name"'),
        ('column.with.dots', 'postgresql', '"column.with.dots"'),
        ('table"quoted', 'postgresql', '"table""quoted"'),
        # SQLite
        ('table_name', 'sqlite', '"table_name"'),
        ('table"quoted', 'sqlite', '"table""quoted"'),
    ])
    def test_quote_identifier(self, identifier, dialect, expected):
        """Test identifier quoting for all database types"""
        assert quote_identifier(identifier, dialect) == expected

    def test_quote_identifier_error(self):
        """Test error handling in quote_identifier"""
        with pytest.raises(ValueError, match='Unknown dialect'):
            quote_identifier('table_name', 'unknown')

    def test_connection_detection_with_postgres(self, mocker):
        """Test PostgreSQL connection type detection"""
        # Create connection that looks like a postgres connection
        pg_conn = _create_mock_connection(mocker, 'postgresql')

        # Test detection with actual implementation, not mocked function
        assert get_dialect_name(pg_conn) == 'postgresql'

        # Test transaction object that wraps a connection
        tx = _create_mock_transaction(mocker, pg_conn, 'postgresql')
        assert get_dialect_name(tx) == 'postgresql'

        # Test with SQLAlchemy connection
        sa_pg_conn = _create_mock_sqlalchemy_connection(mocker, 'postgresql')
        assert get_dialect_name(sa_pg_conn) == 'postgresql'

    def test_connection_detection_with_sqlite(self, mocker):
        """Test SQLite connection type detection"""
        # Create connection that looks like a SQLite connection
        sqlite_conn = _create_mock_connection(mocker, 'sqlite')

        # Test detection with actual implementation, not mocked function
        assert get_dialect_name(sqlite_conn) == 'sqlite'

        # Test with SQLAlchemy connection
        sa_sqlite_conn = _create_mock_sqlalchemy_connection(mocker, 'sqlite')
        assert get_dialect_name(sa_sqlite_conn) == 'sqlite'

    def test_connection_type_detection_postgres(self, mocker):
        """Test PostgreSQL connection type detection"""
        # Create mock PostgreSQL connection with proper attributes
        pg_conn = _create_mock_connection(mocker, 'postgresql')

        # Test dialect name detection
        assert get_dialect_name(pg_conn) == 'postgresql'

        # Test with transaction object that wraps a connection
        tx = _create_mock_transaction(mocker, pg_conn, 'postgresql')
        assert get_dialect_name(tx) == 'postgresql'

        # Test with SQLAlchemy connection
        sa_pg_conn = _create_mock_sqlalchemy_connection(mocker, 'postgresql')
        assert get_dialect_name(sa_pg_conn) == 'postgresql'

    def test_connection_type_detection_sqlite(self, mocker):
        """Test SQLite connection type detection"""
        # Create mock SQLite connection with proper attributes
        sl_conn = _create_mock_connection(mocker, 'sqlite')

        # Test dialect name detection
        assert get_dialect_name(sl_conn) == 'sqlite'

        # Test with transaction object that wraps a connection
        tx = _create_mock_transaction(mocker, sl_conn, 'sqlite')
        assert get_dialect_name(tx) == 'sqlite'

        # Test with SQLAlchemy connection
        sa_sl_conn = _create_mock_sqlalchemy_connection(mocker, 'sqlite')
        assert get_dialect_name(sa_sl_conn) == 'sqlite'

    def test_get_dialect_name(self, mocker):
        """Test getting dialect name from various connection types"""
        # Test with SQLAlchemy engine
        mock_engine = mocker.MagicMock()
        mock_engine.dialect = mocker.MagicMock()
        mock_engine.dialect.name = 'postgresql'
        assert get_dialect_name(mock_engine) == 'postgresql'

        # Test with SQLAlchemy connection
        sa_conn = _create_mock_sqlalchemy_connection(mocker, 'sqlite')
        assert get_dialect_name(sa_conn) == 'sqlite'

        # Test with invalid object - use spec to prevent auto-creating dialect attribute
        invalid_obj = mocker.MagicMock(spec=['some_random_attr'])
        try:
            result = get_dialect_name(invalid_obj)
            assert result is None
        except AttributeError:
            # This is also acceptable - the function may raise for invalid objects
            pass


class TestSQLOperations:
    """Tests for SQL manipulation functions"""

    def test_standardize_placeholders(self, mocker):
        """Test placeholder conversion between database types"""
        # Test PostgreSQL -> SQLite conversion (? placeholders)
        pg_sql = 'SELECT * FROM users WHERE id = %s AND name LIKE %s'
        sqlite_sql = standardize_placeholders(pg_sql, 'sqlite')
        assert '?' in sqlite_sql
        assert '%s' not in sqlite_sql
        assert sqlite_sql == 'SELECT * FROM users WHERE id = ? AND name LIKE ?'

        # Test SQLite -> PostgreSQL conversion (%s placeholders)
        sqlite_sql = 'SELECT * FROM users WHERE id = ? AND name LIKE ?'
        pg_sql = standardize_placeholders(sqlite_sql, 'postgresql')
        assert '?' not in pg_sql
        assert '%s' in pg_sql
        assert pg_sql == 'SELECT * FROM users WHERE id = %s AND name LIKE %s'

    def test_standardize_placeholders_with_regex_patterns(self, mocker):
        """Test placeholder standardization with regex patterns preservation"""
        # SQL with regexp_replace containing question marks (should be preserved)
        regex_sql = "SELECT regexp_replace(column, '\\?+', 'X') FROM table WHERE id = ?"

        # Convert placeholders to PostgreSQL style
        result = standardize_placeholders(regex_sql, 'postgresql')

        # The regexp pattern should be preserved, but the parameter placeholder changed
        assert "regexp_replace(column, '\\?+', 'X')" in result
        assert 'id = %s' in result

    def test_build_insert_sql(self, mocker):
        """Test INSERT SQL generation"""
        # Test PostgreSQL INSERT SQL generation
        pg_sql = build_insert_sql('postgresql', 'users', ('id', 'name', 'email'))

        # Verify basic structure
        assert 'INSERT INTO' in pg_sql.upper()
        assert '"users"' in pg_sql
        assert '("id", "name", "email")' in pg_sql
        assert 'VALUES (%s, %s, %s)' in pg_sql

        # Test SQLite INSERT SQL generation
        sqlite_sql = build_insert_sql('sqlite', 'users', ('id', 'name', 'email'))

        # Verify basic structure
        assert 'INSERT INTO' in sqlite_sql.upper()
        assert '"users"' in sqlite_sql
        assert '("id", "name", "email")' in sqlite_sql
        assert 'VALUES (?, ?, ?)' in sqlite_sql


if __name__ == '__main__':
    __import__('pytest').main([__file__])
