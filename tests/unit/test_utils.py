"""
Unit tests for database utility functions.
"""

import logging

import pytest
from database.operations.upsert import _build_insert_sql, _build_upsert_sql
from database.utils.connection_utils import get_dialect_name
from database.utils.connection_utils import is_psycopg_connection
from database.utils.connection_utils import is_pyodbc_connection
from database.utils.connection_utils import is_sqlite3_connection
from database.utils.connection_utils import isconnection
from database.utils.sql import quote_identifier, sanitize_sql_for_logging
from database.utils.sql import standardize_placeholders

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
        # Add dialect for SQLAlchemy compatibility
        mock_conn.dialect = mocker.MagicMock()
        mock_conn.dialect.name = 'postgresql'
    elif db_type == 'mssql':
        # Set SQL Server-specific attributes that the detection function checks
        mock_conn.__class__.__module__ = 'pyodbc'
        mock_conn.__class__.__name__ = 'Connection'
        mock_conn.getinfo = mocker.MagicMock()
        # Add dialect for SQLAlchemy compatibility
        mock_conn.dialect = mocker.MagicMock()
        mock_conn.dialect.name = 'mssql'
    elif db_type == 'sqlite':
        # Set SQLite-specific attributes that the detection function checks
        mock_conn.__class__.__module__ = 'sqlite3'
        mock_conn.__class__.__name__ = 'Connection'
        mock_conn.execute = mocker.MagicMock()
        # Add dialect for SQLAlchemy compatibility
        mock_conn.dialect = mocker.MagicMock()
        mock_conn.dialect.name = 'sqlite'

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


def _create_mock_transaction(mocker, connection):
    """Helper to create a mock transaction that wraps a connection"""
    mock_tx = mocker.MagicMock()

    # Set the connection as an attribute
    mock_tx.connection = connection

    # Add connection property that returns the underlying connection
    type(mock_tx).connection = mocker.PropertyMock(return_value=connection)

    # Set driver_connection for unwrapping logic
    mock_tx.driver_connection = connection.driver_connection

    return mock_tx


class TestDatabaseUtilities:
    """Tests for general database utility functions"""

    @pytest.mark.parametrize(('db_type', 'identifier', 'expected'), [
        # PostgreSQL
        ('postgresql', 'table_name', '"table_name"'),
        ('postgresql', 'column.with.dots', '"column.with.dots"'),
        ('postgresql', 'table"quoted', '"table""quoted"'),
        # SQLite
        ('sqlite', 'table_name', '"table_name"'),
        ('sqlite', 'table"quoted', '"table""quoted"'),
        # SQL Server
        ('mssql', 'table_name', '[table_name]'),
        ('mssql', 'table]with]brackets', '[table]]with]]brackets]'),
    ])
    def test_quote_identifier(self, db_type, identifier, expected):
        """Test identifier quoting for all database types"""
        assert quote_identifier(db_type, identifier) == expected

    def test_quote_identifier_error(self):
        """Test error handling in quote_identifier"""
        with pytest.raises(ValueError, match='Unknown database type'):
            quote_identifier('unknown', 'table_name')

    def test_connection_detection_with_postgres(self, mocker):
        """Test PostgreSQL connection type detection"""
        # Create connection that looks like a postgres connection
        pg_conn = _create_mock_connection(mocker, 'postgresql')

        # Test detection with actual implementation, not mocked function
        assert get_dialect_name(pg_conn) == 'postgresql'

        # Test transaction object that wraps a connection
        tx = _create_mock_transaction(mocker, pg_conn)
        assert get_dialect_name(tx) == 'postgresql'

        # Test with SQLAlchemy connection
        sa_pg_conn = _create_mock_sqlalchemy_connection(mocker, 'postgresql')
        assert get_dialect_name(sa_pg_conn) == 'postgresql'

    def test_connection_detection_with_sqlserver(self, mocker):
        """Test SQL Server connection type detection"""
        # Create connection that looks like a SQL Server connection
        sql_conn = _create_mock_connection(mocker, 'mssql')

        # Test detection with actual implementation, not mocked function
        assert get_dialect_name(sql_conn) == 'mssql'

        # Test with SQLAlchemy connection
        sa_sql_conn = _create_mock_sqlalchemy_connection(mocker, 'mssql')
        assert get_dialect_name(sa_sql_conn) == 'mssql'

    def test_connection_detection_with_sqlite(self, mocker):
        """Test SQLite connection type detection"""
        # Create connection that looks like a SQLite connection
        sqlite_conn = _create_mock_connection(mocker, 'sqlite')

        # Test detection with actual implementation, not mocked function
        assert get_dialect_name(sqlite_conn) == 'sqlite'

        # Test with SQLAlchemy connection
        sa_sqlite_conn = _create_mock_sqlalchemy_connection(mocker, 'sqlite')
        assert get_dialect_name(sa_sqlite_conn) == 'sqlite'

    def test_isconnection(self, mocker):
        """Test isconnection function with various connection types"""
        # Create different types of connections
        pg_mock = _create_mock_connection(mocker, 'postgresql')
        sql_mock = _create_mock_connection(mocker, 'mssql')
        sqlite_mock = _create_mock_connection(mocker, 'sqlite')

        # Create a non-database object for comparison
        non_db_mock = mocker.MagicMock()
        non_db_mock.__class__.__module__ = 'builtins'
        non_db_mock.__class__.__name__ = 'object'

        # Create an object with just driver_connection attribute
        sqlalchemy_like = mocker.MagicMock()
        sqlalchemy_like.driver_connection = mocker.MagicMock()

        # Test all types of connections
        assert isconnection(pg_mock)
        assert isconnection(sql_mock)
        assert isconnection(sqlite_mock)
        assert isconnection(sqlalchemy_like)
        assert not isconnection(non_db_mock)

        # Test with SQLAlchemy connections
        sa_pg_conn = _create_mock_sqlalchemy_connection(mocker, 'postgresql')
        sa_sql_conn = _create_mock_sqlalchemy_connection(mocker, 'mssql')
        sa_sqlite_conn = _create_mock_sqlalchemy_connection(mocker, 'sqlite')
        assert isconnection(sa_pg_conn)
        assert isconnection(sa_sql_conn)
        assert isconnection(sa_sqlite_conn)

    def test_connection_type_detection_postgres(self, mocker):
        """Test PostgreSQL connection type detection"""
        # Create mock PostgreSQL connection with proper attributes
        pg_conn = _create_mock_connection(mocker, 'postgresql')

        # Test dialect name detection
        assert get_dialect_name(pg_conn) == 'postgresql'

        # Test with transaction object that wraps a connection
        tx = _create_mock_transaction(mocker, pg_conn)
        assert get_dialect_name(tx) == 'postgresql'

        # Test with SQLAlchemy connection
        sa_pg_conn = _create_mock_sqlalchemy_connection(mocker, 'postgresql')
        assert get_dialect_name(sa_pg_conn) == 'postgresql'

    def test_connection_type_detection_sqlserver(self, mocker):
        """Test SQL Server connection type detection"""
        # Create mock SQL Server connection with proper attributes
        ss_conn = _create_mock_connection(mocker, 'mssql')

        # Test dialect name detection
        assert get_dialect_name(ss_conn) == 'mssql'

        # Test with transaction object that wraps a connection
        tx = _create_mock_transaction(mocker, ss_conn)
        assert get_dialect_name(tx) == 'mssql'

        # Test with SQLAlchemy connection
        sa_ss_conn = _create_mock_sqlalchemy_connection(mocker, 'mssql')
        assert get_dialect_name(sa_ss_conn) == 'mssql'

    def test_connection_type_detection_sqlite(self, mocker):
        """Test SQLite connection type detection"""
        # Create mock SQLite connection with proper attributes
        sl_conn = _create_mock_connection(mocker, 'sqlite')

        # Test dialect name detection
        assert get_dialect_name(sl_conn) == 'sqlite'

        # Test with transaction object that wraps a connection
        tx = _create_mock_transaction(mocker, sl_conn)
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
        sa_conn = _create_mock_sqlalchemy_connection(mocker, 'mssql')
        assert get_dialect_name(sa_conn) == 'mssql'

        # Test with invalid object
        assert get_dialect_name(mocker.MagicMock()) is None


class TestSQLOperations:
    """Tests for SQL manipulation functions"""

    def test_standardize_placeholders(self, mocker):
        """Test placeholder conversion between database types"""
        # Create mock connections of different types
        pg_conn = _create_mock_connection(mocker, 'postgresql')
        sqlite_conn = _create_mock_connection(mocker, 'sqlite')

        # Test PostgreSQL -> SQLite conversion
        pg_sql = 'SELECT * FROM users WHERE id = %s AND name LIKE %s'
        sqlite_sql = standardize_placeholders(sqlite_conn, pg_sql)
        assert '?' in sqlite_sql
        assert '%s' not in sqlite_sql
        assert sqlite_sql == 'SELECT * FROM users WHERE id = ? AND name LIKE ?'

        # Test SQLite -> PostgreSQL conversion
        sqlite_sql = 'SELECT * FROM users WHERE id = ? AND name LIKE ?'
        pg_sql = standardize_placeholders(pg_conn, sqlite_sql)
        assert '?' not in pg_sql
        assert '%s' in pg_sql
        assert pg_sql == 'SELECT * FROM users WHERE id = %s AND name LIKE %s'

    def test_standardize_placeholders_with_regex_patterns(self, mocker):
        """Test placeholder standardization with regex patterns preservation"""
        # Create mock PostgreSQL connection
        pg_conn = _create_mock_connection(mocker, 'postgresql')

        # SQL with regexp_replace containing question marks (should be preserved)
        regex_sql = "SELECT regexp_replace(column, '\\?+', 'X') FROM table WHERE id = ?"

        # Convert placeholders
        result = standardize_placeholders(pg_conn, regex_sql)

        # The regexp pattern should be preserved, but the parameter placeholder changed
        assert "regexp_replace(column, '\\?+', 'X')" in result
        assert 'id = %s' in result

    @pytest.mark.parametrize(('sql', 'args', 'expected'), [
        # Test basic SQL with positional parameters
        ('INSERT INTO users (username, password) VALUES (%s, %s)',
         ['admin', 'supersecret'],
         {'sql': 'INSERT INTO users (username, password) VALUES (%s, %s)',
          'args': ['admin', '***']}),

        # Test dictionary parameters with sensitive info
        ('INSERT INTO users (username, password, api_key) VALUES (%(username)s, %(password)s, %(api_key)s)',
         {'username': 'admin', 'password': 'secret', 'api_key': 'key123'},
         {'sql': 'INSERT INTO users (username, password, api_key) VALUES (%(username)s, %(password)s, %(api_key)s)',
          'args': {'username': 'admin', 'password': '***', 'api_key': '***'}}),

        # Test with other sensitive parameter names
        ('UPDATE users SET auth_token = %s WHERE id = %s',
         ['abc123token', 42],
         {'sql': 'UPDATE users SET auth_token = %s WHERE id = %s',
          'args': ['***', 42]}),

        # Test with no args
        ('SELECT * FROM users', None,
         {'sql': 'SELECT * FROM users', 'args': None}),

        # Test SQL Server parameters
        ('INSERT INTO users (username, password) VALUES (?, ?)',
         ['admin', 'supersecret'],
         {'sql': 'INSERT INTO users (username, password) VALUES (?, ?)',
          'args': ['admin', '***']}),
    ])
    def test_sanitize_sql(self, sql, args, expected):
        """Test SQL sanitization with various input patterns"""
        sanitized_sql, sanitized_args = sanitize_sql_for_logging(sql, args)

        # Verify SQL remains unchanged
        assert sanitized_sql == expected['sql']

        # Verify args are properly sanitized
        if expected['args'] is None:
            assert sanitized_args is None
            return

        if isinstance(args, dict):
            for key, expected_value in expected['args'].items():
                assert sanitized_args[key] == expected_value, f"Sanitization failed for key '{key}'"
        elif isinstance(args, list):
            for i, (actual, expected_value) in enumerate(zip(sanitized_args, expected['args'])):
                assert actual == expected_value, f'Sanitization failed at position {i}'

    def test_build_insert_sql(self, mocker):
        """Test INSERT SQL generation"""
        # Create mock connections for different database types
        mock_postgres_conn = _create_mock_connection(mocker, 'postgresql')
        mock_sqlite_conn = _create_mock_connection(mocker, 'sqlite')

        # Test PostgreSQL INSERT SQL generation
        pg_sql = _build_insert_sql(mock_postgres_conn, 'users', ('id', 'name', 'email'))

        # Verify basic structure
        assert 'INSERT INTO' in pg_sql.upper()
        assert '"users"' in pg_sql
        assert '("id", "name", "email")' in pg_sql
        assert 'VALUES (%s, %s, %s)' in pg_sql
        # Verify PostgreSQL RETURNING clause is present
        assert 'RETURNING' in pg_sql.upper()

        # Test SQLite INSERT SQL generation
        sqlite_sql = _build_insert_sql(mock_sqlite_conn, 'users', ('id', 'name', 'email'))

        # Verify basic structure
        assert 'INSERT INTO' in sqlite_sql.upper()
        assert '"users"' in sqlite_sql
        assert '("id", "name", "email")' in sqlite_sql
        assert 'VALUES (?, ?, ?)' in sqlite_sql
        # SQLite doesn't have RETURNING
        assert 'RETURNING' not in sqlite_sql.upper()

    @pytest.fixture
    def mock_postgres_conn(self, mocker):
        """Create a mock PostgreSQL connection for testing"""
        mock_conn = _create_mock_connection(mocker, 'postgresql')

        # Set up required attributes for proper identification
        mock_conn.in_transaction = False

        # Add diagnostics information
        mock_conn.info = {
            'dbname': 'test_db',
            'user': 'test_user',
            'host': 'localhost',
            'port': '5432'
        }

        return mock_conn

    @pytest.fixture
    def mock_sqlite_conn(self, mocker):
        """Create a mock SQLite connection for testing"""
        mock_conn = _create_mock_connection(mocker, 'sqlite')
        mock_conn.in_transaction = False
        return mock_conn

    @pytest.fixture
    def mock_sqlserver_conn(self, mocker):
        """Create a mock SQL Server connection for testing"""
        mock_conn = _create_mock_connection(mocker, 'mssql')
        mock_conn.in_transaction = False
        return mock_conn

    def test_build_upsert_sql_postgres(self, mock_postgres_conn):
        """Test PostgreSQL UPSERT SQL generation"""
        sql = _build_upsert_sql(
            mock_postgres_conn,
            table='users',
            columns=('id', 'username', 'email', 'last_login'),
            key_columns=['id'],           # conflict columns
            update_always=['email'],      # always update these columns
            update_if_null=['last_login'],  # update these columns when null
            db_type='postgresql'
        )

        # Verify complete structure with proper clause order and quoting
        assert sql.upper().startswith('INSERT INTO')
        assert '"users"' in sql
        assert '("id", "username", "email", "last_login")' in sql
        assert 'VALUES (%s, %s, %s, %s)' in sql
        assert 'ON CONFLICT ("id")' in sql
        assert 'DO UPDATE SET' in sql
        assert '"email" = excluded."email"' in sql
        assert 'coalesce("users"."last_login", excluded."last_login")' in sql
        assert sql.upper().endswith('RETURNING *')

    def test_build_upsert_sql_sqlite(self, mock_sqlite_conn):
        """Test SQLite UPSERT SQL generation"""
        sql = _build_upsert_sql(
            mock_sqlite_conn,
            table='users',
            columns=('id', 'username', 'email', 'last_login'),
            key_columns=['id'],           # conflict columns
            update_always=['email'],      # always update these columns
            update_if_null=['last_login'],  # update these columns when null
            db_type='sqlite'
        )

        # Verify complete structure with proper clause order and quoting for SQLite
        assert sql.upper().startswith('INSERT INTO')
        assert '"users"' in sql
        assert '("id", "username", "email", "last_login")' in sql
        assert 'VALUES (?, ?, ?, ?)' in sql
        assert 'ON CONFLICT ("id")' in sql
        assert 'DO UPDATE SET' in sql
        assert '"email" = excluded."email"' in sql
        assert 'COALESCE("users"."last_login", excluded."last_login")' in sql
        # SQLite doesn't have RETURNING
        assert 'RETURNING' not in sql.upper()

    def test_build_upsert_sql_sqlserver(self, mock_sqlserver_conn):
        """Test SQL Server UPSERT (MERGE) SQL generation"""
        sql = _build_upsert_sql(
            mock_sqlserver_conn,
            table='users',
            columns=('id', 'username', 'email', 'last_login'),
            key_columns=['id'],           # conflict columns
            update_always=['email'],      # always update these columns
            update_if_null=['last_login'],  # update these columns when null
            db_type='mssql'
        )

        # Verify complete structure with proper clause order and quoting for SQL Server
        assert 'MERGE INTO' in sql.upper()
        assert '[users]' in sql
        assert 'USING' in sql.upper()
        assert 'AS target' in sql.lower()
        assert 'ON target.[id] = src.[id]' in sql
        assert 'WHEN MATCHED THEN UPDATE SET' in sql.upper()
        assert 'target.[email] = src.[email]' in sql
        assert 'WHEN NOT MATCHED THEN INSERT' in sql.upper()
        assert '([id], [username], [email], [last_login])' in sql
        assert 'VALUES (src.[id], src.[username], src.[email], src.[last_login])' in sql


if __name__ == '__main__':
    __import__('pytest').main([__file__])
