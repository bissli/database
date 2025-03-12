"""
Unit tests for database utility functions.
"""

import pytest
from database.operations.upsert import _build_insert_sql, _build_upsert_sql
from database.utils.sql import quote_identifier, sanitize_sql_for_logging
from database.utils.sql import standardize_placeholders


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

    def test_connection_detection_with_postgres(self, create_simple_mock_connection, create_simple_mock_transaction):
        """Test PostgreSQL connection type detection"""
        # Import connection utility functions
        from database.utils.connection_utils import is_psycopg_connection

        # Create connection that looks like a postgres connection
        pg_conn = create_simple_mock_connection('postgresql')

        # Test connection type detection
        assert is_psycopg_connection(pg_conn) is True

        # Test transaction object that wraps a connection
        tx = create_simple_mock_transaction(pg_conn)
        assert is_psycopg_connection(tx) is True

    def test_connection_detection_with_sqlserver(self, create_simple_mock_connection):
        """Test SQL Server connection type detection"""
        # Import connection utility functions
        from database.utils.connection_utils import is_pyodbc_connection

        # Create connection that looks like a SQL Server connection
        sql_conn = create_simple_mock_connection('mssql')

        # Test connection type detection
        assert is_pyodbc_connection(sql_conn) is True

    def test_connection_detection_with_sqlite(self, create_simple_mock_connection):
        """Test SQLite connection type detection"""
        # Import connection utility functions
        from database.utils.connection_utils import is_sqlite3_connection

        # Create connection that looks like a SQLite connection
        sqlite_conn = create_simple_mock_connection('sqlite')

        # Test connection type detection
        assert is_sqlite3_connection(sqlite_conn) is True

    def test_connection_detection_with_unknown_type(self, create_simple_mock_connection):
        """Test unknown connection type detection"""
        # Import connection utility functions
        from database.utils.connection_utils import is_psycopg_connection
        from database.utils.connection_utils import is_pyodbc_connection
        from database.utils.connection_utils import is_sqlite3_connection
        from database.utils.connection_utils import isconnection

        # Create a mock for an unknown database connection type
        unknown_conn = create_simple_mock_connection('unknown')

        # None of the specific connection type checks should match
        assert is_psycopg_connection(unknown_conn) is False
        assert is_pyodbc_connection(unknown_conn) is False
        assert is_sqlite3_connection(unknown_conn) is False

        # The generic isconnection should also return False
        assert isconnection(unknown_conn) is False

        # Also test a completely non-database object
        non_db_conn = create_simple_mock_connection('unknown')
        # Override the class to be a non-database object
        non_db_conn.__class__.__module__ = 'builtins'
        non_db_conn.__class__.__qualname__ = 'object'
        non_db_conn.__class__.__name__ = 'object'
        assert is_psycopg_connection(non_db_conn) is False
        assert is_pyodbc_connection(non_db_conn) is False
        assert is_sqlite3_connection(non_db_conn) is False
        assert isconnection(non_db_conn) is False

    def test_isconnection_postgres(self, create_simple_mock_connection):
        """Test isconnection function with PostgreSQL connections"""
        # Import required functions
        from database.utils.connection_utils import isconnection

        # Create a mock PostgreSQL connection
        pg_mock = create_simple_mock_connection('postgresql')
        other_mock = create_simple_mock_connection('unknown')

        # Test that isconnection properly identifies the PostgreSQL connection
        assert isconnection(pg_mock)
        assert not isconnection(other_mock)

    def test_isconnection_sqlserver(self, create_simple_mock_connection):
        """Test isconnection function with SQL Server connections"""
        # Import required functions
        from database.utils.connection_utils import isconnection

        # Create a mock SQL Server connection
        odbc_mock = create_simple_mock_connection('mssql')
        other_mock = create_simple_mock_connection('unknown')

        # Test the isconnection function
        assert isconnection(odbc_mock)
        assert not isconnection(other_mock)

    def test_isconnection_sqlite(self, create_simple_mock_connection):
        """Test isconnection function with SQLite connections"""
        # Import required functions
        from database.utils.connection_utils import isconnection

        # Create a mock SQLite connection
        sqlite_mock = create_simple_mock_connection('sqlite')
        other_mock = create_simple_mock_connection('unknown')

        # Test the isconnection function
        assert isconnection(sqlite_mock)
        assert not isconnection(other_mock)

    def test_connection_type_functions_postgres(self, create_simple_mock_connection, create_simple_mock_transaction):
        """Test individual connection type functions with PostgreSQL connection"""
        from database.utils.connection_utils import is_psycopg_connection
        from database.utils.connection_utils import is_pyodbc_connection
        from database.utils.connection_utils import is_sqlite3_connection

        # Create mock PostgreSQL connection
        pg_conn = create_simple_mock_connection('postgresql')

        # Test specific type detection functions
        assert is_psycopg_connection(pg_conn) is True
        assert is_pyodbc_connection(pg_conn) is False
        assert is_sqlite3_connection(pg_conn) is False

        # Test with transaction object that wraps a connection
        tx = create_simple_mock_transaction(pg_conn)
        assert is_psycopg_connection(tx) is True

    def test_connection_type_functions_sqlserver(self, create_simple_mock_connection, create_simple_mock_transaction):
        """Test individual connection type functions with SQL Server connection"""
        from database.utils.connection_utils import is_psycopg_connection
        from database.utils.connection_utils import is_pyodbc_connection
        from database.utils.connection_utils import is_sqlite3_connection

        # Create mock SQL Server connection
        ss_conn = create_simple_mock_connection('mssql')

        # Test specific type detection functions
        assert is_psycopg_connection(ss_conn) is False
        assert is_pyodbc_connection(ss_conn) is True
        assert is_sqlite3_connection(ss_conn) is False

        # Test with transaction object that wraps a connection
        tx = create_simple_mock_transaction(ss_conn)
        assert is_pyodbc_connection(tx) is True

    def test_connection_type_functions_sqlite(self, create_simple_mock_connection, create_simple_mock_transaction):
        """Test individual connection type functions with SQLite connection"""
        from database.utils.connection_utils import is_psycopg_connection
        from database.utils.connection_utils import is_pyodbc_connection
        from database.utils.connection_utils import is_sqlite3_connection

        # Create mock SQLite connection
        sl_conn = create_simple_mock_connection('sqlite')

        # Test specific type detection functions
        assert is_psycopg_connection(sl_conn) is False
        assert is_pyodbc_connection(sl_conn) is False
        assert is_sqlite3_connection(sl_conn) is True

        # Test with transaction object that wraps a connection
        tx = create_simple_mock_transaction(sl_conn)
        assert is_sqlite3_connection(tx) is True


class TestSQLOperations:
    """Tests for SQL manipulation functions"""

    def test_standardize_placeholders(self, create_simple_mock_connection):
        """Test placeholder conversion between database types"""
        # Create mock connections of different types
        pg_conn = create_simple_mock_connection('postgresql')
        sqlite_conn = create_simple_mock_connection('sqlite')

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

    def test_build_insert_sql(self, create_simple_mock_connection):
        """Test INSERT SQL generation"""
        # Create mock postgres connection
        mock_postgres_conn = create_simple_mock_connection('postgresql')

        # Test PostgreSQL INSERT SQL generation
        sql = _build_insert_sql(mock_postgres_conn, 'users', ('id', 'name', 'email'))

        assert 'insert into "users"' in sql.lower()
        assert '("id", "name", "email")' in sql.replace('  ', ' ')
        assert 'values (%s, %s, %s)' in sql.lower().replace('  ', ' ')
        # Check for RETURNING clause only if it's actually part of the implementation

    def test_build_upsert_sql(self, mock_postgres_conn):
        """Test PostgreSQL UPSERT SQL generation"""
        sql = _build_upsert_sql(
            mock_postgres_conn,
            table='users',
            columns=('id', 'username', 'email', 'last_login'),
            key_columns=['id'],           # conflict columns
            update_always=['email'],      # always update these columns
            update_if_null=['last_login']  # update these columns when null
        )

        assert 'insert into "users"' in sql.lower()
        assert '("id", "username", "email", "last_login")' in sql
        assert 'VALUES (%s, %s, %s, %s)' in sql
        assert 'on conflict ("id")' in sql.lower()
        assert 'do update set' in sql.lower()
        assert '"email" = excluded."email"' in sql
        assert 'coalesce("users"."last_login", excluded."last_login")' in sql
        assert 'returning' in sql.lower()  # PostgreSQL should have RETURNING clause


if __name__ == '__main__':
    __import__('pytest').main([__file__])
