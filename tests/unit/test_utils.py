import sqlite3
from unittest.mock import MagicMock, patch

import psycopg
import pymssql
import pytest
from database.operations.upsert import _build_insert_sql, _build_upsert_sql
from database.utils.connection_utils import is_psycopg_connection
from database.utils.connection_utils import is_pymssql_connection
from database.utils.connection_utils import is_sqlite3_connection
from database.utils.connection_utils import isconnection
from database.utils.sql import quote_identifier, sanitize_sql_for_logging


class TestConnectionDetection:
    """Test suite for connection detection functions"""

    def test_is_psycopg_connection(self):
        """Test PostgreSQL connection detection"""
        # Create mock connection
        mock_conn = MagicMock(spec=psycopg.Connection)
        wrapped_conn = MagicMock()
        wrapped_conn.connection = mock_conn

        # Patch isinstance to handle mock objects
        with patch('database.client.isinstance', side_effect=lambda obj, cls: isinstance(obj, MagicMock | cls)):
            # Test direct connection
            assert is_psycopg_connection(mock_conn)

        # Test wrapped connection
        assert is_psycopg_connection(wrapped_conn)

        # Test non-PostgreSQL connection
        non_pg_conn = MagicMock()
        assert not is_psycopg_connection(non_pg_conn)

    def test_is_pymssql_connection(self):
        """Test SQL Server connection detection"""
        # Create mock connection
        mock_conn = MagicMock(spec=pymssql.Connection)
        wrapped_conn = MagicMock()
        wrapped_conn.connection = mock_conn

        # Test direct connection
        assert is_pymssql_connection(mock_conn)

        # Test wrapped connection
        assert is_pymssql_connection(wrapped_conn)

        # Test non-SQL Server connection
        non_mssql_conn = MagicMock()
        assert not is_pymssql_connection(non_mssql_conn)

    def test_is_sqlite3_connection(self):
        """Test SQLite connection detection"""
        # Create mock connection
        mock_conn = MagicMock(spec=sqlite3.Connection)
        wrapped_conn = MagicMock()
        wrapped_conn.connection = mock_conn

        # Test direct connection
        assert is_sqlite3_connection(mock_conn)

        # Test wrapped connection
        assert is_sqlite3_connection(wrapped_conn)

        # Test non-SQLite connection
        non_sqlite_conn = MagicMock()
        assert not is_sqlite3_connection(non_sqlite_conn)

    def test_isconnection(self):
        """Test general connection detection"""
        # Create mock connections
        pg_conn = MagicMock()
        pg_conn.connection = MagicMock(spec=psycopg.Connection)

        sqlite_conn = MagicMock()
        sqlite_conn.connection = MagicMock(spec=sqlite3.Connection)

        mssql_conn = MagicMock()
        mssql_conn.connection = MagicMock(spec=pymssql.Connection)

        non_db_conn = MagicMock()

        # Test detection
        assert isconnection(pg_conn)
        assert isconnection(sqlite_conn)
        assert isconnection(mssql_conn)
        assert not isconnection(non_db_conn)


class TestQuoteIdentifier:
    """Test suite for quote_identifier function"""

    def test_quote_identifier_postgres(self):
        """Test PostgreSQL identifier quoting"""
        assert quote_identifier('postgres', 'table_name') == '"table_name"'
        assert quote_identifier('postgres', 'column.with.dots') == '"column.with.dots"'
        assert quote_identifier('postgres', 'table"quoted') == '"table""quoted"'

    def test_quote_identifier_sqlite(self):
        """Test SQLite identifier quoting"""
        assert quote_identifier('sqlite', 'table_name') == '"table_name"'
        assert quote_identifier('sqlite', 'table"quoted') == '"table""quoted"'

    def test_quote_identifier_sqlserver(self):
        """Test SQL Server identifier quoting"""
        assert quote_identifier('sqlserver', 'table_name') == '[table_name]'
        assert quote_identifier('sqlserver', 'table]with]brackets') == '[table]]with]]brackets]'

    def test_quote_identifier_unsupported(self):
        """Test quote_identifier with unsupported connection type"""
        with pytest.raises(ValueError, match='Unknown database type'):
            quote_identifier('unknown', 'table_name')


class TestSQLSanitization:
    """Test suite for SQL sanitization functions"""

    def test_sanitize_sql_for_logging_parameters(self):
        """Test sanitization of parameters with sensitive terms"""
        sql = 'INSERT INTO users (username, password) VALUES (%s, %s)'
        args = ['admin', 'supersecret']

        sanitized_sql, sanitized_args = sanitize_sql_for_logging(sql, args)

        # SQL remains unchanged
        assert sanitized_sql == sql
        # Only password parameter is masked
        assert sanitized_args[0] == 'admin'
        assert sanitized_args[1] == '***'

    def test_sanitize_sql_for_logging_dict_parameters(self):
        """Test sanitization of dictionary parameters with sensitive terms"""
        sql = 'INSERT INTO users (username, password, api_key) VALUES (%(username)s, %(password)s, %(api_key)s)'
        args = {'username': 'admin', 'password': 'supersecret', 'api_key': 'abc123'}

        sanitized_sql, sanitized_args = sanitize_sql_for_logging(sql, args)

        # SQL remains unchanged
        assert sanitized_sql == sql
        # Only sensitive parameters are masked
        assert sanitized_args['username'] == 'admin'
        assert sanitized_args['password'] == '***'
        assert sanitized_args['api_key'] == '***'

    def test_sanitize_sql_for_logging_insert_columns(self):
        """Test sanitization of parameters in INSERT statements"""
        sql = 'INSERT INTO users (username, password, credit_card) VALUES (%s, %s, %s)'
        args = ['admin', 'supersecret', '4111-1111-1111-1111']

        sanitized_sql, sanitized_args = sanitize_sql_for_logging(sql, args)

        # Order of parameters should match column order
        assert sanitized_args[0] == 'admin'
        assert sanitized_args[1] == '***'  # password masked
        assert sanitized_args[2] == '***'  # credit_card masked

    def test_sanitize_sql_for_logging_none_args(self):
        """Test sanitization with no args"""
        sql = 'SELECT * FROM users'

        sanitized_sql, sanitized_args = sanitize_sql_for_logging(sql)

        assert sanitized_sql == sql
        assert sanitized_args is None


class TestSQLBuilders:
    """Test suite for SQL building functions"""

    def test_build_insert_sql_postgres(self):
        """Test PostgreSQL INSERT SQL generation"""
        conn = MagicMock()
        conn.connection = MagicMock(spec=psycopg.Connection)
        conn.get_driver_type = MagicMock(return_value='postgres')

        # Patch quote_identifier to return PostgreSQL style quoting
        with patch('database.utils.sql.quote_identifier',
                   side_effect=lambda db_type, ident: f'"{ident}"'):
            sql = _build_insert_sql(conn, 'users', ('id', 'username', 'email'))

            assert 'insert into "users"' in sql.lower()
            assert '("id","username","email")' in sql.replace(' ', '')

            # More flexible assertion that doesn't depend on exact spacing
            assert all(x in sql.lower().replace(' ', '') for x in ['values', '(%s,%s,%s)'])

    def test_build_upsert_sql_postgres(self):
        """Test PostgreSQL UPSERT SQL generation"""
        conn = MagicMock()
        conn.connection = MagicMock(spec=psycopg.Connection)
        conn.get_driver_type = MagicMock(return_value='postgres')

        sql = _build_upsert_sql(
            conn,
            'users',
            ('id', 'username', 'email', 'last_login'),
            ['id'],
            ['email'],
            ['last_login'],
            'postgres'
        )

        # Check for ON CONFLICT clause
        assert 'on conflict ("id")' in sql.lower()

        # Check for DO UPDATE clause
        assert 'do update set' in sql.lower()

        # Check that email is always updated
        assert '"email" = excluded."email"' in sql

        # Check that last_login is only updated when null
        assert 'coalesce("users"."last_login", excluded."last_login")' in sql


if __name__ == '__main__':
    __import__('pytest').main([__file__])
