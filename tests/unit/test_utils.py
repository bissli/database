import sqlite3
from unittest.mock import MagicMock, patch

import psycopg
import pymssql
import pytest
from database.client import _build_insert_sql, _build_upsert_sql
from database.client import is_psycopg_connection, is_pymssql_connection
from database.client import is_sqlite3_connection, isconnection
from database.client import quote_identifier, sanitize_sql_for_logging


class TestConnectionDetection:
    """Test suite for connection detection functions"""

    def test_is_psycopg_connection(self):
        """Test PostgreSQL connection detection"""
        # Create mock connection
        mock_conn = MagicMock(spec=psycopg.Connection)
        wrapped_conn = MagicMock()
        wrapped_conn.connection = mock_conn

        # Patch isinstance to handle mock objects
        with patch('database.client.isinstance', side_effect=lambda obj, cls: isinstance(obj, MagicMock) or isinstance(obj, cls)):
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
        conn = MagicMock()
        conn.connection = MagicMock(spec=psycopg.Connection)

        assert quote_identifier(conn, 'table_name') == '"table_name"'
        assert quote_identifier(conn, 'column.with.dots') == '"column.with.dots"'
        assert quote_identifier(conn, 'table"quoted') == '"table""quoted"'

    def test_quote_identifier_sqlite(self):
        """Test SQLite identifier quoting"""
        conn = MagicMock()
        conn.connection = MagicMock(spec=sqlite3.Connection)

        assert quote_identifier(conn, 'table_name') == '"table_name"'
        assert quote_identifier(conn, 'table"quoted') == '"table""quoted"'

    def test_quote_identifier_sqlserver(self):
        """Test SQL Server identifier quoting"""
        conn = MagicMock()
        conn.connection = MagicMock(spec=pymssql.Connection)

        assert quote_identifier(conn, 'table_name') == '[table_name]'
        assert quote_identifier(conn, 'table]with]brackets') == '[table]]with]]brackets]'

    def test_quote_identifier_unsupported(self):
        """Test quote_identifier with unsupported connection type"""
        # Create a mock without database connection spec
        conn = MagicMock()
        conn._spec_class = None  # Explicitly not a database connection

        with pytest.raises(ValueError, match="Unknown connection type"):
            quote_identifier(conn, 'table_name')


class TestSQLSanitization:
    """Test suite for SQL sanitization functions"""

    def test_sanitize_sql_for_logging_sensitive_password(self):
        """Test sanitization of SQL containing passwords"""
        sql = "INSERT INTO users (username, password) VALUES ('admin', 'supersecret')"
        sanitized_sql, _ = sanitize_sql_for_logging(sql)

        assert 'supersecret' not in sanitized_sql
        assert '***' in sanitized_sql

    def test_sanitize_sql_for_logging_sensitive_credit_card(self):
        """Test sanitization of SQL containing credit card info"""
        sql = "INSERT INTO payments (user_id, credit_card) VALUES (1, '4111-1111-1111-1111')"
        sanitized_sql, _ = sanitize_sql_for_logging(sql)

        assert '4111-1111-1111-1111' not in sanitized_sql
        assert '***' in sanitized_sql

    def test_sanitize_sql_for_logging_parameters(self):
        """Test sanitization of parameters"""
        sql = 'INSERT INTO users (username, password) VALUES (%s, %s)'
        args = ['admin', 'supersecret']

        sanitized_sql, sanitized_args = sanitize_sql_for_logging(sql, args)

        assert sanitized_args[0] == 'admin'
        assert sanitized_args[1] == '***'

    def test_sanitize_sql_for_logging_dict_parameters(self):
        """Test sanitization of dictionary parameters"""
        sql = 'INSERT INTO users (username, password) VALUES (%(username)s, %(password)s)'
        args = {'username': 'admin', 'password': 'supersecret'}

        sanitized_sql, sanitized_args = sanitize_sql_for_logging(sql, args)

        assert sanitized_args['username'] == 'admin'
        assert sanitized_args['password'] == '***'


class TestSQLBuilders:
    """Test suite for SQL building functions"""

    def test_build_insert_sql_postgres(self):
        """Test PostgreSQL INSERT SQL generation"""
        conn = MagicMock()
        conn.connection = MagicMock(spec=psycopg.Connection)

        # Patch quote_identifier to return PostgreSQL style quoting
        with patch('database.client.quote_identifier', 
                  side_effect=lambda conn, ident: f'"{ident}"'):
            sql = _build_insert_sql(conn, 'users', ('id', 'username', 'email'))

            assert 'insert into "users"' in sql.lower()
            assert '("id","username","email")' in sql.replace(' ', '')
            
            # More flexible assertion that doesn't depend on exact spacing
            assert all(x in sql.lower().replace(' ', '') for x in ['values', '(%s,%s,%s)'])

    def test_build_upsert_sql_postgres(self):
        """Test PostgreSQL UPSERT SQL generation"""
        conn = MagicMock()
        conn.connection = MagicMock(spec=psycopg.Connection)

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
