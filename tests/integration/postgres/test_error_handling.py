from unittest.mock import patch

import database as db
import psycopg
import pytest
import sqlalchemy.exc
from database.connection import check_connection
from database.exceptions import is_retryable_error


class TestIsRetryableError:
    """Tests for is_retryable_error function."""

    def test_ssl_errors_are_retryable(self):
        exc = psycopg.OperationalError('SSL SYSCALL error: EOF detected')
        assert is_retryable_error(exc) is True

    def test_connection_closed_is_retryable(self):
        exc = psycopg.OperationalError('server closed the connection unexpectedly')
        assert is_retryable_error(exc) is True

    def test_connection_reset_is_retryable(self):
        exc = psycopg.OperationalError('connection reset by peer')
        assert is_retryable_error(exc) is True

    def test_timeout_is_retryable(self):
        exc = psycopg.OperationalError('connection timed out')
        assert is_retryable_error(exc) is True

    def test_network_unreachable_is_retryable(self):
        exc = psycopg.OperationalError('network is unreachable')
        assert is_retryable_error(exc) is True

    def test_too_many_connections_is_retryable(self):
        exc = psycopg.OperationalError('too many connections for role')
        assert is_retryable_error(exc) is True

    def test_generic_error_not_retryable(self):
        exc = psycopg.OperationalError('some random error')
        assert is_retryable_error(exc) is False

    def test_type_error_not_retryable(self):
        exc = psycopg.OperationalError('invalid input syntax for type integer')
        assert is_retryable_error(exc) is False

    def test_syntax_error_not_retryable(self):
        exc = psycopg.ProgrammingError('syntax error at or near "SELECT"')
        assert is_retryable_error(exc) is False


def test_check_connection_decorator_retries_transient_errors(psql_docker, pg_conn):
    """Test the check_connection decorator retries transient errors"""
    retry_count = [0]

    @check_connection(max_retries=3, retry_delay=0.01)
    def failing_function(conn, fail_count=2):
        if retry_count[0] < fail_count:
            retry_count[0] += 1
            raise psycopg.OperationalError('SSL SYSCALL error: connection reset')
        return 'Success'

    result = failing_function(pg_conn, fail_count=2)
    assert result == 'Success'
    assert retry_count[0] == 2


def test_check_connection_decorator_fails_immediately_for_non_retryable(psql_docker, pg_conn):
    """Test that non-retryable errors fail immediately without retry"""
    retry_count = [0]

    @check_connection(max_retries=3, retry_delay=0.01)
    def failing_function(conn):
        retry_count[0] += 1
        # Non-retryable error - should fail immediately
        raise psycopg.OperationalError('invalid input syntax for type integer')

    with pytest.raises(psycopg.OperationalError):
        failing_function(pg_conn)

    # Should have only been called once - no retries for non-retryable errors
    assert retry_count[0] == 1


def test_check_connection_decorator_max_retries_exceeded(psql_docker, pg_conn):
    """Test that retryable errors eventually fail after max retries"""
    retry_count = [0]

    @check_connection(max_retries=3, retry_delay=0.01)
    def failing_function(conn):
        retry_count[0] += 1
        raise psycopg.OperationalError('SSL connection has been closed unexpectedly')

    with pytest.raises(psycopg.OperationalError):
        failing_function(pg_conn)

    assert retry_count[0] == 3


def test_check_connection_with_check_retryable_disabled(psql_docker, pg_conn):
    """Test that check_retryable=False retries all matching errors"""
    retry_count = [0]

    @check_connection(max_retries=3, retry_delay=0.01, check_retryable=False)
    def failing_function(conn, fail_count=2):
        if retry_count[0] < fail_count:
            retry_count[0] += 1
            raise psycopg.OperationalError('some generic error')
        return 'Success'

    result = failing_function(pg_conn, fail_count=2)
    assert result == 'Success'
    assert retry_count[0] == 2


def test_connection_timeout(psql_docker):
    """Test connection timeout handling"""
    # Attempt to connect with a very short timeout to a non-existent server
    # SQLAlchemy wraps the underlying psycopg exception
    with pytest.raises((*db.DbConnectionError, sqlalchemy.exc.OperationalError)):
        db.connect({
            'drivername': 'postgresql',
            'hostname': 'nonexistent.host',
            'username': 'postgresql',
            'password': 'postgresql',
            'database': 'test',
            'port': 5432,
            'timeout': 1  # 1 second timeout
        })


def test_upsert_rows_retries_on_connection_error(psql_docker, pg_conn):
    """Test that upsert_rows retries on OperationalError (e.g., SSL errors)"""
    pg_conn.execute("""
        CREATE TABLE IF NOT EXISTS test_retry (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
    """)

    call_count = [0]
    original_executemany = pg_conn.cursor().__class__.executemany

    def mock_executemany(self, operation, seq_of_parameters, *args, **kwargs):
        call_count[0] += 1
        if call_count[0] < 3:
            raise psycopg.OperationalError('SSL SYSCALL error: EOF detected')
        return original_executemany(self, operation, seq_of_parameters, *args, **kwargs)

    with patch.object(pg_conn.cursor().__class__, 'executemany', mock_executemany):
        rows = [{'id': 1, 'name': 'test'}]
        pg_conn.upsert_rows('test_retry', rows)

    assert call_count[0] == 3

    pg_conn.execute('DROP TABLE test_retry')


if __name__ == '__main__':
    __import__('pytest').main([__file__])
