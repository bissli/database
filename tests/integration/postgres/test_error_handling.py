import database as db
import psycopg
import pytest
import sqlalchemy.exc
from database.utils.connection_utils import check_connection


def test_check_connection_decorator(psql_docker, pg_conn):
    """Test the check_connection decorator's retry behavior"""
    # Count how many times we retry
    retry_count = [0]

    # Create a function that will fail N times then succeed
    @check_connection(max_retries=3, retry_delay=0.1)
    def failing_function(conn, fail_count=2):
        if retry_count[0] < fail_count:
            retry_count[0] += 1
            raise psycopg.OperationalError('Simulated connection error')
        return 'Success'

    # Call the function - should retry twice then succeed
    result = failing_function(pg_conn, fail_count=2)
    assert result == 'Success'
    assert retry_count[0] == 2

    # Reset and try with more failures than allowed retries
    retry_count[0] = 0

    @check_connection(max_retries=3, retry_delay=0.1)
    def failing_function_2(conn, fail_count=5):
        if retry_count[0] < min(fail_count, 3):
            retry_count[0] += 1
            raise psycopg.OperationalError('Simulated connection error')
        return 'Success'
    with pytest.raises(psycopg.OperationalError):
        failing_function_2(pg_conn, fail_count=5)

    # Should have stopped after max_retries
    assert retry_count[0] == 3


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


if __name__ == '__main__':
    __import__('pytest').main([__file__])
