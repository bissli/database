import time
from unittest.mock import MagicMock, patch

import database as db
import pytest


def test_check_connection_decorator(psql_docker, conn):
    """Test the check_connection decorator's retry behavior"""
    # Count how many times we retry
    retry_count = [0]

    # Create a function that will fail N times then succeed
    @db.core.transaction.check_connection(max_retries=3, retry_delay=0.1)
    def failing_function(conn, fail_count=2):
        if retry_count[0] < fail_count:
            retry_count[0] += 1
            raise db.DbConnectionError[0]('Simulated connection error')
        return 'Success'

    # Call the function - should retry twice then succeed
    result = failing_function(conn, fail_count=2)
    assert result == 'Success'
    assert retry_count[0] == 2

    # Reset and try with more failures than allowed retries
    retry_count[0] = 0

    @db.core.transaction.check_connection(max_retries=3, retry_delay=0.1)
    def failing_function_2(conn, fail_count=5):
        if retry_count[0] < min(fail_count, 3):
            retry_count[0] += 1
            raise db.DbConnectionError[0]('Simulated connection error')
        return 'Success'
    with pytest.raises(db.DbConnectionError[0]):
        failing_function_2(conn, fail_count=5)

    # Should have stopped after max_retries
    assert retry_count[0] == 3


def test_simpler_reconnect(psql_docker, conn):
    """A simpler test for automatic reconnection after connection failure"""
    calls = [0]

    @db.core.transaction.check_connection(max_retries=3, retry_delay=0.1)
    def failing_then_succeeding(connection, raise_error=True):
        calls[0] += 1
        if calls[0] == 1 and raise_error:
            if hasattr(conn.connection, 'close'):
                try:
                    conn.connection.close()
                except:
                    pass
            raise db.DbConnectionError[0]('Simulated complete connection failure')
        return calls[0]
    original_connection = conn.connection
    conn.options.check_connection = True
    result = failing_then_succeeding(conn)
    assert calls[0] == 2, 'Function should be called exactly twice'
    assert conn.connection is not original_connection, 'Connection should have been replaced'
    count = db.select_scalar(conn, 'SELECT COUNT(*) FROM test_table')
    assert isinstance(count, int)


def simulate_connection_failure(cn, sql, *args):
    """Function to simulate a connection failure"""
    raise db.DbConnectionError[0]('Simulated connection failure')


def test_automatic_reconnect(psql_docker, conn):
    """Test automatic reconnection after connection failure"""
    # Store the original connection for later comparison
    original_connection = conn.connection

    # Patch db.execute to simulate a connection failure on the first call only
    execute_calls = [0]
    new_mock_connection = MagicMock()
    mock_conn_wrapper = MagicMock()
    mock_conn_wrapper.connection = new_mock_connection
    mock_conn_wrapper.options = conn.options
    mock_conn_wrapper.driver_type = 'postgresql'

    @db.core.transaction.check_connection(max_retries=3, retry_delay=0.1)
    def test_execute_with_failure(connection, sql):
        execute_calls[0] += 1
        if execute_calls[0] == 1:
            # First call - simulate failure
            raise db.DbConnectionError[0]('Simulated connection failure')
        # Subsequent calls - pass through to real function
        return 'Success'
    # Ensure connection checking is enabled
    conn.options.check_connection = True

    # Apply the patch
    with patch('database.core.connection.connect', return_value=mock_conn_wrapper):
        # This should fail once, reconnect, and then succeed
        result = test_execute_with_failure(conn, 'SELECT 1')

    # The connection should have been re-established
    assert execute_calls[0] == 2, 'Function should be called exactly twice (fail + retry)'

    # Verify the connection is working
    assert conn.connection == new_mock_connection, 'Connection should have been replaced'


def test_connection_timeout(psql_docker):
    """Test connection timeout handling"""
    # Attempt to connect with a very short timeout to a non-existent server
    with pytest.raises(db.DbConnectionError):
        db.connect({
            'drivername': 'postgresql',
            'hostname': 'nonexistent.host',
            'username': 'postgresql',
            'password': 'postgresql',
            'database': 'test',
            'port': 5432,
            'timeout': 1  # 1 second timeout
        })

@pytest.mark.skip
def test_transaction_retry(psql_docker, conn):
    """Test transaction retry behavior"""
    # Simulate a temporary failure in the middle of a transaction
    transaction_attempts = [0]

    # Define a function that will fail on first attempt
    def execute_with_temporary_failure():
        transaction_attempts[0] += 1
        with db.transaction(conn) as tx:
            # First statement succeeds
            tx.execute('INSERT INTO test_table (name, value) VALUES (%s, %s)',
                       f'RetryTest-{transaction_attempts[0]}', 100)

            # Simulate a temporary failure on first attempt only
            if transaction_attempts[0] == 1:
                raise db.DbConnectionError[0]('Simulated temporary failure')

            # Second statement only executes if no failure
            tx.execute('INSERT INTO test_table (name, value) VALUES (%s, %s)',
                       f'RetryTest2-{transaction_attempts[0]}', 200)

    # Execute with retry
    max_attempts = 3
    attempt = 0
    success = False

    while attempt < max_attempts and not success:
        try:
            execute_with_temporary_failure()
            success = True
        except db.DbConnectionError:
            attempt += 1
            time.sleep(0.1)  # Short delay before retry

    assert success
    assert transaction_attempts[0] == 2  # Should succeed on second attempt

    # Verify only the successful transaction was committed
    # We should not see RetryTest-1 but should see RetryTest-2 and RetryTest2-2
    results = db.select(conn, "SELECT name, value FROM test_table WHERE name LIKE 'RetryTest%'")

    assert len(results) == 2
    assert 'RetryTest-1' not in results['name'].values
    assert 'RetryTest-2' in results['name'].values
    assert 'RetryTest2-2' in results['name'].values


if __name__ == '__main__':
    __import__('pytest').main([__file__])
