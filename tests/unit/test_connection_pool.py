import time
from unittest.mock import MagicMock, patch

import pytest
from database.core.connection import ConnectionPool
from database.options import DatabaseOptions


@pytest.fixture
def mock_db_options():
    """Create mock database options for testing"""
    options = DatabaseOptions(
        drivername='sqlite',
        database=':memory:',
        check_connection=False
    )
    return options


@patch('database.core.connection.connect')
def test_connection_pool_get_connection(mock_connect, mock_db_options):
    """Test connection pool creates and gets connections"""
    # Setup mock connection
    from tests.fixtures.mocks import _create_simple_mock_connection
    mock_conn = _create_simple_mock_connection('sqlite')
    mock_connect.return_value = mock_conn

    # Patch adapter registry to prevent it from trying to apply to mock
    with patch('database.adapter_registry'):
        # Create pool
        pool = ConnectionPool(mock_db_options, max_connections=3, max_idle_time=60)

        # Get a connection from the pool
        conn1 = pool.get_connection()
        assert conn1 == mock_conn
        assert mock_connect.call_count == 1

        # Get another connection - should create a new one
        from tests.fixtures.mocks import _create_simple_mock_connection
        mock_conn2 = _create_simple_mock_connection('sqlite')
        mock_connect.return_value = mock_conn2
        conn2 = pool.get_connection()
        assert conn2 == mock_conn2
        assert mock_connect.call_count == 2

        # Release a connection back to the pool
        pool.release_connection(conn1)

        # Get another connection - should reuse the released one
        conn3 = pool.get_connection()
        assert conn3 == conn1
        assert mock_connect.call_count == 2  # No new connection created


@patch('database.core.connection.connect')
def test_connection_pool_max_connections(mock_connect, mock_db_options):
    """Test connection pool enforces max connections"""
    # Setup mock connections
    mock_connections = [MagicMock() for _ in range(3)]
    mock_connect.side_effect = mock_connections

    # Patch adapter registry to prevent it from trying to apply to mock
    with patch('database.adapter_registry'):
        # Create pool with max 3 connections
        pool = ConnectionPool(mock_db_options, max_connections=3, max_idle_time=60)

        # Get all 3 connections
        conn1 = pool.get_connection()
        conn2 = pool.get_connection()
        conn3 = pool.get_connection()

        # Try to get a 4th connection - should raise error
        with pytest.raises(RuntimeError, match='Connection pool exhausted'):
            pool.get_connection()

        # Release one connection
        pool.release_connection(conn2)

        # Should be able to get another connection now
        conn4 = pool.get_connection()
        assert conn4 == conn2  # Reused the released connection


@patch('database.core.connection.connect')
def test_connection_pool_expired_connections(mock_connect, mock_db_options):
    """Test connection pool cleans up expired connections"""
    # Setup mock connection
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    # Patch adapter registry to prevent it from trying to apply to mock
    with patch('database.adapter_registry'):
        # Create pool with very short idle time
        pool = ConnectionPool(mock_db_options, max_connections=3, max_idle_time=0.1)

        # Get and release a connection
        conn1 = pool.get_connection()
        pool.release_connection(conn1)

        # Wait for the connection to expire
        time.sleep(0.2)

        # Create a new mock connection for the next call
        mock_conn2 = MagicMock()
        mock_connect.return_value = mock_conn2

        # Get a connection - should create a new one since the old one expired
        conn2 = pool.get_connection()
        assert conn2 == mock_conn2
        assert mock_connect.call_count == 2

        # Verify the expired connection was closed
        mock_conn.connection.close.assert_called_once()


@patch('database.core.connection.connect')
def test_connection_pool_close_all(mock_connect, mock_db_options):
    """Test connection pool close_all method"""
    # Setup mock connections
    mock_conn1 = MagicMock()
    mock_conn2 = MagicMock()
    mock_connect.side_effect = [mock_conn1, mock_conn2]

    # Patch adapter registry to prevent it from trying to apply to mock
    with patch('database.adapter_registry'):
        # Create pool
        pool = ConnectionPool(mock_db_options, max_connections=3)

        # Get and release connections
        conn1 = pool.get_connection()
        conn2 = pool.get_connection()
        pool.release_connection(conn1)
        # Keep conn2 in use

        # Close all connections
        pool.close_all()

        # Verify released connection was closed
        mock_conn1.connection.close.assert_called_once()

        # In-use connection should not be forcibly closed
        mock_conn2.connection.close.assert_not_called()


@patch('database.core.connection.connect')
def test_connection_pool_error_handling(mock_connect, mock_db_options):
    """Test connection pool handles errors gracefully"""
    # Setup mock connection that raises error on close
    mock_conn = MagicMock()
    mock_conn.connection.close.side_effect = Exception('Connection close error')
    mock_connect.return_value = mock_conn

    # Patch adapter registry to prevent it from trying to apply to mock
    with patch('database.adapter_registry'):
        # Create pool
        pool = ConnectionPool(mock_db_options, max_connections=3, max_idle_time=0.1)

        # Get and release a connection
        conn1 = pool.get_connection()
        pool.release_connection(conn1)

        # Wait for the connection to expire
        time.sleep(0.2)

        # Create a new mock connection for the next call
        mock_conn2 = MagicMock()
        mock_connect.return_value = mock_conn2

        # This should not raise despite error in closing expired connection
        conn2 = pool.get_connection()
        assert conn2 == mock_conn2


if __name__ == '__main__':
    __import__('pytest').main([__file__])
