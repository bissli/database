"""
Mock connection utilities for database tests.

Provides simple mock connections for testing connection type detection
without requiring actual database connections.

Usage:
    def test_connection_detection(create_simple_mock_connection):
        pg_conn = create_simple_mock_connection('postgresql')
        sqlite_conn = create_simple_mock_connection('sqlite')
"""
import pytest


def _create_simple_mock_connection(connection_type='postgresql'):
    """
    Create a simple mock database connection with the specified connection type.

    This is a lightweight mock specifically designed for connection type detection
    testing.

    Args:
        connection_type: Database type ('postgresql', 'sqlite', 'unknown')

    Returns
        Simple mock connection object that will pass type detection
    """
    class MockConn:
        def __init__(self):
            pass

    conn = MockConn()

    if connection_type == 'postgresql':
        conn.__class__.__module__ = 'psycopg'
        conn.__class__.__qualname__ = 'Connection'
        conn.__class__.__name__ = 'Connection'
    elif connection_type == 'sqlite':
        conn.__class__.__module__ = 'sqlite3'
        conn.__class__.__qualname__ = 'Connection'
        conn.__class__.__name__ = 'Connection'
    elif connection_type == 'unknown':
        conn.__class__.__module__ = 'unknown_db'
        conn.__class__.__qualname__ = 'Connection'
        conn.__class__.__name__ = 'Connection'

    return conn


@pytest.fixture
def create_simple_mock_connection():
    """
    Fixture that provides a factory function to create simple mock connections.

    Returns
        Factory function that creates mock connections of specified type

    Example usage:
        def test_connection_detection(create_simple_mock_connection):
            pg_conn = create_simple_mock_connection('postgresql')
            sqlite_conn = create_simple_mock_connection('sqlite')
    """
    def factory(connection_type='postgresql'):
        return _create_simple_mock_connection(connection_type)

    return factory
