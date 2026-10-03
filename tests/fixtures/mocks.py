"""Stand-ins for a DBAPI connection, typed by driver module alone.
"""
import pytest

_DRIVER_MODULES = {
    'postgresql': 'psycopg',
    'sqlite': 'sqlite3',
    'unknown': 'unknown_db',
    }


@pytest.fixture
def create_simple_mock_connection():
    """Factory of objects whose class reads as the named driver's Connection.
    """
    def factory(connection_type='postgresql'):
        class MockConn:
            pass

        if connection_type in _DRIVER_MODULES:
            MockConn.__module__ = _DRIVER_MODULES[connection_type]
            MockConn.__qualname__ = 'Connection'
            MockConn.__name__ = 'Connection'
        return MockConn()

    return factory
