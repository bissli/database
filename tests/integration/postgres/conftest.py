"""
Fixtures for PostgreSQL-specific integration tests.
"""
import time

import pytest


@pytest.fixture
def test_table_prefix():
    """Generate a unique test table prefix for isolation."""
    return f'test_autocommit_{int(time.time())}'
