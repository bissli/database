"""Fixtures for PostgreSQL-only integration tests.
"""
import time

import pytest


@pytest.fixture
def test_table_prefix():
    """Table name prefix unique to the current second, for isolation.
    """
    return f'test_autocommit_{int(time.time())}'
