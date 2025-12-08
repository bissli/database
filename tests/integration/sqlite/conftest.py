"""
Fixtures for SQLite-specific integration tests.
"""
import time

import database as db
import pytest
import pathlib


@pytest.fixture
def test_table_prefix():
    """Generate a unique test table prefix for isolation."""
    return f'test_autocommit_{int(time.time())}'


@pytest.fixture
def sqlite_file_conn():
    """File-based SQLite connection fixture for testing persistence across connections."""
    db_file = f'./test_sqlite_{int(time.time())}.db'

    conn = db.connect({
        'drivername': 'sqlite',
        'database': db_file
    })

    # Create test schema
    create_table = """
    CREATE TABLE test_table (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        value INTEGER NOT NULL
    )
    """
    db.execute(conn, create_table)

    # Insert test data
    insert_data = """
    INSERT INTO test_table (name, value) VALUES
    ('Alice', 10),
    ('Bob', 20),
    ('Charlie', 30)
    """
    db.execute(conn, insert_data)

    yield conn

    conn.close()
    if pathlib.Path(db_file).exists():
        pathlib.Path(db_file).unlink()
