import database as db
import pytest


@pytest.fixture
def sqlite_conn():
    """Create an in-memory SQLite database for testing"""
    # Create connection with default pandas data_loader
    conn = db.connect({
        'drivername': 'sqlite',
        'database': ':memory:'
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
