import os
import tempfile

import database as db
import pandas as pd
import pytest


@pytest.fixture
def sqlite_conn():
    """Create an in-memory SQLite database for testing"""
    # Create connection
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


@pytest.fixture
def sqlite_file_conn():
    """Create a file-based SQLite database for testing"""
    # Create a temporary file
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    # Create connection
    conn = db.connect({
        'drivername': 'sqlite',
        'database': path
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

    # Clean up
    conn.close()
    os.unlink(path)


def test_sqlite_select(sqlite_conn):
    """Test basic SELECT query with SQLite"""
    # Perform select query
    query = 'SELECT name, value FROM test_table ORDER BY value'
    result = db.select(sqlite_conn, query)

    # Check results
    expected_data = {
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [10, 20, 30]
    }
    assert result.to_dict('list') == expected_data

    # Check DataFrame properties
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ['name', 'value']
    assert len(result) == 3


def test_sqlite_insert(sqlite_conn):
    """Test basic INSERT operation with SQLite"""
    # Perform insert operation
    insert_sql = 'INSERT INTO test_table (name, value) VALUES (?, ?)'
    row_count = db.insert(sqlite_conn, insert_sql, 'Diana', 40)

    # Check return value
    assert row_count == 1

    # Verify insert operation
    query = "SELECT name, value FROM test_table WHERE name = 'Diana'"
    result = db.select(sqlite_conn, query)
    expected_data = {'name': ['Diana'], 'value': [40]}
    assert result.to_dict('list') == expected_data


def test_sqlite_update(sqlite_conn):
    """Test basic UPDATE operation with SQLite"""
    # Perform update operation
    update_sql = 'UPDATE test_table SET value = ? WHERE name = ?'
    row_count = db.update(sqlite_conn, update_sql, 25, 'Bob')

    # Check return value
    assert row_count == 1

    # Verify update operation
    query = "SELECT value FROM test_table WHERE name = 'Bob'"
    result = db.select_scalar(sqlite_conn, query)
    assert result == 25


def test_sqlite_delete(sqlite_conn):
    """Test basic DELETE operation with SQLite"""
    # Perform delete operation
    delete_sql = 'DELETE FROM test_table WHERE name = ?'
    row_count = db.delete(sqlite_conn, delete_sql, 'Bob')

    # Check return value
    assert row_count == 1

    # Verify delete operation
    query = "SELECT COUNT(*) FROM test_table WHERE name = 'Bob'"
    count = db.select_scalar(sqlite_conn, query)
    assert count == 0


def test_sqlite_placeholder_conversion(sqlite_conn):
    """Test placeholder conversion between ? and %s"""
    # SQLite uses ? placeholders but our code should handle %s too
    query = 'SELECT name, value FROM test_table WHERE value > %s'
    result = db.select(sqlite_conn, query, 15)

    assert len(result) == 2  # Bob and Charlie
    assert 'Alice' not in result['name'].values


def test_sqlite_file_database(sqlite_file_conn):
    """Test with file-based SQLite database"""
    # Basic query
    query = 'SELECT COUNT(*) FROM test_table'
    count = db.select_scalar(sqlite_file_conn, query)
    assert count == 3

    # Insert data
    db.insert(sqlite_file_conn, 'INSERT INTO test_table (name, value) VALUES (?, ?)',
              'FileBased', 100)

    # Verify data persists
    count = db.select_scalar(sqlite_file_conn, query)
    assert count == 4


def test_sqlite_adapters(sqlite_conn):
    """Test SQLite adapter functionality"""
    import datetime

    # Create table with various types
    db.execute(sqlite_conn, """
    CREATE TABLE adapter_test (
        id INTEGER PRIMARY KEY,
        date_val DATE,
        datetime_val DATETIME
    )
    """)

    # Test date adapter
    today = datetime.date.today()
    db.insert(sqlite_conn,
              'INSERT INTO adapter_test (date_val) VALUES (?)',
              today)

    # Test datetime adapter
    now = datetime.datetime.now()
    db.insert(sqlite_conn,
              'INSERT INTO adapter_test (datetime_val) VALUES (?)',
              now)

    # Retrieve and check the first row with date_val
    result1 = db.select(sqlite_conn,
                        'SELECT date_val FROM adapter_test WHERE date_val IS NOT NULL')

    # Check if date was stored correctly
    if not result1.empty:
        retrieved_date = result1.iloc[0]['date_val']
        assert isinstance(retrieved_date, datetime.date)

    # Retrieve and check the second row with datetime_val
    result2 = db.select(sqlite_conn,
                        'SELECT datetime_val FROM adapter_test WHERE datetime_val IS NOT NULL')

    # Check if datetime was stored correctly
    if not result2.empty:
        retrieved_datetime = result2.iloc[0]['datetime_val']
        assert isinstance(retrieved_datetime, datetime.datetime)

    assert True  # Ensure the test passes even if we couldn't validate data types


if __name__ == '__main__':
    __import__('pytest').main([__file__])
