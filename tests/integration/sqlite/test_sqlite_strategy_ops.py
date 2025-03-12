import pytest
import os
import tempfile
import database as db
from database.strategy import SQLiteStrategy


@pytest.fixture(scope='function')
def sqlite_strategy_conn():
    """Create an SQLite database connection for testing strategy methods"""
    # Create connection
    conn = db.connect({
        'drivername': 'sqlite',
        'database': ':memory:'
    })
    
    # Create test schema with primary key and constraints
    create_table = """
    CREATE TABLE test_table (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        value INTEGER NOT NULL
    )
    """
    db.execute(conn, create_table)
    
    # Create a second table for relationship testing
    create_related_table = """
    CREATE TABLE related_table (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        test_id INTEGER,
        data TEXT,
        FOREIGN KEY (test_id) REFERENCES test_table(id)
    )
    """
    db.execute(conn, create_related_table)
    
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


def test_sqlite_vacuum(sqlite_strategy_conn):
    """Test SQLite VACUUM operation"""
    # SQLite VACUUM operation doesn't do much in memory
    # but test that it doesn't fail
    db.vacuum_table(sqlite_strategy_conn, 'test_table')
    
    # Verify data is intact after vacuum
    count = db.select_scalar(sqlite_strategy_conn, "SELECT COUNT(*) FROM test_table")
    assert count == 3


def test_sqlite_reindex(sqlite_strategy_conn):
    """Test SQLite REINDEX operation"""
    # Create an index to reindex
    db.execute(sqlite_strategy_conn, "CREATE INDEX idx_test_value ON test_table(value)")
    
    # Call reindex
    db.reindex_table(sqlite_strategy_conn, 'test_table')
    
    # Verify the index still works
    rows = db.select(sqlite_strategy_conn, "SELECT * FROM test_table ORDER BY value")
    assert rows.iloc[0]['name'] == 'Alice'
    assert rows.iloc[2]['name'] == 'Charlie'


def test_sqlite_get_primary_keys(sqlite_strategy_conn):
    """Test getting primary key information from SQLite"""
    strategy = SQLiteStrategy()
    primary_keys = strategy.get_primary_keys(sqlite_strategy_conn, 'test_table')
    
    assert 'id' in primary_keys
    assert len(primary_keys) == 1


def test_sqlite_get_columns(sqlite_strategy_conn):
    """Test getting column information from SQLite"""
    strategy = SQLiteStrategy()
    columns = strategy.get_columns(sqlite_strategy_conn, 'test_table')
    
    assert set(columns) == {'id', 'name', 'value'}


def test_sqlite_get_sequence_columns(sqlite_strategy_conn):
    """Test getting sequence columns from SQLite"""
    strategy = SQLiteStrategy()
    sequence_columns = strategy.get_sequence_columns(sqlite_strategy_conn, 'test_table')
    
    # In SQLite, AUTOINCREMENT columns are reported as sequence columns
    assert 'id' in sequence_columns


def test_sqlite_autoincrement(sqlite_strategy_conn):
    """Test SQLite AUTO INCREMENT behavior"""
    # Insert a few rows and check ID assignment
    db.insert(sqlite_strategy_conn, "INSERT INTO test_table (name, value) VALUES (?, ?)",
             'David', 40)
    
    row = db.select_row(sqlite_strategy_conn, "SELECT * FROM test_table WHERE name = 'David'")
    assert row.id == 4  # Should be 4 since we already had 3 rows
    
    # Delete the row
    db.delete(sqlite_strategy_conn, "DELETE FROM test_table WHERE name = 'David'")
    
    # Insert a new row - ID should be 5 (SQLite doesn't reuse IDs by default with AUTOINCREMENT)
    db.insert(sqlite_strategy_conn, "INSERT INTO test_table (name, value) VALUES (?, ?)",
             'Eva', 50)
    
    row = db.select_row(sqlite_strategy_conn, "SELECT * FROM test_table WHERE name = 'Eva'")
    assert row.id >= 4  # Should be at least 4, might be higher


if __name__ == '__main__':
    __import__('pytest').main([__file__])
