import io

import database as db
from database.strategy import PostgresStrategy


def test_vacuum_table(psql_docker, pg_conn):
    """Test the vacuum_table functionality"""
    # This operation requires no special setup, just call it on an existing table
    db.vacuum_table(pg_conn, 'test_table')

    # There's no direct way to verify vacuum ran, but check the table still exists
    count = db.select_scalar(pg_conn, 'select count(*) from test_table')
    assert count > 0, 'Table should still exist and contain data after vacuum'


def test_reindex_table(psql_docker, pg_conn):
    """Test the reindex_table functionality"""
    # Create an index to reindex
    db.execute(pg_conn, """
        CREATE INDEX IF NOT EXISTS test_idx_value ON test_table(value)
    """)

    # Perform reindex
    db.reindex_table(pg_conn, 'test_table')

    # Verify the index still exists
    index_exists = db.select_scalar(pg_conn, """
        SELECT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE tablename = 'test_table' AND indexname = 'test_idx_value'
        )
    """)
    assert index_exists, 'Index should still exist after reindex'


def test_cluster_table(psql_docker, pg_conn):
    """Test the cluster_table functionality"""
    # Create an index to cluster on
    db.execute(pg_conn, """
        CREATE INDEX IF NOT EXISTS test_idx_cluster ON test_table(value)
    """)

    # Perform cluster
    db.cluster_table(pg_conn, 'test_table', 'test_idx_cluster')

    # There's no direct way to verify clustering worked, but the operation should not error
    # Check table still has correct data
    count = db.select_scalar(pg_conn, 'select count(*) from test_table')
    assert count > 0, 'Table should still contain data after clustering'


def test_strategy_get_primary_keys(psql_docker, pg_conn):
    """Test that get_primary_keys correctly identifies primary keys"""
    # Create a table with composite primary key
    db.execute(pg_conn, """
        CREATE TEMPORARY TABLE test_composite_pk (
            id1 INT,
            id2 INT,
            data TEXT,
            PRIMARY KEY (id1, id2)
        )
    """)

    # Get primary keys using the strategy directly
    strategy = PostgresStrategy()
    pk_columns = strategy.get_primary_keys(pg_conn, 'test_composite_pk')

    # Verify primary keys
    assert set(pk_columns) == {'id1', 'id2'}

    # Test with the regular table
    pk_columns = strategy.get_primary_keys(pg_conn, 'test_table')
    assert pk_columns == ['name'], "Should correctly identify 'name' as primary key"


def test_strategy_get_sequence_columns(psql_docker, pg_conn):
    """Test that get_sequence_columns correctly identifies sequence columns"""
    # Create a table with a serial column
    db.execute(pg_conn, """
        CREATE TEMPORARY TABLE test_sequence_columns (
            id SERIAL PRIMARY KEY,
            non_serial_id INT,
            data TEXT
        )
    """)

    # Get sequence columns using the strategy directly
    strategy = PostgresStrategy()
    seq_columns = strategy.get_sequence_columns(pg_conn, 'test_sequence_columns')

    # Verify sequence columns
    assert 'id' in seq_columns
    assert 'non_serial_id' not in seq_columns


def test_copy_from(psql_docker, pg_conn):
    """Test bulk loading data using PostgreSQL COPY."""
    db.execute(pg_conn, """
        CREATE TEMPORARY TABLE test_copy (
            name TEXT,
            value INTEGER
        )
    """)

    csv_data = io.StringIO('David,40\nEva,50\nFrank,60\n')
    rowcount = db.copy_from(pg_conn, 'test_copy', csv_data, ['name', 'value'])

    assert rowcount == 3

    rows = db.select(pg_conn, 'SELECT name, value FROM test_copy ORDER BY value')
    assert len(rows) == 3
    assert rows[0]['name'] == 'David'
    assert rows[0]['value'] == 40
    assert rows[2]['name'] == 'Frank'
    assert rows[2]['value'] == 60


def test_copy_from_without_columns(psql_docker, pg_conn):
    """Test COPY without specifying columns uses table column order."""
    db.execute(pg_conn, """
        CREATE TEMPORARY TABLE test_copy_nocol (
            col1 TEXT,
            col2 INTEGER
        )
    """)

    csv_data = io.StringIO('Alice,100\nBob,200\n')
    rowcount = db.copy_from(pg_conn, 'test_copy_nocol', csv_data)

    assert rowcount == 2

    count = db.select_scalar(pg_conn, 'SELECT COUNT(*) FROM test_copy_nocol')
    assert count == 2


def test_copy_from_empty_file(psql_docker, pg_conn):
    """Test COPY with empty file inserts no rows."""
    db.execute(pg_conn, """
        CREATE TEMPORARY TABLE test_copy_empty (
            name TEXT,
            value INTEGER
        )
    """)

    csv_data = io.StringIO('')
    rowcount = db.copy_from(pg_conn, 'test_copy_empty', csv_data, ['name', 'value'])

    assert rowcount == 0

    count = db.select_scalar(pg_conn, 'SELECT COUNT(*) FROM test_copy_empty')
    assert count == 0


if __name__ == '__main__':
    __import__('pytest').main([__file__])
