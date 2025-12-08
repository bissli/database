import database as db


def test_empty_results_handling(psql_docker, pg_conn):
    """Test that empty query results are handled properly"""

    # Test select with no results
    result = db.select(pg_conn, 'SELECT * FROM test_table WHERE 1=0')
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 0

    # Test select_column with no results
    result = db.select_column(pg_conn, 'SELECT name FROM test_table WHERE 1=0')
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 0

    # Test select_row_or_none with no results
    result = db.select_row_or_none(pg_conn, 'SELECT * FROM test_table WHERE 1=0')
    assert result is None  # This should still be None by design

    # Test select_scalar_or_none with no results
    result = db.select_scalar_or_none(pg_conn, 'SELECT COUNT(*) FROM test_table WHERE 1=0')
    assert result == 0  # This should not be None

    # Test transaction select with no results
    with db.transaction(pg_conn) as tx:
        result = tx.select('SELECT * FROM test_table WHERE 1=0')
        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 0

        # Check select_column in transaction
        column = tx.select_column('SELECT name FROM test_table WHERE 1=0')
        assert column is not None
        assert isinstance(column, list)
        assert len(column) == 0


def test_empty_results_key_preservation(psql_docker, pg_conn):
    """Test that empty query results preserve dictionary keys"""

    # Create a temporary table with specific columns
    with db.transaction(pg_conn) as tx:
        tx.execute("""
            CREATE TEMPORARY TABLE empty_test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value DOUBLE PRECISION,
                created_at TIMESTAMP
            )
        """)

        # Run a query that returns no rows but should preserve keys
        result = tx.select('SELECT id, name, value, created_at FROM empty_test')

        # Verify structure is preserved
        assert isinstance(result, list)
        assert len(result) == 0

        # To check the dictionary keys we need to insert a row and then delete it
        # to get the column structure
        tx.execute("INSERT INTO empty_test VALUES (1, 'test', 1.0, CURRENT_TIMESTAMP)")
        result_with_row = tx.select('SELECT id, name, value, created_at FROM empty_test')
        assert len(result_with_row) == 1
        assert list(result_with_row[0].keys()) == ['id', 'name', 'value', 'created_at']

        # Clean up the row
        tx.execute('DELETE FROM empty_test')

        # Test with specific column order
        result2_with_row = tx.select('SELECT created_at, name, id FROM empty_test')
        assert len(result2_with_row) == 0

        # Insert another row to check order
        tx.execute("INSERT INTO empty_test VALUES (2, 'test2', 2.0, CURRENT_TIMESTAMP)")
        result2_with_row = tx.select('SELECT created_at, name, id FROM empty_test')
        assert list(result2_with_row[0].keys()) == ['created_at', 'name', 'id']


if __name__ == '__main__':
    __import__('pytest').main([__file__])
