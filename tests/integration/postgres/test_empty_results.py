import database as db
import pandas as pd


def test_empty_results_handling(psql_docker, conn):
    """Test that empty query results are handled properly"""

    # Test select with no results
    result = db.select(conn, 'SELECT * FROM test_table WHERE 1=0')
    assert result is not None
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0

    # Test select_column with no results
    result = db.select_column(conn, 'SELECT name FROM test_table WHERE 1=0')
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 0

    # Test select_row_or_none with no results
    result = db.select_row_or_none(conn, 'SELECT * FROM test_table WHERE 1=0')
    assert result is None  # This should still be None by design

    # Test select_scalar_or_none with no results
    result = db.select_scalar_or_none(conn, 'SELECT COUNT(*) FROM test_table WHERE 1=0')
    assert result is None  # This should still be None by design

    # Test transaction select with no results
    with db.transaction(conn) as tx:
        result = tx.select('SELECT * FROM test_table WHERE 1=0')
        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

        # Check select_column in transaction
        column = tx.select_column('SELECT name FROM test_table WHERE 1=0')
        assert column is not None
        assert isinstance(column, list)
        assert len(column) == 0

    # Test callproc with no results
    result = db.callproc(conn, 'SELECT * FROM test_table WHERE 1=0')
    assert result is not None
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0

    # Test callproc with return_all and no results
    results = db.callproc(conn, 'SELECT * FROM test_table WHERE 1=0', return_all=True)
    assert results is not None
    assert isinstance(results, list)
    assert len(results) == 0 or (len(results) > 0 and len(results[0]) == 0)


def test_empty_results_column_preservation(psql_docker, conn):
    """Test that empty query results preserve column information"""
    
    # Create a temporary table with specific columns
    with db.transaction(conn) as tx:
        tx.execute("""
            CREATE TEMPORARY TABLE empty_test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value DOUBLE PRECISION,
                created_at TIMESTAMP
            )
        """)
        
        # Run a query that returns no rows but should preserve columns
        result = tx.select("SELECT id, name, value, created_at FROM empty_test")
        
        # Verify structure is preserved
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert list(result.columns) == ['id', 'name', 'value', 'created_at']
        
        # Test with specific column order
        result2 = tx.select("SELECT created_at, name, id FROM empty_test")
        assert list(result2.columns) == ['created_at', 'name', 'id']
        
        # Test callproc with empty results
        all_results = db.callproc(conn, """
            SELECT id, name FROM empty_test;
            SELECT value, created_at FROM empty_test;
        """, return_all=True)
        
        assert len(all_results) == 2
        assert list(all_results[0].columns) == ['id', 'name']
        assert list(all_results[1].columns) == ['value', 'created_at']


def test_empty_results_column_preservation(psql_docker, conn):
    """Test that empty query results preserve column information"""
    
    # Create a temporary table with specific columns
    with db.transaction(conn) as tx:
        tx.execute("""
            CREATE TEMPORARY TABLE empty_test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value DOUBLE PRECISION,
                created_at TIMESTAMP
            )
        """)
        
        # Run a query that returns no rows but should preserve columns
        result = tx.select("SELECT id, name, value, created_at FROM empty_test")
        
        # Verify structure is preserved
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert list(result.columns) == ['id', 'name', 'value', 'created_at']
        
        # Test with specific column order
        result2 = tx.select("SELECT created_at, name, id FROM empty_test")
        assert list(result2.columns) == ['created_at', 'name', 'id']
        
        # Test callproc with empty results
        all_results = db.callproc(conn, """
            SELECT id, name FROM empty_test;
            SELECT value, created_at FROM empty_test;
        """, return_all=True)
        
        assert len(all_results) == 2
        assert list(all_results[0].columns) == ['id', 'name']
        assert list(all_results[1].columns) == ['value', 'created_at']


if __name__ == '__main__':
    __import__('pytest').main([__file__])
