"""
Integration tests for SQL Server transaction handling.

These tests specifically focus on the behavior of transactions in SQL Server
to identify potential issues with commits, rollbacks, and data visibility.
"""
import database as db
import pytest
from database.utils.auto_commit import diagnose_connection


def test_sqlserver_basic_transaction(sconn):
    """Test basic transaction with a simple insert and verify"""

    # First clean up any existing test table
    with db.transaction(sconn) as tx:
        tx.execute("IF OBJECT_ID('dbo.transaction_test', 'U') IS NOT NULL DROP TABLE dbo.transaction_test")
        tx.execute('CREATE TABLE dbo.transaction_test (id INT, value VARCHAR(100))')

    # Verify the table exists and is empty
    with db.transaction(sconn) as tx:
        count = tx.select_scalar('SELECT COUNT(*) FROM dbo.transaction_test')
        assert count == 0, 'Table should be empty after creation'

    # Insert in a transaction and commit
    with db.transaction(sconn) as tx:
        # Log connection state at the beginning
        conn_state = diagnose_connection(sconn)
        print(f'Connection state at start: {conn_state}')

        # Insert a test row
        tx.execute('INSERT INTO dbo.transaction_test (id, value) VALUES (?, ?)', 1, 'test value')

        # Verify the row exists within the same transaction
        count = tx.select_scalar('SELECT COUNT(*) FROM dbo.transaction_test')
        assert count == 1, 'Row should be visible within the transaction'

        # Log connection state before exit
        conn_state = diagnose_connection(sconn)
        print(f'Connection state before exit: {conn_state}')

    # Verify the row still exists after transaction commit
    with db.transaction(sconn) as tx:
        count = tx.select_scalar('SELECT COUNT(*) FROM dbo.transaction_test')
        assert count == 1, 'Row should be visible after transaction commit'

    # Test rollback
    try:
        with db.transaction(sconn) as tx:
            tx.execute('INSERT INTO dbo.transaction_test (id, value) VALUES (?, ?)', 2, 'will rollback')
            # Verify the row exists within the transaction
            count = tx.select_scalar('SELECT COUNT(*) FROM dbo.transaction_test')
            assert count == 2, 'New row should be visible within the transaction'
            # Force an exception to trigger rollback
            raise ValueError('Forced exception to test rollback')
    except ValueError:
        pass  # Expected exception

    # Verify the second row was rolled back
    with db.transaction(sconn) as tx:
        count = tx.select_scalar('SELECT COUNT(*) FROM dbo.transaction_test')
        assert count == 1, 'Only the first row should exist after rollback'

    # Clean up
    with db.transaction(sconn) as tx:
        tx.execute('DROP TABLE dbo.transaction_test')


def test_sqlserver_nested_transaction_error(sconn):
    """Test that nested transactions raise an appropriate error"""

    with db.transaction(sconn) as tx1:
        # Attempting to start a nested transaction should raise an error
        with pytest.raises(RuntimeError) as excinfo:
            with db.transaction(sconn) as tx2:
                pass

        assert 'Nested transactions are not supported' in str(excinfo.value)


def test_sqlserver_autocommit_behavior(sconn):
    """Test autocommit behavior outside of explicit transactions"""

    # Clean up any existing test table - USE db.execute instead of direct connection execute
    db.execute(sconn, "IF OBJECT_ID('dbo.autocommit_test', 'U') IS NOT NULL DROP TABLE dbo.autocommit_test")
    db.execute(sconn, 'CREATE TABLE dbo.autocommit_test (id INT, value VARCHAR(100))')

    # Insert without an explicit transaction (should use autocommit)
    db.execute(sconn, 'INSERT INTO dbo.autocommit_test (id, value) VALUES (?, ?)', 1, 'auto commit')

    # Verify the row exists
    count = db.select_scalar(sconn, 'SELECT COUNT(*) FROM dbo.autocommit_test')
    assert count == 1, 'Row should be committed with autocommit'

    # Clean up
    db.execute(sconn, 'DROP TABLE dbo.autocommit_test')


def test_sqlserver_multiple_statements_transaction(sconn):
    """Test multiple statements in a single transaction"""

    # Clean up any existing test table
    with db.transaction(sconn) as tx:
        tx.execute("IF OBJECT_ID('dbo.multi_test', 'U') IS NOT NULL DROP TABLE dbo.multi_test")
        tx.execute('CREATE TABLE dbo.multi_test (id INT, value VARCHAR(100))')

    # Execute multiple statements in a single transaction
    with db.transaction(sconn) as tx:
        # Insert multiple rows
        for i in range(5):
            tx.execute(
                'INSERT INTO dbo.multi_test (id, value) VALUES (?, ?)',
                i, f'value {i}'
            )

        # Verify all rows exist within the transaction
        count = tx.select_scalar('SELECT COUNT(*) FROM dbo.multi_test')
        assert count == 5, 'All rows should be visible within the transaction'

    # Verify all rows exist after transaction commit
    with db.transaction(sconn) as tx:
        count = tx.select_scalar('SELECT COUNT(*) FROM dbo.multi_test')
        assert count == 5, 'All rows should be visible after transaction commit'

    # Clean up
    with db.transaction(sconn) as tx:
        tx.execute('DROP TABLE dbo.multi_test')


def test_sqlserver_hardcoded_values_transaction(sconn):
    """Test transaction with hardcoded values (no placeholders)"""

    # Create a test table
    with db.transaction(sconn) as tx:
        tx.execute("IF OBJECT_ID('dbo.hardcoded_test', 'U') IS NOT NULL DROP TABLE dbo.hardcoded_test")
        tx.execute('CREATE TABLE dbo.hardcoded_test (id INT, name VARCHAR(50), value DECIMAL(10,2))')

    # Execute transaction with hardcoded values (no parameter placeholders)
    with db.transaction(sconn) as tx:
        # Insert with hardcoded values - no placeholders
        tx.execute("INSERT INTO dbo.hardcoded_test (id, name, value) VALUES (1, 'Test Item', 123.45)")
        tx.execute("INSERT INTO dbo.hardcoded_test (id, name, value) VALUES (2, 'Another Item', 678.90)")

        # Even query with hardcoded filter condition
        count = tx.select_scalar("SELECT COUNT(*) FROM dbo.hardcoded_test WHERE name = 'Test Item'")
        assert count == 1, 'Should find exactly one row with the hardcoded name'

        # Query with hardcoded arithmetic on values - using general select instead of select_scalar
        result = tx.select('SELECT SUM(value) AS total FROM dbo.hardcoded_test')
        print(f'SUM query result: {result}')

        # Check if we got any results at all
        assert len(result) > 0, 'SUM query should return at least one row'

        # Now try accessing the value - first check what columns are present
        if result:
            print(f'Available columns: {list(result[0].keys())}')

        # Try to access the total
        if result and len(result) > 0:
            if 'total' in result[0]:
                total = result[0]['total']
            elif 'SUM(value)' in result[0]:
                total = result[0]['SUM(value)']
            else:
                # Try first column whatever it's called
                total = list(result[0].values())[0]

            assert abs(float(total) - 802.35) < 0.001, 'Sum should be 802.35'

    # Verify data persists after transaction
    with db.transaction(sconn) as tx:
        rows = tx.select('SELECT * FROM dbo.hardcoded_test ORDER BY id')
        assert len(rows) == 2, 'Should have two rows after commit'

        assert rows[0]['id'] == 1
        assert rows[0]['name'] == 'Test Item'
        assert abs(float(rows[0]['value']) - 123.45) < 0.001

        assert rows[1]['id'] == 2
        assert rows[1]['name'] == 'Another Item'
        assert abs(float(rows[1]['value']) - 678.90) < 0.001

    # Clean up
    with db.transaction(sconn) as tx:
        tx.execute('DROP TABLE dbo.hardcoded_test')


def test_sqlserver_hardcoded_literals_transaction(sconn):
    """Test transaction with different types of hardcoded literals to isolate the issue"""

    # Create a test table
    with db.transaction(sconn) as tx:
        tx.execute("IF OBJECT_ID('dbo.literal_test', 'U') IS NOT NULL DROP TABLE dbo.literal_test")
        tx.execute("CREATE TABLE dbo.literal_test (id INT, name VARCHAR(50), value DECIMAL(10,2))")

    # Test different types of literals within a transaction
    with db.transaction(sconn) as tx:
        # Insert with literals
        tx.execute("INSERT INTO dbo.literal_test (id, name, value) VALUES (1, 'Test', 10.5)")
        
        # Test various direct queries to see what works
        print("\nTesting direct literal queries:")
        
        # Simple SELECT with no WHERE
        result1 = tx.select("SELECT * FROM dbo.literal_test")
        print(f"Plain SELECT: {result1}")
        
        # SELECT with hardcoded WHERE clause
        result2 = tx.select("SELECT * FROM dbo.literal_test WHERE id = 1")
        print(f"SELECT with hardcoded WHERE: {result2}")
        
        # SELECT with parameterized WHERE clause
        result3 = tx.select("SELECT * FROM dbo.literal_test WHERE id = ?", 1)
        print(f"SELECT with parameterized WHERE: {result3}")
        
        # Aggregate without GROUP BY
        result4 = tx.select("SELECT SUM(value) AS sum_value FROM dbo.literal_test")
        print(f"SUM aggregate: {result4}")
        
        # Aggregate with direct value
        result5 = tx.select("SELECT 100.5 + SUM(value) AS calculated FROM dbo.literal_test")
        print(f"SUM with direct value: {result5}")
        
        # Simple direct value
        result6 = tx.select("SELECT 42 AS answer")
        print(f"Direct value: {result6}")
        
        # Direct string literal
        result7 = tx.select("SELECT 'hello' AS greeting")
        print(f"String literal: {result7}")
        
        # Try a more complex query
        tx.execute("INSERT INTO dbo.literal_test (id, name, value) VALUES (2, 'Another', 20.5)")
        result8 = tx.select("SELECT id, name, value FROM dbo.literal_test ORDER BY id")
        print(f"Multi-row result: {result8}")
        
        # Try COUNT with literal comparison
        result9 = tx.select("SELECT COUNT(*) AS row_count FROM dbo.literal_test WHERE value > 5")
        print(f"COUNT with literal comparison: {result9}")
        
        # Try different method with aggregate - using a subquery
        result10 = tx.select("SELECT * FROM (SELECT SUM(value) AS total FROM dbo.literal_test) t")
        print(f"SUM via subquery: {result10}")
        
        # Try without alias
        result11 = tx.select("SELECT SUM(value) FROM dbo.literal_test")
        print(f"SUM without alias: {result11}")
        
    # Clean up
    with db.transaction(sconn) as tx:
        tx.execute("DROP TABLE dbo.literal_test")


if __name__ == '__main__':
    __import__('pytest').main([__file__])
