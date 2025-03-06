import database as db
import pytest



def test_transaction(psql_docker, conn):
    """Test successful transaction with multiple operations"""
    # Perform transaction operations
    update_sql = 'update test_table set value = %s where name = %s'
    insert_sql = 'insert into test_table (name, value) values (%s, %s)'
    
    with db.transaction(conn) as tx:
        tx.execute(update_sql, 91, 'George')
        tx.execute(insert_sql, 'Hannah', 102)

    # Verify transaction operations
    query = 'select name, value from test_table where name in (%s, %s) order by name'
    result = db.select(conn, query, 'George', 'Hannah')
    
    assert len(result) == 2
    assert result.iloc[0]['name'] == 'George'
    assert result.iloc[0]['value'] == 91
    assert result.iloc[1]['name'] == 'Hannah'
    assert result.iloc[1]['value'] == 102


def test_transaction_rollback(psql_docker, conn):
    """Test transaction rollback on error"""
    # First get current data for George
    original_value = db.select_scalar(conn, 'select value from test_table where name = %s', 'George')
    
    # Perform transaction with an error
    update_sql = 'update test_table set value = %s where name = %s'
    bad_sql = 'insert into nonexistent_table values (1)'
    
    try:
        with db.transaction(conn) as tx:
            tx.execute(update_sql, 999, 'George')
            # This should fail and trigger rollback
            tx.execute(bad_sql)
    except Exception:
        pass  # Expected exception
    
    # Verify the value was NOT updated due to rollback
    new_value = db.select_scalar(conn, 'select value from test_table where name = %s', 'George')
    assert new_value == original_value, 'Transaction should have rolled back'


def test_transaction_select(psql_docker, conn):
    """Test select within a transaction"""
    with db.transaction(conn) as tx:
        # Insert data
        tx.execute('insert into test_table (name, value) values (%s, %s)', 'TransactionTest', 200)
        
        # Select data within same transaction
        result = tx.select('select value from test_table where name = %s', 'TransactionTest')
        
        # Verify we can see our own changes
        assert len(result) == 1
        assert result.iloc[0]['value'] == 200


def test_connect_respawn(psql_docker, conn):
    """Test connection respawn behavior"""
    query = 'select count(1) from test_table'
    initial_count = db.select_scalar(conn, query)
    
    # Close connection
    conn.close()
    
    # Should reconnect automatically
    new_count = db.select_scalar(conn, query)
    assert new_count == initial_count


def test_nested_transactions_not_supported(psql_docker, conn):
    """Test that nested transactions raise appropriate errors"""
    with db.transaction(conn) as tx1:
        # Start a nested transaction - this should fail
        with pytest.raises(Exception):
            # Call directly with the same connection
            db.transaction(conn)


if __name__ == '__main__':
    __import__('pytest').main([__file__])
