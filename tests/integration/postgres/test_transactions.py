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
    assert result[0]['name'] == 'George'
    assert result[0]['value'] == 91
    assert result[1]['name'] == 'Hannah'
    assert result[1]['value'] == 102


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
        assert result[0]['value'] == 200


def test_transaction_execute_returning_dict_params(psql_docker, conn):
    """Test execute with RETURNING clause and dictionary parameters"""
    # Create a table with an auto-incrementing ID and other columns
    db.execute(conn, """
    CREATE TEMPORARY TABLE returning_test (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        value INTEGER NOT NULL
    )
    """)

    # Test with a dictionary parameter and returnid
    with db.transaction(conn) as tx:
        params = {
            'name': 'TestDict',
            'value': 100
        }

        # Execute INSERT with RETURNING clause
        result_id = tx.execute("""
        INSERT INTO returning_test (name, value)
        VALUES (%(name)s, %(value)s)
        RETURNING id
        """, params, returnid='id')

        # Verify the result is not None and is a number
        assert result_id is not None
        assert isinstance(result_id, int)
        assert result_id > 0

        # Verify the row exists
        row = tx.select_row('SELECT * FROM returning_test WHERE id = %s', result_id)
        assert row['name'] == 'TestDict'
        assert row['value'] == 100


def test_transaction_execute_returning_multiple_values(psql_docker, conn):
    """Test execute with RETURNING multiple values and dictionary parameters"""
    # Create a table with multiple columns
    db.execute(conn, """
    CREATE TEMPORARY TABLE multi_return_test (
        id SERIAL PRIMARY KEY,
        inst_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        value INTEGER NOT NULL
    )
    """)

    # Test with a dictionary parameter and multiple returnid values
    with db.transaction(conn) as tx:
        params = {
            'inst_id': 33476,
            'name': 'MultiReturn',
            'value': 500
        }

        # Execute INSERT with RETURNING clause for multiple columns
        inst_id, id_val = tx.execute("""
        INSERT INTO multi_return_test (inst_id, name, value)
        VALUES (%(inst_id)s, %(name)s, %(value)s)
        RETURNING inst_id, id
        """, params, returnid=['inst_id', 'id'])

        # Verify both values were returned correctly
        assert inst_id == 33476
        assert isinstance(id_val, int)
        assert id_val > 0

        # Verify the row exists with correct values
        row = tx.select_row('SELECT * FROM multi_return_test WHERE id = %s', id_val)
        assert row['inst_id'] == 33476
        assert row['name'] == 'MultiReturn'
        assert row['value'] == 500


def test_transaction_execute_returning_multiple_rows(psql_docker, conn):
    """Test execute with RETURNING multiple rows of multiple values"""
    # Create a table for multi-row operations
    db.execute(conn, """
    CREATE TEMPORARY TABLE multi_row_test (
        id SERIAL PRIMARY KEY,
        category TEXT NOT NULL,
        value INTEGER NOT NULL
    )
    """)

    # Insert some initial data to update
    categories = ['electronics', 'clothing', 'food']
    for i, category in enumerate(categories):
        db.execute(conn, """
        INSERT INTO multi_row_test (category, value)
        VALUES (%s, %s)
        """, category, i*10)

    # Test with an UPDATE that affects multiple rows
    with db.transaction(conn) as tx:
        # Update all rows and get back multiple values from each
        results = tx.execute("""
        UPDATE multi_row_test
        SET value = value + 100
        RETURNING id, category, value
        """, returnid=['id', 'category', 'value'])

        # Verify we got a list of lists with all three expected rows
        assert isinstance(results, list)
        assert len(results) == 3  # Should match the number of categories

        # Each result should be a list of [id, category, value]
        for row in results:
            assert len(row) == 3
            assert isinstance(row[0], int)  # id
            assert isinstance(row[1], str)  # category
            assert isinstance(row[2], int)  # value
            assert row[2] >= 100  # The updated value should be at least 100

        # Check if all categories were included in the results
        result_categories = [row[1] for row in results]
        for category in categories:
            assert category in result_categories


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
        with pytest.raises(RuntimeError):
            # Call directly with the same connection
            db.transaction(conn)


if __name__ == '__main__':
    __import__('pytest').main([__file__])
