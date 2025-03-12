import database as db


def test_simple_query(sconn):
    """Test basic query execution with SQL Server"""
    # Test simple select query
    result = db.select(sconn, 'SELECT name AS name, value AS value FROM test_table ORDER BY value')
    assert isinstance(result, list)
    assert len(result) == 6
    assert 'name' in result[0]
    assert 'value' in result[0]
    assert result[0]['name'] == 'Alice'
    assert result[0]['value'] == 10


def test_parameterized_query(sconn):
    """Test parameterized query execution"""
    # Test with single parameter
    result = db.select(sconn, 'SELECT name AS name FROM test_table WHERE value > %s', 30)
    assert isinstance(result, list)
    assert len(result) == 3
    names = [row['name'] for row in result]
    assert 'Fiona' in names

    # Test with multiple parameters
    result = db.select(sconn, 'SELECT name AS name FROM test_table WHERE value BETWEEN %s AND %s', 20, 50)
    assert len(result) == 3
    names = [row['name'] for row in result]
    assert set(names) == {'Bob', 'Charlie', 'Ethan'}


def test_connection_properties(sconn):
    """Test SQL Server connection properties"""
    from database.utils.connection_utils import is_pyodbc_connection
    from database.utils.connection_utils import isconnection
    assert isconnection(sconn)
    assert is_pyodbc_connection(sconn)


def test_data_manipulation(sconn):
    """Test data manipulation operations with SQL Server"""
    # Insert new row
    db.insert(sconn, 'INSERT INTO test_table (name, value) VALUES (%s, %s)',
              'David', 40)

    # Verify insertion
    result = db.select_row(sconn, 'SELECT name AS name, value AS value FROM test_table WHERE name = %s', 'David')
    assert result.name == 'David'
    assert result.value == 40

    # Update row
    db.update(sconn, 'UPDATE test_table SET value = %s WHERE name = %s', 45, 'David')

    # Verify update
    value = db.select_scalar(sconn, 'SELECT value AS value_col FROM test_table WHERE name = %s', 'David')
    assert value == 45

    # Delete row
    db.delete(sconn, 'DELETE FROM test_table WHERE name = %s', 'David')

    # Verify deletion
    exists = db.select_scalar_or_none(sconn, 'SELECT 1 AS exists_col FROM test_table WHERE name = %s', 'David')
    assert exists is None


def test_transaction(sconn):
    """Test transaction behavior with SQL Server"""
    # Start transaction
    with db.transaction(sconn) as tx:
        # Insert within transaction
        tx.execute('INSERT INTO test_table (name, value) VALUES (%s, %s)', 'Transaction', 100)

        # Verify row exists within transaction
        count = tx.select_scalar('SELECT COUNT(*) AS row_count FROM test_table WHERE name = %s', 'Transaction')
        assert count == 1

    # Verify commit happened
    count = db.select_scalar(sconn, 'SELECT COUNT(*) AS row_count FROM test_table WHERE name = %s', 'Transaction')
    assert count == 1

    # Test rollback
    try:
        with db.transaction(sconn) as tx:
            tx.execute('INSERT INTO test_table (name, value) VALUES (%s, %s)', 'Rollback', 200)

            # Verify row exists within transaction
            count = tx.select_scalar('SELECT COUNT(*) AS row_count FROM test_table WHERE name = %s', 'Rollback')
            assert count == 1

            # Trigger rollback with exception
            raise Exception('Force rollback')
    except Exception:
        pass

    # Verify row doesn't exist after rollback
    count = db.select_scalar(sconn, 'SELECT COUNT(*) AS row_count FROM test_table WHERE name = %s', 'Rollback')
    assert count == 0


if __name__ == '__main__':
    __import__('pytest').main([__file__])
