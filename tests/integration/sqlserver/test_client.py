import database as db
from database.utils.connection_utils import get_dialect_name, isconnection


def test_simple_query(sconn):
    """Test basic query execution with SQL Server.
    
    Verifies the select function returns properly structured results
    with the expected data from a simple SELECT query.
    """
    result = db.select(sconn, 'SELECT name AS name, value AS value FROM test_table ORDER BY value')
    assert isinstance(result, list)
    assert len(result) == 6
    assert 'name' in result[0]
    assert 'value' in result[0]
    assert result[0]['name'] == 'Alice'
    assert result[0]['value'] == 10


def test_parameterized_query(sconn):
    """Test parameterized query execution with SQL Server.
    
    Verifies both single and multi-parameter queries correctly filter 
    results from the test table.
    """
    result = db.select(sconn, 'SELECT name AS name FROM test_table WHERE value > %s', 30)
    assert isinstance(result, list)
    assert len(result) == 3
    names = [row['name'] for row in result]
    assert 'Fiona' in names

    result = db.select(sconn, 'SELECT name AS name FROM test_table WHERE value BETWEEN %s AND %s', 20, 50)
    assert len(result) == 3
    names = [row['name'] for row in result]
    assert set(names) == {'Bob', 'Charlie', 'Ethan'}


def test_connection_properties(sconn):
    """Test SQL Server connection properties.
    
    Verifies connection validation and dialect identification functions
    work correctly with SQL Server connections.
    """
    assert isconnection(sconn)
    assert get_dialect_name(sconn) == 'mssql'


def test_data_manipulation(sconn):
    """Test data manipulation operations with SQL Server.
    
    Verifies CRUD operations work correctly with SQL Server,
    testing a complete cycle of insert, select, update, and delete operations
    on a test record.
    """
    db.insert(sconn, 'INSERT INTO test_table (name, value) VALUES (%s, %s)',
              'David', 40)

    result = db.select_row(sconn, 'SELECT name AS name, value AS value FROM test_table WHERE name = %s', 'David')
    assert result.name == 'David'
    assert result.value == 40

    db.update(sconn, 'UPDATE test_table SET value = %s WHERE name = %s', 45, 'David')
    value = db.select_scalar(sconn, 'SELECT value AS value_col FROM test_table WHERE name = %s', 'David')
    assert value == 45

    db.delete(sconn, 'DELETE FROM test_table WHERE name = %s', 'David')
    exists = db.select_scalar_or_none(sconn, 'SELECT 1 AS exists_col FROM test_table WHERE name = %s', 'David')
    assert exists is None


def test_transaction(sconn):
    """Test transaction behavior with SQL Server.
    
    Verifies transactions can be committed successfully and rolled back
    properly when exceptions occur, ensuring data integrity in both cases.
    """
    with db.transaction(sconn) as tx:
        tx.execute('INSERT INTO test_table (name, value) VALUES (%s, %s)', 'Transaction', 100)

        count = tx.select_scalar('SELECT COUNT(*) AS row_count FROM test_table WHERE name = %s', 'Transaction')
        assert count == 1

    count = db.select_scalar(sconn, 'SELECT COUNT(*) AS row_count FROM test_table WHERE name = %s', 'Transaction')
    assert count == 1

    try:
        with db.transaction(sconn) as tx:
            tx.execute('INSERT INTO test_table (name, value) VALUES (%s, %s)', 'Rollback', 200)
            count = tx.select_scalar('SELECT COUNT(*) AS row_count FROM test_table WHERE name = %s', 'Rollback')
            assert count == 1

            raise Exception('Force rollback')
    except Exception:
        pass

    count = db.select_scalar(sconn, 'SELECT COUNT(*) AS row_count FROM test_table WHERE name = %s', 'Rollback')
    assert count == 0


if __name__ == '__main__':
    __import__('pytest').main([__file__])
