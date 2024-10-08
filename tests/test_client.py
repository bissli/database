import database as db


def test_select(psql_docker, conn):
    # Perform select query
    query = 'SELECT name, value FROM test_table ORDER BY id'
    result = db.select(conn, query)

    # Check results
    expected_data = {'name': ['Alice', 'Bob', 'Charlie', 'Ethan', 'Fiona', 'George'], 'value': [10, 20, 30, 50, 70, 80]}
    assert result.to_dict('list') == expected_data, 'The select query did not return the expected results.'


def test_select_numeric(psql_docker, conn):
    """Test custom numeric adapter (skip the Decimal creation)
    """
    # Perform select query
    query = 'SELECT name, value::numeric as value FROM test_table ORDER BY id'
    result = db.select(conn, query)

    # Check results
    expected_data = {'name': ['Alice', 'Bob', 'Charlie', 'Ethan', 'Fiona', 'George'], 'value': [10, 20, 30, 50, 70, 80]}
    assert result.to_dict('list') == expected_data, 'The select query did not return the expected results.'


def test_insert(psql_docker, conn):
    # Perform insert operation
    insert_sql = 'INSERT INTO test_table (name, value) VALUES (%s, %s)'
    db.insert(conn, insert_sql, 'Diana', 40)

    # Verify insert operation
    query = "SELECT name, value FROM test_table WHERE name = 'Diana'"
    result = db.select(conn, query)
    expected_data = {'name': ['Diana'], 'value': [40]}
    assert result.to_dict('list') == expected_data, 'The insert operation did not insert the expected data.'


def test_update(psql_docker, conn):
    # Perform update operation
    update_sql = 'UPDATE test_table SET value = %s WHERE name = %s'
    db.update(conn, update_sql, 60, 'Ethan')

    # Verify update operation
    query = "SELECT name, value FROM test_table WHERE name = 'Ethan'"
    result = db.select(conn, query)
    expected_data = {'name': ['Ethan'], 'value': [60]}
    assert result.to_dict('list') == expected_data, 'The update operation did not update the data as expected.'


def test_delete(psql_docker, conn):
    # Perform delete operation
    delete_sql = 'DELETE FROM test_table WHERE name = %s'
    db.delete(conn, delete_sql, 'Fiona')

    # Verify delete operation
    query = "SELECT name, value FROM test_table WHERE name = 'Fiona'"
    result = db.select(conn, query)
    assert result.empty, 'The delete operation did not delete the data as expected.'


def test_transaction(psql_docker, conn):
    # Perform transaction operations
    update_sql = 'UPDATE test_table SET value = %s WHERE name = %s'
    insert_sql = 'INSERT INTO test_table (name, value) VALUES (%s, %s)'
    with db.transaction(conn) as tx:
        tx.execute(update_sql, 91, 'George')
        tx.execute(insert_sql, 'Hannah', 102)

    # Verify transaction operations
    query = 'SELECT name, value FROM test_table ORDER BY name'
    result = db.select(conn, query)
    expected_data = {'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Ethan', 'George', 'Hannah'], 'value': [10, 20, 30, 40, 60, 91, 102]}
    assert result.to_dict('list') == expected_data, 'The transaction operations did not produce the expected results.'


def test_connect_respawn(psql_docker, conn):
    query = 'select count(1) from test_table'
    db.select_scalar(conn, query)
    conn.close()  # will respawn on close
    result = db.select_scalar(conn, query)
    expected_data = 7
    assert result == expected_data


def test_upsert(psql_docker, conn):
    """Assert row count is accurate"""
    rows = [{'name': 'Barry', 'value': 50}, {'name': 'Wallace', 'value': 92}]
    assert 2 == db.upsert_rows(conn, 'test_table', rows, update_cols_always=['value'])

    # same value, no update
    rows = [{'name': 'Barry', 'value': 53}]
    assert 1 == db.upsert_rows(conn, 'test_table', rows, update_cols_ifnull=['value'])
    res = db.select_scalar(conn, 'select value from test_table where name = %s', 'Barry')
    assert int(res) == 50

    # auto lookup primary key
    rows = [{'name': 'Barry', 'value': 52}]
    assert 1 == db.upsert_rows(conn, 'test_table', rows, update_cols_always=['value'])
    res = db.select_scalar(conn, 'select value from test_table where name = %s', 'Barry')
    assert int(res) == 52

    # pass in primary key
    rows = [{'name': 'Barry', 'value': 51}, {'name': 'Wallace', 'value': 90}]
    assert 2 == db.upsert_rows(conn, 'test_table', rows, update_cols_key=['name'], update_cols_always=['value'])
    res = db.select_scalar(conn, 'select value from test_table where name = %s', 'Wallace')
    assert int(res) == 90
    res = db.select_scalar(conn, 'select value from test_table where name = %s', 'Barry')
    assert int(res) == 51

    # pass in non-existent column
    rows = [{'name': 'Barry', 'value': 0}]
    db.upsert_rows(conn, 'test_table', rows, update_cols_key=['name'], update_cols_always=['not-value'])
    res = db.select_scalar(conn, 'select value from test_table where name = %s', 'Barry')
    assert int(res) == 51


if __name__ == '__main__':
    __import__('pytest').main([__file__])
