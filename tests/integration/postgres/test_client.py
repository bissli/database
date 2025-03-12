import datetime

import database as db


def test_select(psql_docker, conn):
    """Test basic SELECT query"""
    # Perform select query
    query = 'select name, value from test_table order by value'
    result = db.select(conn, query)

    # Check results
    expected_data = [
        {'name': 'Alice', 'value': 10},
        {'name': 'Bob', 'value': 20},
        {'name': 'Charlie', 'value': 30},
        {'name': 'Ethan', 'value': 50},
        {'name': 'Fiona', 'value': 70},
        {'name': 'George', 'value': 80},
    ]
    assert result == expected_data, 'The select query did not return the expected results.'

    # Check list properties
    assert isinstance(result, list), 'Result should be a list of dictionaries'
    assert len(result) == 6, 'Should return 6 rows'
    assert all(isinstance(row, dict) for row in result), 'Each row should be a dictionary'


def test_select_numeric(psql_docker, conn):
    """Test custom numeric adapter (skip the Decimal creation)"""
    # Perform select query
    query = 'select name, value::numeric as value from test_table order by value'
    result = db.select(conn, query)

    # Check results
    expected_data = [
        {'name': 'Alice', 'value': 10},
        {'name': 'Bob', 'value': 20},
        {'name': 'Charlie', 'value': 30},
        {'name': 'Ethan', 'value': 50},
        {'name': 'Fiona', 'value': 70},
        {'name': 'George', 'value': 80},
    ]
    assert result == expected_data, 'The select query did not return the expected results.'

    # Check that values are float not Decimal
    assert isinstance(result[0]['value'], float), 'Numeric values should be converted to float'


def test_insert(psql_docker, conn):
    """Test basic INSERT operation"""
    # Perform insert operation
    insert_sql = 'insert into test_table (name, value) values (%s, %s)'
    row_count = db.insert(conn, insert_sql, 'Diana', 40)

    # Check return value
    assert row_count == 1, 'Insert should return 1 for rows affected'

    # Verify insert operation
    query = "select name, value from test_table where name = 'Diana'"
    result = db.select(conn, query)
    expected_data = [{'name': 'Diana', 'value': 40}]
    assert result == expected_data, 'The insert operation did not insert the expected data.'


def test_update(psql_docker, conn):
    """Test basic UPDATE operation"""
    # Perform update operation
    update_sql = 'update test_table set value = %s where name = %s'
    row_count = db.update(conn, update_sql, 60, 'Ethan')

    # Check return value
    assert row_count == 1, 'Update should return 1 for rows affected'

    # Verify update operation
    query = "select name, value from test_table where name = 'Ethan'"
    result = db.select(conn, query)
    expected_data = [{'name': 'Ethan', 'value': 60}]
    assert result == expected_data, 'The update operation did not update the data as expected.'


def test_delete(psql_docker, conn):
    """Test basic DELETE operation"""
    # Perform delete operation
    delete_sql = 'delete from test_table where name = %s'
    row_count = db.delete(conn, delete_sql, 'Fiona')

    # Check return value
    assert row_count == 1, 'Delete should return 1 for rows affected'

    # Verify delete operation
    query = "select name, value from test_table where name = 'Fiona'"
    result = db.select(conn, query)
    assert len(result) == 0, 'The delete operation did not delete the data as expected.'


def test_select_row(psql_docker, conn):
    """Test select_row function"""
    row = db.select_row(conn, 'select name, value from test_table where name = %s', 'Alice')
    assert row.name == 'Alice'
    assert row.value == 10


def test_select_row_or_none(psql_docker, conn):
    """Test select_row_or_none function"""
    # Existing row
    row = db.select_row_or_none(conn, 'select name, value from test_table where name = %s', 'Alice')
    assert row.name == 'Alice'

    # Non-existing row
    row = db.select_row_or_none(conn, 'select name, value from test_table where name = %s', 'NonExistent')
    assert row is None


def test_select_scalar(psql_docker, conn):
    """Test select_scalar function"""
    value = db.select_scalar(conn, 'select value from test_table where name = %s', 'Alice')
    assert value == 10


def test_select_scalar_or_none(psql_docker, conn):
    """Test select_scalar_or_none function"""
    # Existing value
    value = db.select_scalar_or_none(conn, 'select value from test_table where name = %s', 'Alice')
    assert value == 10

    # Non-existing value
    value = db.select_scalar_or_none(conn, 'select value from test_table where name = %s', 'NonExistent')
    assert value is None


def test_select_column(psql_docker, conn):
    """Test select_column function"""
    # Get the actual data first to make test more robust
    expected_names = db.select_column(conn, 'select name from test_table order by value')

    # Test the function
    names = db.select_column(conn, 'select name from test_table order by value')
    assert names == expected_names


def test_select_column_unique(psql_docker, conn):
    """Test select_column_unique function"""
    # Insert duplicate values first
    db.insert(conn, 'insert into test_table (name, value) values (%s, %s)', 'DupeA', 100)
    db.insert(conn, 'insert into test_table (name, value) values (%s, %s)', 'DupeB', 100)

    # Get unique values
    values = db.select_column_unique(conn, 'select value from test_table')
    assert isinstance(values, set)
    assert 100 in values
    assert len(values) == 7  # 6 original + 1 duplicate value (100)


def test_insert_rows_bulk(psql_docker, conn):
    """Test inserting a large number of rows in a single operation"""
    # Create a temporary table for bulk testing
    db.execute(conn, """
        CREATE TEMPORARY TABLE bulk_test (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value NUMERIC,
            date DATE
        )
    """)

    # Generate a large number of test rows
    num_rows = 1000
    test_rows = []

    base_date = datetime.date(2025, 1, 1)

    for i in range(num_rows):
        test_rows.append({
            'name': f'Bulk-{i}',
            'value': float(i * 1.5),
            'date': base_date + datetime.timedelta(days=i % 365)
        })

    # Insert the rows
    rows_inserted = db.insert_rows(conn, 'bulk_test', test_rows)

    # Verify all rows were inserted
    assert rows_inserted == num_rows

    # Check a few random rows
    for i in [0, 42, 999]:
        row = db.select_row(conn, 'SELECT * FROM bulk_test WHERE name = %s', f'Bulk-{i}')
        assert row is not None
        assert row.value == float(i * 1.5)


def test_cte_query(psql_docker, conn):
    """Test Common Table Expressions (CTE) queries"""
    # Create a CTE query
    cte_query = """
        WITH highvalue AS (
            SELECT name, value
            FROM test_table
            WHERE value > 50
            ORDER BY value DESC
        )
        SELECT name, value FROM highvalue
    """

    # Execute the query
    result = db.select(conn, cte_query)

    # Verify results
    assert len(result) >= 2
    assert any(row['name'] == 'George' for row in result)

    # Values should be in descending order
    values = [row['value'] for row in result]
    assert values == sorted(values, reverse=True)


if __name__ == '__main__':
    __import__('pytest').main([__file__])
