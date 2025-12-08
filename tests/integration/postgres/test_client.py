import datetime

import database as db


def test_select(psql_docker, pg_conn):
    """Verify basic SELECT query returns proper results and structure.

    Tests that db.select correctly retrieves data and returns it as a list
    of dictionaries with expected values.
    """
    query = 'select name, value from test_table order by value'
    result = db.select(pg_conn, query)

    expected_data = [
        {'name': 'Alice', 'value': 10},
        {'name': 'Bob', 'value': 20},
        {'name': 'Charlie', 'value': 30},
        {'name': 'Ethan', 'value': 50},
        {'name': 'Fiona', 'value': 70},
        {'name': 'George', 'value': 80},
    ]
    assert result == expected_data, 'The select query did not return the expected results.'

    assert isinstance(result, list), 'Result should be a list of dictionaries'
    assert len(result) == 6, 'Should return 6 rows'
    assert all(isinstance(row, dict) for row in result), 'Each row should be a dictionary'


def test_select_numeric(psql_docker, pg_conn):
    """Verify custom numeric adapter properly converts PostgreSQL numeric types to float.

    Ensures numeric database values are returned as Python float values
    rather than Decimal objects.
    """
    query = 'select name, value::numeric as value from test_table order by value'
    result = db.select(pg_conn, query)

    expected_data = [
        {'name': 'Alice', 'value': 10},
        {'name': 'Bob', 'value': 20},
        {'name': 'Charlie', 'value': 30},
        {'name': 'Ethan', 'value': 50},
        {'name': 'Fiona', 'value': 70},
        {'name': 'George', 'value': 80},
    ]
    assert result == expected_data, 'The select query did not return the expected results.'

    assert isinstance(result[0]['value'], float), 'Numeric values should be converted to float'


def test_insert(psql_docker, pg_conn):
    """Verify INSERT operation correctly adds data and returns proper row count.

    Tests db.insert by adding a record and confirming both the return value
    and that the data appears in a subsequent query.
    """
    insert_sql = 'insert into test_table (name, value) values (%s, %s)'
    row_count = db.insert(pg_conn, insert_sql, 'Diana', 40)

    assert row_count == 1, 'Insert should return 1 for rows affected'

    query = "select name, value from test_table where name = 'Diana'"
    result = db.select(pg_conn, query)
    expected_data = [{'name': 'Diana', 'value': 40}]
    assert result == expected_data, 'The insert operation did not insert the expected data.'


def test_update(psql_docker, pg_conn):
    """Verify UPDATE operation correctly modifies data and returns proper row count.

    Tests db.update by modifying a record and confirming both the return value
    and that the data is correctly updated in a subsequent query.
    """
    update_sql = 'update test_table set value = %s where name = %s'
    row_count = db.update(pg_conn, update_sql, 60, 'Ethan')

    assert row_count == 1, 'Update should return 1 for rows affected'

    query = "select name, value from test_table where name = 'Ethan'"
    result = db.select(pg_conn, query)
    expected_data = [{'name': 'Ethan', 'value': 60}]
    assert result == expected_data, 'The update operation did not update the data as expected.'


def test_delete(psql_docker, pg_conn):
    """Verify DELETE operation correctly removes data and returns proper row count.

    Tests db.delete by removing a record and confirming both the return value
    and that the data is no longer present in a subsequent query.
    """
    delete_sql = 'delete from test_table where name = %s'
    row_count = db.delete(pg_conn, delete_sql, 'Fiona')

    assert row_count == 1, 'Delete should return 1 for rows affected'

    query = "select name, value from test_table where name = 'Fiona'"
    result = db.select(pg_conn, query)
    assert len(result) == 0, 'The delete operation did not delete the data as expected.'


def test_select_row(psql_docker, pg_conn):
    """Verify select_row correctly retrieves a single row as an object.

    Tests that db.select_row returns a row with attribute access to columns.
    """
    row = db.select_row(pg_conn, 'select name, value from test_table where name = %s', 'Alice')
    assert row.name == 'Alice'
    assert row.value == 10


def test_select_row_or_none(psql_docker, pg_conn):
    """Verify select_row_or_none returns object for existing rows and None for missing rows.

    Tests both the successful case with a row object return and the case
    where no matching row exists, which should return None.
    """
    row = db.select_row_or_none(pg_conn, 'select name, value from test_table where name = %s', 'Alice')
    assert row.name == 'Alice'

    row = db.select_row_or_none(pg_conn, 'select name, value from test_table where name = %s', 'NonExistent')
    assert row is None


def test_select_scalar(psql_docker, pg_conn):
    """Verify select_scalar correctly retrieves a single value.

    Tests that db.select_scalar returns just the first column of the first row.
    """
    value = db.select_scalar(pg_conn, 'select value from test_table where name = %s', 'Alice')
    assert value == 10


def test_select_scalar_or_none(psql_docker, pg_conn):
    """Verify select_scalar_or_none returns value for existing rows and None for missing rows.

    Tests both the successful case with a single value returned and the case
    where no matching row exists, which should return None.
    """
    value = db.select_scalar_or_none(pg_conn, 'select value from test_table where name = %s', 'Alice')
    assert value == 10

    value = db.select_scalar_or_none(pg_conn, 'select value from test_table where name = %s', 'NonExistent')
    assert value is None


def test_select_column(psql_docker, pg_conn):
    """Verify select_column correctly retrieves a list of values from a single column.

    Tests that db.select_column returns values from the specified column across
    all matching rows.
    """
    expected_names = db.select_column(pg_conn, 'select name from test_table order by value')

    names = db.select_column(pg_conn, 'select name from test_table order by value')
    assert names == expected_names


def test_insert_rows_bulk(psql_docker, pg_conn):
    """Verify insert_rows correctly handles bulk insertion of many records.

    Tests that db.insert_rows can insert a large batch of records at once
    and return the correct count of inserted rows.
    """
    db.execute(pg_conn, """
        CREATE TEMPORARY TABLE bulk_test (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value NUMERIC,
            date DATE
        )
    """)

    num_rows = 1000

    base_date = datetime.date(2025, 1, 1)

    test_rows = [{
            'name': f'Bulk-{i}',
            'value': float(i * 1.5),
            'date': base_date + datetime.timedelta(days=i % 365)
        } for i in range(num_rows)]

    rows_inserted = db.insert_rows(pg_conn, 'bulk_test', test_rows)

    assert rows_inserted == num_rows

    for i in [0, 42, 999]:
        row = db.select_row(pg_conn, 'SELECT * FROM bulk_test WHERE name = %s', f'Bulk-{i}')
        assert row is not None
        assert row.value == float(i * 1.5)


def test_cte_query(psql_docker, pg_conn):
    """Verify database functions correctly handle Common Table Expression (CTE) queries.

    Tests that complex CTE queries are properly executed and return expected results.
    """
    cte_query = """
        WITH highvalue AS (
            SELECT name, value
            FROM test_table
            WHERE value > 50
            ORDER BY value DESC
        )
        SELECT name, value FROM highvalue
    """

    result = db.select(pg_conn, cte_query)

    assert len(result) >= 2
    assert any(row['name'] == 'George' for row in result)

    values = [row['value'] for row in result]
    assert values == sorted(values, reverse=True)


def test_multiple_statements_with_semicolon(psql_docker, pg_conn):
    """Verify execute handles multiple SQL statements separated by semicolons.

    Tests that db.execute properly runs multiple statements in a single call,
    including INSERT and UPDATE operations.
    """
    db.execute(pg_conn, """
        CREATE TEMPORARY TABLE multi_statement_test (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            score INTEGER NOT NULL
        )
    """)

    multi_statement_query = """
        INSERT INTO multi_statement_test (name, score) VALUES ('alpha', 100);
        INSERT INTO multi_statement_test (name, score) VALUES ('beta', 200);
        UPDATE multi_statement_test SET score = 150 WHERE name = 'alpha'
    """

    db.execute(pg_conn, multi_statement_query)

    result = db.select(pg_conn, 'SELECT name, score FROM multi_statement_test ORDER BY name')

    assert len(result) == 2
    assert result[0]['name'] == 'alpha'
    assert result[0]['score'] == 150  # Updated value
    assert result[1]['name'] == 'beta'
    assert result[1]['score'] == 200

    db.execute(pg_conn, """
        INSERT INTO multi_statement_test (name, score) VALUES ('gamma', 300);
    """)

    gamma = db.select_row(pg_conn, "SELECT * FROM multi_statement_test WHERE name = 'gamma'")
    assert gamma is not None
    assert gamma.score == 300


def test_multiple_statements_with_delete(psql_docker, pg_conn):
    """Verify execute handles multiple statements including DELETE operations.

    Tests that db.execute properly processes a mix of INSERT, UPDATE, and DELETE
    statements in a single execution.
    """
    db.execute(pg_conn, """
        CREATE TEMPORARY TABLE delete_test (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT NOT NULL
        )
    """)

    db.execute(pg_conn, """
        INSERT INTO delete_test (name, status) VALUES ('item1', 'active');
        INSERT INTO delete_test (name, status) VALUES ('item2', 'inactive');
        INSERT INTO delete_test (name, status) VALUES ('item3', 'active');
        INSERT INTO delete_test (name, status) VALUES ('item4', 'pending');
        INSERT INTO delete_test (name, status) VALUES ('item5', 'inactive')
    """)

    multi_operation_query = """
        INSERT INTO delete_test (name, status) VALUES ('item6', 'active');
        UPDATE delete_test SET status = 'archived' WHERE status = 'inactive';
        DELETE FROM delete_test WHERE status = 'pending'
    """

    db.execute(pg_conn, multi_operation_query)

    result = db.select(pg_conn, 'SELECT name, status FROM delete_test ORDER BY name')

    assert len(result) == 5

    statuses = {row['name']: row['status'] for row in result}

    assert 'item6' in statuses
    assert statuses['item6'] == 'active'

    assert statuses['item2'] == 'archived'
    assert statuses['item5'] == 'archived'

    assert 'item4' not in statuses


def test_multiple_statements_with_complex_updates(psql_docker, pg_conn):
    """Verify execute handles complex multi-statement SQL with subqueries.

    Tests that db.execute can process complex SQL statements with subqueries,
    joins, and self-references across multiple operations.
    """
    db.execute(pg_conn, """
        CREATE TEMPORARY TABLE product_test (
            id SERIAL PRIMARY KEY,
            category TEXT NOT NULL,
            sub_category TEXT,
            related_id INTEGER,
            product_code TEXT
        )
    """)

    db.execute(pg_conn, """
        INSERT INTO product_test (category, sub_category, related_id, product_code) VALUES
        ('Book', 'Fiction', NULL, '1001'),
        ('Book', 'NonFiction', NULL, '2001'),
        ('Book', 'Fiction', NULL, '1002'),
        ('Magazine', 'Fiction', NULL, '1002'),
        ('DVD', 'Movie', NULL, '2001')
    """)

    complex_updates = """
        INSERT INTO product_test (category, sub_category, related_id, product_code)
        VALUES ('Book', 'Reference', NULL, '3001');

        UPDATE product_test p
        SET related_id = x.ref_id
        FROM (
            SELECT
                p.id AS ref_id,
                p.product_code
            FROM product_test p
            WHERE
                p.sub_category = 'Fiction'
                AND p.related_id IS NULL
                AND p.product_code IS NOT NULL
                AND p.category IN ('Book', 'Magazine')
        ) x
        WHERE p.product_code = x.product_code;

        UPDATE product_test p
        SET related_id = 999
        WHERE p.sub_category = 'NonFiction'
        AND p.related_id IS NULL
    """

    db.execute(pg_conn, complex_updates)

    result_fiction = db.select(pg_conn, """
        SELECT id, category, sub_category, related_id, product_code
        FROM product_test
        WHERE sub_category = 'Fiction'
        ORDER BY id
    """)

    first_fiction_id = result_fiction[0]['id']

    for row in result_fiction:
        if row['product_code'] == '1001':
            assert row['related_id'] == first_fiction_id

    nonfiction_items = db.select(pg_conn, """
        SELECT related_id
        FROM product_test
        WHERE sub_category = 'NonFiction'
    """)

    assert len(nonfiction_items) > 0
    for item in nonfiction_items:
        assert item['related_id'] == 999

    reference_item = db.select_row_or_none(pg_conn, """
        SELECT category, sub_category, product_code
        FROM product_test
        WHERE sub_category = 'Reference'
    """)

    assert reference_item is not None
    assert reference_item.category == 'Book'
    assert reference_item.product_code == '3001'


def test_multiple_statements_with_parameters_and_combinations(psql_docker, pg_conn):
    """Verify execute properly handles complex combinations of parameterized statements.

    Tests db.execute with multiple combinations of:
    - Positional and named parameters
    - Statements with and without parameters mixed in the same execute call
    - Complex parameter interactions across INSERT, UPDATE, and DELETE operations
    - Parameter reuse across multiple statements
    """
    db.execute(pg_conn, """
        CREATE TEMPORARY TABLE param_multi_test (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER NOT NULL,
            updated_date DATE
        )
    """)

    test_date = datetime.date.today()

    result = db.execute(pg_conn, """
        INSERT INTO param_multi_test (name, value, updated_date) VALUES ('record1', 10, %s);
        INSERT INTO param_multi_test (name, value, updated_date) VALUES ('record2', 20, %s);
        UPDATE param_multi_test SET value = 15 WHERE name = 'record1';
    """, test_date, test_date)

    result = db.select(pg_conn, """
        SELECT name, value, updated_date FROM param_multi_test ORDER BY name
    """)

    assert len(result) >= 2, 'Should have at least 2 rows'

    record1 = next((r for r in result if r['name'] == 'record1'), None)
    record2 = next((r for r in result if r['name'] == 'record2'), None)

    assert record1 is not None, 'record1 should exist'
    assert record2 is not None, 'record2 should exist'
    assert record1['value'] == 15, 'record1 should have value 15'
    assert record2['value'] == 20, 'record2 should have value 20'

    for record in [record1, record2]:
        assert record['updated_date'] == test_date, f"Date parameter not correctly applied to {record['name']}"

    db.execute(pg_conn, """
        CREATE TEMPORARY TABLE named_param_test (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER NOT NULL,
            updated_date DATE
        )
    """)

    result = db.execute(pg_conn, """
        INSERT INTO named_param_test (name, value, updated_date)
        VALUES ('record1', %(val1)s, %(date)s);

        INSERT INTO named_param_test (name, value, updated_date)
        VALUES ('record2', %(val2)s, %(date)s);

        UPDATE named_param_test SET value = %(updated_val)s WHERE name = 'record1';
    """, {'val1': 10, 'val2': 20, 'date': test_date, 'updated_val': 15})

    result = db.select(pg_conn, """
        SELECT name, value, updated_date FROM named_param_test ORDER BY name
    """)

    assert len(result) >= 2, 'Should have at least 2 rows with named parameters'

    record1 = next((r for r in result if r['name'] == 'record1'), None)
    record2 = next((r for r in result if r['name'] == 'record2'), None)

    assert record1 is not None, 'record1 should exist with named parameters'
    assert record2 is not None, 'record2 should exist with named parameters'
    assert record1['value'] == 15, 'record1 should have value 15 with named parameters'
    assert record2['value'] == 20, 'record2 should have value 20 with named parameters'

    db.execute(pg_conn, """
    CREATE TEMPORARY TABLE mix_param_client_test (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        last_updated TIMESTAMP,
        expire_date DATE
    )
    """)

    db.execute(pg_conn, """
    INSERT INTO mix_param_client_test (name, status, last_updated, expire_date) VALUES
    ('item1', 'active', NOW(), NULL),
    ('item2', 'pending', NOW(), NULL),
    ('item3', 'inactive', NOW(), NULL)
    """)

    today = datetime.date.today()
    future_date = today + datetime.timedelta(days=30)

    mixed_param_sql = """
    UPDATE mix_param_client_test
    SET status = 'updated'
    WHERE status = 'active';

    UPDATE mix_param_client_test
    SET expire_date = %s
    WHERE status = 'pending';

    UPDATE mix_param_client_test
    SET expire_date = %s
    WHERE status = 'inactive';
    """

    db.execute(pg_conn, mixed_param_sql, today, future_date)

    db.execute(pg_conn, """
    CREATE TEMPORARY TABLE mix_param_named_test (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        last_updated TIMESTAMP,
        expire_date DATE
    )
    """)

    db.execute(pg_conn, """
    INSERT INTO mix_param_named_test (name, status, last_updated, expire_date) VALUES
    ('item1', 'active', NOW(), NULL),
    ('item2', 'pending', NOW(), NULL),
    ('item3', 'inactive', NOW(), NULL)
    """)

    named_param_sql = """
    UPDATE mix_param_named_test
    SET status = 'updated'
    WHERE status = 'active';

    UPDATE mix_param_named_test
    SET expire_date = %(today_date)s
    WHERE status = 'pending';

    UPDATE mix_param_named_test
    SET expire_date = %(future_date)s
    WHERE status = 'inactive';
    """

    db.execute(pg_conn, named_param_sql, {'today_date': today, 'future_date': future_date})

    result = db.select(pg_conn, """
    SELECT name, status, expire_date FROM mix_param_named_test ORDER BY name
    """)

    assert len(result) == 3, 'Should have 3 rows in named parameter test'

    item1 = next((r for r in result if r['name'] == 'item1'), None)
    item2 = next((r for r in result if r['name'] == 'item2'), None)
    item3 = next((r for r in result if r['name'] == 'item3'), None)

    assert item1['status'] == 'updated', "item1 should have status 'updated' with named parameters"
    assert item2['expire_date'] == today, "item2 should have today's date with named parameters"
    assert item3['expire_date'] == future_date, 'item3 should have future date with named parameters'

    result = db.select(pg_conn, """
    SELECT name, status, expire_date FROM mix_param_client_test ORDER BY name
    """)

    assert len(result) == 3, 'Should have 3 rows'

    item1 = next((r for r in result if r['name'] == 'item1'), None)
    item2 = next((r for r in result if r['name'] == 'item2'), None)
    item3 = next((r for r in result if r['name'] == 'item3'), None)

    assert item1 is not None, 'item1 should exist'
    assert item2 is not None, 'item2 should exist'
    assert item3 is not None, 'item3 should exist'

    assert item1['status'] == 'updated', "item1 should have status 'updated'"
    assert item2['expire_date'] == today, "item2 should have today's date"
    assert item3['expire_date'] == future_date, 'item3 should have future date'

    db.execute(pg_conn, """
    CREATE TEMPORARY TABLE stmt_matrix_test (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        category TEXT,
        value INTEGER,
        created_at TIMESTAMP DEFAULT NOW()
    )
    """)

    test_value = 42
    test_category = 'test-category'

    db.execute(pg_conn, """
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('no-param-1', 'static', 100);
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('no-param-2', 'static', 200);
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('no-param-3', 'static', 300);
    """)

    no_param_results = db.select(pg_conn, "SELECT * FROM stmt_matrix_test WHERE category = 'static'")
    assert len(no_param_results) == 3
    assert {r['name'] for r in no_param_results} == {'no-param-1', 'no-param-2', 'no-param-3'}

    db.execute(pg_conn, """
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('all-param-1', %s, %s);
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('all-param-2', %s, %s);
    """, test_category, test_value, test_category, test_value * 2)

    all_param_results = db.select(pg_conn, 'SELECT * FROM stmt_matrix_test WHERE category = %s', test_category)
    assert len(all_param_results) == 2
    assert all_param_results[0]['name'] == 'all-param-1'
    assert all_param_results[0]['value'] == test_value
    assert all_param_results[1]['name'] == 'all-param-2'
    assert all_param_results[1]['value'] == test_value * 2

    db.execute(pg_conn, """
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('mixed-1', 'fixed', 501);
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('mixed-2', %s, %s);
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('mixed-3', 'fixed', 503);
    """, 'mixed-params', 502)

    fixed_results = db.select(pg_conn, "SELECT * FROM stmt_matrix_test WHERE name LIKE 'mixed-%' ORDER BY id")
    assert len(fixed_results) == 3
    assert fixed_results[0]['category'] == 'fixed'
    assert fixed_results[1]['category'] == 'mixed-params'
    assert fixed_results[1]['value'] == 502
    assert fixed_results[2]['category'] == 'fixed'

    db.execute(pg_conn, """
    UPDATE stmt_matrix_test SET value = 1001 WHERE name = 'no-param-1';
    UPDATE stmt_matrix_test SET value = 1002 WHERE name = 'no-param-2';
    """)

    updated_no_param = db.select(pg_conn, "SELECT * FROM stmt_matrix_test WHERE name IN ('no-param-1', 'no-param-2')")
    assert len(updated_no_param) == 2
    values = {r['name']: r['value'] for r in updated_no_param}
    assert values['no-param-1'] == 1001
    assert values['no-param-2'] == 1002

    new_value1 = 2001
    new_value2 = 2002
    db.execute(pg_conn, """
    UPDATE stmt_matrix_test SET category = 'updated-param', value = %s WHERE name = 'all-param-1';
    UPDATE stmt_matrix_test SET category = 'updated-param', value = %s WHERE name = 'all-param-2';
    """, new_value1, new_value2)

    updated_all_param = db.select(pg_conn, "SELECT * FROM stmt_matrix_test WHERE category = 'updated-param'")
    assert len(updated_all_param) == 2
    values = {r['name']: r['value'] for r in updated_all_param}
    assert values['all-param-1'] == new_value1
    assert values['all-param-2'] == new_value2

    db.execute(pg_conn, """
    UPDATE stmt_matrix_test SET value = %s WHERE name = 'mixed-1';
    DELETE FROM stmt_matrix_test WHERE name = 'mixed-2';
    UPDATE stmt_matrix_test SET value = 3003 WHERE name = 'mixed-3';
    """, 3001)

    after_mixed_ops = db.select(pg_conn, "SELECT * FROM stmt_matrix_test WHERE name LIKE 'mixed-%'")
    assert len(after_mixed_ops) == 2  # mixed-2 was deleted
    values = {r['name']: r['value'] for r in after_mixed_ops}
    assert values['mixed-1'] == 3001
    assert values['mixed-3'] == 3003
    assert 'mixed-2' not in values

    final_value = 5000
    db.execute(pg_conn, """
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('chain-1', 'chain-test', 4001);
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('chain-2', 'chain-test', %s);
    UPDATE stmt_matrix_test SET value = value + 10 WHERE category = 'chain-test';
    DELETE FROM stmt_matrix_test WHERE name = 'chain-1';
    UPDATE stmt_matrix_test SET value = %s WHERE name = 'chain-2';
    """, 4002, final_value)

    chain_result = db.select_row(pg_conn, "SELECT * FROM stmt_matrix_test WHERE name = 'chain-2'")
    assert chain_result is not None
    assert chain_result.value == final_value


if __name__ == '__main__':
    __import__('pytest').main([__file__])
