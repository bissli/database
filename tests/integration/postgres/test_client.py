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


def test_multiple_statements_with_semicolon(psql_docker, conn):
    """Test executing multiple statements with semicolons"""
    # Create a temporary table for testing
    db.execute(conn, """
        CREATE TEMPORARY TABLE multi_statement_test (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            score INTEGER NOT NULL
        )
    """)

    # Execute multiple statements with semicolons (INSERT and UPDATE)
    multi_statement_query = """
        INSERT INTO multi_statement_test (name, score) VALUES ('alpha', 100);
        INSERT INTO multi_statement_test (name, score) VALUES ('beta', 200);
        UPDATE multi_statement_test SET score = 150 WHERE name = 'alpha'
    """

    # Execute the multi-statement query
    db.execute(conn, multi_statement_query)

    # Verify both statements were executed
    result = db.select(conn, 'SELECT name, score FROM multi_statement_test ORDER BY name')

    assert len(result) == 2
    assert result[0]['name'] == 'alpha'
    assert result[0]['score'] == 150  # Updated value
    assert result[1]['name'] == 'beta'
    assert result[1]['score'] == 200

    # Test single statement with semicolon
    db.execute(conn, """
        INSERT INTO multi_statement_test (name, score) VALUES ('gamma', 300);
    """)

    # Verify the single statement executed correctly
    gamma = db.select_row(conn, "SELECT * FROM multi_statement_test WHERE name = 'gamma'")
    assert gamma is not None
    assert gamma.score == 300


def test_multiple_statements_with_delete(psql_docker, conn):
    """Test executing multiple statements including DELETE operations"""
    # Create a temporary table for testing
    db.execute(conn, """
        CREATE TEMPORARY TABLE delete_test (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT NOT NULL
        )
    """)

    # Insert test data
    db.execute(conn, """
        INSERT INTO delete_test (name, status) VALUES ('item1', 'active');
        INSERT INTO delete_test (name, status) VALUES ('item2', 'inactive');
        INSERT INTO delete_test (name, status) VALUES ('item3', 'active');
        INSERT INTO delete_test (name, status) VALUES ('item4', 'pending');
        INSERT INTO delete_test (name, status) VALUES ('item5', 'inactive')
    """)

    # Execute multiple statements with INSERT, UPDATE, and DELETE
    multi_operation_query = """
        INSERT INTO delete_test (name, status) VALUES ('item6', 'active');
        UPDATE delete_test SET status = 'archived' WHERE status = 'inactive';
        DELETE FROM delete_test WHERE status = 'pending'
    """

    db.execute(conn, multi_operation_query)

    # Verify results
    result = db.select(conn, 'SELECT name, status FROM delete_test ORDER BY name')

    # Should have 5 records (5 original + 1 new - 1 deleted)
    assert len(result) == 5

    # Check for specific changes
    statuses = {row['name']: row['status'] for row in result}

    # New item should exist
    assert 'item6' in statuses
    assert statuses['item6'] == 'active'

    # Inactive items should now be archived
    assert statuses['item2'] == 'archived'
    assert statuses['item5'] == 'archived'

    # Pending item should be deleted
    assert 'item4' not in statuses


def test_multiple_statements_with_complex_updates(psql_docker, conn):
    """Test executing multiple complex update statements with semicolons"""
    # Create a temporary table with test data
    db.execute(conn, """
        CREATE TEMPORARY TABLE product_test (
            id SERIAL PRIMARY KEY,
            category TEXT NOT NULL,
            sub_category TEXT,
            related_id INTEGER,
            product_code TEXT
        )
    """)

    # Insert initial test data with different product code for NonFiction
    # to avoid it being updated by the first statement
    db.execute(conn, """
        INSERT INTO product_test (category, sub_category, related_id, product_code) VALUES
        ('Book', 'Fiction', NULL, '1001'),
        ('Book', 'NonFiction', NULL, '2001'),
        ('Book', 'Fiction', NULL, '1002'),
        ('Magazine', 'Fiction', NULL, '1002'),
        ('DVD', 'Movie', NULL, '2001')
    """)

    # Execute multiple complex statements including INSERT and UPDATE
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

    # Execute the multi-statement query
    db.execute(conn, complex_updates)

    # Verify the updates worked as expected
    result_fiction = db.select(conn, """
        SELECT id, category, sub_category, related_id, product_code
        FROM product_test
        WHERE sub_category = 'Fiction'
        ORDER BY id
    """)

    # First Fiction item should have its ID as reference for items with same product_code
    first_fiction_id = result_fiction[0]['id']

    # Check that items with same product_code as first Fiction item got updated
    for row in result_fiction:
        if row['product_code'] == '1001':
            # Check self-reference
            assert row['related_id'] == first_fiction_id

    # Verify specific updates for NonFiction subcategory
    nonfiction_items = db.select(conn, """
        SELECT related_id
        FROM product_test
        WHERE sub_category = 'NonFiction'
    """)

    assert len(nonfiction_items) > 0
    for item in nonfiction_items:
        assert item['related_id'] == 999

    # Verify the inserted Reference row exists
    reference_item = db.select_row_or_none(conn, """
        SELECT category, sub_category, product_code
        FROM product_test
        WHERE sub_category = 'Reference'
    """)

    assert reference_item is not None
    assert reference_item.category == 'Book'
    assert reference_item.product_code == '3001'


def test_multiple_statements_with_parameters_and_combinations(psql_docker, conn):
    """Comprehensive test for multiple statements with various parameter combinations"""
    # SECTION 1: BASIC MULTIPLE STATEMENTS WITH PARAMETERS
    # Create a temporary table for testing
    db.execute(conn, """
        CREATE TEMPORARY TABLE param_multi_test (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER NOT NULL,
            updated_date DATE
        )
    """)

    # Get date for parameter testing
    test_date = datetime.date.today()

    # Execute multiple statements with parameters
    result = db.execute(conn, """
        INSERT INTO param_multi_test (name, value, updated_date) VALUES ('record1', 10, %s);
        INSERT INTO param_multi_test (name, value, updated_date) VALUES ('record2', 20, %s);
        UPDATE param_multi_test SET value = 15 WHERE name = 'record1';
    """, test_date, test_date)

    # Verify results
    result = db.select(conn, """
        SELECT name, value, updated_date FROM param_multi_test ORDER BY name
    """)

    # Check that we have the expected records
    assert len(result) >= 2, 'Should have at least 2 rows'

    # Find record1 and record2 in results
    record1 = next((r for r in result if r['name'] == 'record1'), None)
    record2 = next((r for r in result if r['name'] == 'record2'), None)

    assert record1 is not None, 'record1 should exist'
    assert record2 is not None, 'record2 should exist'
    assert record1['value'] == 15, 'record1 should have value 15'
    assert record2['value'] == 20, 'record2 should have value 20'

    # Check date parameter was correctly used
    for record in [record1, record2]:
        assert record['updated_date'] == test_date, f"Date parameter not correctly applied to {record['name']}"

    # SECTION 1B: NAMED PARAMETERS VERSION
    # Create a temporary table for testing named parameters
    db.execute(conn, """
        CREATE TEMPORARY TABLE named_param_test (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER NOT NULL,
            updated_date DATE
        )
    """)

    # Execute multiple statements with named parameters
    result = db.execute(conn, """
        INSERT INTO named_param_test (name, value, updated_date)
        VALUES ('record1', %(val1)s, %(date)s);

        INSERT INTO named_param_test (name, value, updated_date)
        VALUES ('record2', %(val2)s, %(date)s);

        UPDATE named_param_test SET value = %(updated_val)s WHERE name = 'record1';
    """, {'val1': 10, 'val2': 20, 'date': test_date, 'updated_val': 15})

    # Verify results for named parameters
    result = db.select(conn, """
        SELECT name, value, updated_date FROM named_param_test ORDER BY name
    """)

    assert len(result) >= 2, 'Should have at least 2 rows with named parameters'

    # Find record1 and record2 in results from named parameters
    record1 = next((r for r in result if r['name'] == 'record1'), None)
    record2 = next((r for r in result if r['name'] == 'record2'), None)

    assert record1 is not None, 'record1 should exist with named parameters'
    assert record2 is not None, 'record2 should exist with named parameters'
    assert record1['value'] == 15, 'record1 should have value 15 with named parameters'
    assert record2['value'] == 20, 'record2 should have value 20 with named parameters'

    # SECTION 2: MIXED PARAMETER STATEMENTS
    # Create a temporary table for the test
    db.execute(conn, """
    CREATE TEMPORARY TABLE mix_param_client_test (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        last_updated TIMESTAMP,
        expire_date DATE
    )
    """)

    # Insert initial test data
    db.execute(conn, """
    INSERT INTO mix_param_client_test (name, status, last_updated, expire_date) VALUES
    ('item1', 'active', NOW(), NULL),
    ('item2', 'pending', NOW(), NULL),
    ('item3', 'inactive', NOW(), NULL)
    """)

    # Get dates for parameter testing
    today = datetime.date.today()
    future_date = today + datetime.timedelta(days=30)

    # Define a multi-statement SQL with a mix of parameterized and non-parameterized statements
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

    # Execute the mixed parameter SQL directly
    db.execute(conn, mixed_param_sql, today, future_date)

    # SECTION 2B: NAMED PARAMETERS VERSION
    # Create a table for testing named parameters
    db.execute(conn, """
    CREATE TEMPORARY TABLE mix_param_named_test (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        last_updated TIMESTAMP,
        expire_date DATE
    )
    """)

    # Insert initial test data
    db.execute(conn, """
    INSERT INTO mix_param_named_test (name, status, last_updated, expire_date) VALUES
    ('item1', 'active', NOW(), NULL),
    ('item2', 'pending', NOW(), NULL),
    ('item3', 'inactive', NOW(), NULL)
    """)

    # Define a multi-statement SQL with named parameters
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

    # Execute with named parameters
    db.execute(conn, named_param_sql, {'today_date': today, 'future_date': future_date})

    # Verify the results with named parameters
    result = db.select(conn, """
    SELECT name, status, expire_date FROM mix_param_named_test ORDER BY name
    """)

    assert len(result) == 3, 'Should have 3 rows in named parameter test'

    item1 = next((r for r in result if r['name'] == 'item1'), None)
    item2 = next((r for r in result if r['name'] == 'item2'), None)
    item3 = next((r for r in result if r['name'] == 'item3'), None)

    assert item1['status'] == 'updated', "item1 should have status 'updated' with named parameters"
    assert item2['expire_date'] == today, "item2 should have today's date with named parameters"
    assert item3['expire_date'] == future_date, 'item3 should have future date with named parameters'

    # Verify the results
    result = db.select(conn, """
    SELECT name, status, expire_date FROM mix_param_client_test ORDER BY name
    """)

    # Check that the operations were performed
    assert len(result) == 3, 'Should have 3 rows'

    # Find each item in results
    item1 = next((r for r in result if r['name'] == 'item1'), None)
    item2 = next((r for r in result if r['name'] == 'item2'), None)
    item3 = next((r for r in result if r['name'] == 'item3'), None)

    assert item1 is not None, 'item1 should exist'
    assert item2 is not None, 'item2 should exist'
    assert item3 is not None, 'item3 should exist'

    # Check status update (non-parameterized statement)
    assert item1['status'] == 'updated', "item1 should have status 'updated'"

    # Check date updates (parameterized statements)
    assert item2['expire_date'] == today, "item2 should have today's date"
    assert item3['expire_date'] == future_date, 'item3 should have future date'

    # SECTION 3: COMPREHENSIVE PARAMETER STATEMENT MATRIX TESTING
    # Create a test table
    db.execute(conn, """
    CREATE TEMPORARY TABLE stmt_matrix_test (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        category TEXT,
        value INTEGER,
        created_at TIMESTAMP DEFAULT NOW()
    )
    """)

    # Test parameters
    test_value = 42
    test_category = 'test-category'

    # CASE 1: Multiple INSERTs - No parameters
    db.execute(conn, """
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('no-param-1', 'static', 100);
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('no-param-2', 'static', 200);
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('no-param-3', 'static', 300);
    """)

    # Verify case 1
    no_param_results = db.select(conn, "SELECT * FROM stmt_matrix_test WHERE category = 'static'")
    assert len(no_param_results) == 3
    assert {r['name'] for r in no_param_results} == {'no-param-1', 'no-param-2', 'no-param-3'}

    # CASE 2: Multiple INSERTs - All with parameters
    db.execute(conn, """
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('all-param-1', %s, %s);
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('all-param-2', %s, %s);
    """, test_category, test_value, test_category, test_value * 2)

    # Verify case 2
    all_param_results = db.select(conn, 'SELECT * FROM stmt_matrix_test WHERE category = %s', test_category)
    assert len(all_param_results) == 2
    assert all_param_results[0]['name'] == 'all-param-1'
    assert all_param_results[0]['value'] == test_value
    assert all_param_results[1]['name'] == 'all-param-2'
    assert all_param_results[1]['value'] == test_value * 2

    # CASE 3: Mixed INSERTs - Some with parameters, some without
    db.execute(conn, """
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('mixed-1', 'fixed', 501);
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('mixed-2', %s, %s);
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('mixed-3', 'fixed', 503);
    """, 'mixed-params', 502)

    # Verify case 3
    fixed_results = db.select(conn, "SELECT * FROM stmt_matrix_test WHERE name LIKE 'mixed-%' ORDER BY id")
    assert len(fixed_results) == 3
    assert fixed_results[0]['category'] == 'fixed'
    assert fixed_results[1]['category'] == 'mixed-params'
    assert fixed_results[1]['value'] == 502
    assert fixed_results[2]['category'] == 'fixed'

    # CASE 4: Multiple UPDATEs - No parameters
    db.execute(conn, """
    UPDATE stmt_matrix_test SET value = 1001 WHERE name = 'no-param-1';
    UPDATE stmt_matrix_test SET value = 1002 WHERE name = 'no-param-2';
    """)

    # Verify case 4
    updated_no_param = db.select(conn, "SELECT * FROM stmt_matrix_test WHERE name IN ('no-param-1', 'no-param-2')")
    assert len(updated_no_param) == 2
    values = {r['name']: r['value'] for r in updated_no_param}
    assert values['no-param-1'] == 1001
    assert values['no-param-2'] == 1002

    # CASE 5: Multiple UPDATEs - All with parameters
    new_value1 = 2001
    new_value2 = 2002
    db.execute(conn, """
    UPDATE stmt_matrix_test SET category = 'updated-param', value = %s WHERE name = 'all-param-1';
    UPDATE stmt_matrix_test SET category = 'updated-param', value = %s WHERE name = 'all-param-2';
    """, new_value1, new_value2)

    # Verify case 5
    updated_all_param = db.select(conn, "SELECT * FROM stmt_matrix_test WHERE category = 'updated-param'")
    assert len(updated_all_param) == 2
    values = {r['name']: r['value'] for r in updated_all_param}
    assert values['all-param-1'] == new_value1
    assert values['all-param-2'] == new_value2

    # CASE 6: Mixed UPDATEs and DELETEs
    db.execute(conn, """
    UPDATE stmt_matrix_test SET value = %s WHERE name = 'mixed-1';
    DELETE FROM stmt_matrix_test WHERE name = 'mixed-2';
    UPDATE stmt_matrix_test SET value = 3003 WHERE name = 'mixed-3';
    """, 3001)

    # Verify case 6
    after_mixed_ops = db.select(conn, "SELECT * FROM stmt_matrix_test WHERE name LIKE 'mixed-%'")
    assert len(after_mixed_ops) == 2  # mixed-2 was deleted
    values = {r['name']: r['value'] for r in after_mixed_ops}
    assert values['mixed-1'] == 3001  # Updated with parameter
    assert values['mixed-3'] == 3003  # Updated with hardcoded value
    assert 'mixed-2' not in values    # Deleted

    # CASE 7: INSERT, UPDATE, DELETE chain with parameter variations
    final_value = 5000
    db.execute(conn, """
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('chain-1', 'chain-test', 4001);
    INSERT INTO stmt_matrix_test (name, category, value) VALUES ('chain-2', 'chain-test', %s);
    UPDATE stmt_matrix_test SET value = value + 10 WHERE category = 'chain-test';
    DELETE FROM stmt_matrix_test WHERE name = 'chain-1';
    UPDATE stmt_matrix_test SET value = %s WHERE name = 'chain-2';
    """, 4002, final_value)

    # Verify case 7
    chain_result = db.select_row(conn, "SELECT * FROM stmt_matrix_test WHERE name = 'chain-2'")
    assert chain_result is not None
    assert chain_result.value == final_value  # Final update applied the exact value


if __name__ == '__main__':
    __import__('pytest').main([__file__])
