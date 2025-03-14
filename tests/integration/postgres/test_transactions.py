import datetime

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


def test_nested_transactions_not_supported(psql_docker, conn):
    """Test that nested transactions raise appropriate errors"""
    with db.transaction(conn) as tx1:
        # Start a nested transaction - this should fail
        with pytest.raises(RuntimeError):
            # Call directly with the same connection
            db.transaction(conn)


def test_postgres_hardcoded_literals_transaction(psql_docker, conn):
    """Test transaction with different types of hardcoded literals"""

    # Create a test table
    with db.transaction(conn) as tx:
        tx.execute('DROP TABLE IF EXISTS literal_test')
        tx.execute('CREATE TABLE literal_test (id INT, name VARCHAR(50), value DECIMAL(10,2))')

    # Test different types of literals within a transaction
    with db.transaction(conn) as tx:
        # Insert with literals
        tx.execute("INSERT INTO literal_test (id, name, value) VALUES (1, 'Test', 10.5)")

        # Test various direct queries to see what works

        # Simple SELECT with no WHERE
        result1 = tx.select('SELECT * FROM literal_test')
        assert result1 is not None
        assert len(result1) > 0

        # SELECT with hardcoded WHERE clause
        result2 = tx.select('SELECT * FROM literal_test WHERE id = 1')
        assert result2 is not None
        assert len(result2) > 0
        assert result2[0]['id'] == 1

        # SELECT with parameterized WHERE clause
        result3 = tx.select('SELECT * FROM literal_test WHERE id = %s', 1)
        assert result3 is not None
        assert len(result3) > 0
        assert result3[0]['id'] == 1

        # Aggregate without GROUP BY
        result4 = tx.select('SELECT SUM(value) AS sum_value FROM literal_test')
        assert result4 is not None
        assert len(result4) > 0
        assert result4[0]['sum_value'] == 10.5  # First row has value 10.5

        # Aggregate with direct value
        result5 = tx.select('SELECT 100.5 + SUM(value) AS calculated FROM literal_test')
        assert result5 is not None
        assert len(result5) > 0
        assert result5[0]['calculated'] == 111.0  # 100.5 + 10.5

        # Simple direct value
        result6 = tx.select('SELECT 42 AS answer')
        assert result6 is not None
        assert len(result6) > 0
        assert result6[0]['answer'] == 42

        # Direct string literal
        result7 = tx.select("SELECT 'hello' AS greeting")
        assert result7 is not None
        assert len(result7) > 0
        assert result7[0]['greeting'] == 'hello'

        # Try a more complex query
        tx.execute("INSERT INTO literal_test (id, name, value) VALUES (2, 'Another', 20.5)")
        result8 = tx.select('SELECT id, name, value FROM literal_test ORDER BY id')
        assert result8 is not None
        assert len(result8) == 2
        assert result8[0]['id'] == 1
        assert result8[1]['id'] == 2
        assert result8[1]['name'] == 'Another'
        assert result8[1]['value'] == 20.5

        # Try COUNT with literal comparison
        result9 = tx.select('SELECT COUNT(*) AS row_count FROM literal_test WHERE value > 5')
        assert result9 is not None
        assert len(result9) > 0
        assert result9[0]['row_count'] == 2  # Both rows have value > 5

        # Try different method with aggregate - using a subquery
        result10 = tx.select('SELECT * FROM (SELECT SUM(value) AS total FROM literal_test) t')
        assert result10 is not None
        assert len(result10) > 0
        assert result10[0]['total'] == 31.0  # 10.5 + 20.5

        # Try without alias
        result11 = tx.select('SELECT SUM(value) FROM literal_test')
        assert result11 is not None
        assert len(result11) > 0
        assert result11[0]['sum'] == 31.0  # 10.5 + 20.5

    # Clean up
    with db.transaction(conn) as tx:
        tx.execute('DROP TABLE literal_test')


def test_transaction_with_multiple_statements(psql_docker, conn):
    """Test executing multiple statements with semicolons inside a transaction"""
    # Create a temporary table for the test
    db.execute(conn, """
    CREATE TEMPORARY TABLE employee_data (
        id SERIAL PRIMARY KEY,
        dept TEXT NOT NULL,
        salary INTEGER NOT NULL,
        updated_at TIMESTAMP
    )
    """)

    # Insert initial test data
    db.execute(conn, """
    INSERT INTO employee_data (dept, salary, updated_at) VALUES
    ('sales', 1000, NOW()),
    ('marketing', 2000, NOW()),
    ('engineering', 3000, NOW())
    """)

    # Execute multiple statements within a transaction (no parameters)
    with db.transaction(conn) as tx:
        # This SQL contains multiple statements with both INSERTs and UPDATEs separated by semicolons
        tx.execute("""
        INSERT INTO employee_data (dept, salary, updated_at) VALUES ('finance', 4000, NOW());
        INSERT INTO employee_data (dept, salary, updated_at) VALUES ('hr', 2500, NOW());
        UPDATE employee_data SET salary = salary + 100 WHERE dept = 'sales';
        UPDATE employee_data SET salary = salary + 200 WHERE dept = 'marketing';
        UPDATE employee_data SET salary = salary + 300, updated_at = NOW() WHERE dept = 'engineering'
        """)

    # Verify all statements were executed in the transaction
    result = db.select(conn, """
    SELECT dept, salary FROM employee_data ORDER BY dept
    """)

    assert len(result) == 5
    assert result[0]['dept'] == 'engineering'
    assert result[0]['salary'] == 3300  # 3000 + 300
    assert result[1]['dept'] == 'finance'
    assert result[1]['salary'] == 4000  # New insert
    assert result[2]['dept'] == 'hr'
    assert result[2]['salary'] == 2500  # New insert
    assert result[3]['dept'] == 'marketing'
    assert result[3]['salary'] == 2200  # 2000 + 200
    assert result[4]['dept'] == 'sales'
    assert result[4]['salary'] == 1100  # 1000 + 100


def test_transaction_with_complex_multiple_statements(psql_docker, conn):
    """Test executing complex multiple statements with semicolons inside a transaction"""
    # Create test tables
    db.execute(conn, """
    CREATE TEMPORARY TABLE item (
        id SERIAL PRIMARY KEY,
        category TEXT NOT NULL,
        group_name TEXT,
        duplicate_id INTEGER,
        parent_id INTEGER,
        item_code TEXT
    )
    """)

    # Insert test data
    db.execute(conn, """
    INSERT INTO item (category, group_name, duplicate_id, parent_id, item_code) VALUES
    ('Book', 'Primary', NULL, NULL, '1001'),
    ('Book', 'Secondary', NULL, NULL, '1001'),
    ('Book', 'Primary', NULL, NULL, '1002'),
    ('Magazine', 'Primary', NULL, NULL, '1002'),
    ('Video', 'Tertiary', NULL, NULL, '2001')
    """)

    # Execute complex multi-statement updates inside a transaction
    with db.transaction(conn) as tx:
        tx.execute("""
        INSERT INTO item (category, group_name, duplicate_id, parent_id, item_code)
        VALUES ('eBook', 'Primary', NULL, NULL, '3001');

        INSERT INTO item (category, group_name, duplicate_id, parent_id, item_code)
        VALUES ('Software', 'Secondary', NULL, NULL, '4001');

        UPDATE item i
        SET parent_id = x.ref_id
        FROM (
            SELECT
                i.id AS ref_id,
                i.item_code
            FROM item i
            WHERE
                i.group_name = 'Primary'
                AND i.duplicate_id IS NULL
                AND i.parent_id IS NULL
                AND i.item_code IS NOT NULL
                AND i.category IN ('Book', 'Magazine')
        ) x
        WHERE i.item_code = x.item_code;

        UPDATE item i
        SET parent_id = x.ref_id
        FROM (
            SELECT
                i.id AS ref_id,
                i.category
            FROM item i
            WHERE
                i.group_name = 'Primary'
                AND i.duplicate_id IS NULL
                AND i.parent_id IS NULL
                AND i.category IS NOT NULL
                AND i.category IN ('Book', 'Magazine')
        ) x
        WHERE i.category = x.category AND i.parent_id IS NULL;
        """)

    # Verify the updates were successful
    result = db.select(conn, """
    SELECT id, category, group_name, parent_id, item_code
    FROM item
    ORDER BY id
    """)

    # Group the results by item_code for verification
    items_by_code = {}
    primary_items_by_code = {}

    # First, identify all primary items by their codes
    for row in result:
        code = row['item_code']
        if code not in items_by_code:
            items_by_code[code] = []
        items_by_code[code].append(row)

        # Track all Primary records by their ID for each item_code
        if row['group_name'] == 'Primary':
            if code not in primary_items_by_code:
                primary_items_by_code[code] = {}
            primary_items_by_code[code][row['id']] = row

    # For each code, verify all non-Primary items reference one of the Primary items with same code
    for code, items in items_by_code.items():
        if code in primary_items_by_code:
            primary_ids = list(primary_items_by_code[code].keys())

            for item in items:
                # Skip Primary items during verification, they don't need to reference themselves
                if item['group_name'] == 'Primary':
                    continue

                # Verify this item has a valid parent_id that points to one of the Primary items
                assert item['parent_id'] in primary_ids, \
                    f'Item with item_code {code} should reference a Primary item with ID in {primary_ids}'

    # Verify the newly inserted items
    ebook = db.select_row_or_none(conn, """
        SELECT * FROM item WHERE category = 'eBook'
    """)
    assert ebook is not None
    assert ebook.group_name == 'Primary'
    assert ebook.item_code == '3001'

    software = db.select_row_or_none(conn, """
        SELECT * FROM item WHERE category = 'Software'
    """)
    assert software is not None
    assert software.group_name == 'Secondary'
    assert software.item_code == '4001'

    # Check that we have 7 items now (original 5 + 2 new inserts)
    count = db.select_scalar(conn, 'SELECT COUNT(*) FROM item')
    assert count == 7


def test_transaction_with_multiple_statement_combinations(psql_docker, conn):
    """Comprehensive test for executing varied statement combinations with parameters in transactions"""
    # SECTION 1: BASIC MULTIPLE STATEMENTS WITH PARAMETERS
    # Create a temporary table for the test
    db.execute(conn, """
    CREATE TEMPORARY TABLE param_test (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        created_date DATE,
        modified_date DATE
    )
    """)

    # Get today's date for use as a parameter
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    tomorrow = today + datetime.timedelta(days=1)

    # Execute multiple statements with parameters in a transaction
    with db.transaction(conn) as tx:
        # Attempting to execute multiple statements with parameters
        result = tx.execute("""
        INSERT INTO param_test (name, status, created_date)
        VALUES ('item1', 'active', %s);

        INSERT INTO param_test (name, status, created_date, modified_date)
        VALUES ('item2', 'pending', %s, %s);

        UPDATE param_test
        SET status = 'approved', modified_date = %s
        WHERE name = 'item1';
        """, yesterday, yesterday, today, today)

    # Test with named parameters in a transaction
    db.execute(conn, """
    CREATE TEMPORARY TABLE named_param_test (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        created_date DATE,
        modified_date DATE
    )
    """)

    # Execute multiple statements with named parameters in a transaction
    with db.transaction(conn) as tx:
        # Using all named parameters
        result = tx.execute("""
        INSERT INTO named_param_test (name, status, created_date)
        VALUES ('item1', 'active', %(yesterday)s);

        INSERT INTO named_param_test (name, status, created_date, modified_date)
        VALUES ('item2', 'pending', %(yesterday)s, %(today)s);

        UPDATE named_param_test
        SET status = 'approved', modified_date = %(today)s
        WHERE name = 'item1';
        """, {'yesterday': yesterday, 'today': today})

    # Verify named parameter results
    named_result = db.select(conn, """
    SELECT name, status, created_date, modified_date
    FROM named_param_test
    ORDER BY name
    """)

    assert len(named_result) == 2, 'Should have 2 rows with named parameters'

    named_item1 = next((r for r in named_result if r['name'] == 'item1'), None)
    named_item2 = next((r for r in named_result if r['name'] == 'item2'), None)

    assert named_item1['status'] == 'approved', "item1 should have status 'approved'"
    assert named_item1['created_date'] == yesterday
    assert named_item1['modified_date'] == today

    assert named_item2['status'] == 'pending', "item2 should have status 'pending'"
    assert named_item2['created_date'] == yesterday
    assert named_item2['modified_date'] == today

    # Verify the results
    result = db.select(conn, """
    SELECT name, status, created_date, modified_date
    FROM param_test
    ORDER BY name
    """)

    # Check that the operations were performed
    assert len(result) >= 2, 'Should have at least 2 rows'

    # Find item1 and item2 in results
    item1 = next((r for r in result if r['name'] == 'item1'), None)
    item2 = next((r for r in result if r['name'] == 'item2'), None)

    assert item1 is not None, 'item1 should exist'
    assert item2 is not None, 'item2 should exist'
    assert item1['status'] == 'approved', "item1 should have status 'approved'"
    assert item2['status'] == 'pending', "item2 should have status 'pending'"

    # SECTION 2: MIXED PARAMETER STATEMENTS (NO PARAM + PARAM)
    # Create a temporary table for the test
    db.execute(conn, """
    CREATE TEMPORARY TABLE mix_param_test (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        last_updated TIMESTAMP,
        expire_date DATE
    )
    """)

    # Insert initial test data
    db.execute(conn, """
    INSERT INTO mix_param_test (name, status, last_updated, expire_date) VALUES
    ('item1', 'active', NOW(), NULL),
    ('item2', 'pending', NOW(), NULL),
    ('item3', 'inactive', NOW(), NULL)
    """)

    # Get dates for parameter testing
    today = datetime.date.today()
    future_date = today + datetime.timedelta(days=30)

    # Define a multi-statement SQL with a mix of parameterized and non-parameterized statements
    mixed_param_sql = """
    UPDATE mix_param_test
    SET status = 'updated'
    WHERE status = 'active';

    UPDATE mix_param_test
    SET expire_date = %s
    WHERE status = 'pending';

    UPDATE mix_param_test
    SET expire_date = %s
    WHERE status = 'inactive';
    """

    # Execute the mixed parameter SQL in a transaction
    with db.transaction(conn) as tx:
        tx.execute(mixed_param_sql, today, future_date)

    # Test with named parameters in a transaction
    db.execute(conn, """
    CREATE TEMPORARY TABLE mix_named_param_test (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        last_updated TIMESTAMP,
        expire_date DATE
    )
    """)

    # Insert initial test data
    db.execute(conn, """
    INSERT INTO mix_named_param_test (name, status, last_updated, expire_date) VALUES
    ('item1', 'active', NOW(), NULL),
    ('item2', 'pending', NOW(), NULL),
    ('item3', 'inactive', NOW(), NULL)
    """)

    # Define a multi-statement SQL with named parameters
    named_param_sql = """
    UPDATE mix_named_param_test
    SET status = 'updated'
    WHERE status = 'active';

    UPDATE mix_named_param_test
    SET expire_date = %(today)s
    WHERE status = 'pending';

    UPDATE mix_named_param_test
    SET expire_date = %(future)s
    WHERE status = 'inactive';
    """

    # Execute with named parameters in a transaction
    with db.transaction(conn) as tx:
        tx.execute(named_param_sql, {'today': today, 'future': future_date})

    # Verify the named parameter results
    named_result = db.select(conn, """
    SELECT name, status, expire_date
    FROM mix_named_param_test
    ORDER BY name
    """)

    assert len(named_result) == 3, 'Should have 3 rows with named parameters'

    named_item1 = next((r for r in named_result if r['name'] == 'item1'), None)
    named_item2 = next((r for r in named_result if r['name'] == 'item2'), None)
    named_item3 = next((r for r in named_result if r['name'] == 'item3'), None)

    assert named_item1['status'] == 'updated', "item1 should have status 'updated'"
    assert named_item2['expire_date'] == today, "item2 should have today's date"
    assert named_item3['expire_date'] == future_date, 'item3 should have future date'

    # Verify the results
    result = db.select(conn, """
    SELECT name, status, expire_date FROM mix_param_test ORDER BY name
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

    # SECTION 3: COMPREHENSIVE STATEMENT MATRIX TESTING IN TRANSACTIONS
    # Create test table
    db.execute(conn, """
    CREATE TEMPORARY TABLE tx_matrix_test (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        category TEXT,
        value INTEGER,
        created_at TIMESTAMP DEFAULT NOW()
    )
    """)

    # Test values
    test_value = 100
    test_category = 'param-category'

    # CASE 1: INSERTs with no parameters
    with db.transaction(conn) as tx:
        tx.execute("""
        INSERT INTO tx_matrix_test (name, category, value) VALUES ('tx-no-param-1', 'fixed', 101);
        INSERT INTO tx_matrix_test (name, category, value) VALUES ('tx-no-param-2', 'fixed', 102);
        """)

    # Verify case 1
    no_param_results = db.select(conn, "SELECT * FROM tx_matrix_test WHERE category = 'fixed'")
    assert len(no_param_results) == 2

    # CASE 2: INSERTs with all parameters
    with db.transaction(conn) as tx:
        tx.execute("""
        INSERT INTO tx_matrix_test (name, category, value) VALUES ('tx-all-param-1', %s, %s);
        INSERT INTO tx_matrix_test (name, category, value) VALUES ('tx-all-param-2', %s, %s);
        """, test_category, test_value, test_category, test_value * 2)

    # Verify case 2
    all_param_results = db.select(conn, 'SELECT * FROM tx_matrix_test WHERE category = %s', test_category)
    assert len(all_param_results) == 2
    assert all_param_results[0]['value'] == test_value
    assert all_param_results[1]['value'] == test_value * 2

    # CASE 3: Mixed INSERTs (some with parameters, some without)
    with db.transaction(conn) as tx:
        tx.execute("""
        INSERT INTO tx_matrix_test (name, category, value) VALUES ('tx-mixed-1', 'mixed-fixed', 201);
        INSERT INTO tx_matrix_test (name, category, value) VALUES ('tx-mixed-2', %s, %s);
        INSERT INTO tx_matrix_test (name, category, value) VALUES ('tx-mixed-3', 'mixed-fixed', 203);
        """, 'mixed-params', 202)

    # Verify case 3
    mixed_results = db.select(conn, "SELECT * FROM tx_matrix_test WHERE name LIKE 'tx-mixed-%' ORDER BY name")
    assert len(mixed_results) == 3
    values = {r['name']: r['value'] for r in mixed_results}
    assert values['tx-mixed-1'] == 201
    assert values['tx-mixed-2'] == 202
    assert values['tx-mixed-3'] == 203

    # CASE 4: Complex UPDATE-DELETE-INSERT chain with parameter variations
    with db.transaction(conn) as tx:
        # This includes:
        # 1. A parameter-less INSERT
        # 2. A parameterized INSERT
        # 3. A parameter-less UPDATE
        # 4. A parameterized UPDATE
        # 5. A parameter-less DELETE
        # 6. A parameterized DELETE
        result = tx.execute("""
        INSERT INTO tx_matrix_test (name, category, value) VALUES ('tx-chain-1', 'chain', 301);
        INSERT INTO tx_matrix_test (name, category, value) VALUES ('tx-chain-2', 'chain', %s);
        UPDATE tx_matrix_test SET value = 311 WHERE name = 'tx-chain-1';
        UPDATE tx_matrix_test SET value = %s WHERE name = 'tx-chain-2';
        DELETE FROM tx_matrix_test WHERE name = 'tx-no-param-1';
        DELETE FROM tx_matrix_test WHERE value = %s;
        """, 302, 320, test_value)

    # Verify case 4 results
    chain_results = db.select(conn, "SELECT * FROM tx_matrix_test WHERE category = 'chain'")
    assert len(chain_results) == 2

    # Check updates were applied
    values = {r['name']: r['value'] for r in chain_results}
    assert values['tx-chain-1'] == 311  # Updated with static value
    assert values['tx-chain-2'] == 320  # Updated with parameter

    # Check deletes were applied
    assert db.select_row_or_none(conn, "SELECT * FROM tx_matrix_test WHERE name = 'tx-no-param-1'") is None
    assert db.select_row_or_none(conn, 'SELECT * FROM tx_matrix_test WHERE value = %s', test_value) is None

    # CASE 5: Transaction with complex multi-statement INSERT/UPDATE/DELETE mix with RETURNING
    param_value = 500
    with db.transaction(conn) as tx:
        # Execute with parameters in various positions and get returned values
        id1, id2 = tx.execute("""
        INSERT INTO tx_matrix_test (name, category, value) VALUES ('return-test-1', 'return-cat', %s) RETURNING id;
        INSERT INTO tx_matrix_test (name, category, value) VALUES ('return-test-2', 'return-cat', 502) RETURNING id;
        UPDATE tx_matrix_test SET value = value + 10 WHERE category = 'return-cat';
        DELETE FROM tx_matrix_test WHERE name = 'tx-mixed-1';
        UPDATE tx_matrix_test SET value = %s WHERE id IN (
            SELECT id FROM tx_matrix_test WHERE category = 'return-cat' LIMIT 1
        );
        """, param_value, 600, returnid=['id', 'id'])

    # Verify case 5
    assert id1 is not None
    assert isinstance(id1, int)
    assert id2 is not None
    assert isinstance(id2, int)

    # Verify all statements were executed
    return_results = db.select(conn, """
        SELECT * FROM tx_matrix_test WHERE category = 'return-cat' ORDER BY id
    """)

    # There should be two rows
    assert len(return_results) == 2

    # At least one should have value 600 (from the UPDATE with parameter)
    assert any(r['value'] == 600 for r in return_results)

    # The tx-mixed-1 record should be deleted
    assert db.select_row_or_none(conn, "SELECT * FROM tx_matrix_test WHERE name = 'tx-mixed-1'") is None


if __name__ == '__main__':
    __import__('pytest').main([__file__])
