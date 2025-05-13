import datetime

import database as db
import pytest


def test_upsert_basic(psql_docker, conn):
    """Test basic upsert functionality"""
    # Insert new rows
    rows = [{'name': 'Barry', 'value': 50}, {'name': 'Wallace', 'value': 92}]
    row_count = db.upsert_rows(conn, 'test_table', rows, update_cols_always=['value'])
    assert row_count == 2, 'upsert should return 2 for new rows'

    # Verify rows were inserted
    result = db.select(conn, 'select name, value from test_table where name in (%s, %s) order by name',
                       'Barry', 'Wallace')
    assert len(result) == 2
    assert result[0]['name'] == 'Barry'
    assert result[0]['value'] == 50
    assert result[1]['name'] == 'Wallace'
    assert result[1]['value'] == 92

    # Update existing rows
    rows = [{'name': 'Barry', 'value': 51}, {'name': 'Wallace', 'value': 93}]
    row_count = db.upsert_rows(conn, 'test_table', rows, update_cols_always=['value'])

    # Verify rows were updated
    result = db.select(conn, 'select name, value from test_table where name in (%s, %s) order by name',
                       'Barry', 'Wallace')
    assert len(result) == 2
    assert result[0]['value'] == 51
    assert result[1]['value'] == 93


def test_upsert_ifnull(psql_docker, conn):
    """Test upsert with update_cols_ifnull option"""
    # First insert a row
    db.insert(conn, 'insert into test_table (name, value) values (%s, %s)', 'UpsertNull', 100)

    # Try to update with update_cols_ifnull - should not update existing value
    rows = [{'name': 'UpsertNull', 'value': 200}]
    db.upsert_rows(conn, 'test_table', rows, update_cols_ifnull=['value'])

    # Verify value was not updated
    result = db.select_scalar(conn, 'select value from test_table where name = %s', 'UpsertNull')
    assert result == 100, 'Value should not be updated when using update_cols_ifnull'

    # Create a new test table that allows nulls for this specific test
    db.execute(conn, """
    CREATE TEMPORARY TABLE test_nullable (
        name VARCHAR(50) PRIMARY KEY,
        value INTEGER NULL
    )
    """)

    # Insert initial data
    db.insert(conn, 'INSERT INTO test_nullable (name, value) VALUES (%s, %s)', 'UpsertNull', 100)

    # Now set the value to NULL - this works because the new table allows nulls
    db.execute(conn, 'UPDATE test_nullable SET value = NULL WHERE name = %s', 'UpsertNull')

    # Try to update with update_cols_ifnull again - now it should update
    rows = [{'name': 'UpsertNull', 'value': 200}]
    db.upsert_rows(conn, 'test_nullable', rows, update_cols_ifnull=['value'])

    # Verify value was updated
    result = db.select_scalar(conn, 'SELECT value FROM test_nullable WHERE name = %s', 'UpsertNull')
    assert result == 200, 'Value should be updated when target is NULL'


def test_upsert_mixed_operations(psql_docker, conn):
    """Test upsert with mix of inserts and updates"""
    # Make sure we have a clean connection state
    try:
        conn.rollback()
    except:
        pass

    # Create a mix of new and existing rows
    rows = [
        {'name': 'Alice', 'value': 1000},  # Existing - update
        {'name': 'NewPerson1', 'value': 500},  # New - insert
        {'name': 'NewPerson2', 'value': 600}   # New - insert
    ]

    row_count = db.upsert_rows(conn, 'test_table', rows, update_cols_always=['value'])
    assert row_count == 3

    # Verify results
    result = db.select(conn, """
        select name, value from test_table
        where name in (%s, %s, %s)
        order by name
    """, 'Alice', 'NewPerson1', 'NewPerson2')

    assert len(result) == 3
    assert result[0]['name'] == 'Alice'
    assert result[0]['value'] == 1000
    assert result[1]['name'] == 'NewPerson1'
    assert result[1]['value'] == 500
    assert result[2]['name'] == 'NewPerson2'
    assert result[2]['value'] == 600


def test_upsert_reset_sequence(psql_docker, conn):
    """Test sequence reset during upsert"""
    # Make sure we have a clean connection state
    try:
        conn.rollback()
    except:
        pass

    # Create a new table specifically for this test to avoid modifying the shared test_table
    db.execute(conn, """
CREATE TEMPORARY TABLE test_sequence_table (
    test_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    value INTEGER NOT NULL
)
""")

    # Copy data from the test_table to our new table
    db.execute(conn, """
INSERT INTO test_sequence_table (name, value)
SELECT name, value FROM test_table
""")

    # Get the current max sequence value
    current_max_id = db.select_scalar(conn, 'SELECT max(test_id) FROM test_sequence_table')

    # Insert a new row with reset_sequence=False (default)
    new_rows = [{'name': 'Zack', 'value': 150}]
    db.upsert_rows(conn, 'test_sequence_table', new_rows, update_cols_key=['name'])

    # Now get the next sequence value by inserting a row directly
    db.execute(conn, "INSERT INTO test_sequence_table (name, value) VALUES ('SequenceTest1', 200)")
    seq_test1_id = db.select_scalar(conn, "SELECT test_id FROM test_sequence_table WHERE name = 'SequenceTest1'")

    # Insert another row but with reset_sequence=True
    new_rows = [{'name': 'Yvonne', 'value': 175}]
    db.upsert_rows(conn, 'test_sequence_table', new_rows, update_cols_key=['name'], reset_sequence=True)

    # Now the sequence should be set to the max id + 1
    # Insert another row to check
    db.execute(conn, "INSERT INTO test_sequence_table (name, value) VALUES ('SequenceTest2', 225)")
    seq_test2_id = db.select_scalar(conn, "SELECT test_id FROM test_sequence_table WHERE name = 'SequenceTest2'")

    # The new ID should be exactly max_id + 1
    max_id_after_reset = db.select_scalar(conn, "SELECT max(test_id) FROM test_sequence_table WHERE name != 'SequenceTest2'")
    assert seq_test2_id == max_id_after_reset + 1, f'Sequence was not properly reset. Expected {max_id_after_reset+1}, got {seq_test2_id}'

    # Temporary tables are automatically cleaned up


def test_upsert_empty_rows(psql_docker, conn):
    """Test upsert with empty rows list"""
    # Should not error and return 0
    result = db.upsert_rows(conn, 'test_table', [])
    assert result == 0


def test_upsert_large_batch(psql_docker, conn):
    """Test upsert with a large number of rows that exceeds parameter limits"""
    import time

    # Create a temporary table for this test with a simple structure
    db.execute(conn, """
    CREATE TEMPORARY TABLE test_large_batch (
        id INTEGER PRIMARY KEY,
        value TEXT NOT NULL
    )
    """)

    # Create a batch of rows that will exceed PostgreSQL's parameter limit
    # PostgreSQL max parameters is 66000
    # With 2 parameters per row (id and value), we need 35000 rows to exceed it
    start_time = time.time()
    rows = [{'id': i, 'value': f'value-{i}'} for i in range(1, 35001)]

    # Insert the rows
    batch_insert_time = time.time()
    row_count = db.upsert_rows(conn, 'test_large_batch', rows)
    insert_end_time = time.time()

    # Verify correct number of rows inserted
    assert row_count == 35000, f'Expected 35000 rows to be inserted, got {row_count}'

    # Verify data was inserted correctly (check a few values)
    sample_ids = [1, 1000, 10000, 19999]
    for id_val in sample_ids:
        result = db.select_scalar(conn, 'SELECT value FROM test_large_batch WHERE id = %s', id_val)
        assert result == f'value-{id_val}', f'Wrong value for id {id_val}: {result}'

    # Now modify the rows and upsert again to test updates with large batch
    update_rows = [
        {'id': i, 'value': f'updated-{i}'}
        for i in range(1, 35001)
    ]

    # Update the rows (this will use UPSERT with batching)
    update_start_time = time.time()
    update_count = db.upsert_rows(conn, 'test_large_batch', update_rows,
                                  update_cols_key=['id'],
                                  update_cols_always=['value'])
    update_end_time = time.time()

    # Verify correct number of rows updated
    assert update_count == 35000, f'Expected 35000 rows to be updated, got {update_count}'

    # Verify data was updated correctly (check same sample)
    for id_val in sample_ids:
        result = db.select_scalar(conn, 'SELECT value FROM test_large_batch WHERE id = %s', id_val)
        assert result == f'updated-{id_val}', f'Wrong value after update for id {id_val}: {result}'

    # Verify total row count in the table
    total_rows = db.select_scalar(conn, 'SELECT COUNT(*) FROM test_large_batch')
    assert total_rows == 35000, f'Expected 35000 total rows, found {total_rows}'

    # Optional: Print performance metrics
    insert_time = insert_end_time - batch_insert_time
    update_time = update_end_time - update_start_time

    print('\nLarge batch performance:')
    print(f'Insert time for 35000 rows: {insert_time:.2f}s')
    print(f'Update time for 35000 rows: {update_time:.2f}s')

    # Assert reasonable performance (adjust thresholds as appropriate for your system)
    # Note: These are very generous thresholds for debugging - tune for your environment
    assert insert_time < 60, f'Insert too slow: {insert_time:.2f}s'
    assert update_time < 60, f'Update too slow: {update_time:.2f}s'


def test_upsert_invalid_columns(psql_docker, conn):
    """Test upsert with invalid columns"""
    # Make sure we have a clean connection state
    try:
        conn.rollback()
    except:
        pass

    rows = [{'name': 'InvalidTest', 'value': 100, 'nonexistent': 'should be filtered'}]
    db.upsert_rows(conn, 'test_table', rows)

    # Verify the row was inserted without error
    result = db.select_row(conn, 'select name, value from test_table where name = %s', 'InvalidTest')
    assert result.name == 'InvalidTest'
    assert result.value == 100


def test_upsert_no_primary_keys(psql_docker, conn):
    """Test upsert with a table that has no primary keys"""
    # Create a table without primary keys
    db.execute(conn, """
    CREATE TEMPORARY TABLE test_no_pk (
        name VARCHAR(50) NOT NULL,
        value INTEGER NOT NULL
    )
    """)

    # Insert data using upsert
    rows = [
        {'name': 'NoPK1', 'value': 100},
        {'name': 'NoPK2', 'value': 200}
    ]

    # Without primary keys, all rows should be inserted
    row_count = db.upsert_rows(conn, 'test_no_pk', rows)
    assert row_count == 2, 'upsert should insert 2 rows'

    # Verify rows were inserted
    result = db.select(conn, 'select name, value from test_no_pk order by name')
    assert len(result) == 2
    assert result[0]['name'] == 'NoPK1'
    assert result[0]['value'] == 100
    assert result[1]['name'] == 'NoPK2'
    assert result[1]['value'] == 200

    # Try "updating" with upsert again - without primary keys, this should insert new rows, not update
    rows = [
        {'name': 'NoPK1', 'value': 101},
        {'name': 'NoPK2', 'value': 201}
    ]

    row_count = db.upsert_rows(conn, 'test_no_pk', rows, update_cols_always=['value'])
    assert row_count == 2, 'upsert should insert 2 new rows'

    # We should now have 4 rows
    result = db.select(conn, 'select name, value from test_no_pk order by name, value')
    assert len(result) == 4, 'table should have 4 rows total'

    # Verify both old and new rows exist
    assert result[0]['name'] == 'NoPK1'
    assert result[0]['value'] == 100
    assert result[1]['name'] == 'NoPK1'
    assert result[1]['value'] == 101
    assert result[2]['name'] == 'NoPK2'
    assert result[2]['value'] == 200
    assert result[3]['name'] == 'NoPK2'
    assert result[3]['value'] == 201


def test_upsert_invalid_column_filtering(psql_docker, conn):
    """Test that invalid columns are properly filtered in upsert_rows"""

    # Make sure we have a clean connection state
    try:
        conn.rollback()
    except:
        pass

    # Create test data with both valid and invalid columns
    rows = [
        {
            'name': 'ValidationTest1',  # Valid column
            'value': 100,               # Valid column
            'nonexistent_column': 'This should be filtered out'  # Invalid column
        },
        {
            'name': 'ValidationTest2',  # Valid column
            'value': 200,               # Valid column
            'another_bad_column': 42    # Invalid column
        }
    ]

    # Insert with mixed valid/invalid columns
    row_count = db.upsert_rows(conn, 'test_table', rows)

    # Verify the correct number of rows were inserted
    assert row_count == 2, 'Expected 2 rows to be inserted'

    # Verify the rows were inserted correctly with only valid data
    result = db.select(conn, 'SELECT name, value FROM test_table WHERE name LIKE %s ORDER BY name',
                       'ValidationTest%')

    assert len(result) == 2, 'Expected 2 rows to be returned'
    assert result[0]['name'] == 'ValidationTest1'
    assert result[0]['value'] == 100
    assert result[1]['name'] == 'ValidationTest2'
    assert result[1]['value'] == 200

    # Verify the invalid columns don't exist in the table
    with pytest.raises(Exception):
        db.select(conn, 'SELECT nonexistent_column FROM test_table')

    with pytest.raises(Exception):
        db.select(conn, 'SELECT another_bad_column FROM test_table')

    # Test case-insensitive column filtering - mixed case with invalid columns
    rows_mixed_case = [
        {
            'NAME': 'ValidationTest3',        # Valid column (different case)
            'Value': 300,                     # Valid column (different case)
            'INVALID_COLUMN': 'Should filter'  # Invalid column
        },
        {
            'NaMe': 'ValidationTest4',        # Valid column (mixed case)
            'VaLuE': 400,                     # Valid column (mixed case)
            'bad_col': False                  # Invalid column
        }
    ]

    # Insert with mixed case and invalid columns
    row_count = db.upsert_rows(conn, 'test_table', rows_mixed_case)

    # Verify the correct number of rows were inserted
    assert row_count == 2, 'Expected 2 rows to be inserted with case-insensitive column filtering'

    # Verify the rows were inserted correctly with only valid data
    result = db.select(conn, 'SELECT name, value FROM test_table WHERE name IN (%s, %s) ORDER BY name',
                       'ValidationTest3', 'ValidationTest4')

    assert len(result) == 2, 'Expected 2 rows to be returned with case-insensitive column filtering'
    assert result[0]['name'] == 'ValidationTest3'
    assert result[0]['value'] == 300
    assert result[1]['name'] == 'ValidationTest4'
    assert result[1]['value'] == 400


def test_upsert_column_order_independence(psql_docker, conn):
    """Test that upsert works correctly regardless of column order in dictionaries"""

    # Clean up any existing test data
    db.execute(conn, "DELETE FROM test_table WHERE name LIKE 'OrderTest%'")

    # Create test data with different column orders
    rows = [
        {
            'name': 'OrderTest1',   # name first
            'value': 300
        },
        {
            'value': 400,           # value first
            'name': 'OrderTest2'
        }
    ]

    # Insert the rows with different column orders
    row_count = db.upsert_rows(conn, 'test_table', rows)
    assert row_count == 2, 'Expected 2 rows to be inserted'

    # Verify all data was inserted correctly
    result = db.select(conn, 'SELECT name, value FROM test_table WHERE name LIKE %s ORDER BY name',
                       'OrderTest%')

    assert len(result) == 2, 'Expected 2 rows to be returned'
    assert result[0]['name'] == 'OrderTest1'
    assert result[0]['value'] == 300
    assert result[1]['name'] == 'OrderTest2'
    assert result[1]['value'] == 400


def test_upsert_all_invalid_columns(psql_docker, conn):
    """Test that upsert properly handles the case where all columns are invalid"""

    # Create test data with only invalid columns
    rows = [
        {
            'nonexistent1': 'Invalid data',
            'nonexistent2': 123
        }
    ]

    # This should return 0 rows affected and log a warning
    row_count = db.upsert_rows(conn, 'test_table', rows)
    assert row_count == 0, 'Expected 0 rows to be inserted when all columns are invalid'


def test_upsert_case_insensitive_columns(psql_docker, conn):
    """Test that upsert works with case-insensitive column matching"""

    # Clean up any existing test data
    db.execute(conn, "DELETE FROM test_table WHERE name LIKE 'CaseTest%'")

    # Assuming the table has columns 'name' and 'value' with that exact case
    # Insert with different column cases
    rows = [
        {
            'NAME': 'CaseTest1',    # Uppercase column name
            'Value': 101            # Mixed case column name
        },
        {
            'name': 'CaseTest2',    # Exact case column name
            'VALUE': 102            # Uppercase column name
        }
    ]

    # Insert should work despite case differences
    row_count = db.upsert_rows(conn, 'test_table', rows)
    assert row_count == 2, 'Expected 2 rows to be inserted with case-insensitive columns'

    # Verify rows were inserted with correct values
    result = db.select(conn, 'SELECT name, value FROM test_table WHERE name LIKE %s ORDER BY name',
                       'CaseTest%')

    assert len(result) == 2, 'Expected 2 rows with case-insensitive column matching'
    assert result[0]['name'] == 'CaseTest1'
    assert result[0]['value'] == 101
    assert result[1]['name'] == 'CaseTest2'
    assert result[1]['value'] == 102

    # Now test updates with different column case
    update_rows = [
        {
            'NAme': 'CaseTest1',    # Different mixed case
            'vaLUE': 201            # Different mixed case
        }
    ]

    # Update should work despite case differences
    # Note: keys must match the column names in the database (case-sensitive for conflict clause)
    db.upsert_rows(conn, 'test_table', update_rows,
                   update_cols_key=['name'],  # Using exact case as in database
                   update_cols_always=['value'])  # Using exact case as in database

    # Verify update worked
    result = db.select_row(conn, 'SELECT value FROM test_table WHERE name = %s', 'CaseTest1')
    assert result['value'] == 201, 'Expected value to be updated with case-insensitive column matching'

    # Test with extreme case variations
    extreme_case_rows = [
        {
            'NaME': 'CaseTest3',    # Random capitalization
            'vALue': 303            # Random capitalization
        }
    ]

    # Should handle even unusual case variations
    row_count = db.upsert_rows(conn, 'test_table', extreme_case_rows)
    assert row_count == 1, 'Expected 1 row to be inserted with extreme case variations'

    # Verify row inserted correctly
    result = db.select_row(conn, 'SELECT value FROM test_table WHERE name = %s', 'CaseTest3')
    assert result['value'] == 303, 'Expected correct value with extreme case variation'

    # Test case-insensitive query parameter columns
    db.upsert_rows(conn, 'test_table',
                   [{'name': 'CaseTest4', 'value': 404}],
                   update_cols_key=['name'],   # Must use exact case for keys
                   update_cols_always=['value'])  # Must use exact case for update columns

    # Insert another row with the same primary key but different case to test update
    db.upsert_rows(conn, 'test_table',
                   [{'name': 'CaseTest4', 'value': 444}],
                   update_cols_key=['name'],
                   update_cols_always=['value'])

    # Verify the update worked with case-insensitive key & update columns
    result = db.select_row(conn, 'SELECT value FROM test_table WHERE name = %s', 'CaseTest4')
    assert result['value'] == 444, 'Expected value to be updated with case-insensitive parameter columns'


def test_upsert_comprehensive_case_sensitivity(psql_docker, conn):
    """Comprehensive test for case sensitivity in column operations"""

    # Create a test table with mixed case column names if possible
    # PostgreSQL folds unquoted identifiers to lowercase, so this is just
    # to make our test logic clearer - column names are still lowercase in PostgreSQL
    db.execute(conn, """
    CREATE TEMPORARY TABLE case_test_table (
        "Id" SERIAL PRIMARY KEY,        -- Mixed case with quotes
        "UserName" VARCHAR(50) NOT NULL, -- Mixed case with quotes
        "email" VARCHAR(100) NOT NULL,   -- lowercase
        "PHONE" VARCHAR(20) NULL,        -- uppercase
        "lastLogin" TIMESTAMP NULL       -- camelCase
    )
    """)

    # Case variations for each column
    test_cases = [
        # Test case 1: Exact case match
        {
            'row_data': {
                'Id': 1,
                'UserName': 'user1',
                'email': 'user1@example.com',
                'PHONE': '555-1234',
                'lastLogin': '2023-01-01'
            },
            'expected_name': 'user1'
        },
        # Test case 2: All uppercase
        {
            'row_data': {
                'ID': 2,
                'USERNAME': 'user2',
                'EMAIL': 'user2@example.com',
                'PHONE': '555-5678',
                'LASTLOGIN': '2023-01-02'
            },
            'expected_name': 'user2'
        },
        # Test case 3: All lowercase
        {
            'row_data': {
                'id': 3,
                'username': 'user3',
                'email': 'user3@example.com',
                'phone': '555-9012',
                'lastlogin': '2023-01-03'
            },
            'expected_name': 'user3'
        },
        # Test case 4: Mixed case variations
        {
            'row_data': {
                'iD': 4,
                'UsErNaMe': 'user4',
                'EMail': 'user4@example.com',
                'pHoNe': '555-3456',
                'LaStLoGiN': '2023-01-04'
            },
            'expected_name': 'user4'
        },
        # Test case 5: Invalid columns mixed with valid columns in different cases
        {
            'row_data': {
                'ID': 5,
                'UserNAME': 'user5',
                'email': 'user5@example.com',
                'INVALID_COL': 'should be filtered',
                'another_bad': 12345,
                'phoneNumber': '555-7890'  # Slightly different name, should be filtered
            },
            'expected_name': 'user5'
        }
    ]

    # Insert all test cases
    for test_case in test_cases:
        row_count = db.upsert_rows(conn, 'case_test_table', [test_case['row_data']])
        assert row_count == 1, f"Expected 1 row to be inserted for test case with {test_case['expected_name']}"

    # Verify all rows were inserted correctly
    result = db.select(conn, 'SELECT "Id", "UserName", "email", "PHONE" FROM case_test_table ORDER BY "Id"')
    assert len(result) == 5, 'Expected 5 rows to be inserted'

    # Check specific values
    for i, test_case in enumerate(test_cases):
        row = result[i]
        assert row['UserName'] == test_case['expected_name'], f'Wrong username for test case {i+1}'

    # Test updates with case-insensitive key columns
    update_row = {
        'ID': 1,  # Different case from the actual column
        'username': 'user1-updated',
        'EMAIL': 'updated1@example.com'
    }

    # Get column names with exact case from the database
    column_info = db.select(conn, """
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'case_test_table'
    """)
    db_columns = [row['column_name'] for row in column_info]

    # Important: Use exact case from the database for the key and update columns
    db.upsert_rows(conn, 'case_test_table', [update_row],
                   update_cols_key=['Id'],  # Must match the exact case in the database
                   update_cols_always=['UserName', 'email'])

    # Verify update worked
    result = db.select_row(conn, 'SELECT "UserName", "email" FROM case_test_table WHERE "Id" = 1')
    assert result['UserName'] == 'user1-updated', 'Update failed with case-insensitive key'
    assert result['email'] == 'updated1@example.com', 'Update failed with case-insensitive column'

    # Test batch updates with mixed case variations
    batch_updates = [
        {
            'iD': 2,
            'username': 'user2-updated',
            'EMAIL': 'updated2@example.com'
        },
        {
            'Id': 3,
            'USERNAME': 'user3-updated',
            'email': 'updated3@example.com'
        }
    ]

    db.upsert_rows(conn, 'case_test_table', batch_updates,
                   update_cols_key=['Id'],   # Exact case to match database
                   update_cols_always=['UserName', 'email'])  # Exact case to match database

    # Verify batch updates
    result = db.select(conn, 'SELECT "Id", "UserName", "email" FROM case_test_table WHERE "Id" IN (2, 3) ORDER BY "Id"')
    assert len(result) == 2, 'Expected 2 rows to be updated'
    assert result[0]['UserName'] == 'user2-updated', 'Batch update 1 failed'
    assert result[1]['UserName'] == 'user3-updated', 'Batch update 2 failed'

    # Final verification: all rows should still be present with correct updates
    count = db.select_scalar(conn, 'SELECT COUNT(*) FROM case_test_table')
    assert count == 5, 'Expected all 5 rows to be present after updates'


def test_upsert_error_handling(psql_docker, conn):
    """Test that upsert properly handles different types of updates and conflicts"""
    # Create a test table with a unique constraint
    db.execute(conn, """
    CREATE TEMPORARY TABLE test_error_handling (
        id INTEGER PRIMARY KEY,
        name VARCHAR(50) UNIQUE NOT NULL,
        value INTEGER NOT NULL
    )
    """)

    # Insert initial data
    db.execute(conn, 'INSERT INTO test_error_handling VALUES (1, %s, %s)', 'Item1', 100)
    db.execute(conn, 'INSERT INTO test_error_handling VALUES (2, %s, %s)', 'Item2', 200)

    # Test updating via PRIMARY KEY constraint
    pk_update = [{'id': 1, 'name': 'UpdatedItem1', 'value': 150}]

    row_count = db.upsert_rows(
        conn,
        'test_error_handling',
        pk_update,
        update_cols_key=['id'],  # Using id as conflict key
        update_cols_always=['name', 'value']
    )

    # Verify the row was updated via PRIMARY KEY
    result = db.select_row(conn, 'SELECT name, value FROM test_error_handling WHERE id = 1')
    assert result.name == 'UpdatedItem1', 'Row should be updated by ID'
    assert result.value == 150, 'Value should be updated'

    # Test updating via UNIQUE constraint
    unique_update = [{'id': 3, 'name': 'Item2', 'value': 250}]

    row_count = db.upsert_rows(
        conn,
        'test_error_handling',
        unique_update,
        update_cols_key=['name'],  # Using name as conflict key
        update_cols_always=['id', 'value']
    )

    # Verify the row was updated via UNIQUE constraint
    result = db.select_row(conn, 'SELECT id, value FROM test_error_handling WHERE name = %s', 'Item2')
    assert result.id == 3, 'ID should be updated when using name as conflict key'
    assert result.value == 250, 'Value should be updated'

    # Test inserting a completely new row
    new_row = [{'id': 4, 'name': 'Item4', 'value': 400}]

    row_count = db.upsert_rows(
        conn,
        'test_error_handling',
        new_row,
        update_cols_key=['id'],
        update_cols_always=['name', 'value']
    )

    # Verify the new row was inserted
    result = db.select_row(conn, 'SELECT name, value FROM test_error_handling WHERE id = 4')
    assert result.name == 'Item4', 'New row should be inserted'
    assert result.value == 400, 'New row should have correct value'

    # Test that constraint violations are properly caught
    # Since we're in a test, we'll verify that the exception is raised
    conflict_row = [{'id': 5, 'name': 'Item4', 'value': 500}]  # Name conflicts with existing

    with pytest.raises(Exception) as excinfo:
        db.upsert_rows(
            conn,
            'test_error_handling',
            conflict_row,
            update_cols_key=['id'],  # Only handling ID conflicts
            update_cols_always=['name', 'value']
        )

    # Verify the specific type of error raised
    assert 'unique constraint' in str(excinfo.value).lower() or 'duplicate key' in str(excinfo.value).lower(), \
        'Should raise unique constraint violation error'

    # Test a more complex scenario: multiple rows where some would succeed
    # We'll process them individually to avoid batch errors
    rows_with_mixed_conflicts = [
        {'id': 6, 'name': 'Item6', 'value': 600},  # New row - should succeed
        {'id': 7, 'name': 'Item4', 'value': 700},  # Name conflicts - would fail
        {'id': 8, 'name': 'Item8', 'value': 800}   # New row - should succeed
    ]

    # Process each row individually to simulate what would happen with row-by-row processing
    for row in rows_with_mixed_conflicts:
        try:
            db.upsert_rows(
                conn,
                'test_error_handling',
                [row],  # Single row at a time
                update_cols_key=['id'],
                update_cols_always=['name', 'value']
            )
        except Exception as e:
            # Expected exception for the conflicting row
            assert 'Item4' in str(e) or 'unique constraint' in str(e).lower() or 'duplicate key' in str(e).lower(), \
                f'Unexpected error: {e}'

    # Verify the non-conflicting rows were inserted
    result = db.select(conn, 'SELECT id, name, value FROM test_error_handling WHERE id IN (6, 8) ORDER BY id')
    assert len(result) == 2, 'Two new rows should be inserted'
    assert result[0]['name'] == 'Item6', 'Row with id=6 should be inserted'
    assert result[1]['name'] == 'Item8', 'Row with id=8 should be inserted'

    # Finally, verify that no row with id=7 exists (it should have failed)
    count = db.select_scalar(conn, 'SELECT COUNT(*) FROM test_error_handling WHERE id = 7')
    assert count == 0, 'Conflicting row with id=7 should not be inserted'


def test_upsert_with_constraint_name(psql_docker, conn):
    """Test upsert using a constraint name instead of key columns"""
    # Create a test table with a named constraint
    db.execute(conn, """
    CREATE TEMPORARY TABLE test_constraint (
        id INTEGER,
        name VARCHAR(50),
        value INTEGER NOT NULL,
        CONSTRAINT test_unique_constraint UNIQUE (id, name)
    )
    """)

    # Insert initial data
    db.execute(conn, 'INSERT INTO test_constraint VALUES (1, %s, %s)', 'ConstraintTest', 100)

    # Test upsert with constraint name
    rows = [{'id': 1, 'name': 'ConstraintTest', 'value': 200}]

    row_count = db.upsert_rows(
        conn,
        'test_constraint',
        rows,
        update_cols_key='test_unique_constraint',
        update_cols_always=['value']
    )

    # Verify the row was updated
    result = db.select_scalar(conn, 'SELECT value FROM test_constraint WHERE id = 1')
    assert result == 200, 'Row should be updated using constraint-based conflict detection'

    # Test inserting a new row
    rows = [{'id': 2, 'name': 'ConstraintTest2', 'value': 300}]

    row_count = db.upsert_rows(
        conn,
        'test_constraint',
        rows,
        update_cols_key='test_unique_constraint',
        update_cols_always=['value']
    )

    # Verify new row was inserted
    count = db.select_scalar(conn, 'SELECT COUNT(*) FROM test_constraint')
    assert count == 2, 'New row should be inserted'


def test_upsert_with_standard_constraint(psql_docker, conn):
    """Test upsert behavior with regular constraint

    This test verifies that the upsert functionality correctly handles:
    1. Regular ALTER TABLE ADD CONSTRAINT for standard unique constraints
    2. Proper detection of constraint columns
    3. Correct update behavior when constraint values match
    4. With standard constraints (id, name), changing only value results in an update
    """
    # Create a test table for testing with a standard unique constraint
    db.execute(conn, 'DROP TABLE IF EXISTS test_complex_constraint;')

    db.execute(conn, """
        CREATE TABLE test_complex_constraint (
            id INTEGER NOT NULL,
            name VARCHAR(100),
            value INTEGER,
            last_updated TIMESTAMP DEFAULT NOW()
        )
    """)

    # Add a standard unique constraint without expressions
    # This creates a constraint that only includes id and name, NOT value
    # A row with same id and name but different value SHOULD update the existing row
    db.execute(conn, """
        ALTER TABLE test_complex_constraint
        ADD CONSTRAINT complex_unique_constraint
        UNIQUE (id, name)
    """)

    # Test with the CONSTRAINT
    _test_complex_upsert_scenarios(
        conn,
        'test_complex_constraint',
        'complex_unique_constraint'
    )


def test_upsert_with_complex_index(psql_docker, conn):
    """Test upsert behavior with complex unique index

    This test verifies that the upsert functionality correctly handles:
    1. Complex UNIQUE INDEX with expressions
    2. Proper detection of index columns and expressions
    3. Correct update behavior when ALL index values match
    4. Update of non-constrained columns when the constraint matches exactly
    """
    # Create a test table with a complex unique index
    db.execute(conn, 'DROP TABLE IF EXISTS test_complex_index')

    db.execute(conn, """
        CREATE TABLE test_complex_index (
            id INTEGER NOT NULL,
            name VARCHAR(100),
            value INTEGER,
            last_updated TIMESTAMP DEFAULT NOW()
        )
    """)

    # Create the unique index with expressions that include id, name and value
    db.execute(conn, """
        CREATE UNIQUE INDEX complex_unique_index
        ON test_complex_index (id, COALESCE(name, ''), COALESCE(value, -1))
    """)

    # Insert initial test row
    initial_row = {
        'id': 1,
        'name': 'ComplexTest',
        'value': 100,
        'last_updated': datetime.datetime(2023, 3, 17, 17, 47, 15, 906191)
    }

    # Insert the row
    db.insert(conn, """
        INSERT INTO test_complex_index (id, name, value, last_updated)
        VALUES (%s, %s, %s, %s)
    """, initial_row['id'], initial_row['name'], initial_row['value'], initial_row['last_updated'])

    # Verify initial state
    count = db.select_scalar(conn, 'SELECT COUNT(*) FROM test_complex_index WHERE id = 1')
    assert count == 1, 'Should have 1 row initially'

    # First test: Try to update with all constraint columns matching
    # This should update the existing row's last_updated field
    same_constraint_row = {
        'id': 1,                    # Same as original (part of constraint)
        'name': 'ComplexTest',      # Same as original (part of constraint)
        'value': 100,               # Same as original (part of constraint)
        'last_updated': datetime.datetime(2023, 3, 18, 10, 0, 0)  # Different (not in constraint)
    }

    db.upsert_rows(
        conn,
        'test_complex_index',
        [same_constraint_row],
        update_cols_key='complex_unique_index',
        update_cols_always=['last_updated']  # Only update the timestamp
    )

    # Verify we still have just 1 row (it was updated, not inserted)
    count = db.select_scalar(conn, 'SELECT COUNT(*) FROM test_complex_index WHERE id = 1')
    assert count == 1, 'Should still have 1 row after updating non-constraint column'

    # Verify last_updated was changed
    updated_time = db.select_scalar(conn, 'SELECT last_updated FROM test_complex_index WHERE id = 1')
    assert updated_time.date() == datetime.date(2023, 3, 18), 'The last_updated field should be updated'

    # Second test: Now change one of the constraint columns (value)
    # This should insert a new row since the constraint won't match
    different_value_row = {
        'id': 1,                    # Same as original (part of constraint)
        'name': 'ComplexTest',      # Same as original (part of constraint)
        'value': 200,               # Different from original (part of constraint)
        'last_updated': datetime.datetime(2023, 3, 19, 10, 0, 0)  # Different (not in constraint)
    }

    db.upsert_rows(
        conn,
        'test_complex_index',
        [different_value_row],
        update_cols_key='complex_unique_index',
        update_cols_always=['last_updated']  # Trying to update timestamp, but should insert instead
    )

    # Verify we now have 2 rows (new one inserted because constraint didn't match)
    count = db.select_scalar(conn, 'SELECT COUNT(*) FROM test_complex_index WHERE id = 1')
    assert count == 2, 'Should have 2 rows after trying to update with different constraint value'

    # Verify both rows exist with different values
    rows = db.select(conn, 'SELECT id, name, value, last_updated FROM test_complex_index WHERE id = 1 ORDER BY value')

    assert len(rows) == 2, 'Should have 2 distinct rows'
    assert rows[0]['value'] == 100, 'First row should have original value=100'
    assert rows[0]['last_updated'].date() == datetime.date(2023, 3, 18), 'First row should have timestamp from first update'
    assert rows[1]['value'] == 200, 'Second row should have new value=200'
    assert rows[1]['last_updated'].date() == datetime.date(2023, 3, 19), 'Second row should have new timestamp'

    # Third test: Update just the second row by matching its exact constraint values
    update_second_row = {
        'id': 1,                    # Match both rows
        'name': 'ComplexTest',      # Match both rows
        'value': 200,               # Match only second row (part of constraint)
        'last_updated': datetime.datetime(2023, 3, 20, 10, 0, 0)  # New timestamp
    }

    db.upsert_rows(
        conn,
        'test_complex_index',
        [update_second_row],
        update_cols_key='complex_unique_index',
        update_cols_always=['last_updated']  # Only update timestamp
    )

    # Verify we still have 2 rows (one updated, not inserted)
    count = db.select_scalar(conn, 'SELECT COUNT(*) FROM test_complex_index WHERE id = 1')
    assert count == 2, 'Should still have 2 rows after selective update'

    # Verify only the second row's timestamp was updated
    rows = db.select(conn, 'SELECT id, name, value, last_updated FROM test_complex_index WHERE id = 1 ORDER BY value')

    assert len(rows) == 2, 'Should still have 2 distinct rows'
    assert rows[0]['value'] == 100, 'First row should have original value'
    assert rows[0]['last_updated'].date() == datetime.date(2023, 3, 18), 'First row should have unchanged timestamp'
    assert rows[1]['value'] == 200, 'Second row should have same value'
    assert rows[1]['last_updated'].date() == datetime.date(2023, 3, 20), 'Second row should have updated timestamp'


def _test_complex_upsert_scenarios(conn, table_name, constraint_name):
    """Helper function to run the same test scenarios on different tables"""
    # Insert initial test row
    initial_row = {
        'id': 1,
        'name': 'ComplexTest',
        'value': 100,
        'last_updated': datetime.datetime(2023, 3, 17, 17, 47, 15, 906191)
    }
    # Use insert rather than insert_row
    db.insert(conn, f'INSERT INTO {table_name} (id, name, value, last_updated) VALUES (%s, %s, %s, %s)',
              initial_row['id'], initial_row['name'], initial_row['value'], initial_row['last_updated'])

    # Verify initial state
    count = db.select_scalar(conn, f'SELECT COUNT(*) FROM {table_name} WHERE id = 1')
    assert count == 1, 'Should have 1 row initially'

    # Scenario 1: Update with matching constraint values
    update_same_constraint = {
        'id': 1,
        'name': 'ComplexTest',  # Same name
        'value': 100,  # Same value
        'last_updated': datetime.datetime(2023, 3, 17, 21, 47, 15, 908775)  # Different timestamp
    }

    result = db.upsert_rows(
        conn,
        table_name,
        [update_same_constraint],
        update_cols_key=constraint_name, 
        update_cols_always=['value', 'last_updated']  # Add 'value' to columns to update
    )

    # Should still be only one row as it updated
    count = db.select_scalar(conn, f'SELECT COUNT(*) FROM {table_name} WHERE id = 1')
    assert count == 1, 'Should still have 1 row after update with matching constraint'

    # Check the timestamp was updated
    updated_time = db.select_scalar(conn, f'SELECT last_updated FROM {table_name} WHERE id = 1')
    assert updated_time > initial_row['last_updated'], 'The last_updated field should be updated'

    # Scenario 2: Insert with different constraint value
    different_value_row = {
        'id': 1,
        'name': 'ComplexTest',  # Same name
        'value': 200,  # Different value - part of the constraint!
        'last_updated': datetime.datetime(2023, 3, 17, 22, 47, 15, 908775)
    }

    db.upsert_rows(
        conn,
        table_name,
        [different_value_row],
        update_cols_key=constraint_name,  
        update_cols_always=['value', 'last_updated']  # Add 'value' to columns to update
    )

    # With our standard constraint (id, name), changing just the value will UPDATE not INSERT
    count = db.select_scalar(conn, f'SELECT COUNT(*) FROM {table_name} WHERE id = 1')
    assert count == 1, 'Should have 1 row after upsert (value is not part of constraint)'

    # Verify the value was updated
    current_value = db.select_scalar(conn, f'SELECT value FROM {table_name} WHERE id = 1')
    assert current_value == 200, 'Value should be updated to 200'

    # For the second part with complex index, this check will only pass
    # if we're using the second table with the index that includes value
    if 'complex_unique_index' in constraint_name:
        rows = db.select(conn, f'SELECT id, name, value FROM {table_name} WHERE id = 1 ORDER BY value')
        assert len(rows) == 2, f'Expected 2 rows for {table_name} with constraint {constraint_name}'
        assert rows[0]['value'] == 100
        assert rows[1]['value'] == 200

        # For complex_unique_index, we expect 2 rows after updates
        count = db.select_scalar(conn, f'SELECT COUNT(*) FROM {table_name} WHERE id = 1')
        assert count == 2, f'Should have 2 rows for {table_name} with constraint {constraint_name}'
    else:
        # For the first table, we only expect one row since value isn't in the constraint
        rows = db.select(conn, f'SELECT id, name, value FROM {table_name} WHERE id = 1')
        assert len(rows) == 1, f'Expected 1 row for {table_name} with constraint {constraint_name}'
        assert rows[0]['value'] == 200

        # For complex_unique_constraint, we expect 1 row after updates
        count = db.select_scalar(conn, f'SELECT COUNT(*) FROM {table_name} WHERE id = 1')
        assert count == 1, f'Should have 1 row for {table_name} with constraint {constraint_name}'

    # Scenario 3: Update specific row by matching the constraint exactly
    update_specific_row = {
        'id': 1,
        'name': 'ComplexTest',
        'value': 200,  # Target the second row specifically
        'last_updated': datetime.datetime(2023, 3, 18, 10, 0, 0)
    }

    db.upsert_rows(
        conn,
        table_name,
        [update_specific_row],
        update_cols_key=constraint_name,
        update_cols_always=['value', 'last_updated']  # Add 'value' to columns to update
    )

    # Row count depends on constraint type - will be checked in the next section

    # Check the timestamp was updated only for the row with value=200
    last_updated_200 = db.select_scalar(conn,
                                        f'SELECT last_updated FROM {table_name} WHERE id = 1 AND value = 200')
    assert last_updated_200.date() == datetime.date(2023, 3, 18), 'Only the targeted row should be updated'

    # Scenario 4: Test with NULL value in the constrained column
    null_value_row = {
        'id': 2,
        'name': 'NullTest',
        'value': None,  # NULL value in a constrained column
        'last_updated': datetime.datetime(2023, 3, 19, 10, 0, 0)
    }

    db.upsert_rows(
        conn,
        table_name,
        [null_value_row],
        update_cols_key=constraint_name,
        update_cols_always=['value', 'last_updated']  # Add 'value' to columns to update
    )

    # Verify row was inserted
    count = db.select_scalar(conn, f'SELECT COUNT(*) FROM {table_name} WHERE id = 2')
    assert count == 1, 'Should have inserted row with NULL value'

    # Update the NULL value row
    update_null_row = {
        'id': 2,
        'name': 'NullTest',
        'value': None,  # Still NULL
        'last_updated': datetime.datetime(2023, 3, 20, 10, 0, 0)
    }

    db.upsert_rows(
        conn,
        table_name,
        [update_null_row],
        update_cols_key=constraint_name, 
        update_cols_always=['value', 'last_updated']  # Add 'value' to columns to update
    )

    # Should still have 1 row with id=2
    count = db.select_scalar(conn, f'SELECT COUNT(*) FROM {table_name} WHERE id = 2')
    assert count == 1, 'Should still have 1 row with id=2 after update'

    # Check timestamp was updated
    last_updated_null = db.select_scalar(conn,
                                         f'SELECT last_updated FROM {table_name} WHERE id = 2')
    assert last_updated_null.date() == datetime.date(2023, 3, 20), 'Row with NULL should be updated'


def test_upsert_with_column_order_mismatch(psql_docker, conn):
    """Test that upsert correctly handles when column order in dictionaries doesn't match DB schema order"""

    # Create a test table with a specific column order
    db.execute(conn, """
    CREATE TEMPORARY TABLE test_column_order (
        id SERIAL PRIMARY KEY,
        last_name VARCHAR(50) NOT NULL,
        first_name VARCHAR(50) NOT NULL,
        age INTEGER NOT NULL,
        email VARCHAR(100) NULL
    )
    """)

    # Insert data with columns in a completely different order than the schema
    rows = [
        {
            'email': 'john.doe@example.com',     # 5th in schema, 1st in dict
            'age': 30,                           # 4th in schema, 2nd in dict
            'first_name': 'John',                # 3rd in schema, 3rd in dict
            'last_name': 'Doe',                  # 2nd in schema, 4th in dict
            'id': 1                              # 1st in schema, 5th in dict
        },
        {
            'first_name': 'Jane',                # Different order for second row
            'email': 'jane.smith@example.com',
            'id': 2,
            'last_name': 'Smith',
            'age': 28
        }
    ]

    # Insert with mismatched column order
    row_count = db.upsert_rows(conn, 'test_column_order', rows)
    assert row_count == 2, 'Expected 2 rows to be inserted'

    # Verify all data was inserted correctly despite the column order mismatch
    result = db.select(conn, 'SELECT id, first_name, last_name, age, email FROM test_column_order ORDER BY id')

    assert len(result) == 2, 'Expected 2 rows to be returned'

    assert result[0]['id'] == 1
    assert result[0]['first_name'] == 'John'
    assert result[0]['last_name'] == 'Doe'
    assert result[0]['age'] == 30
    assert result[0]['email'] == 'john.doe@example.com'

    assert result[1]['id'] == 2
    assert result[1]['first_name'] == 'Jane'
    assert result[1]['last_name'] == 'Smith'
    assert result[1]['age'] == 28
    assert result[1]['email'] == 'jane.smith@example.com'

    # Now test updates with different column orders
    # Include all non-null fields needed for the upsert to work
    update_rows = [
        {
            'last_name': 'Doe-Updated',          # Different update order
            'id': 1,
            'age': 31,
            'first_name': 'John'                 # Include non-null field
        },
        {
            'age': 29,                           # Different order for second update
            'id': 2,
            'last_name': 'Smith-Updated',
            'first_name': 'Jane'                 # Include non-null field
        }
    ]

    # Update with mismatched column order
    update_count = db.upsert_rows(
        conn,
        'test_column_order',
        update_rows,
        update_cols_key=['id'],
        update_cols_always=['last_name', 'age']  # Only update these fields
    )
    assert update_count == 2, 'Expected 2 rows to be updated'

    # Verify updates were applied correctly despite column order differences
    result = db.select(conn, 'SELECT id, last_name, age FROM test_column_order ORDER BY id')

    assert result[0]['id'] == 1
    assert result[0]['last_name'] == 'Doe-Updated'
    assert result[0]['age'] == 31

    assert result[1]['id'] == 2
    assert result[1]['last_name'] == 'Smith-Updated'
    assert result[1]['age'] == 29


if __name__ == '__main__':
    __import__('pytest').main([__file__])
