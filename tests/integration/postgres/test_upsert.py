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
    """Test upsert with a batch size that exceeds parameter limits"""
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

    # First insert the rows (should use batching internally)
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
