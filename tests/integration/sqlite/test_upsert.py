import database as db


def test_upsert_basic(sqlite_conn):
    """Test basic upsert functionality"""
    # Insert new rows
    rows = [{'name': 'Barry', 'value': 50}, {'name': 'Wallace', 'value': 92}]
    row_count = db.upsert_rows(sqlite_conn, 'test_table', rows, update_cols_always=['value'])
    assert row_count == 2, 'upsert should return 2 for new rows'

    # Verify rows were inserted
    result = db.select(sqlite_conn, 'select name, value from test_table where name in (?, ?) order by name',
                       'Barry', 'Wallace')
    assert len(result) == 2
    assert result[0]['name'] == 'Barry'
    assert result[0]['value'] == 50
    assert result[1]['name'] == 'Wallace'
    assert result[1]['value'] == 92

    # Update existing rows
    rows = [{'name': 'Barry', 'value': 51}, {'name': 'Wallace', 'value': 93}]
    row_count = db.upsert_rows(sqlite_conn, 'test_table', rows, update_cols_always=['value'])

    # Verify rows were updated
    result = db.select(sqlite_conn, 'select name, value from test_table where name in (?, ?) order by name',
                       'Barry', 'Wallace')
    assert len(result) == 2
    assert result[0]['value'] == 51
    assert result[1]['value'] == 93


def test_upsert_ifnull(sqlite_conn):
    """Test upsert with update_cols_ifnull option"""
    # First insert a row
    db.insert(sqlite_conn, 'insert into test_table (name, value) values (?, ?)', 'UpsertNull', 100)

    # Try to update with update_cols_ifnull - should not update existing value
    rows = [{'name': 'UpsertNull', 'value': 200}]
    db.upsert_rows(sqlite_conn, 'test_table', rows, update_cols_ifnull=['value'])

    # Verify value was not updated
    result = db.select_scalar(sqlite_conn, 'select value from test_table where name = ?', 'UpsertNull')
    assert result == 100, 'Value should not be updated when using update_cols_ifnull'

    # Create a new test table that allows nulls for this specific test
    db.execute(sqlite_conn, """
    CREATE TEMPORARY TABLE test_nullable (
        name VARCHAR(50) PRIMARY KEY,
        value INTEGER NULL
    )
    """)

    # Insert initial data
    db.insert(sqlite_conn, 'INSERT INTO test_nullable (name, value) VALUES (?, ?)', 'UpsertNull', 100)

    # Now set the value to NULL - this works because the new table allows nulls
    db.execute(sqlite_conn, 'UPDATE test_nullable SET value = NULL WHERE name = ?', 'UpsertNull')

    # Try to update with update_cols_ifnull again - now it should update
    rows = [{'name': 'UpsertNull', 'value': 200}]
    db.upsert_rows(sqlite_conn, 'test_nullable', rows, update_cols_ifnull=['value'])

    # Verify value was updated
    result = db.select_scalar(sqlite_conn, 'SELECT value FROM test_nullable WHERE name = ?', 'UpsertNull')
    assert result == 200, 'Value should be updated when target is NULL'


def test_upsert_mixed_operations(sqlite_conn):
    """Test upsert with mix of inserts and updates"""
    # Make sure we have a clean connection state
    try:
        sqlite_conn.rollback()
    except:
        pass

    # Create a mix of new and existing rows
    rows = [
        {'name': 'Alice', 'value': 1000},  # Existing - update
        {'name': 'NewPerson1', 'value': 500},  # New - insert
        {'name': 'NewPerson2', 'value': 600}   # New - insert
    ]

    row_count = db.upsert_rows(sqlite_conn, 'test_table', rows, update_cols_always=['value'])
    assert row_count == 3

    # Verify results
    result = db.select(sqlite_conn, """
        select name, value from test_table
        where name in (?, ?, ?)
        order by name
    """, 'Alice', 'NewPerson1', 'NewPerson2')

    assert len(result) == 3
    assert result[0]['name'] == 'Alice'
    assert result[0]['value'] == 1000
    assert result[1]['name'] == 'NewPerson1'
    assert result[1]['value'] == 500
    assert result[2]['name'] == 'NewPerson2'
    assert result[2]['value'] == 600


def test_upsert_rowid(sqlite_conn):
    """Test SQLite's rowid handling during upsert"""
    # Make sure we have a clean connection state
    try:
        sqlite_conn.rollback()
    except:
        pass

    # Create a new table specifically for this test that relies on rowid
    db.execute(sqlite_conn, """
CREATE TEMPORARY TABLE test_rowid_table (
    name VARCHAR(50) UNIQUE NOT NULL,
    value INTEGER NOT NULL
)
""")

    # Insert initial data
    db.insert(sqlite_conn, 'INSERT INTO test_rowid_table (name, value) VALUES (?, ?)', 'First', 100)
    db.insert(sqlite_conn, 'INSERT INTO test_rowid_table (name, value) VALUES (?, ?)', 'Second', 200)

    # Get rowids of initial data
    first_rowid = db.select_scalar(sqlite_conn, 'SELECT rowid FROM test_rowid_table WHERE name = ?', 'First')
    second_rowid = db.select_scalar(sqlite_conn, 'SELECT rowid FROM test_rowid_table WHERE name = ?', 'Second')

    # Insert a new row using upsert
    new_rows = [{'name': 'Third', 'value': 300}]
    db.upsert_rows(sqlite_conn, 'test_rowid_table', new_rows, update_cols_key=['name'])

    # Update an existing row
    update_rows = [{'name': 'First', 'value': 150}]
    db.upsert_rows(sqlite_conn, 'test_rowid_table', update_rows, update_cols_key=['name'], update_cols_always=['value'])

    # Verify the rowid is preserved after update
    first_rowid_after = db.select_scalar(sqlite_conn, 'SELECT rowid FROM test_rowid_table WHERE name = ?', 'First')
    assert first_rowid == first_rowid_after, 'SQLite rowid should be preserved during upsert'

    # Verify the value was updated
    first_value = db.select_scalar(sqlite_conn, 'SELECT value FROM test_rowid_table WHERE name = ?', 'First')
    assert first_value == 150, 'Value should be updated to 150'

    # Get the rowid of the third row
    third_rowid = db.select_scalar(sqlite_conn, 'SELECT rowid FROM test_rowid_table WHERE name = ?', 'Third')
    assert third_rowid > second_rowid, 'New row should have higher rowid'


def test_upsert_empty_rows(sqlite_conn):
    """Test upsert with empty rows list"""
    # Should not error and return 0
    result = db.upsert_rows(sqlite_conn, 'test_table', [])
    assert result == 0


def test_upsert_large_batch(sqlite_conn):
    """Test upsert with a large batch size"""
    import time

    # Create a temporary table for this test with a simple structure
    db.execute(sqlite_conn, """
    CREATE TEMPORARY TABLE test_large_batch (
        id INTEGER PRIMARY KEY,
        value TEXT NOT NULL
    )
    """)

    # For SQLite, we don't need as many rows as PostgreSQL to test batching
    # SQLite has a parameter limit of 999 by default
    # With 2 parameters per row (id and value), we need 500 rows to approach it
    start_time = time.time()
    rows = [{'id': i, 'value': f'value-{i}'} for i in range(1, 501)]

    # First insert the rows (should use batching internally)
    batch_insert_time = time.time()
    row_count = db.upsert_rows(sqlite_conn, 'test_large_batch', rows)
    insert_end_time = time.time()

    # Verify correct number of rows inserted
    assert row_count == 500, f'Expected 500 rows to be inserted, got {row_count}'

    # Verify data was inserted correctly (check a few values)
    sample_ids = [1, 100, 250, 499]
    for id_val in sample_ids:
        result = db.select_scalar(sqlite_conn, 'SELECT value FROM test_large_batch WHERE id = ?', id_val)
        assert result == f'value-{id_val}', f'Wrong value for id {id_val}: {result}'

    # Now modify the rows and upsert again to test updates with large batch
    update_rows = [
        {'id': i, 'value': f'updated-{i}'}
        for i in range(1, 501)
    ]

    # Update the rows (this will use UPSERT with batching)
    update_start_time = time.time()
    update_count = db.upsert_rows(sqlite_conn, 'test_large_batch', update_rows,
                                  update_cols_key=['id'],
                                  update_cols_always=['value'])
    update_end_time = time.time()

    # Verify correct number of rows updated
    assert update_count == 500, f'Expected 500 rows to be updated, got {update_count}'

    # Verify data was updated correctly (check same sample)
    for id_val in sample_ids:
        result = db.select_scalar(sqlite_conn, 'SELECT value FROM test_large_batch WHERE id = ?', id_val)
        assert result == f'updated-{id_val}', f'Wrong value after update for id {id_val}: {result}'

    # Verify total row count in the table
    total_rows = db.select_scalar(sqlite_conn, 'SELECT COUNT(*) FROM test_large_batch')
    assert total_rows == 500, f'Expected 500 total rows, found {total_rows}'

    # Optional: Print performance metrics
    insert_time = insert_end_time - batch_insert_time
    update_time = update_end_time - update_start_time

    print('\nLarge batch performance:')
    print(f'Insert time for 500 rows: {insert_time:.2f}s')
    print(f'Update time for 500 rows: {update_time:.2f}s')

    # Assert reasonable performance (adjust thresholds as appropriate for your system)
    # Note: These are very generous thresholds for debugging - tune for your environment
    assert insert_time < 10, f'Insert too slow: {insert_time:.2f}s'
    assert update_time < 10, f'Update too slow: {update_time:.2f}s'


def test_upsert_invalid_columns(sqlite_conn):
    """Test upsert with invalid columns"""
    # Make sure we have a clean connection state
    try:
        sqlite_conn.rollback()
    except:
        pass

    rows = [{'name': 'InvalidTest', 'value': 100, 'nonexistent': 'should be filtered'}]
    db.upsert_rows(sqlite_conn, 'test_table', rows)

    # Verify the row was inserted without error
    result = db.select_row(sqlite_conn, 'select name, value from test_table where name = ?', 'InvalidTest')
    assert result.name == 'InvalidTest'
    assert result.value == 100


def test_upsert_no_primary_keys(sqlite_conn):
    """Test upsert with a table that has no primary keys"""
    # Create a table without primary keys
    db.execute(sqlite_conn, """
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
    row_count = db.upsert_rows(sqlite_conn, 'test_no_pk', rows)
    assert row_count == 2, 'upsert should insert 2 rows'

    # Verify rows were inserted
    result = db.select(sqlite_conn, 'select name, value from test_no_pk order by name')
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

    row_count = db.upsert_rows(sqlite_conn, 'test_no_pk', rows, update_cols_always=['value'])
    assert row_count == 2, 'upsert should insert 2 new rows'

    # We should now have 4 rows
    result = db.select(sqlite_conn, 'select name, value from test_no_pk order by name, value')
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


if __name__ == '__main__':
    __import__('pytest').main([__file__])
