import database as db


def test_upsert_basic(sconn):
    """Test basic upsert functionality"""
    rows = [{'name': 'Barry', 'value': 50}, {'name': 'Wallace', 'value': 92}]
    row_count = db.upsert_rows(sconn, 'test_table', rows, update_cols_always=['value'])
    assert row_count == 2, 'upsert should return 2 for new rows'

    result = db.select(sconn, 'select name, value from test_table where name in (?, ?) order by name',
                       'Barry', 'Wallace')
    assert len(result) == 2
    assert result[0]['name'] == 'Barry'
    assert result[0]['value'] == 50
    assert result[1]['name'] == 'Wallace'
    assert result[1]['value'] == 92

    rows = [{'name': 'Barry', 'value': 51}, {'name': 'Wallace', 'value': 93}]
    row_count = db.upsert_rows(sconn, 'test_table', rows, update_cols_always=['value'])

    result = db.select(sconn, 'select name, value from test_table where name in (?, ?) order by name',
                       'Barry', 'Wallace')
    assert len(result) == 2
    assert result[0]['value'] == 51
    assert result[1]['value'] == 93


def test_upsert_ifnull(sconn):
    """Test upsert with update_cols_ifnull option"""
    db.insert(sconn, 'insert into test_table (name, value) values (?, ?)', 'UpsertNull', 100)

    rows = [{'name': 'UpsertNull', 'value': 200}]
    db.upsert_rows(sconn, 'test_table', rows, update_cols_ifnull=['value'])

    result = db.select_scalar(sconn, 'select value from test_table where name = ?', 'UpsertNull')
    assert result == 100, 'Value should not be updated when using update_cols_ifnull'

    db.execute(sconn, """
    CREATE TABLE #test_nullable (
        name VARCHAR(50) PRIMARY KEY,
        value INTEGER NULL
    )
    """)

    db.insert(sconn, 'INSERT INTO #test_nullable (name, value) VALUES (?, ?)', 'UpsertNull', 100)

    db.execute(sconn, 'UPDATE #test_nullable SET value = NULL WHERE name = ?', 'UpsertNull')

    rows = [{'name': 'UpsertNull', 'value': 200}]
    db.upsert_rows(sconn, '#test_nullable', rows, update_cols_ifnull=['value'])

    result = db.select_scalar(sconn, 'SELECT value FROM #test_nullable WHERE name = ?', 'UpsertNull')
    assert result == 200, 'Value should be updated when target is NULL'


def test_upsert_mixed_operations(sconn):
    """Test upsert with mix of inserts and updates"""
    # Make sure we have a clean sconnection state
    try:
        sconn.rollback()
    except:
        pass

    # Create a mix of new and existing rows
    rows = [
        {'name': 'Alice', 'value': 1000},  # Existing - update
        {'name': 'NewPerson1', 'value': 500},  # New - insert
        {'name': 'NewPerson2', 'value': 600}   # New - insert
    ]

    row_count = db.upsert_rows(sconn, 'test_table', rows, update_cols_always=['value'])
    assert row_count == 3

    # Verify results
    result = db.select(sconn, """
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


def test_upsert_identity(sconn):
    """Test identity columns during upsert"""
    # Make sure we have a clean sconnection state
    try:
        sconn.rollback()
    except:
        pass

    # Create a new table specifically for this test with an identity column
    db.execute(sconn, """
CREATE TABLE #test_identity_table (
    test_id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    value INTEGER NOT NULL
)
""")

    # Copy data from the test_table to our new table
    db.execute(sconn, """
INSERT INTO #test_identity_table (name, value)
SELECT name, value FROM test_table
WHERE name IN ('Alice', 'Bob', 'Charlie')
""")

    # Get the current max identity value
    current_max_id = db.select_scalar(sconn, 'SELECT max(test_id) FROM #test_identity_table')

    # Insert a new row
    new_rows = [{'name': 'Zack', 'value': 150}]
    db.upsert_rows(sconn, '#test_identity_table', new_rows)

    # Verify the insertion worked
    result = db.select_scalar(sconn, 'SELECT value FROM #test_identity_table WHERE name = ?', 'Zack')
    assert result == 150, 'Row was not inserted correctly'

    # Now insert another row and check that identity is incremented properly
    new_rows = [{'name': 'Yvonne', 'value': 175}]
    db.upsert_rows(sconn, '#test_identity_table', new_rows)

    # Get the highest ID after insertions
    max_id_after = db.select_scalar(sconn, 'SELECT max(test_id) FROM #test_identity_table')

    # Should be higher than the previous max
    assert max_id_after > current_max_id, f'Identity value not incremented. Before: {current_max_id}, After: {max_id_after}'


def test_upsert_empty_rows(sconn):
    """Test upsert with empty rows list"""
    # Should not error and return 0
    result = db.upsert_rows(sconn, 'test_table', [])
    assert result == 0


def test_upsert_large_batch(sconn):
    """Test upsert with a large number of rows that exceeds parameter limits"""
    import time

    # Create a temporary table for this test with a simple structure
    db.execute(sconn, """
    CREATE TABLE #test_large_batch (
        id INTEGER PRIMARY KEY,
        value NVARCHAR(50) NOT NULL
    )
    """)

    start_time = time.time()
    rows = [{'id': i, 'value': f'value-{i}'} for i in range(1, 1201)]

    batch_insert_time = time.time()
    row_count = db.upsert_rows(sconn, '#test_large_batch', rows)
    insert_end_time = time.time()

    # Verify correct number of rows inserted
    assert row_count == 1200, f'Expected 1200 rows to be inserted, got {row_count}'

    # Verify data was inserted correctly (check a few values)
    sample_ids = [1, 100, 500, 1000]
    for id_val in sample_ids:
        result = db.select_scalar(sconn, 'SELECT value FROM #test_large_batch WHERE id = ?', id_val)
        assert result == f'value-{id_val}', f'Wrong value for id {id_val}: {result}'

    # Now modify the rows and upsert again to test updates with large batch
    update_rows = [
        {'id': i, 'value': f'updated-{i}'}
        for i in range(1, 1201)
    ]

    # Update the rows (this will use UPSERT with batching)
    update_start_time = time.time()
    update_count = db.upsert_rows(sconn, '#test_large_batch', update_rows,
                                  update_cols_always=['value'])
    update_end_time = time.time()

    # Verify correct number of rows updated
    assert update_count == 1200, f'Expected 1200 rows to be updated, got {update_count}'

    # Verify data was updated correctly (check same sample)
    for id_val in sample_ids:
        result = db.select_scalar(sconn, 'SELECT value FROM #test_large_batch WHERE id = ?', id_val)
        assert result == f'updated-{id_val}', f'Wrong value after update for id {id_val}: {result}'

    # Verify total row count in the table
    total_rows = db.select_scalar(sconn, 'SELECT COUNT(*) FROM #test_large_batch')
    assert total_rows == 1200, f'Expected 1200 total rows, found {total_rows}'

    # Optional: Print performance metrics
    insert_time = insert_end_time - batch_insert_time
    update_time = update_end_time - update_start_time

    print('\nLarge batch performance:')
    print(f'Insert time for 1200 rows: {insert_time:.2f}s')
    print(f'Update time for 1200 rows: {update_time:.2f}s')

    # Assert reasonable performance (adjust thresholds as appropriate for your system)
    # Note: These are very generous thresholds for debugging - tune for your environment
    assert insert_time < 60, f'Insert too slow: {insert_time:.2f}s'
    assert update_time < 60, f'Update too slow: {update_time:.2f}s'


def test_upsert_invalid_columns(sconn):
    """Test upsert with invalid columns"""
    # Make sure we have a clean sconnection state
    try:
        sconn.rollback()
    except:
        pass

    rows = [{'name': 'InvalidTest', 'value': 100, 'nonexistent': 'should be filtered'}]
    db.upsert_rows(sconn, 'test_table', rows)

    # Verify the row was inserted without error
    result = db.select_row(sconn, 'select name, value from test_table where name = ?', 'InvalidTest')
    assert result.name == 'InvalidTest'
    assert result.value == 100


def test_upsert_no_primary_keys(sconn):
    """Test upsert with a table that has no primary keys"""
    # Create a table without primary keys
    db.execute(sconn, """
    CREATE TABLE #test_no_pk (
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
    row_count = db.upsert_rows(sconn, '#test_no_pk', rows)
    assert row_count == 2, 'upsert should insert 2 rows'

    # Verify rows were inserted
    result = db.select(sconn, 'select name, value from #test_no_pk order by name')
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

    row_count = db.upsert_rows(sconn, '#test_no_pk', rows, update_cols_always=['value'])
    assert row_count == 2, 'upsert should insert 2 new rows'

    # We should now have 4 rows
    result = db.select(sconn, 'select name, value from #test_no_pk order by name, value')
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
