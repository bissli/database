import database as db


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


if __name__ == '__main__':
    __import__('pytest').main([__file__])
