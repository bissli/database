import datetime

import database as db


def test_select(psql_docker, conn):
    # Perform select query
    query = 'select name, value from test_table order by id'
    result = db.select(conn, query)

    # Check results
    expected_data = {'name': ['Alice', 'Bob', 'Charlie', 'Ethan', 'Fiona', 'George'], 'value': [10, 20, 30, 50, 70, 80]}
    assert result.to_dict('list') == expected_data, 'The select query did not return the expected results.'


def test_select_numeric(psql_docker, conn):
    """Test custom numeric adapter (skip the Decimal creation)
    """
    # Perform select query
    query = 'select name, value::numeric as value from test_table order by id'
    result = db.select(conn, query)

    # Check results
    expected_data = {'name': ['Alice', 'Bob', 'Charlie', 'Ethan', 'Fiona', 'George'], 'value': [10, 20, 30, 50, 70, 80]}
    assert result.to_dict('list') == expected_data, 'The select query did not return the expected results.'


def test_insert(psql_docker, conn):
    # Perform insert operation
    insert_sql = 'insert into test_table (name, value) values (%s, %s)'
    db.insert(conn, insert_sql, 'Diana', 40)

    # Verify insert operation
    query = "select name, value from test_table where name = 'Diana'"
    result = db.select(conn, query)
    expected_data = {'name': ['Diana'], 'value': [40]}
    assert result.to_dict('list') == expected_data, 'The insert operation did not insert the expected data.'


def test_update(psql_docker, conn):
    # Perform update operation
    update_sql = 'update test_table set value = %s where name = %s'
    db.update(conn, update_sql, 60, 'Ethan')

    # Verify update operation
    query = "select name, value from test_table where name = 'Ethan'"
    result = db.select(conn, query)
    expected_data = {'name': ['Ethan'], 'value': [60]}
    assert result.to_dict('list') == expected_data, 'The update operation did not update the data as expected.'


def test_delete(psql_docker, conn):
    # Perform delete operation
    delete_sql = 'delete from test_table where name = %s'
    db.delete(conn, delete_sql, 'Fiona')

    # Verify delete operation
    query = "select name, value from test_table where name = 'Fiona'"
    result = db.select(conn, query)
    assert result.empty, 'The delete operation did not delete the data as expected.'


def test_transaction(psql_docker, conn):
    # Perform transaction operations
    update_sql = 'update test_table set value = %s where name = %s'
    insert_sql = 'insert into test_table (name, value) values (%s, %s)'
    with db.transaction(conn) as tx:
        tx.execute(update_sql, 91, 'George')
        tx.execute(insert_sql, 'Hannah', 102)

    # Verify transaction operations
    query = 'select name, value from test_table order by name'
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


def test_upsert_reset_sequence(psql_docker, conn):
    """Test that the reset_sequence option properly resets the sequence after upsert operations"""
    # First, modify the test_table to have a SERIAL id column as primary key
    # While keeping a unique constraint on name for upsert operations
    db.execute(conn, """
alter table test_table drop constraint test_table_pkey;
alter table test_table add column test_id serial primary key;
alter table test_table add constraint test_table_name_unique unique (name);
""")

    # Get the current max sequence value
    current_max_id = db.select_scalar(conn, 'select max(test_id) from test_table')

    # Insert a new row with reset_sequence=False (default)
    new_rows = [{'name': 'Zack', 'value': 150}]
    db.upsert_rows(conn, 'test_table', new_rows, update_cols_key=['name'])

    # Now get the next sequence value by inserting a row directly
    db.execute(conn, "insert into test_table (name, value) values ('SequenceTest1', 200)")
    seq_test1_id = db.select_scalar(conn, "select test_id from test_table where name = 'SequenceTest1'")

    # Insert another row but with reset_sequence=True
    new_rows = [{'name': 'Yvonne', 'value': 175}]
    db.upsert_rows(conn, 'test_table', new_rows, update_cols_key=['name'], reset_sequence=True)

    # Now the sequence should be set to the max id + 1
    # Insert another row to check
    db.execute(conn, "insert into test_table (name, value) values ('SequenceTest2', 225)")
    seq_test2_id = db.select_scalar(conn, "select test_id from test_table where name = 'SequenceTest2'")

    # The new ID should be exactly max_id + 1
    max_id_after_reset = db.select_scalar(conn, "select max(test_id) from test_table where name != 'SequenceTest2'")
    assert seq_test2_id == max_id_after_reset + 1, f'Sequence was not properly reset. Expected {max_id_after_reset+1}, got {seq_test2_id}'

    # Clean up - restore the original primary key
    db.execute(conn, """
alter table test_table drop constraint if exists test_table_name_unique;
alter table test_table drop constraint test_table_pkey;
alter table test_table drop column test_id;
alter table test_table add primary key (name);
""")


def test_date_parameter_handling(psql_docker, conn):
    """Test handling of datetime.date objects as parameters.

    This tests the error case: 'object of type 'datetime.date' has no len()'
    """
    # Setup - insert test data with date field
    db.execute(conn, """
create temporary table test_date_table (
    id serial primary key,
    date date,
    identifier varchar(20),
    duplicate_see_id integer null,
    value integer
)
""")

    test_date = datetime.date(2025, 3, 3)
    test_identifier = 'BBG00RMV8099'

    db.insert(conn, """
insert into test_date_table (date, identifier, value)
values (%s, %s, %s)
""", test_date, test_identifier, 100)

    # Test 1: Using select directly with date parameter
    query = """
select date, identifier, value
from test_date_table
where date = %s
    and identifier = %s
    and duplicate_see_id is null
"""

    # This should work without error
    result = db.select(conn, query, test_date, test_identifier)
    assert len(result) == 1
    assert result.iloc[0]['identifier'] == test_identifier

    # Test 2: Using the same query in a transaction
    with db.transaction(conn) as tx:
        result = tx.select(query, test_date, test_identifier)
        assert len(result) == 1
        assert result.iloc[0]['identifier'] == test_identifier


def test_named_parameter_handling(psql_docker, conn):
    """Test handling of named parameters using a dictionary.

    This tests the error case: 'the query has 2 placeholders but 1 parameters were passed'
    """
    # Setup - create a temporary table for testing
    db.execute(conn, """
create temporary table test_named_params (
    id serial primary key,
    col varchar(20),
    time timestamp,
    value integer
)
""")

    db.execute(conn, """
create temporary table test_named_params_join (
    id serial primary key,
    id_bb_unique varchar(20),
    date date,
    data varchar(50)
)
""")

    # Insert test data
    db.insert(conn, """
insert into test_named_params (col, time, value)
values (%s, %s, %s)
""", 'TEST123', '2025-03-04 10:00:00', 200)

    db.insert(conn, """
insert into test_named_params_join (id_bb_unique, date, data)
values (%s, %s, %s)
""", 'TEST123', '2025-03-03', 'test data')

    # Test 1: Using select with named parameters
    query = """
select q.col, q.value, bu.data
from test_named_params q
left join test_named_params_join bu
    on bu.id_bb_unique = q.col and bu.date = %(pdate)s
where q.time::date = %(bdate)s
"""

    params = {
        'pdate': datetime.date(2025, 3, 3),
        'bdate': datetime.date(2025, 3, 4)
    }

    # This should work without error
    result = db.select(conn, query, params)
    assert len(result) == 1
    assert result.iloc[0]['col'] == 'TEST123'
    assert result.iloc[0]['data'] == 'test data'

    # Test 2: Using the same query in a transaction
    with db.transaction(conn) as tx:
        result = tx.select(query, params)
        assert len(result) == 1
        assert result.iloc[0]['col'] == 'TEST123'
        assert result.iloc[0]['data'] == 'test data'


if __name__ == '__main__':
    __import__('pytest').main([__file__])
