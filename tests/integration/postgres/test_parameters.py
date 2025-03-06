import datetime

import database as db
import pandas as pd


def test_date_parameter_handling(psql_docker, conn):
    """Test handling of datetime.date objects as parameters."""
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
    assert result.iloc[0]['date'] == test_date

    # Test 2: Using the same query in a transaction
    with db.transaction(conn) as tx:
        result = tx.select(query, test_date, test_identifier)
        assert len(result) == 1
        assert result.iloc[0]['identifier'] == test_identifier


def test_named_parameter_handling(psql_docker, conn):
    """Test handling of named parameters using a dictionary."""
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


def test_none_parameter(psql_docker, conn):
    """Test handling of None parameters in queries."""
    result = db.select(conn, 'select %s::text as null_value', None)
    assert pd.isna(result.iloc[0]['null_value'])


def test_list_parameters(psql_docker, conn):
    """Test handling of list parameters for IN clauses."""
    # Insert test data
    names = ['InTest1', 'InTest2', 'InTest3']
    for i, name in enumerate(names):
        db.insert(conn, 'insert into test_table (name, value) values (%s, %s)',
                  name, (i+1)*10)

    # Query with IN clause using string formatting (not ideal but common)
    placeholders = ','.join(['%s'] * len(names))
    query = f'select name, value from test_table where name in ({placeholders}) order by value'

    result = db.select(conn, query, *names)
    assert len(result) == 3
    assert list(result['name']) == names


def test_numeric_parameters(psql_docker, conn):
    """Test handling of numeric parameters."""
    # Test integers
    result = db.select(conn, 'select %s::int as int_val', 42)
    assert result.iloc[0]['int_val'] == 42

    # Test floats
    result = db.select(conn, 'select %s::float as float_val', 42.5)
    assert result.iloc[0]['float_val'] == 42.5

    # Test with computation
    result = db.select(conn, 'select %s + %s as sum_val', 10, 20)
    assert result.iloc[0]['sum_val'] == 30


if __name__ == '__main__':
    __import__('pytest').main([__file__])
