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


def test_named_params_in_clause(psql_docker, conn):
    """Test handling of IN clause with named parameters, including single-item tuples.
    """
    # Setup - create a temporary table for testing
    db.execute(conn, """
create temporary table test_in_clause (
    id serial primary key,
    category varchar(20),
    vendor varchar(20),
    description varchar(50)
)
""")

    # Insert test data
    test_data = [
        ('Electronics', 'Apple', 'Smartphone'),
        ('Electronics', 'Samsung', 'Tablet'),
        ('Clothing', 'Nike', 'Running shoes')
    ]

    for category, vendor, desc in test_data:
        db.insert(conn, """
insert into test_in_clause (category, vendor, description)
values (%s, %s, %s)
""", category, vendor, desc)

    # Test 1: Single-item tuple in IN clause - this was failing previously
    query = """
select
    distinct
    category,
    description
from
    test_in_clause
where
    category in %(categories)s
and
    vendor = %(vendor)s
"""
    params = {'categories': ('Electronics',), 'vendor': 'Apple'}

    result = db.select(conn, query, params)
    assert len(result) == 1
    assert result.iloc[0]['category'] == 'Electronics'
    assert result.iloc[0]['description'] == 'Smartphone'

    # Test 2: Multiple items in IN clause
    params = {'categories': ('Electronics', 'Clothing'), 'vendor': 'Nike'}
    result = db.select(conn, query, params)
    assert len(result) == 1
    assert result.iloc[0]['category'] == 'Clothing'

    # Test 3: IN clause with a different vendor
    params = {'categories': ('Electronics',), 'vendor': 'Samsung'}
    result = db.select(conn, query, params)
    assert len(result) == 1
    assert result.iloc[0]['category'] == 'Electronics'
    assert result.iloc[0]['description'] == 'Tablet'

    # Test 4: No matching data
    params = {'categories': ('Books',), 'vendor': 'Apple'}
    result = db.select(conn, query, params)
    assert len(result) == 0


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


def test_direct_list_parameters(psql_docker, conn):
    """Test the direct list parameter format for IN clauses."""
    # Insert test data
    test_ids = [101, 102, 103]
    for i, test_id in enumerate(test_ids):
        db.insert(conn, 'insert into test_table (name, value) values (%s, %s)',
                 f'DirectTest{i}', test_id)
    
    # Test direct list parameter syntax for IN clause
    result = db.select(conn, 'select name, value from test_table where value IN %s order by value',
                      test_ids)
    
    # Verify results
    assert len(result) == 3
    assert list(result['value']) == test_ids
    
    # Test with a single-item list
    result = db.select(conn, 'select name, value from test_table where value IN %s',
                      [101])
    
    assert len(result) == 1
    assert result.iloc[0]['value'] == 101
    
    # Compare with traditional format
    result_traditional = db.select(conn, 'select name, value from test_table where value IN %s',
                                  ([101],))
    
    assert len(result) == len(result_traditional)
    assert result.iloc[0]['value'] == result_traditional.iloc[0]['value']


def test_direct_lists_for_multiple_in_clauses(psql_docker, conn):
    """Test using direct lists for multiple IN clauses."""
    # Insert test data for multiple categories and statuses
    categories = ['cat1', 'cat2', 'cat3']
    statuses = ['active', 'pending']
    
    # Create a temp table to test with
    db.execute(conn, """
    CREATE TEMPORARY TABLE multi_in_test (
        id SERIAL PRIMARY KEY,
        category TEXT,
        status TEXT
    )
    """)
    
    # Insert test data with different combinations
    for i, (cat, status) in enumerate([(c, s) for c in categories for s in statuses]):
        db.insert(conn, 'INSERT INTO multi_in_test (category, status) VALUES (%s, %s)',
                 cat, status)
    
    # Now test direct lists for multiple IN clauses
    result = db.select(conn, """
    SELECT category, status 
    FROM multi_in_test 
    WHERE category IN %s AND status IN %s
    ORDER BY category, status
    """, ['cat1', 'cat2'], ['active', 'pending'])
    
    # Verify results
    assert len(result) == 4  # 2 categories × 2 statuses
    assert set(result['category'].unique()) == {'cat1', 'cat2'}
    assert set(result['status'].unique()) == {'active', 'pending'}


if __name__ == '__main__':
    __import__('pytest').main([__file__])
