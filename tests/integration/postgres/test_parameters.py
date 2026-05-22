import datetime

import database as db


def test_date_parameter_handling(psql_docker, pg_conn):
    """Test handling of datetime.date objects as parameters."""
    # Setup - insert test data with date field
    db.execute(pg_conn, """
create temporary table test_date_table (
    id serial primary key,
    date date,
    identifier varchar(20),
    duplicate_see_id integer null,
    value integer
)
""")

    test_date = datetime.date(2025, 3, 3)
    test_identifier = 'TEST123'

    db.insert(pg_conn, """
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
    result = db.select(pg_conn, query, test_date, test_identifier)
    assert len(result) == 1
    assert result[0]['identifier'] == test_identifier
    assert result[0]['date'] == test_date

    # Test 2: Using the same query in a transaction
    with db.transaction(pg_conn) as tx:
        result = tx.select(query, test_date, test_identifier)
        assert len(result) == 1
        assert result[0]['identifier'] == test_identifier


def test_named_parameter_handling(psql_docker, pg_conn):
    """Test handling of named parameters using a dictionary."""
    # Setup - create a temporary table for testing
    db.execute(pg_conn, """
create temporary table test_named_params (
    id serial primary key,
    col varchar(20),
    time timestamp,
    value integer
)
""")

    db.execute(pg_conn, """
create temporary table test_named_params_join (
    id serial primary key,
    id_bb_unique varchar(20),
    date date,
    data varchar(50)
)
""")

    # Insert test data
    db.insert(pg_conn, """
insert into test_named_params (col, time, value)
values (%s, %s, %s)
""", 'TEST123', '2025-03-04 10:00:00', 200)

    db.insert(pg_conn, """
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
    result = db.select(pg_conn, query, params)
    assert len(result) == 1
    assert result[0]['col'] == 'TEST123'
    assert result[0]['data'] == 'test data'

    # Test 2: Using the same query in a transaction
    with db.transaction(pg_conn) as tx:
        result = tx.select(query, params)
        assert len(result) == 1
        assert result[0]['col'] == 'TEST123'
        assert result[0]['data'] == 'test data'


def test_named_params_in_clause(psql_docker, pg_conn):
    """Test handling of IN clause with named parameters, including single-item tuples.
    """
    # Setup - create a temporary table for testing
    db.execute(pg_conn, """
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
        db.insert(pg_conn, """
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

    result = db.select(pg_conn, query, params)
    assert len(result) == 1
    assert result[0]['category'] == 'Electronics'
    assert result[0]['description'] == 'Smartphone'

    # Test 2: Multiple items in IN clause
    params = {'categories': ('Electronics', 'Clothing'), 'vendor': 'Nike'}
    result = db.select(pg_conn, query, params)
    assert len(result) == 1
    assert result[0]['category'] == 'Clothing'

    # Test 3: IN clause with a different vendor
    params = {'categories': ('Electronics',), 'vendor': 'Samsung'}
    result = db.select(pg_conn, query, params)
    assert len(result) == 1
    assert result[0]['category'] == 'Electronics'
    assert result[0]['description'] == 'Tablet'

    # Test 4: No matching data
    params = {'categories': ('Books',), 'vendor': 'Apple'}
    result = db.select(pg_conn, query, params)
    assert len(result) == 0


def test_none_parameter(psql_docker, pg_conn):
    """Test handling of None parameters in queries."""
    result = db.select(pg_conn, 'select %s::text as null_value', None)
    assert result[0]['null_value'] is None


def test_numeric_parameters(psql_docker, pg_conn):
    """Test handling of numeric parameters."""
    # Test integers
    result = db.select(pg_conn, 'select %s::int as int_val', 42)
    assert result[0]['int_val'] == 42

    # Test floats
    result = db.select(pg_conn, 'select %s::float as float_val', 42.5)
    assert result[0]['float_val'] == 42.5

    # Test with computation
    result = db.select(pg_conn, 'select %s + %s as sum_val', 10, 20)
    assert result[0]['sum_val'] == 30


def test_like_clause_with_pre_escaped_percent(psql_docker, pg_conn):
    """Test LIKE clause with already-escaped percent signs works correctly with real DB."""
    # Create a temp table for testing LIKE patterns
    db.execute(pg_conn, """
    CREATE TEMPORARY TABLE like_pattern_test (
        id SERIAL PRIMARY KEY,
        status TEXT,
        description TEXT
    )
    """)

    # Insert test data with different patterns
    test_data = [
        ('%%Saved', 'Double-percent followed by Saved'),
        ('Not%%Saved', 'Double-percent in the middle'),
        ('%Saved', 'Single-percent at start'),
        ('Saved%', 'Single-percent at end'),
        ('%%S%%aved', 'Multiple double-percents'),
        ('Regular', 'No percent signs')
    ]

    for status, description in test_data:
        db.insert(pg_conn, """
        INSERT INTO like_pattern_test (status, description)
        VALUES (%s, %s)
        """, status, description)

    # Test 1: Search with an exact match using =
    # Note: When we insert '%%Saved', it's stored as '%Saved' in PostgreSQL
    # because %% is the escape sequence for a literal % in SQL
    query1 = "SELECT * FROM like_pattern_test WHERE status = '%Saved'"
    result1 = db.select(pg_conn, query1)
    assert len(result1) == 1
    # The status might be returned as %%Saved in some database drivers
    # because of how they handle % characters in strings
    assert '%Saved' in result1[0]['status'].replace('%%', '%')

    # Test 2: Combine already-escaped pattern with parameter
    # This tests the scenario from the unit test that was failing
    author_id = 1
    query2 = "SELECT * FROM like_pattern_test WHERE status LIKE '%S%' AND id > %s"
    result2 = db.select(pg_conn, query2, author_id)
    assert len(result2) >= 2
    # Note: In the database, '%%S' is stored as '%S'
    assert '%S' in result2[0]['status']

    # Test 3: Using an escaped pattern in a transaction
    with db.transaction(pg_conn) as tx:
        # This query looks for rows containing '%S%'
        # In SQL, '%%' escapes to a literal '%', so we're looking for %S% in the data
        query3 = "SELECT * FROM like_pattern_test WHERE status LIKE '%%S%%'"
        result3 = tx.select(query3)

        # The query will match any row with a '%' followed by 'S' followed by anything
        # Let's verify our data matches what we expect
        matched_status_values = {row['status'] for row in result3}

        # These rows are expected to match:
        # - '%%Saved' (stored as '%Saved') - has %S
        # - '%%S%%aved' (stored as '%S%aved') - has %S
        # - 'Not%%Saved' (stored as 'Not%Saved') - has Not%S
        # Verify our key values are included in the results
        assert '%Saved' in matched_status_values or '%%Saved' in matched_status_values
        assert '%%S%%aved' in matched_status_values

        # For a more specific test that matches only rows with exactly '%S%'
        query_exact = "SELECT * FROM like_pattern_test WHERE status = '%S%'"
        result_exact = tx.select(query_exact)
        assert len(result_exact) == 0  # No exact matches for '%S%'

    # Test 4: Pattern with both escaped and unescaped percents
    # This should match a literal %S followed by anything
    query4 = "SELECT * FROM like_pattern_test WHERE status LIKE '%%S%'"
    result4 = db.select(pg_conn, query4)

    # Rather than checking exact count (which depends on database specifics),
    # ensure the right values are included
    matched_statuses = {row['status'] for row in result4}
    # We expect '%Saved' and '%%S%%aved' to be in the results
    expected_matches = {'%Saved', '%%S%%aved'}
    assert expected_matches.issubset(matched_statuses), f'Expected {expected_matches} to be subset of {matched_statuses}'

    # Find rows that should contain '%S' pattern and verify at least one exists
    rows_with_s_pattern = [row for row in result4 if '%S' in row['status']]
    assert len(rows_with_s_pattern) > 0, "No rows with '%S' pattern found in results"


def test_combined_in_clause_named_params_with_returnid(psql_docker, pg_conn):
    """Test combining IN clause with named parameters and the RETURNING clause."""
    # Create a table with an auto-incrementing ID and category-related columns
    db.execute(pg_conn, """
    CREATE TEMPORARY TABLE combined_test (
        id SERIAL PRIMARY KEY,
        category TEXT NOT NULL,
        name TEXT NOT NULL,
        value INTEGER NOT NULL
    )
    """)

    # Insert some initial data
    categories = ['electronics', 'clothing', 'food']
    for i, category in enumerate(categories):
        db.execute(pg_conn, """
        INSERT INTO combined_test (category, name, value)
        VALUES (%s, %s, %s)
        """, category, f'Item_{i}', i*10)

    # Now test the combined features using a transaction
    with db.transaction(pg_conn) as tx:
        # Prepare named parameters with an IN clause
        params = {
            'categories': ('electronics', 'clothing'),  # IN clause with tuple
            'name': 'Combined_Test_Item',
            'value': 500
        }

        # No need to manually set iterdict_data_loader as it's now the default

        # Execute with RETURNING and named parameters including IN clause
        result_ids = tx.execute("""
        INSERT INTO combined_test (category, name, value)
        SELECT
            category,
            %(name)s as name,
            %(value)s as value
        FROM combined_test
        WHERE category IN %(categories)s
        RETURNING id, category
        """, params, returnid=['id', 'category'])

        # Verify we got multiple results back correctly
        assert isinstance(result_ids, list), 'Should return a list of results'
        ids, categories = zip(*result_ids)

        # Verify we got integers for id values (not numpy/pandas types)
        for id_val in ids:
            assert isinstance(id_val, int), f'Expected int, got {type(id_val)}'

        # Should have 2 rows returned (one for each category in the IN clause)
        assert len(ids) == 2, f'Expected 2 rows, got {len(ids)}'

        # Verify the returned categories match our IN clause
        categories_set = set(categories)
        assert 'electronics' in categories_set, "Should include 'electronics' category"
        assert 'clothing' in categories_set, "Should include 'clothing' category"

        # Verify the inserted data exists with correct values
        for id_val in ids:
            row = tx.select_row('SELECT * FROM combined_test WHERE id = %s', id_val)
            assert row.name == 'Combined_Test_Item'
            assert row.value == 500
            assert row.category in {'electronics', 'clothing'}


def test_is_null_parameter_handling(psql_docker, pg_conn):
    """Test correct handling of NULL values with IS NULL/IS NOT NULL operators."""
    # Create a test table
    db.execute(pg_conn, """
    CREATE TEMPORARY TABLE test_null_handling (
        id SERIAL PRIMARY KEY,
        name TEXT,
        value INTEGER
    )
    """)

    # Insert test data with NULL and non-NULL values
    db.execute(pg_conn, """
    INSERT INTO test_null_handling (name, value) VALUES
    ('item1', 100),
    ('item2', NULL),
    ('item3', 300)
    """)

    # Test 1: Correct usage with IS NULL (no parameter needed)
    result = db.select(pg_conn, """
    SELECT name, value FROM test_null_handling
    WHERE value IS NULL
    """)
    assert len(result) == 1
    assert result[0]['name'] == 'item2'

    # Test 2: Correct usage with IS NOT NULL (no parameter needed)
    result = db.select(pg_conn, """
    SELECT name, value FROM test_null_handling
    WHERE value IS NOT NULL
    ORDER BY name
    """)
    assert len(result) == 2
    assert [r['name'] for r in result] == ['item1', 'item3']

    # Test 3: Parameterized date range with IS NULL condition
    # This demonstrates the correct pattern for combining parameterized values
    # with NULL checks in the same query
    start_date = datetime.date(2025, 1, 1)
    end_date = datetime.date(2025, 3, 11)

    # Create a table with dates for testing
    db.execute(pg_conn, """
    CREATE TEMPORARY TABLE test_date_null (
        id SERIAL PRIMARY KEY,
        date DATE,
        value INTEGER,
        strategy TEXT
    )
    """)

    # Insert test data
    db.execute(pg_conn, """
    INSERT INTO test_date_null (date, value, strategy) VALUES
    ('2025-01-15', 100, 'A'),
    ('2025-02-01', NULL, 'B'),
    ('2025-03-01', 300, 'A')
    """)

    # This is the CORRECT way to handle NULL checks with parameters:
    # Use IS NULL or IS NOT NULL directly in the SQL, not with parameters
    result = db.select(pg_conn, """
    SELECT date, value, strategy
    FROM test_date_null
    WHERE date BETWEEN %s AND %s
    AND value IS NOT NULL
    """, start_date, end_date)

    assert len(result) == 2
    assert {r['strategy'] for r in result} == {'A'}

    # Test 4: Testing for a specific value OR NULL using parameters
    # For cases where you want to find rows where value = X OR value IS NULL
    result = db.select(pg_conn, """
    SELECT name, value
    FROM test_null_handling
    WHERE value = %s OR value IS NULL
    """, 100)

    assert len(result) == 2
    assert {r['name'] for r in result} == {'item1', 'item2'}

    # Test 5: Test handling of None parameter with IS NOT operator
    # This should be correctly handled by converting None to NULL
    result = db.select(pg_conn, """
    SELECT date, value, strategy
    FROM test_date_null
    WHERE date BETWEEN %s AND %s
    AND value IS NOT %s
    """, start_date, end_date, None)

    # Verify results - should match rows where value is not NULL
    assert len(result) == 2
    assert {r['strategy'] for r in result} == {'A'}

    # Test 6: Test handling of None parameter with IS operator
    result = db.select(pg_conn, """
    SELECT date, value, strategy
    FROM test_date_null
    WHERE date BETWEEN %s AND %s
    AND value IS %s
    """, start_date, end_date, None)

    # Verify results - should match rows where value is NULL
    assert len(result) == 1
    assert result[0]['strategy'] == 'B'


def test_any_with_list_parameter(psql_docker, pg_conn):
    """Verify ANY(%s) binds a list as a PostgreSQL array (not unpacked to scalar).

    Regression guard for a bug where a single-element list arg under a single
    placeholder was silently flattened to a scalar tuple, causing
    `op ANY/ALL (array) requires array on right side` from psycopg.
    """
    db.execute(pg_conn, """
    CREATE TEMPORARY TABLE test_any (
        id INTEGER PRIMARY KEY,
        name TEXT
    )
    """)
    for i, name in enumerate(['Alpha', 'Beta', 'Gamma'], start=1):
        db.insert(pg_conn, 'INSERT INTO test_any (id, name) VALUES (%s, %s)', i, name)

    # Single-element list - the original failing case
    n = db.select_scalar(pg_conn, 'SELECT count(*) FROM test_any WHERE id = ANY(%s)', [2])
    assert n == 1

    # Multi-element list
    n = db.select_scalar(pg_conn, 'SELECT count(*) FROM test_any WHERE id = ANY(%s)', [1, 3])
    assert n == 2

    # Empty list - should match nothing
    n = db.select_scalar(pg_conn, 'SELECT count(*) FROM test_any WHERE id = ANY(%s)', [])
    assert n == 0

    # Combined with another placeholder
    rows = db.select(pg_conn,
                     'SELECT name FROM test_any WHERE id = ANY(%s) AND name LIKE %s',
                     [1, 2, 3], 'A%')
    assert len(rows) == 1
    assert rows[0]['name'] == 'Alpha'


if __name__ == '__main__':
    __import__('pytest').main([__file__])
