"""Placeholder-expansion tests run against PostgreSQL and SQLite.
"""
import database as db
from tests.integration.common.conftest import col


def test_list_parameters(db_conn):
    """Verify a parenthesized in list binds one positional argument per %s.

    Mutation: packing the arguments into one tuple for the first %s.
    Oracle: three inserted names, read back in value order.
    """
    names = ['InTest1', 'InTest2', 'InTest3']
    for i, name in enumerate(names):
        db.execute(
            db_conn,
            'insert into test_table (name, value) values (%s, %s)',
            name, (i + 1) * 10)

    result = db.select_column(
        db_conn,
        'select name from test_table where name in (%s, %s, %s) order by value',
        *names)
    assert result == names


def test_direct_list_parameters(db_conn):
    """Verify in %s expands a flat list, a one-item list, and a wrapped list.

    Mutation: binding the list as one array, or unwrapping only the flat form.
    Oracle: the three inserted values, and 101 alone for the one-item forms.
    """
    test_values = [101, 102, 103]
    for i, value in enumerate(test_values):
        db.execute(
            db_conn,
            'insert into test_table (name, value) values (%s, %s)',
            f'DirectTest{i}', value)

    values_back = db.select_column(
        db_conn,
        'select value from test_table where value in %s order by value',
        test_values)
    assert values_back == test_values

    single = db.select_column(
        db_conn, 'select value from test_table where value in %s', [101])
    assert single == [101]

    wrapped = db.select_column(
        db_conn, 'select value from test_table where value in %s', ([101],))
    assert wrapped == [101]


def test_direct_lists_for_multiple_in_clauses(db_conn):
    """Verify two in %s clauses each expand their own list.

    Mutation: expanding the first list into both clauses.
    Oracle: two categories times two statuses, sorted by hand.
    """
    db.execute(
        db_conn, 'create temporary table multi_in_test (category text, status text)')
    for category in ['cat1', 'cat2', 'cat3']:
        for status in ['active', 'pending']:
            db.execute(
                db_conn,
                'insert into multi_in_test (category, status) values (%s, %s)',
                category, status)

    cats = db.select_column(
        db_conn,
        'select category from multi_in_test where category in %s and status in %s '
        'order by category, status',
        ['cat1', 'cat2'], ['active', 'pending'])
    assert cats == ['cat1', 'cat1', 'cat2', 'cat2']


def test_named_params_in_clause(db_conn):
    """Verify in %(name)s expands a tuple, one-item tuples included.

    Mutation: binding a one-item tuple as a scalar, or leaving the
        expanded %(items_0)s names in pyformat on SQLite.
    Oracle: three hand-inserted products; each lookup names one vendor.
    """
    db.execute(
        db_conn,
        'create temporary table named_in_test (category text, vendor text, description text)')
    products = [
        ('Electronics', 'Apple', 'Smartphone'),
        ('Electronics', 'Samsung', 'Tablet'),
        ('Clothing', 'Nike', 'Running shoes'),
        ]
    for category, vendor, description in products:
        db.execute(
            db_conn,
            'insert into named_in_test (category, vendor, description) values (%s, %s, %s)',
            category, vendor, description)

    query = """
select distinct category, description
from named_in_test
where category in %(categories)s
and vendor = %(vendor)s
"""

    single = db.select(
        db_conn, query, {'categories': ('Electronics',), 'vendor': 'Apple'})
    assert col(single, 'description') == ['Smartphone']

    multi = db.select(
        db_conn, query, {'categories': ('Electronics', 'Clothing'), 'vendor': 'Nike'})
    assert col(multi, 'description') == ['Running shoes']

    none = db.select(db_conn, query, {'categories': ('Books',), 'vendor': 'Apple'})
    assert len(none) == 0


def test_no_placeholders_with_extra_args(db_conn):
    """Verify SQL with no placeholder ignores any extra positional arguments.

    Mutation: passing the arguments to the driver, which raises.
    Oracle: two hand-inserted names and their count.
    """
    db.execute(db_conn, 'create temporary table no_ph_test (name text)')
    db.execute(db_conn, "insert into no_ph_test (name) values ('a'), ('b')")

    names = db.select_column(
        db_conn, 'select name from no_ph_test order by name', 'ignored', 123, True)
    assert names == ['a', 'b']

    with db.transaction(db_conn) as tx:
        assert tx.select_scalar('select count(*) from no_ph_test', 'ignored_param') == 2


if __name__ == '__main__':
    __import__('pytest').main([__file__])
