"""
Database-agnostic tests for parameter handling — IN clause expansion,
named params, list params, and the "ignore extra args" path.

These cases exercise the library's placeholder logic, which lives above
the dialect layer and behaves identically on both backends.
"""
import database as db


def _temp_table_ddl(dialect, name, columns):
    """Return CREATE TEMPORARY TABLE DDL appropriate to the dialect.

    Both backends support CREATE TEMPORARY TABLE; this helper just abstracts
    the SERIAL/AUTOINCREMENT and VARCHAR/TEXT differences.
    """
    return f'CREATE TEMPORARY TABLE {name} ({columns})'


def test_list_parameters(db_conn):
    """A flat list of args under a parenthesized IN clause expands one
    placeholder per arg.
    """
    names = ['InTest1', 'InTest2', 'InTest3']
    for i, name in enumerate(names):
        db.execute(db_conn, 'INSERT INTO test_table (name, value) VALUES (%s, %s)',
                   name, (i + 1) * 10)

    placeholders = ','.join(['%s'] * len(names))
    query = f'SELECT name, value FROM test_table WHERE name IN ({placeholders}) ORDER BY value'

    result = db.select_column(db_conn, query.replace('SELECT name, value', 'SELECT name'),
                              *names)
    assert result == names


def test_direct_list_parameters(db_conn):
    """`IN %s` accepts a flat list and expands it in-place.

    Two equivalent shapes — flat `[1,2,3]` and wrapped `([1,2,3],)` — must
    behave identically.
    """
    test_values = [101, 102, 103]
    for i, v in enumerate(test_values):
        db.execute(db_conn, 'INSERT INTO test_table (name, value) VALUES (%s, %s)',
                   f'DirectTest{i}', v)

    values_back = db.select_column(
        db_conn,
        'SELECT value FROM test_table WHERE value IN %s ORDER BY value',
        test_values,
    )
    assert values_back == test_values

    single = db.select_column(
        db_conn,
        'SELECT value FROM test_table WHERE value IN %s',
        [101],
    )
    assert single == [101]

    wrapped = db.select_column(
        db_conn,
        'SELECT value FROM test_table WHERE value IN %s',
        ([101],),
    )
    assert wrapped == [101]


def test_direct_lists_for_multiple_in_clauses(db_conn, dialect):
    """Two IN clauses in one query, each fed by a direct list."""
    if dialect == 'postgresql':
        ddl = _temp_table_ddl(dialect, 'multi_in_test',
                              'id SERIAL PRIMARY KEY, category TEXT, status TEXT')
    else:
        ddl = _temp_table_ddl(dialect, 'multi_in_test',
                              'id INTEGER PRIMARY KEY, category TEXT, status TEXT')
    db.execute(db_conn, ddl)

    categories = ['cat1', 'cat2', 'cat3']
    statuses = ['active', 'pending']
    for cat in categories:
        for status in statuses:
            db.execute(db_conn,
                       'INSERT INTO multi_in_test (category, status) VALUES (%s, %s)',
                       cat, status)

    cats = db.select_column(
        db_conn,
        'SELECT category FROM multi_in_test WHERE category IN %s AND status IN %s '
        'ORDER BY category, status',
        ['cat1', 'cat2'], ['active', 'pending'],
    )
    assert len(cats) == 4  # 2 categories x 2 statuses
    assert set(cats) == {'cat1', 'cat2'}


def test_named_params_in_clause(db_conn, dialect):
    """Named-parameter IN-clause expansion must work on both backends.

    Library expands 'IN %(items)s' to '(%(items_0)s, %(items_1)s, ...)'.
    On postgres that stays pyformat. On sqlite the library must translate
    each expanded placeholder to ':items_0', ':items_1', ... so sqlite3
    accepts them.
    """
    if dialect == 'postgresql':
        ddl = _temp_table_ddl(dialect, 'named_in_test',
                              'id SERIAL PRIMARY KEY, category TEXT, vendor TEXT, '
                              'description TEXT')
    else:
        ddl = _temp_table_ddl(dialect, 'named_in_test',
                              'id INTEGER PRIMARY KEY, category TEXT, vendor TEXT, '
                              'description TEXT')
    db.execute(db_conn, ddl)
    for cat, vendor, desc in [
        ('Electronics', 'Apple', 'Smartphone'),
        ('Electronics', 'Samsung', 'Tablet'),
        ('Clothing', 'Nike', 'Running shoes'),
    ]:
        db.execute(db_conn,
                   'INSERT INTO named_in_test (category, vendor, description) '
                   'VALUES (%s, %s, %s)', cat, vendor, desc)

    query = """
        SELECT DISTINCT category, description
        FROM named_in_test
        WHERE category IN %(categories)s
        AND vendor = %(vendor)s
    """

    single = db.select(db_conn, query, {'categories': ('Electronics',), 'vendor': 'Apple'})
    assert len(single) == 1

    multi = db.select(db_conn, query,
                      {'categories': ('Electronics', 'Clothing'), 'vendor': 'Nike'})
    assert len(multi) == 1

    none = db.select(db_conn, query, {'categories': ('Books',), 'vendor': 'Apple'})
    assert len(none) == 0


def test_no_placeholders_with_extra_args(db_conn, dialect):
    """A SQL with zero placeholders must ignore any extra positional args.

    This protects accidental-arg-passing from blowing up obviously-correct
    static SQL.
    """
    if dialect == 'postgresql':
        ddl = _temp_table_ddl(dialect, 'no_ph_test', 'id SERIAL PRIMARY KEY, name TEXT')
    else:
        ddl = _temp_table_ddl(dialect, 'no_ph_test',
                              'id INTEGER PRIMARY KEY, name TEXT')
    db.execute(db_conn, ddl)
    db.execute(db_conn, "INSERT INTO no_ph_test (name) VALUES ('a'), ('b')")

    names = db.select_column(db_conn, 'SELECT name FROM no_ph_test ORDER BY name',
                             'ignored', 123, True)
    assert names == ['a', 'b']

    with db.transaction(db_conn) as tx:
        count = tx.select_scalar('SELECT COUNT(*) FROM no_ph_test', 'ignored_param')
        assert count == 2


if __name__ == '__main__':
    __import__('pytest').main([__file__])
