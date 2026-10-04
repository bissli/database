"""PostgreSQL-only parameter-binding tests.
"""
import datetime

import database as db
import pytest


def test_date_parameter_handling(psql_docker, pg_conn):
    """Verify a date argument matches a date column, inside a transaction too.

    Mutation: binding the date as a value the date column does not equal.
    Oracle: the one hand-inserted row for that date and identifier.
    """
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
    db.insert(
        pg_conn,
        'insert into test_date_table (date, identifier, value) values (%s, %s, %s)',
        test_date, 'TEST123', 100)

    query = """
select date, identifier, value
from test_date_table
where date = %s
    and identifier = %s
    and duplicate_see_id is null
"""
    result = db.select(pg_conn, query, test_date, 'TEST123')
    assert result == [{'date': test_date, 'identifier': 'TEST123', 'value': 100}]

    with db.transaction(pg_conn) as tx:
        result = tx.select(query, test_date, 'TEST123')
        assert [r['identifier'] for r in result] == ['TEST123']


def test_named_parameter_handling(psql_docker, pg_conn):
    """Verify named parameters bind by name, inside a transaction too.

    Mutation: binding named parameters in dict order, swapping pdate and bdate.
    Oracle: the join row matches only pdate, the main row only bdate.
    """
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
    db.insert(
        pg_conn,
        'insert into test_named_params (col, time, value) values (%s, %s, %s)',
        'TEST123', '2025-03-04 10:00:00', 200)
    db.insert(
        pg_conn,
        'insert into test_named_params_join (id_bb_unique, date, data) values (%s, %s, %s)',
        'TEST123', '2025-03-03', 'test data')

    query = """
select q.col, q.value, bu.data
from test_named_params q
left join test_named_params_join bu
    on bu.id_bb_unique = q.col and bu.date = %(pdate)s
where q.time::date = %(bdate)s
"""
    params = {'pdate': datetime.date(2025, 3, 3), 'bdate': datetime.date(2025, 3, 4)}
    expected = [{'col': 'TEST123', 'value': 200, 'data': 'test data'}]

    assert db.select(pg_conn, query, params) == expected
    with db.transaction(pg_conn) as tx:
        assert tx.select(query, params) == expected


def test_none_parameter(psql_docker, pg_conn):
    """Verify a None argument binds as SQL null.

    Mutation: binding None as the string 'None'.
    Oracle: a null cast to text reads back as None.
    """
    result = db.select(pg_conn, 'select %s::text as null_value', None)
    assert result[0]['null_value'] is None


def test_numeric_parameters(psql_docker, pg_conn):
    """Verify int and float arguments bind with their values intact.

    Mutation: binding numbers as text, or truncating a float.
    Oracle: hand-computed 42, 42.5, and 10 + 20.
    """
    assert db.select(pg_conn, 'select %s::int as int_val', 42)[0]['int_val'] == 42
    assert db.select(
        pg_conn, 'select %s::float as float_val', 42.5)[0]['float_val'] == 42.5
    assert db.select(pg_conn, 'select %s + %s as sum_val', 10, 20)[0]['sum_val'] == 30


def test_like_clause_with_pre_escaped_percent(psql_docker, pg_conn):
    """Verify literal percent signs survive, with and without bound arguments.

    Mutation: escaping a literal % when nothing is bound, or leaving it
        unescaped beside a %s.
    Oracle: six hand-inserted statuses; a bound %% stores as two characters.
    """
    db.execute(pg_conn, """
create temporary table like_pattern_test (
    id serial primary key,
    status text,
    description text
)
""")
    test_data = [
        ('%%Saved', 'Double-percent followed by Saved'),
        ('Not%%Saved', 'Double-percent in the middle'),
        ('%Saved', 'Single-percent at start'),
        ('Saved%', 'Single-percent at end'),
        ('%%S%%aved', 'Multiple double-percents'),
        ('Regular', 'No percent signs'),
        ]
    for status, description in test_data:
        db.insert(
            pg_conn,
            'insert into like_pattern_test (status, description) values (%s, %s)',
            status, description)

    exact = db.select_column(
        pg_conn, "select status from like_pattern_test where status = '%Saved'")
    assert exact == ['%Saved']

    bound = db.select_column(
        pg_conn,
        "select status from like_pattern_test where status like '%S%' and id > %s order by id",
        1)
    assert bound == ['Not%%Saved', '%Saved', 'Saved%', '%%S%%aved']

    with db.transaction(pg_conn) as tx:
        result = tx.select(
            "select status from like_pattern_test where status like '%%S%%' order by id")
        assert [r['status'] for r in result] == [
            '%%Saved', 'Not%%Saved', '%Saved', 'Saved%', '%%S%%aved',
            ]


def test_combined_in_clause_named_params_with_returnid(psql_docker, pg_conn):
    """Verify a named in tuple and returnid work together in one insert.

    Mutation: returnid keeping only the first row, or the tuple as one value.
    Oracle: two of the three seeded categories fall inside the in tuple.
    """
    db.execute(pg_conn, """
create temporary table combined_test (
    id serial primary key,
    category text not null,
    name text not null,
    value integer not null
)
""")
    for i, category in enumerate(['electronics', 'clothing', 'food']):
        db.execute(
            pg_conn,
            'insert into combined_test (category, name, value) values (%s, %s, %s)',
            category, f'Item_{i}', i * 10)

    insert_sql = """
insert into combined_test (category, name, value)
select
    category,
    %(name)s as name,
    %(value)s as value
from combined_test
where category in %(categories)s
returning id, category
"""
    params = {
        'categories': ('electronics', 'clothing'),
        'name': 'Combined_Test_Item',
        'value': 500,
        }
    with db.transaction(pg_conn) as tx:
        result_ids = tx.execute(insert_sql, params, returnid=['id', 'category'])

        assert isinstance(result_ids, list)
        ids, categories = zip(*result_ids)
        assert all(isinstance(id_val, int) for id_val in ids)
        assert sorted(categories) == ['clothing', 'electronics']

        for id_val in ids:
            row = tx.select_row('select * from combined_test where id = %s', id_val)
            assert row.name == 'Combined_Test_Item'
            assert row.value == 500
            assert row.category in {'electronics', 'clothing'}


def test_is_null_parameter_handling(psql_docker, pg_conn):
    """Verify is %s with None becomes is null, leaving literal is null alone.

    Mutation: binding None after is, or rewriting a literal is null.
    Oracle: three hand-inserted rows, one with a null value.
    """
    db.execute(pg_conn, """
create temporary table test_date_null (
    id serial primary key,
    date date,
    value integer,
    strategy text
)
""")
    db.execute(pg_conn, """
insert into test_date_null (date, value, strategy) values
('2025-01-15', 100, 'A'),
('2025-02-01', null, 'B'),
('2025-03-01', 300, 'A')
""")
    start_date = datetime.date(2025, 1, 1)
    end_date = datetime.date(2025, 3, 11)

    literal_not_null = db.select_column(pg_conn, """
select value
from test_date_null
where date between %s and %s
and value is not null
order by value
""", start_date, end_date)
    assert literal_not_null == [100, 300]

    value_or_null = db.select_column(pg_conn, """
select strategy
from test_date_null
where value = %s or value is null
order by strategy
""", 100)
    assert value_or_null == ['A', 'B']

    bound_is_not = db.select_column(pg_conn, """
select value
from test_date_null
where date between %s and %s
and value is not %s
order by value
""", start_date, end_date, None)
    assert bound_is_not == [100, 300]

    bound_is = db.select_column(pg_conn, """
select strategy
from test_date_null
where date between %s and %s
and value is %s
""", start_date, end_date, None)
    assert bound_is == ['B']


def test_any_with_list_parameter(psql_docker, pg_conn):
    """Verify any(%s) binds a list as one array, a one-item list included.

    Mutation: flattening a one-item list to a scalar.
    Oracle: three hand-inserted ids; counts for [2], [1, 3], and [].
    """
    db.execute(
        pg_conn, 'create temporary table test_any (id integer primary key, name text)')
    for i, name in enumerate(['Alpha', 'Beta', 'Gamma'], start=1):
        db.insert(pg_conn, 'insert into test_any (id, name) values (%s, %s)', i, name)

    count_sql = 'select count(*) from test_any where id = any(%s)'
    assert db.select_scalar(pg_conn, count_sql, [2]) == 1
    assert db.select_scalar(pg_conn, count_sql, [1, 3]) == 2
    assert db.select_scalar(pg_conn, count_sql, []) == 0

    names = db.select_column(
        pg_conn,
        'select name from test_any where id = any(%s) and name like %s',
        [1, 2, 3],
        'A%')
    assert names == ['Alpha']


@pytest.mark.parametrize(('pattern', 'expected'), [
    ('[0-9]%d', 'abcd'),
    ('[0-9]%', 'abdcd'),
    ('c?d$', 'ab7%d'),
    ], ids=['percent-d', 'lone-percent', 'question-mark'])
def test_regexp_replace_pattern_beside_a_bound_argument(
        psql_docker, pg_conn, pattern, expected):
    """Verify a regexp_replace pattern with '%' or '?' runs beside a bound arg.

    Mutation: exempting regexp_replace from percent doubling, so psycopg
              reads '%d' or a lone '%' as a placeholder and raises; or
              the '?' in the pattern turned into a placeholder.
    Oracle: the hand-applied regex on the one stored name 'ab7%dcd'.
    """
    db.execute(pg_conn, 'create temporary table regexp_test (id int, name text)')
    db.insert(pg_conn, 'insert into regexp_test values (%s, %s)', 1, 'ab7%dcd')

    sql = f"select regexp_replace(name, '{pattern}', '') from regexp_test where id = %s"
    assert db.select_column(pg_conn, sql, 1) == [expected]


def test_like_literal_starting_with_s_beside_a_bound_argument(
        psql_docker, pg_conn):
    """Verify like '%smith%' matches beside a bound arg instead of raising.

    Mutation: '%s' left undoubled inside a literal, so psycopg counts it
              as a second placeholder for one argument.
    Oracle: the one stored name 'jsmithe' matches '%smith%'.
    """
    db.execute(pg_conn, 'create temporary table like_test (id int, name text)')
    db.insert(pg_conn, 'insert into like_test values (%s, %s)', 1, 'jsmithe')

    sql = "select name from like_test where name like '%smith%' and id = %s"
    assert db.select_column(pg_conn, sql, 1) == ['jsmithe']


@pytest.mark.parametrize('literal', ['a%(b', '50%', 'a%sb'])
def test_percent_literal_survives_an_inlined_placeholder(
        psql_docker, pg_conn, literal):
    """Verify a '%' literal reads back as written once 'is not %s' inlines.

    Mutation: the no-placeholder branch of Cursor._execute_query running
              the doubled text raw, so '%%' reaches the server.
    Oracle: the literal as written; no bound arg remains to collapse '%%'.
    """
    sql = f"select '{literal}' as x where 1 is not %s"

    assert db.select_column(pg_conn, sql, None) == [literal]


def test_multi_statement_skips_a_placeholder_lookalike_in_a_literal(
        psql_docker, pg_conn):
    """Verify a literal '%s' in one statement does not claim a bound arg.

    Mutation: counting placeholders on the raw statement text instead of
              the masked text, so the count is 2 for one arg and raises.
    Oracle: the hand-written rows ('q%s', 0) and ('r', 7).
    """
    db.execute(pg_conn, 'create temporary table multi_pct (s text, n int)')
    db.execute(
        pg_conn,
        "insert into multi_pct values ('q%s', 0); "
        "insert into multi_pct values ('r', %s)",
        7)

    rows = db.select(pg_conn, 'select s, n from multi_pct order by s')
    assert [(row['s'], row['n']) for row in rows] == [('q%s', 0), ('r', 7)]


def test_double_percent_reads_as_one_with_or_without_args(psql_docker, pg_conn):
    """Verify '%%' in a literal means '%' whether or not the call binds args.

    Mutation: Cursor._execute_query sending a no-args statement raw, or
        _execute_without_params dropping its '%%' collapse.
    Oracle: the hand-written literal 'a%b'.
    """
    assert db.select_scalar(pg_conn, "select 'a%%b'") == 'a%b'
    assert db.select_scalar(pg_conn, "select 'a%%b' || %s", 'x') == 'a%bx'


def test_lone_percent_beside_a_colon_literal_runs_without_args(psql_docker, pg_conn):
    """Verify a ':30' in a literal does not send a lone '%' through psycopg.

    Mutation: Cursor._execute_simple passing an empty arg tuple to psycopg.
    Oracle: the hand-written literal 'a%b'.
    """
    sql = "select 'a%b' where '10:30' > ''"

    assert db.select_scalar(pg_conn, sql) == 'a%b'


def test_percent_in_a_comment_and_a_modulo_bind_with_args(psql_docker, pg_conn):
    """Verify a '%' outside any literal survives a call that binds args.

    Mutation: _escape_percents limited to string literals.
    Oracle: 7 mod 3 is 1, and the bound value as passed.
    """
    sql = """
select 7 % 3 as r, %s as p -- 50% done
"""
    row = db.select_row(pg_conn, sql, 'x')

    assert (row.r, row.p) == (1, 'x')


def test_escaped_quote_in_an_e_string_keeps_the_next_placeholder(psql_docker, pg_conn):
    """Verify a backslash-escaped quote in E'' does not hide a later '%s'.

    Mutation: _protected_ranges ignoring the backslash escape in E''.
    Oracle: the hand-written literal and the bound value.
    """
    row = db.select_row(pg_conn, "select E'it\\'s 50%' as s, %s as p", 'x')

    assert (row.s, row.p) == ("it's 50%", 'x')


def test_odd_percent_run_reads_the_same_with_or_without_args(psql_docker, pg_conn):
    """Verify '%%%' reads back alike whether or not the call binds args.

    Mutation: _escape_percents doubling only a lone '%'.
    Oracle: the no-args result of the same literal, '%%'.
    """
    assert db.select_scalar(pg_conn, "select '%%%'") == '%%'
    assert db.select_scalar(pg_conn, "select '%%%' || %s", 'x') == '%%x'


def test_dict_args_that_all_inline_run_without_params(psql_docker, pg_conn):
    """Verify an inlined named arg leaves no empty dict for psycopg.

    Mutation: Cursor._execute_simple passing an empty dict to psycopg.
    Oracle: the hand-written literal '5%'; the JSONB '?' and the inlined
        null leave no placeholder.
    """
    sql = """select '5%' where '{"k": 1}'::jsonb ? 'k' and 1 is not %(a)s"""

    assert db.select_scalar(pg_conn, sql, {'a': None}) == '5%'


if __name__ == '__main__':
    __import__('pytest').main([__file__])
