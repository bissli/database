"""PostgreSQL-only client tests.
"""
import datetime

import database as db
import pytest

STAGED_ROWS = [
    {'name': 'Alice', 'value': 10},
    {'name': 'Bob', 'value': 20},
    {'name': 'Charlie', 'value': 30},
    {'name': 'Ethan', 'value': 50},
    {'name': 'Fiona', 'value': 70},
    {'name': 'George', 'value': 80},
    ]

DAY = datetime.date(2025, 1, 2)
LATER_DAY = datetime.date(2025, 2, 1)
SEED_STATE = [('a', 1, None), ('b', 2, None), ('c', 3, None)]


def test_select(psql_docker, pg_conn):
    """Verify select returns every row as a dict under the iterdict loader.

    Mutation: a loader returning tuples, or dropping a row.
    Oracle: the six rows stage_test_data writes.
    """
    result = db.select(pg_conn, 'select name, value from test_table order by value')
    assert result == STAGED_ROWS
    assert all(isinstance(row, dict) for row in result)


def test_select_numeric(psql_docker, pg_conn):
    """Verify a numeric column loads as float.

    Mutation: dropping the numeric adapter, which returns Decimal.
    Oracle: the staged rows, each value cast to numeric.
    """
    result = db.select(
        pg_conn, 'select name, value::numeric as value from test_table order by value')
    assert result == STAGED_ROWS
    assert all(isinstance(row['value'], float) for row in result)


def test_insert(psql_docker, pg_conn):
    """Verify insert returns the affected row count and writes the row.

    Mutation: insert returning the cursor or None in place of rowcount.
    Oracle: one hand-written row.
    """
    row_count = db.insert(
        pg_conn, 'insert into test_table (name, value) values (%s, %s)', 'Diana', 40)
    assert row_count == 1

    result = db.select(
        pg_conn, "select name, value from test_table where name = 'Diana'")
    assert result == [{'name': 'Diana', 'value': 40}]


def test_update(psql_docker, pg_conn):
    """Verify update returns the affected row count and changes the row.

    Mutation: update returning the cursor or None in place of rowcount.
    Oracle: Ethan is staged with 50; the update sets 60.
    """
    row_count = db.update(
        pg_conn, 'update test_table set value = %s where name = %s', 60, 'Ethan')
    assert row_count == 1

    result = db.select(
        pg_conn, "select name, value from test_table where name = 'Ethan'")
    assert result == [{'name': 'Ethan', 'value': 60}]


def test_delete(psql_docker, pg_conn):
    """Verify delete returns the affected row count and removes the row.

    Mutation: delete returning the cursor or None in place of rowcount.
    Oracle: Fiona is staged once.
    """
    row_count = db.delete(pg_conn, 'delete from test_table where name = %s', 'Fiona')
    assert row_count == 1

    result = db.select(
        pg_conn, "select name, value from test_table where name = 'Fiona'")
    assert len(result) == 0


def test_insert_rows_bulk(psql_docker, pg_conn):
    """Verify insert_rows writes a thousand rows and counts them.

    Mutation: returning the batch count, or dropping rows past one batch.
    Oracle: 1000 hand-built rows; three sampled values of i * 1.5.
    """
    db.execute(pg_conn, """
create temporary table bulk_test (
    id serial primary key,
    name text not null,
    value numeric,
    date date
)
""")
    num_rows = 1000
    base_date = datetime.date(2025, 1, 1)
    test_rows = [
        {
            'name': f'Bulk-{i}',
            'value': float(i * 1.5),
            'date': base_date + datetime.timedelta(days=i % 365),
            }
        for i in range(num_rows)
        ]
    assert db.insert_rows(pg_conn, 'bulk_test', test_rows) == num_rows

    for i in [0, 42, 999]:
        row = db.select_row(
            pg_conn, 'select * from bulk_test where name = %s', f'Bulk-{i}')
        assert row.value == float(i * 1.5)


def test_cte_query(psql_docker, pg_conn):
    """Verify select returns the rows of a CTE query.

    Mutation: treating only a leading select as row-returning.
    Oracle: Fiona 70 and George 80 are the staged values above 50.
    """
    cte_query = """
with highvalue as (
    select name, value
    from test_table
    where value > 50
)
select name, value from highvalue order by value desc
"""
    result = db.select(pg_conn, cte_query)
    assert result == [{'name': 'George', 'value': 80}, {'name': 'Fiona', 'value': 70}]


def test_multiple_statements_with_semicolon(psql_docker, pg_conn):
    """Verify execute runs every statement of a semicolon-separated batch.

    Mutation: running one statement only, or failing on a trailing semicolon.
    Oracle: two inserts and an update; then one insert ending in a semicolon.
    """
    db.execute(pg_conn, """
create temporary table multi_statement_test (
    id serial primary key,
    name text not null,
    score integer not null
)
""")
    db.execute(pg_conn, """
insert into multi_statement_test (name, score) values ('alpha', 100);
insert into multi_statement_test (name, score) values ('beta', 200);
update multi_statement_test set score = 150 where name = 'alpha'
""")
    result = db.select(
        pg_conn, 'select name, score from multi_statement_test order by name')
    assert result == [{'name': 'alpha', 'score': 150}, {'name': 'beta', 'score': 200}]

    db.execute(pg_conn, """
insert into multi_statement_test (name, score) values ('gamma', 300);
""")
    gamma = db.select_row(
        pg_conn, "select * from multi_statement_test where name = 'gamma'")
    assert gamma.score == 300


def test_multiple_statements_with_delete(psql_docker, pg_conn):
    """Verify one execute runs an insert, an update, and a delete in order.

    Mutation: dropping the last statement, or running them out of order.
    Oracle: five seeded items and the hand-traced result of the batch.
    """
    db.execute(pg_conn, """
create temporary table delete_test (
    id serial primary key,
    name text not null,
    status text not null
)
""")
    db.execute(pg_conn, """
insert into delete_test (name, status) values ('item1', 'active');
insert into delete_test (name, status) values ('item2', 'inactive');
insert into delete_test (name, status) values ('item3', 'active');
insert into delete_test (name, status) values ('item4', 'pending');
insert into delete_test (name, status) values ('item5', 'inactive')
""")
    db.execute(pg_conn, """
insert into delete_test (name, status) values ('item6', 'active');
update delete_test set status = 'archived' where status = 'inactive';
delete from delete_test where status = 'pending'
""")
    result = db.select(pg_conn, 'select name, status from delete_test order by name')
    assert {row['name']: row['status'] for row in result} == {
        'item1': 'active',
        'item2': 'archived',
        'item3': 'active',
        'item5': 'archived',
        'item6': 'active',
        }


def test_multiple_statements_with_complex_updates(psql_docker, pg_conn):
    """Verify a batch with subqueries and blank lines splits correctly.

    Mutation: splitting inside the subquery, or losing the statement after
        a blank line.
    Oracle: 1001 links to itself, NonFiction takes 999, Reference is added.
    """
    db.execute(pg_conn, """
create temporary table product_test (
    id serial primary key,
    category text not null,
    sub_category text,
    related_id integer,
    product_code text
)
""")
    db.execute(pg_conn, """
insert into product_test (category, sub_category, related_id, product_code) values
('Book', 'Fiction', null, '1001'),
('Book', 'NonFiction', null, '2001'),
('Book', 'Fiction', null, '1002'),
('Magazine', 'Fiction', null, '1002'),
('DVD', 'Movie', null, '2001')
""")
    db.execute(pg_conn, """
insert into product_test (category, sub_category, related_id, product_code)
values ('Book', 'Reference', null, '3001');

update product_test p
set related_id = x.ref_id
from (
    select
        p.id as ref_id,
        p.product_code
    from product_test p
    where
        p.sub_category = 'Fiction'
        and p.related_id is null
        and p.product_code is not null
        and p.category in ('Book', 'Magazine')
) x
where p.product_code = x.product_code;

update product_test p
set related_id = 999
where p.sub_category = 'NonFiction'
and p.related_id is null
""")
    linked = db.select_row(
        pg_conn, "select id, related_id from product_test where product_code = '1001'")
    assert linked.related_id == linked.id

    nonfiction = db.select_column(
        pg_conn,
        "select related_id from product_test where sub_category = 'NonFiction'")
    assert nonfiction == [999]

    reference = db.select_row(pg_conn, """
select category, product_code
from product_test
where sub_category = 'Reference'
""")
    assert (reference.category, reference.product_code) == ('Book', '3001')


@pytest.mark.parametrize(('sql', 'args', 'expected'), [
    pytest.param("""
insert into stmt_probe (name, value, day) values ('d', 10, %s);
insert into stmt_probe (name, value, day) values ('e', 20, %s);
update stmt_probe set value = 15 where name = 'd';
""", (DAY, LATER_DAY), [*SEED_STATE, ('d', 15, DAY), ('e', 20, LATER_DAY)],
        id='positional-in-first-two-of-three'),
    pytest.param("""
insert into stmt_probe (name, value, day) values ('d', %(v1)s, %(day)s);
insert into stmt_probe (name, value, day) values ('e', %(v2)s, %(day)s);
update stmt_probe set value = %(v3)s where name = 'd';
""", ({'v1': 10, 'v2': 20, 'day': DAY, 'v3': 15},),
        [*SEED_STATE, ('d', 15, DAY), ('e', 20, DAY)],
        id='named-reused-across-statements'),
    pytest.param("""
update stmt_probe set value = 0 where name = 'a';
update stmt_probe set day = %s where name = 'b';
update stmt_probe set day = %s where name = 'c';
""", (DAY, LATER_DAY), [('a', 0, None), ('b', 2, DAY), ('c', 3, LATER_DAY)],
        id='positional-in-last-two-of-three'),
    pytest.param("""
update stmt_probe set value = 0 where name = 'a';
update stmt_probe set day = %(first)s where name = 'b';
update stmt_probe set day = %(second)s where name = 'c';
""", ({'first': DAY, 'second': LATER_DAY},),
        [('a', 0, None), ('b', 2, DAY), ('c', 3, LATER_DAY)],
        id='named-in-last-two-of-three'),
    pytest.param("""
update stmt_probe set value = %s, day = %s where name = 'a';
update stmt_probe set value = %s, day = %s where name = 'b'
""", (10, DAY, 20, LATER_DAY), [('a', 10, DAY), ('b', 20, LATER_DAY), ('c', 3, None)],
        id='two-in-every-statement'),
    pytest.param("""
update stmt_probe set value = 101 where name = 'a';
update stmt_probe set value = %s where name = 'b';
update stmt_probe set value = 103 where name = 'c';
""", (102,), [('a', 101, None), ('b', 102, None), ('c', 103, None)],
        id='positional-in-middle-only'),
    pytest.param("""
update stmt_probe set value = %s where name = 'a';
delete from stmt_probe where name = 'b';
update stmt_probe set value = 303 where name = 'c';
""", (301,), [('a', 301, None), ('c', 303, None)],
        id='positional-in-first-only-with-delete'),
    pytest.param("""
insert into stmt_probe (name, value) values ('d', 4001);
insert into stmt_probe (name, value) values ('e', %s);
update stmt_probe set value = value + 10 where name in ('d', 'e');
delete from stmt_probe where name = 'd';
update stmt_probe set day = %s where name = 'e';
""", (4002, DAY), [*SEED_STATE, ('e', 4012, DAY)],
        id='positional-in-second-and-fifth-of-five'),
    ])
def test_multiple_statements_bind_args_to_their_statements(
        psql_docker, pg_conn, sql, args, expected):
    """Verify each statement in a batch binds its own arguments.

    Mutation: binding all arguments to the first statement, miscounting
        a statement's placeholders, or not sharing a named argument.
    Oracle: the final table state, worked out by hand for each batch.
    """
    db.execute(
        pg_conn,
        'create temporary table stmt_probe (name text primary key, value integer, day date)')
    db.execute(
        pg_conn,
        "insert into stmt_probe (name, value) values ('a', 1), ('b', 2), ('c', 3)")

    db.execute(pg_conn, sql, *args)

    result = db.select(pg_conn, 'select name, value, day from stmt_probe order by name')
    assert [(r['name'], r['value'], r['day']) for r in result] == expected


if __name__ == '__main__':
    __import__('pytest').main([__file__])
