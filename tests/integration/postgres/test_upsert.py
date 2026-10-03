"""PostgreSQL-only upsert_rows tests.
"""
import datetime
import time

import database as db
import pytest


def test_upsert_reset_sequence(psql_docker, pg_conn):
    """Verify reset_sequence=True moves the serial past an explicit id.

    Mutation: upsert_rows skipping reset_table_sequence.
    Oracle: the upsert writes test_id 100; the next default id is 101.
    """
    db.execute(pg_conn, """
create temporary table test_sequence_table (
    test_id serial primary key,
    name varchar(50) unique not null
)
""")
    db.upsert_rows(
        pg_conn, 'test_sequence_table', [{'test_id': 100, 'name': 'Explicit'}],
        reset_sequence=True)
    db.execute(pg_conn, "insert into test_sequence_table (name) values ('Default')")

    default_id = db.select_scalar(
        pg_conn, "select test_id from test_sequence_table where name = 'Default'")
    assert default_id == 101


def test_upsert_large_batch(psql_docker, pg_conn):
    """Verify a batch past the 65535 bind-parameter limit inserts and updates.

    Mutation: executemany sending every row in one statement.
    Oracle: 35000 two-column rows need 70000 parameters.
    """
    db.execute(pg_conn, """
create temporary table test_large_batch (
    id integer primary key,
    value text not null
)
""")
    ids = range(1, 35001)

    start = time.time()
    rows = [{'id': i, 'value': f'value-{i}'} for i in ids]
    assert db.upsert_rows(pg_conn, 'test_large_batch', rows) == 35000
    insert_time = time.time() - start

    stored = db.select_column(pg_conn, 'select value from test_large_batch order by id')
    assert stored == [f'value-{i}' for i in ids]

    start = time.time()
    rows = [{'id': i, 'value': f'updated-{i}'} for i in ids]
    update_count = db.upsert_rows(
        pg_conn, 'test_large_batch', rows, update_cols_always=['value'])
    update_time = time.time() - start
    assert update_count == 35000

    stored = db.select_column(pg_conn, 'select value from test_large_batch order by id')
    assert stored == [f'updated-{i}' for i in ids]

    assert insert_time < 60, f'Insert too slow: {insert_time:.2f}s'
    assert update_time < 60, f'Update too slow: {update_time:.2f}s'


def test_upsert_maps_keys_to_quoted_mixed_case_columns(psql_docker, pg_conn):
    """Verify keys in any case reach quoted columns and unknown keys drop.

    Mutation: case-sensitive key matching, or quoting the key as given.
    Oracle: a hand-written table after five casings and two updates.
    """
    db.execute(pg_conn, """
create temporary table case_test_table (
    "Id" serial primary key,
    "UserName" varchar(50) not null,
    "email" varchar(100) not null,
    "PHONE" varchar(20) null,
    "lastLogin" timestamp null
)
""")
    rows = [
        {'Id': 1, 'UserName': 'user1', 'email': 'user1@example.com',
         'PHONE': '555-1234', 'lastLogin': '2023-01-01'},
        {'ID': 2, 'USERNAME': 'user2', 'EMAIL': 'user2@example.com',
         'PHONE': '555-5678', 'LASTLOGIN': '2023-01-02'},
        {'id': 3, 'username': 'user3', 'email': 'user3@example.com',
         'phone': '555-9012', 'lastlogin': '2023-01-03'},
        {'iD': 4, 'UsErNaMe': 'user4', 'EMail': 'user4@example.com',
         'pHoNe': '555-3456', 'LaStLoGiN': '2023-01-04'},
        {'ID': 5, 'UserNAME': 'user5', 'email': 'user5@example.com',
         'INVALID_COL': 'dropped', 'another_bad': 12345, 'phoneNumber': '555-7890'},
        ]
    for row in rows:
        assert db.upsert_rows(
            pg_conn, 'case_test_table', [row], use_primary_key=True) == 1

    names = db.select_column(
        pg_conn, 'select "UserName" from case_test_table order by "Id"')
    assert names == ['user1', 'user2', 'user3', 'user4', 'user5']

    db.upsert_rows(
        pg_conn,
        'case_test_table',
        [{'ID': 1, 'username': 'user1-updated', 'EMAIL': 'updated1@example.com'}],
        use_primary_key=True,
        update_cols_always=['UserName', 'email'])
    db.upsert_rows(
        pg_conn,
        'case_test_table',
        [
            {'iD': 2, 'username': 'user2-updated', 'EMAIL': 'updated2@example.com'},
            {'Id': 3, 'USERNAME': 'user3-updated', 'email': 'updated3@example.com'},
            ],
        use_primary_key=True,
        update_cols_always=['UserName', 'email'])

    result = db.select(
        pg_conn, 'select "Id", "UserName", "email" from case_test_table order by "Id"')
    assert [(r['Id'], r['UserName'], r['email']) for r in result] == [
        (1, 'user1-updated', 'updated1@example.com'),
        (2, 'user2-updated', 'updated2@example.com'),
        (3, 'user3-updated', 'updated3@example.com'),
        (4, 'user4', 'user4@example.com'),
        (5, 'user5', 'user5@example.com'),
        ]


def test_upsert_conflict_on_primary_key_or_named_unique_constraint(
        psql_docker, pg_conn):
    """Verify constraint_name replaces the primary key as conflict target.

    Mutation: ignoring constraint_name, which raises on the Item2 row.
    Oracle: hand-written table before and after three upserts.
    """
    db.execute(pg_conn, """
create temporary table test_error_handling (
    id integer primary key,
    name varchar(50) unique not null,
    value integer not null
)
""")
    db.execute(
        pg_conn, 'insert into test_error_handling values (1, %s, %s)', 'Item1', 100)
    db.execute(
        pg_conn, 'insert into test_error_handling values (2, %s, %s)', 'Item2', 200)

    db.upsert_rows(
        pg_conn,
        'test_error_handling',
        [{'id': 1, 'name': 'UpdatedItem1', 'value': 150}],
        update_cols_always=['name', 'value'])
    db.upsert_rows(
        pg_conn,
        'test_error_handling',
        [{'id': 3, 'name': 'Item2', 'value': 250}],
        constraint_name='test_error_handling_name_key',
        update_cols_always=['id', 'value'])
    db.upsert_rows(
        pg_conn,
        'test_error_handling',
        [{'id': 4, 'name': 'Item4', 'value': 400}],
        update_cols_always=['name', 'value'])

    result = db.select(
        pg_conn, 'select id, name, value from test_error_handling order by id')
    assert [(r['id'], r['name'], r['value']) for r in result] == [
        (1, 'UpdatedItem1', 150),
        (3, 'Item2', 250),
        (4, 'Item4', 400),
        ]


def test_upsert_raises_on_another_unique_constraint(psql_docker, pg_conn):
    """Verify a non-target unique violation raises and the connection recovers.

    Mutation: swallowing the error, or leaving the transaction aborted.
    Oracle: Item4 already holds the unique name; id 6 is new.
    """
    db.execute(pg_conn, """
create temporary table test_error_handling (
    id integer primary key,
    name varchar(50) unique not null,
    value integer not null
)
""")
    db.execute(
        pg_conn, 'insert into test_error_handling values (4, %s, %s)', 'Item4', 400)

    with pytest.raises(db.UniqueViolation):
        db.upsert_rows(
            pg_conn,
            'test_error_handling',
            [{'id': 5, 'name': 'Item4', 'value': 500}],
            update_cols_always=['name', 'value'])

    db.upsert_rows(
        pg_conn,
        'test_error_handling',
        [{'id': 6, 'name': 'Item6', 'value': 600}],
        update_cols_always=['name', 'value'])

    ids = db.select_column(pg_conn, 'select id from test_error_handling order by id')
    assert ids == [4, 6]


def test_upsert_with_standard_constraint(psql_docker, pg_conn):
    """Verify a two-column constraint_name updates on a match, else inserts.

    Mutation: a target other than (id, name), or skipping last_updated.
    Oracle: a hand-written table after each upsert.
    """
    db.execute(pg_conn, """
create temporary table test_complex_constraint (
    id integer not null,
    name varchar(100),
    value integer,
    last_updated timestamp default now()
)
""")
    db.execute(pg_conn, """
alter table test_complex_constraint
add constraint complex_unique_constraint
unique (id, name)
""")
    db.insert(
        pg_conn,
        'insert into test_complex_constraint (id, name, value, last_updated) values (%s, %s, %s, %s)',
        1, 'ComplexTest', 100, datetime.datetime(2023, 3, 17, 17, 47, 15, 906191))

    def upsert(row):
        db.upsert_rows(
            pg_conn,
            'test_complex_constraint',
            [row],
            constraint_name='complex_unique_constraint',
            update_cols_always=['value', 'last_updated'])

    def table_state():
        result = db.select(
            pg_conn,
            'select id, value, last_updated from test_complex_constraint order by id')
        return [(r['id'], r['value'], r['last_updated']) for r in result]

    stamp = datetime.datetime(2023, 3, 17, 21, 47, 15, 908775)
    upsert({'id': 1, 'name': 'ComplexTest', 'value': 100, 'last_updated': stamp})
    assert table_state() == [(1, 100, stamp)]

    stamp = datetime.datetime(2023, 3, 17, 22, 47, 15, 908775)
    upsert({'id': 1, 'name': 'ComplexTest', 'value': 200, 'last_updated': stamp})
    assert table_state() == [(1, 200, stamp)]

    null_stamp = datetime.datetime(2023, 3, 19, 10, 0, 0)
    upsert({'id': 2, 'name': 'NullTest', 'value': None, 'last_updated': null_stamp})
    assert table_state() == [(1, 200, stamp), (2, None, null_stamp)]

    null_stamp = datetime.datetime(2023, 3, 20, 10, 0, 0)
    upsert({'id': 2, 'name': 'NullTest', 'value': None, 'last_updated': null_stamp})
    assert table_state() == [(1, 200, stamp), (2, None, null_stamp)]


def test_upsert_with_complex_index(psql_docker, pg_conn):
    """Verify an expression-index constraint_name matches every expression.

    Mutation: a target of plain columns, or one missing the value expression.
    Oracle: a hand-written table after each upsert.
    """
    db.execute(pg_conn, """
create temporary table test_complex_index (
    id integer not null,
    name varchar(100),
    value integer,
    last_updated timestamp default now()
)
""")
    db.execute(pg_conn, """
create unique index complex_unique_index
on test_complex_index (id, coalesce(name, ''), coalesce(value, -1))
""")
    db.insert(
        pg_conn,
        'insert into test_complex_index (id, name, value, last_updated) values (%s, %s, %s, %s)',
        1, 'ComplexTest', 100, datetime.datetime(2023, 3, 17, 17, 47, 15, 906191))

    def upsert(value, last_updated):
        db.upsert_rows(
            pg_conn,
            'test_complex_index',
            [{
                'id': 1,
                'name': 'ComplexTest',
                'value': value,
                'last_updated': last_updated,
                }],
            constraint_name='complex_unique_index',
            update_cols_always=['last_updated'])

    def table_state():
        result = db.select(
            pg_conn,
            'select value, last_updated from test_complex_index order by value')
        return [(r['value'], r['last_updated'].date()) for r in result]

    upsert(100, datetime.datetime(2023, 3, 18, 10, 0, 0))
    assert table_state() == [(100, datetime.date(2023, 3, 18))]

    upsert(200, datetime.datetime(2023, 3, 19, 10, 0, 0))
    assert table_state() == [
        (100, datetime.date(2023, 3, 18)),
        (200, datetime.date(2023, 3, 19)),
        ]

    upsert(200, datetime.datetime(2023, 3, 20, 10, 0, 0))
    assert table_state() == [
        (100, datetime.date(2023, 3, 18)),
        (200, datetime.date(2023, 3, 20)),
        ]


def test_upsert_with_column_order_mismatch(psql_docker, pg_conn):
    """Verify values bind by key when dict order differs from the table's.

    Mutation: binding values in dict order against the table's column order.
    Oracle: a hand-written table after shuffled-key inserts and updates.
    """
    db.execute(pg_conn, """
create temporary table test_column_order (
    id serial primary key,
    last_name varchar(50) not null,
    first_name varchar(50) not null,
    age integer not null,
    email varchar(100) null
)
""")
    rows = [
        {'email': 'john.doe@example.com', 'age': 30, 'first_name': 'John',
         'last_name': 'Doe', 'id': 1},
        {'first_name': 'Jane', 'email': 'jane.smith@example.com', 'id': 2,
         'last_name': 'Smith', 'age': 28},
        ]
    assert db.upsert_rows(pg_conn, 'test_column_order', rows) == 2

    def table_state():
        result = db.select(
            pg_conn,
            'select id, first_name, last_name, age, email from test_column_order order by id')
        return [tuple(r.values()) for r in result]

    assert table_state() == [
        (1, 'John', 'Doe', 30, 'john.doe@example.com'),
        (2, 'Jane', 'Smith', 28, 'jane.smith@example.com'),
        ]

    update_rows = [
        {'last_name': 'Doe-Updated', 'id': 1, 'age': 31, 'first_name': 'John'},
        {'age': 29, 'id': 2, 'last_name': 'Smith-Updated', 'first_name': 'Jane'},
        ]
    update_count = db.upsert_rows(
        pg_conn,
        'test_column_order',
        update_rows,
        use_primary_key=True,
        update_cols_always=['last_name', 'age'])
    assert update_count == 2

    assert table_state() == [
        (1, 'John', 'Doe-Updated', 31, 'john.doe@example.com'),
        (2, 'Jane', 'Smith-Updated', 29, 'jane.smith@example.com'),
        ]


def test_upsert_with_smartcase_update_columns(psql_docker, pg_conn):
    """Verify update_cols_always matches lower-case columns case-insensitively.

    Mutation: dropping an update column whose case differs from the table's.
    Oracle: 'Value' names the value column; the upsert supplies 200.
    """
    db.execute(pg_conn, """
create temporary table test_smart_case (
    name varchar(50) primary key,
    value integer not null
)
""")
    db.insert(
        pg_conn,
        'insert into test_smart_case (name, value) values (%s, %s)',
        'SmartCaseTest', 100)
    db.upsert_rows(
        pg_conn,
        'test_smart_case',
        [{'name': 'SmartCaseTest', 'value': 200}],
        use_primary_key=True,
        update_cols_always=['Value'])

    value = db.select_scalar(
        pg_conn, 'select value from test_smart_case where name = %s', 'SmartCaseTest')
    assert value == 200


if __name__ == '__main__':
    __import__('pytest').main([__file__])
