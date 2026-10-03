"""upsert_rows(conflict_columns=...) on a unique index that is not the key.
"""
import database as db
import pytest
from database.exceptions import ValidationError


@pytest.fixture
def pg_conflict_cols_conn(pg_conn):
    """Table with primary key id and a separate unique index on (a, b).
    """
    db.execute(pg_conn, 'drop table if exists conflict_cols_test')
    db.execute(pg_conn, """
create table conflict_cols_test (
    id serial primary key,
    a varchar(32) not null,
    b varchar(32) not null,
    v integer not null
)
""")
    db.execute(pg_conn,
               'create unique index conflict_cols_ab_idx on conflict_cols_test (a, b)')
    try:
        yield pg_conn
    finally:
        db.execute(pg_conn, 'drop table if exists conflict_cols_test')


@pytest.mark.postgres
@pytest.mark.integration
def test_conflict_columns_inserts_then_updates(pg_conflict_cols_conn):
    """Verify a second upsert on the same (a, b) updates the first row.

    Mutation: targeting the primary key in place of conflict_columns.
    Oracle: one row holding the second upsert's hand-chosen v.
    """
    cn = pg_conflict_cols_conn

    db.upsert_rows(cn, 'conflict_cols_test',
                   [{'a': 'x', 'b': 'y', 'v': 1}],
                   conflict_columns=['a', 'b'],
                   update_cols_always=['v'])

    db.upsert_rows(cn, 'conflict_cols_test',
                   [{'a': 'x', 'b': 'y', 'v': 99}],
                   conflict_columns=['a', 'b'],
                   update_cols_always=['v'])

    row_count = db.select_scalar(cn, 'select count(*) from conflict_cols_test')
    assert row_count == 1, f'Expected 1 row (update, not insert), got {row_count}'
    assert db.select_scalar(cn, 'select v from conflict_cols_test') == 99


@pytest.mark.postgres
@pytest.mark.integration
def test_conflict_columns_rejects_simultaneous_constraint_name(pg_conflict_cols_conn):
    """Verify conflict_columns and constraint_name cannot be combined.

    Mutation: dropping the mutual-exclusion check.
    Oracle: ValidationError naming the conflict.
    """
    with pytest.raises(ValidationError, match='mutually exclusive'):
        db.upsert_rows(pg_conflict_cols_conn, 'conflict_cols_test',
                       [{'a': 'x', 'b': 'y', 'v': 1}],
                       conflict_columns=['a', 'b'],
                       constraint_name='conflict_cols_ab_idx',
                       update_cols_always=['v'])


@pytest.mark.postgres
@pytest.mark.integration
def test_conflict_columns_fails_when_no_matching_unique_index(pg_conn):
    """Verify PostgreSQL rejects conflict columns no unique index covers.

    Mutation: a plain insert when no unique index matches the columns.
    Oracle: PostgreSQL's own on conflict error text.
    """
    db.execute(pg_conn, 'drop table if exists no_idx_test')
    db.execute(pg_conn,
               'create table no_idx_test (id serial primary key, a text, b text, v int)')
    try:
        with pytest.raises(Exception, match='no unique or exclusion constraint'):
            db.upsert_rows(pg_conn, 'no_idx_test',
                           [{'a': 'x', 'b': 'y', 'v': 1}],
                           conflict_columns=['a', 'b'],
                           update_cols_always=['v'])
    finally:
        db.execute(pg_conn, 'drop table if exists no_idx_test')
