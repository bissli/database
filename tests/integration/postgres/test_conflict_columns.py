"""
Integration test for the `conflict_columns` upsert parameter on a table
where the unique constraint is NOT the primary key. This proves the
new param actually generates valid PostgreSQL ON CONFLICT SQL — i.e.
PG accepts the column list against a real unique index that isn't the
PK.
"""
import database as db
import pytest


@pytest.fixture
def pg_conflict_cols_conn(pg_conn):
    """Table with PK=(id) but a separate UNIQUE INDEX on (a, b).

    The point of this fixture is to force the test to actually USE the
    unique index, not the PK — if the library silently fell back to PK,
    the second upsert would either fail (no PK in row data) or insert
    duplicates.
    """
    db.execute(pg_conn, 'DROP TABLE IF EXISTS conflict_cols_test')
    db.execute(pg_conn, """
        CREATE TABLE conflict_cols_test (
            id SERIAL PRIMARY KEY,
            a VARCHAR(32) NOT NULL,
            b VARCHAR(32) NOT NULL,
            v INTEGER NOT NULL
        )
    """)
    db.execute(pg_conn,
               'CREATE UNIQUE INDEX conflict_cols_ab_idx ON conflict_cols_test (a, b)')
    try:
        yield pg_conn
    finally:
        db.execute(pg_conn, 'DROP TABLE IF EXISTS conflict_cols_test')


@pytest.mark.postgres
@pytest.mark.integration
def test_conflict_columns_inserts_then_updates(pg_conflict_cols_conn):
    """Two upserts with the same (a, b) but different v: the second must
    UPDATE the first row, not INSERT a duplicate.
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

    row_count = db.select_scalar(cn, 'SELECT count(*) FROM conflict_cols_test')
    assert row_count == 1, f'Expected 1 row (update, not insert), got {row_count}'
    v = db.select_scalar(cn, 'SELECT v FROM conflict_cols_test')
    assert v == 99


@pytest.mark.postgres
@pytest.mark.integration
def test_conflict_columns_rejects_simultaneous_constraint_name(pg_conflict_cols_conn):
    """conflict_columns and constraint_name are mutually exclusive."""
    from database.exceptions import ValidationError
    cn = pg_conflict_cols_conn
    with pytest.raises(ValidationError, match='mutually exclusive'):
        db.upsert_rows(cn, 'conflict_cols_test',
                       [{'a': 'x', 'b': 'y', 'v': 1}],
                       conflict_columns=['a', 'b'],
                       constraint_name='conflict_cols_ab_idx',
                       update_cols_always=['v'])


@pytest.mark.postgres
@pytest.mark.integration
def test_conflict_columns_fails_when_no_matching_unique_index(pg_conn):
    """If the columns aren't covered by a unique index, PG must reject.

    This proves the param isn't doing anything sneaky — it generates the
    real ON CONFLICT SQL and PG's own constraint resolution applies.
    """
    db.execute(pg_conn, 'DROP TABLE IF EXISTS no_idx_test')
    db.execute(pg_conn, """
        CREATE TABLE no_idx_test (id SERIAL PRIMARY KEY, a TEXT, b TEXT, v INT)
    """)
    try:
        with pytest.raises(Exception, match='no unique or exclusion constraint'):
            db.upsert_rows(pg_conn, 'no_idx_test',
                           [{'a': 'x', 'b': 'y', 'v': 1}],
                           conflict_columns=['a', 'b'],
                           update_cols_always=['v'])
    finally:
        db.execute(pg_conn, 'DROP TABLE IF EXISTS no_idx_test')
