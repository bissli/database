"""SQLiteStrategy maintenance and metadata methods on a live database.
"""
import io

import database as db
import pytest
from database.strategy import SQLiteStrategy


@pytest.fixture
def sqlite_strategy_conn():
    """In-memory connection holding an autoincrement test_table, three rows.
    """
    conn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
    db.execute(conn, """
create table test_table (
    id integer primary key autoincrement,
    name text not null unique,
    value integer not null
)
""")
    db.execute(conn, """
insert into test_table (name, value) values
('Alice', 10),
('Bob', 20),
('Charlie', 30)
""")
    yield conn
    conn.close()


def test_sqlite_vacuum(sqlite_strategy_conn):
    """Verify vacuum_table runs a database-wide vacuum and keeps the rows.

    Mutation: 'vacuum {table}', which SQLite reads as a schema name.
    Oracle: the three staged rows, still there afterward.
    """
    db.vacuum_table(sqlite_strategy_conn, 'test_table')

    count = db.select_scalar(sqlite_strategy_conn, 'select count(*) from test_table')
    assert count == 3


def test_sqlite_reindex(sqlite_strategy_conn):
    """Verify reindex_table runs on a table carrying an index.

    Mutation: PostgreSQL's 'reindex table {table}' form.
    Oracle: the staged rows read back in value order.
    """
    db.execute(sqlite_strategy_conn, 'create index idx_test_value on test_table(value)')

    db.reindex_table(sqlite_strategy_conn, 'test_table')

    names = db.select_column(
        sqlite_strategy_conn, 'select name from test_table order by value')
    assert names == ['Alice', 'Bob', 'Charlie']


def test_sqlite_get_primary_keys(sqlite_strategy_conn):
    """Verify get_primary_keys returns the one primary key column.

    Mutation: reading pk = 0 in place of pk <> 0.
    Oracle: test_table's declared primary key.
    """
    strategy = SQLiteStrategy()

    assert strategy.get_primary_keys(sqlite_strategy_conn, 'test_table') == ['id']


def test_sqlite_get_columns(sqlite_strategy_conn):
    """Verify get_columns returns every column in declaration order.

    Mutation: selecting the type column in place of name, or filtering pk.
    Oracle: test_table's declared columns.
    """
    strategy = SQLiteStrategy()

    assert strategy.get_columns(sqlite_strategy_conn, 'test_table') == [
        'id', 'name', 'value']


def test_sqlite_get_sequence_columns(sqlite_strategy_conn):
    """Verify get_sequence_columns reports the primary key column.

    Mutation: get_sequence_columns returning [] for SQLite.
    Oracle: test_table's autoincrement primary key.
    """
    strategy = SQLiteStrategy()

    assert strategy.get_sequence_columns(sqlite_strategy_conn, 'test_table') == ['id']


def test_sqlite_copy_from_returns_zero(sqlite_strategy_conn, caplog):
    """Verify copy_from loads nothing, returns 0 and logs a warning.

    Mutation: copy_from returning None, or dropping the warning.
    Oracle: the warning text copy_from logs.
    """
    csv_data = io.StringIO('David,40\nEva,50\n')
    rowcount = db.copy_from(
        sqlite_strategy_conn, 'test_table', csv_data, ['name', 'value'])

    assert rowcount == 0
    assert 'COPY operation not supported in SQLite' in caplog.text


def test_sqlite_reset_table_sequence_runs_no_statement(sqlite_strategy_conn):
    """Verify reset_table_sequence on SQLite reads and writes nothing.

    Mutation: reset_sequence looking up a sequence column it never uses.
    Oracle: sqlite3's trace callback, which records every statement run.
    """
    statements = []
    sqlite_strategy_conn.dbapi_connection.set_trace_callback(statements.append)
    try:
        db.reset_table_sequence(sqlite_strategy_conn, 'test_table')
    finally:
        sqlite_strategy_conn.dbapi_connection.set_trace_callback(None)

    assert statements == []


@pytest.mark.parametrize(('table', 'spelling'), [
    ('type', 'type'),
    ('seq', 'seq'),
    ('type', '"type"'),
    ('type', 'main.type'),
    ('seq', 'main."seq"'),
    ])
def test_sqlite_metadata_reads_a_table_named_like_a_pragma_column(
        sqlite_strategy_conn, table, spelling):
    """Verify metadata methods read a table named after a pragma column.

    Mutation: a quoted name into a pragma, or the spelling bound unsplit.
    Oracle: a two-column table whose unique index is named seqno.
    """
    conn = sqlite_strategy_conn
    db.execute(conn, f'create table "{table}" (id integer primary key, z text)')
    db.execute(conn, f'create unique index seqno on "{table}" (z)')
    strategy = SQLiteStrategy()

    assert strategy.get_primary_keys(conn, spelling, bypass_cache=True) == ['id']
    assert strategy.get_columns(conn, spelling, bypass_cache=True) == ['id', 'z']
    assert strategy.get_ordered_columns(conn, spelling) == ['id', 'z']
    assert strategy.get_default_columns(conn, spelling) == ['id', 'z']
    assert strategy.get_unique_columns(conn, spelling, bypass_cache=True) == [['z']]


if __name__ == '__main__':
    __import__('pytest').main([__file__])
