"""Schema introspection on a SQLite ConnectionWrapper.
"""
import sqlite3
import sys
import threading
import time

import database as db
import pytest
from database import ColumnInfo, ValidationError
from database.cache import Cache
from database.strategy import get_db_strategy

PRICE_DDL = """CREATE TABLE price (
    zeta TEXT,
    code TEXT not null default 'x',
    qty numeric(10, 2),
    note,
    primary key (zeta, code),
    unique (qty, note)
)"""


@pytest.fixture
def schema_conn():
    """SQLite connection holding tables, a view, and an internal table.
    """
    conn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
    conn.execute(PRICE_DDL)
    conn.execute('create index price_note on price (note)')
    conn.execute('create table counter (id integer primary key autoincrement)')
    conn.execute('create table sqliteXdata (a)')
    conn.execute('create view price_view as select * from price')
    yield conn
    conn.close()


def test_list_tables_excludes_internal_tables_and_views(schema_conn):
    """Verify list_tables returns user tables only, ordered by name.

    Mutation: like 'sqlite_%' for glob 'sqlite_*', or a dropped filter.
    Oracle: hand-listed tables; autoincrement creates 'sqlite_sequence'.
    """
    assert schema_conn.list_tables() == ['counter', 'price', 'sqliteXdata']


def test_table_exists_matches_tables_by_name_ignoring_case(schema_conn):
    """Verify table_exists finds a table in any case and refuses a view.

    Mutation: dropping 'collate nocase', or dropping the type filter.
    Oracle: SQLite's own rule that identifiers ignore ASCII case.
    """
    assert schema_conn.table_exists('price') is True
    assert schema_conn.table_exists('PRICE') is True
    assert schema_conn.table_exists('price_view') is False
    assert schema_conn.table_exists('missing') is False


def test_describe_columns_reports_each_declared_column_in_order(schema_conn):
    """Verify describe_columns returns declared columns in declaration order.

    Mutation: ordering by name in place of cid, or swapped ColumnInfo fields.
    Oracle: hand-written records from PRICE_DDL.
    """
    assert schema_conn.describe_columns('price') == [
        ColumnInfo('zeta', 'TEXT', False, None, True),
        ColumnInfo('code', 'TEXT', True, "'x'", True),
        ColumnInfo('qty', 'numeric(10, 2)', False, None, False),
        ColumnInfo('note', '', False, None, False),
        ]


def test_describe_columns_raises_on_a_missing_table(schema_conn):
    """Verify describe_columns refuses a table that does not exist.

    Mutation: dropping the empty-result check.
    Oracle: a name no table or view carries.
    """
    with pytest.raises(ValidationError, match='missing'):
        schema_conn.describe_columns('missing')


def test_get_unique_indexes_lists_each_unique_index_in_column_order(schema_conn):
    """Verify every unique index appears with its columns in index order.

    Mutation: no unique filter, order by name for seqno, or no primary key.
    Oracle: PRICE_DDL, whose two keys list their columns out of name order.
    """
    assert schema_conn.get_unique_indexes('price') == [
        ['zeta', 'code'],
        ['qty', 'note'],
        ]


def test_table_ddl_returns_the_create_statement(schema_conn):
    """Verify table_ddl returns the create statement SQLite stored.

    Mutation: reading type 'index', or the name column in place of sql.
    Oracle: PRICE_DDL, the statement that created the table.
    """
    assert schema_conn.table_ddl('price') == PRICE_DDL


def test_table_ddl_raises_on_a_missing_table(schema_conn):
    """Verify table_ddl refuses a table that does not exist.

    Mutation: returning ddl[0] without the empty check.
    Oracle: a name no table carries.
    """
    with pytest.raises(ValidationError, match='missing'):
        schema_conn.table_ddl('missing')


def test_index_ddl_returns_each_stored_index_statement_by_name(schema_conn):
    """Verify index_ddl returns stored index text by index name, constraints out.

    Mutation: dropping 'sql is not null', or ordering by tbl_name.
    Oracle: hand-written statements; PRICE_DDL's two keys build indexes
        with no stored statement.
    """
    schema_conn.execute('create index z_counter on counter (id)')

    assert schema_conn.index_ddl() == [
        'CREATE INDEX price_note on price (note)',
        'CREATE INDEX z_counter on counter (id)',
        ]


def test_foreign_key_violations_reports_each_orphan_row(tmp_path):
    """Verify every orphan row is reported, a missing parent table included.

    Mutation: skipping keys whose parent table is missing, or returning
        the row dicts in place of tuples.
    Oracle: hand-listed orphans, written through a raw sqlite3 connection
        that does not enforce foreign keys; a null key is no orphan.
    """
    path = tmp_path / 'orphans.db'
    raw = sqlite3.connect(path)
    raw.executescript("""
create table parent (id integer primary key);
insert into parent values (1);
create table child (id integer primary key, parent_id integer references parent (id));
insert into child values (10, 1), (11, 99), (12, null);
create table stray (id integer primary key, gone_id integer references gone (id));
insert into stray values (20, 5);
create table keyed (k text primary key, parent_id integer references parent (id))
    without rowid;
insert into keyed values ('a', 98);
""")
    raw.close()

    with db.connect({'drivername': 'sqlite', 'database': str(path)},
                    role='reader') as cn:
        violations = cn.foreign_key_violations()

    assert sorted(violations, key=lambda row: row[0]) == [
        ('child', 11, 'parent', 0),
        ('keyed', None, 'parent', 0),
        ('stray', 20, 'gone', 0),
        ]


def test_foreign_key_violations_raises_on_a_key_with_no_unique_parent(schema_conn):
    """Verify a key whose parent columns carry no unique index raises.

    Mutation: catching the error and returning [], which reports a
        database whose keys cannot be checked as clean.
    Oracle: SQLite's own 'foreign key mismatch' for that schema.
    """
    schema_conn.execute('create table loose (a text)')
    schema_conn.execute('create table tied (a text references loose (a))')

    with pytest.raises(db.OperationalError, match='foreign key mismatch'):
        schema_conn.foreign_key_violations()


@pytest.mark.parametrize('table', ['type', 'seq', 'name'])
def test_introspection_reads_a_table_named_like_a_pragma_column(schema_conn, table):
    """Verify a table named after a pragma column is described as itself.

    Mutation: quote_identifier(table) into a pragma in place of a bound name.
    Oracle: a one-column table with a unique constraint, by hand.
    """
    schema_conn.execute(f'create table "{table}" (z TEXT unique)')

    assert schema_conn.describe_columns(table) == [
        ColumnInfo('z', 'TEXT', False, None, False),
        ]
    assert schema_conn.get_unique_indexes(table) == [['z']]


def test_introspection_ignores_a_temp_table(schema_conn):
    """Verify every method reads the main database only, as list_tables does.

    Mutation: dropping the 'main' argument from a pragma.
    Oracle: a temp table, which lives outside the main database.
    """
    schema_conn.execute('create temp table scratch (z text unique)')

    assert schema_conn.table_exists('scratch') is False
    assert schema_conn.get_unique_indexes('scratch') == []
    with pytest.raises(ValidationError, match='scratch'):
        schema_conn.describe_columns('scratch')


def test_clear_for_table_lets_insert_rows_see_an_added_column(tmp_path):
    """Verify a cleared table's columns are read again on the same engine.

    Mutation: get_table_columns keeping a cache Cache.clear_for_table
        never reaches.
    Oracle: the value written to the added column, read back.
    """
    cn = db.connect({'drivername': 'sqlite', 'database': str(tmp_path / 'w.db')})
    try:
        cn.execute('create table w (code text primary key, n integer)')
        cn.insert_rows('w', [{'code': 'a', 'n': 1}])
        cn.execute('alter table w add column extra text')
        Cache.get_instance().clear_for_table('w')

        cn.insert_rows('w', [{'code': 'b', 'n': 2, 'extra': 'X'}])

        assert db.select_scalar(cn, "select extra from w where code = 'b'") == 'X'
    finally:
        cn.close()


def test_two_database_files_keep_their_own_primary_keys(tmp_path):
    """Verify one table name in two files never shares a cached key list.

    Mutation: engine_cache_id dropped from the cacheable_strategy key.
    Oracle: the primary key each file's DDL declares.
    """
    first = db.connect({'drivername': 'sqlite', 'database': str(tmp_path / 'a.db')})
    second = db.connect({'drivername': 'sqlite', 'database': str(tmp_path / 'b.db')})
    try:
        first.execute('create table widgets (code text primary key, id integer)')
        second.execute('create table widgets (code text, id integer primary key)')

        assert get_db_strategy(first).get_primary_keys(first, 'widgets') == ['code']
        assert get_db_strategy(second).get_primary_keys(second, 'widgets') == ['id']
    finally:
        first.close()
        second.close()


@pytest.mark.slow
def test_schema_lookup_survives_a_concurrent_clear(tmp_path):
    """Verify get_table_columns never raises while another thread clears.

    Mutation: get_table_columns testing and reading the schema cache
        under a lock clear_for_table does not hold.
    Oracle: zero errors; the separate-lock version raises KeyError
        dozens of times a second under this switch interval.
    """
    previous_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    cn = db.connect({'drivername': 'sqlite', 'database': str(tmp_path / 'r.db')})
    cn.execute('create table t (a text primary key, b integer)')
    errors = []
    stop_at = time.monotonic() + 2

    def read_schema():
        while time.monotonic() < stop_at:
            try:
                cn.get_table_columns('t')
                cn.get_table_primary_keys('t')
            except Exception as exc:
                errors.append(exc)

    def clear_schema():
        while time.monotonic() < stop_at:
            Cache.get_instance().clear_for_table('t')

    threads = [threading.Thread(target=read_schema),
               threading.Thread(target=clear_schema)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    finally:
        sys.setswitchinterval(previous_interval)
        cn.close()

    assert errors == []
