"""Schema introspection on a SQLite ConnectionWrapper.
"""
import database as db
import pytest
from database import ColumnInfo, ValidationError

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
